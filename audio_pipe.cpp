#include "audio_pipe.hpp"

#include <cassert>
#include <iostream>
#include <chrono>
#include <time.h>

/* discard incoming text messages over the socket that are longer than this */
#define MAX_RECV_BUF_SIZE (65 * 1024 * 10)
#define RECV_BUF_REALLOC_SIZE (8 * 1024)

/* how often each service thread emits its occupancy summary */
#define THREAD_STATS_INTERVAL_US (2 * 1000000)

namespace {
  static const char* basicAuthUser = std::getenv("MOD_AUDIO_FORK_HTTP_AUTH_USER");
  static const char* basicAuthPassword = std::getenv("MOD_AUDIO_FORK_HTTP_AUTH_PASSWORD");

  static const char *requestedTcpKeepaliveSecs = std::getenv("MOD_AUDIO_FORK_TCP_KEEPALIVE_SECS");
  static int nTcpKeepaliveSecs = requestedTcpKeepaliveSecs ? ::atoi(requestedTcpKeepaliveSecs) : 55;

  /* ── Per-service-thread occupancy counters ──────────────────────────────
   * All of these live in the lws service thread, so plain thread_local
   * scalars are enough — no atomics, no locking on the hot path.
   * The point is to answer one question: what fraction of the service
   * thread's wall clock is spent inside callbacks (and specifically inside
   * the session write_frame calls that playback makes), because whatever is
   * left over is all the thread has to flush uplink audio with. */
  thread_local uint64_t tl_cb_us = 0;          /* total time inside lws_callback */
  thread_local uint64_t tl_cb_binary_us = 0;   /* subset: BINARY_AUDIO callbacks */
  thread_local uint64_t tl_cb_writeable_us = 0;/* subset: CLIENT_WRITEABLE callbacks */
  thread_local uint64_t tl_foreign_write_us = 0;/* subset: switch_core_session_write_frame */
  thread_local uint64_t tl_foreign_locate_us = 0;/* subset: switch_core_session_locate */
  thread_local uint32_t tl_cb_count = 0;
  thread_local uint32_t tl_cb_binary_count = 0;
  thread_local uint32_t tl_cb_writeable_count = 0;
  thread_local uint64_t tl_cb_worst_us = 0;
  thread_local int      tl_cb_worst_reason = -1;

  /* Actual CPU burned by this thread, as opposed to wall time spent in
   * callbacks. cb% only covers time inside lws_callback, so a thread pegged
   * at 100% inside lws_service()'s own poll/bookkeeping looks idle by that
   * measure. top showed one freeswitch thread at 99.9% of a core; this is how
   * we tell whether that thread is ours. */
  static uint64_t threadCpuUs(void) {
    struct timespec ts;
    if (0 != clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts)) return 0;
    return (uint64_t) ts.tv_sec * 1000000ULL + (uint64_t)(ts.tv_nsec / 1000);
  }
}

// remove once we update to lws with this helper
static int dch_lws_http_basic_auth_gen(const char *user, const char *pw, char *buf, size_t len) {
	size_t n = strlen(user), m = strlen(pw);
	char b[128];

	if (len < 6 + ((4 * (n + m + 1)) / 3) + 1)
		return 1;

	memcpy(buf, "Basic ", 6);

	n = lws_snprintf(b, sizeof(b), "%s:%s", user, pw);
	if (n >= sizeof(b) - 2)
		return 2;

	lws_b64_encode_string(b, n, buf + 6, len - 6);
	buf[len - 1] = '\0';

	return 0;
}

uint64_t AudioPipe::nowUs(void) {
  return (uint64_t) std::chrono::duration_cast<std::chrono::microseconds>(
    std::chrono::steady_clock::now().time_since_epoch()).count();
}

void AudioPipe::addForeignWriteUs(uint64_t us) {
  tl_foreign_write_us += us;
}

void AudioPipe::addForeignLocateUs(uint64_t us) {
  tl_foreign_locate_us += us;
}

int AudioPipe::lws_callback(struct lws *wsi,
  enum lws_callback_reasons reason,
  void *user, void *in, size_t len) {

  uint64_t started = nowUs();
  int rc = lws_callback_impl(wsi, reason, user, in, len);
  uint64_t elapsed = nowUs() - started;

  tl_cb_us += elapsed;
  tl_cb_count++;
  if (elapsed > tl_cb_worst_us) {
    tl_cb_worst_us = elapsed;
    tl_cb_worst_reason = (int) reason;
  }
  if (reason == LWS_CALLBACK_CLIENT_RECEIVE) {
    tl_cb_binary_us += elapsed;
    tl_cb_binary_count++;
  }
  else if (reason == LWS_CALLBACK_CLIENT_WRITEABLE) {
    tl_cb_writeable_us += elapsed;
    tl_cb_writeable_count++;
  }
  return rc;
}

int AudioPipe::lws_callback_impl(struct lws *wsi,
  enum lws_callback_reasons reason,
  void *user, void *in, size_t len) {

  struct AudioPipe::lws_per_vhost_data *vhd =
    (struct AudioPipe::lws_per_vhost_data *) lws_protocol_vh_priv_get(lws_get_vhost(wsi), lws_get_protocol(wsi));

  struct lws_vhost* vhost = lws_get_vhost(wsi);
  AudioPipe ** ppAp = (AudioPipe **) user;

  switch (reason) {
    case LWS_CALLBACK_PROTOCOL_INIT:
      vhd = (struct AudioPipe::lws_per_vhost_data *) lws_protocol_vh_priv_zalloc(lws_get_vhost(wsi), lws_get_protocol(wsi), sizeof(struct AudioPipe::lws_per_vhost_data));
      vhd->context = lws_get_context(wsi);
      vhd->protocol = lws_get_protocol(wsi);
      vhd->vhost = lws_get_vhost(wsi);
      break;

    case LWS_CALLBACK_CLIENT_APPEND_HANDSHAKE_HEADER:
      {
        AudioPipe* ap = findPendingConnect(wsi);
        if (ap && ap->hasBasicAuth()) {
          unsigned char **p = (unsigned char **)in, *end = (*p) + len;
          char b[128];
          std::string username, password;

          ap->getBasicAuth(username, password);
          lwsl_notice("AudioPipe::lws_service_thread LWS_CALLBACK_CLIENT_APPEND_HANDSHAKE_HEADER username: %s, password: xxxxxx\n", username.c_str());
          if (dch_lws_http_basic_auth_gen(username.c_str(), password.c_str(), b, sizeof(b))) break;
          if (lws_add_http_header_by_token(wsi, WSI_TOKEN_HTTP_AUTHORIZATION, (unsigned char *)b, strlen(b), p, end)) return -1;
        }
      }
      break;

    case LWS_CALLBACK_EVENT_WAIT_CANCELLED:
      processPendingConnects(vhd);
      processPendingDisconnects(vhd);
      processPendingWrites();
      break;
    case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
      {
        AudioPipe* ap = findAndRemovePendingConnect(wsi);
        int rc = lws_http_client_http_response(wsi);
        lwsl_err("AudioPipe::lws_service_thread LWS_CALLBACK_CLIENT_CONNECTION_ERROR: %s, response status %d\n", in ? (char *)in : "(null)", rc); 
        if (ap) {
          ap->m_state = LWS_CLIENT_FAILED;
          ap->m_callback(ap->m_uuid.c_str(), ap->m_bugname.c_str(), AudioPipe::CONNECT_FAIL, (char *) in);
        }
        else {
          lwsl_err("AudioPipe::lws_service_thread LWS_CALLBACK_CLIENT_CONNECTION_ERROR unable to find wsi %p..\n", wsi); 
        }
      }      
      break;

    case LWS_CALLBACK_CLIENT_ESTABLISHED:
      {
        AudioPipe* ap = findAndRemovePendingConnect(wsi);
        if (ap) {
          *ppAp = ap;
          ap->m_vhd = vhd;
          ap->m_state = LWS_CLIENT_CONNECTED;
          ap->m_callback(ap->m_uuid.c_str(), ap->m_bugname.c_str(), AudioPipe::CONNECT_SUCCESS, NULL);
        }
        else {
          lwsl_err("AudioPipe::lws_service_thread LWS_CALLBACK_CLIENT_ESTABLISHED %s unable to find wsi %p..\n", ap->m_uuid.c_str(), wsi); 
        }
      }      
      break;
    case LWS_CALLBACK_CLIENT_CLOSED:
      {
        AudioPipe* ap = *ppAp;
        if (!ap) {
          lwsl_err("AudioPipe::lws_service_thread LWS_CALLBACK_CLIENT_CLOSED %s unable to find wsi %p..\n", ap->m_uuid.c_str(), wsi); 
          return 0;
        }
        if (ap->m_state == LWS_CLIENT_DISCONNECTING) {
          // closed by us
          ap->m_callback(ap->m_uuid.c_str(), ap->m_bugname.c_str(), AudioPipe::CONNECTION_CLOSED_GRACEFULLY, NULL);
        }
        else if (ap->m_state == LWS_CLIENT_CONNECTED) {
          // closed by far end
          lwsl_notice("%s socket closed by far end\n", ap->m_uuid.c_str());
          ap->m_callback(ap->m_uuid.c_str(), ap->m_bugname.c_str(), AudioPipe::CONNECTION_DROPPED, NULL);
        }
        ap->m_state = LWS_CLIENT_DISCONNECTED;

        //NB: after receiving any of the events above, any holder of a 
        //pointer or reference to this object must treat is as no longer valid

        *ppAp = NULL;
        delete ap;
      }
      break;

    case LWS_CALLBACK_CLIENT_RECEIVE:
      {
        AudioPipe* ap = *ppAp;
        if (!ap) {
          lwsl_err("AudioPipe::lws_service_thread LWS_CALLBACK_CLIENT_RECEIVE %s unable to find wsi %p..\n", ap->m_uuid.c_str(), wsi); 
          return 0;
        }

        if (lws_frame_is_binary(wsi)) {
          /* Binary frame inbound: PCM from realtime provider (already channel-rate after backend resampling).
           * Pass pointer directly — valid only for the duration of this callback.
           * The consumer (fork_session_handle_binary) copies into the playback ring buffer. */
          ap->m_binary_payload     = (uint8_t *) in;
          ap->m_binary_payload_len = len;
          ap->m_callback(ap->m_uuid.c_str(), ap->m_bugname.c_str(), AudioPipe::BINARY_AUDIO, nullptr);
          ap->m_binary_payload     = nullptr;
          ap->m_binary_payload_len = 0;
          return 0;
        }

        if (lws_is_first_fragment(wsi)) {
          // allocate a buffer for the entire chunk of memory needed
          assert(nullptr == ap->m_recv_buf);
          ap->m_recv_buf_len = len + lws_remaining_packet_payload(wsi);
          ap->m_recv_buf = (uint8_t*) malloc(ap->m_recv_buf_len);
          ap->m_recv_buf_ptr = ap->m_recv_buf;
        }

        size_t write_offset = ap->m_recv_buf_ptr - ap->m_recv_buf;
        size_t remaining_space = ap->m_recv_buf_len - write_offset;
        if (remaining_space < len) {
          lwsl_notice("AudioPipe::lws_service_thread LWS_CALLBACK_CLIENT_RECEIVE buffer realloc needed.\n");
          size_t newlen = ap->m_recv_buf_len + RECV_BUF_REALLOC_SIZE;
          if (newlen > MAX_RECV_BUF_SIZE) {
            free(ap->m_recv_buf);
            ap->m_recv_buf = ap->m_recv_buf_ptr = nullptr;
            ap->m_recv_buf_len = 0;
            lwsl_notice("AudioPipe::lws_service_thread LWS_CALLBACK_CLIENT_RECEIVE max buffer exceeded, truncating message.\n");
          }
          else {
            ap->m_recv_buf = (uint8_t*) realloc(ap->m_recv_buf, newlen);
            if (nullptr != ap->m_recv_buf) {
              ap->m_recv_buf_len = newlen;
              ap->m_recv_buf_ptr = ap->m_recv_buf + write_offset;
            }
          }
        }

        if (nullptr != ap->m_recv_buf) {
          if (len > 0) {
            memcpy(ap->m_recv_buf_ptr, in, len);
            ap->m_recv_buf_ptr += len;
          }
          if (lws_is_final_fragment(wsi)) {
            if (nullptr != ap->m_recv_buf) {
              std::string msg((char *)ap->m_recv_buf, ap->m_recv_buf_ptr - ap->m_recv_buf);
              ap->m_callback(ap->m_uuid.c_str(), ap->m_bugname.c_str(), AudioPipe::MESSAGE, msg.c_str());
              if (nullptr != ap->m_recv_buf) free(ap->m_recv_buf);
            }
            ap->m_recv_buf = ap->m_recv_buf_ptr = nullptr;
            ap->m_recv_buf_len = 0;
          }
        }
      }
      break;

    case LWS_CALLBACK_CLIENT_WRITEABLE:
      {
        AudioPipe* ap = *ppAp;
        if (!ap) {
          lwsl_err("AudioPipe::lws_service_thread LWS_CALLBACK_CLIENT_WRITEABLE %s unable to find wsi %p..\n", ap->m_uuid.c_str(), wsi); 
          return 0;
        }

        // check for graceful close - send a zero length binary frame
        if (ap->isGracefulShutdown()) {
          lwsl_notice("%s graceful shutdown - sending zero length binary frame to flush any final responses\n", ap->m_uuid.c_str());
          std::lock_guard<std::mutex> lk(ap->m_audio_mutex);
          int sent = lws_write(wsi, (unsigned char *) ap->m_audio_buffer + LWS_PRE, 0, LWS_WRITE_BINARY);
          return 0;
        }

        // check for text frames to send
        {
          std::lock_guard<std::mutex> lk(ap->m_text_mutex);
          if (ap->m_metadata.length() > 0) {
            uint8_t buf[ap->m_metadata.length() + LWS_PRE];
            memcpy(buf + LWS_PRE, ap->m_metadata.c_str(), ap->m_metadata.length());
            int n = ap->m_metadata.length();
            int m = lws_write(wsi, buf + LWS_PRE, n, LWS_WRITE_TEXT);
            ap->m_metadata.clear();
            if (m < n) {
              return -1;
            }

            // there may be audio data, but only one write per writeable event
            // get it next time
            lws_callback_on_writable(wsi);

            return 0;
          }
        }

        if (ap->m_state == LWS_CLIENT_DISCONNECTING) {
          lws_close_reason(wsi, LWS_CLOSE_STATUS_NORMAL, NULL, 0);
          return -1;
        }

        // check for audio packets
        {
          std::lock_guard<std::mutex> lk(ap->m_audio_mutex);
          if (ap->m_audio_buffer_write_offset > LWS_PRE) {
            size_t datalen = ap->m_audio_buffer_write_offset - LWS_PRE;
            int sent = lws_write(wsi, (unsigned char *) ap->m_audio_buffer + LWS_PRE, datalen, LWS_WRITE_BINARY);
            if (sent < datalen) {
              lwsl_err("AudioPipe::lws_service_thread LWS_CALLBACK_CLIENT_WRITEABLE %s attemped to send %lu only sent %d wsi %p..\n",
                ap->m_uuid.c_str(), datalen, sent, wsi);
            }
            ap->m_audio_buffer_write_offset = LWS_PRE;
            if (sent > 0) {
              ap->m_stat_last_flush_us = nowUs();
              ap->m_stat_flush_count++;
              ap->m_stat_flushed_bytes += (uint64_t) sent;
            }
          }
        }

        return 0;
      }
      break;

    default:
      break;
  }
  return lws_callback_http_dummy(wsi, reason, user, in, len);
}


// static members
static const lws_retry_bo_t retry = {
    nullptr,   // retry_ms_table
    0,         // retry_ms_table_count
    0,         // conceal_count
    UINT16_MAX,         // secs_since_valid_ping
    UINT16_MAX,        // secs_since_valid_hangup
    0          // jitter_percent
};

struct lws_context *AudioPipe::contexts[] = {
  nullptr, nullptr, nullptr, nullptr, nullptr,
  nullptr, nullptr, nullptr, nullptr, nullptr
};
unsigned int AudioPipe::numContexts = 0;
std::atomic<unsigned int> AudioPipe::nchild(0);
std::string AudioPipe::protocolName;
std::mutex AudioPipe::mutex_connects;
std::mutex AudioPipe::mutex_disconnects;
std::mutex AudioPipe::mutex_writes;
std::list<AudioPipe*> AudioPipe::pendingConnects;
std::list<AudioPipe*> AudioPipe::pendingDisconnects;
std::list<AudioPipe*> AudioPipe::pendingWrites;
AudioPipe::log_emit_function AudioPipe::logger;
std::atomic<bool> AudioPipe::stopping(false);
std::vector<std::thread> AudioPipe::serviceThreads;
std::mutex AudioPipe::mutex_contexts;

void AudioPipe::processPendingConnects(lws_per_vhost_data *vhd) {
  std::list<AudioPipe*> connects;
  {
    std::lock_guard<std::mutex> guard(mutex_connects);
    for (auto it = pendingConnects.begin(); it != pendingConnects.end(); ++it) {
      if ((*it)->m_state == LWS_CLIENT_IDLE) {
        connects.push_back(*it);
        (*it)->m_state = LWS_CLIENT_CONNECTING;
      }
    }
  }
  for (auto it = connects.begin(); it != connects.end(); ++it) {
    AudioPipe* ap = *it;
    ap->connect_client(vhd);   
  }
}

void AudioPipe::processPendingDisconnects(lws_per_vhost_data *vhd) {
  std::list<AudioPipe*> disconnects;
  {
    std::lock_guard<std::mutex> guard(mutex_disconnects);
    for (auto it = pendingDisconnects.begin(); it != pendingDisconnects.end(); ++it) {
      if ((*it)->m_state == LWS_CLIENT_DISCONNECTING) disconnects.push_back(*it);
    }
    pendingDisconnects.clear();
  }
  for (auto it = disconnects.begin(); it != disconnects.end(); ++it) {
    AudioPipe* ap = *it;
    lws_callback_on_writable(ap->m_wsi); 
  }
}

void AudioPipe::processPendingWrites() {
  std::list<AudioPipe*> writes;
  {
    std::lock_guard<std::mutex> guard(mutex_writes);
    for (auto it = pendingWrites.begin(); it != pendingWrites.end(); ++it) {
       if ((*it)->m_state == LWS_CLIENT_CONNECTED) writes.push_back(*it);
    }  
    pendingWrites.clear();
  }
  for (auto it = writes.begin(); it != writes.end(); ++it) {
    AudioPipe* ap = *it;
    lws_callback_on_writable(ap->m_wsi);
  }
}

AudioPipe* AudioPipe::findAndRemovePendingConnect(struct lws *wsi) {
  AudioPipe* ap = NULL;
  std::lock_guard<std::mutex> guard(mutex_connects);
  std::list<AudioPipe* > toRemove;

  for (auto it = pendingConnects.begin(); it != pendingConnects.end() && !ap; ++it) {
    int state = (*it)->m_state;

    if ((*it)->m_wsi == nullptr)
      toRemove.push_back(*it);

    if ((state == LWS_CLIENT_CONNECTING) &&
      (*it)->m_wsi == wsi) ap = *it;
  }

  for (auto it = toRemove.begin(); it != toRemove.end(); ++it)
    pendingConnects.remove(*it);

  if (ap) {
    pendingConnects.remove(ap);
  }

  return ap;
}

AudioPipe* AudioPipe::findPendingConnect(struct lws *wsi) {
  AudioPipe* ap = NULL;
  std::lock_guard<std::mutex> guard(mutex_connects);

  for (auto it = pendingConnects.begin(); it != pendingConnects.end() && !ap; ++it) {
    int state = (*it)->m_state;
    if ((state == LWS_CLIENT_CONNECTING) &&
      (*it)->m_wsi == wsi) ap = *it;
  }
  return ap;
}

void AudioPipe::addPendingConnect(AudioPipe* ap) {
  {
    std::lock_guard<std::mutex> guard(mutex_connects);
    pendingConnects.push_back(ap);
    lwsl_notice("%s after adding connect there are %lu pending connects\n",
      ap->m_uuid.c_str(), pendingConnects.size());
  }
  /* Round-robin the wakeup across contexts. Guarded: during shutdown
   * numContexts drops to 0 (which would divide by zero) and slots go null. */
  std::lock_guard<std::mutex> lk(mutex_contexts);
  if (stopping || 0 == numContexts) return;
  struct lws_context *ctx = contexts[nchild++ % numContexts];
  if (ctx) lws_cancel_service(ctx);
}
void AudioPipe::addPendingDisconnect(AudioPipe* ap) {
  ap->m_state = LWS_CLIENT_DISCONNECTING;
  {
    std::lock_guard<std::mutex> guard(mutex_disconnects);
    pendingDisconnects.push_back(ap);
    lwsl_notice("%s after adding disconnect there are %lu pending disconnects\n",
      ap->m_uuid.c_str(), pendingDisconnects.size());
  }
  if (stopping || !ap->m_vhd) return;
  lws_cancel_service(ap->m_vhd->context);
}
void AudioPipe::addPendingWrite(AudioPipe* ap) {
  {
    std::lock_guard<std::mutex> guard(mutex_writes);
    pendingWrites.push_back(ap);
  }
  ap->m_stat_last_queue_us = nowUs();
  ap->m_stat_queue_count++;
  if (stopping || !ap->m_vhd) return;
  lws_cancel_service(ap->m_vhd->context);
}

bool AudioPipe::lws_service_thread(unsigned int nServiceThread) {
  struct lws_context_creation_info info;

  const struct lws_protocols protocols[] = {
    {
      protocolName.c_str(),
      AudioPipe::lws_callback,
      sizeof(void *),
      1024,
    },
    { NULL, NULL, 0, 0 }
  };

  memset(&info, 0, sizeof info); 
  info.port = CONTEXT_PORT_NO_LISTEN; 
  info.protocols = protocols;
  info.options = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;

  info.ka_time = nTcpKeepaliveSecs;                    // tcp keep-alive timer
  info.ka_probes = 4;                   // number of times to try ka before closing connection
  info.ka_interval = 5;                 // time between ka's
  info.timeout_secs = 10;                // doc says timeout for "various processes involving network roundtrips"
  info.keepalive_timeout = 5;           // seconds to allow remote client to hold on to an idle HTTP/1.1 connection 
  info.timeout_secs_ah_idle = 10;       // secs to allow a client to hold an ah without using it
  info.retry_and_idle_policy = &retry;

  lwsl_notice("AudioPipe::lws_service_thread creating context in service thread %d.\n", nServiceThread);

  struct lws_context *context = lws_create_context(&info);
  if (!context) {
    lwsl_err("AudioPipe::lws_service_thread failed creating context in service thread %d..\n", nServiceThread);
    return false;
  }
  {
    std::lock_guard<std::mutex> lk(mutex_contexts);
    contexts[nServiceThread] = context;
  }

  int n;
  uint64_t window_start = nowUs();
  uint64_t cpu_start = threadCpuUs();
  uint64_t loops = 0;
  do {
    n = lws_service(context, 0);
    loops++;

    /* Periodic occupancy report. If cb% approaches 100 the thread has no
     * headroom left to flush uplink audio, which is what starves fork_frame
     * and shows up as "dropping packets!" on every session at once. */
    uint64_t nowus = nowUs();
    uint64_t window = nowus - window_start;
    if (window >= THREAD_STATS_INTERVAL_US) {
      size_t nPendingWrites;
      {
        std::lock_guard<std::mutex> guard(mutex_writes);
        nPendingWrites = pendingWrites.size();
      }
      uint64_t cpu_us = threadCpuUs() - cpu_start;
      lwsl_notice("[LWS-THREAD %u] window_ms=%llu cpu=%llums(%llu%%) loops=%llu cb=%llums(%llu%%,n=%u) "
        "recv=%llums(n=%u) writeable=%llums(n=%u) "
        "session_write_frame=%llums(%llu%%) session_locate=%llums(%llu%%) "
        "worst_cb=%lluus(reason=%d) pending_writes=%zu\n",
        nServiceThread,
        (unsigned long long)(window / 1000),
        (unsigned long long)(cpu_us / 1000),
        (unsigned long long)(cpu_us * 100 / window),
        (unsigned long long) loops,
        (unsigned long long)(tl_cb_us / 1000),
        (unsigned long long)(tl_cb_us * 100 / window), tl_cb_count,
        (unsigned long long)(tl_cb_binary_us / 1000), tl_cb_binary_count,
        (unsigned long long)(tl_cb_writeable_us / 1000), tl_cb_writeable_count,
        (unsigned long long)(tl_foreign_write_us / 1000),
        (unsigned long long)(tl_foreign_write_us * 100 / window),
        (unsigned long long)(tl_foreign_locate_us / 1000),
        (unsigned long long)(tl_foreign_locate_us * 100 / window),
        (unsigned long long) tl_cb_worst_us, tl_cb_worst_reason,
        nPendingWrites);

      tl_cb_us = tl_cb_binary_us = tl_cb_writeable_us = 0;
      tl_foreign_write_us = tl_foreign_locate_us = 0;
      tl_cb_count = tl_cb_binary_count = tl_cb_writeable_count = 0;
      tl_cb_worst_us = 0;
      tl_cb_worst_reason = -1;
      loops = 0;
      window_start = nowus;
      cpu_start = threadCpuUs();
    }
  } while (n >= 0 && !stopping);

  /* This thread created the context, so this thread destroys it. Destroying it
   * from the unloading thread while lws_service() was still running was half of
   * the reload corruption; the other half was detaching, which let a surviving
   * thread pick up the *next* generation's context out of contexts[]. */
  struct lws_context *toDestroy = nullptr;
  {
    std::lock_guard<std::mutex> lk(mutex_contexts);
    toDestroy = contexts[nServiceThread];
    contexts[nServiceThread] = nullptr;
  }
  if (toDestroy) lws_context_destroy(toDestroy);

  lwsl_notice("AudioPipe::lws_service_thread ending in service thread %d\n", nServiceThread);
  return true;
}

void AudioPipe::initialize(const char* protocol, unsigned int nThreads, int loglevel, log_emit_function logger) {
  assert(nThreads > 0 && nThreads <= 10);

  numContexts = nThreads;
  protocolName = protocol;
  stopping = false;
  nchild = 0;
  lws_set_log_level(loglevel, logger);

  lwsl_notice("AudioPipe::initialize starting %d threads with subprotocol %s\n", nThreads, protocol);
  for (unsigned int i = 0; i < numContexts; i++) {
    serviceThreads.emplace_back(&AudioPipe::lws_service_thread, i);
  }
}

bool AudioPipe::deinitialize() {
  lwsl_notice("AudioPipe::deinitialize\n");

  /* Ask every service thread to leave its loop, then wake them so they notice
   * now instead of after an lws_service timeout. Under mutex_contexts so we can
   * never signal a context a thread is already tearing down. */
  stopping = true;
  {
    std::lock_guard<std::mutex> lk(mutex_contexts);
    for (unsigned int i = 0; i < numContexts; i++) {
      if (contexts[i]) lws_cancel_service(contexts[i]);
    }
  }

  /* Join, do not sleep-and-hope. Returning from here has to mean no thread is
   * still executing inside this .so, otherwise the module unloads underneath a
   * running thread and the next load inherits a zombie. */
  for (unsigned int i = 0; i < serviceThreads.size(); i++) {
    if (serviceThreads[i].joinable()) {
      lwsl_notice("AudioPipe::deinitialize joining service thread %d of %zu\n", i + 1, serviceThreads.size());
      serviceThreads[i].join();
    }
  }
  serviceThreads.clear();
  numContexts = 0;
  return true;
}

// instance members
AudioPipe::AudioPipe(const char* uuid, const char* host, unsigned int port, const char* path,
  int sslFlags, size_t bufLen, size_t minFreespace, const char* username, const char* password, char* bugname, notifyHandler_t callback) :
  m_uuid(uuid), m_host(host), m_port(port), m_path(path), m_sslFlags(sslFlags),
  m_audio_buffer_min_freespace(minFreespace), m_audio_buffer_max_len(bufLen), m_gracefulShutdown(false),
  m_audio_buffer_write_offset(LWS_PRE), m_recv_buf(nullptr), m_recv_buf_ptr(nullptr), m_bugname(bugname),
  m_state(LWS_CLIENT_IDLE), m_wsi(nullptr), m_vhd(nullptr), m_callback(callback) {

  if (username && password) {
    m_username.assign(username);
    m_password.assign(password);
  }

  m_audio_buffer = new uint8_t[m_audio_buffer_max_len];
  m_binary_payload = nullptr;
  m_binary_payload_len = 0;

  m_stat_last_flush_us = 0;
  m_stat_last_queue_us = 0;
  m_stat_flush_count = 0;
  m_stat_queue_count = 0;
  m_stat_flushed_bytes = 0;
}
AudioPipe::~AudioPipe() {
  if (m_audio_buffer) delete [] m_audio_buffer;
  if (m_recv_buf) delete [] m_recv_buf;
}

void AudioPipe::connect(void) {
  addPendingConnect(this);
}

bool AudioPipe::connect_client(struct lws_per_vhost_data *vhd) {
  assert(m_audio_buffer != nullptr);
  assert(m_vhd == nullptr);

  struct lws_client_connect_info i;

  memset(&i, 0, sizeof(i));
  i.context = vhd->context;
  i.port = m_port;
  i.address = m_host.c_str();
  i.path = m_path.c_str();
  i.host = i.address;
  i.origin = i.address;
  i.ssl_connection = m_sslFlags;
  i.protocol = protocolName.c_str();
  i.pwsi = &(m_wsi);

  m_state = LWS_CLIENT_CONNECTING;
  m_vhd = vhd;

  m_wsi = lws_client_connect_via_info(&i);
  lwsl_notice("%s attempting connection, wsi is %p\n", m_uuid.c_str(), m_wsi);

  return nullptr != m_wsi;
}

void AudioPipe::bufferForSending(const char* text) {
  if (m_state != LWS_CLIENT_CONNECTED) return;
  {
    std::lock_guard<std::mutex> lk(m_text_mutex);
    m_metadata.append(text);
  }
  addPendingWrite(this);
}

void AudioPipe::unlockAudioBuffer() {
  if (m_audio_buffer_write_offset > LWS_PRE) addPendingWrite(this);
  m_audio_mutex.unlock();
}

void AudioPipe::close() {
  if (m_state != LWS_CLIENT_CONNECTED) return;
  addPendingDisconnect(this);
}

void AudioPipe::do_graceful_shutdown() {
  m_gracefulShutdown = true;
  addPendingWrite(this);
}
