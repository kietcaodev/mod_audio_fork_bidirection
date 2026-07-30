#include <switch.h>
#include <switch_json.h>
#include <string.h>
#include <string>
#include <mutex>
#include <thread>
#include <list>
#include <algorithm>
#include <functional>
#include <cassert>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <regex>
#include <vector>
#include <cmath>    /* [BUG-RE] TEMPORARY: sqrt for the RMS in [MOD-AUDIO-STATS-C] */

#include "base64.hpp"
#include "parser.hpp"
#include "mod_audio_fork.h"
#include "audio_pipe.hpp"

/* Forward decl: defined later in this TU inside extern "C" block */
extern "C" void fork_session_handle_binary(private_t *tech_pvt, switch_core_session_t *session, const uint8_t *data, size_t len);

#define RTP_PACKETIZATION_PERIOD 20
#define FRAME_SIZE_8000  320 /* 20ms frame: 320 bytes at 8kHz PCM16 mono */

namespace {
  static const char *requestedBufferSecs = std::getenv("MOD_AUDIO_FORK_BUFFER_SECS");
  static int nAudioBufferSecs = std::max(1, std::min(requestedBufferSecs ? ::atoi(requestedBufferSecs) : 2, 5));
  static const char *requestedNumServiceThreads = std::getenv("MOD_AUDIO_FORK_SERVICE_THREADS");
  static const char* mySubProtocolName = std::getenv("MOD_AUDIO_FORK_SUBPROTOCOL_NAME") ?
    std::getenv("MOD_AUDIO_FORK_SUBPROTOCOL_NAME") : "audio.drachtio.org";
  static unsigned int nServiceThreads = std::max(1, std::min(requestedNumServiceThreads ? ::atoi(requestedNumServiceThreads) : 1, 5));

  /* Depth of the per-session playback jitter buffer, in 20ms frames.
   * switch_core_session_write_frame() occasionally blocks for two frame
   * intervals instead of one, which at 5 frames (100ms) was enough to overflow
   * and drop audio the backend had already sent. The buffer sits near-empty in
   * steady state, so extra depth costs no latency until a burst actually needs
   * it -- but note that once depth accumulates the writer only drains at
   * realtime, so it persists until the utterance ends or a flush arrives. */
  /* [BUG-RE] TEMPORARY. Set MOD_AUDIO_FORK_DUMP_PCM_UUID to a call uuid (or to
   * "all") and every binary frame that arrives for it is appended verbatim to
   * <temp_dir>/<uuid>.rx.raw. Import in Audacity as PCM16 LE / 8000 Hz / mono.
   * No counter can answer "was it already distorted when it got here"; this
   * can. Off unless the variable is set. */
  static const char *dumpPcmUuid = std::getenv("MOD_AUDIO_FORK_DUMP_PCM_UUID");

  /* Pace the writer off a real timer instead of relying on
   * switch_core_session_write_frame() to block. That reliance held on a busy
   * box (measured avg_write_us=17811 when the channel's own write path was
   * contended) but not on an idle one: on prod the same call reports
   * avg_write_us=0, so nothing paced the audio at all and the write cadence
   * was simply whatever cadence TCP delivered frames in -- 15% of intervals
   * over 30ms and 15% under 10ms, across all 25 calls measured.
   * Set MOD_AUDIO_FORK_PLAYBACK_PACED=0 to fall back to the old behaviour for
   * an A/B. Frames to hold before starting: MOD_AUDIO_FORK_PLAYBACK_PRIME. */
  static const char *requestedPaced = std::getenv("MOD_AUDIO_FORK_PLAYBACK_PACED");
  static int nPlaybackPaced = requestedPaced ? ::atoi(requestedPaced) : 1;
  static const char *requestedPrime = std::getenv("MOD_AUDIO_FORK_PLAYBACK_PRIME");
  static int nPlaybackPrimeFrames =
    std::max(0, std::min(requestedPrime ? ::atoi(requestedPrime) : 2, 10));

  static const char *requestedJitterFrames = std::getenv("MOD_AUDIO_FORK_PLAYBACK_JITTER_FRAMES");
  static int nPlaybackJitterFrames =
    std::max(3, std::min(requestedJitterFrames ? ::atoi(requestedJitterFrames) : 15, 50));
  static unsigned int idxCallCount = 0;
  static uint32_t playCount = 0;

  /* [BUG-RE] TEMPORARY. Measure the waveform actually handed to the channel.
   * Called on exactly the bytes passed to switch_core_session_write_frame, so
   * whatever this reports is what the caller was sent. */
  static void dbg_measure_frame(private_t *tech_pvt, const uint8_t *buf, size_t bytes) {
    const int16_t *s = (const int16_t *) buf;
    size_t n = bytes / 2;
    if (n == 0) return;

    /* Boundary step: discontinuity between frames, which is what a splice
     * sounds like. Tracked separately from interior steps so the two can be
     * compared -- a boundary mean far above the interior mean is the proof. */
    if (tech_pvt->dbg_a_have_prev) {
      uint32_t step = (uint32_t) abs((int) s[0] - tech_pvt->dbg_a_prev_last);
      tech_pvt->dbg_a_bstep_sum += step;
      tech_pvt->dbg_a_bstep_n++;
      if (step > tech_pvt->dbg_a_bstep_max) tech_pvt->dbg_a_bstep_max = step;
    }
    tech_pvt->dbg_a_prev_last = s[n - 1];
    tech_pvt->dbg_a_have_prev = 1;

    int allZero = 1;
    for (size_t i = 0; i < n; i++) {
      int v = s[i];
      int a = v < 0 ? -v : v;
      if (a) allZero = 0;
      if ((uint32_t) a > tech_pvt->dbg_a_peak) tech_pvt->dbg_a_peak = (uint32_t) a;
      if (a >= 32000) tech_pvt->dbg_a_clip++;
      tech_pvt->dbg_a_sumsq += (uint64_t)(v * v);
      if (i > 0) {
        int d = v - s[i - 1];
        tech_pvt->dbg_a_sumabsdiff += (uint64_t)(d < 0 ? -d : d);
        tech_pvt->dbg_a_interior_n++;
      }
    }
    tech_pvt->dbg_a_samples += n;
    if (allZero) tech_pvt->dbg_a_zero_frames++;

    /* Same numbers again, but scoped to a one-second window. A call-wide mean
     * averages a 9-second defect into 70 seconds of clean speech and shows
     * nothing; this keeps the worst second so a localised burst survives. */
    for (size_t i = 0; i < n; i++) {
      int v = s[i];
      int a = v < 0 ? -v : v;
      if ((uint32_t) a > tech_pvt->dbg_w_peak) tech_pvt->dbg_w_peak = (uint32_t) a;
      if (a >= 32000) tech_pvt->dbg_w_clip++;
      tech_pvt->dbg_w_sumsq += (uint64_t)(v * v);
      if (i > 0) {
        int d = v - s[i - 1];
        tech_pvt->dbg_w_sumabsdiff += (uint64_t)(d < 0 ? -d : d);
        tech_pvt->dbg_w_interior_n++;
      }
    }
    tech_pvt->dbg_w_samples += (uint32_t) n;
    tech_pvt->dbg_w_frames++;

    /* 50 frames of 20ms == one second. */
    if (tech_pvt->dbg_w_frames >= 50) {
      tech_pvt->dbg_windows_total++;
      if (tech_pvt->dbg_w_samples > 0 && tech_pvt->dbg_w_interior_n > 0) {
        double wrms = sqrt((double) tech_pvt->dbg_w_sumsq / (double) tech_pvt->dbg_w_samples);
        double wmad = (double) tech_pvt->dbg_w_sumabsdiff / (double) tech_pvt->dbg_w_interior_n;
        /* Ignore near-silent windows: with almost no signal the ratio is noise
         * on noise and would produce false alarms. */
        if (wrms > 200.0) {
          int ratio = (int)(wmad * 100.0 / wrms);
          if (ratio > tech_pvt->dbg_worst_ratio) {
            tech_pvt->dbg_worst_ratio = ratio;
            tech_pvt->dbg_worst_at_ms = tech_pvt->dbg_w_index * 1000;
          }
          /* Baseline measured across 20 calls is 23-25, so 40 is comfortably
           * outside normal speech and marks a window worth looking at. */
          if (ratio > 40) tech_pvt->dbg_windows_over++;
        }
      }
      if (tech_pvt->dbg_w_clip > tech_pvt->dbg_worst_window_clip)
        tech_pvt->dbg_worst_window_clip = tech_pvt->dbg_w_clip;

      tech_pvt->dbg_w_index++;
      tech_pvt->dbg_w_sumsq = tech_pvt->dbg_w_sumabsdiff = 0;
      tech_pvt->dbg_w_samples = tech_pvt->dbg_w_interior_n = 0;
      tech_pvt->dbg_w_frames = tech_pvt->dbg_w_clip = tech_pvt->dbg_w_peak = 0;
    }
  }

  static switch_status_t write_playback_frames_direct(private_t *tech_pvt, switch_core_session_t *session, int max_frames) {
    if (!tech_pvt || !session || !tech_pvt->playback_direct_mode || !tech_pvt->playback_codec_ready ||
        !tech_pvt->playback_buffer || !tech_pvt->playback_mutex || !tech_pvt->playback_chunk ||
        tech_pvt->playback_frame_bytes <= 0) {
      return SWITCH_STATUS_FALSE;
    }

    switch_channel_t *channel = switch_core_session_get_channel(session);
    if (!channel || !switch_channel_ready(channel)) {
      return SWITCH_STATUS_FALSE;
    }

    uint8_t *chunk = tech_pvt->playback_chunk;

    int frames_written = 0;
    while (max_frames <= 0 || frames_written < max_frames) {
      size_t bytes_read = 0;
      switch_mutex_lock(tech_pvt->playback_mutex);
      size_t available = switch_buffer_inuse(tech_pvt->playback_buffer);
      if (available >= (size_t)tech_pvt->playback_frame_bytes) {
        bytes_read = switch_buffer_read(tech_pvt->playback_buffer, chunk, (size_t)tech_pvt->playback_frame_bytes);
      }
      switch_mutex_unlock(tech_pvt->playback_mutex);

      if (bytes_read < (size_t)tech_pvt->playback_frame_bytes) {
        break;
      }

      dbg_measure_frame(tech_pvt, chunk, bytes_read);   /* [BUG-RE] TEMPORARY */

      /* [BUG-RE] TEMPORARY: is the channel also playing something of its own
       * right now? CF_BROADCAST is set while uuid_broadcast/playback owns the
       * write path, so a frame written here at the same moment is a second
       * source on one channel -- two voices mixed, which is what crackle and
       * garbled speech sound like. */
      tech_pvt->dbg_writes_total_checked++;
      if (switch_channel_test_flag(channel, CF_BROADCAST)) {
        tech_pvt->dbg_write_during_broadcast++;
      }

      switch_frame_t frame = { 0 };
      frame.codec = &tech_pvt->playback_codec;
      frame.data = chunk;
      frame.buflen = (uint32_t)bytes_read;
      frame.datalen = (uint32_t)bytes_read;
      frame.samples = (uint32_t)(bytes_read / sizeof(int16_t));
      frame.channels = 1;
      frame.rate = tech_pvt->playback_channel_rate;

      switch_time_t write_start = switch_micro_time_now();
      switch_status_t status = switch_core_session_write_frame(session, &frame, SWITCH_IO_FLAG_NONE, 0);
      switch_time_t write_elapsed_us = switch_micro_time_now() - write_start;
      tech_pvt->dbg_direct_write_us += (uint64_t) write_elapsed_us;
      tech_pvt->dbg_direct_frames++;
      if (status != SWITCH_STATUS_SUCCESS) {
        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
          "(%u) direct playback write failed: status=%d bytes=%zu\n",
          tech_pvt->id, status, bytes_read);
        return status;
      }
      frames_written++;

      /* Counted, not logged: a write taking longer than a couple of frame
       * intervals is expected under load and the per-event warning was pure
       * noise at 50 calls. The total lands in [MOD-BINARY-SUMMARY]. */
      if (write_elapsed_us > 30000) tech_pvt->dbg_direct_slow_writes++;

      /* [BUG-RE] TEMPORARY: interval between consecutive writes. This is the
       * delivery cadence the caller actually hears, and nothing else measures
       * it. Should be 20ms; a gap is silence, a bunch is catch-up. */
      if (tech_pvt->dbg_last_write_us) {
        uint64_t iv_us = (uint64_t)(write_start - tech_pvt->dbg_last_write_us);
        /* Mean over speech intervals only. Including the 10-30s between-turn
         * pauses inflated it to 31-55ms and made it unreadable. */
        if (iv_us <= 500000) {
          tech_pvt->dbg_write_iv_sum += iv_us;
          tech_pvt->dbg_write_iv_n++;
        }
        /* Only 30-500ms counts as a dropout. Anything longer is the pause
         * between turns while the caller speaks -- the first version counted
         * those too, which is why worst_gap_ms came back as 13-31 SECONDS and
         * made the figure useless. */
        if (iv_us > 30000 && iv_us <= 500000) {
          tech_pvt->dbg_write_gaps_30ms++;
          uint32_t gap_ms = (uint32_t)(iv_us / 1000);
          if (gap_ms > tech_pvt->dbg_write_worst_gap_ms) {
            tech_pvt->dbg_write_worst_gap_ms = gap_ms;
            /* offset in played audio, so it lines up with a recording */
            tech_pvt->dbg_write_worst_at_ms = tech_pvt->dbg_direct_frames * 20;
          }
        }
        else if (iv_us > 500000) tech_pvt->dbg_write_pauses++;
        else if (iv_us < 10000) tech_pvt->dbg_write_bunch_10ms++;
      }
      tech_pvt->dbg_last_write_us = (uint64_t) write_start;
    }

    return frames_written > 0 ? SWITCH_STATUS_SUCCESS : SWITCH_STATUS_FALSE;
  }

  /* ── Playback writer thread ───────────────────────────────────────────────
   * One per session. Drains playback_buffer into the channel, letting
   * switch_core_session_write_frame() block for its natural frame interval --
   * that blocking IS the pacing, it just must not happen on the lws service
   * thread, which is shared by every session and also has to flush uplink
   * audio. When the buffer is empty we simply do not write, so the channel's
   * own audio (file playback, silence) passes through untouched. */
  static void *SWITCH_THREAD_FUNC playback_writer_thread(switch_thread_t *thread, void *obj) {
    private_t *tech_pvt = (private_t *) obj;
    switch_core_session_t *session = tech_pvt->session;

    /* Real timer at the channel's frame interval. switch_core_timer_next()
     * returns immediately when the tick is already due, so a write that did
     * block does not get double-paced -- the cadence self-corrects. */
    switch_timer_t timer = { 0 };
    int have_timer = 0;
    int interval_ms = 20;
    if (nPlaybackPaced && tech_pvt->playback_channel_rate > 0 && tech_pvt->playback_frame_bytes > 0) {
      int samples_per_packet = tech_pvt->playback_frame_bytes / 2;
      interval_ms = samples_per_packet * 1000 / tech_pvt->playback_channel_rate;
      if (interval_ms <= 0) interval_ms = 20;
      if (switch_core_timer_init(&timer, "soft", interval_ms, samples_per_packet,
            switch_core_session_get_pool(session)) == SWITCH_STATUS_SUCCESS) {
        have_timer = 1;
      }
    }

    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
      "(%u) playback writer thread started (paced=%d interval=%dms prime=%d)\n",
      tech_pvt->id, have_timer, interval_ms, nPlaybackPrimeFrames);

    /* Hold a couple of frames before the first write so that arrival jitter
     * has something to eat into. Re-armed whenever the buffer runs dry, since
     * flush and end-of-turn both empty it. */
    int primed = 0;

    while (!tech_pvt->playback_thread_stop) {
      switch_channel_t *channel = switch_core_session_get_channel(session);
      /* Exit only once the channel is really gone. A transiently not-ready
       * channel is handled by write_playback_frames_direct returning FALSE,
       * which just makes us retry -- we must not quit for good on that. */
      if (!channel || !switch_channel_up(channel)) break;

      if (!tech_pvt->playback_active || !tech_pvt->playback_buffer) {
        primed = 0;
        switch_yield(10000);   /* idle: nothing to play, latency here is free */
        continue;
      }

      if (!primed) {
        switch_mutex_lock(tech_pvt->playback_mutex);
        size_t have = switch_buffer_inuse(tech_pvt->playback_buffer);
        switch_mutex_unlock(tech_pvt->playback_mutex);
        if (have < (size_t)(nPlaybackPrimeFrames * tech_pvt->playback_frame_bytes)) {
          switch_yield(2000);
          continue;
        }
        primed = 1;
        /* v1.0.9 defect: the soft timer's reference kept advancing during the
         * dry spell, so after re-priming, timer_next() was in arrears and
         * returned immediately for every backlogged tick -- the writes came
         * out in a burst, reproducing exactly the bunching being fixed.
         * Resync so pacing restarts from now. */
        if (have_timer) switch_core_timer_sync(&timer);
      }

      if (have_timer) switch_core_timer_next(&timer);

      /* No separate "is there data" probe: write_playback_frames_direct already
       * checks under the lock and reports FALSE when it wrote nothing, so
       * peeking first only doubled the per-frame locking. */
      if (write_playback_frames_direct(tech_pvt, session, 1) != SWITCH_STATUS_SUCCESS) {
        primed = 0;            /* ran dry: re-prime before speaking again */
        if (!have_timer) switch_yield(2000);
      }
    }

    if (have_timer) switch_core_timer_destroy(&timer);

    /* Nothing logged on exit: the same figures are in [MOD-BINARY-SUMMARY],
     * which fires once per session in fork_session_cleanup. */
    return NULL;
  }

  static void start_playback_writer(private_t *tech_pvt, switch_core_session_t *session) {
    switch_threadattr_t *thd_attr = NULL;
    switch_memory_pool_t *pool = switch_core_session_get_pool(session);

    switch_threadattr_create(&thd_attr, pool);
    switch_threadattr_detach_set(thd_attr, 0);   /* joinable: cleanup must join before freeing the buffer */
    switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE);
    switch_thread_create(&tech_pvt->playback_thread, thd_attr, playback_writer_thread, tech_pvt, pool);
  }

  static void stop_playback_writer(private_t *tech_pvt) {
    if (!tech_pvt->playback_thread) return;
    switch_status_t st = SWITCH_STATUS_SUCCESS;
    tech_pvt->playback_thread_stop = 1;
    switch_thread_join(&st, tech_pvt->playback_thread);
    tech_pvt->playback_thread = NULL;
  }

  void processIncomingMessage(private_t* tech_pvt, switch_core_session_t* session, const char* message) {
    std::string msg = message;
    std::string type;
    cJSON* json = parse_json(session, msg, type) ;
    if (json) {
      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "(%u) processIncomingMessage - received %s message\n", tech_pvt->id, type.c_str());
      cJSON* jsonData = cJSON_GetObjectItem(json, "data");
      if (0 == type.compare("playAudio")) {
        if (jsonData) {
          // dont send actual audio bytes in event message
          cJSON* jsonFile = NULL;
          cJSON* jsonAudio = cJSON_DetachItemFromObject(jsonData, "audioContent");
          int validAudio = (jsonAudio && NULL != jsonAudio->valuestring);

          const char* szAudioContentType = cJSON_GetObjectCstr(jsonData, "audioContentType");
          char fileType[6];
          int sampleRate = 16000;
          if (!szAudioContentType) {
            /* Was an unguarded strcmp: a playAudio message without this field
             * crashed the service thread, taking every other call with it. */
            validAudio = 0;
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
              "(%u) processIncomingMessage - playAudio missing audioContentType\n", tech_pvt->id);
            strcpy(fileType, ".r16");
          }
          else if (0 == strcmp(szAudioContentType, "raw")) {
            cJSON* jsonSR = cJSON_GetObjectItem(jsonData, "sampleRate");
            sampleRate = jsonSR && jsonSR->valueint ? jsonSR->valueint : 0;

            switch(sampleRate) {
              case 8000:
                strcpy(fileType, ".r8");
                break;
              case 16000:
                strcpy(fileType, ".r16");
                break;
              case 24000:
                strcpy(fileType, ".r24");
                break;
              case 32000:
                strcpy(fileType, ".r32");
                break;
              case 48000:
                strcpy(fileType, ".r48");
                break;
              case 64000:
                strcpy(fileType, ".r64");
                break;
              default:
                strcpy(fileType, ".r16");
                break;
            }
          }
          else if (0 == strcmp(szAudioContentType, "wave") || 0 == strcmp(szAudioContentType, "wav")) {
            strcpy(fileType, ".wav");
          }
          else {
            validAudio = 0;
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "(%u) processIncomingMessage - unsupported audioContentType: %s\n", tech_pvt->id, szAudioContentType);
          }

          if (validAudio) {
            char szFilePath[256];

            std::string rawAudio = drachtio::base64_decode(jsonAudio->valuestring);
            switch_snprintf(szFilePath, 256, "%s%s%s_%d.tmp%s", SWITCH_GLOBAL_dirs.temp_dir, 
              SWITCH_PATH_SEPARATOR, tech_pvt->sessionId, playCount++, fileType);
            std::ofstream f(szFilePath, std::ofstream::binary);
            f << rawAudio;
            f.close();

            // add the file to the list of files played for this session, we'll delete when session closes
            struct playout* playout = (struct playout *) malloc(sizeof(struct playout));
            playout->file = (char *) malloc(strlen(szFilePath) + 1);
            strcpy(playout->file, szFilePath);
            playout->next = tech_pvt->playout;
            tech_pvt->playout = playout;

            jsonFile = cJSON_CreateString(szFilePath);
            cJSON_AddItemToObject(jsonData, "file", jsonFile);
          }

          char* jsonString = cJSON_PrintUnformatted(jsonData);
          tech_pvt->responseHandler(session, EVENT_PLAY_AUDIO, jsonString);
          free(jsonString);
          if (jsonAudio) cJSON_Delete(jsonAudio);
        }
        else {
          switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "(%u) processIncomingMessage - missing data payload in playAudio request\n", tech_pvt->id); 
        }
      }
      else if (0 == type.compare("killAudio")) {
        tech_pvt->responseHandler(session, EVENT_KILL_AUDIO, NULL);

        // kill any current playback on the channel
        switch_channel_t *channel = switch_core_session_get_channel(session);
        switch_channel_set_flag_value(channel, CF_BREAK, 2);
      }
      else if (0 == type.compare("transcription")) {
        char* jsonString = cJSON_PrintUnformatted(jsonData);
        tech_pvt->responseHandler(session, EVENT_TRANSCRIPTION, jsonString);
        free(jsonString);        
      }
      else if (0 == type.compare("transfer")) {
        char* jsonString = cJSON_PrintUnformatted(jsonData);
        tech_pvt->responseHandler(session, EVENT_TRANSFER, jsonString);
        free(jsonString);                
      }
      else if (0 == type.compare("disconnect")) {
        char* jsonString = cJSON_PrintUnformatted(jsonData);
        tech_pvt->responseHandler(session, EVENT_DISCONNECT, jsonString);
        free(jsonString);        
      }
      else if (0 == type.compare("error")) {
        char* jsonString = cJSON_PrintUnformatted(jsonData);
        tech_pvt->responseHandler(session, EVENT_ERROR, jsonString);
        free(jsonString);        
      }
      else if (0 == type.compare("json")) {
        char* jsonString = cJSON_PrintUnformatted(json);
        tech_pvt->responseHandler(session, EVENT_JSON, jsonString);
        free(jsonString);
      }
      /* ── Realtime binary playback control messages ─────────────────────── */
      else if (0 == type.compare("flush")) {
        if (tech_pvt->playback_buffer) {
          switch_mutex_lock(tech_pvt->playback_mutex);
          switch_buffer_zero(tech_pvt->playback_buffer);
          switch_mutex_unlock(tech_pvt->playback_mutex);
        }
        /* DEBUG, not INFO: these three fire several times per utterance, so at
         * 50+ concurrent calls they are tens of lines a second of state that
         * the [MOD-BINARY] counters already summarise. Turn DEBUG on when
         * tracing a single call. */
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
          "(%u) flush: cleared playback buffer\n", tech_pvt->id);
      }
      else if (0 == type.compare("enableBinaryPlayback")) {
        /* Optional data.sampleRate field overrides the app-paced default. */
        if (jsonData) {
          cJSON* jsonSR = cJSON_GetObjectItem(jsonData, "sampleRate");
          if (jsonSR && jsonSR->valueint > 0)
            tech_pvt->playback_input_rate = jsonSR->valueint;
        }
        /* Lazy-init the resampler when input rate differs from channel rate */
        if (tech_pvt->playback_input_rate != tech_pvt->playback_channel_rate
            && !tech_pvt->playback_resampler) {
          int err = 0;
          tech_pvt->playback_resampler = speex_resampler_init(
            1,                               /* mono */
            (spx_uint32_t)tech_pvt->playback_input_rate,
            (spx_uint32_t)tech_pvt->playback_channel_rate,
            SWITCH_RESAMPLE_QUALITY, &err);
          if (err != RESAMPLER_ERR_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR,
              "(%u) enableBinaryPlayback: speex_resampler_init failed err=%d\n", tech_pvt->id, err);
            tech_pvt->playback_resampler = nullptr;
            cJSON_Delete(json);
            return;
          }
        }
        tech_pvt->playback_active = 1;
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
          "(%u) enableBinaryPlayback: active=1 input_rate=%d channel_rate=%d\n",
          tech_pvt->id, tech_pvt->playback_input_rate, tech_pvt->playback_channel_rate);
      }
      else if (0 == type.compare("disableBinaryPlayback")) {
        tech_pvt->playback_active = 0;
        if (tech_pvt->playback_buffer) {
          switch_mutex_lock(tech_pvt->playback_mutex);
          switch_buffer_zero(tech_pvt->playback_buffer);
          switch_mutex_unlock(tech_pvt->playback_mutex);
        }
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,
          "(%u) disableBinaryPlayback: active=0\n", tech_pvt->id);
      }
      /* ────────────────────────────────────────────────────────────────────── */
      else {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "(%u) processIncomingMessage - unsupported msg type %s\n", tech_pvt->id, type.c_str());  
      }
      cJSON_Delete(json);
    }
    else {
      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "(%u) processIncomingMessage - could not parse message: %s\n", tech_pvt->id, message);
    }
  }

  static void eventCallback(const char* sessionId, const char* bugname, AudioPipe::NotifyEvent_t event, const char* message) {
    switch_core_session_t* session = switch_core_session_locate(sessionId);
    if (session) {
      switch_channel_t *channel = switch_core_session_get_channel(session);
      switch_media_bug_t *bug = (switch_media_bug_t*) switch_channel_get_private(channel, bugname);
      if (bug) {
        private_t* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
        if (tech_pvt) {
          switch (event) {
            case AudioPipe::CONNECT_SUCCESS:
              switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO, "connection successful\n");
              tech_pvt->responseHandler(session, EVENT_CONNECT_SUCCESS, NULL);
              if (strlen(tech_pvt->initialMetadata) > 0) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "sending initial metadata %s\n", tech_pvt->initialMetadata);
                AudioPipe *pAudioPipe = static_cast<AudioPipe *>(tech_pvt->pAudioPipe);
                pAudioPipe->bufferForSending(tech_pvt->initialMetadata);
              }
            break;
            case AudioPipe::CONNECT_FAIL:
            {
              // first thing: we can no longer access the AudioPipe
              std::stringstream json;
              json << "{\"reason\":\"" << message << "\"}";
              tech_pvt->pAudioPipe = nullptr;
              tech_pvt->responseHandler(session, EVENT_CONNECT_FAIL, (char *) json.str().c_str());
              switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_NOTICE, "connection failed: %s\n", message);
            }
            break;
            case AudioPipe::CONNECTION_DROPPED:
              // first thing: we can no longer access the AudioPipe
              tech_pvt->pAudioPipe = nullptr;
              tech_pvt->responseHandler(session, EVENT_DISCONNECT, NULL);
              switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_NOTICE, "connection dropped from far end\n");
            break;
            case AudioPipe::CONNECTION_CLOSED_GRACEFULLY:
              // first thing: we can no longer access the AudioPipe
              tech_pvt->pAudioPipe = nullptr;
              switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "connection closed gracefully\n");
            break;
            case AudioPipe::MESSAGE:
              processIncomingMessage(tech_pvt, session, message);
            break;
            case AudioPipe::BINARY_AUDIO:
            {
              /* message == nullptr; payload is in AudioPipe's scratch pointers */
              AudioPipe *pAudioPipe = static_cast<AudioPipe *>(tech_pvt->pAudioPipe);
              if (pAudioPipe && pAudioPipe->getBinaryPayload() && pAudioPipe->getBinaryPayloadLen() > 0) {
                fork_session_handle_binary(tech_pvt,
                  session,
                  pAudioPipe->getBinaryPayload(),
                  pAudioPipe->getBinaryPayloadLen());
              }
            }
            break;
          }
        }
      }
      switch_core_session_rwunlock(session);
    }
  }
  switch_status_t fork_data_init(private_t *tech_pvt, switch_core_session_t *session, char * host, 
    unsigned int port, char* path, int sslFlags, int sampling, int desiredSampling, int channels, 
    char *bugname, char* metadata, responseHandler_t responseHandler) {

    const char* username = nullptr;
    const char* password = nullptr;
    int err;
    switch_codec_implementation_t read_impl;
    switch_codec_implementation_t write_impl;
    switch_channel_t *channel = switch_core_session_get_channel(session);

    switch_core_session_get_read_impl(session, &read_impl);
    switch_core_session_get_write_impl(session, &write_impl);
  
    if (username = switch_channel_get_variable(channel, "MOD_AUDIO_BASIC_AUTH_USERNAME")) {
      password = switch_channel_get_variable(channel, "MOD_AUDIO_BASIC_AUTH_PASSWORD");
    }

    memset(tech_pvt, 0, sizeof(private_t));
  
    strncpy(tech_pvt->sessionId, switch_core_session_get_uuid(session), MAX_SESSION_ID);
    strncpy(tech_pvt->host, host, MAX_WS_URL_LEN);
    tech_pvt->port = port;
    strncpy(tech_pvt->path, path, MAX_PATH_LEN);    
    tech_pvt->sampling = desiredSampling;
    tech_pvt->responseHandler = responseHandler;
    tech_pvt->playout = NULL;
    tech_pvt->channels = channels;
    tech_pvt->id = ++idxCallCount;
    tech_pvt->buffer_overrun_notified = 0;
    tech_pvt->audio_paused = 0;
    tech_pvt->graceful_shutdown = 0;
    strncpy(tech_pvt->bugname, bugname, MAX_BUG_LEN);
    if (metadata) strncpy(tech_pvt->initialMetadata, metadata, MAX_METADATA_LEN);
    
    /* ── Init binary playback state ───────────────────────────────────────── */
    tech_pvt->session               = session;   /* used by the playback writer thread */
    tech_pvt->playback_thread       = nullptr;
    tech_pvt->playback_thread_stop  = 0;
    tech_pvt->playback_active       = 0;
    tech_pvt->playback_input_rate   = 8000;    /* backend sends channel-rate PCM in app-paced mode */
    tech_pvt->playback_channel_rate = (int) (write_impl.actual_samples_per_second ?
      write_impl.actual_samples_per_second : read_impl.actual_samples_per_second);
    tech_pvt->playback_resampler    = nullptr;
    tech_pvt->playback_frame_bytes  = (int) (write_impl.decoded_bytes_per_packet ?
      write_impl.decoded_bytes_per_packet : read_impl.decoded_bytes_per_packet);
    tech_pvt->playback_chunk        = tech_pvt->playback_frame_bytes > 0 ?
      (uint8_t *) switch_core_session_alloc(session, (size_t) tech_pvt->playback_frame_bytes) : nullptr;
    tech_pvt->playback_direct_mode  = 0;
    tech_pvt->playback_codec_ready  = 0;
    switch_mutex_init(&tech_pvt->playback_mutex, SWITCH_MUTEX_NESTED,
      switch_core_session_get_pool(session));
    /* Tiny jitter buffer only. Backend/app owns realtime pacing. */
    switch_buffer_create_dynamic(&tech_pvt->playback_buffer, 4096, 32768, 0);

    if (tech_pvt->playback_channel_rate > 0 && tech_pvt->playback_frame_bytes > 0 &&
        switch_core_codec_init(&tech_pvt->playback_codec,
          "L16",
          NULL,
          NULL,
          tech_pvt->playback_channel_rate,
          (write_impl.microseconds_per_packet ? write_impl.microseconds_per_packet : read_impl.microseconds_per_packet) / 1000,
          1,
          SWITCH_CODEC_FLAG_ENCODE | SWITCH_CODEC_FLAG_DECODE,
          NULL,
          switch_core_session_get_pool(session)) == SWITCH_STATUS_SUCCESS) {
      tech_pvt->playback_codec_ready = 1;
      tech_pvt->playback_direct_mode = 1;  /* playback path armed; the writer thread owns it */
      start_playback_writer(tech_pvt, session);
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
        "(%u) playback direct mode armed: channel_rate=%d frame_bytes=%d\n",
        tech_pvt->id, tech_pvt->playback_channel_rate, tech_pvt->playback_frame_bytes);
    } else {
      /* There is no fallback path any more: WRITE_REPLACE was removed because it
       * never ran, so failing to init the L16 codec means no binary playback. */
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
        "(%u) binary playback UNAVAILABLE: L16 codec init failed (channel_rate=%d frame_bytes=%d)\n",
        tech_pvt->id, tech_pvt->playback_channel_rate, tech_pvt->playback_frame_bytes);
    }
    /* ────────────────────────────────────────────────────────────────────── */
    
    size_t buflen = LWS_PRE + (FRAME_SIZE_8000 * desiredSampling / 8000 * channels * 1000 / RTP_PACKETIZATION_PERIOD * nAudioBufferSecs);

    AudioPipe* ap = new AudioPipe(tech_pvt->sessionId, host, port, path, sslFlags, 
      buflen, read_impl.decoded_bytes_per_packet, username, password, bugname, eventCallback);
    if (!ap) {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "Error allocating AudioPipe\n");
      return SWITCH_STATUS_FALSE;
    }

    tech_pvt->pAudioPipe = static_cast<void *>(ap);

    switch_mutex_init(&tech_pvt->mutex, SWITCH_MUTEX_NESTED, switch_core_session_get_pool(session));

    if (desiredSampling != sampling) {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%u) resampling from %u to %u\n", tech_pvt->id, sampling, desiredSampling);
      tech_pvt->resampler = speex_resampler_init(channels, sampling, desiredSampling, SWITCH_RESAMPLE_QUALITY, &err);
      if (0 != err) {
        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "Error initializing resampler: %s.\n", speex_resampler_strerror(err));
        return SWITCH_STATUS_FALSE;
      }
    }
    else {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%u) no resampling needed for this call\n", tech_pvt->id);
    }

    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%u) fork_data_init\n", tech_pvt->id);

    return SWITCH_STATUS_SUCCESS;
  }

  void destroy_tech_pvt(private_t* tech_pvt) {
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "%s (%u) destroy_tech_pvt\n", tech_pvt->sessionId, tech_pvt->id);
    if (tech_pvt->resampler) {
      speex_resampler_destroy(tech_pvt->resampler);
      tech_pvt->resampler = nullptr;
    }
    if (tech_pvt->mutex) {
      switch_mutex_destroy(tech_pvt->mutex);
      tech_pvt->mutex = nullptr;
    }
    /* ── Binary playback cleanup ───────────────────────────────────────────
     * Join the writer thread FIRST: it touches playback_buffer, playback_mutex
     * and playback_codec, all of which are torn down just below. */
    tech_pvt->playback_active = 0;
    stop_playback_writer(tech_pvt);
    if (tech_pvt->playback_resampler) {
      speex_resampler_destroy(tech_pvt->playback_resampler);
      tech_pvt->playback_resampler = nullptr;
    }
    if (tech_pvt->playback_codec_ready) {
      switch_core_codec_destroy(&tech_pvt->playback_codec);
      tech_pvt->playback_codec_ready = 0;
    }
    if (tech_pvt->playback_buffer) {
      switch_buffer_destroy(&tech_pvt->playback_buffer);
      tech_pvt->playback_buffer = nullptr;
    }
    if (tech_pvt->playback_mutex) {
      switch_mutex_destroy(tech_pvt->playback_mutex);
      tech_pvt->playback_mutex = nullptr;
    }
    /* ────────────────────────────────────────────────────────────────────── */
  }

  void lws_logger(int level, const char *line) {
    switch_log_level_t llevel = SWITCH_LOG_DEBUG;

    switch (level) {
      case LLL_ERR: llevel = SWITCH_LOG_ERROR; break;
      case LLL_WARN: llevel = SWITCH_LOG_WARNING; break;
      case LLL_NOTICE: llevel = SWITCH_LOG_NOTICE; break;
      case LLL_INFO: llevel = SWITCH_LOG_INFO; break;
      break;
    }
	  switch_log_printf(SWITCH_CHANNEL_LOG, llevel, "%s\n", line);
  }
}

extern "C" {
  int parse_ws_uri(switch_channel_t *channel, const char* szServerUri, char* host, char *path, unsigned int* pPort, int* pSslFlags) {
    int i = 0, offset;
    char server[MAX_WS_URL_LEN + MAX_PATH_LEN];
    char *saveptr;
    int flags = LCCSCF_USE_SSL;
    
    if (switch_true(switch_channel_get_variable(channel, "MOD_AUDIO_FORK_ALLOW_SELFSIGNED"))) {
      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "parse_ws_uri - allowing self-signed certs\n");
      flags |= LCCSCF_ALLOW_SELFSIGNED;
    }
    if (switch_true(switch_channel_get_variable(channel, "MOD_AUDIO_FORK_SKIP_SERVER_CERT_HOSTNAME_CHECK"))) {
      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "parse_ws_uri - skipping hostname check\n");
      flags |= LCCSCF_SKIP_SERVER_CERT_HOSTNAME_CHECK;
    }
    if (switch_true(switch_channel_get_variable(channel, "MOD_AUDIO_FORK_ALLOW_EXPIRED"))) {
      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "parse_ws_uri - allowing expired certs\n");
      flags |= LCCSCF_ALLOW_EXPIRED;
    }

    // get the scheme
    strncpy(server, szServerUri, MAX_WS_URL_LEN + MAX_PATH_LEN);
    if (0 == strncmp(server, "https://", 8) || 0 == strncmp(server, "HTTPS://", 8)) {
      *pSslFlags = flags;
      offset = 8;
      *pPort = 443;
    }
    else if (0 == strncmp(server, "wss://", 6) || 0 == strncmp(server, "WSS://", 6)) {
      *pSslFlags = flags;
      offset = 6;
      *pPort = 443;
    }
    else if (0 == strncmp(server, "http://", 7) || 0 == strncmp(server, "HTTP://", 7)) {
      offset = 7;
      *pSslFlags = 0;
      *pPort = 80;
    }
    else if (0 == strncmp(server, "ws://", 5) || 0 == strncmp(server, "WS://", 5)) {
      offset = 5;
      *pSslFlags = 0;
      *pPort = 80;
    }
    else {
      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "parse_ws_uri - error parsing uri %s: invalid scheme\n", szServerUri);;
      return 0;
    }

    std::string strHost(server + offset);
    std::regex re("^(.+?):?(\\d+)?(/.*)?$");
    std::smatch matches;
    if(std::regex_search(strHost, matches, re)) {
      /*
      for (int i = 0; i < matches.length(); i++) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "parse_ws_uri - %d: %s\n", i, matches[i].str().c_str());
      }
      */
      strncpy(host, matches[1].str().c_str(), MAX_WS_URL_LEN);
      if (matches[2].str().length() > 0) {
        *pPort = atoi(matches[2].str().c_str());
      }
      if (matches[3].str().length() > 0) {
        strncpy(path, matches[3].str().c_str(), MAX_PATH_LEN);
      }
      else {
        strcpy(path, "/");
      }
    } else {
      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "parse_ws_uri - invalid format %s\n", strHost.c_str());
      return 0;
    }
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "parse_ws_uri - host %s, path %s\n", host, path);

    return 1;
  }

  switch_status_t fork_init() {
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_audio_fork: version:                   %s\n", MOD_AUDIO_FORK_VERSION);
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_audio_fork: audio buffer (in secs):    %d secs\n", nAudioBufferSecs);
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_audio_fork: sub-protocol:              %s\n", mySubProtocolName);
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_audio_fork: lws service threads:       %d\n", nServiceThreads);
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_audio_fork: playback jitter frames:    %d (%d ms)\n",
      nPlaybackJitterFrames, nPlaybackJitterFrames * RTP_PACKETIZATION_PERIOD);
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_audio_fork: playback paced / prime:     %d / %d frames\n",
      nPlaybackPaced, nPlaybackPrimeFrames);
 
    int logs = LLL_ERR | LLL_WARN | LLL_NOTICE ;
     //LLL_INFO | LLL_PARSER | LLL_HEADER | LLL_EXT | LLL_CLIENT  | LLL_LATENCY | LLL_DEBUG ;
    AudioPipe::initialize(mySubProtocolName, nServiceThreads, logs, lws_logger);
   return SWITCH_STATUS_SUCCESS;
  }

  switch_status_t fork_cleanup() {
    bool cleanup = false;
    cleanup = AudioPipe::deinitialize();
    if (cleanup == true) {
        return SWITCH_STATUS_SUCCESS;
    }
    return SWITCH_STATUS_FALSE;
  }

  switch_status_t fork_session_init(switch_core_session_t *session, 
              responseHandler_t responseHandler,
              uint32_t samples_per_second, 
              char *host,
              unsigned int port,
              char *path,
              int sampling,
              int sslFlags,
              int channels,
              char *bugname,
              char* metadata, 
              void **ppUserData)
  {    	
    int err;

    // allocate per-session data structure
    private_t* tech_pvt = (private_t *) switch_core_session_alloc(session, sizeof(private_t));
    if (!tech_pvt) {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "error allocating memory!\n");
      return SWITCH_STATUS_FALSE;
    }
    if (SWITCH_STATUS_SUCCESS != fork_data_init(tech_pvt, session, host, port, path, sslFlags, samples_per_second, sampling, channels, 
      bugname, metadata, responseHandler)) {
      destroy_tech_pvt(tech_pvt);
      return SWITCH_STATUS_FALSE;
    }

    *ppUserData = tech_pvt;
    return SWITCH_STATUS_SUCCESS;
  }

   switch_status_t fork_session_connect(void **ppUserData) {
    private_t *tech_pvt = static_cast<private_t *>(*ppUserData);
    AudioPipe *pAudioPipe = static_cast<AudioPipe*>(tech_pvt->pAudioPipe);
    pAudioPipe->connect();
    return SWITCH_STATUS_SUCCESS;
  }

  switch_status_t fork_session_cleanup(switch_core_session_t *session, char *bugname, char* text, int channelIsClosing) {
    switch_channel_t *channel = switch_core_session_get_channel(session);
    switch_media_bug_t *bug = (switch_media_bug_t*) switch_channel_get_private(channel, bugname);
    if (!bug) {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "fork_session_cleanup: no bug %s - websocket conection already closed\n", bugname);
      return SWITCH_STATUS_FALSE;
    }
    private_t* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
    uint32_t id = tech_pvt->id;

    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%u) fork_session_cleanup\n", id);

    if (!tech_pvt) return SWITCH_STATUS_FALSE;
    AudioPipe *pAudioPipe = static_cast<AudioPipe *>(tech_pvt->pAudioPipe);
      
    switch_mutex_lock(tech_pvt->mutex);

    // get the bug again, now that we are under lock
    {
      switch_media_bug_t *bug = (switch_media_bug_t*) switch_channel_get_private(channel, bugname);
      if (bug) {
        switch_channel_set_private(channel, bugname, NULL);
        if (!channelIsClosing) {
          switch_core_media_bug_remove(session, &bug);
        }
      }
    }

    // delete any temp files
    struct playout* playout = tech_pvt->playout;
    while (playout) {
      std::remove(playout->file);
      free(playout->file);
      struct playout *tmp = playout;
      playout = playout->next;
      free(tmp);
    }

    if (pAudioPipe && text) pAudioPipe->bufferForSending(text);

    /* Snapshot uplink stats before close(): once closed, the lws thread deletes
     * the AudioPipe on LWS_CALLBACK_CLIENT_CLOSED and the pointer goes stale. */
    uint32_t uplinkFlushes = pAudioPipe ? pAudioPipe->getFlushCount() : 0;
    uint32_t uplinkQueues  = pAudioPipe ? pAudioPipe->getQueueCount() : 0;
    uint64_t uplinkKb      = pAudioPipe ? pAudioPipe->getFlushedBytes() / 1024 : 0;

    if (pAudioPipe) pAudioPipe->close();

    if (tech_pvt->dbg_binary_frames_rx > 0) {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO,
        "(%u) [MOD-BINARY-SUMMARY] rx=%u bad_frame_size=%u direct_slow_writes=%u input_rate=%d channel_rate=%d frame_bytes=%d "
        "playback_hwm=%u playback_overflow_frames=%u "
        "direct_frames=%u direct_write_ms=%llu avg_write_us=%llu uplink_flushes=%u uplink_queues=%u uplink_kb=%llu\n",
        id,
        tech_pvt->dbg_binary_frames_rx,
        tech_pvt->dbg_binary_bad_frame_size,
        tech_pvt->dbg_direct_slow_writes,
        tech_pvt->playback_input_rate,
        tech_pvt->playback_channel_rate,
        tech_pvt->playback_frame_bytes,
        tech_pvt->dbg_playback_hwm_bytes,
        tech_pvt->dbg_playback_overflow_frames,
        tech_pvt->dbg_direct_frames,
        (unsigned long long)(tech_pvt->dbg_direct_write_us / 1000),
        (unsigned long long)(tech_pvt->dbg_direct_frames ?
          tech_pvt->dbg_direct_write_us / tech_pvt->dbg_direct_frames : 0),
        uplinkFlushes, uplinkQueues, (unsigned long long) uplinkKb);

      /* [BUG-RE] TEMPORARY: what the waveform itself looked like.
       * mad_x100_over_rms is the number to read: clean telephony speech lands
       * well under 100; at or above 100 the signal carries high-frequency
       * content 8 kHz cannot represent, which is what crackle is. */
      if (tech_pvt->dbg_a_samples > 0) {
        double rms = sqrt((double) tech_pvt->dbg_a_sumsq / (double) tech_pvt->dbg_a_samples);
        double mad = tech_pvt->dbg_a_interior_n
          ? (double) tech_pvt->dbg_a_sumabsdiff / (double) tech_pvt->dbg_a_interior_n : 0.0;
        double bstep = tech_pvt->dbg_a_bstep_n
          ? (double) tech_pvt->dbg_a_bstep_sum / (double) tech_pvt->dbg_a_bstep_n : 0.0;
        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO,
          "(%u) [MOD-AUDIO-STATS-C] samples=%llu rms=%d peak=%u clip=%u zero_frames=%u "
          "mad=%d mad_x100_over_rms=%d bstep_mean=%d bstep_max=%u bstep_n=%u "
          "writes=%u during_broadcast=%u "
          "worst_1s_ratio=%d worst_at_ms=%u worst_1s_clip=%u windows_over40=%u/%u "
          "write_iv_mean_ms=%d gaps30_500=%u bunch10=%u pauses=%u worst_gap_ms=%u worst_gap_at_ms=%u "
          "rx_iv_mean_ms=%d rx_gaps30_500=%u rx_bunch10=%u rx_pauses=%u rx_worst_gap_ms=%u rx_worst_at_ms=%u\n",
          id,
          (unsigned long long) tech_pvt->dbg_a_samples,
          (int) rms, tech_pvt->dbg_a_peak, tech_pvt->dbg_a_clip, tech_pvt->dbg_a_zero_frames,
          (int) mad, rms > 0.0 ? (int)(mad * 100.0 / rms) : -1,
          (int) bstep, tech_pvt->dbg_a_bstep_max, tech_pvt->dbg_a_bstep_n,
          tech_pvt->dbg_writes_total_checked, tech_pvt->dbg_write_during_broadcast,
          tech_pvt->dbg_worst_ratio, tech_pvt->dbg_worst_at_ms,
          tech_pvt->dbg_worst_window_clip,
          tech_pvt->dbg_windows_over, tech_pvt->dbg_windows_total,
          tech_pvt->dbg_write_iv_n
            ? (int)(tech_pvt->dbg_write_iv_sum / tech_pvt->dbg_write_iv_n / 1000) : -1,
          tech_pvt->dbg_write_gaps_30ms, tech_pvt->dbg_write_bunch_10ms,
          tech_pvt->dbg_write_pauses,
          tech_pvt->dbg_write_worst_gap_ms, tech_pvt->dbg_write_worst_at_ms,
          tech_pvt->dbg_rx_iv_n
            ? (int)(tech_pvt->dbg_rx_iv_sum / tech_pvt->dbg_rx_iv_n / 1000) : -1,
          tech_pvt->dbg_rx_gaps_30ms, tech_pvt->dbg_rx_bunch_10ms,
          tech_pvt->dbg_rx_pauses,
          tech_pvt->dbg_rx_worst_gap_ms, tech_pvt->dbg_rx_worst_at_ms);
      }
    }

    /* Nothing can reach this tech_pvt through the channel any more (the bug was
     * detached above under this lock), so release before tearing down --
     * destroy_tech_pvt destroys this very mutex, and destroying a locked mutex
     * is undefined. It also joins the writer thread, which we would rather not
     * wait for while holding a lock. */
    tech_pvt->pAudioPipe = nullptr;
    switch_mutex_unlock(tech_pvt->mutex);

    destroy_tech_pvt(tech_pvt);
    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO, "(%u) fork_session_cleanup: connection closed\n", id);
    return SWITCH_STATUS_SUCCESS;
  }

  switch_status_t fork_session_send_text(switch_core_session_t *session, char *bugname, char* text) {
    switch_channel_t *channel = switch_core_session_get_channel(session);
    switch_media_bug_t *bug = (switch_media_bug_t*) switch_channel_get_private(channel, bugname);
    if (!bug) {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "fork_session_send_text failed because no bug\n");
      return SWITCH_STATUS_FALSE;
    }
    private_t* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
  
    if (!tech_pvt) return SWITCH_STATUS_FALSE;
    AudioPipe *pAudioPipe = static_cast<AudioPipe *>(tech_pvt->pAudioPipe);
    if (pAudioPipe && text) pAudioPipe->bufferForSending(text);

    return SWITCH_STATUS_SUCCESS;
  }

  switch_status_t fork_session_pauseresume(switch_core_session_t *session, char *bugname, int pause) {
    switch_channel_t *channel = switch_core_session_get_channel(session);
    switch_media_bug_t *bug = (switch_media_bug_t*) switch_channel_get_private(channel, bugname);
    if (!bug) {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "fork_session_pauseresume failed because no bug\n");
      return SWITCH_STATUS_FALSE;
    }
    private_t* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
  
    if (!tech_pvt) return SWITCH_STATUS_FALSE;

    switch_core_media_bug_flush(bug);
    tech_pvt->audio_paused = pause;
    return SWITCH_STATUS_SUCCESS;
  }

  switch_status_t fork_session_graceful_shutdown(switch_core_session_t *session, char *bugname) {
    switch_channel_t *channel = switch_core_session_get_channel(session);
    switch_media_bug_t *bug = (switch_media_bug_t*) switch_channel_get_private(channel, bugname);
    if (!bug) {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "fork_session_graceful_shutdown failed because no bug\n");
      return SWITCH_STATUS_FALSE;
    }
    private_t* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
  
    if (!tech_pvt) return SWITCH_STATUS_FALSE;

    tech_pvt->graceful_shutdown = 1;

    AudioPipe *pAudioPipe = static_cast<AudioPipe *>(tech_pvt->pAudioPipe);
    if (pAudioPipe) pAudioPipe->do_graceful_shutdown();

    return SWITCH_STATUS_SUCCESS;
  }

  switch_bool_t fork_frame(switch_core_session_t *session, switch_media_bug_t *bug) {
    private_t* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
    size_t inuse = 0;
    bool dirty = false;
    char *p = (char *) "{\"msg\": \"buffer overrun\"}";

    if (!tech_pvt || tech_pvt->audio_paused || tech_pvt->graceful_shutdown) return SWITCH_TRUE;
    
    if (switch_mutex_trylock(tech_pvt->mutex) == SWITCH_STATUS_SUCCESS) {
      if (!tech_pvt->pAudioPipe) {
        switch_mutex_unlock(tech_pvt->mutex);
        return SWITCH_TRUE;
      }
      AudioPipe *pAudioPipe = static_cast<AudioPipe *>(tech_pvt->pAudioPipe);
      if (pAudioPipe->getLwsState() != AudioPipe::LWS_CLIENT_CONNECTED) {
        switch_mutex_unlock(tech_pvt->mutex);
        return SWITCH_TRUE;
      }

      pAudioPipe->lockAudioBuffer();
      size_t available = pAudioPipe->binarySpaceAvailable();
      if (NULL == tech_pvt->resampler) {
        switch_frame_t frame = { 0 };
        frame.data = pAudioPipe->binaryWritePtr();
        frame.buflen = available;
        while (true) {

          // check if buffer would be overwritten; dump packets if so
          if (available < pAudioPipe->binaryMinSpace()) {
            if (!tech_pvt->buffer_overrun_notified) {
              tech_pvt->buffer_overrun_notified = 1;
              tech_pvt->responseHandler(session, EVENT_BUFFER_OVERRUN, NULL);
            }
            /* Report *why* we ran out of room: the buffer only overflows if the
             * lws service thread stopped flushing it. since_flush_ms growing to
             * the full buffer depth (nAudioBufferSecs) means zero drain. */
            uint64_t nowus = AudioPipe::nowUs();
            uint64_t lastFlush = pAudioPipe->getLastFlushUs();
            uint64_t lastQueue = pAudioPipe->getLastQueueUs();
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
              "(%u) dropping packets! [MOD-UPLINK-STALL] since_flush_ms=%lld since_queue_ms=%lld "
              "flushes=%u queues=%u flushed_kb=%llu offset=%zu buf_max=%zu lws_state=%d\n",
              tech_pvt->id,
              lastFlush ? (long long)((nowus - lastFlush) / 1000) : -1LL,
              lastQueue ? (long long)((nowus - lastQueue) / 1000) : -1LL,
              pAudioPipe->getFlushCount(),
              pAudioPipe->getQueueCount(),
              (unsigned long long)(pAudioPipe->getFlushedBytes() / 1024),
              pAudioPipe->getWriteOffset(),
              pAudioPipe->getWriteOffset() + available,
              (int) pAudioPipe->getLwsState());
            pAudioPipe->binaryWritePtrResetToZero();

            frame.data = pAudioPipe->binaryWritePtr();
            frame.buflen = available = pAudioPipe->binarySpaceAvailable();
          }

          switch_status_t rv = switch_core_media_bug_read(bug, &frame, SWITCH_TRUE);
          if (rv != SWITCH_STATUS_SUCCESS) break;
          if (frame.datalen) {
            pAudioPipe->binaryWritePtrAdd(frame.datalen);
            frame.buflen = available = pAudioPipe->binarySpaceAvailable();
            frame.data = pAudioPipe->binaryWritePtr();
            dirty = true;
          }
        }
      }
      else {
        uint8_t data[SWITCH_RECOMMENDED_BUFFER_SIZE];
        switch_frame_t frame = { 0 };
        frame.data = data;
        frame.buflen = SWITCH_RECOMMENDED_BUFFER_SIZE;
        while (switch_core_media_bug_read(bug, &frame, SWITCH_TRUE) == SWITCH_STATUS_SUCCESS) {
          if (frame.datalen) {
            spx_uint32_t out_len = available >> 1;  // space for samples which are 2 bytes
            spx_uint32_t in_len = frame.samples;

            speex_resampler_process_interleaved_int(tech_pvt->resampler, 
              (const spx_int16_t *) frame.data, 
              (spx_uint32_t *) &in_len, 
              (spx_int16_t *) ((char *) pAudioPipe->binaryWritePtr()),
              &out_len);

            if (out_len > 0) {
              // bytes written = num samples * 2 * num channels
              size_t bytes_written = out_len << tech_pvt->channels;
              pAudioPipe->binaryWritePtrAdd(bytes_written);
              available = pAudioPipe->binarySpaceAvailable();
              dirty = true;
            }
            if (available < pAudioPipe->binaryMinSpace()) {
              if (!tech_pvt->buffer_overrun_notified) {
                tech_pvt->buffer_overrun_notified = 1;
                uint64_t nowus = AudioPipe::nowUs();
                uint64_t lastFlush = pAudioPipe->getLastFlushUs();
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                  "(%u) dropping packets! [MOD-UPLINK-STALL resampled] since_flush_ms=%lld flushes=%u queues=%u lws_state=%d\n",
                  tech_pvt->id,
                  lastFlush ? (long long)((nowus - lastFlush) / 1000) : -1LL,
                  pAudioPipe->getFlushCount(),
                  pAudioPipe->getQueueCount(),
                  (int) pAudioPipe->getLwsState());
                tech_pvt->responseHandler(session, EVENT_BUFFER_OVERRUN, NULL);
              }
              break;
            }
          }
        }
      }

      pAudioPipe->unlockAudioBuffer();
      switch_mutex_unlock(tech_pvt->mutex);
    }
    return SWITCH_TRUE;
  }

  /* ── fork_session_handle_binary ───────────────────────────────────────────
   * Called from eventCallback when a BINARY_AUDIO event fires, on the shared
   * lws service thread. Resamples inbound PCM (playback_input_rate ->
   * playback_channel_rate) into the per-session jitter buffer and returns
   * immediately. The session's own writer thread drains it and does the
   * blocking write to the channel.
   * ─────────────────────────────────────────────────────────────────────── */
  void fork_session_handle_binary(private_t *tech_pvt, switch_core_session_t *session, const uint8_t *data, size_t len) {
    if (!tech_pvt || !tech_pvt->playback_active || !tech_pvt->playback_buffer || !data || len == 0)
      return;

    const int16_t *in_pcm   = (const int16_t *) data;
    spx_uint32_t   in_len   = (spx_uint32_t)(len / 2);  /* samples */

    const int16_t *write_ptr    = in_pcm;
    spx_uint32_t   write_samples = in_len;

    /* Resample if needed (e.g. 16kHz → 8kHz for G.711 channels) */
    int16_t resampled[2048];
    if (tech_pvt->playback_resampler) {
      spx_uint32_t out_len = (spx_uint32_t)(sizeof(resampled) / sizeof(int16_t));
      speex_resampler_process_int(tech_pvt->playback_resampler, 0,
        in_pcm, &in_len,
        resampled, &out_len);
      write_ptr     = resampled;
      write_samples = out_len;
    }

    if (write_samples == 0) return;

    size_t write_bytes = write_samples * 2;
    const size_t MAX_BUFFER = (size_t)nPlaybackJitterFrames *
      (tech_pvt->playback_frame_bytes > 0 ? (size_t)tech_pvt->playback_frame_bytes : FRAME_SIZE_8000);

    if (tech_pvt->playback_frame_bytes > 0 && write_bytes != (size_t)tech_pvt->playback_frame_bytes) {
      tech_pvt->dbg_binary_bad_frame_size++;
      if (tech_pvt->dbg_binary_bad_frame_size == 1 || tech_pvt->dbg_binary_bad_frame_size % 20 == 0) {
        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING,
          "(%u) [MOD-BINARY-SIZE] #%u expected=%dB got=%zuB input_rate=%d channel_rate=%d\n",
          tech_pvt->id, tech_pvt->dbg_binary_bad_frame_size,
          tech_pvt->playback_frame_bytes, write_bytes,
          tech_pvt->playback_input_rate, tech_pvt->playback_channel_rate);
      }
    }

    switch_mutex_lock(tech_pvt->playback_mutex);
    size_t inuse = switch_buffer_inuse(tech_pvt->playback_buffer);
    if (inuse > tech_pvt->dbg_playback_hwm_bytes)
      tech_pvt->dbg_playback_hwm_bytes = (uint32_t) inuse;
    if (inuse + write_bytes > MAX_BUFFER) {
      /* Overflow: drop oldest data to make room */
      size_t drop = (inuse + write_bytes) - MAX_BUFFER;
      if (drop > inuse) drop = inuse;
      switch_buffer_toss(tech_pvt->playback_buffer, drop);
      tech_pvt->dbg_playback_overflow_frames++;
      /* Rate-limited: one full-buffer episode produces a run of these, and at
       * 50 sessions the unthrottled version buried everything else. */
      if (tech_pvt->dbg_playback_overflow_frames == 1 ||
          tech_pvt->dbg_playback_overflow_frames % 25 == 0) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
          "(%u) [MOD-BINARY] OVERFLOW #%u: dropped %zu B | inuse_before=%zu write_bytes=%zu max_buffer=%zu depth_frames=%d\n",
          tech_pvt->id, tech_pvt->dbg_playback_overflow_frames,
          drop, inuse, write_bytes, MAX_BUFFER, nPlaybackJitterFrames);
      }
    }
    switch_buffer_write(tech_pvt->playback_buffer, write_ptr, write_bytes);
    switch_mutex_unlock(tech_pvt->playback_mutex);

    tech_pvt->dbg_binary_frames_rx++;

    /* [BUG-RE] TEMPORARY: arrival cadence -- when frames REACH us, before any
     * buffering. The counterpart of the write cadence below; comparing the two
     * on one call locates the jitter source. Runs on the lws service thread. */
    {
      uint64_t rx_now = AudioPipe::nowUs();
      if (tech_pvt->dbg_last_rx_us) {
        uint64_t iv_us = rx_now - tech_pvt->dbg_last_rx_us;
        if (iv_us <= 500000) {
          tech_pvt->dbg_rx_iv_sum += iv_us;
          tech_pvt->dbg_rx_iv_n++;
          if (iv_us > 30000) {
            tech_pvt->dbg_rx_gaps_30ms++;
            uint32_t gap_ms = (uint32_t)(iv_us / 1000);
            if (gap_ms > tech_pvt->dbg_rx_worst_gap_ms) {
              tech_pvt->dbg_rx_worst_gap_ms = gap_ms;
              tech_pvt->dbg_rx_worst_at_ms = tech_pvt->dbg_binary_frames_rx * 20;
            }
          }
          else if (iv_us < 10000) tech_pvt->dbg_rx_bunch_10ms++;
        }
        else tech_pvt->dbg_rx_pauses++;
      }
      tech_pvt->dbg_last_rx_us = rx_now;
    }

    /* [BUG-RE] TEMPORARY: dump exactly what arrived, before the jitter buffer
     * and before any resampling, so the bytes can be listened to directly. */
    if (dumpPcmUuid && (0 == strcmp(dumpPcmUuid, "all") ||
                        0 == strcmp(dumpPcmUuid, tech_pvt->sessionId))) {
      char szDumpPath[512];
      switch_snprintf(szDumpPath, sizeof(szDumpPath), "%s%s%s.rx.raw",
        SWITCH_GLOBAL_dirs.temp_dir, SWITCH_PATH_SEPARATOR, tech_pvt->sessionId);
      std::ofstream f(szDumpPath, std::ios::binary | std::ios::app);
      if (f.is_open()) {
        f.write((const char *) data, (std::streamsize) len);
        f.close();
      }
    }
    /* Deliberately no write_frame here: this runs on the shared lws service
     * thread. The per-session playback writer thread picks the data up. */
  }

}

