#ifndef __MOD_FORK_H__
#define __MOD_FORK_H__

#include <switch.h>
#include <libwebsockets.h>
#include <speex/speex_resampler.h>

#include <unistd.h>

/* Bump on every change that gets deployed, so a reload proves which build is
 * actually loaded. FreeSWITCH reports "+OK module loaded" even when it kept the
 * previous .so, so the version line in the log is the only reliable check. */
#define MOD_AUDIO_FORK_VERSION "1.0.10-paced"

#define MY_BUG_NAME "audio_fork"
#define MAX_BUG_LEN (64)
#define MAX_SESSION_ID (256)
#define MAX_WS_URL_LEN (512)
#define MAX_PATH_LEN (4096)

#define EVENT_TRANSCRIPTION   "mod_audio_fork::transcription"
#define EVENT_TRANSFER        "mod_audio_fork::transfer"
#define EVENT_PLAY_AUDIO      "mod_audio_fork::play_audio"
#define EVENT_KILL_AUDIO      "mod_audio_fork::kill_audio"
#define EVENT_DISCONNECT      "mod_audio_fork::disconnect"
#define EVENT_ERROR           "mod_audio_fork::error"
#define EVENT_CONNECT_SUCCESS "mod_audio_fork::connect"
#define EVENT_CONNECT_FAIL    "mod_audio_fork::connect_failed"
#define EVENT_BUFFER_OVERRUN  "mod_audio_fork::buffer_overrun"
#define EVENT_JSON            "mod_audio_fork::json"

#define MAX_METADATA_LEN (8192)

struct playout {
  char *file;
  struct playout* next;
};

typedef void (*responseHandler_t)(switch_core_session_t* session, const char* eventName, char* json);

struct private_data {
	switch_mutex_t *mutex;
	char sessionId[MAX_SESSION_ID];
  char bugname[MAX_BUG_LEN+1];
  SpeexResamplerState *resampler;
  responseHandler_t responseHandler;
  void *pAudioPipe;
  int ws_state;
  char host[MAX_WS_URL_LEN];
  unsigned int port;
  char path[MAX_PATH_LEN];
  int sampling;
  struct playout* playout;
  int  channels;
  unsigned int id;
  int buffer_overrun_notified:1;
  int audio_paused:1;
  int graceful_shutdown:1;
  char initialMetadata[8192];

  /* ── Binary playback (realtime audio mode) ─────────────────────────────── */
  switch_buffer_t   *playback_buffer;       /* tiny jitter buffer holding channel-rate PCM ready to inject */
  switch_mutex_t    *playback_mutex;        /* protects playback_buffer */
  SpeexResamplerState *playback_resampler;  /* lazy-init: resample inbound PCM -> channel native rate */
  switch_codec_t    playback_codec;         /* L16 codec used for direct caller playback */
  int  playback_input_rate;                 /* sample rate arriving from WS (default 8000, overridden by enableBinaryPlayback) */
  int  playback_channel_rate;               /* native channel rate (8000 for G.711, 16000 for wideband) */
  int  playback_frame_bytes;                /* one outbound packet in decoded linear bytes */
  int  playback_active:1;                   /* 1 after enableBinaryPlayback received */
  int  playback_direct_mode:1;              /* 1 when direct switch_core_session_write_frame is active */
  int  playback_codec_ready:1;              /* L16 playback codec initialized */

  /* ── Dedicated playback writer thread ──────────────────────────────────────
   * switch_core_session_write_frame() blocks for a full RTP frame interval
   * (~20ms) whenever it contends with the channel's own write path. Doing that
   * on the shared lws service thread starved every other session's uplink
   * flush, so each session drains its own jitter buffer on its own thread. */
  switch_core_session_t *session;           /* owning session, for the writer thread */
  switch_thread_t   *playback_thread;       /* NULL when no writer was started */
  /* Pool-allocated scratch for one outbound frame. Preallocated because the
   * writer touches it 50x/sec per session; a per-frame heap allocation here was
   * 10k malloc/free per second at 200 calls. */
  uint8_t          *playback_chunk;
  /* Not a bitfield on purpose: the session thread sets this while the lws
   * thread writes the playback_* bitfields above, and sharing one storage unit
   * across threads makes those read-modify-writes race. */
  int  playback_thread_stop;                /* set by cleanup, polled by the writer */
  /* ── Per-session counters, reported once in [MOD-BINARY-SUMMARY] ─────────
   * Kept in production: one line per call is cheap and it is the only record
   * of audio quality (frames dropped, how deep the jitter buffer had to go)
   * after the call is gone. */
  uint32_t dbg_binary_frames_rx;           /* total binary frames received from WS */
  uint32_t dbg_binary_bad_frame_size;      /* binary frames that do not match one channel packet */
  uint32_t dbg_direct_slow_writes;         /* writes that blocked longer than 30ms */
  uint32_t dbg_direct_frames;              /* playback frames written to the channel */
  uint64_t dbg_direct_write_us;            /* cumulative us spent in switch_core_session_write_frame */
  uint32_t dbg_playback_hwm_bytes;         /* deepest the playback jitter buffer ever got */
  uint32_t dbg_playback_overflow_frames;   /* frames dropped because that buffer was full */

  /* ── [BUG-RE] TEMPORARY: audio *content* instrumentation ──────────────────
   * Every existing counter measures timing and counts; none of them look at
   * the samples. All of them read clean while callers still hear crackle, so
   * these measure the waveform itself, on the exact bytes handed to the
   * channel. Remove once the root cause is confirmed and fixed.
   *
   *  mad/rms  - clean 8 kHz speech is lowpass, so the mean absolute
   *             sample-to-sample difference is well below RMS. A ratio near or
   *             above 1.0 means high-frequency garbage: byte-misaligned PCM,
   *             aliasing from a bad downsample, or noise. This is the crackle
   *             detector.
   *  bstep    - mean step across a frame boundary vs the mean step inside a
   *             frame. Much larger at boundaries means frames are being
   *             spliced discontinuously, which is heard as clicks.
   *  clip     - samples at full scale, which PCMU/PCMA companding turns into
   *             harsh distortion. */
  uint64_t dbg_a_samples;
  uint64_t dbg_a_sumsq;                    /* sum of s^2, for RMS */
  uint64_t dbg_a_sumabsdiff;               /* sum |s[n]-s[n-1]| within frames */
  uint64_t dbg_a_interior_n;
  uint64_t dbg_a_bstep_sum;                /* sum |first - previous frame's last| */
  uint32_t dbg_a_bstep_n;
  uint32_t dbg_a_bstep_max;
  uint32_t dbg_a_peak;
  uint32_t dbg_a_clip;                     /* |s| >= 32000 */
  uint32_t dbg_a_zero_frames;              /* frames that are entirely silence */
  int32_t  dbg_a_prev_last;                /* last sample of the previous frame */
  int      dbg_a_have_prev;

  /* [BUG-RE] TEMPORARY: collision between our writer and the channel's own
   * audio. A and C both measure clean speech, so the distortion happens after
   * we hand the frame over -- and the obvious way that happens is two sources
   * writing one channel. The backend disables binary playback before it runs a
   * file playback, so this counts how often that sequencing actually fails:
   * frames we wrote while FreeSWITCH had a broadcast/playback in progress. */
  uint32_t dbg_write_during_broadcast;
  uint32_t dbg_writes_total_checked;

  /* [BUG-RE] TEMPORARY: per-second windows, because whole-call averages hide
   * exactly the defect being chased. A caller reported two crackly utterances
   * between second 11 and 20 of a ~70s call; 9 bad seconds out of 70 barely
   * move a call-wide mean, which is why every aggregate above read clean.
   * These keep the WORST one-second window and where it was, so a localised
   * burst of distortion is visible and can be lined up against the timestamp
   * the listener reported. */
  uint64_t dbg_w_sumsq;                    /* current window: sum s^2 */
  uint64_t dbg_w_sumabsdiff;               /* current window: sum |s[n]-s[n-1]| */
  uint32_t dbg_w_samples;
  uint32_t dbg_w_interior_n;
  uint32_t dbg_w_frames;                   /* frames into the current window */
  uint32_t dbg_w_clip;
  uint32_t dbg_w_peak;
  uint32_t dbg_w_index;                    /* which window (1 per second) */
  int      dbg_worst_ratio;                /* highest mad*100/rms seen in a window */
  uint32_t dbg_worst_at_ms;                /* offset of that window into playback */
  uint32_t dbg_worst_window_clip;          /* most clipped samples in one window */
  uint32_t dbg_windows_over;               /* windows whose ratio exceeded the bar */
  uint32_t dbg_windows_total;

  /* [BUG-RE] TEMPORARY: the one thing nothing measures -- WHEN we write.
   * A confirmed-crackly window (37-41s of call 23ba65fb) shows the recording
   * jumping between loud bursts and 300ms of near-silence, while the backend
   * reported duty=100% holes=0 and this module reported clean content. Both
   * are blind because one measures when frames were SENT and the other
   * measures what is IN them; neither measures the interval between writes.
   * With playback_hwm=0 and avg_write_us=0 there is no buffer and no pacing on
   * prod, so arrival jitter passes straight through as gaps at the caller.
   * Frames should leave here every 20ms. */
  uint64_t dbg_last_write_us;
  uint64_t dbg_write_iv_sum;
  uint32_t dbg_write_iv_n;
  uint32_t dbg_write_gaps_30ms;            /* interval > 30ms: a hole the caller hears */
  uint32_t dbg_write_bunch_10ms;           /* interval < 10ms: catch-up bunching */
  uint32_t dbg_write_worst_gap_ms;
  uint32_t dbg_write_worst_at_ms;          /* offset into this session's playback */
  uint32_t dbg_write_pauses;               /* interval > 500ms: between turns, not a defect */

  /* [BUG-RE] TEMPORARY: ARRIVAL cadence, same buckets as the write cadence.
   * v1.0.9 paced the writes and the gap counts did not move, which splits the
   * hypothesis in two: either the pacing has a defect (timer catch-up burst
   * after an underrun) or the frames simply ARRIVE with gaps bigger than the
   * prime cushion -- the writer cannot play what has not arrived. Comparing
   * rx_gaps against write gaps on the same call decides it: rx ~= write means
   * the jitter is upstream (backend send or TCP) and the module is faithful;
   * rx clean while write is gapped means the writer still mishandles it. */
  uint64_t dbg_last_rx_us;
  uint64_t dbg_rx_iv_sum;                  /* only intervals <= 500ms */
  uint32_t dbg_rx_iv_n;
  uint32_t dbg_rx_gaps_30ms;               /* 30-500ms arrival gap */
  uint32_t dbg_rx_bunch_10ms;              /* < 10ms: frames arriving bunched */
  uint32_t dbg_rx_pauses;                  /* > 500ms: between utterances */
  uint32_t dbg_rx_worst_gap_ms;
  uint32_t dbg_rx_worst_at_ms;             /* offset in received audio */
  /* ────────────────────────────────────────────────────────────────────────── */
};

typedef struct private_data private_t;

#endif
