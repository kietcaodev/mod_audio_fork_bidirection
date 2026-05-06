#ifndef __MOD_FORK_H__
#define __MOD_FORK_H__

#include <switch.h>
#include <libwebsockets.h>
#include <speex/speex_resampler.h>

#include <unistd.h>

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
  switch_buffer_t   *playback_buffer;       /* ring buffer holding channel-rate PCM ready to inject */
  switch_mutex_t    *playback_mutex;        /* protects playback_buffer */
  SpeexResamplerState *playback_resampler;  /* lazy-init: resample 16kHz inbound -> channel native rate */
  switch_codec_t    playback_codec;         /* L16 codec used for direct caller playback */
  int  playback_input_rate;                 /* sample rate arriving from WS (default 16000) */
  int  playback_channel_rate;               /* native channel rate (8000 for G.711, 16000 for wideband) */
  int  playback_frame_bytes;                /* one outbound packet in decoded linear bytes */
  int  playback_active:1;                   /* 1 after enableBinaryPlayback received */
  int  playback_direct_mode:1;              /* 1 when direct switch_core_session_write_frame is active */
  int  playback_codec_ready:1;              /* L16 playback codec initialized */
  int  playback_logged_first_direct_write:1;
  int  playback_logged_write_replace_skip:1;
  /* ── Debug counters ────────────────────────────────────────────────────── */
  uint32_t dbg_binary_frames_rx;           /* total binary frames received from WS */
  uint32_t dbg_wr_frames_full;             /* WRITE_REPLACE: full frames sent */
  uint32_t dbg_wr_frames_partial;          /* WRITE_REPLACE: partial (silence-padded) frames */
  uint32_t dbg_wr_frames_underrun;         /* WRITE_REPLACE: empty buffer (silence kept) */
  /* ────────────────────────────────────────────────────────────────────────── */
};

typedef struct private_data private_t;

#endif
