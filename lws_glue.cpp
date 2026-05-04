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

#include "base64.hpp"
#include "parser.hpp"
#include "mod_audio_fork.h"
#include "audio_pipe.hpp"

/* Forward decl: defined later in this TU inside extern "C" block */
extern "C" void fork_session_handle_binary(private_t *tech_pvt, switch_core_session_t *session, const uint8_t *data, size_t len);

#define RTP_PACKETIZATION_PERIOD 20
#define FRAME_SIZE_8000  320 /*which means each 20ms frame as 320 bytes at 8 khz (1 channel only)*/

namespace {
  static const char *requestedBufferSecs = std::getenv("MOD_AUDIO_FORK_BUFFER_SECS");
  static int nAudioBufferSecs = std::max(1, std::min(requestedBufferSecs ? ::atoi(requestedBufferSecs) : 2, 5));
  static const char *requestedNumServiceThreads = std::getenv("MOD_AUDIO_FORK_SERVICE_THREADS");
  static const char* mySubProtocolName = std::getenv("MOD_AUDIO_FORK_SUBPROTOCOL_NAME") ?
    std::getenv("MOD_AUDIO_FORK_SUBPROTOCOL_NAME") : "audio.drachtio.org";
  static unsigned int nServiceThreads = std::max(1, std::min(requestedNumServiceThreads ? ::atoi(requestedNumServiceThreads) : 1, 5));
  static unsigned int idxCallCount = 0;
  static uint32_t playCount = 0;

  static switch_status_t write_playback_frames_direct(private_t *tech_pvt, switch_core_session_t *session) {
    if (!tech_pvt || !session || !tech_pvt->playback_direct_mode || !tech_pvt->playback_codec_ready ||
        !tech_pvt->playback_buffer || !tech_pvt->playback_mutex || tech_pvt->playback_frame_bytes <= 0) {
      return SWITCH_STATUS_FALSE;
    }

    switch_channel_t *channel = switch_core_session_get_channel(session);
    if (!channel || !switch_channel_ready(channel)) {
      return SWITCH_STATUS_FALSE;
    }

    std::vector<uint8_t> chunk((size_t)tech_pvt->playback_frame_bytes);

    while (true) {
      size_t bytes_read = 0;
      switch_mutex_lock(tech_pvt->playback_mutex);
      size_t available = switch_buffer_inuse(tech_pvt->playback_buffer);
      if (available >= (size_t)tech_pvt->playback_frame_bytes) {
        bytes_read = switch_buffer_read(tech_pvt->playback_buffer, chunk.data(), (size_t)tech_pvt->playback_frame_bytes);
      }
      switch_mutex_unlock(tech_pvt->playback_mutex);

      if (bytes_read < (size_t)tech_pvt->playback_frame_bytes) {
        break;
      }

      switch_frame_t frame = { 0 };
      frame.codec = &tech_pvt->playback_codec;
      frame.data = chunk.data();
      frame.buflen = (uint32_t)bytes_read;
      frame.datalen = (uint32_t)bytes_read;
      frame.samples = (uint32_t)(bytes_read / sizeof(int16_t));
      frame.channels = 1;
      frame.rate = tech_pvt->playback_channel_rate;

      switch_status_t status = switch_core_session_write_frame(session, &frame, SWITCH_IO_FLAG_NONE, 0);
      if (status != SWITCH_STATUS_SUCCESS) {
        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
          "(%u) direct playback write failed: status=%d bytes=%zu\n",
          tech_pvt->id, status, bytes_read);
        return status;
      }

      if (!tech_pvt->playback_logged_first_direct_write) {
        tech_pvt->playback_logged_first_direct_write = 1;
        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO,
          "(%u) direct playback write active: frame_bytes=%d channel_rate=%d\n",
          tech_pvt->id, tech_pvt->playback_frame_bytes, tech_pvt->playback_channel_rate);
      }
    }

    return SWITCH_STATUS_SUCCESS;
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
          if (0 == strcmp(szAudioContentType, "raw")) {
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
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
          "(%u) flush: cleared playback buffer\n", tech_pvt->id);
      }
      else if (0 == type.compare("enableBinaryPlayback")) {
        /* Optional data.sampleRate field overrides default 16000 */
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
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
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
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
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
    tech_pvt->playback_active       = 0;
    tech_pvt->playback_input_rate   = 16000;   /* backend always sends 16kHz PCM */
    tech_pvt->playback_channel_rate = (int) (write_impl.actual_samples_per_second ?
      write_impl.actual_samples_per_second : read_impl.actual_samples_per_second);
    tech_pvt->playback_resampler    = nullptr;
    tech_pvt->playback_frame_bytes  = (int) (write_impl.decoded_bytes_per_packet ?
      write_impl.decoded_bytes_per_packet : read_impl.decoded_bytes_per_packet);
    tech_pvt->playback_direct_mode  = 0;
    tech_pvt->playback_codec_ready  = 0;
    tech_pvt->playback_logged_first_direct_write = 0;
    tech_pvt->playback_logged_write_replace_skip = 0;
    switch_mutex_init(&tech_pvt->playback_mutex, SWITCH_MUTEX_NESTED,
      switch_core_session_get_pool(session));
    /* 32 kbytes ~ 2 seconds of 8kHz mono PCM */
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
      tech_pvt->playback_direct_mode = 1;
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO,
        "(%u) playback direct mode armed: channel_rate=%d frame_bytes=%d\n",
        tech_pvt->id, tech_pvt->playback_channel_rate, tech_pvt->playback_frame_bytes);
    } else {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING,
        "(%u) playback direct mode unavailable, falling back to WRITE_REPLACE queue\n",
        tech_pvt->id);
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
    /* ── Binary playback cleanup ─────────────────────────────────────────── */
    tech_pvt->playback_active = 0;
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
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_audio_fork: audio buffer (in secs):    %d secs\n", nAudioBufferSecs);
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_audio_fork: sub-protocol:              %s\n", mySubProtocolName);
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_audio_fork: lws service threads:       %d\n", nServiceThreads);
 
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
    if (pAudioPipe) pAudioPipe->close();

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
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "(%u) dropping packets!\n", 
              tech_pvt->id);
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
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "(%u) dropping packets!\n", 
                  tech_pvt->id);
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
   * Called from eventCallback when a BINARY_AUDIO event fires.
   * Resamples inbound PCM (playback_input_rate → playback_channel_rate),
   * writes the result into the per-session ring buffer, then tries direct
   * session playback on parked calls. WRITE_REPLACE remains as a fallback.
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
    const size_t MAX_BUFFER = 32000;  /* ~2 s at 8kHz mono */

    switch_mutex_lock(tech_pvt->playback_mutex);
    size_t inuse = switch_buffer_inuse(tech_pvt->playback_buffer);
    if (inuse + write_bytes > MAX_BUFFER) {
      /* Overflow: drop oldest data to make room */
      size_t drop = (inuse + write_bytes) - MAX_BUFFER;
      if (drop > inuse) drop = inuse;
      switch_buffer_toss(tech_pvt->playback_buffer, drop);
      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,
        "(%u) playback buffer overflow — dropped %zu bytes\n", tech_pvt->id, drop);
    }
    switch_buffer_write(tech_pvt->playback_buffer, write_ptr, write_bytes);
    switch_mutex_unlock(tech_pvt->playback_mutex);

    if (tech_pvt->playback_direct_mode && session) {
      write_playback_frames_direct(tech_pvt, session);
    }
  }

}

