// FX-4CR (BG2FX) with F5BUD V2 firmware.
//
// USB: internal hub -> C-Media UAC1 codec (RX + TX audio) and a USB-serial
// bridge for CAT. CAT emulates a Kenwood TS-590S(G) at 115200 8N1; the
// stream_uac FX-4CR profile opens the bridge and keeps RTS (= PTT) low.
// TX audio is only taken from USB in DATA-L/DATA-U, so the radio menu
// "Digital mode (CAT)" must be DATA (the default) for MD2 to select DATA-U.
#include "radio_control_backend.h"

#include <cstdio>
#include <cstring>

#include "esp_log.h"
#include "stream_uac.h"

static const char* TAG = "RADIO_FX4CR";

static bool fx4cr_ready(void) {
    return cat_cdc_ready();
}

static esp_err_t fx4cr_send_cmd(const char* command, uint32_t timeout_ms) {
    if (!cat_cdc_ready()) return ESP_ERR_INVALID_STATE;
    return cat_cdc_send(reinterpret_cast<const uint8_t*>(command),
                        strlen(command), timeout_ms);
}

static esp_err_t fx4cr_sync_frequency_mode(int freq_hz) {
    // MD2 = USB, which F5BUD maps to DATA-U when "Digital mode (CAT)" = DATA.
    // DA1 additionally requests TS-590 data mode; ignored if unsupported.
    esp_err_t err = fx4cr_send_cmd("MD2;", 200);
    if (err != ESP_OK) return err;
    fx4cr_send_cmd("DA1;", 200);
    fx4cr_send_cmd("FR0;", 200);
    fx4cr_send_cmd("FT0;", 200);

    char command[32];
    snprintf(command, sizeof(command), "FA%011d;", freq_hz);
    err = fx4cr_send_cmd(command, 200);
    if (err == ESP_OK) {
        ESP_LOGI(TAG, "FX-4CR sync ok freq=%d", freq_hz);
    }
    return err;
}

static esp_err_t fx4cr_begin_tx(int freq_hz, int tx_base_hz) {
    (void)freq_hz;
    (void)tx_base_hz;
    esp_err_t err = fx4cr_send_cmd("TX;", 200);
    if (err == ESP_OK) {
        ESP_LOGI(TAG, "FX-4CR TX start");
    }
    return err;
}

static esp_err_t fx4cr_set_tone_hz(float tone_hz) {
    (void)tone_hz;  // TX tones are synthesized into the UAC OUT stream
    return ESP_OK;
}

static esp_err_t fx4cr_end_tx(void) {
    uac_tx_end();
    esp_err_t err = fx4cr_send_cmd("RX;", 200);
    if (err == ESP_OK) {
        ESP_LOGI(TAG, "FX-4CR TX stop");
    }
    return err;
}

static esp_err_t fx4cr_set_tune(bool enable, int freq_hz, int tone_hz) {
    if (!enable) {
        return fx4cr_end_tx();
    }
    esp_err_t err = fx4cr_sync_frequency_mode(freq_hz);
    if (err != ESP_OK) return err;
    err = fx4cr_send_cmd("TX;", 200);
    if (err != ESP_OK) return err;
    if (!uac_tx_begin_tune(static_cast<float>(tone_hz))) {
        fx4cr_send_cmd("RX;", 200);
        return ESP_ERR_INVALID_STATE;
    }
    return ESP_OK;
}

static esp_err_t fx4cr_on_audio_start(void) {
    ESP_LOGI(TAG, "FX-4CR CAT/UAC backend initialized");
    return ESP_OK;
}

static esp_err_t fx4cr_start_tx_waveform(float base_hz,
                                         const uint8_t* symbols,
                                         size_t symbol_count,
                                         float tone_spacing_hz,
                                         float symbol_period_s,
                                         uint32_t samples_per_symbol_48k) {
    (void)symbol_period_s;
    return uac_tx_begin_cpfsk(base_hz, symbols, symbol_count, tone_spacing_hz,
                              samples_per_symbol_48k)
               ? ESP_OK
               : ESP_ERR_INVALID_STATE;
}

static const radio_control_ops_t k_ops = {
    .name = "fx4cr",
    .ready = fx4cr_ready,
    .on_audio_start = fx4cr_on_audio_start,
    .sync_frequency_mode = fx4cr_sync_frequency_mode,
    .begin_tx = fx4cr_begin_tx,
    .set_tone_hz = fx4cr_set_tone_hz,
    .end_tx = fx4cr_end_tx,
    .set_tune = fx4cr_set_tune,
    .set_time = nullptr,  // TS-590 CAT has no clock command
    .start_tx_waveform = fx4cr_start_tx_waveform,
};

const radio_control_ops_t* radio_control_fx4cr_get_ops(void) {
    return &k_ops;
}
