#pragma once

// ============================================================================
// debug_log.h — optional capture of all ESP_LOG output to internal storage.
//
// While a radio occupies the Cardputer's only USB-C port there is no serial
// console. When enabled (MENU P3 -> RxTx log: ON+DBG), every ESP_LOG line is
// kept in a RAM ring buffer placed in .noinit, so it also survives a crash
// reboot. The buffer is appended to DEBUG.txt only at fixed points (never
// continuously, to avoid flash stalls and wear): after an abnormal reset,
// when radio audio stops, and before O->5 copies files to SD.
// ============================================================================

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

#define DEBUG_LOG_FILE "DEBUG.txt"

// Install the log hook. Call once, early. Keeps lines captured before a
// crash/watchdog/brownout reset so they can be dumped after boot.
void debug_log_init(void);
void debug_log_set_enabled(bool enabled);
bool debug_log_enabled(void);
// True when the ring holds lines from before the last reset not yet dumped.
bool debug_log_has_pre_reset_lines(void);
// Append not-yet-dumped lines to DEBUG.txt under a "===== title =====" line.
// No-op when disabled or nothing new. Storage must be available (main task).
bool debug_log_dump(const char* title);

#ifdef __cplusplus
}
#endif
