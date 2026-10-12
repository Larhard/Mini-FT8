#pragma once

// In-RAM capture of ESP_LOG output for field debugging. While the radio
// occupies the Cardputer's only USB-C port there is no serial console, so
// the most recent log lines are kept in a ring buffer and can be written to
// internal storage (and then copied to SD with O->5).

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Install the log hook (once) and clear the buffer.
void log_capture_start(void);
// Write the captured lines to `name` on internal storage. Main task only.
bool log_capture_dump(const char* name);

#ifdef __cplusplus
}
#endif
