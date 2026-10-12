#include "log_capture.h"

#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <string>

#include "esp_heap_caps.h"
#include "esp_log.h"
#include "freertos/FreeRTOS.h"
#include "storage_service.h"

static constexpr size_t k_ring_size = 8192;
static char* s_ring = nullptr;     // allocated on first start
static size_t s_head = 0;          // next write position
static bool s_wrapped = false;
static portMUX_TYPE s_lock = portMUX_INITIALIZER_UNLOCKED;
static vprintf_like_t s_prev_vprintf = nullptr;

static int capture_vprintf(const char* fmt, va_list args) {
    va_list copy;
    va_copy(copy, args);
    char line[192];
    int n = vsnprintf(line, sizeof(line), fmt, copy);
    va_end(copy);
    if (n > 0 && s_ring) {
        size_t len = (size_t)n < sizeof(line) ? (size_t)n : sizeof(line) - 1;
        taskENTER_CRITICAL(&s_lock);
        for (size_t i = 0; i < len; ++i) {
            s_ring[s_head++] = line[i];
            if (s_head == k_ring_size) {
                s_head = 0;
                s_wrapped = true;
            }
        }
        taskEXIT_CRITICAL(&s_lock);
    }
    return s_prev_vprintf ? s_prev_vprintf(fmt, args) : vprintf(fmt, args);
}

void log_capture_start(void) {
    if (!s_ring) {
        s_ring = static_cast<char*>(heap_caps_malloc(k_ring_size, MALLOC_CAP_DEFAULT));
        if (!s_ring) return;
        s_prev_vprintf = esp_log_set_vprintf(capture_vprintf);
    }
    taskENTER_CRITICAL(&s_lock);
    s_head = 0;
    s_wrapped = false;
    taskEXIT_CRITICAL(&s_lock);
}

bool log_capture_dump(const char* name) {
    if (!s_ring || !name) return false;
    std::string out;
    out.reserve(k_ring_size);
    taskENTER_CRITICAL(&s_lock);
    const size_t head = s_head;
    const bool wrapped = s_wrapped;
    taskEXIT_CRITICAL(&s_lock);
    // Not locked while copying: a concurrent line may tear, which is fine
    // for a diagnostic dump and keeps the critical section short.
    out.append("
===== log dump =====
");
    if (wrapped) out.append(s_ring + head, k_ring_size - head);
    out.append(s_ring, head);
    // Append, not storage_file_write_atomic(): internal FATFS has no long
    // file names (CONFIG_FATFS_LFN_NONE), so its "<name>.tmp" temp file
    // cannot be created for names like USBLOG.txt.
    return storage_file_append(name, out, "", true);
}
