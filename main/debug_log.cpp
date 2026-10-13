#include "debug_log.h"

#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

#include "esp_attr.h"
#include "esp_log.h"
#include "freertos/FreeRTOS.h"
#include "storage_service.h"

// 6 KB is roughly the last minute of normal logging and keeps the heap
// headroom the FT4 decoder needs.
static constexpr size_t k_ring_size = 6 * 1024;
static constexpr uint32_t k_magic = 0x44424731;  // "DBG1"

// .noinit: not cleared at boot, so a panic/watchdog/brownout reset keeps the
// lines leading up to it. The magic word tells valid content from the
// random RAM contents after a power-on.
typedef struct {
    uint32_t magic;
    uint32_t enabled;   // capture switch, also remembered across a reset
    uint32_t total;     // bytes ever written (ring position = total % size)
    uint32_t dumped;    // value of total at the last dump
    uint32_t boot_mark; // value of total when this boot started
    char buf[k_ring_size];
} ring_t;

static __NOINIT_ATTR ring_t s_ring;
static portMUX_TYPE s_lock = portMUX_INITIALIZER_UNLOCKED;
static vprintf_like_t s_prev_vprintf = nullptr;
static bool s_installed = false;

static int capture_vprintf(const char* fmt, va_list args) {
    if (s_ring.enabled) {
        va_list copy;
        va_copy(copy, args);
        char line[160];
        int n = vsnprintf(line, sizeof(line), fmt, copy);
        va_end(copy);
        if (n > 0) {
            size_t len = (size_t)n < sizeof(line) ? (size_t)n : sizeof(line) - 1;
            taskENTER_CRITICAL(&s_lock);
            for (size_t i = 0; i < len; ++i) {
                s_ring.buf[s_ring.total % k_ring_size] = line[i];
                s_ring.total++;
            }
            taskEXIT_CRITICAL(&s_lock);
        }
    }
    return s_prev_vprintf ? s_prev_vprintf(fmt, args) : vprintf(fmt, args);
}

void debug_log_init(void) {
    if (s_installed) return;
    if (s_ring.magic != k_magic || s_ring.dumped > s_ring.total) {
        memset(&s_ring, 0, offsetof(ring_t, buf));
        s_ring.magic = k_magic;
    }
    s_ring.boot_mark = s_ring.total;
    s_prev_vprintf = esp_log_set_vprintf(capture_vprintf);
    s_installed = true;
}

void debug_log_set_enabled(bool enabled) {
    s_ring.enabled = enabled ? 1 : 0;
}

bool debug_log_enabled(void) {
    return s_ring.enabled != 0;
}

bool debug_log_has_pre_reset_lines(void) {
    return s_ring.magic == k_magic && s_ring.boot_mark > s_ring.dumped;
}

bool debug_log_dump(const char* title) {
    if (s_ring.magic != k_magic) return false;
    taskENTER_CRITICAL(&s_lock);
    const uint32_t total = s_ring.total;
    const uint32_t dumped = s_ring.dumped;
    taskEXIT_CRITICAL(&s_lock);
    if (total == dumped) return true;

    uint32_t start = dumped;
    std::string out;
    out.reserve(k_ring_size + 64);
    out += "\n===== ";
    out += title ? title : "dump";
    out += " =====\n";
    if (total - start > k_ring_size) {
        start = total - k_ring_size;
        out += "[... older lines overwritten ...]\n";
    }
    // Not locked while copying: a line written meanwhile may tear, which is
    // acceptable for a diagnostic dump and keeps the critical section short.
    for (uint32_t pos = start; pos < total; ++pos) {
        out += s_ring.buf[pos % k_ring_size];
    }
    // Append, not storage_file_write_atomic(): internal FATFS has no long
    // file names, so the "<name>.tmp" temp file would not be valid.
    if (!storage_file_append(DEBUG_LOG_FILE, out, "", true)) return false;
    s_ring.dumped = total;
    return true;
}
