#ifndef SCAN_SYNC_H
#define SCAN_SYNC_H

#include <stdint.h>

#define SCAN_SYNC_PHASE_SNAPSHOT_ASYNC 0
#define SCAN_SYNC_PHASE_SCAN_END 1

typedef struct
{
    uint64_t scan_counter;
    unsigned long tick;
    uint8_t phase;
    uint64_t timestamp_ns;
} scan_sync_meta_t;

void scan_sync_init(void);
void scan_sync_notify_scan_end(unsigned long tick);
void scan_sync_notify_scan_start(void);
int scan_sync_wait_for_end(uint64_t after_counter, int timeout_ms, scan_sync_meta_t *meta);
int scan_sync_wait_for_start(uint64_t after_counter, int timeout_ms);
uint64_t scan_sync_get_start_counter(void);

#endif /* SCAN_SYNC_H */
