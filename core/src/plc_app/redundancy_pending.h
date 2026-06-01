#ifndef REDUNDANCY_PENDING_H
#define REDUNDANCY_PENDING_H

#include <stddef.h>
#include <stdint.h>

#define REDUNDANCY_PENDING_MAX_BYTES 69632U

void redundancy_pending_init(void);

/** Queue payload for barrier apply (phase 5). Returns 0 on success. */
int redundancy_pending_store(const uint8_t *buf, size_t len);

/** Apply queued payload at scan-cycle barrier; returns 1 if something was applied. */
int redundancy_pending_apply_at_barrier(void);

int redundancy_pending_has_pending(void);

uint64_t redundancy_pending_last_applied_seq(void);

void redundancy_pending_set_frame_seq(uint64_t seq);

/** Allocate monotonic frame_seq for legacy IMAGE_SNAPSHOT_SET on shadow standby. */
uint64_t redundancy_pending_alloc_frame_seq(void);

/**
 * Wait until last_applied_frame_seq >= want (barrier apply), or timeout.
 * Returns 0 on success, -1 on timeout.
 */
int redundancy_pending_wait_applied(uint64_t want, int timeout_ms);

#endif /* REDUNDANCY_PENDING_H */
