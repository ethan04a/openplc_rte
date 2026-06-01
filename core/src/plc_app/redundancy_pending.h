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

#endif /* REDUNDANCY_PENDING_H */
