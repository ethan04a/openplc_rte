#ifndef IMAGE_SNAPSHOT_DELTA_H
#define IMAGE_SNAPSHOT_DELTA_H

#include <stddef.h>
#include <stdint.h>

#include "scan_sync.h"

/** Wire magic bytes for output/memory delta payloads (phase 4): "OPDL". */

#define IMAGE_DELTA_VERSION 1

/** Bytes per row for output + memory slice (skips bool_input[8] in full row). */
#define IMAGE_DELTA_OM_ROW_BYTES 60U

/** Dirty bitmap covers BUFFER_SIZE (1024) rows. */
#define IMAGE_DELTA_DIRTY_BITMAP_BYTES 128U

/** Max delta body before falling back to full snapshot (single-datagram friendly). */
#define IMAGE_DELTA_MAX_WIRE_BYTES 1100U

/**
 * Build output/memory delta after scan end (compares OM slice to previous cycle).
 * Caller must hold plugin_driver buffer_mutex.
 * out_buf receives wire-format delta; returns 0 on success.
 */
int image_delta_export_scan_end(uint8_t *out_buf, size_t out_cap, size_t *out_len,
                                const scan_sync_meta_t *meta);

/** Apply delta wire payload into live tables (OM fields only). Caller holds buffer_mutex. */
int image_delta_import(const uint8_t *buf, size_t buf_len);

/** True if buf looks like a delta payload. */
int image_delta_is_delta_payload(const uint8_t *buf, size_t len);

void image_delta_reset_baseline(void);

#endif /* IMAGE_SNAPSHOT_DELTA_H */
