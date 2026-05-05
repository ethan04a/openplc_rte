#ifndef IMAGE_SNAPSHOT_H
#define IMAGE_SNAPSHOT_H

#include <stddef.h>
#include <stdint.h>

#include "image_tables.h"

/** Binary layout version (must match between peers). */
#define IMAGE_SNAPSHOT_VERSION 1

/** Bytes per BUFFER_SIZE row: bool I/O, byte I/O, int/dint/lint I/O, memories, bool memory. */
#define IMAGE_SNAPSHOT_ROW_BYTES 68

#define IMAGE_SNAPSHOT_TOTAL_BYTES ((size_t)BUFFER_SIZE * (size_t)IMAGE_SNAPSHOT_ROW_BYTES)

/**
 * Export full I/O and memory image into buf.
 * Caller must hold plugin_driver buffer_mutex (same as PLC scan / journal).
 * Returns 0 on success, -1 if buf_cap too small or pointers invalid.
 */
int image_snapshot_export(uint8_t *buf, size_t buf_cap, size_t *out_len);

/**
 * Import snapshot into live image tables.
 * Caller must hold plugin_driver buffer_mutex.
 * Returns 0 on success, -1 on version/size mismatch or invalid payload.
 */
int image_snapshot_import(const uint8_t *buf, size_t buf_len);

#endif /* IMAGE_SNAPSHOT_H */
