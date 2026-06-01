#include "image_snapshot_delta.h"

#include <string.h>

#include "../lib/iec_types.h"
#include "image_tables.h"

extern IEC_BOOL *bool_input[BUFFER_SIZE][8];
extern IEC_BOOL *bool_output[BUFFER_SIZE][8];
extern IEC_BYTE *byte_input[BUFFER_SIZE];
extern IEC_BYTE *byte_output[BUFFER_SIZE];
extern IEC_UINT *int_input[BUFFER_SIZE];
extern IEC_UINT *int_output[BUFFER_SIZE];
extern IEC_UDINT *dint_input[BUFFER_SIZE];
extern IEC_UDINT *dint_output[BUFFER_SIZE];
extern IEC_ULINT *lint_input[BUFFER_SIZE];
extern IEC_ULINT *lint_output[BUFFER_SIZE];
extern IEC_UINT *int_memory[BUFFER_SIZE];
extern IEC_UDINT *dint_memory[BUFFER_SIZE];
extern IEC_ULINT *lint_memory[BUFFER_SIZE];
extern IEC_BOOL *bool_memory[BUFFER_SIZE][8];

static uint8_t prev_om[BUFFER_SIZE][IMAGE_DELTA_OM_ROW_BYTES];
static int baseline_valid = 0;

static void export_om_row(size_t i, uint8_t *p)
{
    int b;

    for (b = 0; b < 8; b++)
    {
        *p++ = *bool_output[i][b];
    }
    *p++ = *byte_output[i];
    memcpy(p, int_output[i], sizeof(IEC_UINT));
    p += sizeof(IEC_UINT);
    memcpy(p, dint_output[i], sizeof(IEC_UDINT));
    p += sizeof(IEC_UDINT);
    memcpy(p, lint_output[i], sizeof(IEC_ULINT));
    p += sizeof(IEC_ULINT);
    memcpy(p, int_memory[i], sizeof(IEC_UINT));
    p += sizeof(IEC_UINT);
    memcpy(p, dint_memory[i], sizeof(IEC_UDINT));
    p += sizeof(IEC_UDINT);
    memcpy(p, lint_memory[i], sizeof(IEC_ULINT));
    p += sizeof(IEC_ULINT);
    for (b = 0; b < 8; b++)
    {
        *p++ = *bool_memory[i][b];
    }
}

static void import_om_row(size_t i, const uint8_t *p)
{
    int b;

    for (b = 0; b < 8; b++)
    {
        *bool_output[i][b] = *p++;
    }
    *byte_output[i] = *p++;
    memcpy(int_output[i], p, sizeof(IEC_UINT));
    p += sizeof(IEC_UINT);
    memcpy(dint_output[i], p, sizeof(IEC_UDINT));
    p += sizeof(IEC_UDINT);
    memcpy(lint_output[i], p, sizeof(IEC_ULINT));
    p += sizeof(IEC_ULINT);
    memcpy(int_memory[i], p, sizeof(IEC_UINT));
    p += sizeof(IEC_UINT);
    memcpy(dint_memory[i], p, sizeof(IEC_UDINT));
    p += sizeof(IEC_UDINT);
    memcpy(lint_memory[i], p, sizeof(IEC_ULINT));
    p += sizeof(IEC_ULINT);
    for (b = 0; b < 8; b++)
    {
        *bool_memory[i][b] = *p++;
    }
}

void image_delta_reset_baseline(void)
{
    baseline_valid = 0;
}

int image_delta_is_delta_payload(const uint8_t *buf, size_t len)
{
    if (!buf || len < 4)
    {
        return 0;
    }
    return buf[0] == 'O' && buf[1] == 'P' && buf[2] == 'D' && buf[3] == 'L';
}

int image_delta_export_scan_end(uint8_t *out_buf, size_t out_cap, size_t *out_len,
                                const scan_sync_meta_t *meta)
{
    size_t i;
    size_t off;
    uint32_t changed_count = 0;
    uint8_t bitmap[IMAGE_DELTA_DIRTY_BITMAP_BYTES];

    if (!out_buf || !out_len || !meta)
    {
        return -1;
    }

    memset(bitmap, 0, sizeof(bitmap));

    for (i = 0; i < (size_t)BUFFER_SIZE; i++)
    {
        uint8_t row[IMAGE_DELTA_OM_ROW_BYTES];
        export_om_row(i, row);
        if (!baseline_valid || memcmp(row, prev_om[i], IMAGE_DELTA_OM_ROW_BYTES) != 0)
        {
            bitmap[i / 8] |= (uint8_t)(1U << (i % 8));
            changed_count++;
            memcpy(prev_om[i], row, IMAGE_DELTA_OM_ROW_BYTES);
        }
    }
    baseline_valid = 1;

    if (out_cap < 168U + (size_t)changed_count * (2U + IMAGE_DELTA_OM_ROW_BYTES))
    {
        return -1;
    }

    out_buf[0] = 'O';
    out_buf[1] = 'P';
    out_buf[2] = 'D';
    out_buf[3] = 'L';
    out_buf[4] = (uint8_t)(IMAGE_DELTA_VERSION & 0xFF);
    out_buf[5] = (uint8_t)((IMAGE_DELTA_VERSION >> 8) & 0xFF);
    out_buf[6] = 0;
    out_buf[7] = 0;
    memcpy(out_buf + 8, &meta->scan_counter, sizeof(uint64_t));
    {
        uint64_t tick_u64 = (uint64_t)meta->tick;
        memcpy(out_buf + 16, &tick_u64, sizeof(uint64_t));
    }
    out_buf[24] = meta->phase;
    memset(out_buf + 25, 0, 3);
    memcpy(out_buf + 28, &changed_count, sizeof(uint32_t));
    memset(out_buf + 32, 0, 4);
    memcpy(out_buf + 36, &meta->timestamp_ns, sizeof(uint64_t));
    memcpy(out_buf + 44, bitmap, IMAGE_DELTA_DIRTY_BITMAP_BYTES);

    off = 44U + IMAGE_DELTA_DIRTY_BITMAP_BYTES;
    for (i = 0; i < (size_t)BUFFER_SIZE; i++)
    {
        if ((bitmap[i / 8] & (uint8_t)(1U << (i % 8))) == 0)
        {
            continue;
        }
        uint16_t idx = (uint16_t)i;
        memcpy(out_buf + off, &idx, sizeof(uint16_t));
        off += 2;
        memcpy(out_buf + off, prev_om[i], IMAGE_DELTA_OM_ROW_BYTES);
        off += IMAGE_DELTA_OM_ROW_BYTES;
    }

    *out_len = off;
    return 0;
}

int image_delta_import(const uint8_t *buf, size_t buf_len)
{
    size_t i;
    size_t off;
    uint32_t changed_count;

    if (!image_delta_is_delta_payload(buf, buf_len) ||
        buf_len < 44U + IMAGE_DELTA_DIRTY_BITMAP_BYTES)
    {
        return -1;
    }

    if (buf[4] != (uint8_t)(IMAGE_DELTA_VERSION & 0xFF) ||
        buf[5] != (uint8_t)((IMAGE_DELTA_VERSION >> 8) & 0xFF))
    {
        return -1;
    }

    memcpy(&changed_count, buf + 28, sizeof(uint32_t));
    off = 44U + IMAGE_DELTA_DIRTY_BITMAP_BYTES;
    for (i = 0; i < changed_count; i++)
    {
        uint16_t idx;
        if (off + 2U + IMAGE_DELTA_OM_ROW_BYTES > buf_len)
        {
            return -1;
        }
        memcpy(&idx, buf + off, sizeof(uint16_t));
        off += 2;
        if (idx >= BUFFER_SIZE)
        {
            return -1;
        }
        import_om_row((size_t)idx, buf + off);
        off += IMAGE_DELTA_OM_ROW_BYTES;
    }
    if (off != buf_len)
    {
        return -1;
    }
    return 0;
}
