#include "image_snapshot.h"

#include <string.h>

#include "../lib/iec_types.h"

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

static void export_row(size_t i, uint8_t *p)
{
    int b;
    for (b = 0; b < 8; b++)
    {
        *p++ = *bool_input[i][b];
    }
    for (b = 0; b < 8; b++)
    {
        *p++ = *bool_output[i][b];
    }
    *p++ = *byte_input[i];
    *p++ = *byte_output[i];
    memcpy(p, int_input[i], sizeof(IEC_UINT));
    p += sizeof(IEC_UINT);
    memcpy(p, int_output[i], sizeof(IEC_UINT));
    p += sizeof(IEC_UINT);
    memcpy(p, dint_input[i], sizeof(IEC_UDINT));
    p += sizeof(IEC_UDINT);
    memcpy(p, dint_output[i], sizeof(IEC_UDINT));
    p += sizeof(IEC_UDINT);
    memcpy(p, lint_input[i], sizeof(IEC_ULINT));
    p += sizeof(IEC_ULINT);
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

static void import_row(size_t i, const uint8_t *p)
{
    int b;
    for (b = 0; b < 8; b++)
    {
        *bool_input[i][b] = *p++;
    }
    for (b = 0; b < 8; b++)
    {
        *bool_output[i][b] = *p++;
    }
    *byte_input[i]  = *p++;
    *byte_output[i] = *p++;
    memcpy(int_input[i], p, sizeof(IEC_UINT));
    p += sizeof(IEC_UINT);
    memcpy(int_output[i], p, sizeof(IEC_UINT));
    p += sizeof(IEC_UINT);
    memcpy(dint_input[i], p, sizeof(IEC_UDINT));
    p += sizeof(IEC_UDINT);
    memcpy(dint_output[i], p, sizeof(IEC_UDINT));
    p += sizeof(IEC_UDINT);
    memcpy(lint_input[i], p, sizeof(IEC_ULINT));
    p += sizeof(IEC_ULINT);
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

int image_snapshot_export(uint8_t *buf, size_t buf_cap, size_t *out_len)
{
    size_t i;

    if (!buf || !out_len || buf_cap < IMAGE_SNAPSHOT_TOTAL_BYTES)
    {
        return -1;
    }

    for (i = 0; i < (size_t)BUFFER_SIZE; i++)
    {
        export_row(i, buf + i * (size_t)IMAGE_SNAPSHOT_ROW_BYTES);
    }
    *out_len = IMAGE_SNAPSHOT_TOTAL_BYTES;
    return 0;
}

int image_snapshot_import(const uint8_t *buf, size_t buf_len)
{
    size_t i;

    if (!buf || buf_len != IMAGE_SNAPSHOT_TOTAL_BYTES)
    {
        return -1;
    }

    for (i = 0; i < (size_t)BUFFER_SIZE; i++)
    {
        import_row(i, buf + i * (size_t)IMAGE_SNAPSHOT_ROW_BYTES);
    }
    return 0;
}
