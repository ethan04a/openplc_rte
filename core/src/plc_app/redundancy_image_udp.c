#include "redundancy_image_udp.h"

#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "image_snapshot.h"

static uint64_t read_be64(const uint8_t *p)
{
    uint64_t v = 0;
    int i;
    for (i = 0; i < 8; i++)
    {
        v = (v << 8) | (uint64_t)p[i];
    }
    return v;
}

static void write_be64(uint8_t *p, uint64_t v)
{
    int i;
    for (i = 7; i >= 0; i--)
    {
        p[i] = (uint8_t)(v & 0xffU);
        v >>= 8;
    }
}

uint32_t redundancy_image_udp_crc32(const uint8_t *data, size_t len)
{
    uint32_t crc = 0xFFFFFFFFU;
    size_t i;
    int bit;

    if (!data)
    {
        return 0;
    }
    for (i = 0; i < len; i++)
    {
        crc ^= (uint32_t)data[i];
        for (bit = 0; bit < 8; bit++)
        {
            if (crc & 1U)
            {
                crc = (crc >> 1) ^ 0xEDB88320U;
            }
            else
            {
                crc >>= 1;
            }
        }
    }
    return ~crc;
}

uint16_t redundancy_image_udp_fragment_count(size_t total_len)
{
    if (total_len == 0)
    {
        return 0;
    }
    return (uint16_t)((total_len + REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX - 1) /
                      REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX);
}

static int pack_data_header(uint8_t *hdr, uint64_t session_id, uint64_t frame_seq,
                            uint16_t fragment_index, uint16_t fragment_count,
                            uint32_t fragment_offset, uint32_t total_len, uint32_t payload_crc32,
                            uint16_t frag_payload_len)
{
    memcpy(hdr, REDUNDANCY_IMAGE_UDP_DATA_MAGIC, 4);
    hdr[4] = (uint8_t)((REDUNDANCY_IMAGE_UDP_PROTOCOL_VERSION >> 8) & 0xff);
    hdr[5] = (uint8_t)(REDUNDANCY_IMAGE_UDP_PROTOCOL_VERSION & 0xff);
    hdr[6] = REDUNDANCY_IMAGE_UDP_DATA_FRAGMENT;
    hdr[7] = 0;
    write_be64(hdr + 8, session_id);
    write_be64(hdr + 16, frame_seq);
    hdr[24] = (uint8_t)((fragment_index >> 8) & 0xff);
    hdr[25] = (uint8_t)(fragment_index & 0xff);
    hdr[26] = (uint8_t)((fragment_count >> 8) & 0xff);
    hdr[27] = (uint8_t)(fragment_count & 0xff);
    hdr[28] = (uint8_t)((fragment_offset >> 24) & 0xff);
    hdr[29] = (uint8_t)((fragment_offset >> 16) & 0xff);
    hdr[30] = (uint8_t)((fragment_offset >> 8) & 0xff);
    hdr[31] = (uint8_t)(fragment_offset & 0xff);
    hdr[32] = (uint8_t)((total_len >> 24) & 0xff);
    hdr[33] = (uint8_t)((total_len >> 16) & 0xff);
    hdr[34] = (uint8_t)((total_len >> 8) & 0xff);
    hdr[35] = (uint8_t)(total_len & 0xff);
    hdr[36] = (uint8_t)((payload_crc32 >> 24) & 0xff);
    hdr[37] = (uint8_t)((payload_crc32 >> 16) & 0xff);
    hdr[38] = (uint8_t)((payload_crc32 >> 8) & 0xff);
    hdr[39] = (uint8_t)(payload_crc32 & 0xff);
    (void)frag_payload_len;
    return 0;
}

int redundancy_image_udp_send_frame(int sock, const struct sockaddr_in *peer, uint64_t session_id,
                                    uint64_t frame_seq, const uint8_t *payload, size_t len)
{
    uint8_t packet[REDUNDANCY_IMAGE_UDP_DATA_HEADER_SIZE +
                   REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX];
    uint32_t crc;
    uint16_t frag_count;
    uint16_t i;

    if (!peer || !payload || len == 0 || len > IMAGE_SNAPSHOT_TOTAL_BYTES)
    {
        return -1;
    }
    frag_count = redundancy_image_udp_fragment_count(len);
    if (frag_count == 0 || frag_count > REDUNDANCY_IMAGE_UDP_MAX_FRAGMENTS)
    {
        return -1;
    }
    crc = redundancy_image_udp_crc32(payload, len);
    for (i = 0; i < frag_count; i++)
    {
        uint32_t offset = (uint32_t)i * REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX;
        uint32_t remain = (uint32_t)len - offset;
        uint16_t chunk = (uint16_t)(remain > REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX
                                        ? REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX
                                        : remain);
        ssize_t sent;

        pack_data_header(packet, session_id, frame_seq, i, frag_count, offset, (uint32_t)len, crc,
                         chunk);
        memcpy(packet + REDUNDANCY_IMAGE_UDP_DATA_HEADER_SIZE, payload + offset, chunk);
        sent = sendto(sock, packet, REDUNDANCY_IMAGE_UDP_DATA_HEADER_SIZE + chunk, 0,
                      (const struct sockaddr *)peer, sizeof(*peer));
        if (sent < 0)
        {
            return -1;
        }
    }
    return 0;
}

int redundancy_image_udp_parse_fragment(const uint8_t *packet, size_t packet_len,
                                      redundancy_image_udp_fragment_t *out, uint8_t *status_out)
{
    uint16_t version;
    uint16_t fragment_index;
    uint16_t fragment_count;
    uint32_t fragment_offset;
    uint32_t total_len;
    uint32_t payload_crc32;
    uint16_t expected_count;
    size_t expected_payload;

    if (status_out)
    {
        *status_out = REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER;
    }
    if (!packet || packet_len < REDUNDANCY_IMAGE_UDP_DATA_HEADER_SIZE || !out)
    {
        return -1;
    }
    if (memcmp(packet, REDUNDANCY_IMAGE_UDP_DATA_MAGIC, 4) != 0)
    {
        return -1;
    }
    version = (uint16_t)(((uint16_t)packet[4] << 8) | packet[5]);
    if (version != REDUNDANCY_IMAGE_UDP_PROTOCOL_VERSION || packet[6] != REDUNDANCY_IMAGE_UDP_DATA_FRAGMENT ||
        packet[7] != 0)
    {
        return -1;
    }
    out->session_id = read_be64(packet + 8);
    out->frame_seq  = read_be64(packet + 16);
    fragment_index  = (uint16_t)(((uint16_t)packet[24] << 8) | packet[25]);
    fragment_count  = (uint16_t)(((uint16_t)packet[26] << 8) | packet[27]);
    fragment_offset =
        ((uint32_t)packet[28] << 24) | ((uint32_t)packet[29] << 16) | ((uint32_t)packet[30] << 8) |
        (uint32_t)packet[31];
    total_len = ((uint32_t)packet[32] << 24) | ((uint32_t)packet[33] << 16) |
                ((uint32_t)packet[34] << 8) | (uint32_t)packet[35];
    payload_crc32 = ((uint32_t)packet[36] << 24) | ((uint32_t)packet[37] << 16) |
                    ((uint32_t)packet[38] << 8) | (uint32_t)packet[39];
    out->payload     = packet + REDUNDANCY_IMAGE_UDP_DATA_HEADER_SIZE;
    out->payload_len = packet_len - REDUNDANCY_IMAGE_UDP_DATA_HEADER_SIZE;

    if (total_len == 0 || total_len > IMAGE_SNAPSHOT_TOTAL_BYTES || fragment_count == 0)
    {
        return -1;
    }
    expected_count = redundancy_image_udp_fragment_count(total_len);
    if (fragment_count != expected_count || fragment_index >= fragment_count)
    {
        return -1;
    }
    if (fragment_offset != (uint32_t)fragment_index * REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX ||
        fragment_offset >= total_len)
    {
        return -1;
    }
    expected_payload =
        (size_t)((total_len - fragment_offset) > REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX
                     ? REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX
                     : (total_len - fragment_offset));
    if (out->payload_len != expected_payload)
    {
        return -1;
    }
    out->fragment_index  = fragment_index;
    out->fragment_count  = fragment_count;
    out->fragment_offset = fragment_offset;
    out->total_len       = total_len;
    out->payload_crc32   = payload_crc32;
    if (status_out)
    {
        *status_out = REDUNDANCY_IMAGE_ACK_STATUS_OK;
    }
    return 0;
}

void redundancy_image_udp_assembler_init(redundancy_image_udp_assembler_t *asm_)
{
    if (!asm_)
    {
        return;
    }
    memset(asm_, 0, sizeof(*asm_));
}

static void assembler_clear_pending(redundancy_image_udp_assembler_t *asm_)
{
    if (asm_->pending_payload)
    {
        free(asm_->pending_payload);
        asm_->pending_payload = NULL;
    }
    asm_->pending_active         = 0;
    asm_->pending_frag_received  = 0;
    asm_->pending_fragment_count = 0;
}

void redundancy_image_udp_assembler_record_applied(redundancy_image_udp_assembler_t *asm_,
                                                   uint64_t session_id, uint64_t frame_seq)
{
    if (!asm_)
    {
        return;
    }
    if (session_id > asm_->active_session_id)
    {
        asm_->active_session_id = session_id;
        asm_->last_applied_seq  = 0;
    }
    if (session_id == asm_->active_session_id && frame_seq > asm_->last_applied_seq)
    {
        asm_->last_applied_seq = frame_seq;
    }
}

int redundancy_image_udp_assembler_add(
    redundancy_image_udp_assembler_t *asm_, const redundancy_image_udp_fragment_t *frag,
    uint8_t *status_out, uint64_t *frame_seq_out, uint8_t **payload_out, size_t *payload_len_out)
{
    uint32_t calc_crc;

    if (!asm_ || !frag || !status_out || !frame_seq_out)
    {
        return -1;
    }
    *status_out = REDUNDANCY_IMAGE_ACK_STATUS_OK;
    if (asm_->active_session_id && frag->session_id < asm_->active_session_id)
    {
        return 0;
    }
    if (frag->session_id > asm_->active_session_id)
    {
        asm_->active_session_id = frag->session_id;
        asm_->last_applied_seq  = 0;
        assembler_clear_pending(asm_);
    }
    if (frag->frame_seq <= asm_->last_applied_seq)
    {
        *status_out    = REDUNDANCY_IMAGE_ACK_STATUS_OLD_SEQ;
        *frame_seq_out = frag->frame_seq;
        return 1;
    }
    if (asm_->pending_active)
    {
        if (frag->session_id != asm_->pending_session_id)
        {
            assembler_clear_pending(asm_);
        }
        else if (frag->frame_seq < asm_->pending_frame_seq)
        {
            *status_out    = REDUNDANCY_IMAGE_ACK_STATUS_OLD_SEQ;
            *frame_seq_out = frag->frame_seq;
            return 1;
        }
        else if (frag->frame_seq > asm_->pending_frame_seq)
        {
            assembler_clear_pending(asm_);
        }
    }
    if (!asm_->pending_active)
    {
        asm_->pending_payload = (uint8_t *)malloc(frag->total_len);
        if (!asm_->pending_payload)
        {
            *status_out = REDUNDANCY_IMAGE_ACK_STATUS_APPLY_ERROR;
            return 1;
        }
        memset(asm_->pending_payload, 0, frag->total_len);
        asm_->pending_active           = 1;
        asm_->pending_session_id       = frag->session_id;
        asm_->pending_frame_seq        = frag->frame_seq;
        asm_->pending_fragment_count   = frag->fragment_count;
        asm_->pending_total_len        = frag->total_len;
        asm_->pending_payload_crc32    = frag->payload_crc32;
        asm_->pending_frag_received    = 0;
    }
    if (asm_->pending_fragment_count != frag->fragment_count ||
        asm_->pending_total_len != frag->total_len ||
        asm_->pending_payload_crc32 != frag->payload_crc32)
    {
        assembler_clear_pending(asm_);
        *status_out = REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER;
        return 1;
    }
    if (asm_->pending_frag_len[frag->fragment_index] != 0)
    {
        if (asm_->pending_frag_len[frag->fragment_index] != frag->payload_len ||
            memcmp(asm_->pending_payload + frag->fragment_offset, frag->payload,
                   frag->payload_len) != 0)
        {
            assembler_clear_pending(asm_);
            *status_out = REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER;
            return 1;
        }
    }
    else
    {
        memcpy(asm_->pending_payload + frag->fragment_offset, frag->payload, frag->payload_len);
        asm_->pending_frag_len[frag->fragment_index] = (uint16_t)frag->payload_len;
        asm_->pending_frag_received++;
    }
    if (asm_->pending_frag_received < asm_->pending_fragment_count)
    {
        return 0;
    }
    calc_crc = redundancy_image_udp_crc32(asm_->pending_payload, asm_->pending_total_len);
    if (calc_crc != asm_->pending_payload_crc32)
    {
        assembler_clear_pending(asm_);
        *status_out    = REDUNDANCY_IMAGE_ACK_STATUS_CRC_ERROR;
        *frame_seq_out = frag->frame_seq;
        return 1;
    }
    if (payload_out)
    {
        *payload_out = asm_->pending_payload;
    }
    if (payload_len_out)
    {
        *payload_len_out = asm_->pending_total_len;
    }
    *frame_seq_out = asm_->pending_frame_seq;
    asm_->pending_payload       = NULL;
    asm_->pending_active        = 0;
    asm_->pending_frag_received = 0;
    return 1;
}

int redundancy_image_udp_send_ack_v2(int sock, const struct sockaddr_in *peer, uint8_t status,
                                     uint64_t session_id, uint64_t ack_frame_seq,
                                     uint64_t applied_seq)
{
    uint8_t packet[REDUNDANCY_IMAGE_UDP_ACK_HEADER_V2_SIZE];
    struct timespec now;
    uint64_t ts_ns;

    if (!peer)
    {
        return -1;
    }
    clock_gettime(CLOCK_MONOTONIC, &now);
    ts_ns = (uint64_t)now.tv_sec * 1000000000ULL + (uint64_t)now.tv_nsec;
    memcpy(packet, REDUNDANCY_IMAGE_UDP_ACK_MAGIC, 4);
    packet[4] = (uint8_t)((REDUNDANCY_IMAGE_UDP_PROTOCOL_VERSION_V2 >> 8) & 0xff);
    packet[5] = (uint8_t)(REDUNDANCY_IMAGE_UDP_PROTOCOL_VERSION_V2 & 0xff);
    packet[6] = REDUNDANCY_IMAGE_UDP_ACK_FRAME;
    packet[7] = status;
    write_be64(packet + 8, session_id);
    write_be64(packet + 16, ack_frame_seq);
    write_be64(packet + 24, applied_seq);
    write_be64(packet + 32, ts_ns);
    return sendto(sock, packet, sizeof(packet), 0, (const struct sockaddr *)peer,
                  sizeof(*peer)) < 0
               ? -1
               : 0;
}

int redundancy_image_udp_recv_ack(int sock, const struct sockaddr_in *expected_peer,
                                  uint64_t session_id, uint64_t frame_seq, int timeout_ms,
                                  redundancy_image_udp_ack_t *ack_out)
{
    uint8_t buf[128];
    struct sockaddr_in from;
    socklen_t fromlen = sizeof(from);
    struct timeval tv;
    fd_set rfds;
    ssize_t n;

    if (!expected_peer || !ack_out)
    {
        return -1;
    }
    tv.tv_sec  = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    FD_ZERO(&rfds);
    FD_SET(sock, &rfds);
    if (select(sock + 1, &rfds, NULL, NULL, &tv) <= 0)
    {
        return -1;
    }
    n = recvfrom(sock, buf, sizeof(buf), 0, (struct sockaddr *)&from, &fromlen);
    if (n < (ssize_t)REDUNDANCY_IMAGE_UDP_ACK_HEADER_V2_SIZE)
    {
        return -1;
    }
    if (from.sin_addr.s_addr != expected_peer->sin_addr.s_addr)
    {
        return -1;
    }
    if (memcmp(buf, REDUNDANCY_IMAGE_UDP_ACK_MAGIC, 4) != 0)
    {
        return -1;
    }
    if (((uint16_t)buf[4] << 8) | buf[5]) != REDUNDANCY_IMAGE_UDP_PROTOCOL_VERSION_V2 ||
        buf[6] != REDUNDANCY_IMAGE_UDP_ACK_FRAME)
    {
        return -1;
    }
    ack_out->status        = buf[7];
    ack_out->session_id    = read_be64(buf + 8);
    ack_out->ack_frame_seq = read_be64(buf + 16);
    ack_out->applied_seq   = read_be64(buf + 24);
    ack_out->timestamp_ns  = read_be64(buf + 32);
    if (ack_out->session_id != session_id || ack_out->ack_frame_seq != frame_seq)
    {
        return -1;
    }
    return 0;
}
