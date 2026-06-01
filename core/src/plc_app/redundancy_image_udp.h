#ifndef REDUNDANCY_IMAGE_UDP_H
#define REDUNDANCY_IMAGE_UDP_H

#include <netinet/in.h>
#include <stddef.h>
#include <stdint.h>

#define REDUNDANCY_IMAGE_UDP_DATA_MAGIC "OPUD"
#define REDUNDANCY_IMAGE_UDP_ACK_MAGIC "OPAK"
#define REDUNDANCY_IMAGE_UDP_PROTOCOL_VERSION 1U
#define REDUNDANCY_IMAGE_UDP_PROTOCOL_VERSION_V2 2U
#define REDUNDANCY_IMAGE_UDP_DATA_FRAGMENT 1U
#define REDUNDANCY_IMAGE_UDP_ACK_FRAME 2U
#define REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX 1200U
#define REDUNDANCY_IMAGE_UDP_DATA_HEADER_SIZE 40U
#define REDUNDANCY_IMAGE_UDP_ACK_HEADER_V2_SIZE 40U
#define REDUNDANCY_IMAGE_UDP_FRAME_TIMEOUT_MS 15
#define REDUNDANCY_IMAGE_UDP_ACK_TIMEOUT_MS 5
#define REDUNDANCY_IMAGE_UDP_MAX_FRAGMENTS 64U

#define REDUNDANCY_IMAGE_ACK_STATUS_OK 0U
#define REDUNDANCY_IMAGE_ACK_STATUS_CRC_ERROR 1U
#define REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER 2U
#define REDUNDANCY_IMAGE_ACK_STATUS_NOT_READY 3U
#define REDUNDANCY_IMAGE_ACK_STATUS_NOT_SHADOW 4U
#define REDUNDANCY_IMAGE_ACK_STATUS_APPLY_ERROR 5U
#define REDUNDANCY_IMAGE_ACK_STATUS_OLD_SEQ 6U
#define REDUNDANCY_IMAGE_ACK_STATUS_FRAME_INCOMPLETE 7U

typedef struct
{
    uint64_t session_id;
    uint64_t frame_seq;
    uint16_t fragment_index;
    uint16_t fragment_count;
    uint32_t fragment_offset;
    uint32_t total_len;
    uint32_t payload_crc32;
    const uint8_t *payload;
    size_t payload_len;
} redundancy_image_udp_fragment_t;

typedef struct
{
    uint8_t status;
    uint64_t session_id;
    uint64_t ack_frame_seq;
    uint64_t applied_seq;
    uint64_t timestamp_ns;
} redundancy_image_udp_ack_t;

typedef struct
{
    uint64_t active_session_id;
    uint64_t last_applied_seq;
    uint64_t pending_session_id;
    uint64_t pending_frame_seq;
    uint16_t pending_fragment_count;
    uint32_t pending_total_len;
    uint32_t pending_payload_crc32;
    int pending_active;
    int pending_frag_received;
    uint8_t *pending_payload;
    uint16_t pending_frag_len[REDUNDANCY_IMAGE_UDP_MAX_FRAGMENTS];
} redundancy_image_udp_assembler_t;

uint32_t redundancy_image_udp_crc32(const uint8_t *data, size_t len);

uint16_t redundancy_image_udp_fragment_count(size_t total_len);

int redundancy_image_udp_send_frame(int sock, const struct sockaddr_in *peer, uint64_t session_id,
                                    uint64_t frame_seq, const uint8_t *payload, size_t len);

int redundancy_image_udp_parse_fragment(const uint8_t *packet, size_t packet_len,
                                      redundancy_image_udp_fragment_t *out, uint8_t *status_out);

void redundancy_image_udp_assembler_init(redundancy_image_udp_assembler_t *asm_);

void redundancy_image_udp_assembler_record_applied(redundancy_image_udp_assembler_t *asm_,
                                                   uint64_t session_id, uint64_t frame_seq);

/**
 * Add fragment. Returns: status_out set, payload_out optional complete frame.
 * Return 0 = incomplete, 1 = complete (check status_out), -1 = parse error.
 */
int redundancy_image_udp_assembler_add(
    redundancy_image_udp_assembler_t *asm_, const redundancy_image_udp_fragment_t *frag,
    uint8_t *status_out, uint64_t *frame_seq_out, uint8_t **payload_out, size_t *payload_len_out);

int redundancy_image_udp_send_ack_v2(int sock, const struct sockaddr_in *peer, uint8_t status,
                                     uint64_t session_id, uint64_t ack_frame_seq,
                                     uint64_t applied_seq);

int redundancy_image_udp_recv_ack(int sock, const struct sockaddr_in *expected_peer,
                                  uint64_t session_id, uint64_t frame_seq, int timeout_ms,
                                  redundancy_image_udp_ack_t *ack_out);

#endif /* REDUNDANCY_IMAGE_UDP_H */
