#include "redundancy_udp.h"

#include <arpa/inet.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../drivers/plugin_driver.h"
#include "image_snapshot.h"
#include "image_snapshot_delta.h"
#include "redundancy_image_udp.h"
#include "redundancy_pending.h"
#include "scan_sync.h"

#define REDUNDANCY_UDP_PORT 57576

static uint64_t read_be64_local(const uint8_t *p)
{
    uint64_t v = 0;
    int i;
    for (i = 0; i < 8; i++)
    {
        v = (v << 8) | (uint64_t)p[i];
    }
    return v;
}

typedef struct
{
    int enabled;
    char mode[48];
    char local_ip[64];
    char peer_ip[64];
    uint16_t udp_port;
} redundancy_udp_config_t;

static redundancy_udp_config_t g_cfg;
static pthread_t g_thread;
static volatile int g_running = 0;
static volatile int g_stop    = 0;
static pthread_mutex_t g_stat_mutex = PTHREAD_MUTEX_INITIALIZER;
static uint64_t g_session_id          = 1;
static uint64_t g_frame_seq           = 0;
static uint64_t g_last_applied_seq    = 0;
static uint64_t g_last_rx_frame_seq   = 0;
static uint32_t g_crc_error_count     = 0;
static uint32_t g_apply_error_count   = 0;
static uint32_t g_ack_timeout_count   = 0;

static int parse_json_string(const char *json, const char *key, char *out, size_t out_cap)
{
    char pattern[64];
    const char *p;
    const char *q;
    size_t len;

    snprintf(pattern, sizeof(pattern), "\"%s\"", key);
    p = strstr(json, pattern);
    if (!p)
    {
        return -1;
    }
    p = strchr(p, ':');
    if (!p)
    {
        return -1;
    }
    p = strchr(p, '"');
    if (!p)
    {
        return -1;
    }
    p++;
    q = strchr(p, '"');
    if (!q)
    {
        return -1;
    }
    len = (size_t)(q - p);
    if (len + 1 > out_cap)
    {
        return -1;
    }
    memcpy(out, p, len);
    out[len] = '\0';
    return 0;
}

static int parse_json_bool(const char *json, const char *key, int *out)
{
    const char *p = strstr(json, key);
    if (!p)
    {
        return -1;
    }
    p = strchr(p, ':');
    if (!p)
    {
        return -1;
    }
    p++;
    while (*p == ' ' || *p == '\t')
    {
        p++;
    }
    if (strncmp(p, "true", 4) == 0)
    {
        *out = 1;
        return 0;
    }
    if (strncmp(p, "false", 5) == 0)
    {
        *out = 0;
        return 0;
    }
    return -1;
}

static int peer_addr_matches(const struct sockaddr_in *from, const char *peer_ip)
{
    struct in_addr expected;
    if (!from || !peer_ip)
    {
        return 0;
    }
    if (inet_pton(AF_INET, peer_ip, &expected) != 1)
    {
        return 0;
    }
    return from.sin_addr.s_addr == expected.s_addr;
}

static int bind_udp_socket(const char *local_ip, uint16_t port)
{
    int sock;
    struct sockaddr_in local;

    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0)
    {
        return -1;
    }
    memset(&local, 0, sizeof(local));
    local.sin_family = AF_INET;
    local.sin_port   = htons(port);
    if (inet_pton(AF_INET, local_ip, &local.sin_addr) != 1)
    {
        close(sock);
        return -1;
    }
    if (bind(sock, (struct sockaddr *)&local, sizeof(local)) != 0)
    {
        close(sock);
        return -1;
    }
    return sock;
}

static void *redundancy_udp_master_thread(void *arg)
{
    (void)arg;
    int sock = -1;
    struct sockaddr_in peer;
    uint64_t last_scan = 0;
    uint8_t delta_buf[IMAGE_DELTA_MAX_WIRE_BYTES + 128];

    memset(&peer, 0, sizeof(peer));
    peer.sin_family = AF_INET;
    peer.sin_port   = htons(g_cfg.udp_port);
    inet_pton(AF_INET, g_cfg.peer_ip, &peer.sin_addr);

    while (!g_stop)
    {
        scan_sync_meta_t meta;
        const uint8_t *payload     = NULL;
        size_t payload_len         = 0;
        uint8_t *heap_payload      = NULL;
        size_t delta_len           = 0;
        int use_delta              = 0;

        if (scan_sync_wait_for_end(last_scan, 500, &meta) != 0)
        {
            usleep(10000);
            continue;
        }
        last_scan = meta.scan_counter;

        if (!plugin_driver)
        {
            continue;
        }
        plugin_mutex_take(&plugin_driver->buffer_mutex);
        if (image_delta_export_scan_end(delta_buf, sizeof(delta_buf), &delta_len, &meta) == 0 &&
            delta_len > 0 && delta_len <= IMAGE_DELTA_MAX_WIRE_BYTES)
        {
            payload     = delta_buf;
            payload_len = delta_len;
            use_delta   = 1;
        }
        else
        {
            heap_payload = (uint8_t *)malloc(IMAGE_SNAPSHOT_TOTAL_BYTES);
            if (heap_payload &&
                image_snapshot_export(heap_payload, IMAGE_SNAPSHOT_TOTAL_BYTES, &payload_len) ==
                    0 &&
                payload_len == IMAGE_SNAPSHOT_TOTAL_BYTES)
            {
                payload = heap_payload;
            }
            else
            {
                free(heap_payload);
                heap_payload = NULL;
            }
        }
        plugin_mutex_give(&plugin_driver->buffer_mutex);

        if (!payload)
        {
            continue;
        }

        if (sock < 0)
        {
            sock = bind_udp_socket(g_cfg.local_ip, 0);
            if (sock < 0)
            {
                free(heap_payload);
                continue;
            }
        }

        g_frame_seq++;
        if (g_frame_seq == 0)
        {
            g_session_id++;
            g_frame_seq = 1;
        }

        if (redundancy_image_udp_send_frame(sock, &peer, g_session_id, g_frame_seq, payload,
                                            payload_len) == 0)
        {
            redundancy_image_udp_ack_t ack;
            if (redundancy_image_udp_recv_ack(sock, &peer, g_session_id, g_frame_seq,
                                              REDUNDANCY_IMAGE_UDP_ACK_TIMEOUT_MS, &ack) != 0)
            {
                pthread_mutex_lock(&g_stat_mutex);
                g_ack_timeout_count++;
                pthread_mutex_unlock(&g_stat_mutex);
            }
        }
        (void)use_delta;
        free(heap_payload);
    }
    if (sock >= 0)
    {
        close(sock);
    }
    return NULL;
}

static void *redundancy_udp_standby_thread(void *arg)
{
    (void)arg;
    int sock = -1;
    uint8_t packet[REDUNDANCY_IMAGE_UDP_DATA_HEADER_SIZE +
                   REDUNDANCY_IMAGE_UDP_FRAGMENT_PAYLOAD_MAX + 8];
    redundancy_image_udp_assembler_t assembler;
    struct sockaddr_in peer_expected;

    memset(&peer_expected, 0, sizeof(peer_expected));
    peer_expected.sin_family = AF_INET;
    peer_expected.sin_port   = htons(g_cfg.udp_port);
    inet_pton(AF_INET, g_cfg.peer_ip, &peer_expected.sin_addr);

    redundancy_image_udp_assembler_init(&assembler);

    while (!g_stop)
    {
        struct sockaddr_in from;
        socklen_t fromlen = sizeof(from);
        ssize_t n;
        redundancy_image_udp_fragment_t frag;
        uint8_t parse_status;
        uint8_t asm_status;
        uint64_t frame_seq = 0;
        uint8_t *complete_payload = NULL;
        size_t complete_len = 0;
        int asm_rc;

        if (sock < 0)
        {
            sock = bind_udp_socket(g_cfg.local_ip, g_cfg.udp_port);
            if (sock < 0)
            {
                usleep(50000);
                continue;
            }
        }

        n = recvfrom(sock, packet, sizeof(packet), 0, (struct sockaddr *)&from, &fromlen);
        if (n <= 0)
        {
            continue;
        }
        if (!peer_addr_matches(&from, g_cfg.peer_ip))
        {
            continue;
        }

        if (redundancy_image_udp_parse_fragment(packet, (size_t)n, &frag, &parse_status) != 0)
        {
            uint64_t sid = 0;
            uint64_t fseq = 0;
            if ((size_t)n >= REDUNDANCY_IMAGE_UDP_DATA_HEADER_SIZE)
            {
                sid  = read_be64_local(packet + 8);
                fseq = read_be64_local(packet + 16);
            }
            redundancy_image_udp_send_ack_v2(sock, &from, REDUNDANCY_IMAGE_ACK_STATUS_BAD_HEADER,
                                             sid, fseq, assembler.last_applied_seq);
            continue;
        }

        asm_rc = redundancy_image_udp_assembler_add(&assembler, &frag, &asm_status, &frame_seq,
                                                    &complete_payload, &complete_len);
        if (asm_rc < 0)
        {
            continue;
        }
        if (asm_rc == 0)
        {
            continue;
        }
        if (complete_payload == NULL)
        {
            redundancy_image_udp_send_ack_v2(sock, &from, asm_status, frag.session_id, frame_seq,
                                             assembler.last_applied_seq);
            continue;
        }

        if (asm_status == REDUNDANCY_IMAGE_ACK_STATUS_OK)
        {
            uint8_t ack_status = REDUNDANCY_IMAGE_ACK_STATUS_APPLY_ERROR;
            if (!plugin_driver || !plugin_driver->shadow_standby)
            {
                ack_status = REDUNDANCY_IMAGE_ACK_STATUS_NOT_SHADOW;
            }
            else
            {
                redundancy_pending_set_frame_seq(frame_seq);
                if (redundancy_pending_store(complete_payload, complete_len) == 0)
                {
                    pthread_mutex_lock(&g_stat_mutex);
                    g_last_rx_frame_seq = frame_seq;
                    pthread_mutex_unlock(&g_stat_mutex);
                    if (redundancy_pending_wait_applied(frame_seq, 1600) == 0)
                    {
                        redundancy_image_udp_assembler_record_applied(&assembler,
                                                                      frag.session_id, frame_seq);
                        ack_status = REDUNDANCY_IMAGE_ACK_STATUS_OK;
                        pthread_mutex_lock(&g_stat_mutex);
                        g_last_applied_seq = frame_seq;
                        pthread_mutex_unlock(&g_stat_mutex);
                    }
                    else
                    {
                        ack_status = REDUNDANCY_IMAGE_ACK_STATUS_NOT_READY;
                    }
                }
                else
                {
                    g_apply_error_count++;
                }
            }
            redundancy_image_udp_send_ack_v2(sock, &from, ack_status, frag.session_id, frame_seq,
                                             assembler.last_applied_seq);
        }
        else if (asm_status == REDUNDANCY_IMAGE_ACK_STATUS_CRC_ERROR)
        {
            g_crc_error_count++;
            redundancy_image_udp_send_ack_v2(sock, &from, asm_status, frag.session_id, frame_seq,
                                             assembler.last_applied_seq);
        }
        else
        {
            redundancy_image_udp_send_ack_v2(sock, &from, asm_status, frag.session_id, frame_seq,
                                             assembler.last_applied_seq);
        }
        free(complete_payload);
    }
    if (sock >= 0)
    {
        close(sock);
    }
    return NULL;
}

int redundancy_udp_configure(const char *json)
{
    int enabled = 0;
    if (!json)
    {
        return -1;
    }
    memset(&g_cfg, 0, sizeof(g_cfg));
    if (parse_json_bool(json, "enabled", &enabled) != 0)
    {
        return -1;
    }
    g_cfg.enabled = enabled;
    if (parse_json_string(json, "data_plane_mode", g_cfg.mode, sizeof(g_cfg.mode)) != 0)
    {
        return -1;
    }
    if (parse_json_string(json, "local_heartbeat_ip", g_cfg.local_ip, sizeof(g_cfg.local_ip)) != 0)
    {
        return -1;
    }
    if (parse_json_string(json, "peer_heartbeat_ip", g_cfg.peer_ip, sizeof(g_cfg.peer_ip)) != 0)
    {
        return -1;
    }
    g_cfg.udp_port = REDUNDANCY_UDP_PORT;
    return 0;
}

int redundancy_udp_start(void)
{
    if (!g_cfg.enabled || g_running)
    {
        return -1;
    }
    g_stop = 0;
    if (strstr(g_cfg.mode, "master") != NULL)
    {
        if (pthread_create(&g_thread, NULL, redundancy_udp_master_thread, NULL) != 0)
        {
            return -1;
        }
    }
    else
    {
        if (pthread_create(&g_thread, NULL, redundancy_udp_standby_thread, NULL) != 0)
        {
            return -1;
        }
    }
    g_running = 1;
    return 0;
}

int redundancy_udp_stop(void)
{
    if (!g_running)
    {
        return 0;
    }
    g_stop = 1;
    pthread_join(g_thread, NULL);
    g_running = 0;
    return 0;
}

int redundancy_udp_is_running(void)
{
    return g_running;
}

void redundancy_udp_note_applied(uint64_t seq)
{
    pthread_mutex_lock(&g_stat_mutex);
    g_last_applied_seq = seq;
    pthread_mutex_unlock(&g_stat_mutex);
}

int redundancy_udp_format_status(char *buf, size_t buf_cap)
{
    if (!buf || buf_cap < 64)
    {
        return -1;
    }
    snprintf(buf, buf_cap,
             "REDUNDANCY_SYNC_STATUS:{\"enabled\":%s,\"running\":%s,"
             "\"data_plane_mode\":\"%s\",\"active_session_id\":%llu,"
             "\"last_rx_frame_seq\":%llu,\"last_applied_seq\":%llu,"
             "\"crc_error_count\":%u,\"apply_error_count\":%u,"
             "\"ack_timeout_count\":%u}\n",
             g_cfg.enabled ? "true" : "false", g_running ? "true" : "false", g_cfg.mode,
             (unsigned long long)g_session_id, (unsigned long long)g_last_rx_frame_seq,
             (unsigned long long)g_last_applied_seq, g_crc_error_count, g_apply_error_count,
             g_ack_timeout_count);
    return 0;
}
