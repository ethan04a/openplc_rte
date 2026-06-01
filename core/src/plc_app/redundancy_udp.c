#include "redundancy_udp.h"

#include <arpa/inet.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../drivers/plugin_driver.h"
#include "image_snapshot_delta.h"
#include "redundancy_pending.h"
#include "scan_sync.h"

#define REDUNDANCY_UDP_PORT 57576

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
    char pattern[64];
    const char *p;
    p = strstr(json, key);
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

static void *redundancy_udp_master_thread(void *arg)
{
    (void)arg;
    int sock = -1;
    struct sockaddr_in peer;
    uint64_t last_scan = 0;

    memset(&peer, 0, sizeof(peer));
    peer.sin_family = AF_INET;
    peer.sin_port   = htons(g_cfg.udp_port);
    inet_pton(AF_INET, g_cfg.peer_ip, &peer.sin_addr);

    while (!g_stop)
    {
        scan_sync_meta_t meta;
        uint8_t payload[IMAGE_DELTA_MAX_WIRE_BYTES];
        size_t len = 0;

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
        if (image_delta_export_scan_end(payload, sizeof(payload), &len, &meta) != 0)
        {
            plugin_mutex_give(&plugin_driver->buffer_mutex);
            continue;
        }
        plugin_mutex_give(&plugin_driver->buffer_mutex);

        if (sock < 0)
        {
            sock = socket(AF_INET, SOCK_DGRAM, 0);
            if (sock < 0)
            {
                continue;
            }
            struct sockaddr_in local;
            memset(&local, 0, sizeof(local));
            local.sin_family = AF_INET;
            local.sin_port   = htons(0);
            inet_pton(AF_INET, g_cfg.local_ip, &local.sin_addr);
            bind(sock, (struct sockaddr *)&local, sizeof(local));
        }
        sendto(sock, payload, len, 0, (struct sockaddr *)&peer, sizeof(peer));
        g_frame_seq++;
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
    uint8_t buf[2048];

    while (!g_stop)
    {
        struct sockaddr_in local;
        struct sockaddr_in from;
        socklen_t fromlen = sizeof(from);
        ssize_t n;

        if (sock < 0)
        {
            sock = socket(AF_INET, SOCK_DGRAM, 0);
            if (sock < 0)
            {
                usleep(50000);
                continue;
            }
            memset(&local, 0, sizeof(local));
            local.sin_family = AF_INET;
            local.sin_port   = htons(g_cfg.udp_port);
            inet_pton(AF_INET, g_cfg.local_ip, &local.sin_addr);
            bind(sock, (struct sockaddr *)&local, sizeof(local));
        }

        n = recvfrom(sock, buf, sizeof(buf), 0, (struct sockaddr *)&from, &fromlen);
        if (n <= 0)
        {
            continue;
        }
        if (image_delta_is_delta_payload(buf, (size_t)n))
        {
            redundancy_pending_set_frame_seq(++g_last_rx_frame_seq);
            if (redundancy_pending_store(buf, (size_t)n) == 0)
            {
                pthread_mutex_lock(&g_stat_mutex);
                g_last_rx_frame_seq = g_last_rx_frame_seq;
                pthread_mutex_unlock(&g_stat_mutex);
            }
            else
            {
                g_apply_error_count++;
            }
        }
    }
    if (sock >= 0)
    {
        close(sock);
    }
    return NULL;
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
             "\"data_plane_mode\":\"%s\",\"last_rx_frame_seq\":%llu,"
             "\"last_applied_seq\":%llu,\"crc_error_count\":%u,"
             "\"apply_error_count\":%u}\n",
             g_cfg.enabled ? "true" : "false", g_running ? "true" : "false", g_cfg.mode,
             (unsigned long long)g_last_rx_frame_seq, (unsigned long long)g_last_applied_seq,
             g_crc_error_count, g_apply_error_count);
    return 0;
}
