#include "redundancy_pending.h"

#include <pthread.h>
#include <string.h>

#include "image_snapshot.h"
#include "image_snapshot_delta.h"
#include "scan_sync.h"
#include "../drivers/plugin_driver.h"

void redundancy_udp_note_applied(uint64_t seq);

extern plugin_driver_t *plugin_driver;

static pthread_mutex_t pending_mutex = PTHREAD_MUTEX_INITIALIZER;
static uint8_t pending_buf[REDUNDANCY_PENDING_MAX_BYTES];
static size_t pending_len = 0;
static int pending_ready = 0;
static uint64_t pending_frame_seq = 0;
static uint64_t last_applied_frame_seq = 0;

void redundancy_pending_init(void)
{
    pthread_mutex_lock(&pending_mutex);
    pending_len   = 0;
    pending_ready = 0;
    pending_frame_seq = 0;
    last_applied_frame_seq = 0;
    pthread_mutex_unlock(&pending_mutex);
}

void redundancy_pending_set_frame_seq(uint64_t seq)
{
    pthread_mutex_lock(&pending_mutex);
    pending_frame_seq = seq;
    pthread_mutex_unlock(&pending_mutex);
}

uint64_t redundancy_pending_alloc_frame_seq(void)
{
    uint64_t seq;
    pthread_mutex_lock(&pending_mutex);
    pending_frame_seq++;
    seq = pending_frame_seq;
    pthread_mutex_unlock(&pending_mutex);
    return seq;
}

int redundancy_pending_wait_applied(uint64_t want, int timeout_ms)
{
    uint64_t start_seen = scan_sync_get_start_counter();
    int attempts        = 0;
    int per_wait        = timeout_ms > 0 ? timeout_ms / 8 : 200;
    if (per_wait < 50)
    {
        per_wait = 50;
    }

    if (redundancy_pending_last_applied_seq() >= want)
    {
        return 0;
    }
    while (attempts < 8 && redundancy_pending_last_applied_seq() < want)
    {
        if (scan_sync_wait_for_start(start_seen, per_wait) != 0)
        {
            break;
        }
        start_seen = scan_sync_get_start_counter();
        attempts++;
    }
    return redundancy_pending_last_applied_seq() >= want ? 0 : -1;
}

int redundancy_pending_store(const uint8_t *buf, size_t len)
{
    if (!buf || len == 0 || len > REDUNDANCY_PENDING_MAX_BYTES)
    {
        return -1;
    }
    pthread_mutex_lock(&pending_mutex);
    memcpy(pending_buf, buf, len);
    pending_len   = len;
    pending_ready = 1;
    pthread_mutex_unlock(&pending_mutex);
    return 0;
}

int redundancy_pending_has_pending(void)
{
    int ready;
    pthread_mutex_lock(&pending_mutex);
    ready = pending_ready;
    pthread_mutex_unlock(&pending_mutex);
    return ready;
}

uint64_t redundancy_pending_last_applied_seq(void)
{
    uint64_t seq;
    pthread_mutex_lock(&pending_mutex);
    seq = last_applied_frame_seq;
    pthread_mutex_unlock(&pending_mutex);
    return seq;
}

int redundancy_pending_apply_at_barrier(void)
{
    uint8_t local[REDUNDANCY_PENDING_MAX_BYTES];
    size_t len = 0;
    int ready = 0;
    uint64_t frame_seq = 0;
    int applied = 0;
    int rc = -1;

    if (!plugin_driver)
    {
        return 0;
    }

    pthread_mutex_lock(&pending_mutex);
    if (pending_ready)
    {
        len = pending_len;
        memcpy(local, pending_buf, len);
        frame_seq = pending_frame_seq;
        ready = 1;
        pending_ready = 0;
    }
    pthread_mutex_unlock(&pending_mutex);

    if (!ready)
    {
        return 0;
    }

    plugin_mutex_take(&plugin_driver->buffer_mutex);
    if (image_delta_is_delta_payload(local, len))
    {
        rc = image_delta_import(local, len);
    }
    else if (len == IMAGE_SNAPSHOT_TOTAL_BYTES)
    {
        rc = image_snapshot_import(local, len);
    }
    plugin_mutex_give(&plugin_driver->buffer_mutex);

    if (rc == 0)
    {
        pthread_mutex_lock(&pending_mutex);
        last_applied_frame_seq = frame_seq;
        pthread_mutex_unlock(&pending_mutex);
        redundancy_udp_note_applied(frame_seq);
        applied = 1;
    }
    return applied;
}
