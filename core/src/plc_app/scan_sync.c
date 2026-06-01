#include "scan_sync.h"

#include <errno.h>
#include <pthread.h>
#include <time.h>

static pthread_mutex_t scan_sync_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t scan_sync_cond   = PTHREAD_COND_INITIALIZER;
static uint64_t scan_end_counter        = 0;
static unsigned long scan_end_tick      = 0;
static uint64_t scan_end_timestamp_ns   = 0;
static uint64_t scan_start_counter      = 0;

void scan_sync_init(void)
{
    pthread_mutex_lock(&scan_sync_mutex);
    scan_end_counter      = 0;
    scan_end_tick         = 0;
    scan_end_timestamp_ns = 0;
    scan_start_counter    = 0;
    pthread_mutex_unlock(&scan_sync_mutex);
}

void scan_sync_notify_scan_start(void)
{
    pthread_mutex_lock(&scan_sync_mutex);
    scan_start_counter++;
    pthread_cond_broadcast(&scan_sync_cond);
    pthread_mutex_unlock(&scan_sync_mutex);
}

int scan_sync_wait_for_start(uint64_t after_counter, int timeout_ms)
{
    struct timespec deadline;
    clock_gettime(CLOCK_MONOTONIC, &deadline);
    if (timeout_ms <= 0)
    {
        timeout_ms = 500;
    }
    deadline.tv_sec += (time_t)(timeout_ms / 1000);
    deadline.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L)
    {
        deadline.tv_sec += deadline.tv_nsec / 1000000000L;
        deadline.tv_nsec %= 1000000000L;
    }

    pthread_mutex_lock(&scan_sync_mutex);
    while (scan_start_counter <= after_counter)
    {
        int rc = pthread_cond_timedwait(&scan_sync_cond, &scan_sync_mutex, &deadline);
        if (rc == ETIMEDOUT)
        {
            pthread_mutex_unlock(&scan_sync_mutex);
            return -1;
        }
        if (rc != 0)
        {
            pthread_mutex_unlock(&scan_sync_mutex);
            return -2;
        }
    }
    pthread_mutex_unlock(&scan_sync_mutex);
    return 0;
}

void scan_sync_notify_scan_end(unsigned long tick)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    uint64_t ts_ns =
        (uint64_t)now.tv_sec * 1000000000ULL + (uint64_t)now.tv_nsec;

    pthread_mutex_lock(&scan_sync_mutex);
    scan_end_counter++;
    scan_end_tick         = tick;
    scan_end_timestamp_ns = ts_ns;
    pthread_cond_broadcast(&scan_sync_cond);
    pthread_mutex_unlock(&scan_sync_mutex);
}

int scan_sync_wait_for_end(uint64_t after_counter, int timeout_ms, scan_sync_meta_t *meta)
{
    struct timespec deadline;
    clock_gettime(CLOCK_MONOTONIC, &deadline);

    if (timeout_ms <= 0)
    {
        timeout_ms = 500;
    }
    deadline.tv_sec += (time_t)(timeout_ms / 1000);
    deadline.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
    if (deadline.tv_nsec >= 1000000000L)
    {
        deadline.tv_sec += deadline.tv_nsec / 1000000000L;
        deadline.tv_nsec %= 1000000000L;
    }

    pthread_mutex_lock(&scan_sync_mutex);
    while (scan_end_counter <= after_counter)
    {
        int rc = pthread_cond_timedwait(&scan_sync_cond, &scan_sync_mutex, &deadline);
        if (rc == ETIMEDOUT)
        {
            pthread_mutex_unlock(&scan_sync_mutex);
            return -1;
        }
        if (rc != 0)
        {
            pthread_mutex_unlock(&scan_sync_mutex);
            return -2;
        }
    }

    if (meta)
    {
        meta->scan_counter  = scan_end_counter;
        meta->tick          = scan_end_tick;
        meta->phase         = SCAN_SYNC_PHASE_SCAN_END;
        meta->timestamp_ns  = scan_end_timestamp_ns;
    }
    pthread_mutex_unlock(&scan_sync_mutex);
    return 0;
}

uint64_t scan_sync_get_start_counter(void)
{
    uint64_t value;
    pthread_mutex_lock(&scan_sync_mutex);
    value = scan_start_counter;
    pthread_mutex_unlock(&scan_sync_mutex);
    return value;
}
