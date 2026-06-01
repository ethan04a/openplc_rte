#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "../drivers/plugin_driver.h"
#include "debug_handler.h"
#include "image_snapshot.h"
#include "image_snapshot_delta.h"
#include "plc_state_manager.h"
#include "redundancy_ipc.h"
#include "redundancy_pending.h"
#include "redundancy_udp.h"
#include "scan_cycle_manager.h"
#include "scan_sync.h"
#include "unix_socket.h"
#include "utils/log.h"
#include "utils/utils.h"

extern volatile sig_atomic_t keep_running;
extern PLCState plc_state;
extern plugin_driver_t *plugin_driver;

// helper: read one line terminated by '\n' from a socket
static ssize_t read_line(int fd, char *buffer, size_t max_length)
{
    size_t total_read = 0;
    char ch;
    while (total_read < max_length - 1)
    {
        ssize_t bytes_read = read(fd, &ch, 1);
        if (bytes_read <= 0)
        {
            return bytes_read; // error or connection closed
        }
        if (ch == '\n')
        {
            break; // end of line
        }
        buffer[total_read++] = ch;
    }
    buffer[total_read] = '\0'; // null-terminate the string
    return total_read;
}

static int read_exact(int fd, void *buf, size_t len)
{
    size_t off = 0;

    while (off < len)
    {
        ssize_t r = read(fd, (char *)buf + off, len - off);
        if (r <= 0)
        {
            return -1;
        }
        off += (size_t)r;
    }
    return 0;
}

static int write_all(int fd, const void *buf, size_t len)
{
    size_t off = 0;

    while (off < len)
    {
        ssize_t w = write(fd, (const char *)buf + off, len - off);
        if (w <= 0)
        {
            return -1;
        }
        off += (size_t)w;
    }
    return 0;
}

#define IMAGE_SNAPSHOT_SCAN_END_WAIT_MS 500

static int send_image_snapshot_with_meta(int client_fd, const scan_sync_meta_t *meta)
{
    uint8_t *payload = malloc(IMAGE_SNAPSHOT_TOTAL_BYTES);
    if (!payload)
    {
        const char *err = "IMAGE_SNAPSHOT_GET:ALLOC\n";
        return write_all(client_fd, err, strlen(err));
    }

    size_t out_len = 0;
    int exp_err;

    plugin_mutex_take(&plugin_driver->buffer_mutex);
    exp_err = image_snapshot_export(payload, IMAGE_SNAPSHOT_TOTAL_BYTES, &out_len);
    plugin_mutex_give(&plugin_driver->buffer_mutex);

    if (exp_err != 0 || out_len != IMAGE_SNAPSHOT_TOTAL_BYTES)
    {
        free(payload);
        const char *err = "IMAGE_SNAPSHOT_GET:EXPORT_ERROR\n";
        return write_all(client_fd, err, strlen(err));
    }

    char hdr[160];
    if (meta)
    {
        snprintf(hdr, sizeof(hdr),
                 "IMAGE_SNAPSHOT_HDR:%d:%zu:%" PRIu64 ":%lu:%u:%" PRIu64 "\n",
                 IMAGE_SNAPSHOT_VERSION, (size_t)IMAGE_SNAPSHOT_TOTAL_BYTES,
                 meta->scan_counter, meta->tick, (unsigned)meta->phase, meta->timestamp_ns);
    }
    else
    {
        snprintf(hdr, sizeof(hdr), "IMAGE_SNAPSHOT_HDR:%d:%zu\n", IMAGE_SNAPSHOT_VERSION,
                 (size_t)IMAGE_SNAPSHOT_TOTAL_BYTES);
    }

    int rc = 0;
    if (write_all(client_fd, hdr, strlen(hdr)) != 0 ||
        write_all(client_fd, payload, IMAGE_SNAPSHOT_TOTAL_BYTES) != 0)
    {
        log_error("IMAGE_SNAPSHOT_GET: write failed");
        rc = -1;
    }
    free(payload);
    return rc;
}

static int send_image_delta_scan_end(int client_fd, uint64_t after_counter)
{
    scan_sync_meta_t meta;
    uint8_t buf[IMAGE_DELTA_MAX_WIRE_BYTES + 64];
    size_t len = 0;
    char hdr[64];

    if (!plugin_driver || plc_get_state() != PLC_STATE_RUNNING)
    {
        return -1;
    }
    if (scan_sync_wait_for_end(after_counter, IMAGE_SNAPSHOT_SCAN_END_WAIT_MS, &meta) != 0)
    {
        const char *err = "IMAGE_DELTA_GET:SCAN_END_TIMEOUT\n";
        return write_all(client_fd, err, strlen(err));
    }

    plugin_mutex_take(&plugin_driver->buffer_mutex);
    if (image_delta_export_scan_end(buf, sizeof(buf), &len, &meta) != 0)
    {
        plugin_mutex_give(&plugin_driver->buffer_mutex);
        const char *err = "IMAGE_DELTA_GET:EXPORT_ERROR\n";
        return write_all(client_fd, err, strlen(err));
    }
    plugin_mutex_give(&plugin_driver->buffer_mutex);

    snprintf(hdr, sizeof(hdr), "IMAGE_DELTA_HDR:%zu\n", len);
    if (write_all(client_fd, hdr, strlen(hdr)) != 0 || write_all(client_fd, buf, len) != 0)
    {
        return -1;
    }
    return 0;
}

void handle_unix_socket_commands(const char *command, char *response, size_t response_size)
{
    if (strcmp(command, "PING") == 0)
    {
        strncpy(response, "PING:OK\n", response_size);
    }
    else if (strcmp(command, "STATUS") == 0)
    {
        PLCState current_state = plc_get_state();

        if (current_state == PLC_STATE_INIT)
            strncpy(response, "STATUS:INIT\n", response_size);
        else if (current_state == PLC_STATE_RUNNING)
            strncpy(response, "STATUS:RUNNING\n", response_size);
        else if (current_state == PLC_STATE_STOPPED)
            strncpy(response, "STATUS:STOPPED\n", response_size);
        else if (current_state == PLC_STATE_ERROR)
            strncpy(response, "STATUS:ERROR\n", response_size);
        else if (current_state == PLC_STATE_EMPTY)
            strncpy(response, "STATUS:EMPTY\n", response_size);
        else
            strncpy(response, "STATUS:UNKNOWN\n", response_size);
    }
    else if (strcmp(command, "STOP") == 0)
    {
        if (plc_set_state(PLC_STATE_STOPPED))
            strncpy(response, "STOP:OK\n", response_size);
        else
            strncpy(response, "STOP:ERROR\n", response_size);
    }
    else if (strcmp(command, "START") == 0)
    {
        PLCState current_state = plc_get_state();
        if (current_state != PLC_STATE_RUNNING)
        {
            if (plc_set_state(PLC_STATE_RUNNING))
            {
                strncpy(response, "START:OK\n", response_size);
            }
            else
            {
                strncpy(response, "START:ERROR\n", response_size);
            }
        }
        else
        {
            strncpy(response, "START:ERROR_ALREADY_RUNNING\n", response_size);
            log_error("Received START command but PLC is already RUNNING");
        }
    }
    else if (strcmp(command, "STATS") == 0)
    {
        format_timing_stats_response(response, response_size);
    }
    else if (strncmp(command, "DEBUG:", 6) == 0)
    {
        uint8_t debug_data[4096] = {0};
        size_t data_length       = parse_hex_string(&command[6], debug_data);
        if (data_length > 0)
        {
            data_length = process_debug_data(debug_data, data_length);
            if (data_length > 0)
            {
                bytes_to_hex_string(debug_data, data_length, response, response_size, "DEBUG:");
                size_t len = strlen(response);
                if (len < response_size - 1)
                {
                    response[len]     = '\n';
                    response[len + 1] = '\0';
                }
            }
            else
            {
                strncpy(response, "DEBUG:ERROR_PROCESSING\n", response_size);
            }
        }
        else
        {
            strncpy(response, "DEBUG:ERROR_PARSING\n", response_size);
        }
    }
    else if (strcmp(command, "REDUNDANCY_SHADOW_EXIT") == 0)
    {
        redundancy_shadow_exit_response(response, response_size);
    }
    else if (strncmp(command, "REDUNDANCY_SYNC_CONFIG:", 22) == 0)
    {
        if (redundancy_udp_configure(command + 22) == 0)
        {
            strncpy(response, "REDUNDANCY_SYNC_CONFIG:OK\n", response_size);
        }
        else
        {
            strncpy(response, "REDUNDANCY_SYNC_CONFIG:ERROR\n", response_size);
        }
    }
    else if (strcmp(command, "REDUNDANCY_SYNC_START") == 0)
    {
        if (redundancy_udp_start() == 0)
        {
            strncpy(response, "REDUNDANCY_SYNC_START:OK\n", response_size);
        }
        else
        {
            strncpy(response, "REDUNDANCY_SYNC_START:ERROR\n", response_size);
        }
    }
    else if (strcmp(command, "REDUNDANCY_SYNC_STOP") == 0)
    {
        redundancy_udp_stop();
        strncpy(response, "REDUNDANCY_SYNC_STOP:OK\n", response_size);
    }
    else if (strcmp(command, "REDUNDANCY_SYNC_STATUS") == 0)
    {
        if (redundancy_udp_format_status(response, response_size) != 0)
        {
            strncpy(response, "REDUNDANCY_SYNC_STATUS:ERROR\n", response_size);
        }
    }
    else
    {
        log_error("Unknown command received: %s", command);
        strncpy(response, "COMMAND:ERROR\n", response_size);
    }

    // Always ensure null termination
    response[response_size - 1] = '\0';
}

void *unix_socket_thread(void *arg)
{
    (void)arg;
    int *server_fd_pt = (int *)arg;
    int client_fd;
    char command_buffer[COMMAND_BUFFER_SIZE];

    if (server_fd_pt == NULL)
    {
        log_error("Server file descriptor is NULL");
        return NULL;
    }

    int server_fd = *server_fd_pt;
    if (server_fd < 0)
    {
        log_error("Failed to set up UNIX socket");
        return NULL;
    }

    while (keep_running)
    {
        client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0)
        {
            if (errno == EINTR)
            {
                continue; // Interrupted by signal, retry
            }
            log_error("Unix socket accept failed: %s", strerror(errno));

            // Retry after a short delay
            sleep(1);
            continue;
        }

        log_info("Unix socket client connected");

        while (keep_running)
        {
            ssize_t bytes_read = read_line(client_fd, command_buffer, COMMAND_BUFFER_SIZE);
            if (bytes_read > 0)
            {
                char response[MAX_RESPONSE_SIZE] = {0};

                if (strcmp(command_buffer, "IMAGE_SNAPSHOT_GET") == 0)
                {
                    if (!plugin_driver)
                    {
                        strncpy(response, "IMAGE_SNAPSHOT_GET:NO_DRIVER\n", MAX_RESPONSE_SIZE);
                        write_all(client_fd, response, strlen(response));
                    }
                    else if (plc_get_state() != PLC_STATE_RUNNING)
                    {
                        strncpy(response, "IMAGE_SNAPSHOT_GET:NOT_READY\n", MAX_RESPONSE_SIZE);
                        write_all(client_fd, response, strlen(response));
                    }
                    else if (send_image_snapshot_with_meta(client_fd, NULL) != 0)
                    {
                        strncpy(response, "IMAGE_SNAPSHOT_GET:WRITE_ERROR\n", MAX_RESPONSE_SIZE);
                        write_all(client_fd, response, strlen(response));
                    }
                }
                else if (strncmp(command_buffer, "IMAGE_SNAPSHOT_GET_SCAN_END", 28) == 0 &&
                         (command_buffer[28] == '\0' || command_buffer[28] == ':'))
                {
                    uint64_t after_counter = 0;

                    if (command_buffer[28] == ':')
                    {
                        unsigned long long parsed = 0;
                        if (sscanf(command_buffer + 29, "%llu", &parsed) == 1)
                        {
                            after_counter = (uint64_t)parsed;
                        }
                    }

                    if (!plugin_driver)
                    {
                        strncpy(response, "IMAGE_SNAPSHOT_GET:NO_DRIVER\n", MAX_RESPONSE_SIZE);
                        write_all(client_fd, response, strlen(response));
                    }
                    else if (plc_get_state() != PLC_STATE_RUNNING)
                    {
                        strncpy(response, "IMAGE_SNAPSHOT_GET:NOT_READY\n", MAX_RESPONSE_SIZE);
                        write_all(client_fd, response, strlen(response));
                    }
                    else
                    {
                        scan_sync_meta_t meta;
                        if (scan_sync_wait_for_end(after_counter, IMAGE_SNAPSHOT_SCAN_END_WAIT_MS,
                                                   &meta) != 0)
                        {
                            strncpy(response, "IMAGE_SNAPSHOT_GET:SCAN_END_TIMEOUT\n",
                                    MAX_RESPONSE_SIZE);
                            write_all(client_fd, response, strlen(response));
                        }
                        else if (send_image_snapshot_with_meta(client_fd, &meta) != 0)
                        {
                            strncpy(response, "IMAGE_SNAPSHOT_GET:WRITE_ERROR\n",
                                    MAX_RESPONSE_SIZE);
                            write_all(client_fd, response, strlen(response));
                        }
                    }
                }
                else if (strncmp(command_buffer, "IMAGE_DELTA_GET_SCAN_END", 24) == 0 &&
                         (command_buffer[24] == '\0' || command_buffer[24] == ':'))
                {
                    uint64_t after_counter = 0;
                    if (command_buffer[24] == ':')
                    {
                        unsigned long long parsed = 0;
                        if (sscanf(command_buffer + 25, "%llu", &parsed) == 1)
                        {
                            after_counter = (uint64_t)parsed;
                        }
                    }
                    if (!plugin_driver)
                    {
                        strncpy(response, "IMAGE_DELTA_GET:NO_DRIVER\n", MAX_RESPONSE_SIZE);
                        write_all(client_fd, response, strlen(response));
                    }
                    else if (plc_get_state() != PLC_STATE_RUNNING)
                    {
                        strncpy(response, "IMAGE_DELTA_GET:NOT_READY\n", MAX_RESPONSE_SIZE);
                        write_all(client_fd, response, strlen(response));
                    }
                    else if (send_image_delta_scan_end(client_fd, after_counter) != 0)
                    {
                        strncpy(response, "IMAGE_DELTA_GET:WRITE_ERROR\n", MAX_RESPONSE_SIZE);
                        write_all(client_fd, response, strlen(response));
                    }
                }
                else if (strncmp(command_buffer, "IMAGE_SYNC_PENDING_SET:", 23) == 0)
                {
                    unsigned long long frame_seq = 0;
                    unsigned long sz             = 0;
                    if (sscanf(command_buffer + 23, "%llu:%lu", &frame_seq, &sz) != 2 ||
                        !plugin_driver || sz == 0 || sz > REDUNDANCY_PENDING_MAX_BYTES)
                    {
                        strncpy(response, "IMAGE_SYNC_PENDING_SET:HDR_ERROR\n", MAX_RESPONSE_SIZE);
                        write_all(client_fd, response, strlen(response));
                    }
                    else
                    {
                        uint8_t *payload = malloc((size_t)sz);
                        if (!payload || read_exact(client_fd, payload, (size_t)sz) != 0)
                        {
                            free(payload);
                            strncpy(response, "IMAGE_SYNC_PENDING_SET:READ_ERROR\n",
                                    MAX_RESPONSE_SIZE);
                            write_all(client_fd, response, strlen(response));
                        }
                        else if (plc_get_state() != PLC_STATE_RUNNING)
                        {
                            free(payload);
                            strncpy(response, "IMAGE_SYNC_PENDING_SET:NOT_READY\n",
                                    MAX_RESPONSE_SIZE);
                            write_all(client_fd, response, strlen(response));
                        }
                        else if (!plugin_driver->shadow_standby)
                        {
                            free(payload);
                            strncpy(response, "IMAGE_SYNC_PENDING_SET:NOT_SHADOW\n",
                                    MAX_RESPONSE_SIZE);
                            write_all(client_fd, response, strlen(response));
                        }
                        else
                        {
                            redundancy_pending_set_frame_seq((uint64_t)frame_seq);
                            if (redundancy_pending_store(payload, (size_t)sz) != 0)
                            {
                                free(payload);
                                strncpy(response, "IMAGE_SYNC_PENDING_SET:STORE_ERROR\n",
                                        MAX_RESPONSE_SIZE);
                                write_all(client_fd, response, strlen(response));
                            }
                            else
                            {
                                free(payload);
                                strncpy(response, "IMAGE_SYNC_PENDING_SET:QUEUED\n",
                                        MAX_RESPONSE_SIZE);
                                write_all(client_fd, response, strlen(response));
                            }
                        }
                    }
                }
                else if (strncmp(command_buffer, "IMAGE_SYNC_WAIT_APPLIED:", 24) == 0)
                {
                    unsigned long long want = 0;

                    if (sscanf(command_buffer + 24, "%llu", &want) != 1)
                    {
                        strncpy(response, "IMAGE_SYNC_WAIT_APPLIED:HDR_ERROR\n", MAX_RESPONSE_SIZE);
                        write_all(client_fd, response, strlen(response));
                    }
                    else if (redundancy_pending_wait_applied((uint64_t)want, 1600) == 0)
                    {
                        strncpy(response, "IMAGE_SYNC_WAIT_APPLIED:OK\n", MAX_RESPONSE_SIZE);
                        write_all(client_fd, response, strlen(response));
                    }
                    else
                    {
                        strncpy(response, "IMAGE_SYNC_WAIT_APPLIED:TIMEOUT\n",
                                MAX_RESPONSE_SIZE);
                        write_all(client_fd, response, strlen(response));
                    }
                }
                else if (strncmp(command_buffer, "IMAGE_SNAPSHOT_SET:", 19) == 0)
                {
                    unsigned ver = 0;
                    unsigned long sz = 0;

                    if (sscanf(command_buffer + 19, "%u:%lu", &ver, &sz) != 2 || !plugin_driver)
                    {
                        strncpy(response, "IMAGE_SNAPSHOT_SET:HDR_ERROR\n", MAX_RESPONSE_SIZE);
                        write_all(client_fd, response, strlen(response));
                    }
                    else if (ver != IMAGE_SNAPSHOT_VERSION || sz != IMAGE_SNAPSHOT_TOTAL_BYTES)
                    {
                        strncpy(response, "IMAGE_SNAPSHOT_SET:VERSION_OR_SIZE\n",
                                MAX_RESPONSE_SIZE);
                        write_all(client_fd, response, strlen(response));
                    }
                    else
                    {
                        uint8_t *payload = malloc((size_t)sz);

                        if (!payload || read_exact(client_fd, payload, (size_t)sz) != 0)
                        {
                            free(payload);
                            strncpy(response, "IMAGE_SNAPSHOT_SET:READ_ERROR\n", MAX_RESPONSE_SIZE);
                            write_all(client_fd, response, strlen(response));
                        }
                        else if (plc_get_state() != PLC_STATE_RUNNING)
                        {
                            free(payload);
                            strncpy(response, "IMAGE_SNAPSHOT_SET:NOT_READY\n", MAX_RESPONSE_SIZE);
                            write_all(client_fd, response, strlen(response));
                        }
                        else if (!plugin_driver->shadow_standby)
                        {
                            free(payload);
                            strncpy(response, "IMAGE_SNAPSHOT_SET:NOT_SHADOW\n", MAX_RESPONSE_SIZE);
                            write_all(client_fd, response, strlen(response));
                        }
                        else
                        {
                            (void)redundancy_pending_alloc_frame_seq();
                            if (redundancy_pending_store(payload, (size_t)sz) != 0)
                            {
                                free(payload);
                                strncpy(response, "IMAGE_SNAPSHOT_SET:STORE_ERROR\n",
                                        MAX_RESPONSE_SIZE);
                            }
                            else
                            {
                                free(payload);
                                strncpy(response, "IMAGE_SNAPSHOT_SET:QUEUED\n",
                                        MAX_RESPONSE_SIZE);
                            }
                            write_all(client_fd, response, strlen(response));
                        }
                    }
                }
                else
                {
                    handle_unix_socket_commands(command_buffer, response, MAX_RESPONSE_SIZE);
                    if (strlen(response) > 0)
                    {
                        ssize_t bytes_written = write(client_fd, response, strlen(response));
                        if (bytes_written <= 0)
                        {
                            log_error("Error writing on unix socket: %s", strerror(errno));
                        }
                    }
                }
            }
            else if (bytes_read == 0)
            {
                log_info("Unix socket client disconnected");
                break;
            }
            else
            {
                log_error("Unix socket read failed: %s", strerror(errno));
                break;
            }
        }
        close(client_fd);
    }

    close_unix_socket(server_fd);
    return NULL;
}

void close_unix_socket(int server_fd)
{
    if (server_fd >= 0)
    {
        close(server_fd);
        unlink(SOCKET_PATH);
        log_info("UNIX socket server closed");
    }
}

int setup_unix_socket(void)
{
    int server_fd;
    struct sockaddr_un address;

    // Remove any existing socket file
    unlink(SOCKET_PATH);

    // Create socket
    if ((server_fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0)
    {
        log_error("Socket creation failed: %s", strerror(errno));
        return -1;
    }

    // Configure socket address structure
    memset(&address, 0, sizeof(address));
    address.sun_family = AF_UNIX;
    strncpy(address.sun_path, SOCKET_PATH, sizeof(address.sun_path) - 1);

    // Bind socket to the address
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0)
    {
        log_error("Socket bind failed: %s", strerror(errno));
        close(server_fd);
        return -1;
    }

    // Listen for incoming connections
    if (listen(server_fd, MAX_CLIENTS) < 0)
    {
        log_error("Socket listen failed: %s", strerror(errno));
        close(server_fd);
        return -1;
    }

    log_info("UNIX socket server setup at %s", SOCKET_PATH);

    // Create a thread to handle socket commands
    pthread_t socket_thread;
    int *fd_ptr = malloc(sizeof(int));
    *fd_ptr     = server_fd;
    if (pthread_create(&socket_thread, NULL, unix_socket_thread, fd_ptr) != 0)
    {
        log_error("Failed to create UNIX socket thread: %s", strerror(errno));
        close(server_fd);
        free(fd_ptr);
        return -1;
    }

    return 0;
}
