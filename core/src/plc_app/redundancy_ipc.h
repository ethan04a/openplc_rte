#ifndef REDUNDANCY_IPC_H
#define REDUNDANCY_IPC_H

#include <stddef.h>

/**
 * Leave shadow standby: load field I/O plugins from plugins.conf, init, start.
 * PLC program and image tables stay in process; must be RUNNING and shadow_standby set.
 * Writes a single line response into response (e.g. REDUNDANCY_SHADOW_EXIT:OK).
 */
void redundancy_shadow_exit_response(char *response, size_t response_size);

/**
 * Report whether plc_main is RUNNING, still in shadow standby, and driving field I/O.
 * Writes REDUNDANCY_ROLE_STATUS:running=0|1,shadow=0|1,field_io=0|1
 */
void redundancy_role_status_response(char *response, size_t response_size);

#endif /* REDUNDANCY_IPC_H */
