#ifndef REDUNDANCY_IPC_H
#define REDUNDANCY_IPC_H

#include <stddef.h>

/**
 * Leave shadow standby: load field I/O plugins from plugins.conf, init, start.
 * PLC program and image tables stay in process; must be RUNNING and shadow_standby set.
 * Writes a single line response into response (e.g. REDUNDANCY_SHADOW_EXIT:OK).
 */
void redundancy_shadow_exit_response(char *response, size_t response_size);

#endif /* REDUNDANCY_IPC_H */
