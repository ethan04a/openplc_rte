#ifndef REDUNDANCY_UDP_H
#define REDUNDANCY_UDP_H

#include <stddef.h>
#include <stdint.h>

int redundancy_udp_configure(const char *json);
int redundancy_udp_start(void);
int redundancy_udp_stop(void);
int redundancy_udp_is_running(void);
int redundancy_udp_format_status(char *buf, size_t buf_cap);
void redundancy_udp_note_applied(uint64_t seq);

#endif /* REDUNDANCY_UDP_H */
