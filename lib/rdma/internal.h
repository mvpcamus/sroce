#ifndef INTERNAL_H_
#define INTERNAL_H_

#include "tas_ll.h"

/* File descriptor related definitions */
enum {
    RDMA_UNDEF_SOCKET,
    RDMA_LISTEN_SOCKET,
    RDMA_CONN_SOCKET
};

struct rdma_socket{
    union {
        struct flextcp_connection c;
        struct flextcp_listener l;
    };
    uint8_t type;
};

#define MAX_FD_NUM  (1 << 16)   // TODO: Should be configurable
extern struct rdma_socket* fdmap[MAX_FD_NUM];

/* TAS Application Context */
extern struct flextcp_context* appctx;

#define LISTEN_BACKLOG_MIN  8
#define LISTEN_BACKLOG_MAX  1024

#define CONTROL_TIMEOUT     10  // Block for 10ms

#endif /* INTERNAL_H_ */
