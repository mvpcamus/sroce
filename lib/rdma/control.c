#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "tas_ll.h"
#include "tas_rdma.h"

#include "internal.h"

struct rdma_socket* fdmap[MAX_FD_NUM];
struct flextcp_context* appctx = NULL;

/**
 * NOTE: As the TAS internal structures will change,
 * we may not be able to compile files in lib/tas without
 * signficant revision.
 * [1] Slowpath interface is likely to remain the same.
 * [2] Code in lib/tas assumes an async interface. Progress is
 *      dependent on the user to poll regularly.
 * [3] Clean separation of Slowpath/Fastpath interactions is likely
 *      going to help in resue of code.
 */

static int fd_alloc(void)
{
    int i;
    // Skip 0 to avoid possible confusion
    for (i = 1; i < MAX_FD_NUM; i++)
        if (fdmap[i] == NULL)
            break;

    if (i == MAX_FD_NUM)
        return -1;

    return i;
}

int rdma_init(void)
{
    // 1. Connect with TAS
    if (flextcp_init() != 0)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }

    // 2. Register app context with TAS
    appctx = calloc(1, sizeof(struct flextcp_context));
    if (appctx == NULL)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }
    if (flextcp_context_create(appctx) != 0)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }

    // 3. Initialize internal datastructures
    memset(fdmap, 0, sizeof(fdmap));

    return 0;
}

int rdma_listen(const struct sockaddr_in* localaddr, int backlog)
{
    // 1. Validate localaddr
    // TODO: Check if localaddr is same as TAS addr
    if (localaddr == NULL || localaddr->sin_family != AF_INET)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }

    // 2. Allocate FD and Socket
    int fd = fd_alloc();
    if (fd == -1)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }
    struct rdma_socket* s = calloc(1, sizeof(struct rdma_socket));
    if (s == NULL)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }

    // 3. Set backlog to a valid range
    if (backlog < LISTEN_BACKLOG_MIN)
        backlog = LISTEN_BACKLOG_MIN;
    if (backlog > LISTEN_BACKLOG_MAX)
        backlog = LISTEN_BACKLOG_MAX;

    // 4. listen() IPC to TAS Slowpath
    if (flextcp_listen_open(appctx, &s->l,
            ntohs(localaddr->sin_port), backlog, 0) != 0)
    {
        free(s);
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }

    // 5. Block until TAS Slowpath processes the request
    struct flextcp_event ev;
    int ret;
    memset(&ev, 0, sizeof(struct flextcp_event));
    while (1)
    {
	// TODO: Only poll the kernel
        ret = flextcp_context_poll(appctx, 1, &ev);
        if (ret < 0)
        {
            free(s);
            fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
            return -1;
        }

        if (ret == 1)
            break;

        flextcp_block(appctx, CONTROL_TIMEOUT);
    }

    // 6. Check listen() status
    if (ev.event_type != FLEXTCP_EV_LISTEN_OPEN ||
        ev.ev.listen_open.listener != &s->l ||
        ev.ev.listen_open.status != 0)
    {
        free(s);
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }

    // 7. Store rdma_socket in fdmap
    s->type = RDMA_LISTEN_SOCKET;
    fdmap[fd] = s;

    return fd;
}

int rdma_accept(int listenfd, struct sockaddr_in* remoteaddr,
		void **mr_base, uint32_t *mr_len)
{
    // 1. Find listener rdma_socket
    if (listenfd < 1 || listenfd >= MAX_FD_NUM)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }
    struct rdma_socket* ls = fdmap[listenfd];

    // 2. Allocate FD and Socket
    int fd = fd_alloc();
    if (fd == -1)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }
    struct rdma_socket* s = calloc(1, sizeof(struct rdma_socket));
    if (s == NULL)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }

    // 3. accept() IPC to TAS Slowpath
    if (flextcp_listen_accept(appctx, &ls->l, &s->c) != 0)
    {
        free(s);
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }

    // 4. Block until TAS Slowpath processes the request
    struct flextcp_event ev;
    int ret;
    memset(&ev, 0, sizeof(struct flextcp_event));
    while (1)
    {
	// TODO: Only poll the kernel
        ret = flextcp_context_poll(appctx, 1, &ev);
        if (ret < 0)
        {
            free(s);
            fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
            return -1;
        }

        if (ret == 1)
            break;

        flextcp_block(appctx, CONTROL_TIMEOUT);
    }

    // 5. Check accept() status
    if (ev.event_type != FLEXTCP_EV_LISTEN_ACCEPT ||
        ev.ev.listen_accept.conn != &s->c ||
        ev.ev.listen_accept.status != 0)
    {
        free(s);
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }

    // 6. Store socket in fdmap
    s->type = RDMA_CONN_SOCKET;
    fdmap[fd] = s;

    // 7. Update return parameters
    *mr_base = s->c.mr;
    *mr_len = s->c.mr_len;

    // TODO: Find a way to capture the remote addr
    return fd;
}

int rdma_connect(const struct sockaddr_in* remoteaddr, void **mr_base,
		uint32_t *mr_len)
{
    // 1. Validate Remoteaddr
    if (remoteaddr == NULL || remoteaddr->sin_family != AF_INET)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }

    // 2. Allocate FD and Socket
    int fd = fd_alloc();
    if (fd == -1)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }
    struct rdma_socket* s = calloc(1, sizeof(struct rdma_socket));
    if (s == NULL)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }

    // 3. connect() IPC to TAS Slowpath
    if (flextcp_connection_open(appctx, &s->c,
        ntohl(remoteaddr->sin_addr.s_addr), ntohs(remoteaddr->sin_port)) != 0)
    {
        free(s);
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }

    // 4. Block until TAS Slowpath processes the request
    struct flextcp_event ev;
    int ret;
    memset(&ev, 0, sizeof(struct flextcp_event));
    while (1)
    {
	// TODO: Only poll the kernel
        ret = flextcp_context_poll(appctx, 1, &ev);
        if (ret < 0)
        {
            free(s);
            fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
            return -1;
        }

        if (ret == 1)
            break;

        flextcp_block(appctx, CONTROL_TIMEOUT);
    }

    // 5. Check accept() status
    if (ev.event_type != FLEXTCP_EV_CONN_OPEN ||
        ev.ev.conn_open.conn != &s->c ||
        ev.ev.conn_open.status != 0)
    {
        free(s);
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }

    // 6. Store rdma_socket in fdmap
    s->type = RDMA_CONN_SOCKET;
    fdmap[fd] = s;

    // 7. Update return parameters
    *mr_base = s->c.mr;
    *mr_len = s->c.mr_len;

    return fd;
}
