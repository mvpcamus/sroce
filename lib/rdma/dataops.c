#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "tas_ll.h"
#include "tas_rdma.h"
#include "utils.h"
#include "internal.h"

/* NOTE: Two data operations must not be called concurrently on the same
 * 'fd'
 */

int rdma_read(int fd, uint32_t len, uint32_t loffset, uint32_t roffset)
{
    // 1. Find listener socket
    if (fd < 1 || fd >= MAX_FD_NUM)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }
    struct rdma_socket* s = fdmap[fd];
    if (s->type != RDMA_CONN_SOCKET)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }
    struct flextcp_connection* c = &s->c;

    // 2. Validate address in memory region
    if (((uint64_t) loffset + len) > c->mr_len)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }

    // 3. Acquire Work Queue Entry
	  // NOTE: c->wq_len must be a multiple of sizeof(struct rdma_wqe)
    if (c->wq_len + c->cq_len == c->wq_size){
        // Queue full!
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }
	uint32_t wq_head = (c->wq_tail + c->wq_len) % c->wq_size;
    struct rdma_wqe* wqe_pos = (struct rdma_wqe*)(c->wq_base + wq_head);

    // 4. Fill entries of Work Queue
    int32_t id;
    id = wqe_pos->id = (c->wq_tail + c->wq_len) % c->wq_size;
    wqe_pos->type = RDMA_OP_READ;
    wqe_pos->status = RDMA_PENDING;
    wqe_pos->loff = loffset;
    wqe_pos->roff = roffset;
    wqe_pos->len = len;

    // 5. Increment Queue length
	uint32_t old_len = c->wq_len;
    MEM_BARRIER();
	c->wq_len += sizeof(struct rdma_wqe);

    // TODO: Handle the case where bump queue is full
    // 6. Bump the fast path
    if (rdma_conn_bump(appctx, c) < 0) {
        // Undo the length increment (effectively revert adding wqe)
		c->wq_len = old_len;
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }

    return id;
}

int rdma_write(int fd, uint32_t len, uint32_t loffset, uint32_t roffset)
{
    // 1. Find listener socket
    if (fd < 1 || fd >= MAX_FD_NUM)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }
    struct rdma_socket* s = fdmap[fd];
    if (s->type != RDMA_CONN_SOCKET)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }
    struct flextcp_connection* c = &s->c;

    // 2. Validate address in memory region
    if (((uint64_t) loffset + len) > c->mr_len)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }

    // 3. Acquire Work Queue Entry
    // NOTE: c->wq_len must be a multiple of sizeof(struct rdma_wqe)
    if (c->wq_len + c->cq_len == c->wq_size){
        // Queue full!
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }

	uint32_t wq_head = (c->wq_tail + c->wq_len) % c->wq_size;
    struct rdma_wqe* wqe_pos = (struct rdma_wqe*)(c->wq_base + wq_head);

    // 4. Fill entries of Work Queue
    int32_t id;
    id = wqe_pos->id = (c->wq_tail + c->wq_len) % c->wq_size;
    wqe_pos->type = RDMA_OP_WRITE;
    wqe_pos->status = RDMA_PENDING;
    wqe_pos->loff = loffset;
    wqe_pos->roff = roffset;
    wqe_pos->len = len;

    // 5. Increment Queue length
	uint32_t old_len = c->wq_len;
    MEM_BARRIER();
	c->wq_len += sizeof(struct rdma_wqe);

    // TODO: Handle the case where bump queue is full
    // 6. Bump the fast path
    if (rdma_conn_bump(appctx, c) < 0) {
        // Undo the length increment (effectively revert adding wqe)
		c->wq_len = old_len;
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }

    return id;
}

int rdma_cq_poll(int fd, struct rdma_wqe* compl_evs, uint32_t num){
    int ret;
    if (fd < 1 || fd >= MAX_FD_NUM)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }
    struct rdma_socket* s = fdmap[fd];
    if (s->type != RDMA_CONN_SOCKET)
    {
        fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
        return -1;
    }
    struct flextcp_connection* c = &s->c;
    if (c->cq_len < num * sizeof(struct rdma_wqe))
    {
        ret = rdma_fastpath_poll(appctx, c, num * sizeof(struct rdma_wqe));
        if (ret < 0){
            fprintf(stderr, "[ERROR] %s():%u failed\n", __func__, __LINE__);
            return -1;
        }
    }

    int i = 0;
    struct rdma_wqe* wqe;
    struct rdma_wqe* ev;
    while(c->cq_len > 0 && i < num){
        wqe = (struct rdma_wqe*)(c->wq_base + c->cq_tail);
        ev = compl_evs + i;

        // Copy the wqe data
        memcpy(ev, wqe, sizeof(struct rdma_wqe));

        // Update queue pointers and length
        c->cq_tail = (c->cq_tail + sizeof(struct rdma_wqe)) % c->wq_size;
        c->cq_len -= sizeof(struct rdma_wqe);
        i += 1;
    }
    return i;
}
