#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

#include "fastpath.h"
#include "packet_defs.h"
#include "internal.h"
#include "tas_memif.h"
#include "tas.h"
#include "tas_rdma.h"
#include "tcp_common.h"

#define TCP_MSS 1448

#define RDMA_RQ_PENDING_PARSE 0x0
#define RDMA_RQ_PENDING_DATA  0x10

static inline void fast_rdma_txbuf_copy(struct flextcp_pl_flowst* fl,
      uint32_t len, void* src);
static inline void fast_rdma_rxbuf_copy(struct flextcp_pl_flowst* fl,
      uint32_t rx_head, uint32_t len, void* dst);
static inline void fast_rdmacq_bump(struct flextcp_pl_flowst* fl,
      uint32_t id, uint8_t status);
static inline void arx_rdma_cache_add(struct dataplane_context* ctx,
      uint16_t ctx_id, uint64_t opaque, uint32_t wq_tail, uint32_t cq_head);
void fast_rdma_poll(struct dataplane_context* ctx,
      struct flextcp_pl_flowst* fl);

int fast_rdmawq_bump(struct dataplane_context *ctx, uint32_t flow_id,
    uint32_t new_wq_head, uint32_t new_cq_tail)
{
  struct flextcp_pl_flowst *fs = &fp_state->flowst[flow_id];

  fs_lock(fs);

/**
 * Work queue regions
 *
 *    !!!!!!!!!!!!+++++++++++++%%%%%%%%%%%%%^^^^^^^^^^^^^!!!!!!!!!!!!!
 *  ||------------A------------B------------C------------D------------||
 *
 *  A: cq_tail            !: Free/Unallocated WQEs
 *  B: cq_head            +: Completed WQEs unread by app
 *  C: wq_tail            %: Unack'd WQEs - req. sent but not ack'd
 *  D: wq_head            ^: In-progress WQEs - req. not yet sent by fp
 *
 *  NOTE: head is always non-inclusive - i.e. [tail, head)
 */

  uint32_t wq_len, wq_head, wq_tail;
  uint32_t cq_head, cq_tail;
  wq_len = fs->wq_len;
  wq_head = fs->wq_head;
  wq_tail = fs->wq_tail;
  cq_head = fs->cq_head;
  cq_tail = fs->cq_tail;

  /**
   *  TODO: Validate wq bump
   *
   *  NOTE: Is this too expensive ?!
   *  Should we trust the application input blindly ?
   */
  uint8_t invalid = ((new_wq_head >= wq_len)
        || (wq_tail < new_wq_head && new_wq_head < wq_head)
        || (new_wq_head < wq_head && wq_head < wq_tail)
        || (wq_head < wq_tail && wq_tail <= new_wq_head)
        || (new_cq_tail >= wq_len)
        || (new_cq_tail < cq_tail && cq_tail < cq_head)
        || (cq_tail < cq_head && cq_head < new_cq_tail)
        || (cq_head < new_cq_tail && new_cq_tail < cq_tail)
        || (wq_tail < new_wq_head && wq_tail < cq_head && cq_head < new_wq_head)
        || (wq_tail < new_wq_head && wq_tail < new_cq_tail && new_cq_tail < new_wq_head)
        || (new_wq_head < wq_tail && cq_head > wq_tail)
        || (cq_head < new_wq_head && new_wq_head < wq_tail)
        || (new_wq_head < wq_tail && new_cq_tail > wq_tail)
        || (new_cq_tail < new_wq_head && new_wq_head < wq_tail));

  if (UNLIKELY(invalid))
  {
    goto RDMA_BUMP_ERROR;
  }

  /* Update the queue */
  fs->wq_head = new_wq_head;
  fs->cq_tail = new_cq_tail;

  /* No pending workqueue requests previously !*/
  if (wq_head == wq_tail)
  {
    uint32_t old_avail, new_avail;
    // copy packets into txbuf
    old_avail = tcp_txavail(fs, NULL);
    fast_rdma_poll(ctx, fs);
    new_avail = tcp_txavail(fs, NULL);

    if (old_avail < new_avail) {
      if (qman_set(&ctx->qman, flow_id, fs->tx_rate, new_avail -
            old_avail, TCP_MSS, QMAN_SET_RATE | QMAN_SET_MAXCHUNK
            | QMAN_ADD_AVAIL) != 0)
      {
        fprintf(stderr, "fast_rdmawq_bump: qman_set failed, UNEXPECTED\n");
        abort();
      }
    }
  }
  fs_unlock(fs);
  return -1;  /* Return value compatible with fast_flows_bump() */

RDMA_BUMP_ERROR:
  fs_unlock(fs);
  fprintf(stderr, "Invalid bump flowid=%u len=%u wq_head=%u wq_tail=%u \
          cq_head=%u cq_tail=%u new_wq_head=%u new_cq_tail=%u\n",
          flow_id, wq_len, wq_head, wq_tail, cq_head, cq_tail,
          new_wq_head, new_cq_tail);
  return -1;
}

int fast_rdmarq_bump(struct dataplane_context* ctx,
    struct flextcp_pl_flowst* fs, uint32_t prev_rx_head, uint32_t rx_bump)
{
  uint32_t rq_head, rq_len, rx_head, rx_len, new_rx_head;
  uint8_t cq_bump = 0;
  rq_head = fs->rq_head;
  rq_len = fs->wq_len;
  rx_head = prev_rx_head;
  rx_len = fs->rx_len;
  new_rx_head = prev_rx_head + rx_bump;
  if (new_rx_head >= rx_len)
    new_rx_head -= rx_len;

  uint32_t wqe_pending_rx, rx_bump_len;
  while (rx_head != new_rx_head && rx_bump > 0)
  {
    if (fs->pending_rq_state == RDMA_RQ_PENDING_DATA)
    {
      struct rdma_wqe* wqe = dma_pointer(fs->rq_base + rq_head,
                                            sizeof(struct rdma_wqe));
      wqe_pending_rx = wqe->len;
      rx_bump_len = MIN(wqe_pending_rx, rx_bump);
      void* mr_ptr = dma_pointer(fs->mr_base + wqe->loff, rx_bump_len);
      if (wqe->status == RDMA_PENDING)
        fast_rdma_rxbuf_copy(fs, rx_head, rx_bump_len, mr_ptr);
      else
      {
        /* Ignore this data */
        fs->rx_avail += rx_bump_len;
      }

      rx_head += rx_bump_len;
      if (rx_head >= rx_len)
        rx_head -= rx_len;
      rx_bump -= rx_bump_len;
      wqe_pending_rx -= rx_bump_len;
      wqe->len -= rx_bump_len;
      wqe->loff += rx_bump_len;

      if (wqe_pending_rx == 0)
      {
        if (wqe->status == RDMA_PENDING)
          wqe->status = RDMA_SUCCESS;

        fs->pending_rq_state = RDMA_RQ_PENDING_PARSE;
        rq_head += sizeof(struct rdma_wqe);
        if (rq_head >= rq_len)
          rq_head -= rq_len;
      }
    }
    else
    {
      wqe_pending_rx = 16 - fs->pending_rq_state;
      rx_bump_len = MIN(wqe_pending_rx, rx_bump);
      fast_rdma_rxbuf_copy(fs, rx_head, rx_bump_len, fs->pending_rq_buf + fs->pending_rq_state);

      rx_head += rx_bump_len;
      if (rx_head >= rx_len)
        rx_head -= rx_len;
      rx_bump -= rx_bump_len;
      wqe_pending_rx -= rx_bump_len;
      fs->pending_rq_state += rx_bump_len;

      if (wqe_pending_rx == 0)
      {
        struct rdma_hdr* hdr = (struct rdma_hdr*) fs->pending_rq_buf;
        struct rdma_wqe* wqe = dma_pointer(fs->rq_base + rq_head,
                                            sizeof(struct rdma_wqe));

        /**
         *  TODO: Implement RDMA_READ operations
         */
        uint8_t type = hdr->type;
        if ((type & RDMA_RESPONSE) == RDMA_RESPONSE)
        {
          if ((type & RDMA_READ) == RDMA_READ)
          {
            /* Not yet implemented */
            fprintf(stderr, "%s():%d RDMA_READ Not yet implemented.\n", __func__, __LINE__);
            abort();
          }
          else if ((type & RDMA_WRITE) == RDMA_WRITE)
          {
            /* No more data to be received */
            fs->pending_rq_state = RDMA_RQ_PENDING_PARSE;

            fast_rdmacq_bump(fs, f_beui32(hdr->id), hdr->status);
            cq_bump = 1;
          }
          else
          {
            fprintf(stderr, "%s():%d Invalid request type\n", __func__, __LINE__);
            abort();
          }
        }
        else if ((type & RDMA_REQUEST) == RDMA_REQUEST)
        {
          wqe->id = f_beui32(hdr->id);
          wqe->len = f_beui32(hdr->length);
          wqe->loff = f_beui32(hdr->offset);
          if (wqe->loff + wqe->len > fs->mr_len)
            wqe->status = RDMA_OUT_OF_BOUNDS;
          else
            wqe->status = RDMA_PENDING;
          wqe->roff = 0;

          if ((type & RDMA_READ) == RDMA_READ)
          {
            fprintf(stderr, "%s():%d RDMA_READ Not yet implemented.\n", __func__, __LINE__);
            abort();

            wqe->type = (RDMA_OP_READ);
            fs->pending_rq_state = RDMA_RQ_PENDING_PARSE; /* No more data to be received */
            rq_head += sizeof(struct rdma_wqe);
            if (rq_head >= rq_len)
              rq_head -= rq_len;
          }
          else if ((type & RDMA_WRITE) == RDMA_WRITE)
          {
            wqe->type = (RDMA_OP_WRITE);
          }
          else
          {
            fprintf(stderr, "%s():%d Invalid request type\n", __func__, __LINE__);
            abort();
          }
        }
        else
        {
            fprintf(stderr, "%s():%d Invalid request type\n", __func__, __LINE__);
          abort();
        }
      }
    }
  }

  fs->rq_head = rq_head;
  if (cq_bump)
    arx_rdma_cache_add(ctx, fs->db_id, fs->opaque, fs->wq_tail, fs->cq_head);

  return 0;
}

static inline void arx_rdma_cache_add(struct dataplane_context* ctx,
      uint16_t ctx_id, uint64_t opaque, uint32_t wq_tail, uint32_t cq_head)
{
  uint16_t id = ctx->arx_num++;

  ctx->arx_ctx[id] = ctx_id;
  ctx->arx_cache[id].type = FLEXTCP_PL_ARX_RDMAUPDATE;
  ctx->arx_cache[id].msg.rdmaupdate.opaque = opaque;
  ctx->arx_cache[id].msg.rdmaupdate.wq_tail = wq_tail;
  ctx->arx_cache[id].msg.rdmaupdate.cq_head = cq_head;
}

static inline void fast_rdmacq_bump(struct flextcp_pl_flowst* fl,
      uint32_t id, uint8_t status)
{
  uint32_t cq_head = fl->cq_head;
  uint32_t wq_tail = fl->wq_tail;
  uint32_t wq_len = fl->wq_len;

  while (cq_head != wq_tail)
  {
    struct rdma_wqe* wqe = dma_pointer(fl->wq_base + cq_head,
                                        sizeof(struct rdma_wqe));
    if (wqe->status == RDMA_RESP_PENDING)
    {
      if (wqe->id != id)
      {
        fprintf(stderr, "%s():%d Invalid response received=%u expected=%u\n",
            __func__, __LINE__, wqe->id, id);
        abort();
      }

      wqe->status = status;
      cq_head += sizeof(struct rdma_wqe);
      if (cq_head >= wq_len)
        cq_head -= wq_len;
      break;
    }

    cq_head += sizeof(struct rdma_wqe);
    if (cq_head >= wq_len)
      cq_head -= wq_len;
  }

  fl->cq_head = cq_head;
}

static inline void fast_rdma_rxbuf_copy(struct flextcp_pl_flowst* fl,
      uint32_t rx_head, uint32_t len, void* dst)
{
  uintptr_t buf1, buf2;
  uint32_t len1, len2;

  uint32_t rxbuf_len = fl->rx_len;
  uint64_t rxbuf_base = (fl->rx_base_sp & FLEXNIC_PL_FLOWST_RX_MASK);

  if (rx_head + len > rxbuf_len)
  {
    len1 = (rxbuf_len - rx_head);
    len2 = (len - len1);
  }
  else
  {
    len1 = len;
    len2 = 0;
  }

  buf1 = (uintptr_t) (rxbuf_base + rx_head);
  buf2 = (uintptr_t) (rxbuf_base);

  dma_read(buf1, len1, dst);
  if (len2)
    dma_read(buf2, len2, dst + len1);

  fl->rx_avail += len;
}

static inline void fast_rdma_txbuf_copy(struct flextcp_pl_flowst* fl,
      uint32_t len, void* src)
{
  uintptr_t buf1, buf2;
  uint32_t len1, len2;

  uint32_t txbuf_len = fl->tx_len;
  uint32_t txbuf_head = fl->txb_head;

  if (txbuf_head + len > txbuf_len)
  {
    len1 = (txbuf_len - txbuf_head);
    len2 = (len - len1);
  }
  else
  {
    len1 = len;
    len2 = 0;
  }

  buf1 = (uintptr_t) (fl->tx_base + txbuf_head);
  buf2 = (uintptr_t) (fl->tx_base);

  dma_write(buf1, len1, src);
  if (len2)
    dma_write(buf2, len2, src + len1);

  txbuf_head += len;
  if (txbuf_head >= txbuf_len)
    txbuf_head -= txbuf_len;

  fl->txb_head = txbuf_head;
  fl->tx_avail += len;
}

/* Transmit atmost one WQE */
static inline int fast_rdmawqe_tx(struct flextcp_pl_flowst* fl,
    struct rdma_wqe* wqe, int is_request)
{
  uint32_t tx_seq, tx_len;
  uint32_t free_txbuf_len, wqe_tx_pending_len;
  struct rdma_hdr hdr;
  void* mr_buf;

  tx_seq = fl->wqe_tx_seq;
  free_txbuf_len = fl->tx_len - fl->tx_avail - fl->tx_sent;

  if (wqe->status == RDMA_TX_PENDING)
  {
    wqe_tx_pending_len = wqe->len - tx_seq;
  }
  else
  {
    hdr.type = (is_request ? RDMA_REQUEST : RDMA_RESPONSE)
                 | (wqe->type == RDMA_OP_READ ? RDMA_READ : RDMA_WRITE);
    hdr.status = (is_request ? 0 : wqe->status);
    hdr.length = t_beui32(wqe->len);
    hdr.offset = t_beui32(wqe->roff);
    hdr.id = t_beui32(wqe->id);
    hdr.flags = t_beui16(0);

    fast_rdma_txbuf_copy(fl, sizeof(struct rdma_hdr), &hdr);

    free_txbuf_len -= sizeof(struct rdma_hdr);
    tx_seq = 0;
    wqe_tx_pending_len = wqe->len;

    if (is_request)
    {
      if (wqe->type == RDMA_OP_READ)
      {
        wqe->status = RDMA_RESP_PENDING;
        return 0;
      }
      else
      {
        wqe->status = RDMA_TX_PENDING;
      }
    }
    else
    {
      if (wqe->status == RDMA_SUCCESS && wqe->type == RDMA_OP_READ)
      {
        wqe->status = RDMA_TX_PENDING;
      }
      else
      {
        return 0;
      }
    }    
  }

  tx_len = MIN(wqe_tx_pending_len, free_txbuf_len);
  mr_buf = dma_pointer(fl->mr_base + wqe->loff + tx_seq, wqe_tx_pending_len);
  fast_rdma_txbuf_copy(fl, tx_len, mr_buf);
  tx_seq += tx_len;

  free_txbuf_len -= tx_len;
  wqe_tx_pending_len -= tx_len;

  if (!wqe_tx_pending_len)
  {
    if (is_request)
      wqe->status = RDMA_RESP_PENDING;
    else
      wqe->status = RDMA_SUCCESS;
  }

  return wqe_tx_pending_len;
}

void fast_rdma_poll(struct dataplane_context* ctx,
      struct flextcp_pl_flowst* fl)
{
  uint32_t wq_head, wq_tail, rq_head, rq_tail, tx_seq;
  uint32_t free_txbuf_len, ret, is_rqe;
  struct rdma_wqe* wqe;

  wq_head = fl->wq_head;
  wq_tail = fl->wq_tail;
  rq_head = fl->rq_head;
  rq_tail = fl->rq_tail;
  free_txbuf_len = fl->tx_len - fl->tx_avail - fl->tx_sent;

  if (fl->wqe_tx_seq > 0)
  {
    is_rqe = 0;
    tx_seq = fl->wqe_tx_seq;
  }
  else if (fl->rqe_tx_seq > 0)
  {
    is_rqe = 1;
    tx_seq = fl->rqe_tx_seq;
  }
  else
  {
    is_rqe = 0;
    tx_seq = 0;
  }

  while (free_txbuf_len > 0)
  {
    if (rq_head == rq_tail && wq_head == wq_tail)
      break;

    if (!is_rqe && wq_head == wq_tail)
    {
      is_rqe = 1;
      continue;
    }

    if (is_rqe && rq_head == rq_tail)
    {
      is_rqe = 0;
      continue;
    }

    if (!is_rqe)
    {
      wqe = dma_pointer(fl->wq_base + wq_tail, sizeof(struct rdma_wqe));

      /* New WQE to be processed */
      if (UNLIKELY(wqe->loff + wqe->len > fl->mr_len))
      {
        wqe->status = RDMA_OUT_OF_BOUNDS;
        goto NEXT_WQE;
      }
    }
    else
    {
      wqe = dma_pointer(fl->rq_base + rq_tail, sizeof(struct rdma_wqe));
    }

    /* New request/response */
    if (tx_seq == 0)
    {
      if (free_txbuf_len < sizeof(struct rdma_hdr))
        break;
    }

    ret = fast_rdmawqe_tx(fl, wqe, !is_rqe);
    if (ret > 0)
    {
      tx_seq = wqe->len - ret;
      break;
    }

NEXT_WQE:
    if (is_rqe)
    {
      rq_tail += sizeof(struct rdma_wqe);
      if (rq_tail >= fl->wq_len)
        rq_tail -= fl->wq_len;
    }
    else
    {
      wq_tail += sizeof(struct rdma_wqe);
      if (wq_tail >= fl->wq_len)
        wq_tail -= fl->wq_len;
    }
    tx_seq = 0;
    is_rqe = (is_rqe ? 0 : 1);
    free_txbuf_len = fl->tx_len - fl->tx_avail - fl->tx_sent;
  }

  fl->wq_tail = wq_tail;
  fl->rq_tail = rq_tail;
  if (is_rqe)
    fl->rqe_tx_seq = tx_seq;
  else
    fl->wqe_tx_seq = tx_seq;
}
