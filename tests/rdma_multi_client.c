#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include <tas_rdma.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#define NUM_CONNECTIONS     65535
#define MESSAGE_SIZE        64
#define MRSIZE              64*1024
#define WQSIZE              1024

#define EXEC_LEN 120
#define ITERATION 5000000

#define HIST_START_US 0
#define HIST_RESOL 1000
#define HIST_BUCKETS 499

int fd[NUM_CONNECTIONS];
void* mr_base[NUM_CONNECTIONS];
uint32_t mr_len[NUM_CONNECTIONS];
int count[NUM_CONNECTIONS];
struct rdma_wqe ev[WQSIZE];

uint32_t run_count;

static inline uint64_t get_nanos(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t) ts.tv_sec * 1000 * 1000 * 1000 + ts.tv_nsec;
}

static __inline__ unsigned long long rdtsc(void)
{
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ((unsigned long long) lo)| (((unsigned long long) hi) << 32);
}

struct hist {
    uint32_t index;
    uint32_t counts;
    uint64_t min;
    uint64_t max;
    uint32_t pert[7]; // index of percentiles
    uint32_t lat[HIST_BUCKETS+1];
};

#define TIMING_COUNTS (WQSIZE * 10)

struct timings {
    uint32_t write;
    uint32_t read;
    uint32_t counts;
    uint64_t time[TIMING_COUNTS];
};

static inline void histogram(struct hist* buckets, uint64_t latency)
{
    buckets->min = (buckets->min < latency) ? buckets->min : latency;
    buckets->max = (buckets->max > latency) ? buckets->max : latency;
    buckets->index = latency / HIST_RESOL;
    if (buckets->index > HIST_BUCKETS) buckets->index = HIST_BUCKETS;
    buckets->lat[buckets->index]++;
}

static void print_hist(struct hist* buckets)
{
    fprintf(stderr, "\n*** LATENCY HISTOGRAM ***\n");
    fprintf(stderr, "resolution: %d, buckets: %d, counts: %d\n", HIST_RESOL, HIST_BUCKETS, buckets->counts);
    fprintf(stderr, "min: %ld, max: %ld\n", buckets->min, buckets->max);
    fprintf(stderr, "percentiles: ");
    fprintf(stderr, "%d, %d, %d, %d, %d, %d, %d ", buckets->pert[0], buckets->pert[1],
            buckets->pert[2], buckets->pert[3], buckets->pert[4], buckets->pert[5], buckets->pert[6]);
    fprintf(stderr, " [50, 75, 90, 95, 99, 99.9, 99.99%%]\n\n");
    for (int i=0; i<HIST_BUCKETS+1; i++) {
        fprintf(stderr, "%d\t%d\n", (i+1), buckets->lat[i]);
    }
    fprintf(stderr, "\n");
}

static void percentile(struct hist* buckets)
{
    uint32_t counts = 0;
    for (int i=0; i<HIST_BUCKETS+1; i++) {
        counts += buckets->lat[i];
    }
    buckets->counts = counts;
    buckets->pert[0] = (uint32_t)(counts * 0.5);
    buckets->pert[1] = (uint32_t)(counts * 0.75);
    buckets->pert[2] = (uint32_t)(counts * 0.90);
    buckets->pert[3] = (uint32_t)(counts * 0.95);
    buckets->pert[4] = (uint32_t)(counts * 0.99);
    buckets->pert[5] = (uint32_t)(counts * 0.999);
    buckets->pert[6] = (uint32_t)(counts * 0.9999);

    for (int i=0; i<7; i++) {
        counts = 0;
        for (int j=0; j<HIST_BUCKETS+1; j++) {
            counts += buckets->lat[j];
            if (counts >= buckets->pert[i]) {
                buckets->pert[i] = j+1;
                break;
            }
        }
    }
}

static inline uint32_t save_time(struct timings* buf)
{
    if (buf->counts+1 > TIMING_COUNTS) {
        fprintf(stderr, "LATENCY BUFFER FULL!\n");
        return -1;
    }
    buf->counts++;
    buf->time[buf->write] = rdtsc();
    buf->write++;
    if (buf->write >= TIMING_COUNTS) buf->write %= TIMING_COUNTS;
    return buf->write;
}

static inline uint64_t read_time(struct timings* buf)
{
    uint64_t ret;
    if (buf->counts == 0) {
        fprintf(stderr, "LATENCY BUFFER EMPTY!\n");
        return 0;
    }
    buf->counts--;
    ret = rdtsc() - buf->time[buf->read];
    buf->read++;
    if (buf->read >= TIMING_COUNTS) buf->read %= TIMING_COUNTS;
    return ret;
}

int main(int argc, char* argv[])
{
    assert(argc == 6);
    char* rip = argv[1];
    int rport = atoi(argv[2]);
    int num_conns = atoi(argv[3]);
    uint32_t msg_len = (uint32_t) atoi(argv[4]);
    int pending_msgs = atoi(argv[5]);
    uint64_t compl_msgs = 0;
    uint64_t iterations = ITERATION / num_conns;

    assert(num_conns < NUM_CONNECTIONS);
    assert(msg_len < MRSIZE);
    assert(pending_msgs < WQSIZE);

    fprintf(stderr, "conns: %d, msg: %u, pend_msg: %d\n", num_conns, msg_len, pending_msgs);

    rdma_init();
    struct sockaddr_in remoteaddr;
    remoteaddr.sin_family = AF_INET;
    remoteaddr.sin_addr.s_addr = inet_addr(rip);
    remoteaddr.sin_port = htons(rport);

    struct hist latency_hist;
    memset(&latency_hist, 0, sizeof(struct hist));
    latency_hist.min--;
    struct timings latency_time[num_conns];
    memset(latency_time, 0, sizeof(struct timings) * num_conns);

    for (int i = 0; i < num_conns; i++)
    {
        fd[i] = rdma_connect(&remoteaddr, &mr_base[i], &mr_len[i]);

        if (fd[i] < 0)
        {
            fprintf(stderr, "Connection failed\n");
            return -1;
        }
    }

    fprintf(stderr, "SRoCE Connections established: %d\n", num_conns);

    char* c = mr_base[0];
    char f = 0;
    for (int i = 0; i < mr_len[0]; i++)
    {
        *c = f;
        c++;
        f++;
    }
    count[0] = pending_msgs;
    for (int i = 1; i < num_conns; i++)
    {
        memcpy(mr_base[i], mr_base[0], mr_len[0]);
        count[i] = pending_msgs;
    }

    fprintf(stderr, "msgs\tkbps\tus\n");

    uint64_t start_time = get_nanos();
    uint64_t iter = 0;
    uint64_t latency_count = 0;
    uint64_t total_latency = 0;
    uint64_t cur_latency = 0;
    while (run_count < EXEC_LEN)
    {
        iter ++;
        for (int i = 0; i < num_conns; i++)
        {
            int ret = rdma_cq_poll(fd[i], ev, WQSIZE);
            if (ret < 0)
            {
                fprintf(stderr, "%s():%d\n", __func__, __LINE__);
                return -1;
            }
#ifndef NOVERIFY
            for (int j = 0; j < ret; j++)
            {
                if (ev[j].status != RDMA_SUCCESS)
                {
                    fprintf(stderr, "%s():%d id=%u status=%u\n", __func__, __LINE__, ev[j].id, ev[j].status);
                    return -1;
                }

                // calculate latency #TODO: histogram
                cur_latency = read_time(&latency_time[i]);
                histogram(&latency_hist, cur_latency);
                total_latency += cur_latency;
                latency_count++;
            }
#endif
            count[i] += ret;
            compl_msgs += ret;

            int j;
            for (j = 0; j < count[i]; j++)
            {
                int ret = rdma_write(fd[i], msg_len, 0, 0);
                if (ret < 0)
                {
                    fprintf(stderr, "%s():%d\n", __func__, __LINE__);
                    return -1;
                }

                // record rdma_write timing
                assert(save_time(&latency_time[i]) >= 0);
            }
            count[i] -= j;
        }

        if (iter % iterations == 0)
        {
            uint64_t cur_time = get_nanos();
            double diff = (cur_time - start_time)/1000000000.;
            double tpt = (compl_msgs*msg_len*8)/(diff * 1024);
            double latency = (total_latency/1000.)/latency_count;
/*          fprintf(stderr, "Msgs: %lu Bytes: %lu Time: %lf Throughput=%lf Kbps Latency=%lf us\n",
                    compl_msgs, compl_msgs*msg_len, diff, tpt, latency); */
            fprintf(stderr, "%lu\t%lf\t%lf\n", compl_msgs, tpt, latency);

            compl_msgs = 0;
            start_time = cur_time;
            run_count++;
        }
    }

    printf("\nlatency count: %ld\n", latency_count);
    percentile(&latency_hist);
    print_hist(&latency_hist);
    printf("\nconnection %d ended\n", rport);
    return 0;
}
