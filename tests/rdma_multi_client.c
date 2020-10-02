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

int fd[NUM_CONNECTIONS];
void* mr_base[NUM_CONNECTIONS];
uint32_t mr_len[NUM_CONNECTIONS];
int count[NUM_CONNECTIONS];
struct rdma_wqe ev[WQSIZE];
uint64_t latency[8 * WQSIZE * 30];

#define EXEC_LEN 120
#define ITERATION 5000000
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

                if (ev[j].id % 200 == 0)
                {
                    total_latency += (rdtsc() - latency[ev[j].id]);
                    latency_count++;
                }
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

                if (ret % 200 == 0)
                {
                    latency[ret] = rdtsc();
                }
            }
            count[i] -= j;
        }

        if (iter % iterations == 0)
        {
            uint64_t cur_time = get_nanos();
            double diff = (cur_time - start_time)/1000000000.;
            double tpt = (compl_msgs*msg_len*8)/(diff * 1024);
            double latency = (total_latency/3000.)/latency_count;
/*          fprintf(stderr, "Msgs: %lu Bytes: %lu Time: %lf Throughput=%lf Kbps Latency=%lf us\n",
                    compl_msgs, compl_msgs*msg_len, diff, tpt, latency); */
            fprintf(stderr, "%lu\t%lf\t%lf\n", compl_msgs, tpt, latency);

            compl_msgs = 0;
            start_time = cur_time;
            run_count++;
        }
    }
    printf("\nconnection %d ended\n", rport);
    return 0;
}
