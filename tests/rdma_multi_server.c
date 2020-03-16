#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <tas_rdma.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#define NUM_CONNECTIONS     65535
#define MESSAGE_SIZE        64
#define NUM_PENDING_MSGS    63

int fd[NUM_CONNECTIONS];
void* mr_base[NUM_CONNECTIONS];
uint32_t mr_len[NUM_CONNECTIONS];

int main(int argc, char* argv[])
{
    assert(argc == 4);

    char* ip = argv[1];
    int port = atoi(argv[2]);
    int num_connections = atoi(argv[3]);
    assert(num_connections < NUM_CONNECTIONS);
    fprintf(stderr, "Params: ip=%s port=%d conns=%d\n", ip, port, num_connections);

    rdma_init();
    struct sockaddr_in localaddr, remoteaddr;
    localaddr.sin_family = AF_INET;
    localaddr.sin_addr.s_addr = inet_addr(ip);
    localaddr.sin_port = htons(port);
    int lfd = rdma_listen(&localaddr, 1024);

    for (int i = 0; i < num_connections; i++)
    {
        fd[i] = rdma_accept(lfd, &remoteaddr, &mr_base[i], &mr_len[i]);

        if (fd[i] < 0)
        {
            fprintf(stderr, "Connection failed\n");
            return -1;
        }
    }

    fprintf(stderr, "Connections established: %d\n", num_connections);
    getchar();
    return 0;
}