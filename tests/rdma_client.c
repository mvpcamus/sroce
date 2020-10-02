#include <stdio.h>
#include <stdint.h>

#include <tas_rdma.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

int main()
{
  const char ip[] = "10.0.0.2";
  rdma_init();
  struct sockaddr_in remoteaddr;
  remoteaddr.sin_family = AF_INET;
  remoteaddr.sin_addr.s_addr = inet_addr(ip);
  remoteaddr.sin_port = htons(5005);

  void *mr_base;
  uint32_t mr_len;

  int fd = rdma_connect(&remoteaddr, &mr_base, &mr_len);

  if (fd < 0)
    fprintf(stderr, "Connection failed\n");

  getchar();

  char *ptr = (char*) mr_base;
  for (int i = 0; i < mr_len; i++)
  {
    fprintf(stderr, "%c", ptr[i]);
  }
  fprintf(stderr, "\n");

  return 0;
}
