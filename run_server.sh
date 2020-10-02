#!/bin/bash
cd "${0%/*}"

echo number of servers?
read servers
echo number of connections?
read conns

NUMA_0="0,2,4,6,8,10,12,14,16,18,20,22,24,26,28"
NUMA_1="1,3,5,7,9,11,13,15,17,19,21,23,25,27,29"
port_s=5000
for i in `seq 1 $servers`;
do
  let port=(port_s+i)
  sudo taskset -c $NUMA_1 ./tests/rdma_multi_server 10.0.0.3 $port $conns &
done
exit
