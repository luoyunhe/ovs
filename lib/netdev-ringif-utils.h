#ifndef NETDEV_RINGIF_UTILS_H
#define NETDEV_RINGIF_UTILS_H

#include <rte_config.h>
#include <rte_ring.h>
struct rte_ring **alloc_ring(const char *name, int socket_id,
                             size_t queue_size);

void free_ring(const char *name);

int runtime_main(const char *unix_sock_path);

static inline struct rte_ring *get_tx_ring(struct rte_ring *ring_arr[], int qid) {
  return ring_arr[qid * 2 + 1];
}

static inline struct rte_ring *get_rx_ring(struct rte_ring *ring_arr[], int qid) {
  return ring_arr[qid * 2];
}
#endif