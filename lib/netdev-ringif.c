#include <rte_config.h>
#include <rte_mbuf_core.h>
#include <rte_mempool.h>
#include <rte_ring.h>
#include <rte_ring_core.h>

#include "dp-packet.h"
#include "netdev-provider.h"
#include "netdev-ringif-utils.h"
#include "netdev-ringif.h"
#include "openvswitch/compiler.h"
#include "openvswitch/types.h"
#include "openvswitch/util.h"
#include "openvswitch/vlog.h"
#include "ovs-thread.h"

#include <config.h>
#include <errno.h>
#include <signal.h>
#include <stdbool.h>
#include <stdlib.h>

extern size_t dpdk_copy_batch_to_mbuf(struct netdev *netdev,
                                      struct dp_packet_batch *batch);

VLOG_DEFINE_THIS_MODULE(netdev_ringif);

static struct vlog_rate_limit rl = VLOG_RATE_LIMIT_INIT(5, 20);

#define QUEUE_SIZE 8
#define SOCKET0 0

static struct ovsthread_once ringif_thread_once = OVSTHREAD_ONCE_INITIALIZER;

static struct rte_mempool *mp;

const char *socket_path = "/var/run/openvswitch/ringif.sock";

struct netdev_rxq_ringif {
  struct netdev_rxq up;
  int fd;
};

struct netdev_ringif {
  struct netdev up;
  struct ovs_mutex mutex;

  struct eth_addr mac;
  int socket_id;
  int requested_socket_id;

  uint16_t tx_buf_num;

  uint16_t rx_buf_num;

  uint64_t rx_packets;
  uint64_t rx_bytes;
  uint64_t tx_packets;
  uint64_t tx_bytes;

  unsigned int ifi_flags;
  struct rte_ring **ring;
};

static struct netdev_ringif *netdev_ringif_cast(const struct netdev *netdev) {
  return CONTAINER_OF(netdev, struct netdev_ringif, up);
}

static struct netdev_rxq_ringif *
netdev_rxq_memif_cast(const struct netdev_rxq *rx) {
  return CONTAINER_OF(rx, struct netdev_rxq_ringif, up);
}

static void *ringif_thread(void *f_ OVS_UNUSED) {
  int err = 0;
  ovsrcu_quiesce_start();
  err = runtime_main(socket_path);

  ovs_abort(err, "ringif runtime return");

  return NULL;
}

static int netdev_ringif_init(void) {
  VLOG_INFO("%s", __func__);
  if (ovsthread_once_start(&ringif_thread_once)) {
    mp = rte_pktmbuf_pool_create("ringif_mempool", 8192, 256, 0,
                                 RTE_MBUF_DEFAULT_BUF_SIZE, SOCKET0);
    if (mp == NULL) {
      ovs_abort(ENOMEM, "Failed to create mbuf pool");
    }
    ovs_thread_create("ringif_conn", ringif_thread, NULL);
    ovsthread_once_done(&ringif_thread_once);
  }
  return 0;
}

static void netdev_ringif_destruct(struct netdev *netdev) {
  struct netdev_ringif *dev = netdev_ringif_cast(netdev);
  VLOG_INFO("%s", __func__);
  free_ring(netdev->name);
  ovs_mutex_destroy(&dev->mutex);
}

static struct netdev *netdev_ringif_alloc(void) {
  struct netdev_ringif *dev;

  VLOG_INFO("%s", __func__);

  dev = xzalloc(sizeof *dev);
  if (dev) {
    return &dev->up;
  }
  return NULL;
}

static void netdev_ringif_dealloc(struct netdev *netdev) {
  struct netdev_ringif *dev = netdev_ringif_cast(netdev);

  free(dev);
}

static void OVS_UNUSED vlog_hex_dump(char *ptr, int size) {
  struct ds s;
  int i;

  ds_init(&s);

  for (i = 0; i < size; i++) {
    ds_put_hex(&s, ptr++, 1);
  }
  VLOG_INFO("%s", ds_cstr(&s));
  ds_destroy(&s);
}

static size_t netdev_dpdk_common_send(struct netdev *netdev,
                                      struct dp_packet_batch *batch) {
  size_t cnt = batch->count;
  struct dp_packet *packet;
  bool need_copy = false;

  DP_PACKET_BATCH_FOR_EACH(i, packet, batch) {
    if (packet->source != DPBUF_DPDK) {
      need_copy = true;
      break;
    }
  }

  /* Copy dp-packets to mbufs. */
  if (OVS_UNLIKELY(need_copy)) {
    cnt = dpdk_copy_batch_to_mbuf(netdev, batch);
  }
  return cnt;
}

static int netdev_ringif_batch_send(struct netdev *netdev, int qid,
                                    struct dp_packet_batch *batch,
                                    bool concurrent_txq OVS_UNUSED) {
  struct rte_mbuf **pkts;
  struct netdev_ringif *dev = netdev_ringif_cast(netdev);
  struct rte_ring *ring = get_tx_ring(dev->ring, qid);

  int cnt = netdev_dpdk_common_send(netdev, batch);

  int nb_tx =
      rte_ring_enqueue_burst(ring, (void **)batch->packets, batch->count, NULL);

  if (OVS_UNLIKELY(nb_tx != cnt)) {
    pkts = (struct rte_mbuf **)batch->packets;
    /* Free buffers, which we couldn't transmit. */
    rte_pktmbuf_free_bulk(&pkts[nb_tx], cnt - nb_tx);
  }

  return 0;
}

static int netdev_ringif_rxq_recv(struct netdev_rxq *rxq,
                                  struct dp_packet_batch *batch,
                                  int *qfill OVS_UNUSED) {
  struct netdev_ringif *dev = netdev_ringif_cast(rxq->netdev);
  int qid = rxq->queue_id;
  struct rte_ring *ring = get_rx_ring(dev->ring, qid);

  int nb_rx = rte_ring_dequeue_burst(ring, (void **)batch->packets,
                                     NETDEV_MAX_BURST, NULL);
  if (!nb_rx) {
    return EAGAIN;
  }

  dev->rx_buf_num += nb_rx;
  dev->rx_packets += nb_rx;

  // 更新接收字节计数
  for (int i = 0; i < nb_rx; i++) {
    struct rte_mbuf *mbuf = (struct rte_mbuf *)batch->packets[i];
    dev->rx_bytes += rte_pktmbuf_pkt_len(mbuf);
  }

  batch->count = nb_rx;
  return 0;
}

static int netdev_ringif_construct(struct netdev *netdev) {
  struct netdev_ringif *dev = netdev_ringif_cast(netdev);

  VLOG_INFO("%s", __func__);

  /* Set netdev. */
  netdev->n_rxq = QUEUE_SIZE;
  netdev->n_txq = QUEUE_SIZE;
  ovs_mutex_init(&dev->mutex);
  int socket_id = rte_lcore_to_socket_id(rte_get_main_lcore());
  dev->socket_id = socket_id < 0 ? SOCKET0 : socket_id;
  dev->requested_socket_id = dev->socket_id;

  struct rte_ring **ring = alloc_ring(netdev->name, dev->socket_id, QUEUE_SIZE);
  if (ring == NULL) {
    return ENOMEM;
  }
  dev->ring = ring;

  netdev_request_reconfigure(netdev);
  return 0;
}

static int netdev_ringif_update_flags(struct netdev *netdev,
                                      enum netdev_flags off,
                                      enum netdev_flags on,
                                      enum netdev_flags *old_flagsp) {
  struct netdev_ringif *dev = netdev_ringif_cast(netdev);

  *old_flagsp = dev->ifi_flags;
  /* Only support NETDEV_UP. */
  if (on & NETDEV_UP) {
    dev->ifi_flags |= NETDEV_UP;
  }
  if (off & NETDEV_UP) {
    dev->ifi_flags &= ~NETDEV_UP;
  }

  return 0;
}

static int netdev_ringif_get_etheraddr(const struct netdev *netdev OVS_UNUSED,
                                       struct eth_addr *mac) {
  struct netdev_ringif *dev = netdev_ringif_cast(netdev);

  *mac = dev->mac;
  return 0;
}

static int netdev_ringif_set_etheraddr(struct netdev *netdev,
                                       const struct eth_addr mac) {
  struct netdev_ringif *dev = netdev_ringif_cast(netdev);

  VLOG_DBG("set mac " ETH_ADDR_FMT, ETH_ADDR_ARGS(mac));
  memcpy(&dev->mac, &mac, sizeof mac);
  return 0;
}

static int netdev_ringif_rxq_construct(struct netdev_rxq *rxq OVS_UNUSED) {

  return 0;
}

static void netdev_ringif_rxq_dealloc(struct netdev_rxq *rxq_) {
  struct netdev_rxq_ringif *rx = netdev_rxq_memif_cast(rxq_);
  free(rx);
}

static struct netdev_rxq *netdev_ringif_rxq_alloc(void) {
  struct netdev_rxq_ringif *rx = xzalloc(sizeof *rx);
  return &rx->up;
}

static int netdev_ringif_get_stats(const struct netdev *netdev,
                                   struct netdev_stats *stats) {
  struct netdev_ringif *dev = netdev_ringif_cast(netdev);

  ovs_mutex_lock(&dev->mutex);

  stats->rx_packets = dev->rx_packets;
  stats->rx_bytes = dev->rx_bytes;
  stats->tx_packets = dev->tx_packets;
  stats->tx_bytes = dev->tx_bytes;

  stats->rx_errors = 0;
  stats->tx_errors = 0;
  stats->rx_dropped = 0;
  stats->tx_dropped = 0;

  ovs_mutex_unlock(&dev->mutex);
  return 0;
}

static int netdev_ringif_set_config(struct netdev *netdev OVS_UNUSED,
                                    const struct smap *args OVS_UNUSED,
                                    char **errp OVS_UNUSED) {
  return 0;
}

static int netdev_ringif_get_config(const struct netdev *netdev OVS_UNUSED,
                                    struct smap *args OVS_UNUSED) {

  return 0;
}

static int netdev_ringif_reconfigure(struct netdev *netdev) {
  VLOG_INFO("%s", __func__);

  netdev->n_rxq = QUEUE_SIZE;
  netdev->n_txq = QUEUE_SIZE;

  netdev_change_seq_changed(netdev);

  return 0;
}

static void netdev_ringif_rxq_destruct(struct netdev_rxq *rxq OVS_UNUSED) {}

static const struct netdev_class ringif_class = {
    .type = "ringif",
    .is_pmd = true,
    .init = netdev_ringif_init,
    .construct = netdev_ringif_construct,
    .destruct = netdev_ringif_destruct,
    .alloc = netdev_ringif_alloc,
    .dealloc = netdev_ringif_dealloc,
    .update_flags = netdev_ringif_update_flags,
    .get_etheraddr = netdev_ringif_get_etheraddr,
    .set_etheraddr = netdev_ringif_set_etheraddr,
    .get_stats = netdev_ringif_get_stats,
    /* Rx and Tx */
    .rxq_alloc = netdev_ringif_rxq_alloc,
    .rxq_dealloc = netdev_ringif_rxq_dealloc,
    .rxq_construct = netdev_ringif_rxq_construct,
    .rxq_recv = netdev_ringif_rxq_recv,
    .send = netdev_ringif_batch_send,
    .rxq_destruct = netdev_ringif_rxq_destruct,
    /* Config. */
    .set_config = netdev_ringif_set_config,
    .get_config = netdev_ringif_get_config,
    .reconfigure = netdev_ringif_reconfigure,
};

void netdev_ringif_register(void) { netdev_register_provider(&ringif_class); }
