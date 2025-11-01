#include <config.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#include <rte_config.h>
#include <rte_mbuf.h>
#include <rte_ring_core.h>

#include "hash.h"
#include "netdev-ringif-utils.h"
#include "openvswitch/hmap.h"
#include "ovs-thread.h"
#include "util.h"

#define MAX_NAME_LEN (RTE_RING_NAMESIZE - sizeof("_r0") + 1)

struct ring_usage_record {
  struct hmap_node node;
  char key[RTE_MEMZONE_NAMESIZE];

  bool is_release_by_ringif;
  bool is_register;
  size_t num_ring;
  time_t last_update_time; // 记录is_release_by_ringif状态最后更新的时间戳
  struct rte_ring *ring_arr[];
};

static struct hmap record_map = HMAP_INITIALIZER(&record_map);
static struct ovs_mutex hmap_mutex = OVS_MUTEX_INITIALIZER;

/**
 * 释放rte_ring及其包含的所有mbuf
 * @param ring 要释放的rte_ring指针
 */
static inline void free_ring_with_mbufs(struct rte_ring *ring) {
  if (!ring) {
    return;
  }

  // 在释放ring之前，先释放ring中所有的mbuf
  void *mbuf = NULL;
  while (rte_ring_sc_dequeue(ring, &mbuf) == 0) {
    if (mbuf) {
      rte_pktmbuf_free(mbuf);
    }
  }

  // 然后释放ring本身
  rte_ring_free(ring);
}

// free_all_rings_with_mbufs 释放所有的ring
static inline void free_all_rings_with_mbufs(struct rte_ring *ring_arr[],
                                             size_t num) {
  if (!ring_arr) {
    return;
  }

  // 释放所有rte_ring
  size_t i;
  for (i = 0; i < num; i++) {
    free_ring_with_mbufs(ring_arr[i]);
    ring_arr[i] = NULL;
  }
}

// 请求类型字符串前缀
#define REG_PREFIX "reg:"
#define UNREG_PREFIX "unreg:"
#define LIST_COMMAND "list"

// 响应消息常量
#define RESP_ERROR_INVALID_REQUEST "Error: Invalid request\n"
#define RESP_ERROR_MISSING_NAME "Error: Missing record name\n"
#define RESP_SUCCESS_REGISTERED "Success: Registered\n"
#define RESP_ERROR_NOT_FOUND "Error: Record not found\n"
#define RESP_SUCCESS_UNREGISTERED "Success: Unregistered\n"
#define RESP_END_OF_LIST "End of list\n"
#define RESP_ERROR_UNKNOWN_COMMAND "Error: Unknown command\n"

struct rte_ring **alloc_ring(const char *name, int socket_id,
                             size_t queue_size) {
  if (!name || queue_size > 10) {
    return NULL;
  }
  char ring_name[RTE_MEMZONE_NAMESIZE];
  size_t i, index = 0;
  struct rte_ring *ring_ptr;
  size_t hash = hash_string(name, 0);
  struct ring_usage_record *exist_record;
  size_t alloc_size = sizeof(struct rte_ring *) * queue_size * 2 +
                      sizeof(struct ring_usage_record);
  // 查询是否存在已经分配的ring
  ovs_mutex_lock(&hmap_mutex);
  HMAP_FOR_EACH_IN_BUCKET(exist_record, node, hash, &record_map) {
    if (strncmp(exist_record->key, name, MAX_NAME_LEN) == 0) {
      // 找到已存在的记录，返回其ring数组
      ovs_mutex_unlock(&hmap_mutex);
      exist_record->is_release_by_ringif = false;
      exist_record->last_update_time = time(NULL); // 更新时间戳
      return exist_record->ring_arr;
    }
  }

  struct ring_usage_record *record = xmalloc(alloc_size);
  // init
  strncpy(record->key, name, MAX_NAME_LEN);
  record->is_release_by_ringif = false;
  record->is_register = false;
  record->num_ring = queue_size * 2;
  record->last_update_time = time(NULL); // 设置初始时间戳
  for (i = 0; i < queue_size; i++) {
    // 创建接收ring
    snprintf(ring_name, RTE_MEMZONE_NAMESIZE, "%s_r%lu", name, i);
    ring_ptr = rte_ring_create(ring_name, 4096, socket_id,
                               RING_F_SP_ENQ | RING_F_SC_DEQ);
    if (ring_ptr == NULL) {
      goto clean_ring;
    }
    record->ring_arr[index] = ring_ptr;
    index++;

    // 创建发送ring
    snprintf(ring_name, RTE_MEMZONE_NAMESIZE, "%s_t%lu", name, i);
    ring_ptr = rte_ring_create(ring_name, 4096, socket_id, RING_F_SC_DEQ);
    if (ring_ptr == NULL) {
      goto clean_ring;
    }
    record->ring_arr[index] = ring_ptr;
    index++;
  }
  // add map
  hmap_insert(&record_map, &record->node, hash);
  ovs_mutex_unlock(&hmap_mutex);

  return record->ring_arr;
clean_ring:
  ovs_mutex_unlock(&hmap_mutex);
  free_all_rings_with_mbufs(record->ring_arr, index);

  // 释放record内存
  free(record);
  return NULL;
}

// free_ring 根据name释放的ring
void free_ring(const char *name) {
  if (!name) {
    return;
  }

  ovs_mutex_lock(&hmap_mutex);

  size_t hash = hash_string(name, 0);
  struct ring_usage_record *record = NULL;

  // 查找对应的记录
  HMAP_FOR_EACH_IN_BUCKET(record, node, hash, &record_map) {
    if (strncmp(record->key, name, MAX_NAME_LEN) == 0) {
      // 找到匹配的记录
      if (!record->is_register) {
        // is_register为false时，直接释放资源
        hmap_remove(&record_map, &record->node);
        ovs_mutex_unlock(&hmap_mutex);

        // 释放所有rte_ring
        free_all_rings_with_mbufs(record->ring_arr, record->num_ring);

        // 释放record内存
        free(record);
      } else {
        // is_register为true时，仅标记为待释放
        record->is_release_by_ringif = true;
        record->last_update_time = time(NULL); // 更新时间戳
        ovs_mutex_unlock(&hmap_mutex);
      }
      return;
    }
  }

  // 没有找到匹配的记录
  ovs_mutex_unlock(&hmap_mutex);
}

// 定时清理函数，清理is_release_by_ringif为true且长时间未更新的record
static void cleanup_old_records(void) {
  time_t now = time(NULL);
  struct ring_usage_record *record, *next;

  ovs_mutex_lock(&hmap_mutex);

  HMAP_FOR_EACH_SAFE(record, next, node, &record_map) {
    if (record->is_release_by_ringif &&
        (now - record->last_update_time) > 60 * 60 * 24) {

      // 释放所有rte_ring
      free_all_rings_with_mbufs(record->ring_arr, record->num_ring);

      // 从哈希表中移除并释放内存
      hmap_remove(&record_map, &record->node);
      free(record);
    }
  }

  ovs_mutex_unlock(&hmap_mutex);
}

// 处理客户端请求
static void handle_client_request(int client_fd) {
  char buffer[1024];
  ssize_t bytes_read;

  // 读取请求字符串
  bytes_read = read(client_fd, buffer, sizeof(buffer) - 1);
  if (bytes_read <= 0) {
    write(client_fd, RESP_ERROR_INVALID_REQUEST,
          strlen(RESP_ERROR_INVALID_REQUEST));
    close(client_fd);
    return;
  }

  // 确保字符串以null结尾并移除换行符
  buffer[bytes_read] = '\0';
  if (buffer[bytes_read - 1] == '\n') {
    buffer[bytes_read - 1] = '\0';
  }

  // 处理注册请求: "reg:record-name"
  if (strstr(buffer, REG_PREFIX) == buffer) {
    const char *name = buffer + strlen(REG_PREFIX);

    if (*name == '\0') {
      write(client_fd, RESP_ERROR_MISSING_NAME,
            strlen(RESP_ERROR_MISSING_NAME));
      close(client_fd);
      return;
    }

    size_t hash = hash_string(name, 0);
    struct ring_usage_record *record = NULL;

    ovs_mutex_lock(&hmap_mutex);

    // 查找是否已存在
    HMAP_FOR_EACH_IN_BUCKET(record, node, hash, &record_map) {
      if (strncmp(record->key, name, MAX_NAME_LEN) == 0) {
        // 找到记录，标记为已注册
        record->is_register = true;
        record->last_update_time = time(NULL);
        write(client_fd, RESP_SUCCESS_REGISTERED,
              strlen(RESP_SUCCESS_REGISTERED));
        ovs_mutex_unlock(&hmap_mutex);
        close(client_fd);
        return;
      }
    }

    ovs_mutex_unlock(&hmap_mutex);
    write(client_fd, RESP_ERROR_NOT_FOUND, strlen(RESP_ERROR_NOT_FOUND));
  }
  // 处理反注册请求: "unreg:record-name"
  else if (strstr(buffer, UNREG_PREFIX) == buffer) {
    const char *name = buffer + strlen(UNREG_PREFIX);

    if (*name == '\0') {
      write(client_fd, RESP_ERROR_MISSING_NAME,
            strlen(RESP_ERROR_MISSING_NAME));
      close(client_fd);
      return;
    }

    size_t hash = hash_string(name, 0);
    struct ring_usage_record *record = NULL;

    ovs_mutex_lock(&hmap_mutex);

    // 查找记录
    HMAP_FOR_EACH_IN_BUCKET(record, node, hash, &record_map) {
      if (strncmp(record->key, name, MAX_NAME_LEN) == 0) {
        // 找到记录，取消注册
        record->is_register = false;
        record->last_update_time = time(NULL);

        // 如果同时is_release_by_ringif为true，则立即清理
        if (record->is_release_by_ringif) {
          // 释放所有rte_ring
          free_all_rings_with_mbufs(record->ring_arr, record->num_ring);

          // 从哈希表中移除并释放内存
          hmap_remove(&record_map, &record->node);
          free(record);
          record = NULL;
        }

        write(client_fd, RESP_SUCCESS_UNREGISTERED,
              strlen(RESP_SUCCESS_UNREGISTERED));
        ovs_mutex_unlock(&hmap_mutex);
        close(client_fd);
        return;
      }
    }

    ovs_mutex_unlock(&hmap_mutex);
    write(client_fd, RESP_ERROR_NOT_FOUND, strlen(RESP_ERROR_NOT_FOUND));
  }
  // 处理查询列表请求: "list"
  else if (strcmp(buffer, LIST_COMMAND) == 0) {
    ovs_mutex_lock(&hmap_mutex);
    // 检查是否有记录
    if (!hmap_is_empty(&record_map)) {
      struct ring_usage_record *record;
      HMAP_FOR_EACH(record, node, &record_map) {
        char record_info[512];
        time_t last_update = record->last_update_time;
        struct tm time_info;
        char time_str[20];
        localtime_r(&last_update, &time_info);
        strftime(time_str, sizeof(time_str), "%Y-%m-%d %H:%M:%S", &time_info);

        snprintf(record_info, sizeof(record_info),
                 "Name: %s, Register: %s, Release: %s, Rings: %lu, Last "
                 "Update: %s\n",
                 record->key, record->is_register ? "true" : "false",
                 record->is_release_by_ringif ? "true" : "false", record->num_ring,
                 time_str);
        write(client_fd, record_info, strlen(record_info));
      }
    }

    ovs_mutex_unlock(&hmap_mutex);
    // 打印结束标记
    write(client_fd, RESP_END_OF_LIST, strlen(RESP_END_OF_LIST));
  }
  // 未知请求
  else {
    write(client_fd, RESP_ERROR_UNKNOWN_COMMAND,
          strlen(RESP_ERROR_UNKNOWN_COMMAND));
  }

  close(client_fd);
}

// runtime_main 运行时线程主函数
int runtime_main(const char *unix_sock_path) {
  int server_fd, epoll_fd;
  struct sockaddr_un addr;
  struct epoll_event ev, events[10];
  time_t last_cleanup_time = time(NULL);

  // 创建Unix socket
  server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (server_fd < 0) {
    perror("socket creation failed");
    return EIO; // 返回EIO表示I/O错误
  }

  // 设置socket为非阻塞
  int flags = fcntl(server_fd, F_GETFL, 0);
  fcntl(server_fd, F_SETFL, flags | O_NONBLOCK);

  // 清理可能存在的旧socket文件
  unlink(unix_sock_path);

  // 绑定地址
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, unix_sock_path, sizeof(addr.sun_path) - 1);

  if (bind(server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    perror("bind failed");
    close(server_fd);
    return EADDRINUSE; // 返回EADDRINUSE表示地址已被占用
  }

  // 开始监听
  if (listen(server_fd, 5) < 0) {
    perror("listen failed");
    close(server_fd);
    unlink(unix_sock_path);
    return EINVAL; // 返回EINVAL表示参数无效
  }

  // 创建epoll实例
  epoll_fd = epoll_create1(0);
  if (epoll_fd < 0) {
    perror("epoll_create1 failed");
    close(server_fd);
    unlink(unix_sock_path);
    return ENOMEM; // 返回ENOMEM表示内存分配失败
  }

  // 添加server socket到epoll
  ev.events = EPOLLIN;
  ev.data.fd = server_fd;
  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) < 0) {
    perror("epoll_ctl: server_fd failed");
    close(epoll_fd);
    close(server_fd);
    unlink(unix_sock_path);
    return EINVAL; // 返回EINVAL表示参数无效
  }

  // 主事件循环
  while (1) {
    int nfds =
        epoll_wait(epoll_fd, events, 10, 1000); // 1秒超时，用于检查定时任务

    // 处理事件
    for (int i = 0; i < nfds; i++) {
      if (events[i].data.fd == server_fd) {
        // 接受新连接
        int client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0) {
          perror("accept failed");
          continue;
        }

        // 处理客户端请求
        handle_client_request(client_fd);
      }
    }

    // 检查是否需要执行清理任务（每10分钟）
    time_t now = time(NULL);
    if (now - last_cleanup_time >= 10 * 60) {
      cleanup_old_records();
      last_cleanup_time = now;
    }
  }

  // 清理资源（实际上不会执行到这里，因为前面是无限循环）
  close(epoll_fd);
  close(server_fd);
  unlink(unix_sock_path);
  return 0; // 返回0表示正常退出（虽然不会执行到）
}