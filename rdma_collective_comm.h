#ifndef RDMA_COLLECTIVE_COMM_H
#define RDMA_COLLECTIVE_COMM_H

#include <stddef.h>
#include <infiniband/verbs.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// Protocol selection threshold
#define EAGER_THRESHOLD (4 * 1024)  // 4KB threshold for eager vs rendezvous

#define QP_EXCHANGE_PORT_BASE 12345 // Use a known port
#define MR_EXCHANGE_PORT_BASE 18000 // Use a known port

// Supported data types
typedef enum {
    DATATYPE_INT,
    DATATYPE_FLOAT,
    // Add more as needed
} DATATYPE;

// Supported operations
typedef enum {
    OP_SUM,
    OP_MULT
    // Add more as needed
} OPERATION;

// RDMA process group handle
typedef struct {
    int rank;                 // This process's rank in the group - a unique integer identifier assigned to each process in a group of cooperating processes
    int size;                 // Total number of processes
    struct ibv_context *ctx;  // RDMA device context
    struct ibv_pd *pd;        // Protection domain
    struct ibv_cq *cq;        // Completion queue
    struct ibv_qp **qps;      // Array of QPs (one for each neighbor)
    struct ibv_mr *mr_send;   // Memory region for send buffer
    struct ibv_mr *mr_recv;   // Memory region for recv buffer
    void *sendbuf;            // Send buffer
    void *recvbuf;            // Receive buffer
    size_t bufsize;           // Buffer size in bytes
    // Add more fields as needed for pipelining, zero-copy, etc.
    // E.g., chunk size, pipeline depth, neighbor info, etc.
    char **servernames;       // Array of server names/IPs
    // Rendezvous/zero-copy support
    uint32_t *remote_rkeys;      // Array of remote rkeys (one per peer)
    uintptr_t *remote_addrs;     // Array of remote buffer addresses (one per peer)
    uint32_t local_rkey;         // Local rkey (for your own buffer)
    uintptr_t local_addr;        // Local buffer address
} pg_handle_t;

typedef struct {
    uint16_t lid;
    uint32_t qpn;
    uint32_t psn;
} qp_info_t;

typedef struct {
    uint32_t rkey;
    uintptr_t addr;
} mr_info_t;

#ifdef __cplusplus
extern "C" {
#endif

// Connect processes in a ring and set up RDMA resources
int connect_process_group(char *servername, void **pg_handle);

// All-reduce collective operation
int pg_all_reduce(void *sendbuf, void *recvbuf, int count,
                  DATATYPE datatype, OPERATION op, void *pg_handle);

// Close and clean up process group
int pg_close(void *pg_handle);


// Helper: Resolve hostname to IP address
static char* get_ip_from_hostname(const char *hostname) {
    struct addrinfo hints = {0}, *res;
    hints.ai_family = AF_INET;  // IPv4 only
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(hostname, NULL, &hints, &res) != 0) {
        printf("[DEBUG] Failed to resolve hostname: %s\n", hostname);
        return NULL;  // Failed to resolve
    }

    struct sockaddr_in *addr = (struct sockaddr_in*)res->ai_addr;
    char *ip = strdup(inet_ntoa(addr->sin_addr));
    freeaddrinfo(res);
    return ip;
}

// Connect to a hostname:port, return socket fd
static int tcp_connect(const char *hostname, int port) {
    // Resolve hostname to IP
    struct addrinfo hints = {0}, *res;
    hints.ai_family = AF_INET;  // IPv4 only
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(hostname, NULL, &hints, &res) != 0) {
        printf("[DEBUG] Failed to resolve hostname: %s\n", hostname);
        return -1;
    }

    struct sockaddr_in *addr = (struct sockaddr_in*)res->ai_addr;
    char ip_str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &(addr->sin_addr), ip_str, INET_ADDRSTRLEN);

    // Now connect using the IP
    char portstr[16];
    snprintf(portstr, sizeof(portstr), "%d", port);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *res_connect;
    if (getaddrinfo(ip_str, portstr, &hints, &res_connect) != 0) {
        printf("[ERROR] Failed to getaddrinfo for IP: %s\n", ip_str);
        freeaddrinfo(res);
        return -1;
    }

    int sock = -1;
    for (struct addrinfo *rp = res_connect; rp; rp = rp->ai_next) {
        for (int attempt = 0; attempt < 10; ++attempt) { // Try 10 times
            sock = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
            if (sock == -1) continue;
            if (connect(sock, rp->ai_addr, rp->ai_addrlen) == 0) {
                freeaddrinfo(res_connect);
                freeaddrinfo(res);
                return sock;
            }
            close(sock); sock = -1;
            usleep(2000000); // Wait 2s before retry
        }
    }

    printf("[ERROR] Failed to connect to %s:%d after 10 attempts\n", ip_str, port);
    freeaddrinfo(res_connect);
    freeaddrinfo(res);
    return -1;
}

// Listen on a port, return accepted socket fd
static int tcp_listen_accept(int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    bind(sock, (struct sockaddr *)&addr, sizeof(addr));
    listen(sock, 1);
    int client = accept(sock, NULL, NULL);
    close(sock);
    return client;
}

#ifdef __cplusplus
}
#endif

#endif // RDMA_COLLECTIVE_COMM_H
