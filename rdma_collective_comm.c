#include "rdma_collective_comm.h"


// Helper: Convert datatype enum to size
/**
 * @brief Convert datatype enum to size
 * @param dtype: datatype enum value
 * @return size in bytes for the given datatype
 */
static size_t datatype_size(DATATYPE dtype) {
    switch (dtype) {
        case DATATYPE_INT: return sizeof(int);
        case DATATYPE_FLOAT: return sizeof(float);
        default: return 0;
    }
}

// Helper: Parse comma-separated server list into array
// We expect the server list to be a comma-separated list of hostnames or IP addresses
/**
 * @brief Parse a comma-separated list of hostnames or IP addresses into an array of strings
 * @param list: comma-separated list of hostnames or IP addresses (e.g. "server1,server2,server3")
 * @param out_names: pointer to pointer to array of strings that will be set to the array of strings
 * @param out_count: pointer to integer that will be set to the number of servers
 * @return 0 on success, -1 on failure
 */
static int parse_server_list(const char *list, char ***out_names, int *out_count) {
    if (!list || !out_names || !out_count) return -1;
    // Count commas to determine number of servers
    int count = 1;
    for (const char *p = list; *p; ++p) if (*p == ',') ++count;
    char **names = (char **)calloc(count, sizeof(char *));
    if (!names) return -1;
    char *copy = strdup(list);  // strdup duplicates the string pointed to by list
    if (!copy) { free(names); return -1; }
    int idx = 0;
    char *token = strtok(copy, ",");
    while (token && idx < count) {
        names[idx++] = strdup(token);
        token = strtok(NULL, ",");
    }
    free(copy);
    *out_names = names;
    *out_count = count;
    return 0;
}

// Helper to clean up all RDMA resources in handle
/**
 * @brief Clean up all RDMA resources in the process group handle
 * @param handle: pointer to the process group handle
 */
static void cleanup_pg_handle(pg_handle_t *handle) {
    if (!handle) return;
    if (handle->mr_send) ibv_dereg_mr(handle->mr_send);
    if (handle->mr_recv) ibv_dereg_mr(handle->mr_recv);
    if (handle->sendbuf) free(handle->sendbuf);
    if (handle->recvbuf) free(handle->recvbuf);
    if (handle->qps) {
        for (int i = 0; i < 2; ++i) {
            if (handle->qps[i]) ibv_destroy_qp(handle->qps[i]);
        }
        free(handle->qps);
    }
    if (handle->cq) ibv_destroy_cq(handle->cq);
    if (handle->pd) ibv_dealloc_pd(handle->pd);
    if (handle->ctx) ibv_close_device(handle->ctx);
    if (handle->remote_rkeys) free(handle->remote_rkeys);
    if (handle->remote_addrs) free(handle->remote_addrs);
    if (handle->servernames) {
        for (int i = 0; i < handle->size; ++i) free(handle->servernames[i]);
        free(handle->servernames);
    }
    free(handle);
}

// Helper to transition a QP to RTR(ready to receive) and RTS(ready to send)
/**
 * @brief Connect a QP to a remote peer
 * @param qp: pointer to the QP to connect
 * @param local: local QP info (lid, qpn, psn)
 * @param remote: remote QP info (lid, qpn, psn)
 * @return 0 on success, -1 on failure
 */
static int connect_qp(struct ibv_qp *qp, qp_info_t *local, qp_info_t *remote) {
    struct ibv_qp_attr attr;
    int flags;

    // INIT
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.pkey_index = 0;
    attr.port_num = 1;
    attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    if (ibv_modify_qp(qp, &attr, flags)) {
        perror("Failed to move QP to INIT");
        return -1;
    }

    // RTR
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_4096;
    attr.dest_qp_num = remote->qpn;
    attr.rq_psn = remote->psn;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    memset(&attr.ah_attr, 0, sizeof(attr.ah_attr));
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = remote->lid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = 1;

    flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
            IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
            IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    if (ibv_modify_qp(qp, &attr, flags)) {
        perror("Failed to modify QP to RTR");
        return -1;
    }

    // Move to RTS
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = local->psn;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.max_rd_atomic = 1;
    flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;

    if (ibv_modify_qp(qp, &attr, flags)) {
        perror("Failed to modify QP to RTS");
        fprintf(stderr, "errno: %d\n", errno);
        return -1;
    }
    return 0;
}

// Connect processes in a ring and set up RDMA resources
/**
 * @brief Connect processes in a ring and set up RDMA resources.
 * This function initializes RDMA resources, connects to neighbors, and prepares the process group handle.
 * @param servername: name of the server to connect to
 * @param pg_handle: pointer to the process group handle
 * @return 0 on success, -1 on failure
 */
int connect_process_group(char *servername, void **pg_handle) {
    // Parse server list
    char **names = NULL;
    int size = 0;
    if (parse_server_list(servername, &names, &size) != 0) {
        fprintf(stderr, "Failed to parse server list\n");
        return -1;
    }
    // Get this host's name
    char myhost[256];

    // gethostname is a system call that gets the hostname of the current process (e.g. "mlxstud01")
    if (gethostname(myhost, sizeof(myhost)) != 0) {
        perror("gethostname failed");
        // Clean up
        for (int i = 0; i < size; ++i) free(names[i]);
        free(names);
        return -1;
    }
    myhost[sizeof(myhost)-1] = '\0'; // ensure the string is null-terminated

    // Determine rank - support multiple processes per node using LOCAL_RANK
    int rank = -1;
    int local_rank = 0;
    char *local_rank_env = getenv("LOCAL_RANK");
    if (local_rank_env) {
        local_rank = atoi(local_rank_env);
    }
    int found = 0;
    for (int i = 0; i < size; ++i) {
        if (strcmp(myhost, names[i]) == 0) {
            if (found == local_rank) {
                rank = i;
                break;
            }
            found++;
        }
    }
    if (rank == -1) {
        fprintf(stderr, "Host %s with LOCAL_RANK=%d not found in server list (found %d matches)\n", myhost, local_rank, found);
        for (int i = 0; i < size; ++i) free(names[i]);
        free(names);
        return -1;
    }
    // Allocate and fill handle - this is a pointer to a struct that will be used to store the RDMA resources for this process
    pg_handle_t *handle = (pg_handle_t *)calloc(1, sizeof(pg_handle_t));
    if (!handle) {
        for (int i = 0; i < size; ++i) free(names[i]);
        free(names);
        return -1;
    }
    handle->rank = rank;
    handle->size = size;
    handle->servernames = names;
    handle->remote_rkeys = calloc(size, sizeof(uint32_t));
    handle->remote_addrs = calloc(size, sizeof(uintptr_t));

    *pg_handle = handle;

    // 1. Open RDMA device (get ibv_context)
    int num_devices = 0;
    struct ibv_device **dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list || num_devices == 0) {
        cleanup_pg_handle(handle);
        fprintf(stderr, "No RDMA devices found\n");
        return -1;
    }
    handle->ctx = ibv_open_device(dev_list[0]);
    ibv_free_device_list(dev_list);
    if (!handle->ctx) {
        cleanup_pg_handle(handle);
        fprintf(stderr, "Failed to open RDMA device\n");
        return -1;
    }

    // 2. Allocate Protection Domain
    handle->pd = ibv_alloc_pd(handle->ctx);
    if (!handle->pd) {
        cleanup_pg_handle(handle);
        fprintf(stderr, "Failed to allocate PD\n");
        return -1;
    }

    // 3. Create Completion Queue
    handle->cq = ibv_create_cq(handle->ctx, 16, NULL, NULL, 0);
    if (!handle->cq) {
        cleanup_pg_handle(handle);
        fprintf(stderr, "Failed to create CQ\n");
        return -1;
    }

    // 3.5. Create QPs for left and right neighbors in the ring
    handle->qps = calloc(2, sizeof(struct ibv_qp *)); // [0]=left, [1]=right
    if (!handle->qps) {
        cleanup_pg_handle(handle);
        fprintf(stderr, "Failed to allocate QP array\n");
        return -1;
    }
    struct ibv_qp_init_attr qp_init_attr = {
            .send_cq = handle->cq,
            .recv_cq = handle->cq,
            .cap = {
                    .max_send_wr = 16,
                    .max_recv_wr = 16,
                    .max_send_sge = 1,
                    .max_recv_sge = 1,
            },
            .qp_type = IBV_QPT_RC, // Reliable Connection
    };
    for (int i = 0; i < 2; ++i) {
        handle->qps[i] = ibv_create_qp(handle->pd, &qp_init_attr);
        if (!handle->qps[i]) {
            cleanup_pg_handle(handle);
            fprintf(stderr, "Failed to create QP %d\n", i);
            // Cleanup omitted for brevity
            return -1;
        }
    }

    // Exchange QP info with neighbors

    int left = (handle->rank - 1 + handle->size) % handle->size;
    int right = (handle->rank + 1) % handle->size;

    // Gather local QP info
    struct ibv_port_attr port_attr;
    ibv_query_port(handle->ctx, 1, &port_attr);  // 1 is the port number of the RDMA device
    qp_info_t myinfo[2]; // holds the QP information for your own process.
    // myinfo[0] is the left QP (QP you use to communicate with your left neighbor), myinfo[1] is the right QP (QP you use to communicate with your right neighbor)
    // Loop through the two QPs (left and right) and set the LID, QPN, and PSN
    for (int i = 0; i < 2; ++i) {
        myinfo[i].lid = port_attr.lid;
        myinfo[i].qpn = handle->qps[i]->qp_num;
        myinfo[i].psn = 1000 + (handle->rank * 100) + (i * 50); // Rank-based unique PSNs
    }

    /*
    * | Variable      | Represents                                      | Used for...                        |
    * |---------------|-------------------------------------------------|------------------------------------|
    * | myinfo[0]   | Your left QP (to left neighbor)                 | Local info for left QP             |
    * | myinfo[1]   | Your right QP (to right neighbor)               | Local info for right QP            |
    * | leftinfo    | Left neighbor’s QP (that talks to you)          | Remote info for left QP connection |
    * | rightinfo   | Right neighbor’s QP (that talks to you)         | Remote info for right QP connection|
    */

    int sock_left;
    int sock_right;
    qp_info_t rightinfo;
    qp_info_t leftinfo;

    if (handle->rank == 0) {
        // Rank 0: connect first, then accept  (to avoid deadlock)
        sock_right = tcp_connect(handle->servernames[right], QP_EXCHANGE_PORT_BASE + ((handle->rank + 1) % handle->size));
        if (sock_right < 0) {
            cleanup_pg_handle(handle);
            perror("tcp_connect right");
            return -1;
        }
        write(sock_right, &myinfo[1], sizeof(qp_info_t));
        read(sock_right, &rightinfo, sizeof(qp_info_t));
        close(sock_right);

        sock_left = tcp_listen_accept(QP_EXCHANGE_PORT_BASE + handle->rank);
        if (sock_left < 0) {
            cleanup_pg_handle(handle);
            perror("tcp_listen_accept left");
            return -1;
        }
        read(sock_left, &leftinfo, sizeof(qp_info_t));
        write(sock_left, &myinfo[0], sizeof(qp_info_t));
        close(sock_left);
    } else {
        // All other ranks: accept first, then connect
        int sock_left = tcp_listen_accept(QP_EXCHANGE_PORT_BASE + handle->rank);
        if (sock_left < 0) {
            cleanup_pg_handle(handle);
            perror("tcp_listen_accept left");
            return -1;
        }
        read(sock_left, &leftinfo, sizeof(qp_info_t));
        write(sock_left, &myinfo[0], sizeof(qp_info_t));
        close(sock_left);

        sock_right = tcp_connect(handle->servernames[right], QP_EXCHANGE_PORT_BASE + ((handle->rank + 1) % handle->size));
        if (sock_right < 0) {
            cleanup_pg_handle(handle);
            perror("tcp_connect right");
            return -1;
        }
        write(sock_right, &myinfo[1], sizeof(qp_info_t));
        read(sock_right, &rightinfo, sizeof(qp_info_t));
        close(sock_right);
    }


    // 4. Allocate and register buffer
    handle->bufsize = 1024*1024; // was originally 4096
    handle->sendbuf = malloc(handle->bufsize);
    if (!handle->sendbuf) {
        cleanup_pg_handle(handle);
        fprintf(stderr, "Failed to allocate sendbuf\n");
        return -1;
    }
    handle->mr_send = ibv_reg_mr(
            handle->pd,
            handle->sendbuf,
            handle->bufsize,
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ
    );
    if (!handle->mr_send) {
        cleanup_pg_handle(handle);
        fprintf(stderr, "Failed to register memory region\n");
        return -1;
    }
    handle->local_rkey = handle->mr_send->rkey;
    handle->local_addr = (uintptr_t)handle->sendbuf;

    // 4.5. Allocate and register recvbuf
    handle->recvbuf = malloc(handle->bufsize);
    if (!handle->recvbuf) {
        cleanup_pg_handle(handle);
        fprintf(stderr, "Failed to allocate recvbuf\n");
        return -1;
    }
    handle->mr_recv = ibv_reg_mr(
            handle->pd,
            handle->recvbuf,
            handle->bufsize,
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ
    );
    if (!handle->mr_recv) {
        cleanup_pg_handle(handle);
        fprintf(stderr, "Failed to register recvbuf memory region\n");
        return -1;
    }


    // For left QP (index 0)
    if (connect_qp(handle->qps[0], &myinfo[0], &leftinfo)) {
        cleanup_pg_handle(handle);
        fprintf(stderr, "Failed to connect left QP\n");
        return -1;
    }

    // For right QP (index 1)
    if (connect_qp(handle->qps[1], &myinfo[1], &rightinfo)) {
        cleanup_pg_handle(handle);
        fprintf(stderr, "Failed to connect right QP\n");
        return -1;
    }

    // Exchange memory region info (rkey, addr) with neighbors
    mr_info_t my_mrinfo, right_mrinfo, left_mrinfo;
    my_mrinfo.rkey = handle->mr_recv->rkey;
    my_mrinfo.addr = (uintptr_t)handle->recvbuf;

    if (handle->rank == 0){  // connect first, then accept
        sock_right = tcp_connect(handle->servernames[right], MR_EXCHANGE_PORT_BASE + ((handle->rank + 1) % handle->size)); // Use a different port for MR exchange
        if (sock_right < 0) {
            cleanup_pg_handle(handle);
            perror("tcp_connect right (MR)");
            return -1;
        }
        write(sock_right, &my_mrinfo, sizeof(mr_info_t)); // Send our MR info
        read(sock_right, &right_mrinfo, sizeof(mr_info_t)); // Receive right neighbor's MR info
        close(sock_right);

        handle->remote_rkeys[right] = right_mrinfo.rkey;
        handle->remote_addrs[right] = right_mrinfo.addr;

        sock_left = tcp_listen_accept(MR_EXCHANGE_PORT_BASE + handle->rank);
        if (sock_left < 0) {
            cleanup_pg_handle(handle);
            perror("tcp_listen_accept left (MR)");
            return -1;
        }
        read(sock_left, &left_mrinfo, sizeof(mr_info_t)); // Receive left neighbor's MR info
        write(sock_left, &my_mrinfo, sizeof(mr_info_t));  // Send our MR info
        close(sock_left);

        handle->remote_rkeys[left] = left_mrinfo.rkey;
        handle->remote_addrs[left] = left_mrinfo.addr;
    }
    else{  // accept first, connect second
        sock_left = tcp_listen_accept(MR_EXCHANGE_PORT_BASE + handle->rank);
        if (sock_left < 0) {
            cleanup_pg_handle(handle);
            perror("tcp_listen_accept left (MR)");
            return -1;
        }
        read(sock_left, &left_mrinfo, sizeof(mr_info_t)); // Receive left neighbor's MR info
        write(sock_left, &my_mrinfo, sizeof(mr_info_t));  // Send our MR info
        close(sock_left);

        handle->remote_rkeys[left] = left_mrinfo.rkey;
        handle->remote_addrs[left] = left_mrinfo.addr;

        sock_right = tcp_connect(handle->servernames[right], MR_EXCHANGE_PORT_BASE + ((handle->rank + 1) % handle->size)); // Use a different port for MR exchange
        if (sock_right < 0) {
            cleanup_pg_handle(handle);
            perror("tcp_connect right (MR)");
            return -1;
        }
        write(sock_right, &my_mrinfo, sizeof(mr_info_t)); // Send our MR info
        read(sock_right, &right_mrinfo, sizeof(mr_info_t)); // Receive right neighbor's MR info
        close(sock_right);

        handle->remote_rkeys[right] = right_mrinfo.rkey;
        handle->remote_addrs[right] = right_mrinfo.addr;
    }

    // Check all pointers
    if (!handle->ctx || !handle->pd || !handle->cq || !handle->qps ||
        !handle->mr_send || !handle->mr_recv || !handle->sendbuf || !handle->recvbuf ||
        !handle->remote_rkeys || !handle->remote_addrs) {
        fprintf(stderr, "Resource allocation or registration failed\n");
        cleanup_pg_handle(handle);
        return -1;
    }
    return 0;
}

// Helper: perform the reduction operation for a chunk
/**
 * @brief Reduce a chunk of data using the specified operation
 * @param dst: pointer to the destination buffer where the result will be stored
 * @param src: pointer to the source buffer containing the data to reduce
 * @param count: number of elements in the chunk
 * @param datatype: data type of the elements (e.g. DATATYPE_INT, DATATYPE_FLOAT)
 * @param op: reduction operation to apply (e.g. OPERATION_SUM, OPERATION_MULT)
 */
static void reduce_chunk(void *dst, void *src, int count, DATATYPE datatype, OPERATION op) {
    if (op == OP_SUM) {
        if (datatype == DATATYPE_INT) {
            int *d = (int *)dst;
            int *s = (int *)src;
            for (int i = 0; i < count; ++i) {
                d[i] += s[i];
            }
        }
        else if (datatype == DATATYPE_FLOAT) {
            float *d = (float *)dst;
            float *s = (float *)src;
            for (int i = 0; i < count; ++i) {
                d[i] += s[i];
            }
        }
    }
    else if (op == OP_MULT){
        if (datatype == DATATYPE_INT) {
            int *d = (int *)dst;
            int *s = (int *)src;
            for (int i = 0; i < count; ++i){
                d[i] *= s[i];
            }
        }
        else if (datatype == DATATYPE_FLOAT) {
            float *d = (float *)dst;
            float *s = (float *)src;
            for (int i = 0; i < count; ++i){
                d[i] *= s[i];
            }
        }

    }
}

/**
 * @brief Post a non-blocking send and receive operation.
 * This function posts a receive operation first, then a send operation.
 * It ensures that the buffers are within the registered memory region.
 * It uses the step_id to track the operation.
 * @param handle: pointer to the process group handle
 * @param send_ptr: pointer to the buffer to send
 * @param send_count: number of elements to send
 * @param recv_ptr: pointer to the buffer to receive
 * @param recv_count: number of elements to receive
 * @param step_id: unique identifier for this step (used for tracking)
 * @param type_size: size of the data type being sent/received
 * @return 0 on success, -1 on failure
 */
static int post_send_recv_nonblocking(pg_handle_t *handle, void *send_ptr, int send_count,
                                      void *recv_ptr, int recv_count, int step_id, size_t type_size) {
    // Verify that recv_ptr is within the registered memory region
    if (recv_ptr < handle->recvbuf ||
        (char*)recv_ptr + recv_count * type_size > (char*)handle->recvbuf + handle->mr_recv->length) {
        fprintf(stderr, "Receive buffer pointer outside registered memory region\n");
        return -1;
    }

    // Post receive
    struct ibv_sge recv_sge = {
            .addr = (uintptr_t)recv_ptr,
            .length = recv_count * type_size,
            .lkey = handle->mr_recv->lkey
    };
    struct ibv_recv_wr recv_wr = {
            .wr_id = step_id,
            .sg_list = &recv_sge,
            .num_sge = 1,
            .next = NULL
    };
    struct ibv_recv_wr *bad_recv_wr;
    if (ibv_post_recv(handle->qps[0], &recv_wr, &bad_recv_wr)) {
        return -1;
    }


    // Verify that send_ptr is within the registered memory region
    if (send_ptr < handle->sendbuf ||
        (char*)send_ptr + send_count * type_size > (char*)handle->sendbuf + handle->mr_send->length) {
        fprintf(stderr, "Send buffer pointer outside registered memory region\n");
        return -1;
    }

    struct ibv_sge send_sge = {
            .addr = (uintptr_t)send_ptr,
            .length = send_count * type_size,
            .lkey = handle->mr_send->lkey
    };
    struct ibv_send_wr send_wr = {
            .wr_id = step_id,
            .sg_list = &send_sge,
            .num_sge = 1,
            .opcode = IBV_WR_SEND,
            .send_flags = IBV_SEND_SIGNALED,
            .next = NULL
    };
    struct ibv_send_wr *bad_send_wr;
    if (ibv_post_send(handle->qps[1], &send_wr, &bad_send_wr)) {
        return -1;
    }

    return 0;
}

// Track operations in flight
typedef struct {
    int step;
    void *recv_buffer;
    int recv_count;
    int completed;
} pipeline_op_t;


// Eager protocol for small messages using send/recv
/**
 * @brief Eager reduce-scatter collective operation using ring algorithm with send/recv and pipelining.
 * 
 * ALGORITHM OVERVIEW:
 * The steps in this function:
 * 1. Initialize: Set up buffers and divide data into chunks per process
 * 2. Ring Algorithm: For (size-1) steps, each process:
 *    - Sends its current chunk to the RIGHT neighbor
 *    - Receives a chunk from the LEFT neighbor
 *    - Reduces the received chunk with local data
 *    - Updates send buffer with the reduced result
 * 3. Final Placement: Extract the fully reduced chunk for this process
 * 
 * PIPELINING MECHANISM:
 * This implementation uses a 2-slot pipelining approach to optimize performance:
 * - Two staging buffers (ops[0] and ops[1]) alternate between steps
 * - While one buffer is being used for reduction, the other can receive new data
 * - This overlaps computation (reduction) with communication (receive)
 * - The current_op variable toggles between 0 and 1 each step
 * 
 * BUFFER ORGANIZATION:
 * - main_buf: Full workspace containing all chunks for reduction
 * - recv_chunk_buf: Base address for staging area (after main_buf)
 * - ops[0].recv_buffer: First staging slot for incoming chunks
 * - ops[1].recv_buffer: Second staging slot for incoming chunks
 * - sendbuf: Registered memory for outgoing chunks (updated each step)
 * 
 * @param handle: pointer to the process group handle
 * @param recvbuf: pointer to the buffer to receive - this is the buffer that will contain the scattered data
 * @param count: number of elements in the array that will be scattered
 * @param datatype: data type of the elements (e.g. DATATYPE_INT, DATATYPE_FLOAT)
 * @param op: reduction operation to apply (e.g. OPERATION_SUM, OPERATION_MAX)
 * @return 0 on success, -1 on failure
 */
static int eager_reduce_scatter(pg_handle_t *handle, void *recvbuf, int count, DATATYPE datatype, OPERATION op) {
    int rank = handle->rank;
    int size = handle->size;
    size_t type_size = datatype_size(datatype);
    int chunk_size = count / size;
    int remainder = count % size;
    size_t total_bytes = count * type_size;
    size_t max_chunk_bytes = (chunk_size + remainder) * type_size;
    size_t pipeline_buf_size = 2 * max_chunk_bytes;

    if (total_bytes + pipeline_buf_size > handle->mr_recv->length) {
        fprintf(stderr, "[pipelined_reduce_scatter] recvbuf too small for pipelining\n");
        return -1;
    }

    // Set up reduction workspace and staging buffers
    char *main_buf = (char *)handle->recvbuf;                        // full reduction workspace
    char *recv_chunk_buf = main_buf + total_bytes;                   // 2-slot staging area

    // Copy initial data into both send and receive registered buffers
    memcpy(main_buf, recvbuf, count * type_size);
    memcpy(handle->sendbuf, recvbuf, count * type_size);

    // Pipeline operation metadata
    pipeline_op_t ops[2];
    ops[0].recv_buffer = recv_chunk_buf;
    ops[1].recv_buffer = recv_chunk_buf + max_chunk_bytes;
    ops[0].completed = 0;
    ops[1].completed = 0;

    // Initialize staging buffers to zero
    memset(ops[0].recv_buffer, 0, max_chunk_bytes);
    memset(ops[1].recv_buffer, 0, max_chunk_bytes);

    int current_op = 0;
    int used_eager_protocol = 0;  // Flag to track if we used eager protocol


    for (int step = 0; step < size - 1; ++step) {
        // Correct reduce-scatter ring algorithm:
        // Each process sends the chunk it's currently responsible for to the right neighbor
        // and receives from the left neighbor to reduce into the target chunk
        int send_chunk_idx = (rank - 1 - step + size) % size;
        int recv_chunk_idx = (rank - step - 2 + size) % size;
        int send_count = (send_chunk_idx == size - 1) ? (chunk_size + remainder) : chunk_size;
        int recv_count = (recv_chunk_idx == size - 1) ? (chunk_size + remainder) : chunk_size;

        char *send_ptr = (char*)handle->sendbuf + send_chunk_idx * chunk_size * type_size;

        //EAGER PROTOCOL FOR SMALL MESSAGES
        used_eager_protocol = 1;  // Mark that we used eager protocol

        // Post send/recv for current step
        if (post_send_recv_nonblocking(handle, send_ptr, send_count,
                                        ops[current_op].recv_buffer, recv_count, step, type_size) != 0) {
            return -1;
        }

        // Wait for current step's completion
        struct ibv_wc wc;
        int send_completed = 0, recv_completed = 0;
        while (send_completed == 0 || recv_completed == 0) {
            int ne = ibv_poll_cq(handle->cq, 1, &wc);
            if (ne > 0) {
                if (wc.status == IBV_WC_SUCCESS) {
                    if (wc.opcode == IBV_WC_SEND) {
                        send_completed = 1;
                    } else if (wc.opcode == IBV_WC_RECV) {
                        recv_completed = 1;
                    }
                } else if (wc.status != IBV_WC_SUCCESS) {
                    fprintf(stderr, "CQ error: %s\n", ibv_wc_status_str(wc.status));
                    return -1;
                }
            }
        }

        // Process the current step's received data
        char *curr_dst = main_buf + recv_chunk_idx * chunk_size * type_size;
        
        reduce_chunk(curr_dst, ops[current_op].recv_buffer, recv_count, datatype, op);
        
        // Update send buffer for next step - we'll send what we just reduced
        char *send_dst = (char*)handle->sendbuf + recv_chunk_idx * chunk_size * type_size;
        memcpy(send_dst, curr_dst, recv_count * type_size);

        // Switch to the other receive buffer for next iteration
        current_op = 1 - current_op;
    }
    // After reduce-scatter, each process should have only its own chunk
    // But the fully reduced chunk is in a different position due to the ring algorithm
    memset(recvbuf, 0, count * type_size);
    
    // In the ring reduce-scatter algorithm, after (size-1) steps,
    // the fully reduced chunk for process 'rank' ends up at position:
    // Same logic as rendezvous: each process gets its own chunk position
    int fully_reduced_chunk_idx = rank;
    int my_chunk_count = (rank == size - 1) ? (chunk_size + remainder) : chunk_size;
    char *fully_reduced_chunk_src = main_buf + fully_reduced_chunk_idx * chunk_size * type_size;
    char *my_chunk_dst = (char*)recvbuf;  // Place reduced chunk at beginning for reduce-scatter
    
    memcpy(my_chunk_dst, fully_reduced_chunk_src, my_chunk_count * type_size);

    return 0;
}

/**
 * @brief Polls a completion queue (CQ) until a specific work completion is found.
 * * This function blocks until it dequeues a work completion that matches the
 * expected work request ID and opcode. It also handles and reports errors
 * for any failed work completions.
 * * @param cq The completion queue to poll.
 * @param expected_opcode The specific opcode to wait for (e.g., IBV_WC_SEND, IBV_WC_RDMA_READ).
 * @param wr_id The unique work request ID of the operation to wait for.
 */
static void wait_for_completion(struct ibv_cq *cq, enum ibv_wc_opcode expected_opcode, uint64_t wr_id) {
    struct ibv_wc wc;
    int ne; // Number of entries
    
    // Loop indefinitely until we find the specific completion we're looking for.
    while (1) {
        // Poll the CQ. This is a non-blocking call. 
        // It returns the number of completions found (or 0 if empty).
        ne = ibv_poll_cq(cq, 1, &wc);
        if (ne < 0) {
            fprintf(stderr, "Fatal error: ibv_poll_cq failed\n");
            return; // Or exit, depending on desired error handling
        }

        // If we found a completion, check if it's the one we want.
        if (ne > 0) {
            // First, check if the operation associated with this completion failed.
            if (wc.status != IBV_WC_SUCCESS) {
                fprintf(stderr, "Work completion failed with status %s for wr_id %lu\n",
                       ibv_wc_status_str(wc.status), wc.wr_id);
                // For RDMA failures, we should handle gracefully rather than continue with corrupted data
                if (wc.wr_id == wr_id && wc.opcode == expected_opcode) {
                    return;
                }
                // Continue polling for other completions
                continue;
            }

            // Now, check if this successful completion is the one we are waiting for.
            if (wc.opcode == expected_opcode && wc.wr_id == wr_id) {
                // Success! We found the specific completion. Exit the loop.
                return;
            }
            
            // If it was a successful completion but not the one we were looking for,
            // we simply ignore it and continue polling. This allows the function to
            // filter out completions from other concurrent operations.
        }
    }
}

// Rendezvous protocol for large messages using RDMA_READ
/**
 * @brief Rendezvous reduce-scatter collective operation using ring algorithm with RDMA_READ and pipelining.
 * 
 * ALGORITHM OVERVIEW:
 * The steps in this function:
 * 1. Initialize: Set up buffers, calculate chunk distributions, and allocate displacement arrays
 * 2. Ring Algorithm: For (size-1) steps, each process:
 *    - Exposes its current chunk via handshake protocol (request/response)
 *    - Performs RDMA_READ from LEFT neighbor to get chunk data
 *    - Reduces the received chunk with local data
 *    - Updates send buffer with the reduced result
 * 3. Final Placement: Extract the fully reduced chunk for this process
 * 
 * RENDEZVOUS PROTOCOL (3-Phase Handshake):
 * Phase A: Request Exchange
 *   - Each process sends request to LEFT neighbor and receives request from RIGHT neighbor
 *   - Requests contain step_id and data_size information
 * Phase B: Response Exchange  
 *   - Each process sends response to RIGHT neighbor and receives response from LEFT neighbor
 *   - Responses contain remote_addr and remote_rkey for RDMA_READ access
 * Phase C: RDMA_READ Execution
 *   - Each process performs RDMA_READ from LEFT neighbor using received credentials
 *   - Zero-copy data transfer directly to local staging buffer
 * 
 * PIPELINING MECHANISM:
 * This implementation uses sophisticated pipelining to optimize large message transfers:
 * - Two staging buffers (ops[0] and ops[1]) alternate between steps
 * - RDMA_READ from step N overlaps with reduction processing from step N-1
 * - Handshake for step N+1 overlaps with RDMA_READ from step N
 * - The current_op variable toggles between 0 and 1 each step
 * - Pipeline depth allows 3 operations to proceed concurrently
 * 
 * BUFFER ORGANIZATION:
 * - main_buf: Full workspace containing all chunks for reduction
 * - recv_chunk_buf: Base address for staging area (after main_buf)
 * - ops[0].recv_buffer: First staging slot for incoming RDMA_READ data
 * - ops[1].recv_buffer: Second staging slot for incoming RDMA_READ data  
 * - sendbuf: Registered memory for exposing chunks (stable during handshake)
 * - Displacement arrays: Handle uneven chunk distribution
 * 
 * CHUNK DISTRIBUTION:
 * Supports uneven chunk distribution for cases where count % size != 0:
 * - counts[i]: Number of elements in chunk i
 * - displs[i]: Starting displacement for chunk i
 * - Last (remainder) processes get one extra element each
 * 
 * @param handle: pointer to the process group handle
 * @param recvbuf: pointer to the buffer to receive - this is the buffer that will contain the scattered data
 * @param count: number of elements in the array that will be scattered
 * @param datatype: data type of the elements (e.g. DATATYPE_INT, DATATYPE_FLOAT)
 * @param op: reduction operation to apply (e.g. OPERATION_SUM, OPERATION_MAX)
 * @return 0 on success, -1 on failure
 */
static int rendezvous_reduce_scatter(pg_handle_t *handle, void *recvbuf, int count, DATATYPE datatype, OPERATION op) {
    int rank = handle->rank;
    int size = handle->size;
    size_t type_size = datatype_size(datatype);
    
    // Calculate chunk sizes and displacements correctly
    int chunk_size = count / size;  // Base chunk size
    int remainder = count % size;   // Extra elements for last processes
    
    // Pre-calculate the exact size and displacement of each chunk
    int *counts = (int *)malloc(size * sizeof(int));
    int *displs = (int *)malloc(size * sizeof(int));
    
    for (int i = 0; i < size; ++i) {
        // Each process gets at least chunk_size elements
        // The last (remainder) processes get one extra element each
        counts[i] = (i < size - remainder) ? chunk_size : chunk_size + 1;
        displs[i] = (i == 0) ? 0 : displs[i - 1] + counts[i - 1];
    }
    
    // Calculate buffer requirements using the largest chunk
    size_t max_chunk_bytes = 0;
    for (int i = 0; i < size; i++) {
        size_t chunk_bytes = counts[i] * type_size;
        if (chunk_bytes > max_chunk_bytes) {
            max_chunk_bytes = chunk_bytes;
        }
    }
    
    size_t total_bytes = count * type_size;
    size_t pipeline_buf_size = 2 * max_chunk_bytes;

    if (total_bytes + pipeline_buf_size > handle->mr_recv->length) {
        fprintf(stderr, "[rendezvous_reduce_scatter] recvbuf too small for pipelining\n");
        free(counts);
        free(displs);
        return -1;
    }

    char *main_buf = (char *)handle->recvbuf;
    char *recv_chunk_buf = main_buf + total_bytes;
    memcpy(main_buf, recvbuf, count * type_size);
    memcpy(handle->sendbuf, recvbuf, count * type_size);

    pipeline_op_t ops[2];
    ops[0].recv_buffer = recv_chunk_buf;
    ops[1].recv_buffer = recv_chunk_buf + max_chunk_bytes;
    int current_op = 0;
    int prev_op = 1;

    typedef struct { int step_id; } request_t;
    typedef struct { uintptr_t remote_addr; uint32_t remote_rkey; } response_t;

    // *** Allocate handshake message buffers inside the registered send buffer ***
    // We reserve a small space at the end of the buffer for this.
    // Ensure `handle->bufsize` is large enough to not overlap with data.
    size_t msg_size = sizeof(request_t) + sizeof(response_t);
    char* handshake_buf_base = (char*)handle->sendbuf + handle->bufsize - (2 * msg_size);
    
    request_t* left_req   = (request_t*)(handshake_buf_base);
    response_t* left_resp  = (response_t*)(handshake_buf_base + sizeof(request_t));
    request_t* right_req  = (request_t*)(handshake_buf_base + msg_size);
    response_t* right_resp = (response_t*)(handshake_buf_base + msg_size + sizeof(request_t));

    for (int step = 0; step < size - 1; ++step) {
        // Ring reduce-scatter: each process sends its own chunk in step 0,
        // then sends what it received and reduced in previous steps
        int send_chunk_idx = (rank - 1 - step + size) % size;
        int recv_chunk_idx = (rank - step - 2 + size) % size;
        
        // Calculate counts for sending and receiving using the correct chunk sizes
        int send_count = counts[send_chunk_idx];
        int recv_count = counts[recv_chunk_idx];

        // STEP 1: PROCESS PREVIOUS STEP'S DATA FIRST (IF ANY)
        // Wait for previous RDMA_READ to complete and reduce into main_buf
        // This MUST happen before preparing the send buffer to ensure we have the latest data
        if (step > 0) {
            int prev_step = step - 1;
            int prev_recv_chunk_idx = (rank - prev_step - 2 + size) % size;
            int prev_recv_count = counts[prev_recv_chunk_idx];

            wait_for_completion(handle->cq, IBV_WC_RDMA_READ, prev_step);

            // Use displacement array for correct addressing
            char *prev_dst = main_buf + displs[prev_recv_chunk_idx] * type_size;

            // Reduce the received data into main_buf 
            reduce_chunk(prev_dst, ops[prev_op].recv_buffer, prev_recv_count, datatype, op);
        }

        // STEP 2: PREPARE SEND BUFFER (STABLE SNAPSHOT)
        // NOW copy the current (up-to-date) chunk from main_buf that will be exposed for RDMA_READ
        // This creates a stable snapshot that won't be modified during this step
        // Use displacement array for correct addressing
        char *send_src = main_buf + displs[send_chunk_idx] * type_size;
        char *send_ptr = (char*)handle->sendbuf + displs[send_chunk_idx] * type_size;
        
        memcpy(send_ptr, send_src, send_count * type_size);

        // STEP 3: HANDSHAKE - EXPOSE THE STABLE SEND BUFFER
        // Now perform the handshake, exposing the stable sendbuf that was prepared earlier
        struct ibv_recv_wr *bad_wr;
        struct ibv_send_wr *bad_send_wr;
        
        // -- Phase A: Exchange Requests --
        
        // Post receive for the request from our RIGHT neighbor.
        struct ibv_sge recv_req_sge = {.addr = (uintptr_t)right_req, .length = sizeof(request_t), .lkey = handle->mr_send->lkey};
        struct ibv_recv_wr recv_req_wr = {.wr_id = step * 10, .sg_list = &recv_req_sge, .num_sge = 1};
        if(ibv_post_recv(handle->qps[1], &recv_req_wr, &bad_wr)) return -1;

        // Send our request to our LEFT neighbor.
        left_req->step_id = step;
        struct ibv_sge send_req_sge = {.addr = (uintptr_t)left_req, .length = sizeof(request_t), .lkey = handle->mr_send->lkey};
        struct ibv_send_wr send_req_wr = {.wr_id = step * 10, .opcode = IBV_WR_SEND, .send_flags = IBV_SEND_SIGNALED, .sg_list = &send_req_sge, .num_sge = 1};
        if(ibv_post_send(handle->qps[0], &send_req_wr, &bad_send_wr)) return -1;

        // Wait for Phase A completions - collect both without strict ordering
        int recv_req_done = 0, send_req_done = 0;
        while (!recv_req_done || !send_req_done) {
            struct ibv_wc wc;
            int ne = ibv_poll_cq(handle->cq, 1, &wc);
            if (ne > 0) {
                if (wc.status != IBV_WC_SUCCESS) {
                    fprintf(stderr, "[RDMA-READ] Phase A completion failed: %s\n", ibv_wc_status_str(wc.status));
                    return -1;
                }
                
                if (wc.wr_id == step * 10 && wc.opcode == IBV_WC_RECV && !recv_req_done) {
                    recv_req_done = 1;
                } else if (wc.wr_id == step * 10 && wc.opcode == IBV_WC_SEND && !send_req_done) {
                    send_req_done = 1;
                }
            }
        }
        
        // -- Phase B: Exchange Responses --

        // Post receive for the response from our LEFT neighbor.
        struct ibv_sge recv_resp_sge = {.addr = (uintptr_t)left_resp, .length = sizeof(response_t), .lkey = handle->mr_send->lkey};
        struct ibv_recv_wr recv_resp_wr = {.wr_id = step * 10 + 1, .sg_list = &recv_resp_sge, .num_sge = 1};
        if(ibv_post_recv(handle->qps[0], &recv_resp_wr, &bad_wr)) return -1;

        // Send our response to our RIGHT neighbor with the STABLE send buffer address
        // This is the buffer that was prepared at the beginning of the step and won't change
        right_resp->remote_addr = (uintptr_t)send_ptr;
        right_resp->remote_rkey = handle->mr_send->rkey;
        
        struct ibv_sge send_resp_sge = {.addr = (uintptr_t)right_resp, .length = sizeof(response_t), .lkey = handle->mr_send->lkey};
        struct ibv_send_wr send_resp_wr = {.wr_id = step * 10 + 1, .opcode = IBV_WR_SEND, .send_flags = IBV_SEND_SIGNALED, .sg_list = &send_resp_sge, .num_sge = 1};
        if(ibv_post_send(handle->qps[1], &send_resp_wr, &bad_send_wr)) return -1;

        // Wait for Phase B completions - collect both without strict ordering
        int recv_resp_done = 0, send_resp_done = 0;
        while (!recv_resp_done || !send_resp_done) {
            struct ibv_wc wc;
            int ne = ibv_poll_cq(handle->cq, 1, &wc);
            if (ne > 0) {
                if (wc.status != IBV_WC_SUCCESS) {
                    fprintf(stderr, "[RDMA-READ] Phase B completion failed: %s\n", ibv_wc_status_str(wc.status));
                    return -1;
                }
                
                if (wc.wr_id == step * 10 + 1 && wc.opcode == IBV_WC_RECV && !recv_resp_done) {
                    recv_resp_done = 1;
                } else if (wc.wr_id == step * 10 + 1 && wc.opcode == IBV_WC_SEND && !send_resp_done) {
                    send_resp_done = 1;
                }
            }
        }


        // STEP 4: ISSUE NEW RDMA_READ FOR CURRENT STEP        
        struct ibv_sge read_sge = {
            .addr = (uintptr_t)ops[current_op].recv_buffer,
            .length = recv_count * type_size,
            .lkey = handle->mr_recv->lkey
        };

        struct ibv_send_wr read_wr = {
            .wr_id = step,
            .opcode = IBV_WR_RDMA_READ,
            .send_flags = IBV_SEND_SIGNALED,
            .sg_list = &read_sge,
            .num_sge = 1,
            .wr.rdma = {
                .remote_addr = left_resp->remote_addr,
                .rkey = left_resp->remote_rkey
            }
        };


        if (ibv_post_send(handle->qps[0], &read_wr, &bad_send_wr)) {
            fprintf(stderr, "[RDMA-READ] Failed to post RDMA_READ\n");
            return -1;
        }
                
        // NOTE: The send buffer is now GUARANTEED to be stable for the entire duration
        // of this step because we prepared it at the beginning and never modify it again
        
        prev_op = current_op;
        current_op = 1 - current_op;
    }

    // Final Step Processing
    if (size > 1) {
        int final_step = size - 2;
        int final_recv_chunk_idx = (rank - final_step - 2 + size) % size;
        int final_recv_count = counts[final_recv_chunk_idx];

        wait_for_completion(handle->cq, IBV_WC_RDMA_READ, final_step);
        
        // Use displacement array for correct addressing
        char *final_dst = main_buf + displs[final_recv_chunk_idx] * type_size;
        
        reduce_chunk(final_dst, ops[prev_op].recv_buffer, final_recv_count, datatype, op);
    }

    // Final data placement using displacement array
    
    memset(recvbuf, 0, count * type_size);
    
    int chunk_position_for_my_data = (rank) % size;
    char *my_chunk_src = main_buf + displs[chunk_position_for_my_data] * type_size;

    char *my_chunk_dst = (char*)recvbuf;
    
    // Copy the correct number of elements - use counts[rank] for the destination size
    memcpy(my_chunk_dst, my_chunk_src, counts[rank] * type_size);
    memcpy(my_chunk_dst, my_chunk_src, counts[rank] * type_size);

    // CRITICAL: Add barrier synchronization to ensure all processes complete before cleanup
    // This prevents early termination causing "transport retry counter exceeded" errors
    
    // Simple barrier using existing handshake infrastructure
    // Each process sends a completion signal to both neighbors and waits for both
    typedef struct { int completion_signal; } barrier_msg_t;
    barrier_msg_t *completion_msg = (barrier_msg_t*)((char*)handle->sendbuf + handle->bufsize - sizeof(barrier_msg_t));
    completion_msg->completion_signal = rank;
    
    // Send completion signal to both neighbors
    struct ibv_sge barrier_sge = {.addr = (uintptr_t)completion_msg, .length = sizeof(barrier_msg_t), .lkey = handle->mr_send->lkey};
    struct ibv_send_wr barrier_wr_left = {.wr_id = 9999, .opcode = IBV_WR_SEND, .send_flags = IBV_SEND_SIGNALED, .sg_list = &barrier_sge, .num_sge = 1};
    struct ibv_send_wr barrier_wr_right = {.wr_id = 9998, .opcode = IBV_WR_SEND, .send_flags = IBV_SEND_SIGNALED, .sg_list = &barrier_sge, .num_sge = 1};
    
    // Post receives for completion signals from both neighbors
    barrier_msg_t *left_completion = (barrier_msg_t*)((char*)handle->sendbuf + handle->bufsize - 2*sizeof(barrier_msg_t));
    barrier_msg_t *right_completion = (barrier_msg_t*)((char*)handle->sendbuf + handle->bufsize - 3*sizeof(barrier_msg_t));
    
    struct ibv_sge left_barrier_sge = {.addr = (uintptr_t)left_completion, .length = sizeof(barrier_msg_t), .lkey = handle->mr_send->lkey};
    struct ibv_recv_wr left_barrier_wr = {.wr_id = 9997, .sg_list = &left_barrier_sge, .num_sge = 1};
    struct ibv_sge right_barrier_sge = {.addr = (uintptr_t)right_completion, .length = sizeof(barrier_msg_t), .lkey = handle->mr_send->lkey};
    struct ibv_recv_wr right_barrier_wr = {.wr_id = 9996, .sg_list = &right_barrier_sge, .num_sge = 1};
    
    struct ibv_recv_wr *bad_recv_wr;
    struct ibv_send_wr *bad_send_wr;
    
    ibv_post_recv(handle->qps[0], &left_barrier_wr, &bad_recv_wr);
    ibv_post_recv(handle->qps[1], &right_barrier_wr, &bad_recv_wr);
    
    ibv_post_send(handle->qps[0], &barrier_wr_left, &bad_send_wr);
    ibv_post_send(handle->qps[1], &barrier_wr_right, &bad_send_wr);
    
    // Wait for all barrier operations to complete
    int barrier_ops_done = 0;
    while (barrier_ops_done < 4) { // 2 sends + 2 receives
        struct ibv_wc wc;
        int ne = ibv_poll_cq(handle->cq, 1, &wc);
        if (ne > 0 && wc.status == IBV_WC_SUCCESS && wc.wr_id >= 9996 && wc.wr_id <= 9999) {
            barrier_ops_done++;
        }
    }
    
    // Clean up
    free(counts);
    free(displs);
    return 0;
}


/**
 * @brief Pipelined reduce-scatter collective operation
 * @param handle: pointer to the process group handle
 * @param recvbuf: pointer to the buffer to receive - this is the buffer that will contain the scattered data
 * @param count: number of elements in the array that will be scattered
 * @param datatype: data type of the elements (e.g. DATATYPE_INT, DATATYPE_FLOAT)
 * @param op: reduction operation to apply (e.g. OPERATION_SUM, OPERATION_MAX)
 * @return 0 on success, -1 on failure
 */
static int pipelined_reduce_scatter(pg_handle_t *handle, void *recvbuf, int count, DATATYPE datatype, OPERATION op) {
    // Choose between eager and rendezvous based on message size
    if (count * datatype_size(datatype) <= EAGER_THRESHOLD) {
        return eager_reduce_scatter(handle, recvbuf, count, datatype, op);
    } else {
        return rendezvous_reduce_scatter(handle, recvbuf, count, datatype, op);
    }
}

// Eager protocol for small messages using send/recv
/**
 * @brief Eager all-gather collective operation using ring algorithm with send/recv and pipelining.
 * 
 * ALGORITHM OVERVIEW:
 * The steps in this function:
 * 1. Initialize: Set up buffers and position each process's reduced chunk in correct location
 * 2. Ring Algorithm: For (size-1) steps, each process:
 *    - Sends its current chunk to the RIGHT neighbor
 *    - Receives a chunk from the LEFT neighbor
 *    - Copies the received chunk to the correct position (no reduction, just gather)
 *    - Updates send buffer with the newly received chunk
 * 3. Final Result: All processes have complete data from all other processes
 * 
 * PIPELINING MECHANISM:
 * This implementation uses a 2-slot pipelining approach to optimize performance:
 * - Two staging buffers (ops[0] and ops[1]) alternate between steps
 * - While one buffer is being used for copying, the other can receive new data
 * - This overlaps computation (copying) with communication (receive)
 * - The current_op variable toggles between 0 and 1 each step
 * 
 * BUFFER ORGANIZATION:
 * - main_buf: Full workspace containing all chunks being gathered
 * - recv_chunk_buf: Base address for staging area (after main_buf)
 * - ops[0].recv_buffer: First staging slot for incoming chunks
 * - ops[1].recv_buffer: Second staging slot for incoming chunks
 * - sendbuf: Registered memory for outgoing chunks (updated each step)
 * 
 * INITIALIZATION PHASE:
 * After reduce-scatter, each process has its reduced chunk at the beginning of recvbuf.
 * All-gather must reposition this data:
 * - Clear main_buf completely
 * - Place my reduced chunk at position (rank * chunk_size) in main_buf
 * - This ensures each chunk is in its final position before ring circulation begins
 * 
 * COMMUNICATION PATTERN:
 * Ring Pattern Example (4 processes):
 * Initial: P0 has chunk0 at pos0, P1 has chunk1 at pos1, P2 has chunk2 at pos2, P3 has chunk3 at pos3
 * Step 0: P0 sends chunk0→P1, P1 sends chunk1→P2, P2 sends chunk2→P3, P3 sends chunk3→P0
 *         P0 receives chunk3, P1 receives chunk0, P2 receives chunk1, P3 receives chunk2
 * Step 1: P0 sends chunk3→P1, P1 sends chunk0→P2, P2 sends chunk1→P3, P3 sends chunk2→P0
 *         P0 receives chunk2, P1 receives chunk3, P2 receives chunk0, P3 receives chunk1
 * Step 2: P0 sends chunk2→P1, P1 sends chunk3→P2, P2 sends chunk0→P3, P3 sends chunk1→P0
 *         P0 receives chunk1, P1 receives chunk2, P2 receives chunk3, P3 receives chunk0
 * Result: All processes have all chunks: [chunk0, chunk1, chunk2, chunk3]
 * 
 * DIFFERENCE FROM REDUCE-SCATTER:
 * - No reduction operation, just copying/gathering data
 * - All processes end up with the same complete dataset
 * - Input: each process has one reduced chunk; Output: all processes have all chunks
 * 
 * @param handle: pointer to the process group handle
 * @param recvbuf: pointer to the buffer to receive - this is the buffer that will contain the gathered data
 * @param count: number of elements in the array that will be gathered
 * @param datatype: data type of the elements (e.g. DATATYPE_INT, DATATYPE_FLOAT)
 * @return 0 on success, -1 on failure
 */
static int eager_all_gather(pg_handle_t *handle, void *recvbuf, int count, DATATYPE datatype) {
    int rank = handle->rank;
    int size = handle->size;
    size_t type_size = datatype_size(datatype);
    int chunk_size = count / size;
    int remainder = count % size;
    size_t total_bytes = count * type_size;
    size_t max_chunk_bytes = (chunk_size + remainder) * type_size;
    size_t pipeline_buf_size = 2 * max_chunk_bytes;

    if (total_bytes + pipeline_buf_size > handle->mr_recv->length) {
        fprintf(stderr, "[eager_all_gather] recvbuf too small for pipelining\n");
        return -1;
    }

    // Set up reduction workspace and staging buffers
    char *main_buf = (char *)handle->recvbuf;                        // full workspace
    char *recv_chunk_buf = main_buf + total_bytes;                   // 2-slot staging area

    // After reduce-scatter, each process has its reduced chunk at the beginning of recvbuf
    // We need to position this data correctly for the all-gather operation
    
    // Clear main_buf first
    memset(main_buf, 0, count * type_size);
    
    // Place my reduced chunk data in the correct position
    int my_chunk_count = (rank == size - 1) ? (chunk_size + remainder) : chunk_size;
    char *my_chunk_dst = main_buf + rank * chunk_size * type_size;
    memcpy(my_chunk_dst, recvbuf, my_chunk_count * type_size);
    
    // Copy to sendbuf for communication
    memcpy(handle->sendbuf, main_buf, count * type_size);

    // Pipeline operation metadata
    pipeline_op_t ops[2];
    ops[0].recv_buffer = recv_chunk_buf;
    ops[1].recv_buffer = recv_chunk_buf + max_chunk_bytes;
    ops[0].completed = 0;
    ops[1].completed = 0;

    // Initialize staging buffers to zero
    memset(ops[0].recv_buffer, 0, max_chunk_bytes);
    memset(ops[1].recv_buffer, 0, max_chunk_bytes);

    int current_op = 0;

    for (int step = 0; step < size - 1; ++step) {
        // All-gather ring algorithm: similar to reduce-scatter but just copy (no reduction)
        // Each process sends the chunk it currently has and receives the next chunk
        int send_chunk_idx = (rank - step + size) % size;
        int recv_chunk_idx = (rank - step - 1 + size) % size;
        int send_count = (send_chunk_idx == size - 1) ? (chunk_size + remainder) : chunk_size;
        int recv_count = (recv_chunk_idx == size - 1) ? (chunk_size + remainder) : chunk_size;

        char *send_ptr = (char*)handle->sendbuf + send_chunk_idx * chunk_size * type_size;

        //EAGER PROTOCOL FOR SMALL MESSAGES

        // Post send/recv for current step
        if (post_send_recv_nonblocking(handle, send_ptr, send_count,
                                        ops[current_op].recv_buffer, recv_count, step, type_size) != 0) {
            return -1;
        }

        // Wait for current step's completion
        struct ibv_wc wc;
        int send_completed = 0, recv_completed = 0;
        while (send_completed == 0 || recv_completed == 0) {
            int ne = ibv_poll_cq(handle->cq, 1, &wc);
            if (ne > 0) {
                if (wc.status == IBV_WC_SUCCESS) {
                    if (wc.opcode == IBV_WC_SEND) {
                        send_completed = 1;
                    } else if (wc.opcode == IBV_WC_RECV) {
                        recv_completed = 1;
                    }
                } else {
                    fprintf(stderr, "[ALL-GATHER] CQ error: %s\n", ibv_wc_status_str(wc.status));
                    return -1;
                }
            }
        }

        // Process the current step's received data - just copy, no reduction
        char *curr_dst = main_buf + recv_chunk_idx * chunk_size * type_size;
        
        // All-gather: just copy the received data, no reduction
        memcpy(curr_dst, ops[current_op].recv_buffer, recv_count * type_size);
        
        // Update send buffer for next step - we'll send what we just received
        char *send_dst = (char*)handle->sendbuf + recv_chunk_idx * chunk_size * type_size;
        memcpy(send_dst, curr_dst, recv_count * type_size);

        // Switch to the other receive buffer for next iteration
        current_op = 1 - current_op;
    }
    
    // After all-gather, copy the complete result to the user buffer
    memcpy(recvbuf, main_buf, count * type_size);

    return 0;
}

// Rendezvous protocol for large messages using RDMA_READ
/**
 * @brief Rendezvous all-gather collective operation using ring algorithm with RDMA_READ and advanced pipelining.
 * 
 * ALGORITHM OVERVIEW:
 * The rendezvous all-gather implements a sophisticated 3-phase handshake protocol:
 * 1. Initialization: Position each process's reduced chunk correctly and set up RDMA regions
 * 2. Ring Algorithm: For (size-1) steps, each process:
 *    - Notifies RIGHT neighbor about available data (send ready signal)
 *    - Receives notification from LEFT neighbor (recv ready signal)
 *    - Performs RDMA_READ to pull data directly from LEFT neighbor's memory
 *    - Copies the read data to the correct position (no reduction, just gather)
 *    - Sends completion acknowledgment to LEFT neighbor
 *    - Receives completion acknowledgment from RIGHT neighbor
 * 3. Synchronization: All processes converge with complete gathered data
 * 
 * PIPELINING MECHANISM - 3-LEVEL OVERLAP:
 * This implementation uses advanced pipelining with 3 levels of overlap:
 * 
 * Level 1 - Communication Pipeline (2-slot):
 * - Two staging buffers (ops[0] and ops[1]) alternate between steps
 * - While one buffer handles RDMA_READ, the other can process completion acknowledgments
 * - The current_op variable toggles between 0 and 1 each step
 * 
 * Level 2 - Protocol Pipeline (3-phase handshake):
 * - Ready Signal: "I have data available for RDMA_READ"
 * - RDMA_READ: Direct memory access to pull chunk data
 * - Completion ACK: "RDMA_READ finished, data is safe to overwrite"
 * - Each phase can overlap with other processes' phases
 * 
 * Level 3 - Ring Pipeline (step overlap):
 * - While step N processes completion ACKs, step N+1 can start ready signals
 * - Multiple steps can be "in flight" simultaneously across the ring
 * - Maximizes bandwidth utilization across all network links
 * 
 * BUFFER ORGANIZATION:
 * - main_buf: Full workspace containing all chunks being gathered
 * - recv_chunk_buf: Base address for staging area (after main_buf)
 * - ops[0].recv_buffer: First staging slot for RDMA_READ targets
 * - ops[1].recv_buffer: Second staging slot for RDMA_READ targets
 * - RDMA registered memory: Each process's main_buf is registered for remote reads
 * - Signal buffers: Small registered regions for ready/completion handshake
 * 
 * INITIALIZATION PHASE:
 * After reduce-scatter, each process has its reduced chunk at the beginning of recvbuf.
 * Rendezvous all-gather must reposition this data:
 * - Clear main_buf completely  
 * - Place my reduced chunk at position (rank * chunk_size) in main_buf
 * - Register main_buf for RDMA operations (allow remote reads)
 * - This ensures each chunk is in its final position before ring circulation begins
 * 
 * @param handle: pointer to the process group handle
 * @param recvbuf: pointer to the buffer to receive - this is the buffer that will contain the gathered data
 * @param count: number of elements in the array that will be gathered
 * @param datatype: data type of the elements (e.g. DATATYPE_INT, DATATYPE_FLOAT)
 * @return 0 on success, -1 on failure
 */
static int rendezvous_all_gather(pg_handle_t *handle, void *recvbuf, int count, DATATYPE datatype) {
    int rank = handle->rank;
    int size = handle->size;
    size_t type_size = datatype_size(datatype);
    
    // Calculate chunk sizes and displacements correctly
    int chunk_size = count / size;  // Base chunk size
    int remainder = count % size;   // Extra elements for last processes
    
    // Pre-calculate the exact size and displacement of each chunk
    int *counts = (int *)malloc(size * sizeof(int));
    int *displs = (int *)malloc(size * sizeof(int));
    
    for (int i = 0; i < size; ++i) {
        // Each process gets at least chunk_size elements
        // The last (remainder) processes get one extra element each
        counts[i] = (i < size - remainder) ? chunk_size : chunk_size + 1;
        displs[i] = (i == 0) ? 0 : displs[i - 1] + counts[i - 1];
    }
    
    // Calculate buffer requirements using the largest chunk
    size_t max_chunk_bytes = 0;
    for (int i = 0; i < size; i++) {
        size_t chunk_bytes = counts[i] * type_size;
        if (chunk_bytes > max_chunk_bytes) {
            max_chunk_bytes = chunk_bytes;
        }
    }
    
    size_t total_bytes = count * type_size;
    size_t pipeline_buf_size = 2 * max_chunk_bytes;

    if (total_bytes + pipeline_buf_size > handle->mr_recv->length) {
        fprintf(stderr, "[rendezvous_all_gather] recvbuf too small for pipelining\n");
        free(counts);
        free(displs);
        return -1;
    }

    char *main_buf = (char *)handle->recvbuf;
    char *recv_chunk_buf = main_buf + total_bytes;
    
    // After reduce-scatter, each process has its reduced chunk at the beginning of recvbuf
    // We need to position this data correctly for the all-gather operation
    
    // Clear main_buf first
    memset(main_buf, 0, count * type_size);
    
    // Place my reduced chunk data in the correct position
    int my_chunk_count = counts[rank];
    char *my_chunk_dst = main_buf + displs[rank] * type_size;
    memcpy(my_chunk_dst, recvbuf, my_chunk_count * type_size);
    
    // Copy to sendbuf for RDMA exposure
    memcpy(handle->sendbuf, main_buf, count * type_size);

    pipeline_op_t ops[2];
    ops[0].recv_buffer = recv_chunk_buf;
    ops[1].recv_buffer = recv_chunk_buf + max_chunk_bytes;
    int current_op = 0;
    int prev_op = 1;

    typedef struct { int step_id; } request_t;
    typedef struct { uintptr_t remote_addr; uint32_t remote_rkey; } response_t;

    // Allocate handshake message buffers inside the registered send buffer
    size_t msg_size = sizeof(request_t) + sizeof(response_t);
    char* handshake_buf_base = (char*)handle->sendbuf + handle->bufsize - (2 * msg_size);
    
    request_t* left_req   = (request_t*)(handshake_buf_base);
    response_t* left_resp  = (response_t*)(handshake_buf_base + sizeof(request_t));
    request_t* right_req  = (request_t*)(handshake_buf_base + msg_size);
    response_t* right_resp = (response_t*)(handshake_buf_base + msg_size + sizeof(request_t));

    for (int step = 0; step < size - 1; ++step) {
        // All-gather ring: each process sends its current chunk and receives the next chunk
        int send_chunk_idx = (rank - step + size) % size;
        int recv_chunk_idx = (rank - step - 1 + size) % size;
        
        // Calculate counts for sending and receiving using the correct chunk sizes
        int send_count = counts[send_chunk_idx];
        int recv_count = counts[recv_chunk_idx];

        // STEP 1: PROCESS PREVIOUS STEP'S DATA FIRST (IF ANY)
        // Wait for previous RDMA_READ to complete and copy into main_buf
        if (step > 0) {
            int prev_step = step - 1;
            int prev_recv_chunk_idx = (rank - prev_step - 1 + size) % size;
            int prev_recv_count = counts[prev_recv_chunk_idx];

            wait_for_completion(handle->cq, IBV_WC_RDMA_READ, prev_step);

            // Use displacement array for correct addressing
            char *prev_dst = main_buf + displs[prev_recv_chunk_idx] * type_size;

            // All-gather: just copy the received data into main_buf 
            memcpy(prev_dst, ops[prev_op].recv_buffer, prev_recv_count * type_size);
        }

        // STEP 2: PREPARE SEND BUFFER (STABLE SNAPSHOT)
        // Copy the current chunk from main_buf that will be exposed for RDMA_READ
        char *send_src = main_buf + displs[send_chunk_idx] * type_size;
        char *send_ptr = (char*)handle->sendbuf + displs[send_chunk_idx] * type_size;
        
        memcpy(send_ptr, send_src, send_count * type_size);

        // STEP 3: HANDSHAKE - EXPOSE THE STABLE SEND BUFFER
        struct ibv_recv_wr *bad_wr;
        struct ibv_send_wr *bad_send_wr;
        
        // -- Phase A: Exchange Requests --
        
        // Post receive for the request from our RIGHT neighbor.
        struct ibv_sge recv_req_sge = {.addr = (uintptr_t)right_req, .length = sizeof(request_t), .lkey = handle->mr_send->lkey};
        struct ibv_recv_wr recv_req_wr = {.wr_id = step * 10, .sg_list = &recv_req_sge, .num_sge = 1};
        if(ibv_post_recv(handle->qps[1], &recv_req_wr, &bad_wr)) return -1;

        // Send our request to our LEFT neighbor.
        left_req->step_id = step;
        struct ibv_sge send_req_sge = {.addr = (uintptr_t)left_req, .length = sizeof(request_t), .lkey = handle->mr_send->lkey};
        struct ibv_send_wr send_req_wr = {.wr_id = step * 10, .opcode = IBV_WR_SEND, .send_flags = IBV_SEND_SIGNALED, .sg_list = &send_req_sge, .num_sge = 1};
        if(ibv_post_send(handle->qps[0], &send_req_wr, &bad_send_wr)) return -1;

        // Wait for Phase A completions
        int recv_req_done = 0, send_req_done = 0;
        while (!recv_req_done || !send_req_done) {
            struct ibv_wc wc;
            int ne = ibv_poll_cq(handle->cq, 1, &wc);
            if (ne > 0) {
                if (wc.status != IBV_WC_SUCCESS) {
                    fprintf(stderr, "[ALL-GATHER-RDMA-READ] Phase A completion failed: %s\n", ibv_wc_status_str(wc.status));
                    return -1;
                }
                
                if (wc.wr_id == step * 10 && wc.opcode == IBV_WC_RECV && !recv_req_done) {
                    recv_req_done = 1;
                } else if (wc.wr_id == step * 10 && wc.opcode == IBV_WC_SEND && !send_req_done) {
                    send_req_done = 1;
                }
            }
        }
        
        // -- Phase B: Exchange Responses --

        // Post receive for the response from our LEFT neighbor.
        struct ibv_sge recv_resp_sge = {.addr = (uintptr_t)left_resp, .length = sizeof(response_t), .lkey = handle->mr_send->lkey};
        struct ibv_recv_wr recv_resp_wr = {.wr_id = step * 10 + 1, .sg_list = &recv_resp_sge, .num_sge = 1};
        if(ibv_post_recv(handle->qps[0], &recv_resp_wr, &bad_wr)) return -1;

        // Send our response to our RIGHT neighbor with the STABLE send buffer address
        right_resp->remote_addr = (uintptr_t)send_ptr;
        right_resp->remote_rkey = handle->mr_send->rkey;
        
        struct ibv_sge send_resp_sge = {.addr = (uintptr_t)right_resp, .length = sizeof(response_t), .lkey = handle->mr_send->lkey};
        struct ibv_send_wr send_resp_wr = {.wr_id = step * 10 + 1, .opcode = IBV_WR_SEND, .send_flags = IBV_SEND_SIGNALED, .sg_list = &send_resp_sge, .num_sge = 1};
        if(ibv_post_send(handle->qps[1], &send_resp_wr, &bad_send_wr)) return -1;

        // Wait for Phase B completions
        int recv_resp_done = 0, send_resp_done = 0;
        while (!recv_resp_done || !send_resp_done) {
            struct ibv_wc wc;
            int ne = ibv_poll_cq(handle->cq, 1, &wc);
            if (ne > 0) {
                if (wc.status != IBV_WC_SUCCESS) {
                    fprintf(stderr, "[ALL-GATHER-RDMA-READ] Phase B completion failed: %s\n", ibv_wc_status_str(wc.status));
                    return -1;
                }
                
                if (wc.wr_id == step * 10 + 1 && wc.opcode == IBV_WC_RECV && !recv_resp_done) {
                    recv_resp_done = 1;
                } else if (wc.wr_id == step * 10 + 1 && wc.opcode == IBV_WC_SEND && !send_resp_done) {
                    send_resp_done = 1;
                }
            }
        }

        // STEP 4: ISSUE NEW RDMA_READ FOR CURRENT STEP
        struct ibv_sge read_sge = {
            .addr = (uintptr_t)ops[current_op].recv_buffer,
            .length = recv_count * type_size,
            .lkey = handle->mr_recv->lkey
        };

        struct ibv_send_wr read_wr = {
            .wr_id = step,
            .opcode = IBV_WR_RDMA_READ,
            .send_flags = IBV_SEND_SIGNALED,
            .sg_list = &read_sge,
            .num_sge = 1,
            .wr.rdma = {
                .remote_addr = left_resp->remote_addr,
                .rkey = left_resp->remote_rkey
            }
        };

        if (ibv_post_send(handle->qps[0], &read_wr, &bad_send_wr)) {
            fprintf(stderr, "[ALL-GATHER-RDMA-READ] Failed to post RDMA_READ\n");
            return -1;
        }
                
        prev_op = current_op;
        current_op = 1 - current_op;
    }

    // Final Step Processing
    if (size > 1) {
        int final_step = size - 2;
        int final_recv_chunk_idx = (rank - final_step - 1 + size) % size;
        int final_recv_count = counts[final_recv_chunk_idx];

        wait_for_completion(handle->cq, IBV_WC_RDMA_READ, final_step);
        
        // Use displacement array for correct addressing
        char *final_dst = main_buf + displs[final_recv_chunk_idx] * type_size;
        
        // All-gather: just copy the received data
        memcpy(final_dst, ops[prev_op].recv_buffer, final_recv_count * type_size);
    }

    // Final data placement - copy complete result to user buffer
    memcpy(recvbuf, main_buf, count * type_size);

    // Add barrier synchronization    
    // Simple barrier using existing handshake infrastructure
    typedef struct { int completion_signal; } barrier_msg_t;
    barrier_msg_t *completion_msg = (barrier_msg_t*)((char*)handle->sendbuf + handle->bufsize - sizeof(barrier_msg_t));
    completion_msg->completion_signal = rank;
    
    // Send completion signal to both neighbors
    struct ibv_sge barrier_sge = {.addr = (uintptr_t)completion_msg, .length = sizeof(barrier_msg_t), .lkey = handle->mr_send->lkey};
    struct ibv_send_wr barrier_wr_left = {.wr_id = 9999, .opcode = IBV_WR_SEND, .send_flags = IBV_SEND_SIGNALED, .sg_list = &barrier_sge, .num_sge = 1};
    struct ibv_send_wr barrier_wr_right = {.wr_id = 9998, .opcode = IBV_WR_SEND, .send_flags = IBV_SEND_SIGNALED, .sg_list = &barrier_sge, .num_sge = 1};
    
    // Post receives for completion signals from both neighbors
    barrier_msg_t *left_completion = (barrier_msg_t*)((char*)handle->sendbuf + handle->bufsize - 2*sizeof(barrier_msg_t));
    barrier_msg_t *right_completion = (barrier_msg_t*)((char*)handle->sendbuf + handle->bufsize - 3*sizeof(barrier_msg_t));
    
    struct ibv_sge left_barrier_sge = {.addr = (uintptr_t)left_completion, .length = sizeof(barrier_msg_t), .lkey = handle->mr_send->lkey};
    struct ibv_recv_wr left_barrier_wr = {.wr_id = 9997, .sg_list = &left_barrier_sge, .num_sge = 1};
    struct ibv_sge right_barrier_sge = {.addr = (uintptr_t)right_completion, .length = sizeof(barrier_msg_t), .lkey = handle->mr_send->lkey};
    struct ibv_recv_wr right_barrier_wr = {.wr_id = 9996, .sg_list = &right_barrier_sge, .num_sge = 1};
    
    struct ibv_recv_wr *bad_recv_wr;
    struct ibv_send_wr *bad_send_wr;
    
    ibv_post_recv(handle->qps[0], &left_barrier_wr, &bad_recv_wr);
    ibv_post_recv(handle->qps[1], &right_barrier_wr, &bad_recv_wr);
    
    ibv_post_send(handle->qps[0], &barrier_wr_left, &bad_send_wr);
    ibv_post_send(handle->qps[1], &barrier_wr_right, &bad_send_wr);
    
    // Wait for all barrier operations to complete
    int barrier_ops_done = 0;
    while (barrier_ops_done < 4) { // 2 sends + 2 receives
        struct ibv_wc wc;
        int ne = ibv_poll_cq(handle->cq, 1, &wc);
        if (ne > 0 && wc.status == IBV_WC_SUCCESS && wc.wr_id >= 9996 && wc.wr_id <= 9999) {
            barrier_ops_done++;
        }
    }
    
    // Clean up
    free(counts);
    free(displs);
    return 0;
}

/**
 * @brief Pipelined all-gather collective operation
 * @param handle: pointer to the process group handle
 * @param recvbuf: pointer to the buffer to receive - this is the buffer that will contain the gathered data
 * @param count: number of elements in the array that will be gathered
 * @param datatype: data type of the elements (e.g. DATATYPE_INT, DATATYPE_FLOAT)
 * @return 0 on success, -1 on failure
 */
static int pipelined_all_gather(pg_handle_t *handle, void *recvbuf, int count, DATATYPE datatype) {
    // Choose between eager and rendezvous based on message size
    if (count * datatype_size(datatype) <= EAGER_THRESHOLD) {
        return eager_all_gather(handle, recvbuf, count, datatype);
    } else {
        return rendezvous_all_gather(handle, recvbuf, count, datatype);
    }
}

// All-reduce collective operation (ring, pipelined, zero-copy for large messages)
/**
 * @brief All-reduce collective operation (ring, pipelined, zero-copy for large messages)
 * @param sendbuf: pointer to the buffer to send - this is the buffer that will be reduced (already contains the data to be reduced)
 * @param recvbuf: pointer to the buffer to receive - this is the buffer that will contain the reduced data
 * @param count: number of elements in the array that will be reduced (e.g. an array of ints of size N will have count = N)
 * @param datatype: data type of the elements (e.g. DATATYPE_INT, DATATYPE_FLOAT)
 * @param op: operation to perform (e.g. OPERATION_SUM, OPERATION_MAX, OPERATION_MIN)
 * @param pg_handle: pointer to the process group handle
 * @return 0 on success, -1 on failure
 */
int pg_all_reduce(void *sendbuf, void *recvbuf, int count,
                  DATATYPE datatype, OPERATION op, void *pg_handle) {
    if (!pg_handle || !sendbuf || !recvbuf || count <= 0) {
        fprintf(stderr, "[pg_all_reduce] Invalid arguments\n");
        return -1;
    }
    pg_handle_t *handle = (pg_handle_t *)pg_handle;
    int rank = handle->rank;
    int size = handle->size;
    size_t type_size = datatype_size(datatype);
    if (type_size == 0) {
        fprintf(stderr, "[pg_all_reduce] Unsupported datatype\n");
        return -1;
    }
 
    // Copy entire sendbuf to recvbuf as initial value
    memcpy(recvbuf, sendbuf, count * type_size);

    // Reduce-Scatter phase
    if (pipelined_reduce_scatter(handle, recvbuf, count, datatype, op) != 0) {
        fprintf(stderr, "[pg_all_reduce] Reduce-Scatter phase failed\n");
        return -1;
    }

    // All-Gather phase
    if (pipelined_all_gather(handle, recvbuf, count, datatype) != 0) {
        fprintf(stderr, "[pg_all_reduce] All-Gather phase failed\n");
        return -1;
    }

    return 0;
}

// Close and clean up process group
/**
 * @brief Close and clean up process group
 * @param pg_handle: pointer to the process group handle
 * @return 0 on success, -1 on failure
 */
int pg_close(void *pg_handle) {
    pg_handle_t *handle = (pg_handle_t *)pg_handle;
    cleanup_pg_handle(handle);
    return 0;
}
