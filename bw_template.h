#pragma once

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/param.h>
#include <sys/time.h>
#include <stdlib.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <time.h>

#include "common.h"

#define WC_BATCH (1)
int kv_buf_size = roundup(sizeof(kv_buf), sysconf(_SC_PAGESIZE));
enum
{
    PINGPONG_RECV_WRID = 1,
    PINGPONG_SEND_WRID = 2,
};

static int page_size;

struct pingpong_dest
{
    int lid;
    int qpn;
    int psn;
    union ibv_gid gid;
};


// Converts an MTU(Maximum Transmission Unit:largest size of a packet that can be sent over a network without needing to be fragmented) value to the
// corresponding enum value. Used to correctly set the MTU  of the connection in RDMA communication. The function takes an integer MTU value and
// outputs the corresponding enum value (`enum ibv_mtu`) if it matches one of the supported MTU values. This is critical for ensuring proper data
// packet size when establishing a reliable RDMA connection.
int pp_mtu_to_enum(int mtu, enum ibv_mtu &res)
{
    switch (mtu)
    {
    case 256:
        res = IBV_MTU_256;
        break;
    case 512:
        res = IBV_MTU_512;
        break;
    case 1024:
        res = IBV_MTU_1024;
        break;
    case 2048:
        res = IBV_MTU_2048;
        break;
    case 4096:
        res = IBV_MTU_4096;
        break;
    default:
        return -1;
    }
    return 0;
}

// Gets the local LID (Local Identifier) of a specified port on an RDMA device. LID is a unique identifier for nodes in an InfiniBand subnet. This
// function is used to retrieve the local LID that helps identify the endpoint within the InfiniBand network, which is essential during connection
// establishment.
uint16_t pp_get_local_lid(struct ibv_context *context, int port)
{
    struct ibv_port_attr attr;

    if (ibv_query_port(context, port, &attr))
        return 0;

    return attr.lid;
}

// Retrieves information about a specific port of an RDMA device, including link layer type, LID, and other attributes. This function is used to
// gather details about the port that are necessary for setting up the RDMA connection properly, such as determining whether the link layer is 
// InfiniBand or Ethernet.
int pp_get_port_info(struct ibv_context *context, int port,
                     struct ibv_port_attr *attr)
{
    return ibv_query_port(context, port, attr);
}

// Converts a wire-format GID (Global Identifier) into an RDMA-friendly GID representation. GID is used to uniquely identify a node in the network,
// especially in RoCE (RDMA over Converged Ethernet) environments. This function is called when receiving a GID from another node, and it converts 
// the received wire-format GID into a format that can be used in RDMA operations.
void wire_gid_to_gid(const char *wgid, union ibv_gid *gid)
{
    char tmp[9];
    uint32_t v32;
    int i;

    for (tmp[8] = 0, i = 0; i < 4; ++i)
    {
        memcpy(tmp, wgid + i * 8, 8);
        sscanf(tmp, "%x", &v32);
        *(uint32_t *)(&gid->raw[i * 4]) = ntohl(v32);
    }
}


// Converts an RDMA GID into a wire-format representation that can be sent over the network. The GID needs to be shared between nodes to set up
// communication paths. This function is used when preparing to send the GID over a socket connection to another node during the connection setup phase.
void gid_to_wire_gid(const union ibv_gid *gid, char wgid[])
{
    int i;

    for (i = 0; i < 4; ++i)
        sprintf(&wgid[i * 8], "%08x", htonl(*(uint32_t *)(gid->raw + i * 4)));
}


// Connects the Queue Pair (QP) with another node's endpoint information by modifying the QP state to Ready-to-Receive (RTR) and Ready-to-Send (RTS). 
// The function sets attributes like Path MTU, destination QP number, LID, and GID, making the QP ready for RDMA communication. This is a critical 
// step in establishing the RDMA connection between two nodes.
static int pp_connect_ctx(struct pingpong_context *ctx, int port, int my_psn,
                          enum ibv_mtu mtu, int sl,
                          struct pingpong_dest *dest, int sgid_idx)
{
    struct ibv_qp_attr attr = {
        .qp_state = IBV_QPS_RTR,
        .path_mtu = mtu,
        .rq_psn = dest->psn,
        .dest_qp_num = dest->qpn,
        .ah_attr = {
            .dlid = dest->lid,
            .sl = sl,
            .src_path_bits = 0,
            .is_global = 0,
            .port_num = port},
        .max_dest_rd_atomic = 1,
        .min_rnr_timer = 12};

    if (dest->gid.global.interface_id)
    {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.dgid = dest->gid;
        attr.ah_attr.grh.sgid_index = sgid_idx;
    }
    if (ibv_modify_qp(ctx->qp, &attr,
                      IBV_QP_STATE |
                          IBV_QP_AV |
                          IBV_QP_PATH_MTU |
                          IBV_QP_DEST_QPN |
                          IBV_QP_RQ_PSN |
                          IBV_QP_MAX_DEST_RD_ATOMIC |
                          IBV_QP_MIN_RNR_TIMER))
    {
        fprintf(stderr, "Failed to modify QP to RTR\n");
        return 1;
    }

    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = my_psn;
    attr.max_rd_atomic = 1;
    if (ibv_modify_qp(ctx->qp, &attr,
                      IBV_QP_STATE |
                          IBV_QP_TIMEOUT |
                          IBV_QP_RETRY_CNT |
                          IBV_QP_RNR_RETRY |
                          IBV_QP_SQ_PSN |
                          IBV_QP_MAX_QP_RD_ATOMIC))
    {
        fprintf(stderr, "Failed to modify QP to RTS\n");
        return 1;
    }

    return 0;
}

// Exchanges connection information (e.g., LID, QPN, PSN, GID) with the server to establish a connection from the client-side. This function uses
// TCP sockets to send and receive the connection information, which is essential to set up the RDMA communication between the client and the server.
static struct pingpong_dest *pp_client_exch_dest(const char *servername, int port,
                                                 const struct pingpong_dest *my_dest)
{
    struct addrinfo *res, *t;
    struct addrinfo hints = {
        .ai_family = AF_INET,
        .ai_socktype = SOCK_STREAM};
    char *service;
    char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
    int n;
    int sockfd = -1;
    struct pingpong_dest *rem_dest = NULL;
    char gid[33];

    if (asprintf(&service, "%d", port) < 0)
        return NULL;

    n = getaddrinfo(servername, service, &hints, &res);

    if (n < 0)
    {
        fprintf(stderr, "%s for %s:%d\n", gai_strerror(n), servername, port);
        free(service);
        return NULL;
    }

    for (t = res; t; t = t->ai_next)
    {
        sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (sockfd >= 0)
        {
            if (!connect(sockfd, t->ai_addr, t->ai_addrlen))
                break;
            close(sockfd);
            sockfd = -1;
        }
    }

    freeaddrinfo(res);
    free(service);

    if (sockfd < 0)
    {
        fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
        return NULL;
    }

    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
    if (write(sockfd, msg, sizeof msg) != sizeof msg)
    {
        fprintf(stderr, "Couldn't send local address\n");
        goto out;
    }

    if (read(sockfd, msg, sizeof msg) != sizeof msg)
    {
        perror("client read");
        fprintf(stderr, "Couldn't read remote address\n");
        goto out;
    }

    write(sockfd, "done", sizeof "done");

    rem_dest = (struct pingpong_dest *)malloc(sizeof *rem_dest);
    if (!rem_dest)
        goto out;

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);

out:
    close(sockfd);
    return rem_dest;
}


// Exchanges connection information with the client to establish the connection from the server-side. It listens on a specified port, accepts 
// an incoming connection, and exchanges RDMA-related information, such as LID, QPN, PSN, and GID. This is necessary for establishing a 
// connection-oriented RDMA session.
static struct pingpong_dest *pp_server_exch_dest(struct pingpong_context *ctx,
                                                 int ib_port, enum ibv_mtu mtu,
                                                 int port, int sl,
                                                 const struct pingpong_dest *my_dest,
                                                 int sgid_idx)
{
    struct addrinfo *res, *t;
    struct addrinfo hints = {
        .ai_flags = AI_PASSIVE,
        .ai_family = AF_INET,
        .ai_socktype = SOCK_STREAM};
    char *service;
    char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
    int n;
    int sockfd = -1, connfd;
    struct pingpong_dest *rem_dest = NULL;
    char gid[33];

    if (asprintf(&service, "%d", port) < 0)
        return NULL;

    n = getaddrinfo(NULL, service, &hints, &res);

    if (n < 0)
    {
        fprintf(stderr, "%s for port %d\n", gai_strerror(n), port);
        free(service);
        return NULL;
    }

    for (t = res; t; t = t->ai_next)
    {
        sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (sockfd >= 0)
        {
            n = 1;

            setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);

            if (!bind(sockfd, t->ai_addr, t->ai_addrlen))
                break;
            close(sockfd);
            sockfd = -1;
        }
    }

    freeaddrinfo(res);
    free(service);

    if (sockfd < 0)
    {
        fprintf(stderr, "Couldn't listen to port %d\n", port);
        return NULL;
    }

    listen(sockfd, 1);
    connfd = accept(sockfd, NULL, 0);
    close(sockfd);
    if (connfd < 0)
    {
        fprintf(stderr, "accept() failed\n");
        return NULL;
    }

    n = read(connfd, msg, sizeof msg);
    if (n != sizeof msg)
    {
        perror("server read");
        fprintf(stderr, "%d/%d: Couldn't read remote address\n", n, (int)sizeof msg);
        goto out;
    }

    rem_dest = (struct pingpong_dest *)malloc(sizeof *rem_dest);
    if (!rem_dest)
        goto out;

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);

    if (pp_connect_ctx(ctx, ib_port, my_dest->psn, mtu, sl, rem_dest, sgid_idx))
    {
        fprintf(stderr, "Couldn't connect to remote QP\n");
        free(rem_dest);
        rem_dest = NULL;
        goto out;
    }

    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
    if (write(connfd, msg, sizeof msg) != sizeof msg)
    {
        fprintf(stderr, "Couldn't send local address\n");
        free(rem_dest);
        rem_dest = NULL;
        goto out;
    }

    read(connfd, msg, sizeof msg);

out:
    close(connfd);
    return rem_dest;
}

#include <sys/param.h>

// Initializes the RDMA context, which includes setting up resources such as Protection Domain (PD), Memory Region (MR), Completion Queue (CQ),
// and Queue Pair (QP). This function is responsible for allocating and initializing all the resources required for RDMA operations, making 
// it a foundational step before any data transfer can occur.
static struct pingpong_context *pp_init_ctx(struct ibv_device *ib_dev, int size,
                                            int rx_depth, int tx_depth, int port,
                                            int use_event, int is_server)
{
    struct pingpong_context *ctx;

    ctx = (struct pingpong_context *)calloc(1, sizeof *ctx);
    if (!ctx)
        return NULL;

    ////////
    size = kv_buf_size;
    //////////////////////
    ctx->size = size;
    ctx->rx_depth = rx_depth;
    ctx->routs = rx_depth;

    ctx->buf = malloc(roundup(size, page_size));

    if (!ctx->buf)
    {
        fprintf(stderr, "Couldn't allocate work buf.\n");
        return NULL;
    }

    memset(ctx->buf, 0x7b + is_server, size);

    ctx->context = ibv_open_device(ib_dev);
    if (!ctx->context)
    {
        fprintf(stderr, "Couldn't get context for %s\n",
                ibv_get_device_name(ib_dev));
        return NULL;
    }

    if (use_event)
    {
        ctx->channel = ibv_create_comp_channel(ctx->context);
        if (!ctx->channel)
        {
            fprintf(stderr, "Couldn't create completion channel\n");
            return NULL;
        }
    }
    else
        ctx->channel = NULL;

    ctx->pd = ibv_alloc_pd(ctx->context);
    if (!ctx->pd)
    {
        fprintf(stderr, "Couldn't allocate PD\n");
        return NULL;
    }

    ctx->mr = ibv_reg_mr(ctx->pd, ctx->buf, size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);

    if (!ctx->mr)
    {
        fprintf(stderr, "Couldn't register MR\n");
        return NULL;
    }

    ctx->cq = ibv_create_cq(ctx->context, rx_depth + tx_depth, NULL,
                            ctx->channel, 0);
    if (!ctx->cq)
    {
        fprintf(stderr, "Couldn't create CQ\n");
        return NULL;
    }

    {
        struct ibv_qp_init_attr attr = {
            .send_cq = ctx->cq,
            .recv_cq = ctx->cq,
            .cap = {
                .max_send_wr = tx_depth,
                .max_recv_wr = rx_depth,
                .max_send_sge = 1,
                .max_recv_sge = 1},
            .qp_type = IBV_QPT_RC};

        ctx->qp = ibv_create_qp(ctx->pd, &attr);
        if (!ctx->qp)
        {
            fprintf(stderr, "Couldn't create QP\n");
            return NULL;
        }
    }

    {
        struct ibv_qp_attr attr = {
            .qp_state = IBV_QPS_INIT,
            .qp_access_flags = IBV_ACCESS_REMOTE_READ |
                               IBV_ACCESS_REMOTE_WRITE,
            .pkey_index = 0,
            .port_num = port};

        if (ibv_modify_qp(ctx->qp, &attr,
                          IBV_QP_STATE |
                              IBV_QP_PKEY_INDEX |
                              IBV_QP_PORT |
                              IBV_QP_ACCESS_FLAGS))
        {
            fprintf(stderr, "Failed to modify QP to INIT\n");
            return NULL;
        }
    }

    return ctx;
}


// Closes the RDMA context, releasing all resources such as Queue Pairs (QPs), Completion Queues (CQs), Memory Regions (MRs), and Protection Domains
// (PDs). This function is essential for cleaning up and freeing resources when the RDMA connection is no longer needed to avoid memory leaks.
int pp_close_ctx(struct pingpong_context *ctx)
{
    if (ibv_destroy_qp(ctx->qp))
    {
        fprintf(stderr, "Couldn't destroy QP\n");
        return 1;
    }

    if (ibv_destroy_cq(ctx->cq))
    {
        fprintf(stderr, "Couldn't destroy CQ\n");
        return 1;
    }

    if (ibv_dereg_mr(ctx->mr))
    {
        fprintf(stderr, "Couldn't deregister MR\n");
        return 1;
    }

    if (ibv_dealloc_pd(ctx->pd))
    {
        fprintf(stderr, "Couldn't deallocate PD\n");
        return 1;
    }

    if (ctx->channel)
    {
        if (ibv_destroy_comp_channel(ctx->channel))
        {
            fprintf(stderr, "Couldn't destroy completion channel\n");
            return 1;
        }
    }

    if (ibv_close_device(ctx->context))
    {
        fprintf(stderr, "Couldn't release context\n");
        return 1;
    }

    free(ctx->buf);
    free(ctx);

    return 0;
}


// Posts receive requests to the Queue Pair (QP) to prepare for receiving incoming data. This function sets up buffers in the receive queue, 
// which is crucial before initiating data transfer. Posting receive buffers ensures that incoming messages have a place to be stored when 
// they arrive.
static int pp_post_recv(struct pingpong_context *ctx, int n)
{
    struct ibv_sge list = {
        .addr = (uintptr_t)ctx->buf,
        .length = ctx->size,
        .lkey = ctx->mr->lkey};
    struct ibv_recv_wr wr = {
        .wr_id = PINGPONG_RECV_WRID,
        .next = NULL,
        .sg_list = &list,
        .num_sge = 1};
    struct ibv_recv_wr *bad_wr;
    int i;

    for (i = 0; i < n; ++i)
        if (ibv_post_recv(ctx->qp, &wr, &bad_wr))
            break;

    return i;
}


// Posts a send request using the context's pre-registered buffer. This function is used to initiate a send operation, which transfers data 
// from the local memory to the remote peer. It is often used for small control messages or simple data exchanges.
int pp_post_send(struct pingpong_context *ctx)
{
    struct ibv_sge list = {
        .addr = (uint64_t)ctx->buf,
        .length = ctx->size,
        .lkey = ctx->mr->lkey};

    struct ibv_send_wr *bad_wr, wr = {
                                    .wr_id = PINGPONG_SEND_WRID,
                                    .next = NULL,
                                    .sg_list = &list,
                                    .num_sge = 1,
                                    .opcode = IBV_WR_SEND,
                                    .send_flags = IBV_SEND_SIGNALED};

    return ibv_post_send(ctx->qp, &wr, &bad_wr);
}


// Posts a send request using user-provided data to a specified remote address. This function allows the sending of data using RDMA_WRITE or 
// RDMA_READ, enabling direct memory access operations on the remote machine. It is a key function for leveraging the efficiency of RDMA operations.
int pp_post_send(struct pingpong_context *ctx, const char *local_data, void *remote_ptr, uint32_t rkey, enum ibv_wr_opcode opcode)
{
    struct ibv_sge list = {
        .addr = (uintptr_t)local_data,
        .length = ctx->size,
        .lkey = ctx->mr->lkey};

    struct ibv_send_wr *bad_wr, wr = {
                                    .wr_id = PINGPONG_SEND_WRID,
                                    .next = NULL,
                                    .sg_list = &list,
                                    .num_sge = 1,
                                    .opcode = opcode,
                                    .send_flags = IBV_SEND_SIGNALED};

    wr.wr.rdma.remote_addr = (uintptr_t)remote_ptr;
    wr.wr.rdma.rkey = rkey;

    return ibv_post_send(ctx->qp, &wr, &bad_wr);
}


// Waits for a specified number of completions on the Completion Queue (CQ). Completions are generated when send or receive operations complete, 
// and this function ensures that all posted operations have completed successfully before proceeding. It is crucial for synchronizing RDMA 
// operations and ensuring data integrity.
int pp_wait_completions(struct pingpong_context *ctx, int iters)
{
    int rcnt = 0, scnt = 0;
    while (rcnt + scnt < iters)
    {
        struct ibv_wc wc[WC_BATCH];
        int ne, i;

        do
        {
            ne = ibv_poll_cq(ctx->cq, WC_BATCH, wc);
            if (ne < 0)
            {
                fprintf(stderr, "poll CQ failed %d\n", ne);
                return 1;
            }

        } while (ne < 1);

        for (i = 0; i < ne; ++i)
        {
            if (wc[i].status != IBV_WC_SUCCESS)
            {
                fprintf(stderr, "Failed status %s (%d) for wr_id %d\n",
                        ibv_wc_status_str(wc[i].status),
                        wc[i].status, (int)wc[i].wr_id);
                return 1;
            }

            switch ((int)wc[i].wr_id)
            {
            case PINGPONG_SEND_WRID:
                ++scnt;
                break;

            case PINGPONG_RECV_WRID:
                // if (--ctx->routs <= 10)
                // {
                //     ctx->routs += pp_post_recv(ctx, ctx->rx_depth - ctx->routs);
                //     if (ctx->routs < ctx->rx_depth)
                //     {
                //         fprintf(stderr,
                //                 "Couldn't post receive (%d)\n",
                //                 ctx->routs);
                //         return 1;
                //     }
                // }
                ++rcnt;
                break;

            default:
                fprintf(stderr, "Completion for unknown wr_id %d\n",
                        (int)wc[i].wr_id);
                return 1;
            }
        }
    }
    return 0;
}


// Displays usage information for the program, including available command-line options. This function provides users with guidance on how
// to run the program and use the various options available for configuring the RDMA connection.
static void usage(const char *argv0)
{
    printf("Usage:\n");
    printf("  %s            start a server and wait for connection\n", argv0);
    printf("  %s <host>     connect to server at <host>\n", argv0);
    printf("\n");
    printf("Options:\n");
    printf("  -p, --port=<port>      listen on/connect to port <port> (default 18515)\n");
    printf("  -d, --ib-dev=<dev>     use IB device <dev> (default first device found)\n");
    printf("  -i, --ib-port=<port>   use port <port> of IB device (default 1)\n");
    printf("  -s, --size=<size>      size of message to exchange (default 4096)\n");
    printf("  -m, --mtu=<size>       path MTU (default 1024)\n");
    printf("  -r, --rx-depth=<dep>   number of receives to post at a time (default 500)\n");
    printf("  -n, --iters=<iters>    number of exchanges (default 1000)\n");
    printf("  -l, --sl=<sl>          service level value\n");
    printf("  -e, --events           sleep on CQ events (default poll)\n");
    printf("  -g, --gid-idx=<gid index> local port gid index\n");
}



int main_ex(char *servername, KV_handle *kv_handle)
{
    int argc = kv_handle->argc;
    char **argv = kv_handle->argv;

    struct ibv_device **dev_list;
    struct ibv_device *ib_dev;
    struct pingpong_context *ctx;
    struct pingpong_dest my_dest;
    struct pingpong_dest *rem_dest;
    char *ib_devname = NULL;
    int port = 1299;
    int ib_port = 1;
    enum ibv_mtu mtu = IBV_MTU_2048;
    int rx_depth = 100;
    int tx_depth = 100;
    int iters = 1000;
    int use_event = 1;
    int size = 1;
    int sl = 0;
    int gidx = -1;
    char gid[33];

    srand48(getpid() * time(NULL));

    while (1)
    {
        int c;

        static struct option long_options[] = {
            {.name = "port", .has_arg = 1, .val = 'p'},
            {.name = "ib-dev", .has_arg = 1, .val = 'd'},
            {.name = "ib-port", .has_arg = 1, .val = 'i'},
            {.name = "size", .has_arg = 1, .val = 's'},
            {.name = "mtu", .has_arg = 1, .val = 'm'},
            {.name = "rx-depth", .has_arg = 1, .val = 'r'},
            {.name = "iters", .has_arg = 1, .val = 'n'},
            {.name = "sl", .has_arg = 1, .val = 'l'},
            {.name = "events", .has_arg = 0, .val = 'e'},
            {.name = "gid-idx", .has_arg = 1, .val = 'g'},
            {0}};

        c = getopt_long(argc, argv, "p:d:i:s:m:r:n:l:eg:", long_options, NULL);
        if (c == -1)
            break;

        switch (c)
        {
        case 'p':
            port = strtol(optarg, NULL, 0);
            if (port < 0 || port > 65535)
            {
                usage(argv[0]);
                return 1;
            }
            break;

        case 'd':
            ib_devname = strdup(optarg);
            break;

        case 'i':
            ib_port = strtol(optarg, NULL, 0);
            if (ib_port < 0)
            {
                usage(argv[0]);
                return 1;
            }
            break;

        case 's':
            size = strtol(optarg, NULL, 0);
            break;

        case 'm':
            if (pp_mtu_to_enum(strtol(optarg, NULL, 0), mtu) < 0)
            {
                usage(argv[0]);
                return 1;
            }
            break;

        case 'r':
            rx_depth = strtol(optarg, NULL, 0);
            break;

        case 'n':
            iters = strtol(optarg, NULL, 0);
            break;

        case 'l':
            sl = strtol(optarg, NULL, 0);
            break;

        case 'e':
            ++use_event;
            break;

        case 'g':
            gidx = strtol(optarg, NULL, 0);
            break;

        default:
            usage(argv[0]);
            return 1;
        }
    }

    page_size = sysconf(_SC_PAGESIZE);

    dev_list = ibv_get_device_list(NULL);
    if (!dev_list)
    {
        perror("Failed to get IB devices list");
        return 1;
    }

    if (!ib_devname)
    {
        ib_dev = *dev_list;
        if (!ib_dev)
        {
            fprintf(stderr, "No IB devices found\n");
            return 1;
        }
    }
    else
    {
        int i;
        for (i = 0; dev_list[i]; ++i)
            if (!strcmp(ibv_get_device_name(dev_list[i]), ib_devname))
                break;
        ib_dev = dev_list[i];
        if (!ib_dev)
        {
            fprintf(stderr, "IB device %s not found\n", ib_devname);
            return 1;
        }
    }

    ctx = pp_init_ctx(ib_dev, size, rx_depth, tx_depth, ib_port, use_event, !servername);
    if (!ctx)
        return 1;

    ctx->routs = pp_post_recv(ctx, ctx->rx_depth);
    if (ctx->routs < ctx->rx_depth)
    {
        fprintf(stderr, "Couldn't post receive (%d)\n", ctx->routs);
        return 1;
    }

    if (use_event)
        if (ibv_req_notify_cq(ctx->cq, 0))
        {
            fprintf(stderr, "Couldn't request CQ notification\n");
            return 1;
        }

    if (pp_get_port_info(ctx->context, ib_port, &ctx->portinfo))
    {
        fprintf(stderr, "Couldn't get port info\n");
        return 1;
    }

    my_dest.lid = ctx->portinfo.lid;
    if (ctx->portinfo.link_layer == IBV_LINK_LAYER_INFINIBAND && !my_dest.lid)
    {
        fprintf(stderr, "Couldn't get local LID\n");
        return 1;
    }

    if (gidx >= 0)
    {
        if (ibv_query_gid(ctx->context, ib_port, gidx, &my_dest.gid))
        {
            fprintf(stderr, "Could not get local gid for gid index %d\n", gidx);
            return 1;
        }
    }
    else
        memset(&my_dest.gid, 0, sizeof my_dest.gid);

    my_dest.qpn = ctx->qp->qp_num;
    my_dest.psn = lrand48() & 0xffffff;
    inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid);
    printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           my_dest.lid, my_dest.qpn, my_dest.psn, gid);

    if (servername)
        rem_dest = pp_client_exch_dest(servername, port, &my_dest);
    else
        rem_dest = pp_server_exch_dest(ctx, ib_port, mtu, port, sl, &my_dest, gidx);

    if (!rem_dest)
        return 1;

    inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
    printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           rem_dest->lid, rem_dest->qpn, rem_dest->psn, gid);

    if (servername)
        if (pp_connect_ctx(ctx, ib_port, my_dest.psn, mtu, sl, rem_dest, gidx))
            return 1;

    kv_handle->ctx = ctx;

    ibv_free_device_list(dev_list);
    free(rem_dest);
    return 0;
}
