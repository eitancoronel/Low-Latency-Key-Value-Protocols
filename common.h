#pragma once
#include <string>
#include <vector>
#include <map>
#include <infiniband/verbs.h>

using namespace std;

// Declaration of an external function to close a pingpong context
extern int pp_close_ctx(struct pingpong_context *ctx);

// Structure representing the context of a "pingpong" communication
struct pingpong_context {
    struct ibv_context *context;          // Verbs context, represents a device context
    struct ibv_comp_channel *channel;     // Completion event channel
    struct ibv_pd *pd;                    // Protection domain
    struct ibv_mr *mr;                    // Memory region for RDMA operations
    struct ibv_cq *cq;                    // Completion queue
    struct ibv_qp *qp;                    // Queue pair for send/receive operations
    void *buf;                            // Buffer for data transfer
    int size;                             // Size of the buffer
    int rx_depth;                         // Depth of the receive queue
    int routs;                            // Number of outstanding receive requests
    struct ibv_port_attr portinfo;        // Port attributes
};

// Typedef for convenience
typedef struct pingpong_context ppctx;

// Class representing a handle to the key-value store
class KV_handle {
public:
    // Constructor initializes command-line arguments
    KV_handle(int _argc, char **_argv) : argc(_argc), argv(_argv) {}

    int argc;        // Number of command-line arguments
    char **argv;     // Command-line arguments
    ppctx *ctx;      // Pointer to the pingpong context
    string servername; // Server name or address
};

// Class representing the buffer used for key-value operations
class kv_buf {
public:
    kv_buf() {
        init();  // Initialize the buffer
    }

    // Function to initialize or reset the buffer
    void init() {
        key_len = 0;             // Length of the key
        val_len = 0;             // Length of the value
        is_set = false;          // Flag indicating if the operation is a 'SET' (true) or 'GET' (false)

        rdv_addr = nullptr;      // Remote address for RDMA operations
        rdv_rkey = 0;            // Remote key for RDMA operations
        rdv_val_size = -1;       // Size of the value in RDMA operations

        is_eager = true;         // Flag indicating if the operation is in eager mode
        memset(kv, 0, 4096);     // Zero out the buffer
    }

    int key_len;       // Length of the key stored in 'kv'
    int val_len;       // Length of the value stored in 'kv' (after the key)
    bool is_set;       // True if the operation is 'SET', false for 'GET'

    void *rdv_addr;    // Address used in RDMA operations (rendezvous mode)
    uint32_t rdv_rkey; // Remote key used in RDMA operations
    int rdv_val_size;  // Size of the value to be transferred in RDMA operations

    bool is_eager;     // Indicates if the operation uses eager mode (true) or rendezvous mode (false)

    char kv[4096];     // Buffer to store the key and value
};

// Class representing the server's data and state
class server_data {
public:
    // Constructor initializes the number of clients and sets up the client RDMA memory regions
    server_data(int _n_clients) : n_clients(_n_clients), client_mr_rdv(_n_clients, nullptr) {}

    map<string, char *> kv_map;                           // Map to store key-value pairs
    map<string, std::pair<bool, int>> kv_locks;           // Map to manage locks on keys: <key, <is_locked, client_id>>
    int n_clients;                                        // Number of clients connected to the server
    vector<struct ibv_mr *> client_mr_rdv;                // Vector of memory regions for client RDMA operations
};
