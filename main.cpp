#include <iostream>
#include "bw_template.h"
#include <fstream>
#include <ranges>
#include <string_view>
#include <sstream>
#include <chrono>

int NB_OF_CLIENTS = 2;  // Number of clients that the server will handle

// Function declarations for the key-value store API
int kv_open(char *servername, void **kv_handle);
int kv_set(void *kv_handle, const char *key, const char *value);
int kv_get(void *kv_handle, const char *key, char **value);
void kv_release(char *value);
int kv_close(void *kv_handle);

// Function to receive data in the key-value handle
void kv_recv(KV_handle &kv_handle) {
    pp_post_recv(kv_handle.ctx, 1);          // Post a receive request
    pp_wait_completions(kv_handle.ctx, 1);   // Wait for the completion of the receive
}

// Function to send a request using the key-value handle
int post_request(KV_handle &kv_handle) {
    if (pp_post_send(kv_handle.ctx)) {  // Post a send request
        std::cout << "pp_post_send failed" << std::endl;
        return -1;
    }
    pp_wait_completions(kv_handle.ctx, 1);  // Wait for the send completion
    return 0;
}

// Check if a key is locked by another client
bool is_locked(server_data &s_handle, std::string &key, int i = -1) {
    if (s_handle.kv_locks[key].first) {
        if (i == -1) {
            return true;  // Key is locked and no client index provided
        }
        if (s_handle.kv_locks[key].second != i) {
            return true;  // Key is locked by another client
        }
    }
    return false;  // Key is not locked
}

// Attempt to acquire a lock on a key for a client
bool get_lock(server_data &s_handle, std::string &key, int i) {
    if (is_locked(s_handle, key, i)) {
        return false;  // Lock acquisition failed
    }
    s_handle.kv_locks[key] = std::make_pair(true, i);  // Lock the key for client 'i'
    std::cout << i << " got lock on " << key << std::endl;
    return true;  // Lock acquired successfully
}

// Release the lock on a key for a client
void unlock(server_data &s_handle, std::string &key, int i) {
    s_handle.kv_locks[key] = std::make_pair(false, -1);  // Unlock the key
    std::cout << i << " freed lock on " << key << std::endl;
}

// Server-side function to handle 'GET' requests
int server_perform_get(server_data &s_handle, KV_handle &kv_handle, int i) {
    kv_buf *buf = ((kv_buf *)(kv_handle.ctx->buf));  // Buffer containing the request
    ppctx *ctx = kv_handle.ctx;
    std::string key(buf->kv, buf->key_len);  // Extract the key from the buffer

    std::cout << "Enter server_perform_get with key: " << key << std::endl;

    // Attempt to get a lock on the key
    if (!get_lock(s_handle, key, i)) {
        std::cout << i << " Couldn't get lock, will retry..." << std::endl;
        return 1;  // Indicate that the lock was not acquired
    }

    // Check if the key exists in the key-value map
    if (!s_handle.kv_map.contains(key) || !s_handle.kv_map[key]) {
        std::cout << "Key not found" << std::endl;
        buf->val_len = 0;  // Indicate that the value length is zero
        unlock(s_handle, key, i);  // Release the lock
        return post_request(kv_handle);  // Send a response back to the client
    }

    int buf_size = buf->key_len + strlen(s_handle.kv_map[key]);
    if (buf_size < 4096) {
        // Eager mode: value is small enough to send immediately
        std::cout << "Eager mode of server_perform_get, val: " << s_handle.kv_map[key] << std::endl;
        strncpy(buf->kv + buf->key_len, s_handle.kv_map[key], strlen(s_handle.kv_map[key]));
        buf->val_len = strlen(s_handle.kv_map[key]);
        unlock(s_handle, key, i);  // Release the lock
        return post_request(kv_handle);  // Send the value back to the client
    }

    // Rendezvous mode: value is too large, use RDMA operations
    buf->is_eager = false;
    if (buf->rdv_val_size == -1) {
        // Set the size of the value to be transferred
        std::cout << "Setting rdv_val_size: " << strlen(s_handle.kv_map[key]) << std::endl;
        buf->rdv_val_size = strlen(s_handle.kv_map[key]);

        // Parallelize communications and memory allocations
        if (post_request(kv_handle)) {
            return -1;  // Error in sending the response
        }

        // Register memory for RDMA transfer
        s_handle.client_mr_rdv[i] = ibv_reg_mr(
            ctx->pd,
            (void *)s_handle.kv_map[key],
            strlen(s_handle.kv_map[key]),
            IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE
        );
        return 0;
    } else {
        // Perform RDMA write operation to send the value
        struct ibv_mr *prev_mr = ctx->mr;
        int prev_size = ctx->size;
        ctx->mr = s_handle.client_mr_rdv[i];
        ctx->size = strlen(s_handle.kv_map[key]);

        std::cout << "Performing RDMA_WRITE" << std::endl;
        if (!buf->rdv_addr || !buf->rdv_rkey) {
            std::cout << "Invalid RDV info from buf" << std::endl;
            return -1;
        }

        // Post the RDMA write request
        pp_post_send(ctx, s_handle.kv_map[key], buf->rdv_addr, buf->rdv_rkey, IBV_WR_RDMA_WRITE);
        pp_wait_completions(ctx, 1);  // Wait for the RDMA write to complete

        // Clean up and restore previous memory region
        ibv_dereg_mr(ctx->mr);
        ctx->mr = prev_mr;
        ctx->size = prev_size;
        s_handle.client_mr_rdv[i] = nullptr;
        unlock(s_handle, key, i);  // Release the lock

        return post_request(kv_handle);  // Send a response back to the client
    }

    unlock(s_handle, key, i);  // Release the lock (redundant here)
    return 0;
}

// Server-side function to handle 'SET' requests
int server_perform_set(server_data &s_handle, KV_handle &kv_handle, int i) {
    kv_buf *buf = ((kv_buf *)(kv_handle.ctx->buf));  // Buffer containing the request
    ppctx *ctx = kv_handle.ctx;
    std::string key(buf->kv, buf->key_len);  // Extract the key from the buffer

    // Check if the key is locked
    if (is_locked(s_handle, key)) {
        std::cout << i << " couldn't get lock, will retry..." << std::endl;
        return 1;  // Indicate that the lock was not acquired
    }

    // Delete existing value if it exists
    if (s_handle.kv_map.contains(key) && s_handle.kv_map[key]) {
        delete s_handle.kv_map[key];
        s_handle.kv_map[key] = nullptr;
    }

    if (buf->is_eager) {
        // Eager mode: value is small enough to receive immediately
        std::cout << "Eager mode of server_perform_set" << std::endl;
        s_handle.kv_map[key] = new char[buf->val_len + 1];
        strncpy(s_handle.kv_map[key], buf->kv + buf->key_len, buf->val_len);
        s_handle.kv_map[key][buf->val_len] = '\0';  // Null-terminate the value

        std::cout << "Set key: " << key << " val: " << s_handle.kv_map[key] << std::endl;
        return post_request(kv_handle);  // Send a response back to the client
    } else {
        // Rendezvous mode: value is too large, use RDMA operations
        std::cout << "RDV mode of server_perform_set" << std::endl;
        int val_size = buf->rdv_val_size;
        std::cout << "Got val size: " << val_size << std::endl;
        s_handle.kv_map[key] = new char[val_size + 1];
        s_handle.kv_map[key][val_size] = '\0';  // Null-terminate the value

        // Register memory for RDMA transfer
        struct ibv_mr *rdv_mr = ibv_reg_mr(
            ctx->pd,
            s_handle.kv_map[key],
            val_size,
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE
        );
        buf->rdv_addr = rdv_mr->addr;
        buf->rdv_rkey = rdv_mr->rkey;

        if (post_request(kv_handle)) {
            return -1;  // Error in sending the response
        }

        kv_recv(kv_handle);  // Receive the data via RDMA write
    }

    return 0;
}

// Main server function that handles multiple clients
void main_server(KV_handle &args) {
    server_data s_handle(NB_OF_CLIENTS);  // Server data structure
    std::vector<KV_handle> clients;       // Vector to hold client handles

    // Initialize connections with all clients
    for (int i = 0; i < s_handle.n_clients; i++) {
        clients.emplace_back(args.argc, args.argv);

        if (kv_open(nullptr, (void **)&clients[i])) {
            std::cout << "kv_open failed" << std::endl;
            return;
        }

        std::cout << "Connected to client #" << i << std::endl;
    }

    // Post initial receive requests for all clients
    for (KV_handle &client : clients) {
        pp_post_recv(client.ctx, 1);
    }

    bool retries[NB_OF_CLIENTS] = {false};  // Track clients that need to retry operations
    while (true) {
        int i = 0;
        for (KV_handle &client : clients) {
            int ne = 0;
            if (!retries[i]) {
                struct ibv_wc wc[1];
                ne = ibv_poll_cq(client.ctx->cq, 1, wc);  // Poll the completion queue

                if (ne < 0) {
                    std::cerr << "poll CQ failed: " << ne << std::endl;
                    return;
                }
            } else {
                ne = 1;  // Client is retrying a previous operation
                std::cout << i << " is now retrying" << std::endl;
            }

            if (ne) {
                kv_buf *buf = (kv_buf *)(client.ctx->buf);  // Get the buffer
                if (buf->is_set) {
                    // Handle 'SET' request
                    std::cout << "Got a SET from " << i << std::endl;
                    int res = server_perform_set(s_handle, client, i);
                    if (res) {
                        if (res == 1) {
                            retries[i] = true;  // Retry needed
                            i++;
                            continue;
                        } else {
                            std::cerr << "server_perform_set failed" << std::endl;
                            return;
                        }
                    }
                } else {
                    // Handle 'GET' request
                    std::cout << "Got a GET from " << i << std::endl;
                    int res = server_perform_get(s_handle, client, i);
                    if (res) {
                        if (res == 1) {
                            std::cout << "Couldn't get lock, will retry later" << std::endl;
                            retries[i] = true;  // Retry needed
                            i++;
                            continue;
                        } else {
                            std::cerr << "server_perform_get failed" << std::endl;
                            return;
                        }
                    }
                }
                pp_post_recv(client.ctx, 1);  // Post another receive request
                retries[i] = false;            // Reset retry flag
            }
            i++;
        }
    }
}

// Client-side function to open a connection to the server
int kv_open(char *servername, void **kv_handle) {
    KV_handle *handle = (KV_handle *)kv_handle;
    return main_ex(servername, handle);  // Initialize the connection
}

// Client-side function to set a key-value pair
int kv_set(void *kv_handle, const char *key, const char *value) {
    KV_handle &handle = *(KV_handle *)kv_handle;
    ppctx *ctx = handle.ctx;
    kv_buf *buf = (kv_buf *)handle.ctx->buf;
    buf->init();  // Initialize the buffer
    buf->is_set = true;  // Indicate a 'SET' operation

    strncpy(buf->kv, key, strlen(key));
    buf->key_len = strlen(key);

    if (strlen(key) + strlen(value) < 4096) {
        // Eager mode: value is small enough to send immediately
        buf->is_eager = true;
        strncpy(buf->kv + strlen(key), value, strlen(value));
        buf->val_len = strlen(value);

        if (post_request(handle)) {
            return -1;  // Error in sending the request
        }
        kv_recv(handle);  // Wait for the server's response
    } else {
        // Rendezvous mode: value is too large, use RDMA operations
        buf->is_eager = false;
        buf->rdv_val_size = strlen(value);

        if (post_request(handle)) {
            return -1;  // Error in sending the request
        }

        // Parallelize communications and memory allocations
        struct ibv_mr *new_mr = ibv_reg_mr(
            ctx->pd,
            (void *)value,
            strlen(value),
            IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE
        );
        kv_recv(handle);  // Wait for the server to prepare for RDMA

        int prev_size = ctx->size;
        ctx->size = strlen(value);
        struct ibv_mr *prev_mr = ctx->mr;
        ctx->mr = new_mr;

        // Perform RDMA write to send the value to the server
        pp_post_send(ctx, value, buf->rdv_addr, buf->rdv_rkey, IBV_WR_RDMA_WRITE);
        pp_wait_completions(ctx, 1);  // Wait for the RDMA write to complete

        ctx->size = prev_size;
        ibv_dereg_mr(ctx->mr);  // Deregister the memory region
        ctx->mr = prev_mr;

        if (post_request(handle)) {
            return -1;  // Error in sending the final request
        }
    }

    return 0;  // 'SET' operation completed successfully
}

// Client-side function to get the value of a key
int kv_get(void *kv_handle, const char *key, char **value) {
    KV_handle &handle = *(KV_handle *)kv_handle;
    ppctx *ctx = handle.ctx;
    kv_buf *buf = (kv_buf *)handle.ctx->buf;
    buf->init();  // Initialize the buffer

    buf->is_set = false;  // Indicate a 'GET' operation
    strncpy(buf->kv, key, strlen(key));
    buf->key_len = strlen(key);

    if (post_request(handle)) {
        return -1;  // Error in sending the request
    }
    kv_recv(handle);  // Wait for the server's response

    if (buf->is_eager) {
        // Eager mode: value is small enough to receive immediately
        *value = new char[buf->val_len + 1];
        strncpy(*value, buf->kv + buf->key_len, buf->val_len);
        (*value)[buf->val_len] = '\0';  // Null-terminate the value
    } else {
        // Rendezvous mode: value is too large, use RDMA operations
        int val_size = buf->rdv_val_size;
        *value = new char[val_size + 1];
        (*value)[val_size] = '\0';  // Null-terminate the value

        struct ibv_mr *new_mr = ibv_reg_mr(
            ctx->pd,
            *value,
            val_size,
            IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE
        );
        buf->rdv_addr = new_mr->addr;
        buf->rdv_rkey = new_mr->rkey;
        post_request(handle);  // Send the RDMA info to the server

        kv_recv(handle);  // Wait for the RDMA write to complete

        ibv_dereg_mr(new_mr);  // Deregister the memory region
    }

    return 0;  // 'GET' operation completed successfully
}

// Function to release the memory allocated for a value
void kv_release(char *value) {
    if (value) {
        free(value);  // Free the allocated memory
    }
}

// Client-side function to close the connection to the server
int kv_close(void *kv_handle) {
    return pp_close_ctx(((KV_handle *)kv_handle)->ctx);  // Close the context
}

// Function to calculate throughput
double calculate_throughput(size_t data_size_bytes, const std::chrono::high_resolution_clock::time_point& start_time, const std::chrono::high_resolution_clock::time_point& end_time) {
    // Calculate the elapsed time in seconds
    std::chrono::duration<double> elapsed_time = end_time - start_time;
    double elapsed_seconds = elapsed_time.count();

    // Calculate throughput in bytes per second
    double throughput_bps = data_size_bytes / elapsed_seconds;

    // Convert to megabytes per second (MB/s)
    double throughput_mbps = throughput_bps / (1024 * 1024);

    return throughput_mbps;
}

// Function to perform a sanity test of the key-value store
void sanity_test(KV_handle &kv_handle) {
    while (1) {
        std::string youpi_key = "test1";
        std::string youpi_val = "val1";
        char *value = nullptr;

        // Test 'SET' and 'GET' operations
        kv_set(&kv_handle, youpi_key.c_str(), youpi_val.c_str());
        kv_get(&kv_handle, youpi_key.c_str(), &value);
        std::cout << value << std::endl;
        kv_release(value);

        youpi_key = "test2";
        kv_set(&kv_handle, youpi_key.c_str(), youpi_val.c_str());
        kv_get(&kv_handle, youpi_key.c_str(), &value);
        std::cout << value << std::endl;
        kv_release(value);

        kv_get(&kv_handle, youpi_key.c_str(), &value);
        std::cout << value << std::endl;
        kv_release(value);

        youpi_key = "test1";
        youpi_val = "val2";
        kv_set(&kv_handle, youpi_key.c_str(), youpi_val.c_str());
        kv_get(&kv_handle, youpi_key.c_str(), &value);
        std::cout << value << std::endl;
        kv_release(value);

        // Test with a long value
        youpi_key = "test3";
        const int MAX_LENGTH = 4096;
        char long_value[MAX_LENGTH + 1] = {0};
        for (int i = 0; i < MAX_LENGTH; ++i) {
            long_value[i] = 'a';
        }
        long_value[MAX_LENGTH] = '\0';
        std::cout << "Length of long_value " << strlen(long_value) << std::endl;

        kv_set(&kv_handle, youpi_key.c_str(), long_value);
        kv_get(&kv_handle, youpi_key.c_str(), &value);

        std::cout << value << std::endl;
        std::cout << "Value length " << strlen(value) << std::endl;
        kv_release(value);
    }
    kv_close(&kv_handle);
}



// Main function
int main(int argc, char *argv[]) {
    char *servername = nullptr;
    char *inputname = nullptr;
    KV_handle kv_handle(argc, argv);
    if (optind == argc - 1) {
        // Test mode with server name provided
        std::cout << "Test mode" << std::endl;
        servername = strdup(argv[optind]);
    } else if (optind == argc - 2) {
        // Input mode with server name and input file provided
        std::cout << "Input mode" << std::endl;
        servername = strdup(argv[optind]);
        inputname = strdup(argv[optind + 1]);
    } else if (optind < argc) {
        usage(argv[0]);  // Display usage information
        return 1;
    }

    if (servername) {
        kv_handle.servername = std::string(servername);
        if (kv_open(servername, (void **)&kv_handle)) {
            std::cout << "kv_open failed" << std::endl;
            return -1;
        }
        if (!inputname) {
            // No input file provided, run the sanity test
            sanity_test(kv_handle);
            return 0;
        }

        // Process commands from the input file
        std::string line;
        std::ifstream inputFile(inputname);
        if (inputFile.is_open()) {
            while (std::getline(inputFile, line)) {
                // File format:
                // SET <key> <value>
                // GET <key>
                // see example in the test file

                // Split the line into words
                auto split = line
                    | std::ranges::views::split(' ')
                    | std::ranges::views::transform([](auto &&str) {
                        return std::string_view(&*str.begin(), std::ranges::distance(str));
                    });

                std::vector<std::string> args;
                for (auto &&word : split) {
                    args.push_back(std::string(word));
                }

                if (args.size() == 2) {
                    // 'GET' operation
                    std::cout << "GET " << args[1] << std::endl;
                    char *value;
                    kv_get(&kv_handle, args[1].c_str(), &value);
                    std::cout << "--------- " << value << "-------" << std::endl;
                    kv_release(value);
                }
                if (args.size() == 3) {
                    // 'SET' operation
                    std::cout << "SET " << args[1] << " " << args[2] << std::endl;
                    kv_set(&kv_handle, args[1].c_str(), args[2].c_str());
                }
            }
            inputFile.close();
        }
    } else {
        // No server name provided, run the server
        main_server(kv_handle);
    }

    return 0;
}
