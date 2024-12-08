### README.md

---
If you have any questions about the project feel free to contact me: https://www.linkedin.com/in/eitan-coronel/

# Low-Latency Key-Value Store

## Overview
This project implements a high-performance key-value store to explore and compare two protocols for data transfer: 
- **Eager protocol**: Optimized for small payloads with immediate data transfer.  
- **Rendezvous protocol**: Designed for large payloads, leveraging RDMA to achieve zero-copy data transfer.

The goal is to evaluate the trade-offs between these two approaches in terms of latency and throughput, highlighting their respective strengths and weaknesses.

---

## What We Accomplished
We developed a client-server application using C and the Verbs API, capable of:
1. Efficiently handling `GET` and `SET` operations for key-value pairs.
2. Automatically selecting the appropriate protocol based on payload size:
   - **Eager** for small payloads (low overhead).
   - **Rendezvous** for large payloads (zero-copy efficiency).
3. Supporting multiple concurrent clients interacting with a single server (using the locks mechanism).

---

## Results
The comparison revealed:
- **Eager protocol** excels for small payloads (<4KB) due to its minimal overhead.  
- **Rendezvous protocol** significantly outperforms Eager for large payloads (≥4KB) by avoiding data copying and leveraging RDMA for direct memory transfers.

These findings demonstrate the importance of protocol selection in optimizing performance for varying data sizes.

---

## How to Run
1. Compile the project using the provided `comp.sh` file.
2. Run the server:
   ```bash
   ./key_value_store server
   ```
3. Run clients and perform operations using input files or the test1 file provided:
   ```bash
   ./key_value_store client <server_name> <input_file>
   ```
   Example input file:
   ```
   SET key1 value1
   GET key1
   ```

---
