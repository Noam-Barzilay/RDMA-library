# RDMA Rendezvous Pipelined Reduce-Scatter

This project implements a **pipelined reduce-scatter collective operation** using **RDMA Verbs** in C, supporting 2–4 processes in a **ring topology**.

The implementation supports:
- Both **eager** and **rendezvous** protocols
- **Pipelining** of large messages
- **Zero-copy** data transfers via RDMA Read/Write
- Custom memory registration and peer exchange of metadata (address, rkey)
- Basic correctness validation
