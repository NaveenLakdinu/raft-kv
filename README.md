# ⚡ Distributed Fault-Tolerant Key-Value Store (Raft Consensus in Go)

A production-grade distributed system implementing the **Raft Consensus Algorithm** from scratch in **Go**, featuring an **In-Memory Key-Value State Machine**, **Disk Persistence (WAL)**, and a **Real-Time Interactive Web Dashboard**.

This project demonstrates strong consistency, distributed consensus, quorum commits, automatic leader failover, and crash-recovery under network partition simulations.

---

## 🏛️ System Architecture

```text
                           +----------------------------+
                           |  Client / Web Dashboard    |
                           |   (HTTP on :8080 Console)  |
                           +--------------+-------------+
                                          |
                                          | Propose Command
                                          v
+-----------------------------------------------------------------------------------+
| 3-Node Raft Consensus Cluster                                                    |
|                                                                                   |
|   +---------------------+   RPC Heartbeat / Log Append   +---------------------+  |
|   |   Node 0 (Peer)     |<==============================>|   Node 1 (Leader)   |  |
|   |  127.0.0.1:8001     |                                |  127.0.0.1:8002     |  |
|   +----------+----------+                                +----------+----------+  |
|              ^                                                      |             |
|              |                RPC Heartbeat / Log Append            |             |
|              +======================+===============================+             |
|                                     |                                             |
|                                     v                                             |
|                          +---------------------+                                  |
|                          |   Node 2 (Peer)     |                                  |
|                          |  127.0.0.1:8003     |                                  |
|                          +----------+----------+                                  |
+-------------------------------------+---------------------------------------------+
                                      |
                     applyCh (Committed Entries Pipeline)
                                      v
                        +----------------------------+
                        |  State Machine (KV Store)  |
                        |   - SET key=value          |
                        |   - GET key                |
                        |   - DEL key                |
                        +--------------+-------------+
                                       |
                          Persistent Disk Storage
                                       v
                           [ data/node_*.raft ]