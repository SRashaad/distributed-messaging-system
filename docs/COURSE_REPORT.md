# Fault-Tolerant Distributed Messaging System  
## Course Project Report

**Module:** Distributed Systems (prototype submission)  
**Repository:** `distributed-messaging-system`  
**Implementation language:** Go  
**Coordination:** Apache ZooKeeper  
**Inter-node / client transport:** gRPC (Protocol Buffers)

---

## Authors

| Name | Registration No. | Email | Primary contribution area |
| :--- | :--- | :--- | :--- |
| Imansa Bodini | IT24101844 | pedhuruarachchigepremarathne@gmail.com | Fault tolerance |
| Senul Mintharu | IT24101497 | senulmintharu@gmail.com | Data replication & consistency |
| Sabeelur Rashaad | IT24100146 | it24100146@my.sliit.lk | Time synchronization |
| Vimukthi Herath | IT24101500 | vimukthiherath123@gmail.com | Consensus & agreement |

---

## Abstract

This report describes a **fault-tolerant, leader-based distributed messaging prototype**. The system runs as a **cluster of peer nodes** that agree on a single **leader** using **Apache ZooKeeper** (ephemeral sequential znodes under a shared election path). Clients publish messages through **gRPC**; the leader appends to a **replicated log**, broadcasts **AppendEntries**-style RPCs to followers, and waits for a **quorum** before treating a write as committed. **Lamport logical timestamps** attach to messages so that consumption can present a **causally consistent ordering** independent of wall-clock skew. A small **HTTP dashboard** (Go bridge + static UI) polls node status and demonstrates **follower redirect** behaviour when publishes target a non-leader. We summarise architecture, responsibilities, verification approach, limitations, and future work.

**Keywords:** distributed systems, leader election, ZooKeeper, gRPC, replication, quorum, Lamport clocks, fault tolerance.

---

## 1. Introduction

### 1.1 Problem statement

Building a reliable messaging layer across multiple processes requires: (1) a **single writer** at a time for ordered appends, (2) **replication** so followers stay consistent, (3) **failure handling** when nodes or the leader disappear, and (4) a **clear ordering semantics** for readers when messages may arrive out of order at different replicas.

### 1.2 Objectives

- Run a **multi-node** cluster with automatic **leader election** backed by ZooKeeper.  
- Ensure writes are **replicated** and only acknowledged after **majority** confirmation where the implementation enforces quorum.  
- Provide **client access** via gRPC (CLI and optional dashboard bridge).  
- Apply **Lamport time** for logical ordering on read paths.  
- Demonstrate **failover** by stopping the leader and observing re-election.

### 1.3 Scope

The deliverable is a **research / coursework prototype**: it illustrates core mechanisms (election, append replication, quorum gate, logical ordering, basic recovery paths). It is **not** a production messaging product (security hardening, operational metrics, horizontal scaling to large clusters, and full persistence policies are out of scope unless explicitly extended).

---

## 2. Background and related ideas

- **Leader-based replication:** One node serialises writes; followers apply the same ordered log.  
- **Raft-style vocabulary:** Terms, log indices, AppendEntries, and leader redirect concepts inform the design even where **ZooKeeper** implements election.  
- **ZooKeeper:** Ephemeral nodes disappear when a session ends, enabling **crash detection** and **re-election** without a custom bully algorithm in application code.  
- **Lamport clocks:** A scalar clock updated on send/receive (here simplified via tick/update on events) yields a **partial order** useful for **deterministic consume ordering**.

---

## 3. System architecture

### 3.1 Process model

Each **node** is one OS process running `cmd/server`. Nodes listen on distinct TCP ports (e.g. 5001–5003) for gRPC. All nodes connect to **ZooKeeper** for election metadata.

### 3.2 Layered view

1. **Transport:** gRPC server/client, protobuf services (`ConsensusService`, `MessagingService`), handlers in `internal/transport/`.  
2. **Consensus / election:** `internal/consensus/` — ZooKeeper election loop, role (follower / leader transition), interaction with AppendEntries handling.  
3. **Replication:** `internal/replication/` — in-memory replicated log, manager broadcasting to peers, quorum tracking.  
4. **Fault tolerance:** `internal/fault/` — heartbeat / detection / recovery hooks supporting survival scenarios.  
5. **Time sync:** `internal/timesync/` — Lamport clock on publish and updates when entries arrive at followers.  
6. **Storage:** `internal/storage/` — message store applied from committed/replicated entries; optional file-backed helpers exist in the codebase for extended experiments.  
7. **Orchestration:** `internal/node/` wires modules and implements publish/consume behaviour at the node boundary.  
8. **Dashboard (optional):** `cmd/dashboard` — HTTP JSON API and static SPA; translates REST to gRPC (`GetStatus`, `Publish`, `Consume`).

### 3.3 Data flow (publish)

1. Client sends **Publish** to a node (leader or follower).  
2. If the node is **not** the leader, the implementation signals that the client must use the **leader** (CLI and dashboard implement retry / redirect patterns).  
3. On the leader: Lamport **tick**, assign log index/term, **replicate** to followers, **wait for quorum**, then apply to the local **message store** and return success.

### 3.4 Data flow (consume)

**Consume** reads from the node’s message store and returns entries sorted by **(Lamport timestamp, log index)** as a deterministic tie-break, so clients see a stable causal ordering.

---

## 4. Module responsibilities (team alignment)

| Area | Main packages / files | Responsibility |
| :--- | :--- | :--- |
| Fault tolerance | `internal/fault/` | Failure detection, heartbeat-related behaviour, recovery / log catch-up paths. |
| Replication | `internal/replication/` | Append-only log, parallel AppendEntries to peers, quorum wait before commit semantics at the node layer. |
| Time synchronization | `internal/timesync/` | Lamport clock; follower clock update when receiving replicated entries. |
| Consensus & agreement | `internal/consensus/` | ZooKeeper election integration, leader/follower state, handling of consensus RPCs. |
| Transport & API | `internal/transport/`, `internal/transport/proto/` | gRPC services, protobuf definitions, `GetStatus` for observability. |

Per-member deep dives are in `docs/member1_fault_tolerance.md` through `docs/member4_consensus.md`.

---

## 5. Implementation highlights

- **ZooKeeper election:** Nodes create ephemeral sequential znodes; the ordering determines leadership; watchers trigger re-evaluation on failure.  
- **Quorum-gated publish:** Replication manager collects acknowledgements; publish does not complete successfully if quorum is not reached within the configured wait.  
- **Deduplication (leader path):** Before expensive replication, the leader can detect duplicate payloads already stored and short-circuit.  
- **CLI client:** `cmd/client` can follow **not the leader** style errors and redial, illustrating **client-side failover** with fixed peer alternates in the demo client.  
- **Dashboard:** Presents **node id**, **gRPC dial address**, **role** (follower / **Electing…** for candidate / leader), term, log length, and a **live log table**; **publish** can show a trace when redirecting to the leader.

---

## 6. Testing and verification

- **Unit tests:** `go test ./...` exercises consensus and related packages (see `internal/consensus/*_test.go` and others).  
- **Integration tests:** `test/integration/cluster_test.go` validates cluster behaviour when a full environment is available.  
- **Manual end-to-end:** Documented in **`docs/running_the_cluster_guide.md`**: start ZK, three nodes, kill leader, observe promotion, publish/consume via CLI.  
- **Dashboard:** Manual verification of status cards, log ordering, and publish traces against a live cluster.

No large-scale performance benchmark is claimed; latency and throughput depend on hardware, ZK, and Go gRPC defaults.

---

## 7. Limitations and future work

- **Persistence:** The primary demonstration path uses an **in-memory** replicated log and store; durable crash recovery across full disk loss would require wiring **persistent log/state** as the default bootstrap path (file or DB) and careful replay ordering.  
- **Security:** gRPC uses **insecure** credentials suitable for localhost demos only.  
- **Scalability:** Tested at small **N** (3–5); very large clusters would need connection pooling, backpressure, and operational tooling.  
- **Observability:** Structured metrics (Prometheus), tracing, and centralised logging are not included.  
- **Election vs pure Raft:** Election is **ZooKeeper-mediated**; this is a deliberate engineering trade-off for the coursework prototype.

---

## 8. Conclusion

The team implemented a **coherent distributed messaging prototype** that combines **ZooKeeper leader election**, **quorum-replicated appends over gRPC**, **Lamport-based ordering on reads**, and **fault-tolerance hooks** suitable for demonstration and assessment. The repository includes **run guides**, **module documentation**, a **submission README**, and an optional **dashboard** to make behaviour visible to evaluators.

---

## 9. References (indicative)

1. Lamport, L. “Time, Clocks, and the Ordering of Events in a Distributed System.” *Communications of the ACM*, 1978.  
2. Ongaro, D., Ousterhout, J. “In Search of an Understandable Consensus Algorithm (Raft).” *USENIX ATC*, 2014.  
3. Apache ZooKeeper documentation — *Overview*, *Recipes*, *Ephemeral Nodes*.  
4. gRPC Authors. *gRPC Go Quick Start* and *Protocol Buffers Language Guide*.  

---

## Appendix A — How to run (summary)

See the root **`README.md`** and **`docs/running_the_cluster_guide.md`** for exact commands (ZooKeeper, `go run ./cmd/server`, optional `scripts/start-cluster.ps1`, dashboard on port **8090** if **8080** is blocked on Windows, CLI client).

---

## Appendix B — Repository map (high level)

- `cmd/server` — node entrypoint  
- `cmd/client` — CLI  
- `cmd/dashboard` — HTTP bridge + embedded UI  
- `internal/node` — composition root  
- `internal/consensus`, `internal/replication`, `internal/fault`, `internal/timesync`, `internal/storage`, `internal/transport` — subsystems  
- `docs/` — evaluator guide, member reports, this document  

---

*End of report. Export this file to PDF for submission if your module requires PDF format.*
