# Starter Guide — Fault-Tolerant Distributed Messaging System

> A step-by-step guide for all team members to set up, understand, and begin working on the project.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Repository Setup](#2-repository-setup)
3. [Project Structure Overview](#3-project-structure-overview)
4. [Team Member Assignments](#4-team-member-assignments)
5. [Understanding the Architecture](#5-understanding-the-architecture)
6. [How Modules Connect](#6-how-modules-connect)
7. [Development Workflow](#7-development-workflow)
8. [Building and Running](#8-building-and-running)
9. [Testing Your Module](#9-testing-your-module)
10. [Common Distributed Systems Terminology](#10-common-distributed-systems-terminology)
11. [Frequently Asked Questions](#11-frequently-asked-questions)

---

## 1. Prerequisites

Before starting, make sure you have the following installed:

| Tool                   | Version | Purpose                              |
| :--------------------- | :------ | :----------------------------------- |
| **Go**                 | 1.21+   | Programming language                 |
| **Git**                | Latest  | Version control                      |
| **protoc**             | 3.x     | Protocol Buffers compiler (for gRPC) |
| **protoc-gen-go**      | Latest  | Go code generator for protobuf       |
| **protoc-gen-go-grpc** | Latest  | gRPC code generator for Go           |
| **Make**               | Any     | Build automation (optional)          |

### Install Go

Download from [https://go.dev/dl/](https://go.dev/dl/) and follow the installation instructions. Verify:

```bash
go version
# Should output: go version go1.21.x or higher
```

### Install protoc (Protocol Buffers compiler)

Download from [https://github.com/protocolbuffers/protobuf/releases](https://github.com/protocolbuffers/protobuf/releases). Then install the Go plugins:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

---

## 2. Repository Setup

```bash
# Clone the repository
git clone <your-repo-url>
cd distributed-messaging-system

# Install Go dependencies
go mod tidy

# Verify everything compiles
go build ./...

# Create your feature branch
git checkout develop
git pull origin develop
git checkout -b feature/<your-module>
# Example: git checkout -b feature/consensus
```

---

## 3. Project Structure Overview

```
distributed-messaging-system/
├── cmd/                        ← Entry points (server + client binaries)
│   ├── server/main.go          ← Starts a cluster node
│   └── client/main.go          ← CLI client for publish/consume
│
├── config/config.go            ← Configuration (timeouts, ports, peers)
│
├── internal/                   ← Core modules (private to this project)
│   ├── consensus/              ← [Vimukthi] Raft leader election & agreement
│   │   ├── raft.go             ← Core consensus logic
│   │   ├── election.go         ← Election timeouts & vote tracking
│   │   └── state.go            ← Data structures (RPC types, LogEntry)
│   │
│   ├── replication/            ← [Senul] Log replication & quorum commit
│   │   ├── log.go              ← Replicated log (append-only)
│   │   ├── manager.go          ← Replication orchestrator
│   │   └── quorum.go           ← Quorum (majority) tracker
│   │
│   ├── fault/                  ← [Imansa] Failure detection & recovery
│   │   ├── detector.go         ← Heartbeat-based failure detector
│   │   ├── heartbeat.go        ← Leader heartbeat emitter
│   │   └── recovery.go         ← Node recovery & log sync
│   │
│   ├── timesync/               ← [Sabeelur] Lamport clocks & event ordering
│   │   ├── lamport.go          ← Lamport logical clock
│   │   ├── event.go            ← Event type definitions
│   │   └── orderer.go          ← Event ordering logic
│   │
│   ├── storage/                ← [Sabeelur] Message storage
│   │   ├── store.go            ← Committed message store
│   │   ├── log_store.go        ← Persistent log storage (optional)
│   │   └── snapshot.go         ← Log compaction snapshots (optional)
│   │
│   ├── transport/              ← [Shared] gRPC network layer
│   │   ├── grpc_server.go      ← Incoming RPC server
│   │   ├── grpc_client.go      ← Outgoing RPC client
│   │   └── proto/
│   │       └── messaging.proto ← Protocol Buffers definitions
│   │
│   └── node/                   ← [Shared] Module integration
│       └── node.go             ← Wires all modules together
│
├── pkg/logger/logger.go        ← [Shared] Structured logging
├── scripts/                    ← Cluster management scripts
├── test/integration/           ← End-to-end tests
├── docs/                       ← Documentation (you are here!)
├── go.mod                      ← Go module definition
└── Makefile                    ← Build targets
```

---

## 4. Team Member Assignments

| Member               | Module                         | Files                                       | Branch                    |
| :------------------- | :----------------------------- | :------------------------------------------ | :------------------------ |
| **Vimukthi Herath**  | Consensus & Agreement          | `internal/consensus/*`                      | `feature/consensus`       |
| **Senul Mintharu**   | Data Replication & Consistency | `internal/replication/*`                    | `feature/replication`     |
| **Imansa Bodini**    | Fault Tolerance                | `internal/fault/*`                          | `feature/fault-tolerance` |
| **Sabeelur Rashaad** | Time Synchronization & Storage | `internal/timesync/*`, `internal/storage/*` | `feature/time-sync`       |

**Shared responsibility (all members):**

- `cmd/` — Entry points
- `config/` — Configuration
- `internal/transport/` — gRPC layer
- `internal/node/` — Module integration
- `pkg/logger/` — Logging
- `test/integration/` — Integration tests

---

## 5. Understanding the Architecture

### The Big Picture

```
Client → Leader Node → Replicate to Followers → Quorum ACK → Commit → Respond to Client
```

1. **Client** sends a message to the **Leader** via gRPC.
2. **Leader** appends the message to its log with a **Lamport timestamp**.
3. **Leader** sends the entry to all **Followers** via `AppendEntries` RPC.
4. Each **Follower** appends the entry and ACKs back to the Leader.
5. Once a **quorum (majority)** ACKs, the Leader **commits** the entry.
6. The **committed entry** is applied to the message store.
7. **Client** receives a success response.

### Node States (Raft)

Every node is always in one of three states:

```
   election timeout          receives majority votes
┌──────────────────┐     ┌───────────────────────────┐
│                  ▼     │                            │
│  ┌──────────┐    ┌─────┴─────┐    ┌──────────┐     │
│  │ Follower │───►│ Candidate │───►│  Leader  │     │
│  └──────────┘    └───────────┘    └──────────┘     │
│       ▲                                │           │
│       └────────────────────────────────┘           │
│              discovers higher term                  │
└─────────────────────────────────────────────────────┘
```

- **Follower**: Default state. Listens for heartbeats from Leader.
- **Candidate**: Triggered by election timeout. Requests votes.
- **Leader**: Won election. Handles all writes, sends heartbeats.

---

## 6. How Modules Connect

```
                ┌─────────────────┐
                │    Consensus    │ ← Vimukthi
                │    (raft.go)    │
                └────────┬────────┘
                         │ uses
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
   ┌────────────┐ ┌───────────┐ ┌────────────┐
   │ Replication│ │  TimesyncTimesync │ │   Fault    │
   │  (Senul)   │ │ (Sabeelur)│ │ (Imansa)   │
   └────────────┘ └───────────┘ └────────────┘
          │              │              │
          └──────────────┼──────────────┘
                         ▼
                  ┌─────────────┐
                  │  Transport  │ ← Shared
                  │   (gRPC)    │
                  └─────────────┘
```

### Key Integration Points

| From → To               | How They Connect                                |
| :---------------------- | :---------------------------------------------- |
| Consensus → Replication | Leader calls `ReplicateEntry()` after appending |
| Consensus → Fault       | Election triggered by `OnFailure()` callback    |
| Consensus → Timesync    | `Tick()` on each state transition               |
| Replication → Timesync  | `Tick()` on each log append                     |
| Fault → Consensus       | Heartbeat timeout triggers `StartElection()`    |
| Timesync → Replication  | Lamport timestamp in every `LogEntry`           |
| Transport → All         | gRPC handles all inter-node communication       |

### Communication Rule

**Modules communicate ONLY through interfaces — never by accessing internal state.**

This means you can develop and test your module independently using mock implementations of other modules' interfaces.

---

## 7. Development Workflow

### Step 1: Read Your Module's Documentation

Each member has a dedicated documentation file in `docs/`:

- `docs/member1_fault_tolerance.md` — Imansa
- `docs/member2_replication.md` — Senul
- `docs/member3_timesync.md` — Sabeelur
- `docs/member4_consensus.md` — Vimukthi

### Step 2: Read the Skeleton Code

Open your module's files and read every comment carefully. The comments explain:

- What each struct represents
- What each function should do
- How it connects to other modules

### Step 3: Implement One Function at a Time

Start with the simplest function and work your way up:

1. Implement the constructor (`New...()` function)
2. Implement simple getters
3. Implement core logic
4. Write unit tests for each function

### Step 4: Write Unit Tests

Create test files alongside your implementation:

```
internal/consensus/raft_test.go
internal/replication/log_test.go
internal/fault/detector_test.go
internal/timesync/lamport_test.go
```

### Step 5: Commit and Push

```bash
git add .
git commit -m "feat(consensus): implement RequestVote handler"
git push origin feature/consensus
```

### Step 6: Create a Pull Request

Open a PR from your feature branch to `develop`. Request review from at least one teammate.

---

## 8. Building and Running

```bash
# Build both binaries
make build

# Run a single node
go run cmd/server/main.go --id node1 --port 5001 --peers "localhost:5002,localhost:5003"

# Run the client
go run cmd/client/main.go --leader "localhost:5001" --action publish --message "Hello"

# Run unit tests
make test

# Run integration tests
make test-integration
```

---

## 9. Testing Your Module

### Unit Test Example (Lamport Clock)

```go
// internal/timesync/lamport_test.go
package timesync

import "testing"

func TestClockTick(t *testing.T) {
    clock := NewClock()

    val := clock.Tick()
    if val != 1 {
        t.Errorf("expected 1, got %d", val)
    }

    val = clock.Tick()
    if val != 2 {
        t.Errorf("expected 2, got %d", val)
    }
}

func TestClockUpdate(t *testing.T) {
    clock := NewClock()
    clock.Tick() // clock = 1

    val := clock.Update(5) // max(1, 5) + 1 = 6
    if val != 6 {
        t.Errorf("expected 6, got %d", val)
    }
}
```

### Running Tests

```bash
# Test a specific package
go test ./internal/timesync/... -v

# Test with race detection
go test ./internal/... -race -v

# Test all packages
make test
```

---

## 10. Common Distributed Systems Terminology

| Term                 | Definition                                                      |
| :------------------- | :-------------------------------------------------------------- |
| **Consensus**        | Agreement among nodes on a single value or sequence of values   |
| **Leader**           | The node that coordinates all writes and replication            |
| **Follower**         | A node that passively receives updates from the Leader          |
| **Candidate**        | A node attempting to become Leader through an election          |
| **Term**             | A logical time period; incremented with each new election       |
| **Quorum**           | A majority of nodes (⌊N/2⌋ + 1) needed for commit decisions     |
| **Heartbeat**        | Periodic message from Leader to assert authority                |
| **Election Timeout** | Time a Follower waits before starting an election               |
| **Lamport Clock**    | A logical clock for ordering events without synchronized time   |
| **Happens-Before**   | Causal relationship: event A happened before event B            |
| **Replicated Log**   | An ordered, append-only sequence of entries stored on all nodes |
| **Commit**           | An entry is committed when acknowledged by a quorum             |
| **AppendEntries**    | RPC sent by Leader to replicate log entries (or heartbeat)      |
| **RequestVote**      | RPC sent by Candidate to request votes during election          |
| **Log Consistency**  | All non-faulty nodes have the same committed log prefix         |
| **Failover**         | Automatic transition of leadership when the Leader fails        |
| **Recovery**         | Re-integration of a previously failed node                      |

---

## 11. Frequently Asked Questions

### Q: What should I implement first?

Start with constructors and simple getters. Then implement core logic. Save integration with other modules for last.

### Q: How do I test my module without other modules being ready?

Use mock implementations of other modules' interfaces. For example, to test consensus without replication, create a mock `ReplicatedLog` that stores entries in a simple slice.

### Q: What is a quorum?

A quorum is a majority of nodes. In a 3-node cluster, quorum = 2. In a 5-node cluster, quorum = 3. This ensures any two quorums always share at least one node.

### Q: What's the difference between a Lamport clock and a real clock?

A Lamport clock is a counter, not a time value. It captures _potential_ causality, not wall-clock time. Two events with the same Lamport timestamp are concurrent (neither caused the other).

### Q: Can I add more files to my module?

Yes! The skeleton provides the minimum structure. Feel free to add helper files, but keep them in your module's package.

### Q: What does `TODO: implement` mean?

Every `TODO` comment marks a function body that you need to fill in. The comment above it explains exactly what the function should do.

---

_Good luck with the implementation! Remember: start simple, test often, and communicate with your teammates._
