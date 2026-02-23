# Fault-Tolerant Distributed Messaging System

> A strongly consistent, leader-based distributed messaging system built in Go, featuring Raft-inspired consensus, quorum-based replication, Lamport logical clocks, and automatic failover.

[![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go&logoColor=white)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## Table of Contents

- [System Overview](#system-overview)
- [Architecture Diagram](#architecture-diagram)
- [Overall Workflow](#overall-workflow)
- [Module Responsibilities](#module-responsibilities)
- [Team Members](#-team-members)
- [Folder Structure](#folder-structure)
- [Branching Strategy](#branching-strategy)
- [Development Workflow](#development-workflow)
- [Commit Message Standards](#commit-message-standards)
- [Integration Workflow](#integration-workflow)
- [How to Run](#how-to-run)
- [Evaluation Criteria Alignment](#evaluation-criteria-alignment)
- [Technologies Used](#technologies-used)
- [How We Build This — A Step-by-Step Story](#how-we-build-this--a-step-by-step-story)

---

## System Overview

This system implements a **fault-tolerant distributed messaging platform** designed for environments where message ordering, delivery guarantees, and node failures must be handled gracefully. The architecture follows a **leader-based model** inspired by the Raft consensus protocol.

### Core Design Principles

| Principle                     | Implementation                                                          |
| :---------------------------- | :---------------------------------------------------------------------- |
| **Strong Consistency**        | All committed messages are replicated to a quorum before acknowledgment |
| **Leader-Based Coordination** | A single elected leader serializes all write operations                 |
| **Raft-Inspired Consensus**   | Leader election via randomized timeouts, term-based voting              |
| **Quorum Commit Rule**        | Majority (`⌊N/2⌋ + 1`) must acknowledge before commit                   |
| **Logical Time Ordering**     | Lamport timestamps establish causal ordering across nodes               |
| **Failure Detection**         | Periodic heartbeats with configurable timeout thresholds                |
| **Automatic Failover**        | Leadership re-election triggered upon leader failure detection          |
| **Log Synchronization**       | Recovering nodes replay the replicated log to reach consistency         |

### System Guarantees

- **Safety**: No two leaders exist in the same term.
- **Liveness**: The system makes progress as long as a majority of nodes are operational.
- **Consistency**: All non-faulty nodes converge to the same committed log state.
- **Causal Ordering**: Lamport clocks enforce _happens-before_ relationships across messages.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        CLIENT LAYER                                     │
│                                                                         │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐         │
│   │ Client A │    │ Client B │    │ Client C │    │ Client D │         │
│   └────┬─────┘    └────┬─────┘    └────┬─────┘    └────┬─────┘         │
│        │               │               │               │                │
│        └───────────────┼───────────────┼───────────────┘                │
│                        │               │                                │
│                        ▼               ▼                                │
│              ┌─────────────────────────────────┐                        │
│              │       gRPC / TCP Gateway        │                        │
│              └──────────────┬──────────────────┘                        │
└─────────────────────────────┼───────────────────────────────────────────┘
                              │
┌─────────────────────────────┼───────────────────────────────────────────┐
│                     NODE CLUSTER (N=3 or 5)                             │
│                              │                                          │
│     ┌────────────────────────┼────────────────────────┐                 │
│     │                        ▼                        │                 │
│     │            ┌───────────────────┐                │                 │
│     │            │   LEADER (Node 1) │                │                 │
│     │            │                   │                │                 │
│     │            │ ┌───────────────┐ │                │                 │
│     │            │ │  Consensus    │ │                │                 │
│     │            │ │  Module       │ │                │                 │
│     │            │ │  (Raft)       │ │                │                 │
│     │            │ └───────┬───────┘ │                │                 │
│     │            │         │         │                │                 │
│     │            │ ┌───────▼───────┐ │                │                 │
│     │            │ │  Replicated   │ │                │                 │
│     │            │ │  Log          │ │                │                 │
│     │            │ └───────┬───────┘ │                │                 │
│     │            │         │         │                │                 │
│     │            │ ┌───────▼───────┐ │                │                 │
│     │            │ │  Lamport      │ │                │                 │
│     │            │ │  Clock        │ │                │                 │
│     │            │ └───────────────┘ │                │                 │
│     │            └─────────┬─────────┘                │                 │
│     │                      │                          │                 │
│     │         ┌────────────┼────────────┐             │                 │
│     │         │  AppendEntries / Heartbeat            │                 │
│     │         │  (Quorum Replication)   │             │                 │
│     │         ▼                         ▼             │                 │
│     │  ┌──────────────┐         ┌──────────────┐     │                 │
│     │  │ FOLLOWER     │         │ FOLLOWER     │     │                 │
│     │  │ (Node 2)     │         │ (Node 3)     │     │                 │
│     │  │              │         │              │     │                 │
│     │  │ ┌──────────┐ │         │ ┌──────────┐ │     │                 │
│     │  │ │Replicated│ │         │ │Replicated│ │     │                 │
│     │  │ │Log       │ │         │ │Log       │ │     │                 │
│     │  │ └──────────┘ │         │ └──────────┘ │     │                 │
│     │  │ ┌──────────┐ │         │ ┌──────────┐ │     │                 │
│     │  │ │Failure   │ │         │ │Failure   │ │     │                 │
│     │  │ │Detector  │ │         │ │Detector  │ │     │                 │
│     │  │ └──────────┘ │         │ └──────────┘ │     │                 │
│     │  └──────────────┘         └──────────────┘     │                 │
│     │                                                │                 │
│     └────────────────────────────────────────────────┘                 │
│                                                                         │
│   ┌─────────────────────────────────────────────────────────────┐       │
│   │                   SHARED INFRASTRUCTURE                     │       │
│   │                                                             │       │
│   │  ┌─────────────┐  ┌──────────────┐  ┌───────────────────┐  │       │
│   │  │ Transport   │  │ Config       │  │ Structured        │  │       │
│   │  │ Layer (gRPC)│  │ Management   │  │ Logging           │  │       │
│   │  └─────────────┘  └──────────────┘  └───────────────────┘  │       │
│   └─────────────────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Overall Workflow

### 1. Cluster Bootstrap

1. Nodes start and load configuration (node ID, peer addresses, timeouts).
2. Each node initializes its Lamport clock, empty log, and failure detector.
3. An initial leader election takes place via Raft-style `RequestVote` RPCs.
4. The node receiving a majority of votes transitions to **Leader** state.

### 2. Message Publishing (Write Path)

```
Client ──► Leader ──► AppendEntries(entry) ──► Followers
                            │
                   Wait for quorum ACK
                            │
                   Commit entry to log
                            │
                   Apply to state machine
                            │
                   Respond to Client ◄──────────────────
```

1. Client sends a publish request to the **Leader**.
2. Leader appends the message to its local log with a Lamport timestamp.
3. Leader sends `AppendEntries` RPCs to all followers in parallel.
4. Upon receiving acknowledgment from a **quorum** (`⌊N/2⌋ + 1`), the entry is **committed**.
5. Leader advances `commitIndex` and notifies followers via the next heartbeat.
6. Client receives a success response only after commit.

### 3. Failure Detection & Recovery

1. Followers expect periodic heartbeats from the Leader.
2. If no heartbeat arrives within the **election timeout**, the follower becomes a **Candidate**.
3. The Candidate increments its **term**, votes for itself, and sends `RequestVote` RPCs.
4. A new Leader is elected if a majority grants their vote.
5. Recovering nodes receive missing log entries via `AppendEntries` and synchronize.

### 4. Message Consumption (Read Path)

1. Clients may read from any node (for availability) or from the Leader (for linearizability).
2. Only **committed** entries are visible to consumers.
3. Messages are returned with their Lamport timestamps for causal ordering.

---

## Module Responsibilities

### Module 1: Fault Tolerance

**Owner**: Imansa Bodini

| Component         | Description                                                 |
| :---------------- | :---------------------------------------------------------- |
| Heartbeat Emitter | Leader periodically sends heartbeats to all followers       |
| Heartbeat Monitor | Followers track last-received heartbeat timestamp           |
| Election Timeout  | Randomized timeout triggers candidate transition            |
| Failure Detector  | Declares a node failed after configurable missed heartbeats |
| Recovery Manager  | Re-integrates recovered nodes; triggers log catch-up        |
| Health Reporter   | Exposes node health status for monitoring/debugging         |

**Key Interfaces**:

```go
type FailureDetector interface {
    StartMonitoring(ctx context.Context)
    ReportHeartbeat(nodeID string)
    IsAlive(nodeID string) bool
    OnFailure(callback func(nodeID string))
}

type RecoveryManager interface {
    InitiateRecovery(nodeID string) error
    SyncLog(nodeID string, fromIndex uint64) error
}
```

**Files**: `internal/fault/detector.go`, `internal/fault/heartbeat.go`, `internal/fault/recovery.go`

---

### Module 2: Data Replication & Consistency

**Owner**: Senul Mintharu

| Component             | Description                                              |
| :-------------------- | :------------------------------------------------------- |
| Replicated Log        | Append-only ordered log of message entries               |
| AppendEntries Handler | Processes incoming replication RPCs from the Leader      |
| Quorum Tracker        | Tracks acknowledgments and determines commit eligibility |
| Commit Applier        | Applies committed entries to the state machine           |
| Log Compaction        | Periodic snapshotting to bound log growth (optional)     |
| Consistency Checker   | Validates log prefix consistency across nodes            |

**Key Interfaces**:

```go
type LogEntry struct {
    Index     uint64
    Term      uint64
    Timestamp uint64  // Lamport timestamp
    Data      []byte
}

type ReplicatedLog interface {
    Append(entry LogEntry) error
    GetEntry(index uint64) (LogEntry, error)
    GetEntriesFrom(index uint64) ([]LogEntry, error)
    LastIndex() uint64
    CommitUpTo(index uint64) error
}

type ReplicationManager interface {
    ReplicateEntry(entry LogEntry) error
    WaitForQuorum(index uint64, timeout time.Duration) (bool, error)
}
```

**Files**: `internal/replication/log.go`, `internal/replication/manager.go`, `internal/replication/quorum.go`

---

### Module 3: Time Synchronization

**Owner**: Sabeelur Rashaad

| Component                 | Description                                            |
| :------------------------ | :----------------------------------------------------- |
| Lamport Clock             | Monotonically increasing logical clock per node        |
| Timestamp Generator       | Assigns Lamport timestamps to all events               |
| Clock Synchronizer        | Updates local clock upon receiving remote timestamps   |
| Event Orderer             | Resolves causal ordering using Lamport timestamps      |
| Causal Dependency Tracker | Tracks _happens-before_ relationships between messages |

**Key Interfaces**:

```go
type LamportClock interface {
    Tick() uint64                    // Increment and return
    Update(received uint64) uint64  // max(local, received) + 1
    Current() uint64                // Read current value
}

type EventOrderer interface {
    OrderEvents(events []Event) []Event
    HappensBefore(a, b Event) bool
}

type Event struct {
    NodeID    string
    Timestamp uint64
    Type      EventType
    Data      []byte
}
```

**Lamport Clock Rules**:

1. Before each local event: `clock = clock + 1`
2. Before sending a message: `clock = clock + 1`, attach `clock` to message
3. Upon receiving a message with timestamp `t`: `clock = max(clock, t) + 1`

**Files**: `internal/timesync/lamport.go`, `internal/timesync/orderer.go`, `internal/timesync/event.go`

---

### Module 4: Consensus & Agreement (Raft-Inspired)

**Owner**: Vimukthi Herath

| Component              | Description                                                   |
| :--------------------- | :------------------------------------------------------------ |
| State Machine          | Manages node state transitions: Follower → Candidate → Leader |
| Leader Election        | Implements `RequestVote` RPC with term-based voting           |
| Term Manager           | Tracks and enforces monotonically increasing term numbers     |
| Vote Tracker           | Records votes granted/received per term                       |
| Log Consistency Check  | Ensures candidate's log is at least as up-to-date as voter's  |
| Leadership Maintenance | Leader sends periodic heartbeats to assert authority          |

**Key Interfaces**:

```go
type NodeState int

const (
    Follower  NodeState = iota
    Candidate
    Leader
)

type ConsensusModule interface {
    Start(ctx context.Context)
    State() NodeState
    CurrentTerm() uint64
    HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse
    HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse
    ProposeEntry(data []byte) (uint64, error)
}

type RequestVoteRequest struct {
    Term         uint64
    CandidateID  string
    LastLogIndex uint64
    LastLogTerm  uint64
}

type RequestVoteResponse struct {
    Term        uint64
    VoteGranted bool
}

type AppendEntriesRequest struct {
    Term         uint64
    LeaderID     string
    PrevLogIndex uint64
    PrevLogTerm  uint64
    Entries      []LogEntry
    LeaderCommit uint64
}

type AppendEntriesResponse struct {
    Term    uint64
    Success bool
}
```

**Files**: `internal/consensus/raft.go`, `internal/consensus/election.go`, `internal/consensus/state.go`

---

## 👥 Team Members

| Responsibility                   | Name             | Registration No. | Email                                  |
| :------------------------------- | :--------------- | :--------------- | :------------------------------------- |
| Fault Tolerance                  | Imansa Bodini    | IT24101844       | pedhuruarachchigepremarathne@gmail.com |
| Data Replication & Consistency   | Senul Mintharu   | IT24101497       | senulmintharu@gmail.com                |
| Time Synchronization             | Sabeelur Rashaad | IT24100146       | it24100146@my.sliit.lk                 |
| Consensus & Agreement Algorithms | Vimukthi Herath  | IT24101500       | vimukthiherath123@gmail.com            |

---

## Folder Structure

```
distributed-messaging-system/
├── cmd/
│   ├── server/
│   │   └── main.go              # Node entry point
│   └── client/
│       └── main.go              # CLI client entry point
│
├── internal/
│   ├── consensus/               # Module 4: Raft-inspired consensus
│   │   ├── raft.go              # Core consensus module
│   │   ├── election.go          # Leader election logic
│   │   └── state.go             # Node state management
│   │
│   ├── replication/             # Module 2: Data replication & consistency
│   │   ├── log.go               # Replicated log implementation
│   │   ├── manager.go           # Replication orchestration
│   │   └── quorum.go            # Quorum tracking
│   │
│   ├── timesync/                # Module 3: Time synchronization
│   │   ├── lamport.go           # Lamport clock implementation
│   │   ├── orderer.go           # Event ordering logic
│   │   └── event.go             # Event type definitions
│   │
│   ├── fault/                   # Module 1: Fault tolerance
│   │   ├── detector.go          # Failure detection
│   │   ├── heartbeat.go         # Heartbeat mechanism
│   │   └── recovery.go          # Node recovery & log sync
│   │
│   ├── transport/               # Network communication layer
│   │   ├── grpc_server.go       # gRPC server setup
│   │   ├── grpc_client.go       # gRPC client connections
│   │   └── proto/
│   │       └── messaging.proto  # Protocol Buffers definitions
│   │
│   ├── storage/                  # Module 5: State machine & persistence
│   │   ├── store.go             # In-memory message store
│   │   ├── log_store.go         # Persistent log storage (optional)
│   │   └── snapshot.go          # Log compaction via snapshots (optional)
│   │
│   └── node/                    # Node lifecycle management
│       └── node.go              # Node initialization & coordination
│
├── config/
│   └── config.go                # Configuration loading & defaults
│
├── pkg/
│   └── logger/
│       └── logger.go            # Structured logging utility
│
├── scripts/
│   ├── start_cluster.sh         # Launch a local 3-node cluster
│   └── stop_cluster.sh          # Gracefully stop all nodes
│
├── docs/                            # Per-member documentation
│   ├── STARTER_GUIDE.md             # Getting started guide for all members
│   ├── member1_fault_tolerance.md   # Imansa's module documentation
│   ├── member2_replication.md       # Senul's module documentation
│   ├── member3_timesync.md          # Sabeelur's module documentation
│   └── member4_consensus.md         # Vimukthi's module documentation
│
├── test/
│   └── integration/
│       └── cluster_test.go      # End-to-end cluster tests
│
├── go.mod
├── go.sum
├── Makefile
├── .gitignore
└── README.md
```

### Directory Descriptions

| Directory               | Purpose                                                      |
| :---------------------- | :----------------------------------------------------------- |
| `cmd/`                  | Application entry points (server and client binaries)        |
| `internal/`             | Private application packages (not importable externally)     |
| `internal/consensus/`   | Raft-inspired leader election and log agreement              |
| `internal/replication/` | Log replication, quorum management, commit logic             |
| `internal/timesync/`    | Lamport logical clocks and causal event ordering             |
| `internal/fault/`       | Heartbeat monitoring, failure detection, node recovery       |
| `internal/transport/`   | gRPC-based inter-node and client-node communication          |
| `internal/node/`        | Node lifecycle, initialization, and module coordination      |
| `internal/storage/`     | State machine, message store, snapshots (Sabeelur)           |
| `config/`               | Configuration structures and loading utilities               |
| `pkg/`                  | Shared utilities (logging) — importable by external packages |
| `scripts/`              | Cluster management scripts for local development             |
| `docs/`                 | Per-member implementation guides and starter documentation   |
| `test/`                 | Integration and end-to-end tests                             |

---

## Branching Strategy

```
main
 │
 ├── develop                          ← Integration branch
 │    │
 │    ├── feature/fault-tolerance     ← Imansa: heartbeats, failure detection, recovery
 │    ├── feature/replication         ← Senul: log replication, quorum, commit
 │    ├── feature/time-sync           ← Sabeelur: Lamport clocks, event ordering
 │    ├── feature/consensus           ← Vimukthi: Raft election, state machine
 │    │
 │    ├── feature/transport           ← Shared: gRPC transport layer
 │    └── fix/*                       ← Bug fix branches
 │
 └── release/v1.0                     ← Submission-ready release
```

### Branch Purposes

| Branch                    | Purpose                                           | Protected |
| :------------------------ | :------------------------------------------------ | :-------- |
| `main`                    | Stable, submission-ready code only                | Yes       |
| `develop`                 | Integration branch; all features merge here first | Yes       |
| `feature/fault-tolerance` | Fault detection, heartbeats, recovery (Imansa)    | No        |
| `feature/replication`     | Log replication, quorum tracking (Senul)          | No        |
| `feature/time-sync`       | Lamport clocks, event ordering (Sabeelur)         | No        |
| `feature/consensus`       | Raft election, state transitions (Vimukthi)       | No        |
| `feature/transport`       | Shared gRPC transport layer                       | No        |
| `fix/*`                   | Bug fixes (e.g., `fix/election-timeout`)          | No        |
| `release/v1.0`            | Final release candidate for submission            | No        |

### Branch Rules

- **Never push directly** to `main` or `develop`.
- All changes go through **Pull Requests** with at least **1 reviewer**.
- Feature branches are created from `develop` and merged back into `develop`.
- Only `develop` is merged into `main` after integration testing passes.

---

## Development Workflow

### Phase 1 — Foundation (Weeks 1–2)

```
All members collaborate on:
  1. Project scaffolding (go.mod, folder structure, Makefile)
  2. Shared transport layer (gRPC protobuf definitions)
  3. Node configuration and startup logic
  4. Structured logging utility
```

### Phase 2 — Module Development (Weeks 3–6)

Each member works on their `feature/*` branch independently:

```
┌──────────────────────────────────────────────────────────┐
│  Vimukthi (Consensus)          Imansa (Fault Tolerance)  │
│  ───────────────────           ───────────────────────── │
│  - Node state machine          - Heartbeat emitter       │
│  - RequestVote RPC             - Heartbeat monitor       │
│  - Leader election             - Failure detector        │
│  - Term management             - Recovery manager        │
│                                                          │
│  Senul (Replication)           Sabeelur (Time Sync)      │
│  ──────────────────            ─────────────────────     │
│  - Replicated log              - Lamport clock           │
│  - AppendEntries handler       - Clock synchronizer      │
│  - Quorum tracker              - Event orderer           │
│  - Commit applier              - Causal dependency       │
└──────────────────────────────────────────────────────────┘
```

### Phase 3 — Integration (Weeks 7–8)

```
feature/consensus     ──┐
feature/replication   ──┼──► develop ──► Integration Testing
feature/time-sync     ──┤
feature/fault-tolerance─┘
```

1. Each member creates a PR from their feature branch into `develop`.
2. Code review by at least one other team member.
3. Resolve merge conflicts collaboratively.
4. Run integration tests against a local 3-node cluster.

### Phase 4 — Stabilization & Submission (Weeks 9–10)

```
develop ──► release/v1.0 ──► main
```

1. Create `release/v1.0` from `develop`.
2. Fix any remaining bugs on `fix/*` branches merged into `release/v1.0`.
3. Final merge into `main` and tag `v1.0.0`.

---

## Commit Message Standards

All commits must follow the **Conventional Commits** format:

```
<type>(<scope>): <short description>

[optional body]
[optional footer]
```

### Types

| Type       | Usage                                      |
| :--------- | :----------------------------------------- |
| `feat`     | New feature or functionality               |
| `fix`      | Bug fix                                    |
| `refactor` | Code restructuring without behavior change |
| `docs`     | Documentation updates                      |
| `test`     | Adding or modifying tests                  |
| `chore`    | Build, CI, or tooling changes              |

### Scopes

| Scope         | Module                                  |
| :------------ | :-------------------------------------- |
| `consensus`   | Raft election, state machine            |
| `replication` | Log replication, quorum                 |
| `timesync`    | Lamport clocks, event ordering          |
| `fault`       | Heartbeats, failure detection, recovery |
| `transport`   | gRPC communication layer                |
| `node`        | Node lifecycle                          |
| `config`      | Configuration                           |

### Examples

```
feat(consensus): implement RequestVote RPC handler
fix(fault): correct election timeout randomization range
refactor(replication): extract quorum tracking into separate module
test(timesync): add unit tests for Lamport clock update rule
docs(readme): add architecture diagram and workflow description
chore: configure Makefile with build and test targets
```

---

## Integration Workflow

### Module Dependency Graph

```
                    ┌─────────────┐
                    │  Consensus  │
                    │  (Module 4) │
                    └──────┬──────┘
                           │ uses
              ┌────────────┼────────────┐
              ▼            ▼            ▼
     ┌────────────┐ ┌───────────┐ ┌──────────┐
     │ Replication│ │   Time    │ │  Fault   │
     │ (Module 2) │ │   Sync   │ │Tolerance │
     │            │ │(Module 3) │ │(Module 1)│
     └────────────┘ └───────────┘ └──────────┘
              │            │            │
              └────────────┼────────────┘
                           ▼
                    ┌─────────────┐
                    │  Transport  │
                    │   (gRPC)    │
                    └─────────────┘
```

### Integration Points

| Producer Module | Consumer Module | Interface                                        |
| :-------------- | :-------------- | :----------------------------------------------- |
| Consensus       | Replication     | Leader calls `ReplicateEntry()` after log append |
| Consensus       | Fault Tolerance | Election triggered by `OnFailure()` callback     |
| Consensus       | Time Sync       | `Tick()` called on each state transition         |
| Replication     | Time Sync       | `Tick()` called on each log append               |
| Fault Tolerance | Consensus       | Heartbeat timeout triggers `StartElection()`     |
| Time Sync       | Replication     | Lamport timestamp attached to every `LogEntry`   |
| Transport       | All Modules     | gRPC handles all inter-node RPC delivery         |

### Integration Contracts

Each module exposes a well-defined Go `interface`. Modules communicate **only** through these interfaces — never by accessing each other's internal state. This enables:

- Independent development and testing via mocks
- Clean separation of concerns
- Easy integration by wiring interfaces in `internal/node/node.go`

```go
// internal/node/node.go — wiring example
type Node struct {
    ID        string
    Consensus consensus.ConsensusModule
    Log       replication.ReplicatedLog
    Clock     timesync.LamportClock
    Detector  fault.FailureDetector
    Transport transport.Transport
}
```

---

## How to Run

### Prerequisites

- **Go** 1.21 or later
- **protoc** (Protocol Buffers compiler) with `protoc-gen-go` and `protoc-gen-go-grpc`
- **Make** (optional, for convenience targets)

### Build

```bash
# Clone the repository
git clone https://github.com/<org>/distributed-messaging-system.git
cd distributed-messaging-system

# Install dependencies
go mod tidy

# Build server and client binaries
make build
```

### Run a Local Cluster (3 Nodes)

```bash
# Terminal 1 — Node 1
go run cmd/server/main.go --id node1 --port 5001 --peers "localhost:5002,localhost:5003"

# Terminal 2 — Node 2
go run cmd/server/main.go --id node2 --port 5002 --peers "localhost:5001,localhost:5003"

# Terminal 3 — Node 3
go run cmd/server/main.go --id node3 --port 5003 --peers "localhost:5001,localhost:5002"
```

### Send a Message

```bash
# Publish a message via the CLI client
go run cmd/client/main.go --leader "localhost:5001" --action publish --message "Hello, distributed world!"

# Consume messages
go run cmd/client/main.go --leader "localhost:5001" --action consume
```

### Run Tests

```bash
# Unit tests
make test

# Integration tests (requires a running cluster)
make test-integration
```

### Makefile Targets

| Target                  | Description                              |
| :---------------------- | :--------------------------------------- |
| `make build`            | Compile server and client binaries       |
| `make test`             | Run all unit tests                       |
| `make test-integration` | Run integration tests                    |
| `make proto`            | Regenerate gRPC code from `.proto` files |
| `make lint`             | Run `golangci-lint`                      |
| `make clean`            | Remove build artifacts                   |

---

## Evaluation Criteria Alignment

| Evaluation Criterion         | How This Project Addresses It                                                                           |
| :--------------------------- | :------------------------------------------------------------------------------------------------------ |
| **Distributed Architecture** | Multi-node cluster with leader-follower topology, gRPC-based communication                              |
| **Fault Tolerance**          | Heartbeat-based failure detection, automatic leader re-election, node recovery with log synchronization |
| **Consistency Model**        | Strong consistency via quorum-based majority commit; no stale reads from leader                         |
| **Consensus Algorithm**      | Raft-inspired leader election with term-based voting and log consistency checks                         |
| **Data Replication**         | Replicated append-only log with `AppendEntries` RPC; quorum acknowledgment before commit                |
| **Time Synchronization**     | Lamport logical clocks enforcing causal ordering of all events and messages                             |
| **Modularity**               | Clean separation into four independent modules with well-defined interfaces                             |
| **Code Quality**             | Idiomatic Go, structured logging, comprehensive tests, conventional commits                             |
| **Documentation**            | Academic-grade README, architecture diagrams, interface specifications                                  |
| **Teamwork**                 | Clear responsibility allocation, Git branching strategy, integration workflow                           |

---

## Technologies Used

| Technology                                  | Purpose                                     |
| :------------------------------------------ | :------------------------------------------ |
| [Go 1.21+](https://go.dev/)                 | Primary implementation language             |
| [gRPC](https://grpc.io/)                    | Inter-node and client-node RPC framework    |
| [Protocol Buffers](https://protobuf.dev/)   | Serialization format for RPC messages       |
| [golangci-lint](https://golangci-lint.run/) | Static analysis and linting                 |
| [Go testing](https://pkg.go.dev/testing)    | Unit and integration test framework         |
| [Make](https://www.gnu.org/software/make/)  | Build automation                            |
| [Git](https://git-scm.com/)                 | Version control with conventional branching |

---

## How We Build This — A Step-by-Step Story

This section tells the story of how the team should build this project, step by step, in simple language. Read it like a recipe — follow the order, and everything will come together smoothly.

---

### Chapter 1: Everyone Starts Together (Week 1)

**What happens:** Before anyone touches their own module, the whole team sits down and sets up the project together.

**Why this matters:** If one person sets up the project differently from another, things will break when you try to combine your work later. Starting together avoids that.

**What to do:**

1. **One person** (any team member) creates the GitHub repository and pushes the skeleton code.
2. **Everyone** clones the repo to their own machine:
   ```bash
   git clone https://github.com/<org>/distributed-messaging-system.git
   cd distributed-messaging-system
   ```
3. **Everyone** installs Go 1.21+ and runs `go mod tidy` to download dependencies.
4. **Everyone** runs `make build` to make sure the project compiles with no errors.
5. **Everyone** creates their own feature branch from `develop`:
   - Imansa: `git checkout -b feature/fault-tolerance develop`
   - Senul: `git checkout -b feature/replication develop`
   - Sabeelur: `git checkout -b feature/time-sync develop`
   - Vimukthi: `git checkout -b feature/consensus develop`
6. **Everyone** reads their personal documentation file in the `docs/` folder.

> **Think of it this way:** You're all building different rooms of the same house. Before you start working on your room, you need to agree on where the doors and walls go — that's what the skeleton code and interfaces do.

---

### Chapter 2: Sabeelur Starts First — The Clock (Week 2)

**Who works:** Sabeelur Rashaad (Time Synchronization & Storage)

**Why first?** Almost every other module needs Lamport timestamps. When Senul stores a log entry, it needs a timestamp. When Vimukthi sends a vote request, it needs a timestamp. The clock is like the heartbeat of the entire system — everything depends on it.

**What to build (in this order):**

1. **`internal/timesync/lamport.go`** — The Lamport Clock
   - This is just a counter with three operations: `Tick()` (add 1), `Update(received)` (take the bigger number + 1), and `Current()` (read the number).
   - Think of it as a page number. Every time something happens, you turn to the next page. If someone sends you something from page 50 and you're on page 30, you jump to page 51.
   - **Time needed:** About 1-2 hours.

2. **`internal/timesync/event.go`** — Events
   - A simple box that holds: what happened (send, receive, or internal), who did it (node ID), and when (Lamport timestamp).
   - Just a struct and a `String()` method for printing.
   - **Time needed:** About 30 minutes.

3. **`internal/timesync/orderer.go`** — Event Orderer
   - Takes a list of events and sorts them by timestamp. If two events have the same timestamp, sort by node ID (alphabetical) to break the tie.
   - **Time needed:** About 1 hour.

4. **`internal/storage/store.go`** — Message Store
   - A simple in-memory map. When a message is committed, `Apply()` stores it. `Get()` retrieves it.
   - **Time needed:** About 1-2 hours.

> **Once Sabeelur finishes the clock**, push it to your branch and let the team know. The others will import your `LamportClock` interface when they need timestamps.

---

### Chapter 3: Imansa and Senul Start Next — In Parallel (Weeks 2-3)

Once Sabeelur's clock is ready (or even the interface is defined), two members can work at the same time because their modules don't depend on each other.

---

#### Imansa's Track: Fault Tolerance

**Who works:** Imansa Bodini

**What is this?** Your module is the system's immune system. It watches for sick or dead nodes and raises the alarm.

**What to build (in this order):**

1. **`internal/fault/heartbeat.go`** — Heartbeat Emitter
   - The Leader sends a small "I'm alive" message to every follower at regular intervals (like a pulse).
   - Implement `Start()` — a loop that runs every `heartbeatInterval` milliseconds and sends a heartbeat.
   - **Time needed:** About 1-2 hours.

2. **`internal/fault/detector.go`** — Failure Detector
   - Each follower keeps track of when it last heard from the leader.
   - `ReportHeartbeat(nodeID)` — updates the "last seen" timestamp.
   - `IsAlive(nodeID)` — checks if the last heartbeat was recent enough.
   - `StartMonitoring()` — a background loop that checks all nodes periodically.
   - If a node is declared dead, call the `OnFailure` callback (this will trigger a new election).
   - **Time needed:** About 2-3 hours.

3. **`internal/fault/recovery.go`** — Recovery Manager
   - When a crashed node comes back online, this module sends it the log entries it missed.
   - `SyncLog(nodeID, fromIndex)` — sends entries from `fromIndex` to the end of the log.
   - **Time needed:** About 1-2 hours.

> **Analogy:** Imagine a group chat where the teacher (Leader) sends a "good morning" message every 5 seconds. If you don't see a message for 15 seconds, you assume the teacher left, and someone else becomes the new teacher.

---

#### Senul's Track: Replication

**Who works:** Senul Mintharu

**What is this?** Your module is the filing system. It makes sure every node has the same copy of every message, in the same order.

**What to build (in this order):**

1. **`internal/replication/quorum.go`** — Quorum Tracker
   - The simplest piece. It counts how many nodes have said "I stored the entry."
   - In a 3-node cluster, you need 2 out of 3 to agree (that's a quorum/majority).
   - `RecordAck(index, nodeID)` — marks that a node stored the entry.
   - `HasQuorum(index)` — returns true if enough nodes responded.
   - **Time needed:** About 1-2 hours.

2. **`internal/replication/log.go`** — Replicated Log
   - An ordered list of entries. Think of it like a notebook — you can only write on the next empty page (append-only).
   - `Append(entry)` — add to the end.
   - `GetEntry(index)` — read a specific page.
   - `CommitUpTo(index)` — mark everything up to this page as "officially saved."
   - **Time needed:** About 2-3 hours.

3. **`internal/replication/manager.go`** — Replication Manager
   - The conductor. When the Leader gets a new message:
     1. Append it to the local log.
     2. Send it to every follower (in parallel, using goroutines).
     3. Wait until enough followers respond (quorum).
     4. Mark the entry as committed.
   - **Time needed:** About 2-3 hours.

> **Analogy:** Imagine you're the class monitor (Leader). A student gives you a note. You write it in your notebook, then pass copies to two classmates. Once at least one classmate says "I wrote it down too" (2 out of 3 = quorum), you tell the student "your note is saved."

---

### Chapter 4: Vimukthi Builds Consensus — The Brain (Weeks 3-4)

**Who works:** Vimukthi Herath

**Why after the others?** Consensus is the module that ties everything together. It uses the replicated log (Senul's work), the Lamport clock (Sabeelur's work), and triggers fault detection (Imansa's work). You can start the data structures early, but the full logic needs the other pieces.

**What to build (in this order):**

1. **`internal/consensus/state.go`** — Data Structures
   - No logic here — just define the structs and constants. These are already in the skeleton.
   - Review the fields, make sure they match what the other modules expect.
   - **This can be done in Week 1-2 alongside the others.**
   - **Time needed:** About 30 minutes.

2. **`internal/consensus/election.go`** — Leader Election
   - **Election timeout:** Each node waits a random amount of time (e.g., 150ms to 300ms). If it doesn't hear from a leader in that time, it starts an election.
   - **Voting:** The candidate asks every other node "Will you vote for me?" A node votes yes if:
     - The candidate's term is higher than its own.
     - It hasn't already voted for someone else this term.
     - The candidate's log is at least as complete as its own.
   - **Winning:** If the candidate gets votes from a majority, it becomes Leader.
   - **Time needed:** About 3-4 hours.

3. **`internal/consensus/raft.go`** — The Full Raft Node
   - This is the big one. It runs the main loop:
     - **As Follower:** Wait for heartbeats. If timeout → become Candidate.
     - **As Candidate:** Request votes. If majority → become Leader. If higher term seen → become Follower.
     - **As Leader:** Send heartbeats. Accept new messages. Replicate to followers.
   - `ProposeEntry(data)` — Only the leader can do this. Takes the message, creates a log entry, hands it to Senul's replication manager.
   - `HandleRequestVote(req)` — Process a vote request from another candidate.
   - `HandleAppendEntries(req)` — Process entries from the leader (or heartbeats).
   - **Time needed:** About 4-6 hours.

> **Analogy:** Think of a classroom. The teacher (Leader) is in charge. If the teacher leaves (crash), the students wait a random amount of time. The first student to say "I'll be the teacher" asks everyone to vote. If most students agree, that student becomes the new teacher. If two students ask at the same time (split vote), they wait random times and try again.

---

### Chapter 5: Everyone Comes Together — Integration (Weeks 5-6)

**Who works:** Everyone, together.

**What happens:** Now each module works on its own. It's time to connect them.

**Step-by-step:**

1. **Each member** pushes their latest code to their feature branch.

2. **Each member** creates a Pull Request (PR) from their feature branch into `develop`.

3. **Review each other's PRs.** Look for:
   - Does it match the interface we agreed on?
   - Are there any bugs you can spot?
   - Does the code compile?

4. **Merge into `develop`** one by one, in this order:

   ```
   1st: feature/time-sync       (Sabeelur)  — no dependencies
   2nd: feature/replication      (Senul)     — uses timesync
   3rd: feature/fault-tolerance  (Imansa)    — uses replication for recovery
   4th: feature/consensus        (Vimukthi)  — uses all three above
   ```

5. **Wire everything in `internal/node/node.go`:**
   - Create instances of each module.
   - Connect them: pass the clock to the replication manager, pass the log to the consensus module, pass the failure callback to the election manager.
   - This is where the house comes together — you're connecting the rooms with doors.

6. **Fix any integration bugs.** Things that commonly go wrong:
   - Mismatched function signatures (one module expects `uint64`, another sends `int`).
   - Missing imports or circular dependencies.
   - Race conditions (use `go test -race` to detect these).

---

### Chapter 6: Test the Whole System (Weeks 6-7)

**Who works:** Everyone.

**What to test:**

1. **Start 3 nodes** and verify one gets elected as Leader:

   ```bash
   # In three separate terminals:
   go run cmd/server/main.go --id node1 --port 5001 --peers "localhost:5002,localhost:5003"
   go run cmd/server/main.go --id node2 --port 5002 --peers "localhost:5001,localhost:5003"
   go run cmd/server/main.go --id node3 --port 5003 --peers "localhost:5001,localhost:5002"
   ```

2. **Publish a message** and check all nodes have it:

   ```bash
   go run cmd/client/main.go --leader "localhost:5001" --action publish --message "test"
   ```

3. **Kill the leader** (Ctrl+C on the leader's terminal) and watch a new leader get elected.

4. **Restart the killed node** and verify it catches up (receives the messages it missed).

5. **Check causal ordering** — publish multiple messages and verify they come back in the correct Lamport order.

6. **Run the integration tests:**
   ```bash
   make test-integration
   ```

---

### Chapter 7: Polish and Submit (Weeks 8-9)

1. Fix any remaining bugs.
2. Write unit tests for each module (aim for at least 3-5 tests per file).
3. Make sure `make build`, `make test`, and `make lint` all pass cleanly.
4. Merge `develop` into `release/v1.0`, then into `main`.
5. Tag the release:
   ```bash
   git tag -a v1.0.0 -m "Final submission"
   git push origin v1.0.0
   ```

---

### The Big Picture: Who Does What and When

```
Week 1       Week 2       Week 3       Week 4       Week 5       Week 6+
──────       ──────       ──────       ──────       ──────       ──────

ALL          Sabeelur     Imansa       Vimukthi     ALL          ALL
Setup &      Clock ────►  Heartbeat    Election     Integrate    Test &
Skeleton     Events       Detector     Raft Node    & Wire       Polish
             Orderer      Recovery                  in node.go
             Store
                          Senul
                          Quorum ────►
                          Log
                          Manager
```

**Summary:**

| Order | Who                       | What                          | Why This Order                                   |
| :---- | :------------------------ | :---------------------------- | :----------------------------------------------- |
| 1st   | Everyone                  | Setup, clone, branches        | Everyone needs the same starting point           |
| 2nd   | Sabeelur                  | Lamport Clock + Store         | Others need timestamps; store needed for commits |
| 3rd   | Imansa + Senul (parallel) | Fault Detection + Replication | Independent modules, no dependency on each other |
| 4th   | Vimukthi                  | Consensus (Raft)              | Needs clock, log, and fault detection to work    |
| 5th   | Everyone                  | Integration in node.go        | Connect all the pieces                           |
| 6th   | Everyone                  | Testing & bug fixes           | Make sure it all works end-to-end                |
| 7th   | Everyone                  | Polish & submit               | Clean code, docs, final release                  |

---

> **Remember:** You're not building four separate projects — you're building ONE system with four parts. Talk to each other. If you change an interface, tell the person who uses it. If you're stuck, ask. The best distributed systems are built by teams that communicate well — just like the nodes in your system.

---

## References

- Lamport, L. (1978). _Time, Clocks, and the Ordering of Events in a Distributed System_. Communications of the ACM, 21(7), 558–565.
- Ongaro, D., & Ousterhout, J. (2014). _In Search of an Understandable Consensus Algorithm (Raft)_. USENIX ATC.
- Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2011). _Distributed Systems: Concepts and Design_ (5th ed.). Pearson.

---

<p align="center">
  <i>Built with discipline. Designed for resilience.</i>
</p> _Time, Clocks, and the Ordering of Events in a Distributed System_. Communications of the ACM, 21(7), 558–565.
- Ongaro, D., & Ousterhout, J. (2014). _In Search of an Understandable Consensus Algorithm (Raft)_. USENIX ATC.
- Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2011). _Distributed Systems: Concepts and Design_ (5th ed.). Pearson.

---

<p align="center">
  <i>Built with discipline. Designed for resilience.</i>
</p>
