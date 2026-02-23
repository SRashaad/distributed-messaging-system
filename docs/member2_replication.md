# Member Documentation: Data Replication & Consistency Module

**Owner:** Senul Mintharu (IT24101497)  
**Branch:** `feature/replication`  
**Package:** `internal/replication/`

---

## Overview

You are responsible for the **Data Replication & Consistency** module. This is the core data layer of the system. Your module ensures that every message published to the cluster is stored reliably on a majority of nodes before being considered committed.

Your module provides three key capabilities:

1. **Replicated Log** — An append-only ordered sequence of log entries stored on every node.
2. **Replication Manager** — Orchestrates sending entries from the Leader to all Followers.
3. **Quorum Tracker** — Counts acknowledgments and determines when a majority has stored an entry.

---

## Your Files

| File                              | Purpose                                               |
| :-------------------------------- | :---------------------------------------------------- |
| `internal/replication/log.go`     | Replicated log interface and in-memory implementation |
| `internal/replication/manager.go` | Replication orchestration (Leader → Followers)        |
| `internal/replication/quorum.go`  | Quorum (majority) acknowledgment tracking             |

---

## What You Need to Implement

### 1. `log.go` — Replicated Log

The `InMemoryLog` is an append-only, ordered sequence of `LogEntry` records. Every node has its own copy. The Leader appends entries first, then replicates to Followers.

**Key functions to implement:**

| Function                | What It Does                                                               |
| :---------------------- | :------------------------------------------------------------------------- |
| `NewInMemoryLog()`      | Creates an empty log. Initialize entries slice and commitIndex=0.          |
| `Append(entry)`         | Adds entry to the end. Validate `entry.Index == len(entries)+1` (no gaps). |
| `GetEntry(index)`       | Returns the entry at the given 1-based index. Bounds check.                |
| `GetEntriesFrom(index)` | Returns all entries from index to end (used for replication).              |
| `LastIndex()`           | Returns `len(entries)` as uint64.                                          |
| `LastTerm()`            | Returns the Term of the last entry, or 0 if empty.                         |
| `CommitUpTo(index)`     | Advances `commitIndex` to index. Validate bounds.                          |
| `CommitIndex()`         | Returns the current commit index.                                          |

**Important:** Entries are 1-indexed (first entry has Index=1). Use `sync.RWMutex` for thread safety.

### 2. `manager.go` — Replication Manager

The `Manager` coordinates log replication from the Leader to all Followers. When a new entry is proposed, it appends locally and then sends `AppendEntries` RPCs to every follower in parallel.

**Key functions to implement:**

| Function                        | What It Does                                                      |
| :------------------------------ | :---------------------------------------------------------------- |
| `NewManager(log, clusterSize)`  | Creates a manager with the given log and a QuorumTracker.         |
| `ReplicateEntry(entry)`         | Appends entry locally, then sends to all followers via transport. |
| `WaitForQuorum(index, timeout)` | Blocks until quorum ACKs the entry or timeout expires.            |

**Replication flow:**

1. Leader appends entry to local log → `log.Append(entry)`
2. Leader sends `AppendEntries` to each follower in a goroutine
3. Each follower ACK → `quorum.RecordAck(index, nodeID)`
4. Leader checks → `quorum.HasQuorum(index)`
5. If quorum → `log.CommitUpTo(index)`

### 3. `quorum.go` — Quorum Tracker

The `QuorumTracker` counts how many nodes have acknowledged (stored) each log entry. When the count reaches the majority threshold, the entry can be committed.

**Key functions to implement:**

| Function                        | What It Does                                                |
| :------------------------------ | :---------------------------------------------------------- |
| `NewQuorumTracker(clusterSize)` | Creates tracker. `majority = clusterSize/2 + 1`.            |
| `RecordAck(index, nodeID)`      | Records that nodeID has stored the entry at index.          |
| `HasQuorum(index)`              | Returns `true` if `len(acks[index]) >= majority`.           |
| `AckCount(index)`               | Returns count of ACKs for the given index.                  |
| `Cleanup(belowIndex)`           | Removes tracking data for committed entries (saves memory). |

---

## How Your Module Connects to Others

```
         Client publishes message
                  │
                  ▼
         ┌────────────────┐
         │   Consensus    │  ← Vimukthi
         │  ProposeEntry()│
         └───────┬────────┘
                 │ calls
                 ▼
  ┌──────────────────────────────────────┐
  │         Your Module (Replication)     │
  │                                       │
  │  ┌──────────┐ ┌────────┐ ┌────────┐ │
  │  │   Log    │ │Manager │ │ Quorum │ │
  │  │ (log.go) │ │(mgr.go)│ │(qrm.go)│ │
  │  └──────────┘ └────────┘ └────────┘ │
  └───────┬──────────────────────┬───────┘
          │                      │
          ▼                      ▼
   ┌────────────┐         ┌────────────┐
   │  Timesync  │         │ Transport  │
   │ (Sabeelur) │         │  (gRPC)    │
   └────────────┘         └────────────┘
```

| Your Component | Connects To         | How                                                            |
| :------------- | :------------------ | :------------------------------------------------------------- |
| Log            | Consensus           | Raft reads/writes the log for AppendEntries consistency checks |
| Log            | Recovery (Imansa)   | Recovery reads the log to send missing entries                 |
| Manager        | Transport           | Sends AppendEntries RPCs to followers                          |
| Manager        | Timesync (Sabeelur) | Each LogEntry gets a Lamport timestamp                         |
| Quorum         | Consensus           | Leader checks HasQuorum() before committing                    |

---

## Suggested Implementation Order

1. **`quorum.go`** — Simplest. Pure counting logic with no dependencies.
2. **`log.go`** — In-memory append/read with bounds checking.
3. **`manager.go`** — Depends on log and quorum. Transport integration comes last.

---

## Unit Test Ideas

```go
// internal/replication/quorum_test.go
func TestQuorumTracker(t *testing.T) {
    qt := NewQuorumTracker(3) // 3 nodes, majority = 2

    qt.RecordAck(1, "node1")
    if qt.HasQuorum(1) { t.Error("should not have quorum with 1 ACK") }

    qt.RecordAck(1, "node2")
    if !qt.HasQuorum(1) { t.Error("should have quorum with 2 ACKs") }
}

// internal/replication/log_test.go
func TestLogAppendAndGet(t *testing.T) {
    log := NewInMemoryLog()

    entry := consensus.LogEntry{Index: 1, Term: 1, Data: []byte("hello")}
    if err := log.Append(entry); err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    got, err := log.GetEntry(1)
    if err != nil { t.Fatalf("unexpected error: %v", err) }
    if string(got.Data) != "hello" { t.Error("data mismatch") }
}
```

---

## Key Distributed Systems Concepts

- **Replicated Log**: Every node maintains its own copy. The Leader's log is the source of truth. Followers replicate from the Leader.
- **Quorum**: A majority of nodes (⌊N/2⌋ + 1). In a 3-node cluster, quorum = 2. Ensures any two quorums overlap by at least one node.
- **Commit**: An entry is committed when it has been replicated to a quorum. Committed entries are never lost (even if nodes crash).
- **Log Consistency**: All nodes must have the same committed prefix. The AppendEntries RPC includes `PrevLogIndex` and `PrevLogTerm` to detect inconsistencies.
- **Strong Consistency**: Clients only see committed entries. No stale reads.

---

_Start with the quorum tracker — it's the simplest. Then build the log, and finally the manager._
