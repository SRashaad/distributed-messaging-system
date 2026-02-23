# Member Documentation: Consensus Module (Raft-Inspired)

**Owner:** Vimukthi Herath (IT24100905)  
**Branch:** `feature/consensus`  
**Package:** `internal/consensus/`

---

## Overview

You are responsible for the **Consensus Module** — the brain of the distributed system. This module implements a simplified Raft consensus algorithm that:

1. **Elects a Leader** among the cluster nodes.
2. **Proposes and agrees on log entries** so all nodes store the same data in the same order.
3. **Maintains distributed state** (Follower, Candidate, Leader) across all nodes.

Your code decides WHO the leader is and WHEN an entry is safe to commit. Every other module depends on yours.

---

## Your Files

| File                             | Purpose                                                            |
| :------------------------------- | :----------------------------------------------------------------- |
| `internal/consensus/raft.go`     | Main Raft node logic — state machine, RPC handlers, entry proposal |
| `internal/consensus/election.go` | Leader election: timeout management, vote tracking, log comparison |
| `internal/consensus/state.go`    | Data structures: node states, RPC messages, log entries            |

---

## What You Need to Implement

### 1. `state.go` — Data Structures

This file defines ALL the types used by the consensus module. No logic — just struct definitions and constants.

**Types defined (already in skeleton):**

| Type                    | Purpose                                                          |
| :---------------------- | :--------------------------------------------------------------- |
| `NodeState` (int)       | Enum: Follower (0), Candidate (1), Leader (2)                    |
| `LogEntry`              | Index, Term, Data, Timestamp — one entry in the replicated log   |
| `RequestVoteRequest`    | CandidateID, Term, LastLogIndex, LastLogTerm                     |
| `RequestVoteResponse`   | Term, VoteGranted                                                |
| `AppendEntriesRequest`  | LeaderID, Term, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit |
| `AppendEntriesResponse` | Term, Success                                                    |

**You should:**

- Review the struct fields and add any missing ones (e.g., `NodeID` in responses).
- Add helper methods like `String()` for NodeState.
- Add constants for Follower, Candidate, Leader.

### 2. `election.go` — Leader Election

The election module handles randomized election timeouts and vote counting. Raft uses a first-come-first-served voting rule with term-based invalidation.

**Key functions to implement:**

| Function                                                                        | What It Does                                                                 |
| :------------------------------------------------------------------------------ | :--------------------------------------------------------------------------- |
| `NewElectionManager(minTimeout, maxTimeout)`                                    | Creates a manager with randomized timeout range.                             |
| `ResetTimer()`                                                                  | Restarts the election timer with a new random duration.                      |
| `TimedOut()`                                                                    | Returns true if the election timeout has expired.                            |
| `StartElection(raftNode)`                                                       | Increments term, votes for self, requests votes from all peers.              |
| `IsLogUpToDate(candidateLastIndex, candidateLastTerm, myLastIndex, myLastTerm)` | Returns true if the candidate's log is at least as up-to-date (Raft §5.4.1). |

**Vote Tracking:**

| Function                      | What It Does                                     |
| :---------------------------- | :----------------------------------------------- |
| `NewVoteTracker(clusterSize)` | Creates tracker. Majority = `clusterSize/2 + 1`. |
| `RecordVote(nodeID, granted)` | Records a vote response.                         |
| `HasMajority()`               | Returns true if enough grants received.          |
| `Reset()`                     | Clears votes for a new election round.           |

**Log Up-To-Date Rule (Raft §5.4.1):**

1. Compare last log term: higher term wins.
2. If terms equal: longer log (higher last index) wins.
3. This ensures the elected leader has all committed entries.

**Randomized Timeout:**

- Each node picks a random timeout in `[minTimeout, maxTimeout]` (e.g., 150ms–300ms).
- If no heartbeat received before timeout, the node starts an election.
- Randomization reduces split-vote probability.

### 3. `raft.go` — Raft Node (Main Logic)

The core state machine. This is the most complex file — implement it last.

**Key functions to implement:**

| Function                   | What It Does                                                                                                  |
| :------------------------- | :------------------------------------------------------------------------------------------------------------ |
| `NewRaftNode(config)`      | Creates a Raft node starting as Follower in Term 0.                                                           |
| `Start()`                  | Launches goroutines: election ticker, heartbeat sender (if leader).                                           |
| `Stop()`                   | Stops all goroutines gracefully.                                                                              |
| `State()`                  | Returns current state (Follower/Candidate/Leader).                                                            |
| `CurrentTerm()`            | Returns current term number.                                                                                  |
| `LeaderID()`               | Returns ID of known leader (or "" if unknown).                                                                |
| `HandleRequestVote(req)`   | Processes a vote request. Vote if: term ≥ ours, haven't voted yet (this term), candidate's log is up-to-date. |
| `HandleAppendEntries(req)` | Processes entries from Leader. Validate term, check PrevLogIndex/PrevLogTerm, append entries, advance commit. |
| `ProposeEntry(data)`       | Leader-only: creates a LogEntry, assigns index and term, sends to replication manager.                        |

**State Machine Transitions:**

```
                  timeout
    ┌──────────┐ ────────► ┌──────────┐
    │ Follower │           │Candidate │
    └──────────┘ ◄──────── └──────────┘
         ▲        discover       │
         │        higher term    │ receives
         │                       │ majority
         │        ┌──────────┐   │ votes
         └─────── │  Leader  │ ◄─┘
          higher   └──────────┘
          term     sends heartbeats
```

**Raft Algorithm Steps (simplified):**

1. **All nodes start as Followers** with Term=0.
2. **If Follower times out** → becomes Candidate, increments Term, votes for self, requests votes.
3. **If Candidate gets majority** → becomes Leader, starts sending heartbeats.
4. **If Leader** → sends periodic AppendEntries (heartbeats or real entries) to all followers.
5. **If any node sees a higher Term** → reverts to Follower.

---

## How Your Module Connects to Others

```
                    ┌──────────────────────┐
                    │   Your Module        │
                    │   (Consensus)        │
                    │                       │
                    │  ┌──────┐ ┌────────┐ │
                    │  │ Raft │ │Election│ │
                    │  └──┬───┘ └────────┘ │
                    │     │     ┌────────┐  │
                    │     │     │ State  │  │
                    │     │     └────────┘  │
                    └─────┼────────────────┘
                          │
           ┌──────────────┼──────────────┐
           │              │              │
           ▼              ▼              ▼
    ┌────────────┐ ┌────────────┐ ┌────────────┐
    │Replication │ │ Transport  │ │   Fault    │
    │  (Senul)   │ │  (gRPC)    │ │  (Imansa)  │
    │            │ │            │ │            │
    │ Log.Append │ │ SendVote() │ │ Heartbeat  │
    │ HasQuorum()│ │ SendAE()   │ │ Detection  │
    └────────────┘ └────────────┘ └────────────┘
```

| Your Component   | Connects To         | How                                                   |
| :--------------- | :------------------ | :---------------------------------------------------- |
| RaftNode         | Replication (Senul) | Leader calls `ReplicateEntry()` when proposing        |
| RaftNode         | Transport           | Sends/receives `RequestVote` and `AppendEntries` RPCs |
| RaftNode         | Fault (Imansa)      | Leader heartbeats double as failure detection         |
| RaftNode         | Timesync (Sabeelur) | Ticks Lamport clock on send/receive events            |
| LogEntry (State) | Replication         | LogEntry struct shared with replication log           |

---

## Suggested Implementation Order

1. **`state.go`** — Pure data structures. No logic. Quick to complete.
2. **`election.go`** — Election timeout + vote counting. Moderate complexity.
3. **`raft.go`** — Full state machine. Depends on election + replication. Implement last.

Start with `state.go` (15 min) → `election.go` (1-2 hours) → `raft.go` (2-3 hours).

---

## Unit Test Ideas

```go
// internal/consensus/election_test.go
func TestVoteTracker(t *testing.T) {
    vt := NewVoteTracker(3) // 3 nodes, majority = 2

    vt.RecordVote("node1", true)
    if vt.HasMajority() { t.Error("should not have majority with 1 vote") }

    vt.RecordVote("node2", true)
    if !vt.HasMajority() { t.Error("should have majority with 2 votes") }
}

func TestIsLogUpToDate(t *testing.T) {
    em := NewElectionManager(150, 300)

    // Candidate has term 2, index 5. My log has term 2, index 3.
    // Candidate's log is more up-to-date (same term, higher index).
    if !em.IsLogUpToDate(5, 2, 3, 2) {
        t.Error("candidate with higher index should be up-to-date")
    }

    // Candidate has term 1, index 10. My log has term 2, index 1.
    // My log is more up-to-date (higher term wins regardless of length).
    if em.IsLogUpToDate(10, 1, 1, 2) {
        t.Error("candidate with lower term should NOT be up-to-date")
    }
}

// internal/consensus/raft_test.go
func TestRaftNodeStartsAsFollower(t *testing.T) {
    node := NewRaftNode(config.Default())
    if node.State() != Follower {
        t.Errorf("expected Follower, got %v", node.State())
    }
    if node.CurrentTerm() != 0 {
        t.Errorf("expected term 0, got %d", node.CurrentTerm())
    }
}

func TestHandleRequestVote(t *testing.T) {
    node := NewRaftNode(config.Default())

    req := RequestVoteRequest{
        CandidateID:  "node2",
        Term:         1,
        LastLogIndex: 0,
        LastLogTerm:  0,
    }

    resp := node.HandleRequestVote(req)
    // Should grant vote: higher term, log is up-to-date, haven't voted
    if !resp.VoteGranted {
        t.Error("should grant vote to candidate with higher term")
    }
}
```

---

## Key Distributed Systems Concepts

- **Consensus**: Agreement among distributed nodes on a single value or sequence of values, despite failures.
- **Term**: A logical time period in Raft. Each term has at most one leader. Terms increase monotonically.
- **Leader Election**: The process of choosing one node to coordinate. Raft uses randomized timeouts to avoid split votes.
- **Split Vote**: When two candidates start elections simultaneously and neither gets a majority. Resolved by random timeout restart.
- **Log Matching Property**: If two logs have an entry with the same index and term, all preceding entries are identical. Enforced by `PrevLogIndex`/`PrevLogTerm` checks.
- **Safety**: Raft guarantees that once an entry is committed, no future leader will have a different entry at that index (Election Restriction + Leader Completeness).
- **Liveness**: As long as a majority of nodes are up and can communicate, the system will eventually elect a leader and make progress.

---

## Raft Paper Reference

Your implementation is based on the Raft consensus algorithm (Ongaro & Ousterhout, 2014). Key sections:

- **§5.1** — Leader election
- **§5.2** — Log replication
- **§5.3** — Safety argument
- **§5.4.1** — Election restriction (log up-to-date check)

For the full paper: _"In Search of an Understandable Consensus Algorithm (Extended Version)"_

---

_This is the hardest module. Start with state.go, get election working, then tackle raft.go. Test each piece independently before integrating._
