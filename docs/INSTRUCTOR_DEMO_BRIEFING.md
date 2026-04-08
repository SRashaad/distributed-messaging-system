# Distributed Messaging System: Instructor Demo Briefing

Date: April 8, 2026

## 1. Why this document exists

This is your complete demo script and understanding guide.
It is designed for two goals:

1. Explain the system in very simple language so you sound confident.
2. Explain the technical details accurately so you can answer deeper questions.

It also clearly separates:

1. What we actually built.
2. What is missing compared to stronger MIT-style distributed systems expectations and production systems.

---

## 2. 60-second simple introduction (say this first)

Our project is a fault-tolerant distributed messaging system built in Go.
It runs as a cluster of nodes. One node is the leader, and the others are followers.

Clients send messages to the leader. The leader copies each message to followers.
When most nodes agree they received it, we treat that message as committed.

If the leader crashes, the system elects a new leader automatically using ZooKeeper.
We also use Lamport logical clocks to keep message ordering consistent across nodes even when network delays happen.

So in one sentence: we built a leader-based, quorum-replicated messaging cluster with automatic failover and logical-time ordering.

---

## 3. What usually happens in our system (simple flow)

### 3.1 Cluster startup

1. Start ZooKeeper.
2. Start 3 nodes.
3. Nodes create election znodes in ZooKeeper.
4. The node with highest election priority becomes leader.
5. Other nodes stay followers.

### 3.2 Publish message

1. Client sends `Publish` request.
2. If request hits follower, follower returns "not leader" and indicates leader identity.
3. Leader appends the message to its local log.
4. Leader sends `AppendEntries` RPC to followers.
5. Once quorum ACK is reached, leader commits and applies the message.
6. Client receives success response.

### 3.3 Consume message

1. Client requests committed messages from an index.
2. Node returns stored committed data.
3. Results are sorted by Lamport timestamp (and index tie-breaker).

### 3.4 Failure and recovery

1. If leader stops, ZooKeeper session drops.
2. Election znode disappears.
3. Remaining nodes detect change and new leader emerges.
4. Clients continue by targeting the new leader.

---

## 4. What each module does (simple language)

### 4.1 Fault tolerance

- Heartbeats and failure detection logic exist.
- ZooKeeper handles practical leader-failover trigger through ephemeral node/session behavior.
- Recovery scaffolding exists for syncing a recovered node.

### 4.2 Data replication and consistency

- Leader writes first.
- Followers receive `AppendEntries` replication calls.
- Quorum (majority) controls whether write is considered committed.

### 4.3 Consensus

- Leader election is ZooKeeper-based (not pure Raft election loop in practice).
- Replication model is Raft-inspired (term/index/AppendEntries style).

### 4.4 Time synchronization

- Uses Lamport logical clocks (causal/logical ordering).
- Not NTP clock sync.
- Good for event order, not physical time accuracy.

---

## 5. Technical deep dive (accurate to this codebase)

## 5.1 Architecture and control plane

- Node orchestration is in `internal/node/node.go`.
- Each node initializes transport, consensus, replication manager, detector, clock, and message store.
- ZooKeeper-backed consensus node is created by `consensus.NewRaftNode(...)`.

Important point for your instructor:

- Election mechanism is ZooKeeper ephemeral-sequential znode ordering.
- Replication semantics are Raft-like, but not full Raft safety completeness yet.

## 5.2 Leader election path

- `internal/consensus/raft.go` runs election loop against ZooKeeper.
- Node creates election znode under election path.
- Role is decided by znode order.
- Followers set watches on predecessor znode and re-evaluate when events happen.
- On leadership transition, term/state are updated.

## 5.3 Write path details

Entry point:

- gRPC `Publish` in transport handler calls `Node.PublishMessage(...)`.

Inside `PublishMessage`:

1. If node is not leader: returns redirect-style error.
2. Dedup check by payload against current in-memory store.
3. Lamport clock `Tick()` creates logical timestamp.
4. Create log entry with index/term/timestamp/data.
5. Replication manager appends locally and sends `AppendEntries` RPCs to peers.
6. Waits for quorum with timeout.
7. On quorum success, applies to message store.

## 5.4 Read path details

- `Consume` reads all entries from message store.
- Filters by `from_index`.
- Sorts by timestamp then index.

Strength:

- Deterministic logical ordering for consumers.

Current caveat:

- Read path does not enforce a leader-lease or read-index barrier for strict linearizable reads after partitions.

## 5.5 Replication mechanics

`internal/replication/manager.go` behavior:

1. Append locally.
2. Leader counts self ACK.
3. Fire goroutines to all peers sending `AppendEntries`.
4. Record follower ACKs when `resp.success` is true.
5. `WaitForQuorum` loops until majority or timeout.

Correctness baseline:

- Majority ACK gating exists.

Gap:

- Quorum waiting currently uses a polling loop + `time.Sleep(10ms)` instead of condition-variable signaling.

## 5.6 Durability and persistence

`internal/storage/log_store.go` exists and supports:

- append entries to JSONL
- range reads
- truncate-after
- save/load term and vote

Current durability gap:

- Append path does not call `file.Sync()` before ACK stage in critical path.
- Truncate strategy removes and rewrites file, which is not atomic and can be risky during crash.

## 5.7 Snapshot and compaction

- Snapshot abstractions are defined.
- File snapshot manager methods are TODO stubs.

Impact:

- Log compaction is not complete.
- Large or long-running clusters can suffer unbounded log growth.

## 5.8 Protocol-level considerations

`internal/transport/proto/messaging.proto` currently has:

- `RequestVote`
- `AppendEntries`
- `Publish`
- `Consume`
- `GetStatus`

Missing for full Raft maturity:

- `InstallSnapshot` RPC for lagging followers.
- Fast-backup metadata in append response (`XTerm`, `XIndex`, etc.) for efficient conflict repair.

---

## 6. What is missing compared to MIT-style stronger guarantees

This section is your "honest maturity map".

## 6.1 Concurrency signaling quality

Current:

- Quorum wait uses polling sleep loop.

Expected stronger pattern:

- `sync.Cond` or event-driven signaling, wake up instantly on ACK.

Why it matters:

- Lower latency, lower CPU waste, cleaner correctness behavior under load.

## 6.2 Divergent log conflict handling depth

Current:

- `HandleAppendEntries` includes placeholder consistency checks and currently accepts simplified behavior.

Expected stronger pattern:

- Strict `prevLogIndex/prevLogTerm` validation.
- Fast backup conflict hints in response to skip conflicting terms efficiently.

Why it matters:

- Better safety under crash/restart/divergent-history scenarios.

## 6.3 Crash durability guarantees

Current:

- Persistence interfaces exist, but hard durability contract is not fully enforced in replication critical path.

Expected stronger pattern:

- Write-ahead durability with explicit fsync before commit acknowledgment.
- Atomic update strategies for truncation/compaction.

Why it matters:

- Power loss should not silently violate committed-log assumptions.

## 6.4 Log compaction and catch-up scalability

Current:

- Snapshot manager not implemented.
- No install-snapshot RPC.

Expected stronger pattern:

- Periodic snapshotting.
- Snapshot transfer to very-lagging followers.

Why it matters:

- Long-running systems need bounded storage and efficient recovery.

## 6.5 Read linearizability defense

Current:

- Reads are sorted and deterministic, but leader freshness verification is not enforced on each read path.

Expected stronger pattern:

- Read-index / heartbeat-confirmed leader validity before serving strict reads.

Why it matters:

- Prevent stale reads if a partitioned old leader serves data.

## 6.6 Testing and verification maturity

Current:

- Test files exist, but Makefile is minimal and does not include race-detector test target.

Expected stronger pattern:

- CI-friendly test targets with `go test -race ./...`.
- Broader partition/crash/property tests.

---

## 7. Instructor demo plan (recommended sequence)

Use this exact order to look professional and clear.

## 7.1 Demo structure (10-15 minutes)

1. System goal (1 minute)
2. Cluster startup and status (2 minutes)
3. Publish + quorum replication (2 minutes)
4. Leader failure + failover (2 minutes)
5. Consume + ordering by Lamport time (2 minutes)
6. Honest limitations + roadmap (3-5 minutes)

## 7.2 Step-by-step live flow

1. Start ZooKeeper and mention election dependency.
2. Start 3 nodes and identify leader in logs/status.
3. Publish a few messages (including duplicate payload case).
4. Show commit success only after quorum.
5. Stop current leader process.
6. Show new leader election.
7. Publish again to prove continuity.
8. Consume and explain timestamp + index ordering.
9. Close with "what is missing" and planned upgrades.

## 7.3 What to say while demoing each step

At startup:

- "One node becomes leader through ZooKeeper ephemeral-sequential election. Others watch for changes."

At publish:

- "Leader appends, replicates, waits for majority ACK, then commits and responds."

At failover:

- "When leader dies, its ZooKeeper session ends, znode disappears, and followers re-elect."

At consume:

- "We return committed entries and sort by Lamport logical time for deterministic distributed ordering."

At limitations slide:

- "This is a strong foundation, but we still need strict divergence checks, snapshot RPC, fsync-first durability, and linearizable read barriers to match production-grade systems."

---

## 8. "What we did" vs "What top systems do" (clear comparison)

## 8.1 What we did well

1. Built integrated multi-module distributed prototype end-to-end.
2. Achieved practical failover using ZooKeeper leader election.
3. Implemented quorum-gated write path.
4. Added Lamport-based event ordering for distributed consistency semantics.
5. Added dashboard/status visibility and a runnable local cluster workflow.

## 8.2 What top production systems additionally do

1. Complete Raft safety semantics including robust divergent-log repair.
2. Persistent WAL with strict fsync discipline and atomic file operations.
3. Snapshot + compaction + InstallSnapshot for scale.
4. Read-index/lease-based linearizable reads.
5. Stronger observability, chaos testing, formal/verifiable invariants.
6. Security hardening (authn/authz, TLS, cert rotation, audit trails).

---

## 9. If instructor asks difficult questions

Use these short, honest answers.

Question: "Is this full Raft?"

- "It is Raft-inspired replication with ZooKeeper-based election. Full Raft edge-case completeness is a planned next step."

Question: "Do committed writes survive power loss right now?"

- "We have persistence abstractions, but strict fsync-backed durability is not fully enforced end-to-end in current replication path."

Question: "How do you handle huge log growth?"

- "Snapshot interfaces are present, but full snapshot manager and InstallSnapshot RPC are still pending."

Question: "Are reads strictly linearizable?"

- "Not fully guaranteed yet; we need read-index or heartbeat-confirmed leader freshness before serving strict reads."

Question: "Then what is the contribution value?"

- "We delivered a functioning distributed cluster with real failover, quorum write logic, logical-time ordering, and modular architecture ready for advanced consistency upgrades."

---

## 10. Technical upgrade roadmap you can present confidently

## 10.1 Immediate (high impact, low-medium effort)

1. Replace quorum polling with `sync.Cond` signaling.
2. Add Makefile test targets including race detector.
3. Harden append consistency checks (`prevLogIndex/prevLogTerm`) in live path.

## 10.2 Next (core correctness maturity)

1. Extend append response with conflict metadata.
2. Implement fast backup rollback logic.
3. Enforce fsync-based durability before ACK.

## 10.3 After that (scalability maturity)

1. Implement snapshot manager fully.
2. Add `InstallSnapshot` RPC and recovery flow.
3. Add benchmark and chaos test scenarios.

---

## 11. Final one-minute closing statement

Our system demonstrates the core distributed systems pillars in a working prototype:

- leader-based coordination,
- quorum replication,
- fault-aware failover,
- and logical-time message ordering.

We are transparent about current gaps versus top-tier systems: strict log-divergence handling depth, durable fsync guarantees, snapshot transfer, and strict linearizable reads.

That honesty is important: we did not just build features; we also mapped exactly what remains to reach production-grade distributed correctness.

---

## 12. Optional quick command checklist (for your rehearsal)

1. Start ZooKeeper on `localhost:2181`.
2. Start cluster nodes using your script or individual server commands.
3. Optionally run dashboard and CLI publish/consume actions.
4. Kill leader terminal once to demonstrate failover.
5. Publish again after failover.
6. Consume and explain ordering.

Tip for presentation quality:

- Rehearse this sequence once with a timer and keep one terminal focused on leader logs only.