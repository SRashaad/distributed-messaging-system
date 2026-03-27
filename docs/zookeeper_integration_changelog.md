# 📝 ZooKeeper Integration & System Wiring Changelog

This document serves as a complete record of all the changes made to the `distributed-messaging-system` to integrate Apache ZooKeeper and wire the various modules together.

## 1. Consensus Module (`internal/consensus/`)
*The Consensus logic was originally designed around Raft randomized timeouts. It has been entirely refactored to use ZooKeeper Ephemeral Sequential Znodes.*

* **`state.go` & `state_test.go`**: Implemented `NodeState` (Follower, Candidate, Leader) constants, the `String()` method for logging, and the core RPC request/response structs (RequestVote, AppendEntries). Also added comprehensive unit tests.
* **`election.go` & `election_test.go`**: 
  * Removed manual Raft timeout loop.
  * Implemented `EnsureElectionPath()`, `CreateElectionNode()`, and `DetermineRole()` using the `go-zookeeper/zk` package.
  * Node roles are now determined by checking children of `/election`: the node with the smallest sequence ID becomes Leader, while the others watch the znode immediately preceding them.
  * Maintained `IsLogUpToDate` and `VoteTracker` for backwards compatibility or fallback. Unit tests added.
* **`raft.go` & `raft_test.go`**:
  * Rewrote `NewRaftNode` to accept ZooKeeper server addresses.
  * Implemented `Start()` to connect to ZooKeeper via session events.
  * Created `electionLoop()` to process ZK watches for automatic leader election and failover.
  * Implemented `HandleRequestVote` and `HandleAppendEntries` logic.
  * Implemented `ProposeEntry` to handle client publishes. Unit tests added.

## 2. Shared Infrastructure Wiring
*Modules originally built in silos were connected and finalized so the application compiles and runs.*

* **Configuration (`config/config.go` & `internal/config/config.go`)**:
  * Added `ZookeeperServers` string slice array.
  * Implemented `config.Load()` to parse command-line flags (e.g., `--zk "localhost:2181"`).
  * Converted integer milliseconds to `time.Duration` for system-wide timeout variables.
* **Logger (`pkg/logger/logger.go`)**:
  * Implemented a structured `log/slog` wrapper. Every node now logs with an attached `node_id` key for easy filtering when running multiple nodes in the same terminal.
* **Transport Server (`internal/transport/grpc_server.go`)**:
  * Finalized the gRPC wrapper. `Start()` now binds to a TCP port and runs `grpc.Serve` in a non-blocking goroutine so initialization can continue.
  * Implemented `Stop()` to invoke `GracefulStop()`.
* **Transport Client (`internal/transport/grpc_client.go`)**:
  * Implemented the `PeerClient` connection pool using a mutex-protected map.
  * Added `grpc.Dial` with insecure credentials for academic usage.
  * Defined `SendRequestVote` and `SendAppendEntries` stubs.

## 3. Fault Tolerance & Replication Logic
*Completed Imansa's and Senul's placeholder files to ensure cross-compilation success.*

* **Failure Detector (`internal/fault/detector.go`)**:
  * Implemented the background `StartMonitoring` loop. It iterates over a map of peer nodes, checking `time.Since(lastHeartbeat)` against the timeout, and fires callbacks if a peer dies.
* **Heartbeat Emitter (`internal/fault/heartbeat.go`)**:
  * Implemented the leader's tick mechanism to periodically broadcast heartbeats to followers.
* **Node Recovery (`internal/fault/recovery.go`)**:
  * Finished the API interface that triggers log syncs when ZooKeeper detects a node reconnecting after a crash.
* **Replication Fix (`internal/replication/manager.go`)**:
  * **Critical Bug Fix:** Corrected a type-signature mismatch. `manager.go` expected `log.Append()` to return `(uint64, error)`, while `log.go` only returned `error`. Refactored `ReplicateEntry` to use `entry.Index` directly.

## 4. System Aggregation (`internal/node/node.go`)
*The hardest part of any distributed system: wiring it up. The Node wrapper now acts as the system dependency injector.*

* Instantiated `transport.Server`, `PeerClient`, `InMemoryLog`, `Manager`, `LamportClock`, `MessageStore`, `Detector`, `LogRecovery`, and `RaftNode`.
* Correctly populated node dependencies in the `Start()` sequence:
  1. Start gRPC Server
  2. Map Peer Connections
  3. Start ZooKeeper Consensus Module
  4. Start Failure Detector
  5. Wait for Shutdown via Context Cancellation.

## 5. Entry Points & Dependencies
* **Server Application (`cmd/server/main.go`)**:
  * Introduced OS Signal handling. The node now listens for `SIGINT` (Ctrl+C) and `SIGTERM` to invoke `node.Stop()` and gracefully shut down gRPC and ZooKeeper sessions.
* **Client Application (`cmd/client/main.go`)**:
  * Mapped CLI flags (`--leader`, `--action`, `--message`) to allow interactions with the cluster natively from a terminal.
* **Dependencies (`go.mod`)**:
  * Manually injected `github.com/go-zookeeper/zk v1.0.3` into the dependency tree.
