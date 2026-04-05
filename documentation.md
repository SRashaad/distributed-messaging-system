# Distributed Messaging System — Enhancements & Implementations

This document serves as a comprehensive record of the final system integrations performed to complete the academic requirements for the Distributed Messaging System. It covers the issues identified, exactly how they were addressed, how to test the implementations, and thoughts on future architecture improvements.

## 🛠️ Issues Identified & Fixes Implemented

### 1. Fault Tolerance (Member 1)
**Issues Identified:**
* The mechanism to natively sync data to a crashed/re-joined node was completely mocked (`time.Sleep` placeholders in `internal/fault/recovery.go`).
* Client redirect mechanism was missing. Followers just swallowed messages or didn't provide enough information for clients.

**Fixes Applied:**
* **Log Recovery:** Filled in `recovery.go`'s `InitiateRecovery` and `SyncLog` methods. They now securely grab local `ReplicatedLog` missing entries, bundle them into Protocol Buffer `AppendEntriesRequest` arrays, and beam them down physically through gRPC networks (`transport.PeerClient`).
* **Client Redirections:** Altered `node.go`'s `PublishMessage`. Now, if a client tries publishing data onto a Follower, the Follower dynamically checks the internal ZooKeeper session for the true Leader and throws a strict "redirect to X" error. Rewrote the CLI (`cmd/client/main.go`) to inherently catch these proxy-errors, auto-reconnect, and transmit to the new leader immediately. 

### 2. Data Replication and Consistency (Member 2)
**Issues Identified:**
* The network was completely non-existent! The leader was just logging items and pretending network communication was happening.
* No message deduplication logic existed; duplicate network resends or user-spams flooded the application.
* The Leader was committing changes and telling clients "SUCCESS" before actually calculating whether a Quorum of nodes securely received it.

**Fixes Applied:**
* **Live Appends:** Replaced the mocks in `internal/replication/manager.go`. Now `ReplicateEntry` genuinely calls `client.SendAppendEntries(...)` utilizing Go routines for parallel broadcasting. Follower nodes securely decrypt the grpc requests via the newly established `internal/transport/consensus_handler.go` logic, actively appending it into their own `storage.MessageStore`.
* **Quorum Checks:** Integrated `repManager.WaitForQuorum(...)` into the main application logic so that publishing strictly halts and fails gracefully on timeouts if majority followers do not ACK.
* **Deduplication:** Dropped a safety payload scanner into the publishing pipeline in `node.go`. Prior to wasting Quorum resources on identical data, the server caches existing message histories; if duplicates are detected internally, it cleanly aborts replication while validating the user's initial state.

### 3. Time Synchronization (Member 3)
**Issues Identified:**
* Although Lamport logical ticks were present on `Tick()`, the system blindly ignored timestamps hitting follower nodes. 
* Concurrency tracking didn't actively resolve causal collisions when ordering retrieval.

**Fixes Applied:**
* **Clock Updates:** Implemented `OnAppendEntriesRecv` in `node.go` allowing follower nodes to constantly adjust their local lamport clock to `max(system_clock, leader_message_clock)` as per standard distributed architecture protocols (`n.clock.Update(entry.Timestamp)`).
* **Sorting Resolution**: Swapped generic integer indexing inside the client `Consume` endpoint (`messaging_handler.go`). Instead of returning an unordered map hash or Raft indexing hash, all clients now pull their message payloads precisely mapped causally via the `Timestamp` properties, yielding exactly perfectly ordered messaging environments entirely divorced from drifting global wall-clocks.



---

## 🧪 Testing the Modules

**To begin:**
1. Start ZooKeeper via `bin/zkServer.cmd` / `zkServer.sh start`.
2. Run `sh scripts/start_cluster.sh` or spin up the 3 terminals identically referencing the `running_the_cluster_guide.md`.

### Testing Fault Tolerance:
1. Publish a message to the Leader port (e.g. `5001`).
2. Force-kill the Leader node terminal (`Ctrl+C`).
3. View the logs on the remaining terminals — they will successfully re-hold elections and secure a new Leader instantly due to ephemeral znode timeout.
4. Try using the CLI app to publish tracking the offline Leader. Watch as your request is **Automatically Failed-over/Redirected** completely seamlessly to the actual new leader the cluster dynamically chose.
5. Re-spin up the dead node using its original boot command. The new leader will execute the **Log Recovery** functions and physically beam all missing chats dynamically.

### Testing Log Replication / Consistency / Deduplication:
1. Publish a standard message from the `cmd/client` app: `--action publish --message "Hello Testing"`.
2. Ensure you see `waiting for quorum` log traces.
3. Immediately re-run the exact same `--message "Hello Testing"` CLI script. The terminal will explicitly yell `duplicate message detected, avoiding replication` and silently process the return without creating redundant indices! 
4. Verify by running the `--action consume` on **ANY subset follower node** explicitly! Because grpc correctly mirrors the log, your message exists successfully on all nodes locally!

### Testing Time Synchronization:
1. Turn off Node 3.
2. Publish `Message A` to the active Leader.
3. Start Node 3 (It syncs up the Lamport clock to greater variables!)
4. Publish `Message B` incredibly fast from a concurrent bash script to multiple node ports trying to induce race conditions.
5. Check `--action consume` output from a client. You will explicitly see `Time:` indices listed gracefully mapped by Time then index, resolving chaotic parallel processing flawlessly!


---

## 🔮 Future Enhancements 

1. **Snapshots & Log Compaction:** Our `MessageStore` currently infinitely scales based on map sizing logic. In a proper system, Raft state machines perform log compaction by saving `snapshots` globally cutting overhead byte costs immensely.
2. **Automated Discovery Engine:** Statically providing `localhost:5001,localhost:5002` to configurations limits elastic cloud availability. Integrating a true Service Discovery layer (maybe within the Zookeeper directories) will assist containerization immensely.
3. **Advanced Bi-Directional Streaming:** Upgrading grpc's `Consume` loop from strict `rpc` to `stream rpc` guarantees users dynamically push-notified messages instantly without looping `Consume` scripts locally. 
