# Distributed Messaging System - Core Mechanisms Overview

This document provides a high-level overview of the four foundational pillars used in the distributed messaging system. Note that while the original design was purely Raft-inspired, the application was recently refactored to use **Apache ZooKeeper** for its consensus module, while retaining Raft-like behavior for data replication.

## 1. Consensus (ZooKeeper-based Leader Election)

The system uses **Apache ZooKeeper Ephemeral Sequential Znodes** to elect a single Leader and ensure that only one Leader exists at any time.

* **How it works:** When nodes start, they connect to the ZooKeeper ensemble and create an ephemeral, sequential znode under the `/election` path. ZooKeeper assigns a uniquely increasing sequence number to each node.
* **Determining the Leader:** The node that receives the **smallest sequence ID** automatically becomes the Leader.
* **Followers:** The remaining nodes become Followers. Instead of waiting for randomized timeouts (as originally intended via Raft), they place a "watch" on the znode that has the sequence ID immediately preceding theirs. If the leader fails, its ephemeral znode is deleted, triggering the watch on the next node in line to step up as the new Leader automatically.

## 2. Data Replication (Quorum-Based Commit)

The system uses a **Synchronous, Quorum-based Replicated Log** to guarantee strong consistency across the cluster.

* **Write Path:** When a client sends a publish request, it goes directly to the Leader. The Leader appends the message to its in-memory replicated log.
* **Replication via AppendEntries:** The Leader then sends an `AppendEntries` gRPC request containing the new log entry to all the Followers in parallel.
* **Quorum Rule:** A message is only successfully **"committed"** once the Leader receives an acknowledgment from a **majority (`⌊N/2⌋ + 1`)** of nodes. Once committed, the Leader applies the log, notifies the followers to commit it on their next heartbeat, and returns a success response to the client.

## 3. Time Synchronization (Lamport Logical Clocks)

Because physical clocks in distributed systems are inherently unreliable, the system uses **Lamport Logical Clocks** to establish the *causal ordering* (happens-before relationships) of messages.

* **Mechanism:** Every node maintains a monotonically increasing integer called a Lamport Clock.
* **Sending events:** Before a node processes an event or sends a message, it increments its local clock by 1 (`clock = clock + 1`) and attaches this timestamp to the log entry payload.
* **Receiving events:** When a node receives a message with a timestamp `T`, it updates its own clock to be greater than both its current time and the received time: `clock = max(local_clock, T) + 1`. This mathematical rule ensures that if an event A causally affects event B, A will confidently maintain a lower timestamp than B across the entire cluster.

## 4. Fault Tolerance (Heartbeats & Automatic Recovery)

The system is designed to expect hardware or network failures and handles them via **Active Failure Detection and Log Synchronization**.

* **Heartbeat Emitters & Monitors:** The Leader continuously broadcasts periodic heartbeats to all Followers. The Followers run a background failure detector loop that monitors the `lastHeartbeat` timestamp.
* **Failure Detection:** If a Follower stops receiving heartbeats beyond a configurable timeout threshold, the Leader is declared dead. (Coupled with ZooKeeper's session expiration, the leader's ephemeral node is dropped, initiating a clean failover).
* **Recovery sync:** If a node crashes and later comes back online, a Recovery Manager is triggered. It communicates with the current Leader, who sends the rebounding node all the missing log entries (using `AppendEntries` catch-up RPCs) so it can sync its log state before fully resuming duties in the cluster.
