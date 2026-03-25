# Member Documentation: Fault Tolerance Module

**Owner:** Imansa Bodini (IT24101844)  
**Branch:** `feature/fault-tolerance`  
**Package:** `internal/fault/`

---

## Overview

You are responsible for the **Fault Tolerance** module. This module ensures that the system can detect when a node has failed and recover gracefully. Without fault tolerance, a single node crash would bring down the entire messaging system.

Your module provides three key capabilities:

1. **Heartbeat Emission** — The Leader sends periodic heartbeats to all Followers.
2. **Failure Detection** — Followers detect when the Leader has failed (missed heartbeats).
3. **Node Recovery** — When a crashed node comes back, it catches up with missed entries.

---

## Your Files

| File                          | Purpose                                 |
| :---------------------------- | :-------------------------------------- |
| `internal/fault/detector.go`  | Heartbeat-based failure detection logic |
| `internal/fault/heartbeat.go` | Leader heartbeat emitter                |
| `internal/fault/recovery.go`  | Node recovery and log synchronization   |

---

## What You Need to Implement

### 1. `detector.go` — Failure Detector

The `Detector` struct tracks the last heartbeat time from each known node. If a node hasn't sent a heartbeat within the configured timeout, it is declared failed.

**Key functions to implement:**

| Function                  | What It Does                                                                   |
| :------------------------ | :----------------------------------------------------------------------------- |
| `NewDetector(timeout)`    | Creates a detector with the given timeout. Initialize the `lastHeartbeat` map. |
| `StartMonitoring(ctx)`    | Runs a loop checking node liveness every `timeout/2`. Uses `time.NewTicker`.   |
| `ReportHeartbeat(nodeID)` | Records `time.Now()` as the last heartbeat for the given node.                 |
| `IsAlive(nodeID)`         | Returns `true` if `time.Since(lastHeartbeat[nodeID]) < timeout`.               |
| `OnFailure(callback)`     | Registers a callback function to call when a node fails.                       |
| `checkNodes()`            | Iterates over all tracked nodes, calls failure callbacks for expired ones.     |

**Important:** When the Leader fails, your failure detector invokes a callback that triggers a new election in the consensus module. This is how your module connects to Vimukthi's consensus module.

### 2. `heartbeat.go` — Heartbeat Emitter

The `HeartbeatEmitter` runs on the Leader node only. It sends an empty `AppendEntries` RPC to all followers at a regular interval.

**Key functions to implement:**

| Function                                  | What It Does                                                                           |
| :---------------------------------------- | :------------------------------------------------------------------------------------- |
| `NewHeartbeatEmitter(interval, sendFunc)` | Creates an emitter. `sendFunc` is provided by the transport layer.                     |
| `Start(ctx)`                              | Runs a ticker loop calling `sendFunc()` every `interval`. Stops when ctx is cancelled. |

**Important:** When a node becomes Leader (handled by Vimukthi's consensus module), it starts the HeartbeatEmitter. When it steps down, it cancels the context to stop heartbeats.

### 3. `recovery.go` — Recovery Manager

When a previously crashed node comes back online, the Leader must send it all the entries it missed. This is log synchronization.

**Key functions to implement:**

| Function                     | What It Does                                                                      |
| :--------------------------- | :-------------------------------------------------------------------------------- |
| `NewLogRecovery()`           | Creates a recovery manager. Needs references to the replicated log and transport. |
| `InitiateRecovery(nodeID)`   | Queries the recovering node for its last index, determines missing entries.       |
| `SyncLog(nodeID, fromIndex)` | Sends missing entries via `AppendEntries` RPCs.                                   |

---

## How Your Module Connects to Others

```
┌──────────────────────────────────────────────────────────┐
│                    Your Module (Fault)                     │
│                                                           │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │  Detector    │  │  Heartbeat   │  │  Recovery    │    │
│  │  (detector)  │  │  (heartbeat) │  │  (recovery)  │    │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘    │
│         │                 │                  │            │
└─────────┼─────────────────┼──────────────────┼────────────┘
          │                 │                  │
          ▼                 ▼                  ▼
   ┌────────────┐    ┌────────────┐    ┌─────────────┐
   │ Consensus  │    │ Transport  │    │ Replication  │
   │ (Vimukthi) │    │  (Shared)  │    │  (Senul)     │
   └────────────┘    └────────────┘    └─────────────┘
```

| Your Component       | Connects To | How                                                |
| :------------------- | :---------- | :------------------------------------------------- |
| Detector.OnFailure() | Consensus   | Callback triggers new election when Leader fails   |
| HeartbeatEmitter     | Transport   | Uses transport to broadcast empty AppendEntries    |
| Recovery             | Replication | Reads leader's log to find missing entries         |
| Recovery             | Transport   | Sends entries to recovering node via AppendEntries |

---

## Suggested Implementation Order

1. **`detector.go`** — Start with `NewDetector`, `ReportHeartbeat`, `IsAlive` (simplest).
2. **`detector.go`** — Then implement `StartMonitoring` and `checkNodes`.
3. **`heartbeat.go`** — Simple ticker loop.
4. **`recovery.go`** — Depends on replication and transport being ready.

---

## Unit Test Ideas

```go
// internal/fault/detector_test.go
func TestDetectorReportHeartbeat(t *testing.T) {
    d := NewDetector(100 * time.Millisecond)
    d.ReportHeartbeat("node1")
    if !d.IsAlive("node1") {
        t.Error("node1 should be alive after heartbeat")
    }
}

func TestDetectorTimeout(t *testing.T) {
    d := NewDetector(50 * time.Millisecond)
    d.ReportHeartbeat("node1")
    time.Sleep(100 * time.Millisecond)
    if d.IsAlive("node1") {
        t.Error("node1 should be dead after timeout")
    }
}

func TestDetectorOnFailure(t *testing.T) {
    d := NewDetector(50 * time.Millisecond)
    failed := ""
    d.OnFailure(func(nodeID string) { failed = nodeID })
    d.ReportHeartbeat("node1")
    time.Sleep(100 * time.Millisecond)
    d.checkNodes()
    if failed != "node1" {
        t.Errorf("expected failure callback for node1, got %q", failed)
    }
}
```

---

## Key Distributed Systems Concepts

- **Heartbeat**: A periodic "I'm alive" message from the Leader.
- **Election Timeout**: How long a Follower waits before assuming the Leader has failed. Must be longer than the heartbeat interval.
- **Failure Detector**: An unreliable component that suspects node failures. In an asynchronous system, perfect failure detection is impossible (FLP impossibility), but heartbeat timeouts work well in practice.
- **Recovery**: Re-integrating a node after a crash. The recovered node must catch up with all entries it missed.

---

_Start simple. Get the detector working with basic heartbeat tracking, then build up to monitoring and recovery._
