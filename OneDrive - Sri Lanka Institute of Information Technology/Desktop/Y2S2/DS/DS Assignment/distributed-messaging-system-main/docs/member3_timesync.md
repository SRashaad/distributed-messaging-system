# Member Documentation: Time Synchronization & Storage Module

**Owner:** Sabeelur Rashaad (IT24100686)  
**Branch:** `feature/timesync`  
**Packages:** `internal/timesync/` and `internal/storage/`

---

## Overview

You are responsible for **two modules**:

1. **Time Synchronization** (`internal/timesync/`) — Implements Lamport Logical Clocks to establish causal ordering of events across all nodes without relying on synchronized physical clocks.
2. **Storage** (`internal/storage/`) — Provides the state machine that applies committed log entries and serves reads.

These two modules work together: every event gets a Lamport timestamp, and committed entries are applied to the persistent store in timestamp order.

---

## Your Files

### Time Synchronization (`internal/timesync/`)

| File                           | Purpose                                           |
| :----------------------------- | :------------------------------------------------ |
| `internal/timesync/lamport.go` | Lamport logical clock implementation              |
| `internal/timesync/event.go`   | Event data structure with type and timestamp      |
| `internal/timesync/orderer.go` | Causal event ordering and happens-before checking |

### Storage (`internal/storage/`)

| File                            | Purpose                                                        |
| :------------------------------ | :------------------------------------------------------------- |
| `internal/storage/store.go`     | In-memory key-value message store (receives committed entries) |
| `internal/storage/log_store.go` | Persistent log storage interface (optional / advanced)         |
| `internal/storage/snapshot.go`  | Snapshot creation and loading (optional / advanced)            |

---

## What You Need to Implement

### Part A: Time Synchronization

#### 1. `lamport.go` — Lamport Logical Clock

The Lamport clock is a single monotonically increasing counter. It follows **three rules** from Leslie Lamport's 1978 paper:

**Key functions to implement:**

| Function           | What It Does                                      | Lamport Rule                                       |
| :----------------- | :------------------------------------------------ | :------------------------------------------------- |
| `NewClock(nodeID)` | Creates a clock starting at 0 for the given node. | —                                                  |
| `Tick()`           | Increments counter by 1, returns new value.       | **Rule 1**: Before each internal event, increment. |
| `Update(received)` | Sets counter to `max(local, received) + 1`.       | **Rule 2**: On receiving, take max and increment.  |
| `Current()`        | Returns current counter value (read-only).        | —                                                  |

**Lamport Clock Rules (memorize these):**

- **Rule 1 — On send or internal event:** `counter = counter + 1`
- **Rule 2 — On receive:** `counter = max(counter, received) + 1`
- **Guarantee:** If event A happens before event B, then `clock(A) < clock(B)`. But NOT the converse!

Use `sync.Mutex` — the clock is accessed from multiple goroutines.

#### 2. `event.go` — Event

A simple struct representing something that happened in the system, tagged with a Lamport timestamp.

**Key functions to implement:**

| Function   | What It Does                                                        |
| :--------- | :------------------------------------------------------------------ |
| `String()` | Returns a human-readable representation like `"[SEND] nodeA@ts=5"`. |

**Event types you define:**

- `EventSend` — This node sent a message
- `EventReceive` — This node received a message
- `EventInternal` — A local-only event (e.g. commit)

#### 3. `orderer.go` — Event Orderer

The orderer takes a collection of events and sorts them into a total causal order. It also provides the `HappensBefore` check.

**Key functions to implement:**

| Function              | What It Does                                                                  |
| :-------------------- | :---------------------------------------------------------------------------- |
| `NewOrderer()`        | Creates an orderer instance.                                                  |
| `OrderEvents(events)` | Sorts events by Lamport timestamp. Ties broken by NodeID (gives total order). |
| `HappensBefore(a, b)` | Returns true if `a.Timestamp < b.Timestamp`.                                  |

**Important:** Two events with the same timestamp are concurrent (neither happened before the other). The tie-break by NodeID is arbitrary but deterministic.

---

### Part B: Storage

#### 4. `store.go` — Message Store

The state machine of the system. When a log entry is committed (quorum achieved), it gets **applied** to the store.

**Key functions to implement:**

| Function                     | What It Does                                                      |
| :--------------------------- | :---------------------------------------------------------------- |
| `NewInMemoryStore()`         | Creates an empty store. Initialize the messages map.              |
| `Apply(entry)`               | Deserializes entry data, stores the message. Updates lastApplied. |
| `Get(id)`                    | Retrieves a message by ID.                                        |
| `GetRange(startIdx, endIdx)` | Returns messages applied from log entries in an index range.      |
| `LastApplied()`              | Returns the index of the last applied entry.                      |

**Important:** `Apply()` is called in order — entry 1, then 2, then 3. Never skip entries. Use `sync.RWMutex` for thread safety.

#### 5. `log_store.go` — Persistent Log Storage (Optional)

Saves log entries to disk so a node can recover after a crash.

| Function                    | What It Does                                               |
| :-------------------------- | :--------------------------------------------------------- |
| `NewFileLogStore(dir)`      | Opens/creates a file for storing entries.                  |
| `AppendEntries(entries)`    | Writes entries to the file.                                |
| `GetEntries(from, to)`      | Reads entries from the file.                               |
| `TruncateAfter(index)`      | Removes entries after the given index (for log conflicts). |
| `SaveState(term, votedFor)` | Persists Raft hard state (current term, voted-for).        |
| `LoadState()`               | Loads previously saved state on startup.                   |

This is optional for the initial prototype. Mark as stretch goal.

#### 6. `snapshot.go` — Snapshot Manager (Optional)

Creates point-in-time snapshots of the store to compact the log.

| Function                                     | What It Does                                  |
| :------------------------------------------- | :-------------------------------------------- |
| `CreateSnapshot(store, lastIndex, lastTerm)` | Serializes the store and saves with metadata. |
| `LoadSnapshot()`                             | Reads the latest snapshot from disk.          |
| `HasSnapshot()`                              | Checks if a snapshot file exists.             |

This is optional. Mark as stretch goal.

---

## How Your Modules Connect to Others

```
     ┌──────────────────────────────────────────┐
     │              Your Modules                 │
     │                                           │
     │  ┌──────────────────┐  ┌───────────────┐ │
     │  │   Time Sync      │  │   Storage     │ │
     │  │  ┌────────┐      │  │  ┌─────────┐ │ │
     │  │  │ Clock  │      │  │  │  Store   │ │ │
     │  │  ├────────┤      │  │  ├─────────┤ │ │
     │  │  │ Event  │      │  │  │LogStore │ │ │
     │  │  ├────────┤      │  │  ├─────────┤ │ │
     │  │  │Orderer │      │  │  │Snapshot │ │ │
     │  │  └────────┘      │  │  └─────────┘ │ │
     │  └────────┬─────────┘  └───────┬───────┘ │
     └───────────┼────────────────────┼──────────┘
                 │                    │
        ┌────────┴──────┐    ┌───────┴──────────┐
        │  Replication  │    │   Consensus      │
        │  (Senul)      │    │   (Vimukthi)     │
        │ Each entry    │    │ After commit,    │
        │ gets stamped  │    │ entries applied  │
        └───────────────┘    └──────────────────┘
```

| Your Component | Connects To             | How                                                  |
| :------------- | :---------------------- | :--------------------------------------------------- |
| Lamport Clock  | Transport               | Tick() on send, Update() on receive                  |
| Lamport Clock  | Replication (Senul)     | LogEntry includes Lamport timestamp                  |
| Event/Orderer  | Consensus (Vimukthi)    | Events ordered causally for debugging/delivery       |
| Store          | Replication (Senul)     | After commit, Apply() is called with committed entry |
| Store          | Node (Integration)      | Node reads from store to serve ConsumeMessages       |
| LogStore       | Fault Recovery (Imansa) | Recovery loads log from disk after crash             |

---

## Suggested Implementation Order

### Time Sync (implement first):

1. **`lamport.go`** — 3 functions, no dependencies. Start here.
2. **`event.go`** — Just a struct and String(). Very quick.
3. **`orderer.go`** — Sort and compare. Depends on Event types.

### Storage (implement second):

4. **`store.go`** — In-memory map with Apply(). Core functionality.
5. **`log_store.go`** — File I/O (stretch goal).
6. **`snapshot.go`** — Serialization (stretch goal).

---

## Unit Test Ideas

```go
// internal/timesync/lamport_test.go
func TestLamportTick(t *testing.T) {
    clock := NewClock("node1")
    ts1 := clock.Tick() // 1
    ts2 := clock.Tick() // 2
    if ts2 <= ts1 { t.Error("Tick should monotonically increase") }
}

func TestLamportUpdate(t *testing.T) {
    clock := NewClock("node1")
    clock.Tick() // local = 1
    clock.Update(5) // max(1, 5) + 1 = 6
    if clock.Current() != 6 { t.Errorf("expected 6, got %d", clock.Current()) }
}

// internal/timesync/orderer_test.go
func TestOrderEvents(t *testing.T) {
    orderer := NewOrderer()
    events := []Event{
        {Timestamp: 3, NodeID: "A", Type: EventSend},
        {Timestamp: 1, NodeID: "B", Type: EventReceive},
        {Timestamp: 2, NodeID: "A", Type: EventInternal},
    }
    sorted := orderer.OrderEvents(events)
    if sorted[0].Timestamp != 1 { t.Error("first event should have lowest timestamp") }
}

// internal/storage/store_test.go
func TestStoreApplyAndGet(t *testing.T) {
    store := NewInMemoryStore()
    entry := consensus.LogEntry{Index: 1, Term: 1, Data: []byte("msg1")}
    if err := store.Apply(entry); err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if store.LastApplied() != 1 { t.Error("lastApplied should be 1") }
}
```

---

## Key Distributed Systems Concepts

- **Lamport Logical Clock**: A counter-based mechanism for ordering events without synchronized clocks. Guarantees: if A → B (A causally precedes B), then `C(A) < C(B)`. Does NOT guarantee the converse.
- **Happens-Before (→)**: The fundamental causal ordering relation. `A → B` if A and B are on the same node and A occurred first, or if A is a send and B is the corresponding receive.
- **Concurrent Events**: Two events are concurrent (A ‖ B) if neither happens-before the other. Lamport clocks cannot distinguish concurrent events from causally related ones.
- **Total Order**: Lamport timestamp + node ID gives a total order (breaks ties deterministically).
- **State Machine Replication (SMR)**: Every node applies the same committed entries in the same order, resulting in identical state. Your Store is the state machine.
- **Snapshot**: A compressed representation of the state machine at a point in time. Used to compact the log — entries before the snapshot can be discarded.

---

_Start with Lamport clock — it's self-contained and only ~20 lines of real logic. Then build outward._
