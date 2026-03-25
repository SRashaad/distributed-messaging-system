// =============================================================================
// Module: Time Synchronization
// File: lamport.go
// Responsible Member: Sabeelur Rashaad — Time Synchronization
// Purpose: Implements the Lamport logical clock, which provides a way to
//          order events across distributed nodes without relying on
//          synchronized physical clocks.
//
// Connections:
//   - Used by internal/consensus/raft.go to timestamp log entries (Tick).
//   - Used by internal/consensus/raft.go to update the clock when receiving
//     AppendEntries or RequestVote RPCs (Update).
//   - Used by internal/replication/manager.go to assign timestamps to entries.
//   - The Lamport timestamp is stored in every consensus.LogEntry.Timestamp.
//
// Lamport clock rules (from Lamport, 1978):
//   Rule 1: Before each local event → clock = clock + 1
//   Rule 2: Before sending a message  → clock = clock + 1, attach clock to message
//   Rule 3: On receiving message with timestamp t → clock = max(clock, t) + 1
//
// Important:
//   - Lamport clocks capture *potential* causality, not true causality.
//   - If a.Timestamp < b.Timestamp, a *may* have caused b.
//   - If a caused b, then a.Timestamp < b.Timestamp (guaranteed).
//   - All operations must be thread-safe (use sync.Mutex).
// =============================================================================
package timesync

import "sync"

// LamportClock defines the interface for a Lamport logical clock.
type LamportClock interface {
	// Tick increments the clock by 1 and returns the new value.
	// Called before each local event and before sending a message (Rules 1 & 2).
	Tick() uint64

	// Update synchronizes the clock with a received remote timestamp.
	// Sets clock = max(local, received) + 1 (Rule 3).
	Update(received uint64) uint64

	// Current returns the current clock value without modifying it.
	Current() uint64
}

// Clock implements LamportClock with thread-safe operations.
type Clock struct {
	mu    sync.Mutex // protects the value field
	value uint64     // current logical clock value
}

// NewClock creates a new Lamport clock initialized to 0.
//
// TODO: Return a new Clock with value = 0.
func NewClock() *Clock {
	return nil
}

// Tick increments the clock by 1 and returns the new value.
// Called before each local event and before sending a message.
//
// Implements Lamport Rule 1 and Rule 2:
//   clock = clock + 1
//
// TODO: Lock, increment c.value, return the new value.
func (c *Clock) Tick() uint64 {
	return 0
}

// Update synchronizes the clock with a received remote timestamp.
// Sets the clock to max(local, received) + 1.
// Called upon receiving a message that carries a Lamport timestamp.
//
// Implements Lamport Rule 3:
//   clock = max(clock, received) + 1
//
// TODO: Lock, compare received with c.value, set to max, increment, return.
func (c *Clock) Update(received uint64) uint64 {
	return 0
}

// Current returns the current clock value without modifying it.
// Used for read-only access (e.g., logging, debugging).
//
// TODO: Lock and return c.value.
func (c *Clock) Current() uint64 {
	return 0
}
