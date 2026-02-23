// =============================================================================
// Module: Time Synchronization
// File: orderer.go
// Responsible Member: Sabeelur Rashaad — Time Synchronization
// Purpose: Provides causal ordering of events using Lamport timestamps.
//          The EventOrderer sorts events into a total order and can
//          determine if one event potentially happened before another.
//
// Connections:
//   - Uses Event and EventType from event.go.
//   - Can be used by any module that needs to order events across nodes
//     (e.g., displaying messages in causal order to a client).
//   - Used by internal/node/node.go for debugging and log analysis.
//
// Key concepts:
//   - Total order: events are sorted by Lamport timestamp; ties are broken
//     by NodeID for determinism (ensures all nodes agree on the order).
//   - HappensBefore: if a.Timestamp < b.Timestamp, then a *may* have
//     happened before b. This is a necessary but NOT sufficient condition
//     for true causality (Lamport clocks capture potential causality).
// =============================================================================
package timesync

// EventOrderer defines the interface for ordering events using Lamport timestamps.
type EventOrderer interface {
	// OrderEvents sorts a list of events into total order by Lamport timestamp.
	OrderEvents(events []Event) []Event

	// HappensBefore returns true if event a potentially happened before event b.
	HappensBefore(a, b Event) bool
}

// Orderer implements EventOrderer.
type Orderer struct{}

// NewOrderer creates a new event orderer.
func NewOrderer() *Orderer {
	return nil
}

// OrderEvents sorts events by Lamport timestamp in ascending order.
// Ties (same timestamp) are broken by NodeID for a deterministic total order.
// Returns a new sorted slice without modifying the input.
//
// TODO: Implement:
//   1. Copy the input slice to avoid mutating the original.
//   2. Sort by Timestamp ascending.
//   3. If timestamps are equal, sort by NodeID alphabetically.
//   4. Return the sorted copy.
func (o *Orderer) OrderEvents(events []Event) []Event {
	return nil
}

// HappensBefore determines if event a potentially happened before event b
// based on Lamport timestamps.
//
// Note: Lamport clocks provide a *necessary* condition for causality:
//   - If a caused b, then a.Timestamp < b.Timestamp (guaranteed).
//   - If a.Timestamp < b.Timestamp, a *might* have caused b (not guaranteed).
//   - If a.Timestamp >= b.Timestamp, a definitely did NOT cause b.
//
// TODO: Return true if a.Timestamp < b.Timestamp.
func (o *Orderer) HappensBefore(a, b Event) bool {
	return false
}
