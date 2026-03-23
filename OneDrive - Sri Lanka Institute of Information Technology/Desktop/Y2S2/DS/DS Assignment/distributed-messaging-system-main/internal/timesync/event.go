// =============================================================================
// Module: Time Synchronization
// File: event.go
// Responsible Member: Sabeelur Rashaad — Time Synchronization
// Purpose: Defines the Event struct and EventType constants used to
//          represent timestamped events in the distributed system.
//          Every significant action (sending a message, receiving a message,
//          internal state change) is modeled as an Event with a Lamport timestamp.
//
// Connections:
//   - Used by orderer.go to sort and compare events for causal ordering.
//   - Used by internal/consensus/raft.go to track state transitions.
//   - Used by internal/replication/manager.go to log replication events.
//   - EventType helps categorize events for debugging and analysis.
//
// Event types:
//   - EventSend:     a message was sent to another node
//   - EventReceive:  a message was received from another node
//   - EventInternal: an internal state change (e.g., election, commit)
// =============================================================================
package timesync

// EventType categorizes events in the distributed system.
type EventType int

const (
	// EventSend indicates a message was sent to another node.
	EventSend EventType = iota

	// EventReceive indicates a message was received from another node.
	EventReceive

	// EventInternal indicates an internal state change
	// (e.g., leader election, log commit, state transition).
	EventInternal
)

// String returns a human-readable name for the event type.
// TODO: Implement a switch returning "SEND", "RECEIVE", or "INTERNAL".
func (e EventType) String() string {
	return ""
}

// Event represents a timestamped event in the distributed system.
// Every action in the system is modeled as an Event so it can be
// ordered using Lamport timestamps.
type Event struct {
	NodeID    string    // ID of the node where this event occurred
	Timestamp uint64    // Lamport timestamp assigned to this event
	Type      EventType // category of the event (Send, Receive, Internal)
	Data      []byte    // optional payload associated with the event
}
