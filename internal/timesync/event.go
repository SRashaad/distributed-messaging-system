package timesync

import (
	"fmt"
)

// Event type constants used across time synchronization.
const (
	EventSend     = "SEND"
	EventReceive  = "RECEIVE"
	EventInternal = "INTERNAL"
)

// Event represents a timestamped action in a distributed system.
//
// Event ordering matters because different nodes observe actions at different
// times. Lamport timestamps provide a consistent logical timeline to compare
// events across nodes without relying on synchronized physical clocks.
type Event struct {
	Type      string // use EventSend, EventReceive, or EventInternal
	NodeID    string
	Timestamp uint64
}

// String returns a readable representation of the event.
// Example: "[NodeA] SEND at time 12"
func (e Event) String() string {
	return fmt.Sprintf("[%s] %s at time %d", e.NodeID, e.Type, e.Timestamp)
}
