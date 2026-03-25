package timesync

import (
	"fmt"
	"strings"
)

// Event represents a timestamped action in a distributed system.
//
// Event ordering matters because different nodes observe actions at different
// times. Lamport timestamps provide a consistent logical timeline to compare
// events across nodes without relying on synchronized physical clocks.
type Event struct {
	Type      string // "send", "receive", or "internal"
	NodeID    string
	Timestamp int
}

// String returns a readable representation of the event.
// Example: "[NodeA] SEND at time 12"
func (e Event) String() string {
	return fmt.Sprintf("[%s] %s at time %d", e.NodeID, strings.ToUpper(e.Type), e.Timestamp)
}
