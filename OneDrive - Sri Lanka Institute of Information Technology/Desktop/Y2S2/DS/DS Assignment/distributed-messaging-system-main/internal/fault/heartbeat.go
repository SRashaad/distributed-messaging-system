// =============================================================================
// Module: Fault Tolerance
// File: heartbeat.go
// Responsible Member: Imansa Bodini — Fault Tolerance
// Purpose: The HeartbeatEmitter sends periodic heartbeats from the Leader
//          to all followers. Heartbeats serve two purposes:
//            1. Assert leadership (prevent followers from starting elections).
//            2. Carry commit index updates to followers.
//
// Connections:
//   - Started by internal/consensus/raft.go when a node becomes Leader.
//   - Stopped when the node steps down from Leader (loses election or higher term).
//   - Uses a sendFunc provided by the transport layer to broadcast heartbeats.
//   - Followers' FailureDetector (detector.go) resets its timer on each heartbeat.
//
// Key concepts:
//   - Heartbeat interval is typically much shorter than the election timeout
//     (e.g., 150ms heartbeat vs 300–500ms election timeout).
//   - Heartbeats are empty AppendEntries RPCs (no log entries).
// =============================================================================
package fault

import (
	"context"
	"time"
)

// HeartbeatEmitter sends periodic heartbeats to all peers.
// Used exclusively by the Leader to maintain authority.
type HeartbeatEmitter struct {
	interval time.Duration // how often to send heartbeats
	sendFunc func()        // broadcasts a heartbeat to all peers (provided by transport)
}

// NewHeartbeatEmitter creates a new emitter with the given interval.
// sendFunc is a closure that sends an empty AppendEntries to all peers.
func NewHeartbeatEmitter(interval time.Duration, sendFunc func()) *HeartbeatEmitter {
	return &HeartbeatEmitter{
		interval: interval,
		sendFunc: sendFunc,
	}
}

// Start begins emitting heartbeats at the configured interval.
// Blocks until the context is cancelled (i.e., the node stops being Leader).
func (h *HeartbeatEmitter) Start(ctx context.Context) {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.sendFunc()
		case <-ctx.Done():
			return
		}
	}
}
