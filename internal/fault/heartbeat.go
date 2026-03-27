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
//   - Stopped when the node steps down from Leader.
//   - Uses a sendFunc provided by the transport layer to broadcast heartbeats.
//   - Followers' FailureDetector (detector.go) resets its timer on each heartbeat.
//
// Note: With ZooKeeper handling leader election, heartbeats are still useful
//       for replication commit index propagation and peer health monitoring.
// =============================================================================
package fault

import (
	"context"
	"log"
	"time"
)

// HeartbeatEmitter sends periodic heartbeats to all peers.
type HeartbeatEmitter struct {
	interval time.Duration // how often to send heartbeats
	sendFunc func()        // broadcasts a heartbeat to all peers
}

// NewHeartbeatEmitter creates a new emitter with the given interval.
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

	log.Printf("[fault] Heartbeat emitter started (interval=%v)", h.interval)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[fault] Heartbeat emitter stopped")
			return
		case <-ticker.C:
			h.sendFunc()
		}
	}
}
