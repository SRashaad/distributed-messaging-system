// =============================================================================
// Module: Fault Tolerance
// File: detector.go
// Responsible Member: Imansa Bodini — Fault Tolerance
// Purpose: Implements heartbeat-based failure detection. Each node tracks
//          the last heartbeat received from every known peer. If a peer's
//          heartbeat is not received within the configured timeout, that
//          peer is declared failed and registered callbacks are invoked.
//
// Connections:
//   - Receives heartbeat reports from internal/transport when AppendEntries
//     (heartbeat) RPCs arrive from the leader.
//   - On leader failure, invokes a callback that triggers a new election
//     in internal/consensus/raft.go.
//   - Started during node initialization in internal/node/node.go.
//
// Key concepts:
//   - Heartbeat timeout: if no heartbeat arrives within this duration,
//     the leader is considered failed.
//   - The detector runs a periodic check loop at half the timeout interval
//     to detect failures promptly.
//   - Failure callbacks enable loose coupling between fault detection and
//     the consensus module.
// =============================================================================
package fault

import (
	"context"
	"sync"
	"time"
)

// FailureDetector defines the interface for detecting node failures.
type FailureDetector interface {
	// StartMonitoring begins the periodic liveness check loop.
	StartMonitoring(ctx context.Context)

	// ReportHeartbeat records that a heartbeat was received from the given node.
	ReportHeartbeat(nodeID string)

	// IsAlive returns true if the given node has sent a heartbeat within the timeout.
	IsAlive(nodeID string) bool

	// OnFailure registers a callback invoked when a node is detected as failed.
	OnFailure(callback func(nodeID string))
}

// Detector implements FailureDetector using timestamp-based heartbeat tracking.
type Detector struct {
	mu sync.RWMutex

	timeout       time.Duration          // max time between heartbeats before declaring failure
	lastHeartbeat map[string]time.Time   // nodeID → timestamp of last heartbeat
	callbacks     []func(nodeID string)  // functions to call when a node fails
}

// NewDetector creates a new failure detector with the given timeout.
// A node is considered failed if no heartbeat is received within the timeout.
//
// TODO: Initialize lastHeartbeat map and store the timeout.
func NewDetector(timeout time.Duration) *Detector {
	return nil
}

// StartMonitoring begins periodic checks for node liveness.
// Runs in a loop, checking every timeout/2 interval. Blocks until ctx is cancelled.
//
// TODO: Implement:
//   1. Create a ticker at d.timeout / 2 interval.
//   2. On each tick, call d.checkNodes() to detect failures.
//   3. Stop when ctx.Done() is received.
func (d *Detector) StartMonitoring(ctx context.Context) {
	// TODO: implement
}

// ReportHeartbeat records that a heartbeat was received from the given node.
// Called by the transport layer when an AppendEntries (heartbeat) arrives.
//
// TODO: Set lastHeartbeat[nodeID] = time.Now() with proper locking.
func (d *Detector) ReportHeartbeat(nodeID string) {
	// TODO: implement
}

// IsAlive returns true if the node has sent a heartbeat within the timeout window.
//
// TODO: Check if time.Since(lastHeartbeat[nodeID]) < d.timeout.
func (d *Detector) IsAlive(nodeID string) bool {
	return false
}

// OnFailure registers a callback to be invoked when a node is detected as failed.
// Multiple callbacks can be registered. The consensus module registers a callback
// to trigger a new election when the leader fails.
//
// TODO: Append the callback to d.callbacks.
func (d *Detector) OnFailure(callback func(nodeID string)) {
	// TODO: implement
}

// checkNodes iterates over all known nodes and invokes failure callbacks
// for any node whose last heartbeat exceeds the timeout.
//
// TODO: Implement:
//   1. Read-lock and collect nodes whose heartbeat has expired.
//   2. For each failed node, invoke all registered callbacks.
func (d *Detector) checkNodes() {
	// TODO: implement
}
