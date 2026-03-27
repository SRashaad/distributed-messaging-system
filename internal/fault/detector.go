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
// Note: With ZooKeeper handling leader election, the failure detector's
//       primary role is monitoring peer health for replication/recovery
//       rather than triggering elections. ZooKeeper's session expiry
//       handles leader failure detection automatically.
// =============================================================================
package fault

import (
	"context"
	"log"
	"sync"
	"time"
)

// FailureDetector defines the interface for detecting node failures.
type FailureDetector interface {
	StartMonitoring(ctx context.Context)
	ReportHeartbeat(nodeID string)
	IsAlive(nodeID string) bool
	OnFailure(callback func(nodeID string))
}

// Detector implements FailureDetector using timestamp-based heartbeat tracking.
type Detector struct {
	mu sync.RWMutex

	timeout       time.Duration         // max time between heartbeats before declaring failure
	lastHeartbeat map[string]time.Time  // nodeID → timestamp of last heartbeat
	callbacks     []func(nodeID string) // functions to call when a node fails
}

// NewDetector creates a new failure detector with the given timeout.
func NewDetector(timeout time.Duration) *Detector {
	return &Detector{
		timeout:       timeout,
		lastHeartbeat: make(map[string]time.Time),
		callbacks:     make([]func(nodeID string), 0),
	}
}

// StartMonitoring begins periodic checks for node liveness.
// Runs in a loop, checking every timeout/2 interval. Blocks until ctx is cancelled.
func (d *Detector) StartMonitoring(ctx context.Context) {
	ticker := time.NewTicker(d.timeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[fault] Failure detector stopped")
			return
		case <-ticker.C:
			d.checkNodes()
		}
	}
}

// ReportHeartbeat records that a heartbeat was received from the given node.
func (d *Detector) ReportHeartbeat(nodeID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.lastHeartbeat[nodeID] = time.Now()
}

// IsAlive returns true if the node has sent a heartbeat within the timeout window.
func (d *Detector) IsAlive(nodeID string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	lastSeen, exists := d.lastHeartbeat[nodeID]
	if !exists {
		return false
	}
	return time.Since(lastSeen) < d.timeout
}

// OnFailure registers a callback to be invoked when a node is detected as failed.
func (d *Detector) OnFailure(callback func(nodeID string)) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.callbacks = append(d.callbacks, callback)
}

// checkNodes iterates over all known nodes and invokes failure callbacks
// for any node whose last heartbeat exceeds the timeout.
func (d *Detector) checkNodes() {
	d.mu.RLock()
	var failedNodes []string
	for nodeID, lastSeen := range d.lastHeartbeat {
		if time.Since(lastSeen) >= d.timeout {
			failedNodes = append(failedNodes, nodeID)
		}
	}
	callbacks := make([]func(string), len(d.callbacks))
	copy(callbacks, d.callbacks)
	d.mu.RUnlock()

	// Invoke callbacks outside the lock to avoid deadlocks
	for _, nodeID := range failedNodes {
		log.Printf("[fault] Node %s detected as FAILED", nodeID)
		for _, cb := range callbacks {
			cb(nodeID)
		}
	}
}
