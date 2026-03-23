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

	timeout       time.Duration         // max time between heartbeats before declaring failure
	lastHeartbeat map[string]time.Time  // nodeID → timestamp of last heartbeat
	failed        map[string]bool       // nodeID → already-notified-as-failed flag (avoids repeated callbacks)
	callbacks     []func(nodeID string) // functions to call when a node fails
}

// NewDetector creates a new failure detector with the given timeout.
// A node is considered failed if no heartbeat is received within the timeout.
func NewDetector(timeout time.Duration) *Detector {
	return &Detector{
		timeout:       timeout,
		lastHeartbeat: make(map[string]time.Time),
		failed:        make(map[string]bool),
		callbacks:     []func(nodeID string){},
	}
}

// StartMonitoring begins periodic checks for node liveness.
// Runs in a loop, checking every timeout/2 interval. Blocks until ctx is cancelled.
func (d *Detector) StartMonitoring(ctx context.Context) {
	ticker := time.NewTicker(d.timeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.checkNodes()
		case <-ctx.Done():
			return
		}
	}
}

// ReportHeartbeat records that a heartbeat was received from the given node.
// Called by the transport layer when an AppendEntries (heartbeat) arrives.
func (d *Detector) ReportHeartbeat(nodeID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.lastHeartbeat[nodeID] = time.Now()
	// Clear the failed flag — node is alive again.
	// Full log re-sync is handled separately by LogRecovery.
	d.failed[nodeID] = false
}

// IsAlive returns true if the node has sent a heartbeat within the timeout window.
func (d *Detector) IsAlive(nodeID string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	last, known := d.lastHeartbeat[nodeID]
	if !known {
		return false
	}
	return time.Since(last) < d.timeout
}

// OnFailure registers a callback to be invoked when a node is detected as failed.
// Multiple callbacks can be registered. The consensus module registers a callback
// to trigger a new election when the leader fails.
func (d *Detector) OnFailure(callback func(nodeID string)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.callbacks = append(d.callbacks, callback)
}

// checkNodes iterates over all known nodes and invokes failure callbacks
// for any node whose last heartbeat exceeds the timeout.
// Callbacks are fired outside the lock to prevent deadlocks.
func (d *Detector) checkNodes() {
	d.mu.Lock()
	var newlyFailed []string
	for nodeID, last := range d.lastHeartbeat {
		if time.Since(last) >= d.timeout && !d.failed[nodeID] {
			d.failed[nodeID] = true
			newlyFailed = append(newlyFailed, nodeID)
		}
	}
	// Snapshot the callbacks slice while holding the lock.
	callbacks := make([]func(string), len(d.callbacks))
	copy(callbacks, d.callbacks)
	d.mu.Unlock()

	// Fire callbacks outside the lock so the consensus module can safely
	// call back into the detector (e.g., IsAlive) without deadlocking.
	for _, nodeID := range newlyFailed {
		for _, cb := range callbacks {
			cb(nodeID)
		}
	}
}
