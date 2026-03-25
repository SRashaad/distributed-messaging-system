// =============================================================================
// Module: Fault Tolerance
// File: health.go
// Responsible Member: Imansa Bodini — Fault Tolerance
// Purpose: Implements the Health Reporter component, which exposes the health
//          status of every node in the cluster for monitoring and debugging.
//
//          The HealthReporter aggregates information from the FailureDetector
//          and the local node's own state (role, term, uptime) into a single
//          snapshot that can be queried at any time.
//
// Connections:
//   - Reads liveness data from internal/fault/detector.go (IsAlive).
//   - Called by internal/node/node.go to expose a /health endpoint or
//     to log cluster status periodically.
//   - Used during demos and evaluations to show the real-time state of
//     each node in the cluster.
//
// Health Statuses:
//   StatusHealthy   — node is alive and responding to heartbeats
//   StatusDegraded  — node missed some heartbeats but is not yet failed
//   StatusFailed    — node has been declared dead by the failure detector
//   StatusUnknown   — node has never sent a heartbeat (not yet registered)
// =============================================================================
package fault

import (
	"fmt"
	"sync"
	"time"
)

// NodeStatus represents the health state of a single node.
type NodeStatus int

const (
	StatusUnknown  NodeStatus = iota // never received a heartbeat
	StatusHealthy                    // alive — heartbeat within timeout
	StatusDegraded                   // missed some heartbeats but not declared dead
	StatusFailed                     // declared dead by the failure detector
)

// String returns a human-readable label for the status.
func (s NodeStatus) String() string {
	switch s {
	case StatusHealthy:
		return "HEALTHY"
	case StatusDegraded:
		return "DEGRADED"
	case StatusFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// NodeHealth holds the full health snapshot for a single node.
type NodeHealth struct {
	NodeID          string        // unique identifier of the node
	Status          NodeStatus    // current health status
	LastHeartbeat   time.Time     // when the last heartbeat was received (zero if unknown)
	TimeSinceHB     time.Duration // how long ago the last heartbeat arrived
	HeartbeatTimeout time.Duration // the configured timeout threshold
	Message         string        // human-readable description of the status
}

// ClusterHealth is a snapshot of every known node's health at a point in time.
type ClusterHealth struct {
	ReportedAt   time.Time              // when this snapshot was taken
	NodeReports  map[string]*NodeHealth // nodeID → health report
	HealthyCount int                    // number of nodes currently healthy
	FailedCount  int                    // number of nodes currently failed
	TotalCount   int                    // total number of known nodes
}

// IsQuorumAlive returns true if a majority of nodes are healthy.
// Uses the standard Raft quorum formula: ⌊N/2⌋ + 1 must be healthy.
func (c *ClusterHealth) IsQuorumAlive() bool {
	quorum := (c.TotalCount / 2) + 1
	return c.HealthyCount >= quorum
}

// Summary returns a one-line human-readable cluster health summary.
func (c *ClusterHealth) Summary() string {
	quorumStr := "quorum OK"
	if !c.IsQuorumAlive() {
		quorumStr = "QUORUM LOST"
	}
	return fmt.Sprintf(
		"cluster health at %s — %d/%d healthy, %d failed [%s]",
		c.ReportedAt.Format("15:04:05"),
		c.HealthyCount,
		c.TotalCount,
		c.FailedCount,
		quorumStr,
	)
}

// HealthReporter aggregates health data from the FailureDetector and
// exposes it as structured snapshots for monitoring and debugging.
type HealthReporter struct {
	mu       sync.RWMutex
	detector FailureDetector
	nodes    []string      // all known node IDs in the cluster
	timeout  time.Duration // heartbeat timeout (mirrors Detector's timeout)

	// lastHeartbeats tracks when each node was last seen alive.
	// Updated by RecordHeartbeat so the reporter can compute TimeSinceHB.
	lastHeartbeats map[string]time.Time

	// degradedThreshold is the fraction of the timeout at which a node is
	// considered "degraded" rather than healthy.
	// Default: 0.5 → a node is degraded after 50% of the timeout has elapsed.
	degradedThreshold float64
}

// NewHealthReporter creates a new HealthReporter.
//
// Parameters:
//   - detector : the failure detector to query for liveness data
//   - nodes    : all node IDs in the cluster
//   - timeout  : the heartbeat timeout configured on the detector
func NewHealthReporter(detector FailureDetector, nodes []string, timeout time.Duration) *HealthReporter {
	hr := &HealthReporter{
		detector:          detector,
		nodes:             append([]string{}, nodes...),
		timeout:           timeout,
		lastHeartbeats:    make(map[string]time.Time),
		degradedThreshold: 0.5,
	}
	// Mirror heartbeat reports so the reporter can compute time-since-HB.
	detector.OnFailure(func(nodeID string) {
		// Mark the node's last-seen time as far in the past so TimeSinceHB
		// is accurate even before the next RecordHeartbeat call.
		hr.mu.Lock()
		defer hr.mu.Unlock()
		if _, known := hr.lastHeartbeats[nodeID]; !known {
			hr.lastHeartbeats[nodeID] = time.Time{} // zero time = never seen
		}
	})
	return hr
}

// RecordHeartbeat updates the reporter's last-seen timestamp for a node.
// Call this from the same place you call Detector.ReportHeartbeat().
func (hr *HealthReporter) RecordHeartbeat(nodeID string) {
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.lastHeartbeats[nodeID] = time.Now()
}

// NodeReport returns the health snapshot for a single node.
func (hr *HealthReporter) NodeReport(nodeID string) *NodeHealth {
	hr.mu.RLock()
	lastHB, known := hr.lastHeartbeats[nodeID]
	hr.mu.RUnlock()

	now := time.Now()
	health := &NodeHealth{
		NodeID:           nodeID,
		HeartbeatTimeout: hr.timeout,
	}

	if !known || lastHB.IsZero() {
		health.Status = StatusUnknown
		health.Message = "no heartbeat received yet"
		return health
	}

	health.LastHeartbeat = lastHB
	health.TimeSinceHB = now.Sub(lastHB)

	switch {
	case hr.detector.IsAlive(nodeID):
		degradedAt := time.Duration(float64(hr.timeout) * hr.degradedThreshold)
		if health.TimeSinceHB > degradedAt {
			health.Status = StatusDegraded
			health.Message = fmt.Sprintf(
				"heartbeat %.0fms ago — approaching timeout (%.0fms)",
				float64(health.TimeSinceHB.Milliseconds()),
				float64(hr.timeout.Milliseconds()),
			)
		} else {
			health.Status = StatusHealthy
			health.Message = fmt.Sprintf(
				"last heartbeat %.0fms ago",
				float64(health.TimeSinceHB.Milliseconds()),
			)
		}
	default:
		health.Status = StatusFailed
		health.Message = fmt.Sprintf(
			"no heartbeat for %.0fms — declared failed",
			float64(health.TimeSinceHB.Milliseconds()),
		)
	}

	return health
}

// ClusterReport returns a health snapshot for every known node.
// This is the main method used by monitoring tools and the demo.
func (hr *HealthReporter) ClusterReport() *ClusterHealth {
	report := &ClusterHealth{
		ReportedAt:  time.Now(),
		NodeReports: make(map[string]*NodeHealth, len(hr.nodes)),
		TotalCount:  len(hr.nodes),
	}

	for _, nodeID := range hr.nodes {
		nodeHealth := hr.NodeReport(nodeID)
		report.NodeReports[nodeID] = nodeHealth

		switch nodeHealth.Status {
		case StatusHealthy, StatusDegraded:
			report.HealthyCount++
		case StatusFailed:
			report.FailedCount++
		}
	}

	return report
}

// SetDegradedThreshold sets the fraction of the heartbeat timeout at which
// a node transitions from Healthy to Degraded.
// Must be between 0.0 and 1.0. Default is 0.5.
func (hr *HealthReporter) SetDegradedThreshold(fraction float64) {
	if fraction < 0 || fraction > 1 {
		return
	}
	hr.mu.Lock()
	defer hr.mu.Unlock()
	hr.degradedThreshold = fraction
}