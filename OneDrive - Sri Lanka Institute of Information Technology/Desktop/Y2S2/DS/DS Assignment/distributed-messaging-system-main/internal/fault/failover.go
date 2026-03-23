// =============================================================================
// Module: Fault Tolerance
// File: failover.go
// Responsible Member: Imansa Bodini — Fault Tolerance
// Purpose: Implements automatic failover — when a server (node) is detected
//          as failed, any messages that were routed to it are automatically
//          redirected to a healthy backup server so clients never notice
//          the failure.
//
// How it works (simple version):
//   1. Every message is assigned a "primary" server (the one that handles it).
//   2. The FailoverManager listens to the FailureDetector for failure alerts.
//   3. When a server dies → FailoverManager finds the next healthy server
//      and updates the routing table so future messages go there instead.
//   4. When the failed server recovers → routing can optionally move back.
//
// Connections:
//   - Registers an OnFailure callback with internal/fault/detector.go so it
//     is notified the moment a node is declared dead.
//   - Uses FailureDetector.IsAlive() to find a healthy replacement server.
//   - Called by internal/node/node.go when publishing a message, to resolve
//     which server should receive it.
//
// Example (3-node cluster):
//   Normal:   msg → node1 (primary)
//   node1 dies → FailoverManager picks node2 (first healthy server)
//   Clients:  msg → node2  (transparent, no error)
//   node1 recovers → routing stays on node2 (stable, no unnecessary churn)
// =============================================================================
package fault

import (
	"errors"
	"fmt"
	"sync"
)

// ---------------------------------------------------------------------------
// FailoverManager
// ---------------------------------------------------------------------------

// FailoverManager watches for node failures and automatically redirects
// message routing to healthy backup servers.
type FailoverManager struct {
	mu       sync.RWMutex
	detector FailureDetector // used to check liveness of candidate servers
	nodes    []string        // all known node IDs in the cluster, in priority order

	// routing maps each "primary" nodeID to the nodeID currently serving it.
	// Under normal operation routing["node1"] == "node1".
	// After failover:          routing["node1"] == "node2".
	routing map[string]string

	// failoverLog records every failover event for evaluation / reporting.
	failoverLog []FailoverEvent
}

// FailoverEvent records a single automatic failover for auditing and metrics.
type FailoverEvent struct {
	FailedNode      string // the server that went down
	ReplacementNode string // the server that took over
	AffectedRoutes  int    // how many routes were redirected
}

// NewFailoverManager creates a FailoverManager and wires it into the detector.
//
// Parameters:
//   - detector : the failure detector that will call us when a node dies
//   - nodes    : all node IDs in the cluster (e.g. ["node1","node2","node3"])
//
// The manager immediately registers itself as a failure callback so it
// starts redirecting traffic as soon as any node is declared dead.
func NewFailoverManager(detector FailureDetector, nodes []string) (*FailoverManager, error) {
	if detector == nil {
		return nil, errors.New("failover: detector must not be nil")
	}
	if len(nodes) < 2 {
		return nil, errors.New("failover: need at least 2 nodes to failover between")
	}

	fm := &FailoverManager{
		detector:    detector,
		nodes:       append([]string{}, nodes...), // defensive copy
		routing:     make(map[string]string),
		failoverLog: []FailoverEvent{},
	}

	// Initialise routing: every node routes to itself.
	for _, n := range nodes {
		fm.routing[n] = n
	}

	// Register with detector — called automatically when a node dies.
	detector.OnFailure(fm.handleNodeFailure)

	return fm, nil
}

// ---------------------------------------------------------------------------
// Core API
// ---------------------------------------------------------------------------

// Resolve returns the nodeID that should currently handle messages for
// the given target node. Under normal operation this returns targetNode
// unchanged. After a failover it returns the replacement server.
//
// This is the method called by node.go before sending a message — instead
// of sending directly to "node2", the node asks:
//   actual := fm.Resolve("node2")   // might return "node3" if node2 is down
//   send message to actual
func (fm *FailoverManager) Resolve(targetNode string) (string, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	routed, known := fm.routing[targetNode]
	if !known {
		return "", fmt.Errorf("failover: unknown node %q", targetNode)
	}
	return routed, nil
}

// ResolveAll returns the full routing table snapshot — useful for logging
// and for the report's evaluation section.
func (fm *FailoverManager) ResolveAll() map[string]string {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	snapshot := make(map[string]string, len(fm.routing))
	for k, v := range fm.routing {
		snapshot[k] = v
	}
	return snapshot
}

// RestoreNode is called when a previously failed node comes back online
// (after recovery.go has finished syncing its log). It resets the routing
// for that node back to itself.
//
// We do NOT automatically move other nodes' routes back — that would cause
// unnecessary churn. Only the recovered node's own route is restored.
func (fm *FailoverManager) RestoreNode(nodeID string) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if _, known := fm.routing[nodeID]; !known {
		return fmt.Errorf("failover: unknown node %q", nodeID)
	}
	fm.routing[nodeID] = nodeID
	return nil
}

// FailoverLog returns a copy of all failover events that have occurred.
// Use this in your report to evaluate how many redirections happened and
// which nodes failed most often.
func (fm *FailoverManager) FailoverLog() []FailoverEvent {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	log := make([]FailoverEvent, len(fm.failoverLog))
	copy(log, fm.failoverLog)
	return log
}

// ---------------------------------------------------------------------------
// Internal — failure callback
// ---------------------------------------------------------------------------

// handleNodeFailure is registered with the Detector and called automatically
// whenever a node's heartbeat times out. It finds a healthy replacement and
// updates all affected routes.
func (fm *FailoverManager) handleNodeFailure(failedNode string) {
	replacement := fm.pickReplacement(failedNode)
	if replacement == "" {
		// No healthy node available — nothing we can do.
		return
	}

	fm.mu.Lock()
	defer fm.mu.Unlock()

	affected := 0
	// Redirect every route that currently points to the failed node.
	for primary, current := range fm.routing {
		if current == failedNode {
			fm.routing[primary] = replacement
			affected++
		}
	}

	if affected > 0 {
		fm.failoverLog = append(fm.failoverLog, FailoverEvent{
			FailedNode:      failedNode,
			ReplacementNode: replacement,
			AffectedRoutes:  affected,
		})
	}
}

// pickReplacement finds the first node that is alive and is not the failed
// node. Nodes are tried in the order they were registered — the first
// healthy one wins.
func (fm *FailoverManager) pickReplacement(failedNode string) string {
	for _, candidate := range fm.nodes {
		if candidate == failedNode {
			continue
		}
		if fm.detector.IsAlive(candidate) {
			return candidate
		}
	}
	return "" // all nodes dead — cluster is down
}
