// =============================================================================
// Module: Fault Tolerance
// File: failover_test.go
// Tests for the FailoverManager automatic redirection logic.
// =============================================================================
package fault

import (
	"context"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Mock FailureDetector for failover tests
// ---------------------------------------------------------------------------

// mockDetector implements FailureDetector and lets tests control which nodes
// appear alive and manually fire failure callbacks.
type mockDetector struct {
	alive     map[string]bool
	callbacks []func(string)
}

func newMockDetector(aliveNodes ...string) *mockDetector {
	m := &mockDetector{alive: make(map[string]bool)}
	for _, n := range aliveNodes {
		m.alive[n] = true
	}
	return m
}

func (m *mockDetector) StartMonitoring(_ context.Context)    {}
func (m *mockDetector) ReportHeartbeat(_ string)             {}
func (m *mockDetector) IsAlive(nodeID string) bool           { return m.alive[nodeID] }
func (m *mockDetector) OnFailure(cb func(nodeID string))     { m.callbacks = append(m.callbacks, cb) }

// simulateFailure marks a node as dead and fires all registered callbacks —
// exactly what the real Detector does when a heartbeat times out.
func (m *mockDetector) simulateFailure(nodeID string) {
	m.alive[nodeID] = false
	for _, cb := range m.callbacks {
		cb(nodeID)
	}
}

// simulateRecovery marks a node as alive again (heartbeat resumed).
func (m *mockDetector) simulateRecovery(nodeID string) {
	m.alive[nodeID] = true
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestFailoverManager_NormalRouting(t *testing.T) {
	det := newMockDetector("node1", "node2", "node3")
	fm, err := NewFailoverManager(det, []string{"node1", "node2", "node3"})
	if err != nil {
		t.Fatalf("NewFailoverManager: %v", err)
	}

	// Under normal operation every node routes to itself.
	for _, node := range []string{"node1", "node2", "node3"} {
		got, err := fm.Resolve(node)
		if err != nil {
			t.Fatalf("Resolve(%s): %v", node, err)
		}
		if got != node {
			t.Errorf("expected %s → %s, got %s", node, node, got)
		}
	}
}

func TestFailoverManager_RedirectsOnFailure(t *testing.T) {
	det := newMockDetector("node1", "node2", "node3")
	fm, _ := NewFailoverManager(det, []string{"node1", "node2", "node3"})

	// node1 dies — detector fires the callback automatically.
	det.simulateFailure("node1")

	// node1's messages should now be redirected to node2 (first healthy node).
	got, err := fm.Resolve("node1")
	if err != nil {
		t.Fatalf("Resolve after failover: %v", err)
	}
	if got == "node1" {
		t.Error("expected node1 to be redirected after failure, but still points to itself")
	}
	if got != "node2" {
		t.Errorf("expected redirect to node2, got %s", got)
	}
}

func TestFailoverManager_HealthyNodesUnaffected(t *testing.T) {
	det := newMockDetector("node1", "node2", "node3")
	fm, _ := NewFailoverManager(det, []string{"node1", "node2", "node3"})

	det.simulateFailure("node1")

	// node2 and node3 should still route to themselves.
	for _, node := range []string{"node2", "node3"} {
		got, _ := fm.Resolve(node)
		if got != node {
			t.Errorf("healthy node %s was unexpectedly redirected to %s", node, got)
		}
	}
}

func TestFailoverManager_CascadeFailure(t *testing.T) {
	det := newMockDetector("node1", "node2", "node3")
	fm, _ := NewFailoverManager(det, []string{"node1", "node2", "node3"})

	// node1 dies first — redirected to node2.
	det.simulateFailure("node1")
	got, _ := fm.Resolve("node1")
	if got != "node2" {
		t.Fatalf("after node1 failure expected node2, got %s", got)
	}

	// node2 also dies — node1's messages should now go to node3.
	det.simulateFailure("node2")
	got, _ = fm.Resolve("node1")
	if got != "node3" {
		t.Errorf("after node2 failure expected node3, got %s", got)
	}

	// node2's own route should also redirect to node3.
	got, _ = fm.Resolve("node2")
	if got != "node3" {
		t.Errorf("node2 own route expected node3, got %s", got)
	}
}

func TestFailoverManager_AllNodesDead(t *testing.T) {
	det := newMockDetector("node1", "node2")
	fm, _ := NewFailoverManager(det, []string{"node1", "node2"})

	det.simulateFailure("node1")
	det.simulateFailure("node2")

	// Both nodes dead — Resolve still returns something (last known route),
	// but the FailoverLog should show both events with no replacement.
	log := fm.FailoverLog()
	// First event should have a replacement (node2), second should not
	// because no healthy node was available.
	if len(log) < 1 {
		t.Error("expected at least one failover event recorded")
	}
}

func TestFailoverManager_RestoreAfterRecovery(t *testing.T) {
	det := newMockDetector("node1", "node2", "node3")
	fm, _ := NewFailoverManager(det, []string{"node1", "node2", "node3"})

	// node1 fails, gets redirected to node2.
	det.simulateFailure("node1")
	got, _ := fm.Resolve("node1")
	if got != "node2" {
		t.Fatalf("expected redirect to node2, got %s", got)
	}

	// node1 comes back online (recovery.go finishes syncing its log).
	det.simulateRecovery("node1")
	fm.RestoreNode("node1")

	// node1 should now route to itself again.
	got, _ = fm.Resolve("node1")
	if got != "node1" {
		t.Errorf("after restore expected node1 → node1, got %s", got)
	}
}

func TestFailoverManager_FailoverLogRecorded(t *testing.T) {
	det := newMockDetector("node1", "node2", "node3")
	fm, _ := NewFailoverManager(det, []string{"node1", "node2", "node3"})

	det.simulateFailure("node1")
	det.simulateFailure("node2")

	log := fm.FailoverLog()
	if len(log) != 2 {
		t.Fatalf("expected 2 failover events, got %d", len(log))
	}
	if log[0].FailedNode != "node1" {
		t.Errorf("first event: expected failed=node1, got %s", log[0].FailedNode)
	}
	if log[0].ReplacementNode != "node2" {
		t.Errorf("first event: expected replacement=node2, got %s", log[0].ReplacementNode)
	}
	if log[0].AffectedRoutes < 1 {
		t.Error("expected at least 1 affected route in first event")
	}
}

func TestFailoverManager_UnknownNode(t *testing.T) {
	det := newMockDetector("node1", "node2")
	fm, _ := NewFailoverManager(det, []string{"node1", "node2"})

	_, err := fm.Resolve("node99")
	if err == nil {
		t.Error("expected error for unknown node, got nil")
	}
}

func TestFailoverManager_NeedsAtLeastTwoNodes(t *testing.T) {
	det := newMockDetector("node1")
	_, err := NewFailoverManager(det, []string{"node1"})
	if err == nil {
		t.Error("expected error with only 1 node, got nil")
	}
}

func TestFailoverManager_ResolveAll(t *testing.T) {
	det := newMockDetector("node1", "node2", "node3")
	fm, _ := NewFailoverManager(det, []string{"node1", "node2", "node3"})

	det.simulateFailure("node1")

	table := fm.ResolveAll()
	if len(table) != 3 {
		t.Fatalf("expected 3 entries in routing table, got %d", len(table))
	}
	if table["node1"] == "node1" {
		t.Error("node1 should be redirected after failure")
	}
	if table["node2"] != "node2" {
		t.Error("node2 should still point to itself")
	}
}

func TestFailoverManager_DetectorNil(t *testing.T) {
	_, err := NewFailoverManager(nil, []string{"node1", "node2"})
	if err == nil {
		t.Error("expected error for nil detector")
	}
}

func TestFailoverManager_FastFailover(t *testing.T) {
	// Prove failover is instant (no sleep or delay) — must complete in <10ms.
	det := newMockDetector("node1", "node2", "node3")
	fm, _ := NewFailoverManager(det, []string{"node1", "node2", "node3"})

	start := time.Now()
	det.simulateFailure("node1")
	got, _ := fm.Resolve("node1")
	elapsed := time.Since(start)

	if got == "node1" {
		t.Error("failover did not redirect")
	}
	if elapsed > 10*time.Millisecond {
		t.Errorf("failover took too long: %v (expected <10ms)", elapsed)
	}
}
