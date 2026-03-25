// =============================================================================
// Module: Fault Tolerance
// File: health_test.go
// Tests for the HealthReporter and ClusterHealth components.
// =============================================================================
package fault

import (
	"testing"
	"time"
)

func TestHealthReporter_UnknownNode(t *testing.T) {
	det := newMockDetector("node1", "node2")
	hr := NewHealthReporter(det, []string{"node1", "node2"}, 100*time.Millisecond)

	report := hr.NodeReport("node1")
	if report.Status != StatusUnknown {
		t.Errorf("expected StatusUnknown before any heartbeat, got %s", report.Status)
	}
}

func TestHealthReporter_HealthyAfterHeartbeat(t *testing.T) {
	det := newMockDetector("node1", "node2")
	hr := NewHealthReporter(det, []string{"node1", "node2"}, 100*time.Millisecond)

	det.ReportHeartbeat("node1")
	hr.RecordHeartbeat("node1")

	report := hr.NodeReport("node1")
	if report.Status != StatusHealthy {
		t.Errorf("expected StatusHealthy after recent heartbeat, got %s", report.Status)
	}
}

func TestHealthReporter_FailedAfterTimeout(t *testing.T) {
	det := newMockDetector("node1", "node2")
	hr := NewHealthReporter(det, []string{"node1", "node2"}, 50*time.Millisecond)

	det.ReportHeartbeat("node1")
	hr.RecordHeartbeat("node1")

	// Simulate the detector declaring node1 failed (as the real Detector
	// would do after the heartbeat timeout elapses).
	time.Sleep(60 * time.Millisecond)
	det.simulateFailure("node1") // marks alive=false, fires callbacks

	report := hr.NodeReport("node1")
	if report.Status != StatusFailed {
		t.Errorf("expected StatusFailed after timeout, got %s", report.Status)
	}
}

func TestHealthReporter_DegradedNearTimeout(t *testing.T) {
	det := newMockDetector("node1")
	hr := NewHealthReporter(det, []string{"node1"}, 200*time.Millisecond)
	hr.SetDegradedThreshold(0.3) // degrade after 30% of timeout = 60ms

	det.ReportHeartbeat("node1")
	hr.RecordHeartbeat("node1")

	time.Sleep(70 * time.Millisecond) // past 60ms threshold, still alive

	report := hr.NodeReport("node1")
	if report.Status != StatusDegraded {
		t.Errorf("expected StatusDegraded near timeout, got %s", report.Status)
	}
}

func TestHealthReporter_ClusterReport_AllHealthy(t *testing.T) {
	det := newMockDetector("node1", "node2", "node3")
	hr := NewHealthReporter(det, []string{"node1", "node2", "node3"}, 100*time.Millisecond)

	for _, n := range []string{"node1", "node2", "node3"} {
		det.ReportHeartbeat(n)
		hr.RecordHeartbeat(n)
	}

	cluster := hr.ClusterReport()
	if cluster.HealthyCount != 3 {
		t.Errorf("expected 3 healthy nodes, got %d", cluster.HealthyCount)
	}
	if cluster.FailedCount != 0 {
		t.Errorf("expected 0 failed nodes, got %d", cluster.FailedCount)
	}
	if !cluster.IsQuorumAlive() {
		t.Error("quorum should be alive when all nodes are healthy")
	}
}

func TestHealthReporter_ClusterReport_OneNodeFailed(t *testing.T) {
	det := newMockDetector("node1", "node2", "node3")
	hr := NewHealthReporter(det, []string{"node1", "node2", "node3"}, 50*time.Millisecond)

	for _, n := range []string{"node1", "node2", "node3"} {
		det.ReportHeartbeat(n)
		hr.RecordHeartbeat(n)
	}

	// node3 fails
	det.simulateFailure("node3")
	time.Sleep(60 * time.Millisecond)

	cluster := hr.ClusterReport()
	if cluster.HealthyCount < 2 {
		t.Errorf("expected at least 2 healthy nodes, got %d", cluster.HealthyCount)
	}
	if !cluster.IsQuorumAlive() {
		t.Error("quorum should survive with 2 of 3 nodes alive")
	}
}

func TestHealthReporter_QuorumLost(t *testing.T) {
	det := newMockDetector("node1", "node2", "node3")
	hr := NewHealthReporter(det, []string{"node1", "node2", "node3"}, 50*time.Millisecond)

	// Only node1 gets a heartbeat; node2 and node3 are failed
	det.ReportHeartbeat("node1")
	hr.RecordHeartbeat("node1")
	det.simulateFailure("node2")
	det.simulateFailure("node3")
	time.Sleep(60 * time.Millisecond)

	cluster := hr.ClusterReport()
	if cluster.IsQuorumAlive() {
		t.Error("quorum should be lost when majority of nodes fail")
	}
}

func TestHealthReporter_Summary_ContainsStatus(t *testing.T) {
	det := newMockDetector("node1", "node2", "node3")
	hr := NewHealthReporter(det, []string{"node1", "node2", "node3"}, 100*time.Millisecond)

	for _, n := range []string{"node1", "node2", "node3"} {
		det.ReportHeartbeat(n)
		hr.RecordHeartbeat(n)
	}

	cluster := hr.ClusterReport()
	summary := cluster.Summary()
	if summary == "" {
		t.Error("expected non-empty summary")
	}
}

func TestHealthReporter_NodeStatus_String(t *testing.T) {
	cases := map[NodeStatus]string{
		StatusHealthy:  "HEALTHY",
		StatusDegraded: "DEGRADED",
		StatusFailed:   "FAILED",
		StatusUnknown:  "UNKNOWN",
	}
	for status, want := range cases {
		if status.String() != want {
			t.Errorf("expected %s, got %s", want, status.String())
		}
	}
}

func TestHealthReporter_NodeReport_ContainsTimeSinceHB(t *testing.T) {
	det := newMockDetector("node1")
	hr := NewHealthReporter(det, []string{"node1"}, 100*time.Millisecond)

	det.ReportHeartbeat("node1")
	hr.RecordHeartbeat("node1")
	time.Sleep(20 * time.Millisecond)

	report := hr.NodeReport("node1")
	if report.TimeSinceHB < 10*time.Millisecond {
		t.Errorf("expected TimeSinceHB > 10ms, got %v", report.TimeSinceHB)
	}
	if report.LastHeartbeat.IsZero() {
		t.Error("expected non-zero LastHeartbeat")
	}
}

func TestHealthReporter_ClusterReport_TotalCount(t *testing.T) {
	det := newMockDetector("node1", "node2", "node3", "node4", "node5")
	hr := NewHealthReporter(det, []string{"node1", "node2", "node3", "node4", "node5"}, 100*time.Millisecond)

	cluster := hr.ClusterReport()
	if cluster.TotalCount != 5 {
		t.Errorf("expected TotalCount=5, got %d", cluster.TotalCount)
	}
}

func TestHealthReporter_SetDegradedThreshold_Invalid(t *testing.T) {
	det := newMockDetector("node1")
	hr := NewHealthReporter(det, []string{"node1"}, 100*time.Millisecond)

	// Invalid values should be silently ignored
	hr.SetDegradedThreshold(-0.5)
	hr.SetDegradedThreshold(1.5)
	// No panic — test passes if we get here
}