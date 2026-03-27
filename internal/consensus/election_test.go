// =============================================================================
// Module: Consensus
// File: election_test.go
// Tests for election.go — vote tracking, log comparison, election manager.
// Note: ZooKeeper integration tests require a running ZK instance and are
//       in test/integration/.
// =============================================================================
package consensus

import (
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// ElectionManager tests
// ---------------------------------------------------------------------------

func TestNewElectionManager(t *testing.T) {
	em := NewElectionManager(300, 500)
	if em == nil {
		t.Fatal("NewElectionManager returned nil")
	}
	if em.minTimeout != 300*time.Millisecond {
		t.Errorf("minTimeout = %v, want 300ms", em.minTimeout)
	}
	if em.maxTimeout != 500*time.Millisecond {
		t.Errorf("maxTimeout = %v, want 500ms", em.maxTimeout)
	}
}

func TestRandomTimeout_WithinRange(t *testing.T) {
	em := NewElectionManager(300, 500)

	for i := 0; i < 100; i++ {
		timeout := em.RandomTimeout()
		if timeout < 300*time.Millisecond || timeout > 500*time.Millisecond {
			t.Errorf("RandomTimeout() = %v, want between 300ms and 500ms", timeout)
		}
	}
}

func TestRandomTimeout_ZeroSpread(t *testing.T) {
	em := NewElectionManager(300, 300)
	timeout := em.RandomTimeout()
	if timeout != 300*time.Millisecond {
		t.Errorf("RandomTimeout() = %v for equal min/max, want 300ms", timeout)
	}
}

// ---------------------------------------------------------------------------
// IsLogUpToDate tests (Raft §5.4.1)
// ---------------------------------------------------------------------------

func TestIsLogUpToDate_HigherTerm(t *testing.T) {
	// Candidate term 3 > voter term 2: candidate is more up-to-date
	if !IsLogUpToDate(3, 1, 2, 10) {
		t.Error("candidate with higher term should be up-to-date")
	}
}

func TestIsLogUpToDate_LowerTerm(t *testing.T) {
	// Candidate term 1 < voter term 2: candidate is NOT up-to-date
	if IsLogUpToDate(1, 100, 2, 1) {
		t.Error("candidate with lower term should NOT be up-to-date")
	}
}

func TestIsLogUpToDate_SameTermHigherIndex(t *testing.T) {
	// Same term, candidate index 5 > voter index 3: up-to-date
	if !IsLogUpToDate(2, 5, 2, 3) {
		t.Error("candidate with same term and higher index should be up-to-date")
	}
}

func TestIsLogUpToDate_SameTermLowerIndex(t *testing.T) {
	// Same term, candidate index 3 < voter index 5: NOT up-to-date
	if IsLogUpToDate(2, 3, 2, 5) {
		t.Error("candidate with same term and lower index should NOT be up-to-date")
	}
}

func TestIsLogUpToDate_Equal(t *testing.T) {
	// Same term and same index: up-to-date (equal is ok)
	if !IsLogUpToDate(2, 5, 2, 5) {
		t.Error("candidate with equal term and index should be up-to-date")
	}
}

func TestIsLogUpToDate_BothEmpty(t *testing.T) {
	// Both empty logs: up-to-date
	if !IsLogUpToDate(0, 0, 0, 0) {
		t.Error("both empty logs should be considered up-to-date")
	}
}

// ---------------------------------------------------------------------------
// VoteTracker tests
// ---------------------------------------------------------------------------

func TestNewVoteTracker_ThreeNodes(t *testing.T) {
	vt := NewVoteTracker(3)
	if vt == nil {
		t.Fatal("NewVoteTracker returned nil")
	}
	if vt.majority != 2 {
		t.Errorf("majority = %d for 3-node cluster, want 2", vt.majority)
	}
}

func TestNewVoteTracker_FiveNodes(t *testing.T) {
	vt := NewVoteTracker(5)
	if vt.majority != 3 {
		t.Errorf("majority = %d for 5-node cluster, want 3", vt.majority)
	}
}

func TestVoteTracker_NoVotes(t *testing.T) {
	vt := NewVoteTracker(3)
	if vt.HasMajority() {
		t.Error("should not have majority with no votes")
	}
}

func TestVoteTracker_OneVote(t *testing.T) {
	vt := NewVoteTracker(3)
	vt.RecordVote("node1", true)
	if vt.HasMajority() {
		t.Error("should not have majority with 1 vote in 3-node cluster")
	}
}

func TestVoteTracker_Majority(t *testing.T) {
	vt := NewVoteTracker(3)
	vt.RecordVote("node1", true)
	vt.RecordVote("node2", true)
	if !vt.HasMajority() {
		t.Error("should have majority with 2 votes in 3-node cluster")
	}
}

func TestVoteTracker_RejectedVotesDontCount(t *testing.T) {
	vt := NewVoteTracker(3)
	vt.RecordVote("node1", true)
	vt.RecordVote("node2", false)
	if vt.HasMajority() {
		t.Error("rejected votes should not count toward majority")
	}
}

func TestVoteTracker_AllVotesGranted(t *testing.T) {
	vt := NewVoteTracker(3)
	vt.RecordVote("node1", true)
	vt.RecordVote("node2", true)
	vt.RecordVote("node3", true)
	if !vt.HasMajority() {
		t.Error("should have majority with all 3 votes granted")
	}
}

func TestVoteTracker_Reset(t *testing.T) {
	vt := NewVoteTracker(3)
	vt.RecordVote("node1", true)
	vt.RecordVote("node2", true)
	if !vt.HasMajority() {
		t.Error("should have majority before reset")
	}

	vt.Reset()
	if vt.HasMajority() {
		t.Error("should not have majority after reset")
	}
}

func TestVoteTracker_DuplicateVote(t *testing.T) {
	vt := NewVoteTracker(3)
	vt.RecordVote("node1", true)
	vt.RecordVote("node1", true) // duplicate
	if vt.HasMajority() {
		t.Error("duplicate votes from same node should not give majority")
	}
}

// ---------------------------------------------------------------------------
// Helper function tests
// ---------------------------------------------------------------------------

func TestExtractZnodeName(t *testing.T) {
	tests := []struct {
		fullPath string
		expected string
	}{
		{"/election/node-0000000001", "node-0000000001"},
		{"/election/node-0000000042", "node-0000000042"},
		{"node-0000000001", "node-0000000001"},
	}

	for _, tt := range tests {
		got := ExtractZnodeName(tt.fullPath)
		if got != tt.expected {
			t.Errorf("ExtractZnodeName(%q) = %q, want %q", tt.fullPath, got, tt.expected)
		}
	}
}
