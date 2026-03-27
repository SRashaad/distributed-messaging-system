// =============================================================================
// Module: Consensus
// File: state_test.go
// Tests for state.go — NodeState and data structures
// =============================================================================
package consensus

import "testing"

func TestNodeStateString(t *testing.T) {
	tests := []struct {
		state    NodeState
		expected string
	}{
		{Follower, "Follower"},
		{Candidate, "Candidate"},
		{Leader, "Leader"},
		{NodeState(99), "Unknown(99)"},
	}

	for _, tt := range tests {
		got := tt.state.String()
		if got != tt.expected {
			t.Errorf("NodeState(%d).String() = %q, want %q", int(tt.state), got, tt.expected)
		}
	}
}

func TestNodeStateConstants(t *testing.T) {
	if Follower != 0 {
		t.Errorf("Follower should be 0, got %d", Follower)
	}
	if Candidate != 1 {
		t.Errorf("Candidate should be 1, got %d", Candidate)
	}
	if Leader != 2 {
		t.Errorf("Leader should be 2, got %d", Leader)
	}
}

func TestLogEntryFields(t *testing.T) {
	entry := LogEntry{
		Index:     1,
		Term:      3,
		Timestamp: 42,
		Data:      []byte("Hello, World!"),
	}

	if entry.Index != 1 {
		t.Errorf("expected Index=1, got %d", entry.Index)
	}
	if entry.Term != 3 {
		t.Errorf("expected Term=3, got %d", entry.Term)
	}
	if entry.Timestamp != 42 {
		t.Errorf("expected Timestamp=42, got %d", entry.Timestamp)
	}
	if string(entry.Data) != "Hello, World!" {
		t.Errorf("expected Data='Hello, World!', got %s", string(entry.Data))
	}
}

func TestRequestVoteStructs(t *testing.T) {
	req := RequestVoteRequest{
		Term:         5,
		CandidateID:  "node2",
		LastLogIndex: 10,
		LastLogTerm:  4,
	}
	if req.Term != 5 || req.CandidateID != "node2" {
		t.Error("RequestVoteRequest fields incorrect")
	}

	resp := RequestVoteResponse{
		Term:        5,
		VoteGranted: true,
	}
	if resp.Term != 5 || !resp.VoteGranted {
		t.Error("RequestVoteResponse fields incorrect")
	}
}

func TestAppendEntriesStructs(t *testing.T) {
	req := AppendEntriesRequest{
		Term:         5,
		LeaderID:     "node1",
		PrevLogIndex: 3,
		PrevLogTerm:  4,
		Entries: []LogEntry{
			{Index: 4, Term: 5, Timestamp: 10, Data: []byte("msg1")},
		},
		LeaderCommit: 3,
	}
	if req.Term != 5 || req.LeaderID != "node1" || len(req.Entries) != 1 {
		t.Error("AppendEntriesRequest fields incorrect")
	}

	resp := AppendEntriesResponse{
		Term:    5,
		Success: true,
	}
	if resp.Term != 5 || !resp.Success {
		t.Error("AppendEntriesResponse fields incorrect")
	}
}
