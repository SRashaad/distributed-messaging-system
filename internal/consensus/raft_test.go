// =============================================================================
// Module: Consensus
// File: raft_test.go
// Tests for raft.go
// Note: Since raft.go now tightly integrates with ZooKeeper for election, these
// tests cover the logic that doesn't strictly depend on a live ZK instance,
// such as handling AppendEntries, RequestVote (compatibility), and ProposeEntry.
// Full election tests are done via integration testing.
// =============================================================================
package consensus

import (
	"bytes"
	"testing"
)

func TestNewRaftNode(t *testing.T) {
	node := NewRaftNode("node1", []string{"localhost:2181"})

	if node.State() != Follower {
		t.Errorf("expected Follower, got %v", node.State())
	}
	if node.CurrentTerm() != 0 {
		t.Errorf("expected term 0, got %d", node.CurrentTerm())
	}
	if node.LeaderID() != "" {
		t.Errorf("expected empty leader ID, got %q", node.LeaderID())
	}
}

func TestHandleRequestVote_StaleTerm(t *testing.T) {
	node := NewRaftNode("node1", []string{})
	node.currentTerm = 5 // setup

	req := RequestVoteRequest{
		Term:         4,
		CandidateID:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	resp := node.HandleRequestVote(&req)
	if resp.VoteGranted {
		t.Error("should reject vote for stale term")
	}
	if resp.Term != 5 {
		t.Errorf("expected term 5, got %d", resp.Term)
	}
}

func TestHandleRequestVote_HigherTerm(t *testing.T) {
	node := NewRaftNode("node1", []string{})
	node.currentTerm = 2
	node.votedFor = "node3" // previously voted in term 2

	req := RequestVoteRequest{
		Term:         3,
		CandidateID:  "node2",
		LastLogIndex: 10,
		LastLogTerm:  3,
	}

	resp := node.HandleRequestVote(&req)
	if !resp.VoteGranted {
		t.Error("should grant vote for higher term")
	}
	if resp.Term != 3 {
		t.Errorf("expected node to update to term 3, got %d", resp.Term)
	}
	if node.votedFor != "node2" {
		t.Errorf("expected votedFor node2, got %q", node.votedFor)
	}
}

func TestHandleAppendEntries_StaleTerm(t *testing.T) {
	node := NewRaftNode("node1", []string{})
	node.currentTerm = 5

	req := AppendEntriesRequest{
		Term:     4,
		LeaderID: "leader-node",
	}

	resp := node.HandleAppendEntries(&req)
	if resp.Success {
		t.Error("should reject AppendEntries from stale leader")
	}
}

func TestHandleAppendEntries_Heartbeat(t *testing.T) {
	node := NewRaftNode("node1", []string{})
	node.currentTerm = 2

	req := AppendEntriesRequest{
		Term:         3,
		LeaderID:     "node2",
		LeaderCommit: 5,
	}

	resp := node.HandleAppendEntries(&req)
	if !resp.Success {
		t.Error("should accept heartbeat from valid leader")
	}
	if node.CurrentTerm() != 3 {
		t.Errorf("expected term 3, got %d", node.CurrentTerm())
	}
	if node.LeaderID() != "node2" {
		t.Errorf("expected leader node2, got %q", node.LeaderID())
	}
	if node.commitIndex != 5 {
		t.Errorf("expected commitIndex 5, got %d", node.commitIndex)
	}
}

func TestProposeEntry_NotLeader(t *testing.T) {
	node := NewRaftNode("node1", []string{})
	
	_, err := node.ProposeEntry([]byte("test data"))
	if err == nil {
		t.Error("ProposeEntry should fail if not leader")
	}
}

func TestProposeEntry_Leader(t *testing.T) {
	node := NewRaftNode("node1", []string{})
	// Manually set as leader for testing
	node.state = Leader
	node.currentTerm = 1
	node.commitIndex = 5

	data := []byte("test data")
	index, err := node.ProposeEntry(data)
	
	if err != nil {
		t.Errorf("ProposeEntry failed: %v", err)
	}
	
	if index != 6 {
		t.Errorf("expected index 6, got %d", index)
	}
	if node.commitIndex != 6 {
		t.Errorf("expected commitIndex 6, got %d", node.commitIndex)
	}
}
