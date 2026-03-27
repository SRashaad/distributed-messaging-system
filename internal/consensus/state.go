// =============================================================================
// Module: Consensus
// File: state.go
// Responsible Member: Vimukthi Herath — Consensus & Agreement
// Purpose: Defines all data structures and RPC message types used by the
//          Raft-inspired consensus protocol (backed by ZooKeeper for leader
//          election). This includes node state constants, vote/append RPC
//          requests and responses, and the shared LogEntry struct used
//          across consensus and replication.
//
// Connections:
//   - LogEntry is shared with internal/replication for the replicated log.
//   - RequestVote/AppendEntries types map directly to the gRPC proto messages
//     defined in internal/transport/proto/messaging.proto.
//   - NodeState is used by raft.go and election.go to track role transitions.
// =============================================================================
package consensus

import "fmt"

// ---------------------------------------------------------------------------
// Node State (Raft role)
// ---------------------------------------------------------------------------

// NodeState represents the current role of a node in the consensus protocol.
// A node is always in exactly one of three states: Follower, Candidate, or Leader.
type NodeState int

const (
	// Follower is the default state. Followers passively receive RPCs from
	// the Leader and vote in elections. They do not issue requests on their own.
	Follower NodeState = iota

	// Candidate is a transitional state. With ZooKeeper-based election, this
	// state is used briefly while the node is contesting the election znode.
	Candidate

	// Leader is the active coordinator. The Leader handles all client requests,
	// replicates log entries to Followers, and sends periodic heartbeats.
	Leader
)

// String returns a human-readable name for the node state.
func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return fmt.Sprintf("Unknown(%d)", int(s))
	}
}

// ---------------------------------------------------------------------------
// RequestVote RPC (Raft §5.2)
// ---------------------------------------------------------------------------

// RequestVoteRequest is sent by a Candidate to each node in the cluster
// to request their vote during a leader election.
type RequestVoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteResponse is returned by a node in response to a RequestVote RPC.
type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

// ---------------------------------------------------------------------------
// AppendEntries RPC (Raft §5.3)
// ---------------------------------------------------------------------------

// AppendEntriesRequest is sent by the Leader to replicate log entries and
// to serve as a heartbeat (when Entries is empty).
type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesResponse is returned by a Follower after processing AppendEntries.
type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

// ---------------------------------------------------------------------------
// Log Entry (shared with replication module)
// ---------------------------------------------------------------------------

// LogEntry represents a single entry in the replicated log.
// Every message published to the system becomes a LogEntry.
type LogEntry struct {
	Index     uint64
	Term      uint64
	Timestamp uint64
	Data      []byte
}
