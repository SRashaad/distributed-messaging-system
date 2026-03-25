// =============================================================================
// Module: Consensus
// File: state.go
// Responsible Member: Vimukthi Herath — Consensus & Agreement
// Purpose: Defines all data structures and RPC message types used by the
//          Raft-inspired consensus protocol. This includes node state
//          constants, vote/append RPC requests and responses, and the
//          shared LogEntry struct used across consensus and replication.
//
// Connections:
//   - LogEntry is shared with internal/replication for the replicated log.
//   - RequestVote/AppendEntries types map directly to the gRPC proto messages
//     defined in internal/transport/proto/messaging.proto.
//   - NodeState is used by raft.go and election.go to track role transitions.
// =============================================================================
package consensus

// ---------------------------------------------------------------------------
// Node State (Raft role)
// ---------------------------------------------------------------------------

// NodeState represents the current role of a node in the Raft protocol.
// A node is always in exactly one of three states: Follower, Candidate, or Leader.
type NodeState int

const (
	// Follower is the default state. Followers passively receive RPCs from
	// the Leader and vote in elections. They do not issue requests on their own.
	Follower NodeState = iota

	// Candidate is a transitional state. A Follower becomes a Candidate when
	// it suspects the Leader has failed (election timeout). It requests votes
	// from all other nodes.
	Candidate

	// Leader is the active coordinator. The Leader handles all client requests,
	// replicates log entries to Followers, and sends periodic heartbeats.
	Leader
)

// String returns a human-readable name for the node state.
// TODO: Implement a switch returning "Follower", "Candidate", or "Leader".
func (s NodeState) String() string {
	return ""
}

// ---------------------------------------------------------------------------
// RequestVote RPC (Raft §5.2)
// ---------------------------------------------------------------------------

// RequestVoteRequest is sent by a Candidate to each node in the cluster
// to request their vote during a leader election.
// Fields:
//   - Term:         the candidate's current term number
//   - CandidateID:  unique identifier of the candidate
//   - LastLogIndex: index of the candidate's last log entry (for log comparison)
//   - LastLogTerm:  term of the candidate's last log entry (for log comparison)
type RequestVoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteResponse is returned by a node in response to a RequestVote RPC.
// Fields:
//   - Term:        the responding node's current term (so the candidate can update itself)
//   - VoteGranted: true if the responding node voted for this candidate
type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
}

// ---------------------------------------------------------------------------
// AppendEntries RPC (Raft §5.3)
// ---------------------------------------------------------------------------

// AppendEntriesRequest is sent by the Leader to replicate log entries and
// to serve as a heartbeat (when Entries is empty).
// Fields:
//   - Term:         leader's current term
//   - LeaderID:     so followers can redirect clients to the leader
//   - PrevLogIndex: index of the log entry immediately before the new ones
//   - PrevLogTerm:  term of the PrevLogIndex entry (for consistency check)
//   - Entries:      new log entries to append (empty = heartbeat)
//   - LeaderCommit: leader's current commit index
type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesResponse is returned by a Follower after processing AppendEntries.
// Fields:
//   - Term:    follower's current term (leader steps down if term is higher)
//   - Success: true if the follower's log matched at PrevLogIndex/PrevLogTerm
type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

// ---------------------------------------------------------------------------
// Log Entry (shared with replication module)
// ---------------------------------------------------------------------------

// LogEntry represents a single entry in the replicated log.
// Every message published to the system becomes a LogEntry.
// Fields:
//   - Index:     1-based position in the log
//   - Term:      the leader's term when the entry was created
//   - Timestamp: Lamport logical clock value (assigned by timesync module)
//   - Data:      the raw message payload from the client
type LogEntry struct {
	Index     uint64
	Term      uint64
	Timestamp uint64
	Data      []byte
}
