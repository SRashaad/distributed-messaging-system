// =============================================================================
// Module: Consensus
// File: raft.go
// Responsible Member: Vimukthi Herath — Consensus & Agreement
// Purpose: Core Raft consensus module implementing the ConsensusModule
//          interface. This is the central coordinator for leader election,
//          term management, and log agreement across the cluster.
//
// Connections:
//   - Uses internal/replication.ReplicatedLog to read/write the replicated log.
//   - Uses internal/timesync.LamportClock to assign timestamps to events.
//   - Uses internal/fault.FailureDetector to monitor peer liveness.
//   - Uses internal/transport to send/receive RequestVote and AppendEntries RPCs.
//   - Wired together in internal/node/node.go during initialization.
//
// Key Raft rules to implement:
//   - Only one leader per term (safety guarantee).
//   - Leader sends heartbeats to prevent new elections.
//   - If a node receives a higher term, it steps down to Follower.
//   - Entries are committed only after quorum acknowledgment.
// =============================================================================
package consensus

import (
	"context"
	"sync"
)

// ConsensusModule defines the interface for the core Raft consensus logic.
// All interaction with the consensus layer goes through this interface.
type ConsensusModule interface {
	// Start begins the consensus module's main event loop (election timer, heartbeats).
	Start(ctx context.Context)

	// Stop gracefully shuts down the consensus module and its goroutines.
	Stop()

	// State returns the current role of this node (Follower, Candidate, or Leader).
	State() NodeState

	// CurrentTerm returns the node's current term number.
	CurrentTerm() uint64

	// LeaderID returns the ID of the currently known leader (empty if unknown).
	LeaderID() string

	// HandleRequestVote processes an incoming RequestVote RPC from a candidate.
	HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse

	// HandleAppendEntries processes an incoming AppendEntries RPC from the leader.
	HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse

	// ProposeEntry is called when a client wants to publish a message.
	// Only valid on the Leader. Returns the assigned log index.
	ProposeEntry(data []byte) (uint64, error)
}

// RaftNode implements the ConsensusModule interface.
// It holds the persistent and volatile state described in the Raft paper.
type RaftNode struct {
	mu sync.RWMutex // protects all mutable state below

	id          string    // unique node identifier
	state       NodeState // current role (Follower / Candidate / Leader)
	currentTerm uint64    // latest term this node has seen (persisted)
	votedFor    string    // candidateID that received vote in current term (persisted)
	leaderID    string    // ID of the known leader (volatile)

	// TODO: Add fields for:
	//   - replication.ReplicatedLog  → the local replicated log
	//   - timesync.LamportClock      → the Lamport clock for timestamps
	//   - fault.FailureDetector      → failure detection reference
	//   - transport reference        → for sending RPCs to peers
	//   - election timer channel     → for triggering elections
	//   - stop channel               → for graceful shutdown
}

// NewRaftNode creates a new RaftNode starting in the Follower state.
// Every node begins as a Follower and waits for heartbeats or an election timeout.
//
// TODO: Accept additional parameters for the replication log, clock,
//       failure detector, and transport layer so they can be wired in.
func NewRaftNode(id string) *RaftNode {
	return nil
}

// Start begins the consensus module's main loop.
//
// TODO: Implement the following:
//   1. Start the election timer with a randomized timeout.
//   2. Listen for incoming RPCs (RequestVote, AppendEntries).
//   3. If election timer fires → transition to Candidate and start election.
//   4. If elected Leader → begin sending periodic heartbeats.
//   5. Run until ctx is cancelled.
func (r *RaftNode) Start(ctx context.Context) {
	// TODO: implement
}

// Stop gracefully shuts down the consensus module.
//
// TODO: Signal all internal goroutines to stop and release resources.
func (r *RaftNode) Stop() {
	// TODO: implement
}

// State returns the current node state (Follower, Candidate, or Leader).
//
// TODO: Return r.state with proper locking.
func (r *RaftNode) State() NodeState {
	return Follower
}

// CurrentTerm returns the node's current term number.
//
// TODO: Return r.currentTerm with proper locking.
func (r *RaftNode) CurrentTerm() uint64 {
	return 0
}

// LeaderID returns the ID of the current known leader.
//
// TODO: Return r.leaderID with proper locking.
func (r *RaftNode) LeaderID() string {
	return ""
}

// HandleRequestVote processes an incoming RequestVote RPC from a Candidate.
//
// TODO: Implement Raft §5.2 logic:
//   1. If req.Term < currentTerm → reject (stale candidate).
//   2. If req.Term > currentTerm → update term, step down to Follower, clear votedFor.
//   3. Grant vote if votedFor is empty (or matches candidateID) AND
//      the candidate's log is at least as up-to-date as ours.
//   4. Use IsLogUpToDate() from election.go for the log comparison.
//   5. Return current term and whether the vote was granted.
func (r *RaftNode) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	return nil
}

// HandleAppendEntries processes an incoming AppendEntries RPC from the Leader.
//
// TODO: Implement Raft §5.3 logic:
//   1. If req.Term < currentTerm → reject (stale leader).
//   2. If req.Term >= currentTerm → reset election timer (leader is alive).
//   3. Update leaderID to req.LeaderID.
//   4. Check log consistency at PrevLogIndex / PrevLogTerm.
//   5. If consistent → append new entries to the replicated log.
//   6. Update commitIndex to min(req.LeaderCommit, last new entry index).
//   7. Update Lamport clock using timesync.Clock.Update().
//   8. Return current term and success/failure.
func (r *RaftNode) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	return nil
}

// ProposeEntry proposes a new entry to be replicated across the cluster.
// This is called when a client publishes a message. Only valid on the Leader.
//
// TODO: Implement:
//   1. Verify this node is the Leader (reject otherwise).
//   2. Increment the Lamport clock via timesync.Clock.Tick().
//   3. Create a new LogEntry with current term, next index, Lamport timestamp, and data.
//   4. Append the entry to the local replicated log.
//   5. Trigger replication to all followers via internal/replication.Manager.
//   6. Wait for quorum acknowledgment (majority of nodes).
//   7. Commit the entry and return the assigned log index.
func (r *RaftNode) ProposeEntry(data []byte) (uint64, error) {
	return 0, nil
}
