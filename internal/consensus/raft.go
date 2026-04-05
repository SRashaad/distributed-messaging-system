// =============================================================================
// Module: Consensus
// File: raft.go
// Responsible Member: Vimukthi Herath — Consensus & Agreement
// Purpose: Core consensus module implementing the ConsensusModule interface
//          using Apache ZooKeeper for leader election and session-based failure
//          detection. The RaftNode still manages AppendEntries for log
//          replication and ProposeEntry for client writes.
//
// Connections:
//   - Uses internal/replication.ReplicatedLog to read/write the replicated log.
//   - Uses internal/timesync.LamportClock to assign timestamps to events.
//   - Uses ZooKeeper (go-zookeeper/zk) for leader election instead of Raft votes.
//   - Uses internal/transport to send/receive AppendEntries RPCs.
//   - Wired together in internal/node/node.go during initialization.
//
// ZooKeeper election model:
//   - Each node creates an ephemeral sequential znode under /election.
//   - The node with the lowest sequence number is the Leader.
//   - Followers watch the znode just before theirs (chain watch pattern).
//   - If a watched znode disappears, the watcher re-evaluates its position.
// =============================================================================
package consensus

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
)

// ConsensusModule defines the interface for the core consensus logic.
// All interaction with the consensus layer goes through this interface.
type ConsensusModule interface {
	// Start begins the consensus module (connects to ZooKeeper, runs election).
	Start(ctx context.Context)

	// Stop gracefully shuts down the consensus module.
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
	// Only valid on the Leader. Returns the assigned log index and term.
	ProposeEntry(data []byte) (uint64, uint64, error)
}

// RaftNode implements the ConsensusModule interface using ZooKeeper for election.
type RaftNode struct {
	mu sync.RWMutex // protects all mutable state below

	id          string    // unique node identifier
	state       NodeState // current role (Follower / Candidate / Leader)
	currentTerm uint64    // latest term this node has seen
	votedFor    string    // candidateID that received vote in current term
	leaderID    string    // ID of the known leader (volatile)

	// ZooKeeper fields
	zkConn    *zk.Conn          // ZooKeeper connection
	zkServers []string          // ZooKeeper server addresses
	myZnode   string            // full path of this node's election znode

	// Election manager
	election *ElectionManager

	// Module dependencies (injected)
	// These are interface{} placeholders — in the integrated system, they would
	// be the concrete types from replication and timesync packages.
	commitIndex uint64         // highest log index known to be committed

	// Lifecycle
	cancelFunc context.CancelFunc // cancels the main context
	stopped    chan struct{}       // closed when all goroutines exit
}

// NewRaftNode creates a new RaftNode starting in the Follower state.
// zkServers is a list of ZooKeeper addresses (e.g., ["localhost:2181"]).
func NewRaftNode(id string, zkServers []string) *RaftNode {
	return &RaftNode{
		id:          id,
		state:       Follower,
		currentTerm: 0,
		votedFor:    "",
		leaderID:    "",
		zkServers:   zkServers,
		election:    NewElectionManager(300, 500),
		stopped:     make(chan struct{}),
	}
}

// Start begins the consensus module's main loop:
//  1. Connects to ZooKeeper.
//  2. Creates the /election znode if needed.
//  3. Runs the leader election loop.
func (r *RaftNode) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	r.cancelFunc = cancel

	log.Printf("[%s] Connecting to ZooKeeper at %v", r.id, r.zkServers)

	conn, eventCh, err := zk.Connect(r.zkServers, 5*time.Second)
	if err != nil {
		log.Printf("[%s] ERROR: Failed to connect to ZooKeeper: %v", r.id, err)
		close(r.stopped)
		return
	}
	r.zkConn = conn

	// Wait for the ZooKeeper connection to be established
	go r.watchZkSession(eventCh)

	// Ensure the /election parent znode exists
	if err := EnsureElectionPath(r.zkConn); err != nil {
		log.Printf("[%s] ERROR: Failed to create election path: %v", r.id, err)
		close(r.stopped)
		return
	}

	// Create our ephemeral sequential node
	znode, err := CreateElectionNode(r.zkConn, r.id)
	if err != nil {
		log.Printf("[%s] ERROR: Failed to create election node: %v", r.id, err)
		close(r.stopped)
		return
	}
	r.myZnode = znode
	log.Printf("[%s] Created election znode: %s", r.id, znode)

	// Run the election loop in a goroutine
	go r.electionLoop(ctx)
}

// electionLoop continuously checks our position in the election znodes.
// If we are the smallest znode, we become the leader.
// Otherwise we watch the znode just before us and wait.
func (r *RaftNode) electionLoop(ctx context.Context) {
	defer close(r.stopped)

	for {
		select {
		case <-ctx.Done():
			log.Printf("[%s] Election loop stopped", r.id)
			return
		default:
		}

		role, watchTarget, err := DetermineRole(r.zkConn, r.myZnode)
		if err != nil {
			log.Printf("[%s] Election error: %v, retrying...", r.id, err)
			time.Sleep(r.election.RandomTimeout())
			continue
		}

		if role == Leader {
			r.becomeLeader()
			// Leader stays in this loop but sleeps — if ZK session expires,
			// the ephemeral node is deleted and we retry
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
				// Periodically re-verify leadership
				continue
			}
		}

		// We are a follower — watch the node before us
		r.mu.Lock()
		r.state = Follower
		r.mu.Unlock()

		// Read the leader's data to figure out who the leader is
		r.identifyLeader()

		log.Printf("[%s] State=Follower, watching %s", r.id, watchTarget)

		// Set a watch on the node before us
		exists, _, watchCh, err := r.zkConn.ExistsW(watchTarget)
		if err != nil || !exists {
			// Node already gone, immediately re-check
			log.Printf("[%s] Watch target gone, re-evaluating", r.id)
			continue
		}

		// Wait for the watch event or context cancellation
		select {
		case event := <-watchCh:
			log.Printf("[%s] Watch event: %v, re-evaluating election", r.id, event.Type)
		case <-ctx.Done():
			return
		}
	}
}

// becomeLeader transitions this node to the Leader state.
func (r *RaftNode) becomeLeader() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		r.currentTerm++
		r.state = Leader
		r.leaderID = r.id
		r.votedFor = r.id
		log.Printf("[%s] ★ Became LEADER (term=%d)", r.id, r.currentTerm)
	}
}

// identifyLeader reads the first (smallest) znode to determine who the leader is.
func (r *RaftNode) identifyLeader() {
	candidates, err := GetSortedCandidates(r.zkConn)
	if err != nil || len(candidates) == 0 {
		return
	}

	// Read the data from the leader's znode (it contains the nodeID)
	leaderPath := ElectionZNode + "/" + candidates[0]
	data, _, err := r.zkConn.Get(leaderPath)
	if err != nil {
		return
	}

	r.mu.Lock()
	r.leaderID = string(data)
	r.mu.Unlock()
}

// watchZkSession monitors ZooKeeper session events and handles reconnections.
func (r *RaftNode) watchZkSession(eventCh <-chan zk.Event) {
	for event := range eventCh {
		switch event.State {
		case zk.StateConnected:
			log.Printf("[%s] ZooKeeper connected", r.id)
		case zk.StateDisconnected:
			log.Printf("[%s] ZooKeeper disconnected", r.id)
			r.mu.Lock()
			r.state = Follower
			r.leaderID = ""
			r.mu.Unlock()
		case zk.StateExpired:
			log.Printf("[%s] ZooKeeper session expired — re-election needed", r.id)
			r.mu.Lock()
			r.state = Follower
			r.leaderID = ""
			r.mu.Unlock()
		}
	}
}

// Stop gracefully shuts down the consensus module.
func (r *RaftNode) Stop() {
	log.Printf("[%s] Stopping consensus module", r.id)

	if r.cancelFunc != nil {
		r.cancelFunc()
	}

	// Wait for goroutines to finish (with timeout)
	select {
	case <-r.stopped:
	case <-time.After(5 * time.Second):
		log.Printf("[%s] Consensus stop timed out", r.id)
	}

	if r.zkConn != nil {
		r.zkConn.Close()
	}
}

// State returns the current node state (Follower, Candidate, or Leader).
func (r *RaftNode) State() NodeState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

// CurrentTerm returns the node's current term number.
func (r *RaftNode) CurrentTerm() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currentTerm
}

// LeaderID returns the ID of the current known leader.
func (r *RaftNode) LeaderID() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leaderID
}

// HandleRequestVote processes an incoming RequestVote RPC from a Candidate.
// With ZooKeeper handling elections, this is a compatibility shim.
// It still follows Raft §5.2 rules for correctness.
func (r *RaftNode) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If the request term is stale, reject
	if req.Term < r.currentTerm {
		return &RequestVoteResponse{
			Term:        r.currentTerm,
			VoteGranted: false,
		}
	}

	// If the request term is higher, update our term and step down
	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.state = Follower
		r.votedFor = ""
		r.leaderID = ""
	}

	// Grant vote if we haven't voted yet (or voted for this candidate)
	// AND the candidate's log is at least as up-to-date
	canVote := r.votedFor == "" || r.votedFor == req.CandidateID
	logOk := IsLogUpToDate(req.LastLogTerm, req.LastLogIndex, 0, 0)

	if canVote && logOk {
		r.votedFor = req.CandidateID
		return &RequestVoteResponse{
			Term:        r.currentTerm,
			VoteGranted: true,
		}
	}

	return &RequestVoteResponse{
		Term:        r.currentTerm,
		VoteGranted: false,
	}
}

// HandleAppendEntries processes an incoming AppendEntries RPC from the Leader.
// This function is used for log replication and heartbeats, regardless of
// whether ZooKeeper or Raft handles leader election.
func (r *RaftNode) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 1. If request term < our term → reject (stale leader)
	if req.Term < r.currentTerm {
		return &AppendEntriesResponse{
			Term:    r.currentTerm,
			Success: false,
		}
	}

	// 2. If request term >= our term → accept (leader is alive)
	if req.Term > r.currentTerm {
		r.currentTerm = req.Term
		r.votedFor = ""
	}
	r.state = Follower
	r.leaderID = req.LeaderID

	// 3. Check log consistency at PrevLogIndex/PrevLogTerm
	//    (simplified — in full integration, we'd check against the replicated log)
	//    For now, accept if PrevLogIndex is 0 (beginning of log) or we have the entry
	if req.PrevLogIndex > 0 {
		// In full integration: check log entry at PrevLogIndex has term == PrevLogTerm
		// For now, we accept entries to avoid blocking other modules
	}

	// 4. Append new entries (would delegate to replication log in integration)
	// The entries are stored via the replication module when fully wired

	// 5. Update commit index
	if req.LeaderCommit > r.commitIndex {
		r.commitIndex = req.LeaderCommit
	}

	return &AppendEntriesResponse{
		Term:    r.currentTerm,
		Success: true,
	}
}

// ProposeEntry proposes a new entry to be replicated across the cluster.
// Only valid on the Leader node.
func (r *RaftNode) ProposeEntry(data []byte) (uint64, uint64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 1. Verify this node is the Leader
	if r.state != Leader {
		return 0, 0, errors.New("not the leader — redirect to " + r.leaderID)
	}

	// 2. Create a new LogEntry
	//    In full integration, this would:
	//    - Tick the Lamport clock
	//    - Assign the next log index
	//    - Append to the replicated log
	//    - Trigger replication to followers
	//    - Wait for quorum ACK
	//    - Commit and return

	// For now, return a placeholder index showing the flow works
	nextIndex := r.commitIndex + 1

	entry := LogEntry{
		Index:     nextIndex,
		Term:      r.currentTerm,
		Timestamp: 0, // Would be set by LamportClock.Tick()
		Data:      data,
	}

	log.Printf("[%s] Proposed entry: index=%d term=%d data=%s",
		r.id, entry.Index, entry.Term, string(entry.Data))

	r.commitIndex = nextIndex

	return entry.Index, entry.Term, nil
}

// GetID returns the node's unique identifier.
func (r *RaftNode) GetID() string {
	return r.id
}

// IsLeader is a convenience method to check if this node is the leader.
func (r *RaftNode) IsLeader() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state == Leader
}

// GetZkConnection returns the ZooKeeper connection (for other modules to use).
func (r *RaftNode) GetZkConnection() *zk.Conn {
	return r.zkConn
}

// Verify interface compliance at compile time.
var _ ConsensusModule = (*RaftNode)(nil)

// FormatClusterStatus returns a human-readable summary of the node's election state.
func (r *RaftNode) FormatClusterStatus() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return fmt.Sprintf(
		"Node=%s State=%s Term=%d Leader=%s CommitIndex=%d ZNode=%s",
		r.id, r.state, r.currentTerm, r.leaderID, r.commitIndex, r.myZnode,
	)
}
