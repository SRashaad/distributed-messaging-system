// =============================================================================
// Module: Consensus
// File: election.go
// Responsible Member: Vimukthi Herath — Consensus & Agreement
// Purpose: Handles leader election logic using ZooKeeper ephemeral sequential
//          znodes. Also provides vote tracking utilities and log up-to-date
//          comparison for compatibility with the rest of the system.
//
// Connections:
//   - Called by raft.go to perform leader election via ZooKeeper.
//   - ElectionManager communicates with ZooKeeper for znode-based elections.
//   - VoteTracker collects votes and checks if a majority is reached.
//   - IsLogUpToDate is used by HandleRequestVote in raft.go.
//
// Key concepts:
//   - ZooKeeper ephemeral sequential znodes replace Raft randomized timeouts.
//   - The node with the smallest sequence number becomes the leader.
//   - ZooKeeper watches replace heartbeat-based failure detection for elections.
// =============================================================================
package consensus

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
)

const (
	// ElectionZNode is the parent path in ZooKeeper for election znodes.
	ElectionZNode = "/election"
)

// ElectionManager handles ZooKeeper-based leader election.
// It creates ephemeral sequential znodes to determine who is leader.
type ElectionManager struct {
	minTimeout time.Duration // minimum election timeout (fallback)
	maxTimeout time.Duration // maximum election timeout (fallback)
}

// NewElectionManager creates an ElectionManager with configurable timeout bounds.
// These timeouts are used as fallback if ZooKeeper is temporarily unavailable.
func NewElectionManager(minMs, maxMs int) *ElectionManager {
	return &ElectionManager{
		minTimeout: time.Duration(minMs) * time.Millisecond,
		maxTimeout: time.Duration(maxMs) * time.Millisecond,
	}
}

// RandomTimeout returns a randomized election timeout between [minTimeout, maxTimeout].
// Used as a fallback retry delay when ZooKeeper election needs to be retried.
func (e *ElectionManager) RandomTimeout() time.Duration {
	spread := e.maxTimeout - e.minTimeout
	if spread <= 0 {
		return e.minTimeout
	}
	return e.minTimeout + time.Duration(rand.Int63n(int64(spread)))
}

// IsLogUpToDate determines whether a candidate's log is at least as up-to-date
// as the voter's log. Implements Raft §5.4.1 election restriction:
//   - A candidate with a higher last log term is more up-to-date.
//   - If terms are equal, the candidate with the longer log wins.
func IsLogUpToDate(candidateLastTerm, candidateLastIndex, voterLastTerm, voterLastIndex uint64) bool {
	if candidateLastTerm != voterLastTerm {
		return candidateLastTerm > voterLastTerm
	}
	return candidateLastIndex >= voterLastIndex
}

// VoteTracker collects votes during a leader election round.
type VoteTracker struct {
	votes    map[string]bool // nodeID → whether they granted their vote
	majority int             // number of votes needed (⌊N/2⌋ + 1)
}

// NewVoteTracker creates a VoteTracker for a cluster of the given size.
func NewVoteTracker(clusterSize int) *VoteTracker {
	if clusterSize <= 0 {
		clusterSize = 1
	}
	return &VoteTracker{
		votes:    make(map[string]bool),
		majority: (clusterSize / 2) + 1,
	}
}

// RecordVote records a vote from a peer node.
func (v *VoteTracker) RecordVote(nodeID string, granted bool) {
	v.votes[nodeID] = granted
}

// HasMajority returns true if the number of granted votes >= majority threshold.
func (v *VoteTracker) HasMajority() bool {
	granted := 0
	for _, g := range v.votes {
		if g {
			granted++
		}
	}
	return granted >= v.majority
}

// Reset clears all votes for a new election round.
func (v *VoteTracker) Reset() {
	v.votes = make(map[string]bool)
}

// ---------------------------------------------------------------------------
// ZooKeeper Election Helpers
// ---------------------------------------------------------------------------

// EnsureElectionPath creates the /election znode in ZooKeeper if it doesn't exist.
func EnsureElectionPath(conn *zk.Conn) error {
	exists, _, err := conn.Exists(ElectionZNode)
	if err != nil {
		return fmt.Errorf("checking %s: %w", ElectionZNode, err)
	}
	if !exists {
		_, err = conn.Create(ElectionZNode, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return fmt.Errorf("creating %s: %w", ElectionZNode, err)
		}
	}
	return nil
}

// CreateElectionNode creates an ephemeral sequential znode under /election.
// Returns the full path of the created znode (e.g., /election/node-0000000001).
func CreateElectionNode(conn *zk.Conn, nodeID string) (string, error) {
	path, err := conn.Create(
		ElectionZNode+"/node-",
		[]byte(nodeID),
		zk.FlagEphemeral|zk.FlagSequence,
		zk.WorldACL(zk.PermAll),
	)
	if err != nil {
		return "", fmt.Errorf("creating election node: %w", err)
	}
	return path, nil
}

// GetSortedCandidates returns all children of /election sorted by sequence number.
func GetSortedCandidates(conn *zk.Conn) ([]string, error) {
	children, _, err := conn.Children(ElectionZNode)
	if err != nil {
		return nil, fmt.Errorf("listing election children: %w", err)
	}
	sort.Strings(children)
	return children, nil
}

// ExtractZnodeName extracts just the znode name from a full path.
// Example: "/election/node-0000000001" → "node-0000000001"
func ExtractZnodeName(fullPath string) string {
	parts := strings.Split(fullPath, "/")
	return parts[len(parts)-1]
}

// DetermineRole checks if the given znode path is the smallest (= leader).
// Returns the role and the path of the node to watch (previous in sequence).
func DetermineRole(conn *zk.Conn, myZnode string) (NodeState, string, error) {
	candidates, err := GetSortedCandidates(conn)
	if err != nil {
		return Follower, "", err
	}
	if len(candidates) == 0 {
		return Follower, "", fmt.Errorf("no election candidates found")
	}

	myName := ExtractZnodeName(myZnode)

	// If we are the smallest, we are the leader
	if candidates[0] == myName {
		return Leader, "", nil
	}

	// Find ourselves and watch the node just before us
	for i, c := range candidates {
		if c == myName && i > 0 {
			watchTarget := ElectionZNode + "/" + candidates[i-1]
			return Follower, watchTarget, nil
		}
	}

	return Follower, "", fmt.Errorf("own znode %s not found in candidates", myName)
}
