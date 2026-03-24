// =============================================================================
// Module: Replication
// File: quorum.go
// Responsible Member: Senul Mintharu — Data Replication & Consistency
// Purpose: Tracks acknowledgments from follower nodes for each log index
//          and determines whether a quorum (majority) has been reached.
//          Quorum-based commit is the core safety mechanism: an entry is
//          only committed when a majority of nodes have stored it.
//
// Connections:
//   - Used by manager.go to record ACKs as followers respond.
//   - Used by raft.go to decide when an entry can be committed.
//   - Majority threshold = ⌊N/2⌋ + 1 where N = cluster size.
//
// Key concept:
//   - In a 3-node cluster, quorum = 2 (leader + 1 follower).
//   - In a 5-node cluster, quorum = 3 (leader + 2 followers).
//   - This ensures that any two quorums share at least one common node,
//     which guarantees committed entries are never lost.
// =============================================================================
package replication

import "sync"

// QuorumTracker tracks acknowledgments from followers for each log index.
type QuorumTracker struct {
	mu          sync.RWMutex
	clusterSize int                        // total number of nodes in the cluster
	majority    int                        // quorum threshold (clusterSize/2 + 1)
	acks        map[uint64]map[string]bool // log index → (nodeID → acknowledged)
}

// NewQuorumTracker creates a new QuorumTracker for the given cluster size.
// The majority is calculated as clusterSize/2 + 1.
//
// TODO: Initialize all fields and compute the majority threshold.
func NewQuorumTracker(clusterSize int) *QuorumTracker {
	if clusterSize <= 0 {
		clusterSize = 1
	}

	return &QuorumTracker{
		clusterSize: clusterSize,
		majority:    (clusterSize / 2) + 1,
		acks:        make(map[uint64]map[string]bool),
	}
}

// RecordAck records that a follower has acknowledged (stored) the entry
// at the given log index. Called when an AppendEntries response arrives
// with Success=true.
//
// TODO: Create the inner map if it doesn't exist, then set acks[index][nodeID] = true.
func (q *QuorumTracker) RecordAck(index uint64, nodeID string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// create map for this index if missing
	if q.acks[index] == nil {
		q.acks[index] = make(map[string]bool)
	}

	// record ack
	q.acks[index][nodeID] = true
}

// HasQuorum returns true if the number of acknowledgments for the given
// index meets or exceeds the majority threshold.
// This is the commit condition.
//
// TODO: Compare len(acks[index]) against q.majority.
func (q *QuorumTracker) HasQuorum(index uint64) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return len(q.acks[index]) >= q.majority
}

// AckCount returns the number of acknowledgments received so far for
// the given index. Useful for debugging and monitoring.
//
// TODO: Return len(acks[index]) with proper locking.
func (q *QuorumTracker) AckCount(index uint64) int {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return len(q.acks[index])
}

// Cleanup removes tracking data for indices at or below the given index.
// Prevents unbounded memory growth.
//
// TODO: Iterate over acks and delete entries where idx <= belowIndex.
func (q *QuorumTracker) Cleanup(belowIndex uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for idx := range q.acks {
		if idx <= belowIndex {
			delete(q.acks, idx)
		}
	}
}