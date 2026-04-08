// =============================================================================
// Module: Replication
// File: manager.go
// Responsible Member: Senul Mintharu — Data Replication & Consistency
// Purpose: Orchestrates log replication from the Leader to all Followers.
//          When a client publishes a message, the leader appends it to its
//          local log and then uses the ReplicationManager to send the entry
//          to every follower via AppendEntries RPCs.
//
// Connections:
//   - Uses ReplicatedLog (log.go) to append entries locally.
//   - Uses QuorumTracker (quorum.go) to track follower acknowledgments.
//   - Uses internal/transport to send AppendEntries RPCs to peers.
//   - Called by internal/consensus/raft.go ProposeEntry() on the leader.
//
// Replication flow:
//   1. Leader appends entry to local log.
//   2. Leader sends AppendEntries to all followers in parallel.
//   3. Each follower ACK is recorded in the QuorumTracker.
//   4. Once a quorum (majority) acknowledges, the entry is committed.
// =============================================================================
package replication

import (
	"log"
	"time"

	"distributed-messaging-system/internal/consensus"
	"distributed-messaging-system/internal/transport"
	"distributed-messaging-system/internal/transport/proto"
)

// ReplicationManager defines the interface for orchestrating log replication.
type ReplicationManager interface {
	// ReplicateEntry appends an entry and sends it to all followers.
	ReplicateEntry(entry consensus.LogEntry) error

	// WaitForQuorum blocks until a majority acknowledges the entry, or times out.
	WaitForQuorum(index uint64, timeout time.Duration) (bool, error)
}

// Manager implements ReplicationManager.
type Manager struct {
	log    ReplicatedLog  // the local replicated log
	quorum *QuorumTracker // tracks follower acknowledgments
	peers  []string       // list of follower addresses
	client *transport.PeerClient
	nodeID string         // the current node's ID
}

// NewManager creates a new ReplicationManager.
func NewManager(log ReplicatedLog, clusterSize int, peers []string, client *transport.PeerClient, nodeID string) *Manager {
	return &Manager{
		log:    log,
		quorum: NewQuorumTracker(clusterSize),
		peers:  peers,
		client: client,
		nodeID: nodeID,
	}
}

// ReplicateEntry appends an entry to the local log and initiates
// replication to all followers via AppendEntries RPCs.
func (m *Manager) ReplicateEntry(entry consensus.LogEntry) error {

	// 1. Append locally
	if err := m.log.Append(entry); err != nil {
		return err
	}

	// Leader counts as an ACK immediately
	m.quorum.RecordAck(entry.Index, m.nodeID)

	// 2. For each follower, send AppendEntries
	for _, peer := range m.peers {
		p := peer // capture for goroutine
		
		go func() {
			req := &proto.AppendEntriesRequest{
				Term:         entry.Term,
				LeaderId:     m.nodeID,
				PrevLogIndex: entry.Index - 1,
				PrevLogTerm:  0, // Would query from log in prod, optional for basic replication
				Entries: []*proto.LogEntry{
					{
						Index:     entry.Index,
						Term:      entry.Term,
						Timestamp: entry.Timestamp,
						Data:      entry.Data,
					},
				},
				LeaderCommit: m.log.CommitIndex(),
			}

			// In real implementation, call transport.AppendEntries(p, entry)
			respIf, err := m.client.SendAppendEntries(p, req)
			if err != nil {
				log.Printf("[replication] Failed to send AppendEntries to %s: %v", p, err)
				return
			}
			
			resp, ok := respIf.(*proto.AppendEntriesResponse)
			if !ok {
				log.Printf("[replication] Invalid response from %s", p)
				return
			}

			// 3. Record acknowledgment
			if resp.Success {
				m.quorum.RecordAck(entry.Index, p)
			}
		}()
	}

	return nil
}

// WaitForQuorum blocks until a quorum (majority) has acknowledged the entry
// at the given index, or the timeout expires.
//
// TODO: Implement:
//   1. Periodically check quorum.HasQuorum(index).
//   2. Return true if quorum is reached before the timeout.
//   3. Return false if the timeout expires without quorum.
func (m *Manager) WaitForQuorum(index uint64, timeout time.Duration) (bool, error) {

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if m.quorum.HasQuorum(index) {
			log.Printf("[replication] quorum reached for index=%d (acks=%d)", index, m.quorum.AckCount(index))
			return true, nil
		}
		time.Sleep(10 * time.Millisecond)
	}

	log.Printf("[replication] quorum timeout for index=%d after %v (acks=%d)", index, timeout, m.quorum.AckCount(index))

	return false, nil
}