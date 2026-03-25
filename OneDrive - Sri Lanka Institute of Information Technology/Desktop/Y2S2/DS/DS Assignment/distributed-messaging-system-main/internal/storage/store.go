// =============================================================================
// Module: Storage
// File: store.go
// Responsible Member: Sabeelur Rashaad — Time Synchronization & Storage
// Purpose: Defines the MessageStore interface and its in-memory implementation.
//          The MessageStore provides persistent (or simulated persistent) storage
//          for committed messages. Once a log entry is committed via quorum
//          acknowledgment, it is applied to the MessageStore for client consumption.
//
// Connections:
//   - Receives committed entries from internal/replication (after quorum commit).
//   - Queried by internal/transport/grpc_server.go when clients send Consume requests.
//   - Uses consensus.LogEntry as the storage format.
//   - In a production system, this would be backed by disk or a database,
//     but an in-memory implementation is sufficient for this academic project.
//
// Key concepts:
//   - Only committed entries are stored here (safety guarantee).
//   - Messages are stored in order and can be retrieved by index range.
//   - This separation (log vs. store) mirrors the Raft state machine concept:
//     the log is the replication mechanism, the store is the "applied state."
// =============================================================================
package storage

import (
	"sync"

	"distributed-messaging-system/internal/consensus"
)

// MessageStore defines the interface for storing committed messages.
type MessageStore interface {
	// Apply stores a committed log entry in the message store.
	Apply(entry consensus.LogEntry) error

	// Get retrieves a message by its log index.
	Get(index uint64) (consensus.LogEntry, error)

	// GetRange retrieves all messages with indices in [from, to] inclusive.
	GetRange(from, to uint64) ([]consensus.LogEntry, error)

	// LastApplied returns the index of the last applied (stored) entry.
	LastApplied() uint64
}

// InMemoryStore implements MessageStore using an in-memory map.
// This is the "state machine" in Raft terminology — committed entries
// are applied here and become visible to clients.
type InMemoryStore struct {
	mu          sync.RWMutex               // protects messages and lastIndex
	messages    map[uint64]consensus.LogEntry // index → committed entry
	lastIndex   uint64                       // index of the last applied entry
}

// NewInMemoryStore creates a new empty message store.
//
// TODO: Initialize the messages map and set lastIndex to 0.
func NewInMemoryStore() *InMemoryStore {
	return nil
}

// Apply stores a committed log entry in the message store.
// Called after an entry has been committed (quorum reached).
// Entries must be applied in order (no gaps allowed).
//
// TODO: Implement:
//   1. Validate that entry.Index == lastIndex + 1 (sequential application).
//   2. Store the entry in the messages map.
//   3. Update lastIndex.
func (s *InMemoryStore) Apply(entry consensus.LogEntry) error {
	return nil
}

// Get retrieves a single committed message by its log index.
//
// TODO: Look up the entry in the messages map and return it.
//       Return an error if the index has not been applied yet.
func (s *InMemoryStore) Get(index uint64) (consensus.LogEntry, error) {
	return consensus.LogEntry{}, nil
}

// GetRange retrieves all committed messages with indices in [from, to] inclusive.
// Used by the Consume RPC to return batches of messages to clients.
//
// TODO: Iterate from 'from' to 'to', collect entries, return the slice.
//       Return an error if any index in the range has not been applied.
func (s *InMemoryStore) GetRange(from, to uint64) ([]consensus.LogEntry, error) {
	return nil, nil
}

// LastApplied returns the index of the last entry that has been applied
// to the store. Clients can use this to know how many messages are available.
//
// TODO: Return s.lastIndex with proper locking.
func (s *InMemoryStore) LastApplied() uint64 {
	return 0
}
