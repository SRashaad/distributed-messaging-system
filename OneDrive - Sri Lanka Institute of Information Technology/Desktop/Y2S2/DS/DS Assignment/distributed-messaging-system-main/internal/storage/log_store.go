// =============================================================================
// Module: Storage
// File: log_store.go
// Responsible Member: Sabeelur Rashaad — Time Synchronization & Storage
// Purpose: Provides a persistent log storage interface for crash recovery.
//          In a production Raft implementation, the log and Raft state
//          (currentTerm, votedFor) must survive crashes. This file defines
//          the interface for durable log storage.
//
// Connections:
//   - Used by internal/replication/log.go as a durable backend (optional).
//   - Used by internal/consensus/raft.go to persist currentTerm and votedFor.
//   - For this academic project, the in-memory implementation is sufficient,
//     but this interface shows the correct design for a real system.
//
// Key concepts:
//   - WAL (Write-Ahead Log): entries are written to durable storage before
//     they are acknowledged, ensuring no data loss on crash.
//   - Raft requires that currentTerm and votedFor are persisted so that
//     a node does not vote twice in the same term after a restart.
// =============================================================================
package storage

import "distributed-messaging-system/internal/consensus"

// LogStore defines the interface for persistent log storage.
// In a production system, this would write to disk or a database.
type LogStore interface {
	// AppendEntries writes log entries to durable storage.
	AppendEntries(entries []consensus.LogEntry) error

	// GetEntries reads log entries from durable storage in the given range.
	GetEntries(from, to uint64) ([]consensus.LogEntry, error)

	// TruncateAfter removes all entries after the given index.
	// Used during log conflict resolution in AppendEntries.
	TruncateAfter(index uint64) error

	// SaveState persists the Raft hard state (currentTerm, votedFor).
	SaveState(term uint64, votedFor string) error

	// LoadState loads the persisted Raft hard state.
	LoadState() (term uint64, votedFor string, err error)
}

// FileLogStore implements LogStore using file-based storage.
// For this academic project, implementation is optional —
// the in-memory log in replication/log.go is sufficient.
type FileLogStore struct {
	path string // directory path for log files
}

// NewFileLogStore creates a new file-based log store at the given path.
//
// TODO (optional): Create the directory if it doesn't exist.
func NewFileLogStore(path string) *FileLogStore {
	return nil
}

// AppendEntries writes log entries to durable storage.
//
// TODO (optional): Serialize entries and write to a file.
func (f *FileLogStore) AppendEntries(entries []consensus.LogEntry) error {
	return nil
}

// GetEntries reads log entries from durable storage.
//
// TODO (optional): Read and deserialize entries from the file.
func (f *FileLogStore) GetEntries(from, to uint64) ([]consensus.LogEntry, error) {
	return nil, nil
}

// TruncateAfter removes all entries after the given index.
// This is needed when a follower's log conflicts with the leader's.
//
// TODO (optional): Truncate the log file at the appropriate position.
func (f *FileLogStore) TruncateAfter(index uint64) error {
	return nil
}

// SaveState persists currentTerm and votedFor to survive crashes.
//
// TODO (optional): Write term and votedFor to a state file.
func (f *FileLogStore) SaveState(term uint64, votedFor string) error {
	return nil
}

// LoadState loads the previously persisted Raft state on startup.
//
// TODO (optional): Read term and votedFor from the state file.
func (f *FileLogStore) LoadState() (term uint64, votedFor string, err error) {
	return 0, "", nil
}
