// =============================================================================
// Module: Replication
// File: log.go
// Responsible Member: Senul Mintharu — Data Replication & Consistency
// Purpose: Defines the replicated log interface and its in-memory
//          implementation. The replicated log is an append-only, ordered
//          sequence of LogEntry records. Every message published to the
//          system is stored as a log entry and replicated to all nodes.
//
// Connections:
//   - Uses consensus.LogEntry as the entry type (shared data structure).
//   - Used by internal/replication/manager.go to append and read entries.
//   - Used by internal/consensus/raft.go for AppendEntries consistency checks.
//   - Used by internal/fault/recovery.go to sync missed entries to recovering nodes.
//   - commitIndex is advanced by the quorum tracker once a majority acknowledges.
//
// Key concepts:
//   - Append-only: entries are never modified or deleted (only appended).
//   - 1-indexed: the first entry has Index=1.
//   - commitIndex: entries at or below this index are considered committed
//     and safe to deliver to clients.
// =============================================================================
package replication

import (
	"sync"

	"distributed-messaging-system/internal/consensus"
)

// ReplicatedLog defines the interface for the append-only replicated log.
// Both the leader and followers maintain their own copy of this log.
type ReplicatedLog interface {
	// Append adds a new entry to the end of the log.
	Append(entry consensus.LogEntry) error

	// GetEntry returns the log entry at the given 1-based index.
	GetEntry(index uint64) (consensus.LogEntry, error)

	// GetEntriesFrom returns all entries starting from the given index (inclusive).
	// Used during replication and recovery to send batches of entries.
	GetEntriesFrom(index uint64) ([]consensus.LogEntry, error)

	// LastIndex returns the index of the last entry, or 0 if the log is empty.
	LastIndex() uint64

	// LastTerm returns the term of the last entry, or 0 if the log is empty.
	LastTerm() uint64

	// CommitUpTo advances the commit index to the given value.
	CommitUpTo(index uint64) error

	// CommitIndex returns the current commit index.
	CommitIndex() uint64
}

// InMemoryLog implements ReplicatedLog with an in-memory append-only store.
// For a production system this would be backed by persistent storage,
// but in-memory is sufficient for this academic project.
type InMemoryLog struct {
	mu          sync.RWMutex       // protects entries and commitIndex
	entries     []consensus.LogEntry // the ordered log entries
	commitIndex uint64              // highest index known to be committed
}

// NewInMemoryLog creates a new empty replicated log.
//
// TODO: Initialize the entries slice and set commitIndex to 0.
func NewInMemoryLog() *InMemoryLog {
	return nil
}

// Append adds a new entry to the log.
//
// TODO: Validate that entry.Index equals len(entries)+1 (no gaps allowed).
//       Then append the entry to the slice.
func (l *InMemoryLog) Append(entry consensus.LogEntry) error {
	return nil
}

// GetEntry returns the log entry at the given 1-based index.
//
// TODO: Validate index bounds and return entries[index-1].
func (l *InMemoryLog) GetEntry(index uint64) (consensus.LogEntry, error) {
	return consensus.LogEntry{}, nil
}

// GetEntriesFrom returns all entries from the given index to the end of the log.
// Used by the leader to send missing entries to followers during replication.
//
// TODO: Validate index and return a copy of entries[index-1:].
func (l *InMemoryLog) GetEntriesFrom(index uint64) ([]consensus.LogEntry, error) {
	return nil, nil
}

// LastIndex returns the index of the last log entry, or 0 if empty.
//
// TODO: Return len(entries) as uint64.
func (l *InMemoryLog) LastIndex() uint64 {
	return 0
}

// LastTerm returns the term of the last log entry, or 0 if empty.
//
// TODO: Return the Term field of the last entry.
func (l *InMemoryLog) LastTerm() uint64 {
	return 0
}

// CommitUpTo advances the commit index to the given value.
// Called when the quorum tracker confirms a majority has acknowledged.
//
// TODO: Validate that index does not exceed LastIndex.
//       Set commitIndex = max(commitIndex, index).
func (l *InMemoryLog) CommitUpTo(index uint64) error {
	return nil
}

// CommitIndex returns the current commit index.
// Only entries at or below this index are safe to deliver to clients.
//
// TODO: Return l.commitIndex with proper locking.
func (l *InMemoryLog) CommitIndex() uint64 {
	return 0
}
