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
	"errors"
	"sync"

	"distributed-messaging-system/internal/consensus"
)

// ReplicatedLog defines the interface for the append-only replicated log.
type ReplicatedLog interface {
	Append(entry consensus.LogEntry) error
	GetEntry(index uint64) (consensus.LogEntry, error)
	GetEntriesFrom(index uint64) ([]consensus.LogEntry, error)
	LastIndex() uint64
	LastTerm() uint64
	CommitUpTo(index uint64) error
	CommitIndex() uint64
}

// InMemoryLog implements ReplicatedLog with an in-memory append-only store.
type InMemoryLog struct {
	mu          sync.RWMutex
	entries     []consensus.LogEntry
	commitIndex uint64
}

// NewInMemoryLog creates a new empty replicated log.
//
// TODO: Initialize the entries slice and set commitIndex to 0.
func NewInMemoryLog() *InMemoryLog {
	return &InMemoryLog{
		entries:     make([]consensus.LogEntry, 0),
		commitIndex: 0,
	}
}

// Append adds a new entry to the log.
//
// TODO: Validate that entry.Index equals len(entries)+1 (no gaps allowed).
//       Then append the entry to the slice.
func (l *InMemoryLog) Append(entry consensus.LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	expectedIndex := uint64(len(l.entries) + 1)
	if entry.Index != expectedIndex {
		return errors.New("log append index mismatch: expected sequential index")
	}

	l.entries = append(l.entries, entry)
	return nil
}

// GetEntry returns the log entry at the given 1-based index.
//
// TODO: Validate index bounds and return entries[index-1].
func (l *InMemoryLog) GetEntry(index uint64) (consensus.LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index == 0 || index > uint64(len(l.entries)) {
		return consensus.LogEntry{}, errors.New("log index out of range")
	}

	return l.entries[index-1], nil
}

// GetEntriesFrom returns all entries from the given index to the end.
//
// TODO: Validate index and return a copy of entries[index-1:].
func (l *InMemoryLog) GetEntriesFrom(index uint64) ([]consensus.LogEntry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index == 0 || index > uint64(len(l.entries)) {
		return nil, errors.New("log index out of range")
	}

	result := make([]consensus.LogEntry, len(l.entries[index-1:]))
	copy(result, l.entries[index-1:])

	return result, nil
}

// LastIndex returns the index of the last log entry, or 0 if empty.
//
// TODO: Return len(entries) as uint64.
func (l *InMemoryLog) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return uint64(len(l.entries))
}

// LastTerm returns the term of the last log entry, or 0 if empty.
//
// TODO: Return the Term field of the last entry.
func (l *InMemoryLog) LastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if len(l.entries) == 0 {
		return 0
	}

	return l.entries[len(l.entries)-1].Term
}

// CommitUpTo advances the commit index to the given value.
//
// TODO: Validate that index does not exceed LastIndex.
//       Set commitIndex = max(commitIndex, index).
func (l *InMemoryLog) CommitUpTo(index uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if index > uint64(len(l.entries)) {
		return errors.New("commit index out of bounds")
	}

	if index > l.commitIndex {
		l.commitIndex = index
	}

	return nil
}

// CommitIndex returns the current commit index.
//
// TODO: Return l.commitIndex with proper locking.
func (l *InMemoryLog) CommitIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.commitIndex
}