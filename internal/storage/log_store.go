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

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"distributed-messaging-system/internal/consensus"
)

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
func NewFileLogStore(path string) *FileLogStore {
	os.MkdirAll(path, 0755)
	return &FileLogStore{
		path: path,
	}
}

// AppendEntries writes log entries to durable storage.
func (f *FileLogStore) AppendEntries(entries []consensus.LogEntry) error {
	file, err := os.OpenFile(filepath.Join(f.path, "log.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	for _, e := range entries {
		if err := encoder.Encode(e); err != nil {
			return err
		}
	}
	return nil
}

// GetEntries reads log entries from durable storage.
func (f *FileLogStore) GetEntries(from, to uint64) ([]consensus.LogEntry, error) {
	file, err := os.Open(filepath.Join(f.path, "log.jsonl"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	var results []consensus.LogEntry
	decoder := json.NewDecoder(file)
	for {
		var entry consensus.LogEntry
		if err := decoder.Decode(&entry); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		if entry.Index >= from && entry.Index <= to {
			results = append(results, entry)
		}
	}
	return results, nil
}

// TruncateAfter removes all entries after the given index.
func (f *FileLogStore) TruncateAfter(index uint64) error {
	entries, err := f.GetEntries(1, index)
	if err != nil {
		return err
	}
	os.Remove(filepath.Join(f.path, "log.jsonl")) // overwrite
	return f.AppendEntries(entries)
}

// SaveState persists currentTerm and votedFor to survive crashes.
func (f *FileLogStore) SaveState(term uint64, votedFor string) error {
	state := map[string]interface{}{"term": term, "votedFor": votedFor}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(f.path, "state.json"), data, 0644)
}

// LoadState loads the previously persisted Raft state on startup.
func (f *FileLogStore) LoadState() (term uint64, votedFor string, err error) {
	data, err := os.ReadFile(filepath.Join(f.path, "state.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, "", nil
		}
		return 0, "", err
	}
	var state map[string]interface{}
	if err := json.Unmarshal(data, &state); err != nil {
		return 0, "", err
	}
	
	if t, ok := state["term"].(float64); ok {
		term = uint64(t)
	}
	if v, ok := state["votedFor"].(string); ok {
		votedFor = v
	}
	return term, votedFor, nil
}
