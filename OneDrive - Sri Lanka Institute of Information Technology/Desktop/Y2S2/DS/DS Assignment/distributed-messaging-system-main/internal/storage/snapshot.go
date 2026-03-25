// =============================================================================
// Module: Storage
// File: snapshot.go
// Responsible Member: Sabeelur Rashaad — Time Synchronization & Storage
// Purpose: Defines the Snapshot interface for log compaction. Over time, the
//          replicated log grows without bound. Snapshotting captures the current
//          state at a given index, allowing old log entries to be discarded.
//          This is an optional optimization for this academic project.
//
// Connections:
//   - Used by internal/replication/log.go to compact old entries.
//   - Used by internal/fault/recovery.go to bootstrap a recovering node
//     from a snapshot instead of replaying the entire log.
//   - The snapshot includes the last applied index and term, plus the
//     full state of the MessageStore at that point.
//
// Key concepts:
//   - Log compaction: periodically save state and discard old log entries.
//   - InstallSnapshot RPC: a leader can send a snapshot to a follower
//     that is too far behind (instead of sending thousands of entries).
//   - For this project, snapshot is optional but demonstrates understanding
//     of the Raft log compaction mechanism.
// =============================================================================
package storage

// Snapshot represents a point-in-time capture of the system state.
// It contains the last included log index and term, plus the serialized state.
type Snapshot struct {
	LastIndex uint64 // the last log index included in this snapshot
	LastTerm  uint64 // the term of LastIndex
	Data      []byte // serialized state (all committed messages up to LastIndex)
}

// SnapshotManager defines the interface for creating and loading snapshots.
type SnapshotManager interface {
	// CreateSnapshot captures the system state at the given index.
	CreateSnapshot(lastIndex, lastTerm uint64, state []byte) (*Snapshot, error)

	// LoadSnapshot loads the latest snapshot from storage.
	LoadSnapshot() (*Snapshot, error)

	// HasSnapshot returns true if a snapshot exists.
	HasSnapshot() bool
}

// FileSnapshotManager implements SnapshotManager using file-based storage.
type FileSnapshotManager struct {
	dir string // directory to store snapshot files
}

// NewFileSnapshotManager creates a new snapshot manager at the given directory.
//
// TODO (optional): Create the directory if it doesn't exist.
func NewFileSnapshotManager(dir string) *FileSnapshotManager {
	return nil
}

// CreateSnapshot captures the system state at the given log index.
//
// TODO (optional): Serialize the state to a file named by lastIndex.
func (f *FileSnapshotManager) CreateSnapshot(lastIndex, lastTerm uint64, state []byte) (*Snapshot, error) {
	return nil, nil
}

// LoadSnapshot loads the most recent snapshot from disk.
//
// TODO (optional): Find and deserialize the latest snapshot file.
func (f *FileSnapshotManager) LoadSnapshot() (*Snapshot, error) {
	return nil, nil
}

// HasSnapshot returns true if at least one snapshot file exists.
//
// TODO (optional): Check if the snapshot directory contains any files.
func (f *FileSnapshotManager) HasSnapshot() bool {
	return false
}
