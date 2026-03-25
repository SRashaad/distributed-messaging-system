package fault

import (
	"errors"
	"fmt"
	"testing"

	"distributed-messaging-system/internal/consensus"
	"distributed-messaging-system/internal/replication"
)

type mockLog struct {
	entries     []consensus.LogEntry
	commitIndex uint64
}

func newMockLog(entries ...consensus.LogEntry) *mockLog {
	return &mockLog{entries: entries}
}

func (m *mockLog) Append(e consensus.LogEntry) error {
	m.entries = append(m.entries, e)
	return nil
}

func (m *mockLog) GetEntry(index uint64) (consensus.LogEntry, error) {
	if index == 0 || int(index) > len(m.entries) {
		return consensus.LogEntry{}, fmt.Errorf("index %d out of range", index)
	}
	return m.entries[index-1], nil
}

func (m *mockLog) GetEntriesFrom(index uint64) ([]consensus.LogEntry, error) {
	if index == 0 || int(index) > len(m.entries) {
		return nil, fmt.Errorf("index %d out of range", index)
	}
	return m.entries[index-1:], nil
}

func (m *mockLog) LastIndex() uint64         { return uint64(len(m.entries)) }
func (m *mockLog) LastTerm() uint64          { return 0 }
func (m *mockLog) CommitUpTo(i uint64) error { m.commitIndex = i; return nil }
func (m *mockLog) CommitIndex() uint64       { return m.commitIndex }

var _ replication.ReplicatedLog = (*mockLog)(nil)

func makeEntries(count int) []consensus.LogEntry {
	entries := make([]consensus.LogEntry, count)
	for i := range entries {
		entries[i] = consensus.LogEntry{
			Index: uint64(i + 1),
			Term:  1,
			Data:  []byte(fmt.Sprintf("msg-%d", i+1)),
		}
	}
	return entries
}

func TestLogRecovery_AlreadyUpToDate(t *testing.T) {
	log := newMockLog(makeEntries(5)...)
	queryFn := func(_ string) (uint64, error) { return 5, nil }
	var sent [][]consensus.LogEntry
	sendFn := func(_ string, entries []consensus.LogEntry) error {
		sent = append(sent, entries)
		return nil
	}
	r := NewLogRecovery(log, sendFn, queryFn)
	if err := r.InitiateRecovery("node2"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sent) != 0 {
		t.Error("expected no entries sent when peer is already up to date")
	}
}

func TestLogRecovery_SendsMissingEntries(t *testing.T) {
	log := newMockLog(makeEntries(5)...)
	calls := 0
	queryFn := func(_ string) (uint64, error) {
		calls++
		if calls == 1 {
			return 2, nil
		}
		return 5, nil
	}
	var received []consensus.LogEntry
	sendFn := func(_ string, batch []consensus.LogEntry) error {
		received = append(received, batch...)
		return nil
	}
	r := NewLogRecovery(log, sendFn, queryFn)
	if err := r.InitiateRecovery("node2"); err != nil {
		t.Fatalf("InitiateRecovery: %v", err)
	}
	if len(received) != 3 {
		t.Errorf("expected 3 entries sent, got %d", len(received))
	}
}

func TestLogRecovery_BatchSizeRespected(t *testing.T) {
	log := newMockLog(makeEntries(120)...)
	calls := 0
	queryFn := func(_ string) (uint64, error) {
		calls++
		if calls == 1 {
			return 0, nil
		}
		return 120, nil
	}
	var batches [][]consensus.LogEntry
	sendFn := func(_ string, batch []consensus.LogEntry) error {
		batches = append(batches, batch)
		return nil
	}
	r := NewLogRecovery(log, sendFn, queryFn)
	if err := r.InitiateRecovery("node2"); err != nil {
		t.Fatalf("InitiateRecovery: %v", err)
	}
	if len(batches) != 3 {
		t.Errorf("expected 3 batches, got %d", len(batches))
	}
}

func TestLogRecovery_RetriesOnTransientFailure(t *testing.T) {
	log := newMockLog(makeEntries(3)...)
	queryCalls := 0
	queryFn := func(_ string) (uint64, error) {
		queryCalls++
		if queryCalls == 1 {
			return 0, nil
		}
		return 3, nil
	}
	sendAttempts := 0
	sendFn := func(_ string, _ []consensus.LogEntry) error {
		sendAttempts++
		if sendAttempts == 1 {
			return errors.New("transient network error")
		}
		return nil
	}
	r := NewLogRecovery(log, sendFn, queryFn)
	if err := r.InitiateRecovery("node2"); err != nil {
		t.Fatalf("expected recovery after retry, got: %v", err)
	}
	if sendAttempts < 2 {
		t.Errorf("expected at least 2 send attempts, got %d", sendAttempts)
	}
}

func TestLogRecovery_PermanentSendFailure(t *testing.T) {
	log := newMockLog(makeEntries(3)...)
	queryFn := func(_ string) (uint64, error) { return 0, nil }
	sendFn := func(_ string, _ []consensus.LogEntry) error {
		return errors.New("permanent failure")
	}
	r := NewLogRecovery(log, sendFn, queryFn)
	if err := r.InitiateRecovery("node2"); err == nil {
		t.Fatal("expected error after permanent send failure, got nil")
	}
}

func TestLogRecovery_EmptyNodeID(t *testing.T) {
	log := newMockLog()
	queryFn := func(_ string) (uint64, error) { return 0, nil }
	sendFn := func(_ string, _ []consensus.LogEntry) error { return nil }
	r := NewLogRecovery(log, sendFn, queryFn)
	if err := r.InitiateRecovery(""); err == nil {
		t.Fatal("expected error for empty nodeID, got nil")
	}
}

func TestLogRecovery_QueryFails(t *testing.T) {
	log := newMockLog(makeEntries(3)...)
	queryFn := func(_ string) (uint64, error) {
		return 0, errors.New("peer unreachable")
	}
	sendFn := func(_ string, _ []consensus.LogEntry) error { return nil }
	r := NewLogRecovery(log, sendFn, queryFn)
	if err := r.InitiateRecovery("node2"); err == nil {
		t.Fatal("expected error when query fails, got nil")
	}
}

func TestLogRecovery_SyncLog_EmptyRange(t *testing.T) {
	log := newMockLog(makeEntries(3)...)
	sendFn := func(_ string, _ []consensus.LogEntry) error { return nil }
	queryFn := func(_ string) (uint64, error) { return 3, nil }
	r := NewLogRecovery(log, sendFn, queryFn)
	if err := r.SyncLog("node2", 10); err != nil {
		t.Fatalf("SyncLog with fromIndex > lastIndex should be no-op, got: %v", err)
	}
}

func TestLogRecovery_InconsistencyAfterSync(t *testing.T) {
	log := newMockLog(makeEntries(5)...)
	n := 0
	queryFn2 := func(_ string) (uint64, error) {
		n++
		if n == 1 {
			return 0, nil
		}
		return 3, nil
	}
	sendFn := func(_ string, _ []consensus.LogEntry) error { return nil }
	r := NewLogRecovery(log, sendFn, queryFn2)
	if err := r.InitiateRecovery("node2"); err == nil {
		t.Fatal("expected error when post-sync verification shows inconsistency")
	}
}