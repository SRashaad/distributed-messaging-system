// =============================================================================
// Module: Fault Tolerance
// File: redundancy_test.go
// Tests for the RedundancyManager, ErasureCoder, and helper utilities.
// =============================================================================
package fault

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"

	"distributed-messaging-system/internal/consensus"
)

// ---------------------------------------------------------------------------
// Mock ShardStore
// ---------------------------------------------------------------------------

// mockStore is an in-memory ShardStore used only in tests.
type mockStore struct {
	mu       sync.RWMutex
	replicas map[uint64]consensus.LogEntry
	shards   map[string][]byte // key: "logIndex:shardIndex"
	failOn   map[string]bool   // key: "store" or "load" to simulate failures
}

func newMockStore() *mockStore {
	return &mockStore{
		replicas: make(map[uint64]consensus.LogEntry),
		shards:   make(map[string][]byte),
		failOn:   make(map[string]bool),
	}
}

func (m *mockStore) StoreShard(logIndex uint64, shardIndex int, data []byte) error {
	if m.failOn["store"] {
		return errors.New("mock: store shard failed")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%d:%d", logIndex, shardIndex)
	m.shards[key] = append([]byte{}, data...)
	return nil
}

func (m *mockStore) LoadShard(logIndex uint64, shardIndex int) ([]byte, error) {
	if m.failOn["load"] {
		return nil, errors.New("mock: load shard failed")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := fmt.Sprintf("%d:%d", logIndex, shardIndex)
	d, ok := m.shards[key]
	if !ok {
		return nil, nil // absent shard — not an error
	}
	return d, nil
}

func (m *mockStore) StoreReplica(logIndex uint64, entry consensus.LogEntry) error {
	if m.failOn["store"] {
		return errors.New("mock: store replica failed")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replicas[logIndex] = entry
	return nil
}

func (m *mockStore) LoadReplica(logIndex uint64) (consensus.LogEntry, error) {
	if m.failOn["load"] {
		return consensus.LogEntry{}, errors.New("mock: load replica failed")
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.replicas[logIndex]
	if !ok {
		return consensus.LogEntry{}, fmt.Errorf("mock: no replica for index %d", logIndex)
	}
	return e, nil
}

// ---------------------------------------------------------------------------
// Erasure Coder Tests
// ---------------------------------------------------------------------------

func TestErasureCoder_EncodeDecodeRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		k, m    int
		payload string
	}{
		{"k=2 m=1 short", 2, 1, "hello world"},
		{"k=2 m=1 long", 2, 1, "The quick brown fox jumps over the lazy dog — repeated many times to test larger payloads in the erasure coding scheme."},
		{"k=3 m=2 medium", 3, 2, "distributed systems are fascinating"},
		{"k=4 m=2 binary-ish", 4, 2, "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09"},
		{"k=2 m=1 single byte", 2, 1, "X"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ec, err := NewErasureCoder(ErasureConfig{DataShards: tc.k, ParityShards: tc.m})
			if err != nil {
				t.Fatalf("NewErasureCoder: %v", err)
			}

			shards, err := ec.Encode([]byte(tc.payload))
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if len(shards) != tc.k+tc.m {
				t.Fatalf("expected %d shards, got %d", tc.k+tc.m, len(shards))
			}

			got, err := ec.Decode(shards)
			if err != nil {
				t.Fatalf("Decode (no loss): %v", err)
			}
			if !bytes.Equal(got, []byte(tc.payload)) {
				t.Fatalf("round-trip mismatch:\n  want %q\n  got  %q", tc.payload, got)
			}
		})
	}
}

func TestErasureCoder_RecoverMissingDataShard(t *testing.T) {
	ec, _ := NewErasureCoder(ErasureConfig{DataShards: 2, ParityShards: 1})
	payload := []byte("fault tolerance rocks")

	shards, _ := ec.Encode(payload)

	// Simulate losing data shard 0.
	shards[0] = nil

	got, err := ec.Decode(shards)
	if err != nil {
		t.Fatalf("Decode with missing shard[0]: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("recovery mismatch:\n  want %q\n  got  %q", payload, got)
	}
}

func TestErasureCoder_RecoverMissingParityShard(t *testing.T) {
	ec, _ := NewErasureCoder(ErasureConfig{DataShards: 2, ParityShards: 1})
	payload := []byte("parity shard test")

	shards, _ := ec.Encode(payload)

	// Simulate losing parity shard.
	shards[2] = nil

	got, err := ec.Decode(shards)
	if err != nil {
		t.Fatalf("Decode with missing parity shard: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("recovery mismatch:\n  want %q\n  got  %q", payload, got)
	}
}

func TestErasureCoder_TooManyShardsLost(t *testing.T) {
	ec, _ := NewErasureCoder(ErasureConfig{DataShards: 2, ParityShards: 1})
	payload := []byte("unrecoverable")

	shards, _ := ec.Encode(payload)

	// Lose 2 shards — exceeds m=1 tolerance.
	shards[0] = nil
	shards[1] = nil

	_, err := ec.Decode(shards)
	if err == nil {
		t.Fatal("expected error when too many shards lost, got nil")
	}
}

func TestErasureCoder_InvalidConfig(t *testing.T) {
	_, err := NewErasureCoder(ErasureConfig{DataShards: 0, ParityShards: 1})
	if err == nil {
		t.Fatal("expected error for DataShards=0")
	}
	_, err = NewErasureCoder(ErasureConfig{DataShards: 2, ParityShards: 0})
	if err == nil {
		t.Fatal("expected error for ParityShards=0")
	}
}

// ---------------------------------------------------------------------------
// Replication Strategy Tests
// ---------------------------------------------------------------------------

func TestRedundancyManager_Replication_StoreAndReconstruct(t *testing.T) {
	stores := []ShardStore{newMockStore(), newMockStore(), newMockStore()}
	rm, err := NewRedundancyManager(stores, StrategyReplication, ErasureConfig{})
	if err != nil {
		t.Fatalf("NewRedundancyManager: %v", err)
	}

	entry := consensus.LogEntry{Index: 1, Term: 1, Data: []byte("hello distributed world")}

	report, err := rm.StoreWithReplication(entry)
	if err != nil {
		t.Fatalf("StoreWithReplication: %v", err)
	}

	if report.CopiesStored != 3 {
		t.Errorf("expected 3 copies stored, got %d", report.CopiesStored)
	}
	if report.OverheadPercent != 300 {
		t.Errorf("expected 300%% overhead for 3-copy replication, got %d%%", report.OverheadPercent)
	}

	// Reconstruct from store (simulating all stores alive).
	data, err := rm.ReconstructMessage(1)
	if err != nil {
		t.Fatalf("ReconstructMessage: %v", err)
	}
	if !bytes.Equal(data, entry.Data) {
		t.Fatalf("data mismatch: want %q got %q", entry.Data, data)
	}
}

func TestRedundancyManager_Replication_ToleratesOneLoss(t *testing.T) {
	s0 := newMockStore()
	s1 := newMockStore()
	s2 := newMockStore()
	stores := []ShardStore{s0, s1, s2}

	rm, _ := NewRedundancyManager(stores, StrategyReplication, ErasureConfig{})
	entry := consensus.LogEntry{Index: 2, Term: 1, Data: []byte("survive one failure")}
	rm.StoreWithReplication(entry)

	// Kill store 0 — make it fail on load.
	s0.failOn["load"] = true

	data, err := rm.ReconstructMessage(2)
	if err != nil {
		t.Fatalf("expected reconstruction from surviving replica, got: %v", err)
	}
	if !bytes.Equal(data, entry.Data) {
		t.Fatalf("data mismatch")
	}
}

func TestRedundancyManager_Replication_AllStoresFail(t *testing.T) {
	s0, s1 := newMockStore(), newMockStore()
	s0.failOn["store"] = true
	s1.failOn["store"] = true
	stores := []ShardStore{s0, s1}

	rm, _ := NewRedundancyManager(stores, StrategyReplication, ErasureConfig{})
	entry := consensus.LogEntry{Index: 3, Term: 1, Data: []byte("all dead")}

	_, err := rm.StoreWithReplication(entry)
	if err == nil {
		t.Fatal("expected error when all stores fail")
	}
}

// ---------------------------------------------------------------------------
// Erasure Coding Strategy Tests
// ---------------------------------------------------------------------------

func TestRedundancyManager_ErasureCoding_StoreAndReconstruct(t *testing.T) {
	// 3 stores: 2 data shards + 1 parity shard
	stores := []ShardStore{newMockStore(), newMockStore(), newMockStore()}
	cfg := ErasureConfig{DataShards: 2, ParityShards: 1}

	rm, err := NewRedundancyManager(stores, StrategyErasureCoding, cfg)
	if err != nil {
		t.Fatalf("NewRedundancyManager: %v", err)
	}

	payload := []byte("erasure coded message payload for testing")
	entry := consensus.LogEntry{Index: 10, Term: 2, Data: payload}

	report, err := rm.StoreWithReplication(entry)
	if err != nil {
		t.Fatalf("StoreWithReplication: %v", err)
	}

	if report.CopiesStored != 3 {
		t.Errorf("expected 3 shards stored, got %d", report.CopiesStored)
	}
	// Erasure overhead should be less than 200% for 2+1.
	if report.OverheadPercent > 200 {
		t.Errorf("overhead %d%% seems too high for 2+1 erasure coding", report.OverheadPercent)
	}

	data, err := rm.ReconstructMessage(10)
	if err != nil {
		t.Fatalf("ReconstructMessage: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("data mismatch:\n  want %q\n  got  %q", payload, data)
	}
}

func TestRedundancyManager_ErasureCoding_ToleratesOneStoreLoss(t *testing.T) {
	s0 := newMockStore()
	s1 := newMockStore()
	s2 := newMockStore()
	stores := []ShardStore{s0, s1, s2}
	cfg := ErasureConfig{DataShards: 2, ParityShards: 1}

	rm, _ := NewRedundancyManager(stores, StrategyErasureCoding, cfg)
	payload := []byte("single shard loss recovery test")
	entry := consensus.LogEntry{Index: 11, Term: 2, Data: payload}
	rm.StoreWithReplication(entry)

	// Lose shard 0 (data shard).
	s0.failOn["load"] = true

	data, err := rm.ReconstructMessage(11)
	if err != nil {
		t.Fatalf("reconstruction after losing shard 0: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("data mismatch after reconstruction")
	}
}

func TestRedundancyManager_ErasureCoding_NotEnoughStores(t *testing.T) {
	// Only 2 stores but erasure config needs 3 shards.
	stores := []ShardStore{newMockStore(), newMockStore()}
	cfg := ErasureConfig{DataShards: 2, ParityShards: 1}

	_, err := NewRedundancyManager(stores, StrategyErasureCoding, cfg)
	if err == nil {
		t.Fatal("expected error when fewer stores than shards")
	}
}

// ---------------------------------------------------------------------------
// StorageOverhead Metrics Test
// ---------------------------------------------------------------------------

func TestRedundancyManager_StorageOverheadMetrics(t *testing.T) {
	stores := []ShardStore{newMockStore(), newMockStore(), newMockStore()}
	rm, _ := NewRedundancyManager(stores, StrategyReplication, ErasureConfig{})

	for i := uint64(1); i <= 5; i++ {
		entry := consensus.LogEntry{Index: i, Term: 1, Data: []byte("msg")}
		rm.StoreWithReplication(entry)
	}

	metrics := rm.StorageOverhead()
	if metrics.TotalOriginalBytes == 0 {
		t.Error("expected non-zero original bytes")
	}
	if metrics.TotalReplicatedBytes < metrics.TotalOriginalBytes {
		t.Error("replicated bytes should be >= original bytes")
	}
	if metrics.OverheadPercent < 100 {
		t.Errorf("overhead %d%% should be >= 100%%", metrics.OverheadPercent)
	}
}

// ---------------------------------------------------------------------------
// Helper Utilities Tests
// ---------------------------------------------------------------------------

func TestRotateLeftRight_Inverse(t *testing.T) {
	original := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	for n := 0; n < len(original); n++ {
		rotated := rotateLeft(original, n)
		restored := rotateRight(rotated, n)
		if !bytes.Equal(original, restored) {
			t.Errorf("rotate n=%d: roundtrip failed: got %v", n, restored)
		}
	}
}

func TestXorInPlace(t *testing.T) {
	a := []byte{0b10101010, 0b11001100}
	b := []byte{0b11110000, 0b00001111}
	xorInPlace(a, b)
	expected := []byte{0b01011010, 0b11000011}
	if !bytes.Equal(a, expected) {
		t.Fatalf("XOR result %v != expected %v", a, expected)
	}
}

func TestEncodeDecode_LengthHeader(t *testing.T) {
	data := []byte("test payload")
	padded := encodeLength(len(data), data, 3)
	origLen, payload, err := decodeLength(padded)
	if err != nil {
		t.Fatalf("decodeLength: %v", err)
	}
	if origLen != len(data) {
		t.Fatalf("origLen: want %d got %d", len(data), origLen)
	}
	if !bytes.Equal(payload[:origLen], data) {
		t.Fatalf("payload mismatch")
	}
}
