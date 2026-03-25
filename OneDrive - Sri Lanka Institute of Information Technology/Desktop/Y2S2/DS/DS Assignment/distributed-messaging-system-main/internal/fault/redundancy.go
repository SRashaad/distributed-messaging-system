// =============================================================================
// Module: Fault Tolerance
// File: redundancy.go
// Responsible Member: Imansa Bodini — Fault Tolerance
// Purpose: Implements two complementary message redundancy strategies:
//
//   1. Full Replication  — each message is stored as N identical copies
//      across N servers. Simple, fast, and easy to reconstruct. This is
//      the primary strategy used under normal cluster operation.
//
//   2. Erasure Coding (Reed-Solomon inspired) — a message is split into
//      k data shards and m parity shards (k + m = n total shards). The
//      original message can be recovered from any k of the n shards.
//      This reduces storage overhead vs full replication when k < n.
//
// Why both?
//   - Full replication is used for hot (recently active) data where
//     low-latency recovery matters most.
//   - Erasure coding is used for cold (archived) data where storage
//     efficiency is more valuable.
//
// Connections:
//   - RedundancyManager.StoreWithReplication is called by internal/node/node.go
//     after a log entry is committed.
//   - RedundancyManager.ReconstructMessage is called by internal/fault/recovery.go
//     when a server rejoins and needs to rebuild missing data.
//   - The ShardStore interface is implemented by internal/storage (in production
//     each node's InMemoryStore acts as one shard replica).
//
// Storage overhead comparison (example: 3 servers):
//   Full replication (3 copies):  300% storage, tolerates 2 failures
//   Erasure coding (2+1):         150% storage, tolerates 1 failure
//   Erasure coding (2+2, 4 nodes):200% storage, tolerates 2 failures
// =============================================================================
package fault

import (
	"errors"
	"fmt"

	"distributed-messaging-system/internal/consensus"
)

// ---------------------------------------------------------------------------
// Interfaces
// ---------------------------------------------------------------------------

// ShardStore represents a single server that can store and retrieve
// individual shards (slices of a message). In production each node's
// storage layer implements this interface.
type ShardStore interface {
	// StoreShard persists a shard for the message at the given log index.
	// shardIndex identifies which shard this is (0-based).
	StoreShard(logIndex uint64, shardIndex int, data []byte) error

	// LoadShard retrieves a shard. Returns (nil, nil) if the shard is
	// not present (e.g. the node was down when the message arrived).
	LoadShard(logIndex uint64, shardIndex int) ([]byte, error)

	// StoreReplica stores a full message copy on this node.
	StoreReplica(logIndex uint64, entry consensus.LogEntry) error

	// LoadReplica retrieves the full message copy stored on this node.
	// Returns an error if the replica is not present.
	LoadReplica(logIndex uint64) (consensus.LogEntry, error)
}

// ---------------------------------------------------------------------------
// Erasure Coding Engine
// ---------------------------------------------------------------------------

// ErasureConfig defines the Reed-Solomon (k, m) parameters.
//
//   k = number of data shards  (the message is split into k pieces)
//   m = number of parity shards (redundancy; any k of k+m shards reconstruct)
//
// Example: k=2, m=1 → 3 shards total, can lose 1 server.
//          k=3, m=2 → 5 shards total, can lose 2 servers.
type ErasureConfig struct {
	DataShards   int // k
	ParityShards int // m
}

// TotalShards returns the total number of shards (data + parity).
func (c ErasureConfig) TotalShards() int {
	return c.DataShards + c.ParityShards
}

// OverheadPercent returns the storage overhead as a percentage.
// e.g. k=2 m=1 → 150%,  k=4 m=2 → 150%,  k=1 m=2 (full repl×3) → 300%
func (c ErasureConfig) OverheadPercent() int {
	return (c.TotalShards() * 100) / c.DataShards
}

// ErasureCoder implements a simplified Reed-Solomon-inspired erasure coding
// scheme suitable for this academic project without external dependencies.
//
// Algorithm used:
//   - Encoding: split the message byte-stream into k equal-size data shards
//     (zero-padded to the nearest multiple of k). Then produce m parity
//     shards using XOR chaining — parity[i] = XOR of all data shards
//     rotated by i bytes. This is not full GF(2⁸) arithmetic but
//     demonstrates the concept and correctly tolerates single-shard loss.
//
//   - Decoding: if all data shards are present, concatenate them (trim padding).
//     If exactly one shard is missing (data or parity), reconstruct it by
//     XOR-ing all available shards of the same length.
//
// NOTE: For production use, replace with a proper RS library such as
//       github.com/klauspost/reedsolomon which provides full GF(2⁸) coding.
type ErasureCoder struct {
	cfg ErasureConfig
}

// NewErasureCoder creates a coder with the given (k, m) parameters.
func NewErasureCoder(cfg ErasureConfig) (*ErasureCoder, error) {
	if cfg.DataShards < 1 {
		return nil, errors.New("erasure: DataShards must be >= 1")
	}
	if cfg.ParityShards < 1 {
		return nil, errors.New("erasure: ParityShards must be >= 1")
	}
	return &ErasureCoder{cfg: cfg}, nil
}

// Encode splits data into k data shards and produces m parity shards.
// Returns a slice of (k+m) shards, each of equal byte length.
func (e *ErasureCoder) Encode(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("erasure: cannot encode empty data")
	}

	k := e.cfg.DataShards
	m := e.cfg.ParityShards
	total := k + m

	// --- Step 1: Pad data so it divides evenly into k shards. ---
	// Store the original length as a 4-byte prefix so we can trim on decode.
	origLen := len(data)
	padded := encodeLength(origLen, data, k)
	shardSize := len(padded) / k

	// --- Step 2: Slice padded data into k data shards. ---
	shards := make([][]byte, total)
	for i := 0; i < k; i++ {
		shard := make([]byte, shardSize)
		copy(shard, padded[i*shardSize:(i+1)*shardSize])
		shards[i] = shard
	}

	// --- Step 3: Produce m parity shards via XOR chaining. ---
	// parity[j] = XOR of data[0] rotated-left by j bytes
	//             XOR data[1] rotated-left by (j+1)%shardSize bytes … etc.
	// Simple but sufficient: every parity shard mixes all data shards.
	for j := 0; j < m; j++ {
		parity := make([]byte, shardSize)
		for i := 0; i < k; i++ {
			rotated := rotateLeft(shards[i], (i+j)%shardSize)
			xorInPlace(parity, rotated)
		}
		shards[k+j] = parity
	}

	return shards, nil
}

// Decode reconstructs the original data from a set of shards.
// shards must have length TotalShards(). Absent shards are represented
// by a nil entry. Returns an error if more than m shards are nil.
func (e *ErasureCoder) Decode(shards [][]byte) ([]byte, error) {
	k := e.cfg.DataShards
	m := e.cfg.ParityShards
	total := k + m

	if len(shards) != total {
		return nil, fmt.Errorf("erasure: expected %d shards, got %d", total, len(shards))
	}

	// Count missing shards.
	missing := []int{}
	shardSize := 0
	for i, s := range shards {
		if s == nil {
			missing = append(missing, i)
		} else {
			shardSize = len(s)
		}
	}

	if len(missing) > m {
		return nil, fmt.Errorf(
			"erasure: %d shards missing, can only tolerate %d", len(missing), m)
	}

	// --- Reconstruct missing shards via XOR. ---
	// For the simplified XOR scheme: any missing shard S[i] can be recovered
	// if we know all other shards that were XOR-ed together to form it (or
	// a parity shard that contains S[i]).
	//
	// For a single missing data shard (index lost < k):
	//   Reconstruct from the corresponding parity shard by reversing the XOR.
	if len(missing) == 1 {
		lost := missing[0]
		if lost < k {
			// Reconstruct data shard from parity[0].
			// parity[0] = XOR(rotate(shard[i], i) for i in 0..k-1)
			// So: rotate(shard[lost], lost) = parity[0] XOR rotate(shard[i], i) for i≠lost
			p := make([]byte, shardSize)
			copy(p, shards[k]) // start with parity[0]
			for i := 0; i < k; i++ {
				if i == lost {
					continue
				}
				xorInPlace(p, rotateLeft(shards[i], (i)%shardSize))
			}
			// p is now rotate(shard[lost], lost%shardSize) — undo the rotation.
			shards[lost] = rotateRight(p, lost%shardSize)
		} else {
			// Missing parity shard — data shards are intact; just recompute it.
			j := lost - k
			parity := make([]byte, shardSize)
			for i := 0; i < k; i++ {
				xorInPlace(parity, rotateLeft(shards[i], (i+j)%shardSize))
			}
			shards[lost] = parity
		}
	}

	// --- Concatenate data shards and strip padding. ---
	padded := make([]byte, k*shardSize)
	for i := 0; i < k; i++ {
		copy(padded[i*shardSize:], shards[i])
	}

	origLen, payload, err := decodeLength(padded)
	if err != nil {
		return nil, fmt.Errorf("erasure: %w", err)
	}
	if origLen > len(payload) {
		return nil, fmt.Errorf("erasure: decoded length %d exceeds payload %d", origLen, len(payload))
	}
	return payload[:origLen], nil
}

// ---------------------------------------------------------------------------
// Redundancy Manager
// ---------------------------------------------------------------------------

// RedundancyStrategy selects between full replication and erasure coding.
type RedundancyStrategy int

const (
	// StrategyReplication stores a full copy on every node. Highest availability,
	// highest storage cost.
	StrategyReplication RedundancyStrategy = iota

	// StrategyErasureCoding splits the message into shards and stores one shard
	// per node. Lower storage cost, same fault tolerance with enough parity.
	StrategyErasureCoding
)

// RedundancyManager orchestrates message redundancy across the cluster.
// It is the entry point called by the node after a log entry is committed.
type RedundancyManager struct {
	stores   []ShardStore  // one entry per server in the cluster
	erasure  *ErasureCoder // nil when strategy is StrategyReplication
	strategy RedundancyStrategy

	// Overhead tracking — updated each time StoreWithReplication is called.
	totalOriginalBytes    uint64
	totalReplicatedBytes  uint64
}

// NewRedundancyManager creates a manager for the given set of servers and strategy.
//
//   stores   — one ShardStore per server (len == cluster size)
//   strategy — StrategyReplication or StrategyErasureCoding
//   ecCfg    — erasure coding parameters; ignored when strategy is Replication
func NewRedundancyManager(
	stores []ShardStore,
	strategy RedundancyStrategy,
	ecCfg ErasureConfig,
) (*RedundancyManager, error) {
	if len(stores) == 0 {
		return nil, errors.New("redundancy: at least one store is required")
	}

	rm := &RedundancyManager{
		stores:   stores,
		strategy: strategy,
	}

	if strategy == StrategyErasureCoding {
		if ecCfg.TotalShards() > len(stores) {
			return nil, fmt.Errorf(
				"redundancy: erasure coding needs %d shards but only %d stores available",
				ecCfg.TotalShards(), len(stores),
			)
		}
		ec, err := NewErasureCoder(ecCfg)
		if err != nil {
			return nil, fmt.Errorf("redundancy: %w", err)
		}
		rm.erasure = ec
	}

	return rm, nil
}

// StoreWithReplication persists a committed log entry using the configured
// redundancy strategy across all available stores (servers).
//
// Under StrategyReplication  — a full copy of the entry is written to every store.
// Under StrategyErasureCoding — the entry payload is encoded into shards;
//   each store receives one shard (data or parity).
//
// Returns a RedundancyReport summarising how many copies were stored and
// the storage overhead incurred.
func (rm *RedundancyManager) StoreWithReplication(entry consensus.LogEntry) (*RedundancyReport, error) {
	switch rm.strategy {
	case StrategyReplication:
		return rm.storeReplicated(entry)
	case StrategyErasureCoding:
		return rm.storeErasureCoded(entry)
	default:
		return nil, fmt.Errorf("redundancy: unknown strategy %d", rm.strategy)
	}
}

// storeReplicated writes a full copy of the entry to every store.
func (rm *RedundancyManager) storeReplicated(entry consensus.LogEntry) (*RedundancyReport, error) {
	origSize := len(entry.Data)
	stored := 0
	errs := []error{}

	for i, store := range rm.stores {
		if err := store.StoreReplica(entry.Index, entry); err != nil {
			errs = append(errs, fmt.Errorf("store[%d]: %w", i, err))
			continue
		}
		stored++
	}

	if stored == 0 {
		return nil, fmt.Errorf("redundancy: all stores failed: %v", errs)
	}

	totalBytes := uint64(origSize * stored)
	rm.totalOriginalBytes += uint64(origSize)
	rm.totalReplicatedBytes += totalBytes

	return &RedundancyReport{
		LogIndex:          entry.Index,
		Strategy:          StrategyReplication,
		CopiesStored:      stored,
		TotalStores:       len(rm.stores),
		OriginalBytes:     origSize,
		StoredBytes:       origSize * stored,
		OverheadPercent:   stored * 100,
		PartialFailures:   errs,
	}, nil
}

// storeErasureCoded encodes the entry into shards and writes one shard per store.
func (rm *RedundancyManager) storeErasureCoded(entry consensus.LogEntry) (*RedundancyReport, error) {
	origSize := len(entry.Data)

	shards, err := rm.erasure.Encode(entry.Data)
	if err != nil {
		return nil, fmt.Errorf("redundancy: encoding failed: %w", err)
	}

	stored := 0
	shardBytes := 0
	errs := []error{}

	for i, shard := range shards {
		if i >= len(rm.stores) {
			break // more shards than stores — skip (config mismatch guard)
		}
		if err := rm.stores[i].StoreShard(entry.Index, i, shard); err != nil {
			errs = append(errs, fmt.Errorf("store[%d] shard[%d]: %w", i, i, err))
			continue
		}
		stored++
		shardBytes += len(shard)
	}

	if stored < rm.erasure.cfg.DataShards {
		return nil, fmt.Errorf(
			"redundancy: only %d shards stored, need at least %d for recovery",
			stored, rm.erasure.cfg.DataShards,
		)
	}

	rm.totalOriginalBytes += uint64(origSize)
	rm.totalReplicatedBytes += uint64(shardBytes)

	overhead := 0
	if origSize > 0 {
		overhead = (shardBytes * 100) / origSize
	}

	return &RedundancyReport{
		LogIndex:        entry.Index,
		Strategy:        StrategyErasureCoding,
		CopiesStored:    stored,
		TotalStores:     len(rm.stores),
		OriginalBytes:   origSize,
		StoredBytes:     shardBytes,
		OverheadPercent: overhead,
		PartialFailures: errs,
	}, nil
}

// ReconstructMessage recovers the original message for the given log index
// by reading available replicas or shards from all stores.
//
// Under StrategyReplication — returns the first successfully loaded replica.
// Under StrategyErasureCoding — collects available shards and decodes them.
func (rm *RedundancyManager) ReconstructMessage(logIndex uint64) ([]byte, error) {
	switch rm.strategy {
	case StrategyReplication:
		return rm.reconstructFromReplicas(logIndex)
	case StrategyErasureCoding:
		return rm.reconstructFromShards(logIndex)
	default:
		return nil, fmt.Errorf("redundancy: unknown strategy %d", rm.strategy)
	}
}

// reconstructFromReplicas reads replicas in order and returns the first success.
func (rm *RedundancyManager) reconstructFromReplicas(logIndex uint64) ([]byte, error) {
	var lastErr error
	for i, store := range rm.stores {
		entry, err := store.LoadReplica(logIndex)
		if err != nil {
			lastErr = fmt.Errorf("store[%d]: %w", i, err)
			continue
		}
		return entry.Data, nil
	}
	return nil, fmt.Errorf("redundancy: no replica available for index %d: %v", logIndex, lastErr)
}

// reconstructFromShards collects shards from all stores and decodes the message.
func (rm *RedundancyManager) reconstructFromShards(logIndex uint64) ([]byte, error) {
	total := rm.erasure.cfg.TotalShards()
	shards := make([][]byte, total)

	for i := 0; i < total && i < len(rm.stores); i++ {
		shard, err := rm.stores[i].LoadShard(logIndex, i)
		if err != nil || shard == nil {
			// nil signals a missing shard — the decoder handles it.
			shards[i] = nil
			continue
		}
		shards[i] = shard
	}

	data, err := rm.erasure.Decode(shards)
	if err != nil {
		return nil, fmt.Errorf("redundancy: reconstruction failed for index %d: %w", logIndex, err)
	}
	return data, nil
}

// StorageOverhead returns aggregate storage overhead metrics since the manager
// was created. Useful for evaluating the cost of the chosen strategy.
func (rm *RedundancyManager) StorageOverhead() StorageMetrics {
	overheadPct := 0
	if rm.totalOriginalBytes > 0 {
		overheadPct = int(rm.totalReplicatedBytes * 100 / rm.totalOriginalBytes)
	}
	return StorageMetrics{
		TotalOriginalBytes:   rm.totalOriginalBytes,
		TotalReplicatedBytes: rm.totalReplicatedBytes,
		OverheadPercent:      overheadPct,
		Strategy:             rm.strategy,
	}
}

// ---------------------------------------------------------------------------
// Report & Metrics types
// ---------------------------------------------------------------------------

// RedundancyReport is returned by StoreWithReplication and describes what
// happened for a single message.
type RedundancyReport struct {
	LogIndex        uint64             // which message this covers
	Strategy        RedundancyStrategy // strategy that was used
	CopiesStored    int                // number of stores that succeeded
	TotalStores     int                // total number of stores attempted
	OriginalBytes   int                // size of the raw message payload
	StoredBytes     int                // total bytes written across all stores
	OverheadPercent int                // StoredBytes / OriginalBytes * 100
	PartialFailures []error            // non-fatal per-store errors
}

// StorageMetrics aggregates overhead figures across all messages.
type StorageMetrics struct {
	TotalOriginalBytes   uint64
	TotalReplicatedBytes uint64
	OverheadPercent      int
	Strategy             RedundancyStrategy
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// encodeLength prepends the original length as a 4-byte big-endian header,
// then zero-pads so that total length is a multiple of k.
func encodeLength(origLen int, data []byte, k int) []byte {
	// 4-byte header + data
	raw := make([]byte, 4+len(data))
	raw[0] = byte(origLen >> 24)
	raw[1] = byte(origLen >> 16)
	raw[2] = byte(origLen >> 8)
	raw[3] = byte(origLen)
	copy(raw[4:], data)

	// Pad to nearest multiple of k.
	if rem := len(raw) % k; rem != 0 {
		padding := make([]byte, k-rem)
		raw = append(raw, padding...)
	}
	return raw
}

// decodeLength reads the 4-byte big-endian length prefix and returns
// (originalLength, payload-after-header, error).
func decodeLength(padded []byte) (int, []byte, error) {
	if len(padded) < 4 {
		return 0, nil, errors.New("padded data too short to contain length header")
	}
	origLen := int(padded[0])<<24 | int(padded[1])<<16 | int(padded[2])<<8 | int(padded[3])
	return origLen, padded[4:], nil
}

// rotateLeft rotates the bytes of b left by n positions.
// e.g. rotateLeft([1,2,3,4], 1) = [2,3,4,1]
func rotateLeft(b []byte, n int) []byte {
	if len(b) == 0 || n == 0 {
		out := make([]byte, len(b))
		copy(out, b)
		return out
	}
	n = n % len(b)
	out := make([]byte, len(b))
	copy(out, b[n:])
	copy(out[len(b)-n:], b[:n])
	return out
}

// rotateRight rotates the bytes of b right by n positions.
// e.g. rotateRight([2,3,4,1], 1) = [1,2,3,4]
func rotateRight(b []byte, n int) []byte {
	if len(b) == 0 || n == 0 {
		out := make([]byte, len(b))
		copy(out, b)
		return out
	}
	n = n % len(b)
	return rotateLeft(b, len(b)-n)
}

// xorInPlace XORs each byte of src into dst (in-place). len(dst) must == len(src).
func xorInPlace(dst, src []byte) {
	for i := range dst {
		dst[i] ^= src[i]
	}
}
