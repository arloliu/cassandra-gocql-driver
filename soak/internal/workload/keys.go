// Package workload drives the soak's CQL load and checks its correctness oracles (PLAN §4).
package workload

import (
	"fmt"
	"hash/fnv"
)

// Key space sizes (PLAN §4.1).
const (
	// PrimaryPartitions and ClusteringPerPartition shape the primary kv range: 4096 × 16 keys.
	PrimaryPartitions      = 4096
	ClusteringPerPartition = 16
	// AuxRanges is the number of disjoint aux kv ranges, one per churn slot.
	AuxRanges = 9
	// AuxPartitions is the partition count of one aux range.
	AuxPartitions = 256
	// BlobKeys is the number of large blob rows.
	BlobKeys = 64
	// ScanPartitions and ScanRowsPerPartition shape the scan table.
	ScanPartitions       = 64
	ScanRowsPerPartition = 12000
	// LWTRows is the number of LWT counters.
	LWTRows = 64

	// KVPayloadMin and KVPayloadMax bound a kv payload, uniform in between.
	KVPayloadMin = 100
	KVPayloadMax = 4096
	// BlobPayloadMin and BlobPayloadMax bound a blob payload.
	BlobPayloadMin = 256 << 10
	BlobPayloadMax = 1 << 20
	// ScanPayload is the size of a scan row's payload.
	ScanPayload = 100
)

// TotalKVKeys is every kv key, primary and aux.
const TotalKVKeys = (PrimaryPartitions + AuxRanges*AuxPartitions) * ClusteringPerPartition

// Key is one kv key.
type Key struct {
	// P and C are the partition and clustering key.
	P, C int32
}

// Range is a contiguous block of kv partitions.
type Range struct {
	// FirstP is the first partition; the range is [FirstP, FirstP+Partitions).
	FirstP int32
	// Partitions is the partition count.
	Partitions int32
}

// PrimaryRange returns the primary session's kv range.
//
// Returns:
//   - Range: partitions [0, 4096)
func PrimaryRange() Range {
	return Range{FirstP: 0, Partitions: PrimaryPartitions}
}

// AuxRange returns the kv range of aux slot i, disjoint from the primary and every other slot.
//
// Parameters:
//   - i: the aux slot, 0 ≤ i < AuxRanges
//
// Returns:
//   - Range: 256 partitions after the primary range and the earlier aux slots
//   - error: when i is out of range
func AuxRange(i int) (Range, error) {
	if i < 0 || i >= AuxRanges {
		return Range{}, fmt.Errorf("aux range %d out of [0, %d)", i, AuxRanges)
	}
	return Range{FirstP: int32(PrimaryPartitions + i*AuxPartitions), Partitions: AuxPartitions}, nil
}

// Index returns a key's dense index in [0, TotalKVKeys), for the ledger.
//
// Returns:
//   - int: the index
func (k Key) Index() int {
	return int(k.P)*ClusteringPerPartition + int(k.C)
}

// Keys returns the number of keys in the range.
//
// Returns:
//   - int: partitions × clustering keys
func (r Range) Keys() int {
	return int(r.Partitions) * ClusteringPerPartition
}

// Key returns the range's i-th key.
//
// Parameters:
//   - i: 0 ≤ i < r.Keys()
//
// Returns:
//   - Key: the key
func (r Range) Key(i int) Key {
	return Key{P: r.FirstP + int32(i/ClusteringPerPartition), C: int32(i % ClusteringPerPartition)}
}

// Payload returns a deterministic payload of a size in [min, max], derived from seed.
// The same seed always yields the same bytes, so a verifier can recompute it.
//
// Parameters:
//   - seed: e.g. the key and version
//   - minSize, maxSize: the size bounds
//
// Returns:
//   - []byte: the payload
func Payload(seed uint64, minSize, maxSize int) []byte {
	x := mix64(seed)
	size := minSize
	if maxSize > minSize {
		size += int(x % uint64(maxSize-minSize+1))
	}
	out := make([]byte, size)
	for i := range out {
		if i%8 == 0 {
			x = mix64(x)
		}
		out[i] = byte(x >> (8 * (i % 8)))
	}
	return out
}

// KVSeed is the payload seed of a kv key at a version.
//
// Parameters:
//   - k: the key
//   - ver: the version
//
// Returns:
//   - uint64: the seed
func KVSeed(k Key, ver int64) uint64 {
	h := fnv.New64a()
	var b [16]byte
	for i := range 4 {
		b[i] = byte(k.P >> (8 * i))
		b[4+i] = byte(k.C >> (8 * i))
	}
	for i := range 8 {
		b[8+i] = byte(ver >> (8 * i))
	}
	_, _ = h.Write(b[:])
	return h.Sum64()
}

// mix64 is splitmix64's finalizer.
func mix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
