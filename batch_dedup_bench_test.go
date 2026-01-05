/*
 * Benchmark: Batch Prepare Deduplication
 *
 * Compares the overhead of:
 * 1. Original: Calling otter cache lookup for every batch entry
 * 2. Optimized: Using local map to deduplicate before cache lookup
 *
 * Run with: go test -bench=BenchmarkBatchDedup -benchmem
 */

package gocql

import (
	"context"
	"fmt"
	"testing"

	"github.com/maypok86/otter/v2"
)

// Mock prepared statement to simulate real cache
type mockPreparedStmt struct {
	id       []byte
	metadata string
}

// Simulates the prepared statement cache using otter
type mockPreparedCache struct {
	cache *otter.Cache[preparedKey, *mockPreparedStmt]
}

func newMockPreparedCache() *mockPreparedCache {
	return &mockPreparedCache{
		cache: otter.Must(&otter.Options[preparedKey, *mockPreparedStmt]{
			MaximumSize: 1000,
		}),
	}
}

func (c *mockPreparedCache) get(key preparedKey) (*mockPreparedStmt, bool) {
	return c.cache.GetIfPresent(key)
}

func (c *mockPreparedCache) set(key preparedKey, val *mockPreparedStmt) {
	c.cache.Set(key, val)
}

// Pre-populate cache with statements
func (c *mockPreparedCache) warmUp(keyspace string, statements []string) {
	for i, stmt := range statements {
		key := preparedKey{hostID: "host1", keyspace: keyspace, statement: stmt}
		c.set(key, &mockPreparedStmt{
			id:       []byte(fmt.Sprintf("id-%d", i)),
			metadata: "metadata",
		})
	}
}

// Original approach: lookup cache for every entry
func benchOriginal(cache *mockPreparedCache, keyspace string, entries []string) []*mockPreparedStmt {
	results := make([]*mockPreparedStmt, len(entries))
	for i, stmt := range entries {
		key := preparedKey{hostID: "host1", keyspace: keyspace, statement: stmt}
		info, _ := cache.get(key)
		results[i] = info
	}
	return results
}

// Optimized approach: local map deduplication
func benchOptimized(cache *mockPreparedCache, keyspace string, entries []string) []*mockPreparedStmt {
	results := make([]*mockPreparedStmt, len(entries))

	type localKey struct {
		keyspace  string
		statement string
	}
	localCache := make(map[localKey]*mockPreparedStmt, min(len(entries), 16))

	for i, stmt := range entries {
		lk := localKey{keyspace: keyspace, statement: stmt}
		info, ok := localCache[lk]
		if !ok {
			key := preparedKey{hostID: "host1", keyspace: keyspace, statement: stmt}
			info, _ = cache.get(key)
			localCache[lk] = info
		}
		results[i] = info
	}
	return results
}

// Generate batch entries with specified uniqueness
func generateBatchEntries(total, uniqueCount int) []string {
	statements := make([]string, uniqueCount)
	for i := 0; i < uniqueCount; i++ {
		statements[i] = fmt.Sprintf("INSERT INTO users (id, name, email) VALUES (?, ?, ?) /* stmt %d */", i)
	}

	entries := make([]string, total)
	for i := 0; i < total; i++ {
		entries[i] = statements[i%uniqueCount]
	}
	return entries
}

// Benchmark: 100 entries, 1 unique statement (best case for dedup)
func BenchmarkBatchDedup_100_1Unique_Original(b *testing.B) {
	cache := newMockPreparedCache()
	entries := generateBatchEntries(100, 1)
	cache.warmUp("test_keyspace", entries[:1])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchOriginal(cache, "test_keyspace", entries)
	}
}

func BenchmarkBatchDedup_100_1Unique_Optimized(b *testing.B) {
	cache := newMockPreparedCache()
	entries := generateBatchEntries(100, 1)
	cache.warmUp("test_keyspace", entries[:1])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchOptimized(cache, "test_keyspace", entries)
	}
}

// Benchmark: 100 entries, 10 unique statements (common case)
func BenchmarkBatchDedup_100_10Unique_Original(b *testing.B) {
	cache := newMockPreparedCache()
	entries := generateBatchEntries(100, 10)
	cache.warmUp("test_keyspace", entries[:10])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchOriginal(cache, "test_keyspace", entries)
	}
}

func BenchmarkBatchDedup_100_10Unique_Optimized(b *testing.B) {
	cache := newMockPreparedCache()
	entries := generateBatchEntries(100, 10)
	cache.warmUp("test_keyspace", entries[:10])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchOptimized(cache, "test_keyspace", entries)
	}
}

// Benchmark: 100 entries, 100 unique statements (worst case - no benefit expected)
func BenchmarkBatchDedup_100_100Unique_Original(b *testing.B) {
	cache := newMockPreparedCache()
	entries := generateBatchEntries(100, 100)
	cache.warmUp("test_keyspace", entries)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchOriginal(cache, "test_keyspace", entries)
	}
}

func BenchmarkBatchDedup_100_100Unique_Optimized(b *testing.B) {
	cache := newMockPreparedCache()
	entries := generateBatchEntries(100, 100)
	cache.warmUp("test_keyspace", entries)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchOptimized(cache, "test_keyspace", entries)
	}
}

// Benchmark: 1000 entries, 1 unique statement (stress test)
func BenchmarkBatchDedup_1000_1Unique_Original(b *testing.B) {
	cache := newMockPreparedCache()
	entries := generateBatchEntries(1000, 1)
	cache.warmUp("test_keyspace", entries[:1])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchOriginal(cache, "test_keyspace", entries)
	}
}

func BenchmarkBatchDedup_1000_1Unique_Optimized(b *testing.B) {
	cache := newMockPreparedCache()
	entries := generateBatchEntries(1000, 1)
	cache.warmUp("test_keyspace", entries[:1])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchOptimized(cache, "test_keyspace", entries)
	}
}

// Verify correctness
func TestBatchDedupCorrectness(t *testing.T) {
	cache := newMockPreparedCache()
	entries := generateBatchEntries(100, 5)
	cache.warmUp("test_keyspace", entries[:5])

	original := benchOriginal(cache, "test_keyspace", entries)
	optimized := benchOptimized(cache, "test_keyspace", entries)

	if len(original) != len(optimized) {
		t.Fatalf("length mismatch: original=%d, optimized=%d", len(original), len(optimized))
	}

	for i := range original {
		if original[i] != optimized[i] {
			t.Errorf("mismatch at index %d", i)
		}
	}
}

// Benchmark using actual preparedLRU to be more realistic
func BenchmarkBatchDedup_RealCache_100_1Unique_Original(b *testing.B) {
	cache := newPreparedLRU(1000)
	keyspace := "test_keyspace"
	stmt := "INSERT INTO users (id, name, email) VALUES (?, ?, ?)"

	// Warm up with actual preparedStatment
	key := cache.keyFor("host1", keyspace, stmt)
	cache.set(key, &preparedStatment{
		id: []byte("prepared-id-1"),
	})

	entries := make([]string, 100)
	for i := range entries {
		entries[i] = stmt
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, s := range entries {
			k := cache.keyFor("host1", keyspace, s)
			cache.get(k)
		}
	}
}

func BenchmarkBatchDedup_RealCache_100_1Unique_Optimized(b *testing.B) {
	cache := newPreparedLRU(1000)
	keyspace := "test_keyspace"
	stmt := "INSERT INTO users (id, name, email) VALUES (?, ?, ?)"

	key := cache.keyFor("host1", keyspace, stmt)
	cache.set(key, &preparedStatment{
		id: []byte("prepared-id-1"),
	})

	entries := make([]string, 100)
	for i := range entries {
		entries[i] = stmt
	}

	type localKey struct {
		keyspace  string
		statement string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		localCache := make(map[localKey]*preparedStatment, 16)
		for _, s := range entries {
			lk := localKey{keyspace: keyspace, statement: s}
			if _, ok := localCache[lk]; !ok {
				k := cache.keyFor("host1", keyspace, s)
				info, _ := cache.get(k)
				localCache[lk] = info
			}
		}
	}
}

// Context-aware version to simulate real prepareStatement signature
func BenchmarkBatchDedup_WithContext_Original(b *testing.B) {
	cache := newPreparedLRU(1000)
	ctx := context.Background()
	keyspace := "test_keyspace"
	stmt := "INSERT INTO users (id, name, email) VALUES (?, ?, ?)"

	key := cache.keyFor("host1", keyspace, stmt)
	cache.set(key, &preparedStatment{id: []byte("id")})

	entries := make([]string, 100)
	for i := range entries {
		entries[i] = stmt
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ctx // simulate context usage
		for _, s := range entries {
			k := cache.keyFor("host1", keyspace, s)
			cache.get(k)
		}
	}
}

func BenchmarkBatchDedup_WithContext_Optimized(b *testing.B) {
	cache := newPreparedLRU(1000)
	ctx := context.Background()
	keyspace := "test_keyspace"
	stmt := "INSERT INTO users (id, name, email) VALUES (?, ?, ?)"

	key := cache.keyFor("host1", keyspace, stmt)
	cache.set(key, &preparedStatment{id: []byte("id")})

	entries := make([]string, 100)
	for i := range entries {
		entries[i] = stmt
	}

	type localKey struct {
		keyspace  string
		statement string
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ctx
		localCache := make(map[localKey]*preparedStatment, 16)
		for _, s := range entries {
			lk := localKey{keyspace: keyspace, statement: s}
			if _, ok := localCache[lk]; !ok {
				k := cache.keyFor("host1", keyspace, s)
				info, _ := cache.get(k)
				localCache[lk] = info
			}
		}
	}
}

// Benchmark: 100 entries, 20 unique statements - tests adaptive heuristic cutoff at 16
func BenchmarkBatchDedup_100_20Unique_Original(b *testing.B) {
	cache := newMockPreparedCache()
	entries := generateBatchEntries(100, 20)
	cache.warmUp("test_keyspace", entries[:20])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchOriginal(cache, "test_keyspace", entries)
	}
}

func BenchmarkBatchDedup_100_20Unique_Optimized(b *testing.B) {
	cache := newMockPreparedCache()
	entries := generateBatchEntries(100, 20)
	cache.warmUp("test_keyspace", entries[:20])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchOptimized(cache, "test_keyspace", entries)
	}
}

// Adaptive heuristic version - matches actual implementation
func benchOptimizedAdaptive(cache *mockPreparedCache, keyspace string, entries []string) []*mockPreparedStmt {
	results := make([]*mockPreparedStmt, len(entries))

	const threshold = 16
	type localKey struct {
		keyspace  string
		statement string
	}
	localCache := make(map[localKey]*mockPreparedStmt, min(len(entries), threshold))
	useLocalCache := true

	for i, stmt := range entries {
		if useLocalCache {
			lk := localKey{keyspace: keyspace, statement: stmt}
			info, ok := localCache[lk]
			if ok {
				results[i] = info
				continue
			}
			key := preparedKey{hostID: "host1", keyspace: keyspace, statement: stmt}
			info, _ = cache.get(key)
			if len(localCache) < threshold {
				localCache[lk] = info
			} else {
				useLocalCache = false
			}
			results[i] = info
		} else {
			key := preparedKey{hostID: "host1", keyspace: keyspace, statement: stmt}
			results[i], _ = cache.get(key)
		}
	}
	return results
}

func BenchmarkBatchDedup_100_20Unique_Adaptive(b *testing.B) {
	cache := newMockPreparedCache()
	entries := generateBatchEntries(100, 20)
	cache.warmUp("test_keyspace", entries[:20])

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchOptimizedAdaptive(cache, "test_keyspace", entries)
	}
}

func BenchmarkBatchDedup_100_100Unique_Adaptive(b *testing.B) {
	cache := newMockPreparedCache()
	entries := generateBatchEntries(100, 100)
	cache.warmUp("test_keyspace", entries)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchOptimizedAdaptive(cache, "test_keyspace", entries)
	}
}
