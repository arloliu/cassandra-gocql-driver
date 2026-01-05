package gocql

import "sync"

// callMap is a sharded map from streamID to *callReq.
//
// This is on the hot path for every request (register handler) and response
// (lookup+delete handler). Sharding reduces lock contention compared to a single
// mutex protecting one map, while keeping memory proportional to the number of
// in-flight requests (unlike a dense atomic table sized to max streams).
//
// Invariants expected by Conn:
// - tryStore is used to ensure at most one callReq exists per streamID.
// - loadAndDelete transfers ownership of the callReq to the receiver.
// - snapshot is used on close to notify waiters (it does not delete entries).
// - clear removes all entries (used after close notification to allow GC).
type callMap struct {
	mask   uint32
	shards []callMapShard
}

type callMapShard struct {
	mu sync.Mutex
	m  map[int]*callReq
}

func newCallMap(numShards int) *callMap {
	// Ensure power-of-two shard count so we can use a mask.
	if numShards <= 0 {
		numShards = 64
	}
	shardsPow2 := 1
	for shardsPow2 < numShards {
		shardsPow2 <<= 1
	}

	shards := make([]callMapShard, shardsPow2)
	for i := range shards {
		shards[i].m = make(map[int]*callReq)
	}

	return &callMap{
		mask:   uint32(shardsPow2 - 1),
		shards: shards,
	}
}

func (c *callMap) shard(streamID int) *callMapShard {
	// streamID is a small non-negative int in practice. Masking is enough.
	idx := uint32(streamID) & c.mask
	return &c.shards[idx]
}

// tryStore stores call for streamID if it does not already exist.
// Returns true on success.
func (c *callMap) tryStore(streamID int, call *callReq) bool {
	s := c.shard(streamID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m[streamID] != nil {
		return false
	}

	s.m[streamID] = call

	return true
}

// delete removes the call for streamID if present.
func (c *callMap) delete(streamID int) {
	s := c.shard(streamID)
	s.mu.Lock()
	delete(s.m, streamID)
	s.mu.Unlock()
}

// loadAndDelete atomically gets and removes the call for streamID.
func (c *callMap) loadAndDelete(streamID int) (*callReq, bool) {
	s := c.shard(streamID)
	s.mu.Lock()
	call, ok := s.m[streamID]
	if ok {
		delete(s.m, streamID)
	}
	s.mu.Unlock()

	return call, ok
}

// snapshot returns a point-in-time slice of currently registered calls.
// It does not delete from the map.
func (c *callMap) snapshot() []*callReq {
	// Best-effort sizing: keep it simple; close is rare.
	var calls []*callReq
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		for _, call := range s.m {
			calls = append(calls, call)
		}
		s.mu.Unlock()
	}

	return calls
}

func (c *callMap) clear() {
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		// Allocate a fresh map to drop references quickly.
		s.m = make(map[int]*callReq)
		s.mu.Unlock()
	}
}
