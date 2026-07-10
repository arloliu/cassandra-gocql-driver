package gocql

import "sync/atomic"

// callMap is a dense array of atomic pointers indexed directly by stream ID.
//
// All hot-path operations are lock-free:
//   - tryStore: CAS(nil → call) — succeeds only if the slot is empty.
//   - loadAndDelete: atomic Swap(nil) — returns the previous value.
//   - delete: Store(nil).
//
// The stream allocator (IDGenerator) already uses lock-free CAS on a bitmap.
// This makes the entire request-tracking hot path lock-free, removing the
// last mutex from the per-request critical section.
//
// Memory: one pointer (8 bytes) per stream slot. The slot count comes from
// ClusterConfig.MaxStreams (see streams.New): the default of 2048 for proto v3+
// is 16 KB per connection, the protocol maximum of 32768 is 256 KB, and proto
// v1/v2 (128 slots) is 1 KB.
//
// Close is rare; its O(numStreams) scan is acceptable.
type callMap struct {
	entries []atomic.Pointer[callReq]
}

// newCallMap creates a callMap for the given maximum stream count.
// Pass streams.IDGenerator.NumStreams to size it correctly for the connection.
func newCallMap(maxStreams int) *callMap {
	return &callMap{
		entries: make([]atomic.Pointer[callReq], maxStreams),
	}
}

// tryStore stores call for streamID if the slot is currently empty.
// Returns true on success.
func (c *callMap) tryStore(streamID int, call *callReq) bool {
	return c.entries[streamID].CompareAndSwap(nil, call)
}

// delete clears the slot for streamID.
func (c *callMap) delete(streamID int) {
	c.entries[streamID].Store(nil)
}

// loadAndDelete atomically removes and returns the call for streamID.
func (c *callMap) loadAndDelete(streamID int) (*callReq, bool) {
	call := c.entries[streamID].Swap(nil)
	return call, call != nil
}

// snapshot returns a point-in-time slice of currently registered calls.
// It does not remove entries.
func (c *callMap) snapshot() []*callReq {
	var calls []*callReq
	for i := range c.entries {
		if call := c.entries[i].Load(); call != nil {
			calls = append(calls, call)
		}
	}
	return calls
}

// clear removes all entries.
func (c *callMap) clear() {
	for i := range c.entries {
		c.entries[i].Store(nil)
	}
}
