//go:build all || cassandra
// +build all cassandra

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/cassandra-gocql-driver/v2/internal/streams"
)

func BenchmarkHostConnPool_Pick(b *testing.B) {
	poolSizes := []int{2, 10, 50, 100}

	for _, size := range poolSizes {
		b.Run(fmt.Sprintf("Size_%d", size), func(b *testing.B) {
			// Manual construction of pool to avoid IO and external dependencies
			pool := &hostConnPool{
				size:  size,
				conns: make([]*Conn, size),
			}

			// Setup connections with mocked streams
			for i := 0; i < size; i++ {
				conn := &Conn{
					streams: streams.New(4), // Protocol 4
				}
				pool.conns[i] = conn
			}

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					c := pool.Pick()
					if c == nil {
						b.Fatal("Pick returned nil")
					}
				}
			})
		})
	}
}

func BenchmarkConn_Contention(b *testing.B) {
	// Compare request tracking implementations.
	//
	// Baseline: single mutex + map (original implementation)
	// Refactor: sharded map (callMap)
	//
	// Workloads:
	// - workerStream: each worker reuses its own stream id (no stream allocator cost)
	// - allocator: uses streams.IDGenerator GetStream/Clear each iteration (includes allocator cost)
	//
	// Note: these microbenches are intentionally lock-heavy.
	b.Run("baseline_mutex_map/workerStream", BenchmarkConnCalls_Baseline_WorkerStream)
	b.Run("sharded_map/workerStream", BenchmarkConnCalls_Sharded_WorkerStream)
	b.Run("sharded_map_plus_conn_mu/workerStream", BenchmarkConnCalls_ShardedPlusConnMu_WorkerStream)

	b.Run("baseline_mutex_map/allocator", BenchmarkConnCalls_Baseline_Allocator)
	b.Run("sharded_map/allocator", BenchmarkConnCalls_Sharded_Allocator)
	b.Run("sharded_map_plus_conn_mu/allocator", BenchmarkConnCalls_ShardedPlusConnMu_Allocator)
}

const benchCallMapShards = 64

// The following top-level benchmarks exist so profiling can target one case
// precisely (sub-benchmark regex filtering can be surprisingly finicky).

func BenchmarkConnCalls_Baseline_WorkerStream(b *testing.B) {
	baseline := &callsMutexMap{m: make(map[int]*callReq)}
	benchmarkCallsWorkerStream(b, baseline)
}

func BenchmarkConnCalls_Sharded_WorkerStream(b *testing.B) {
	sharded := newCallMap(benchCallMapShards)
	benchmarkCallsWorkerStream(b, sharded)
}

func BenchmarkConnCalls_ShardedPlusConnMu_WorkerStream(b *testing.B) {
	sharded := newCallMap(benchCallMapShards)
	shardedWithConnMu := &callsWithOuterMu{inner: sharded}
	benchmarkCallsWorkerStream(b, shardedWithConnMu)
}

func BenchmarkConnCalls_Baseline_Allocator(b *testing.B) {
	baseline := &callsMutexMap{m: make(map[int]*callReq)}
	benchmarkCallsWithAllocator(b, baseline)
}

func BenchmarkConnCalls_Sharded_Allocator(b *testing.B) {
	sharded := newCallMap(benchCallMapShards)
	benchmarkCallsWithAllocator(b, sharded)
}

func BenchmarkConnCalls_ShardedPlusConnMu_Allocator(b *testing.B) {
	sharded := newCallMap(benchCallMapShards)
	shardedWithConnMu := &callsWithOuterMu{inner: sharded}
	benchmarkCallsWithAllocator(b, shardedWithConnMu)
}

type callTracker interface {
	tryStore(streamID int, call *callReq) bool
	loadAndDelete(streamID int) (*callReq, bool)
}

// callsWithOuterMu adds an extra mutex to each operation.
//
// This approximates the fixed per-op cost of grabbing Conn.mu for a closed check
// in the real Conn hot path, while still allowing us to compare call-tracking
// structures.
type callsWithOuterMu struct {
	mu    sync.Mutex
	inner callTracker
}

func (c *callsWithOuterMu) tryStore(streamID int, call *callReq) bool {
	c.mu.Lock()
	c.mu.Unlock()
	return c.inner.tryStore(streamID, call)
}

func (c *callsWithOuterMu) loadAndDelete(streamID int) (*callReq, bool) {
	c.mu.Lock()
	c.mu.Unlock()
	return c.inner.loadAndDelete(streamID)
}

// callsMutexMap models the original Conn.calls implementation: one mutex protecting a map.
type callsMutexMap struct {
	mu sync.Mutex
	m  map[int]*callReq
}

func (c *callsMutexMap) tryStore(streamID int, call *callReq) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m[streamID] != nil {
		return false
	}
	c.m[streamID] = call
	return true
}

func (c *callsMutexMap) loadAndDelete(streamID int) (*callReq, bool) {
	c.mu.Lock()
	call, ok := c.m[streamID]
	if ok {
		delete(c.m, streamID)
	}
	c.mu.Unlock()
	return call, ok
}

func benchmarkCallsWorkerStream(b *testing.B, calls callTracker) {
	var workerID atomic.Uint32

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Each worker gets its own stream id, reused across iterations.
		stream := int(workerID.Add(1))
		for pb.Next() {
			call := &callReq{streamID: stream}
			if !calls.tryStore(stream, call) {
				b.Fatal("collision")
			}
			if _, ok := calls.loadAndDelete(stream); !ok {
				b.Fatal("missing")
			}
		}
	})
}

func benchmarkCallsWithAllocator(b *testing.B, calls callTracker) {
	conn := &Conn{
		streams: streams.New(4),
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			stream, ok := conn.streams.GetStream()
			if !ok {
				continue
			}

			call := &callReq{streamID: stream}
			if !calls.tryStore(stream, call) {
				b.Fatal("collision")
			}

			if _, ok := calls.loadAndDelete(stream); !ok {
				b.Fatal("missing")
			}
			conn.streams.Clear(stream)
		}
	})
}
