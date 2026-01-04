//go:build all || unit
// +build all unit

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
)

// BenchmarkPreparedLRUGet measures pure cache lookup performance
func BenchmarkPreparedLRUGet(b *testing.B) {
	cache := newPreparedLRU(1000)

	// Pre-populate cache
	for i := 0; i < 100; i++ {
		key := cache.keyFor(fmt.Sprintf("host%d", i%10), "keyspace", fmt.Sprintf("SELECT * FROM table%d", i))
		cache.set(key, &preparedStatment{id: []byte{byte(i)}})
	}

	keys := make([]preparedKey, 100)
	for i := 0; i < 100; i++ {
		keys[i] = cache.keyFor(fmt.Sprintf("host%d", i%10), "keyspace", fmt.Sprintf("SELECT * FROM table%d", i))
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			cache.get(keys[i%100])
			i++
		}
	})
}

// BenchmarkPreparedLRUConcurrentAccess measures cache contention under high concurrency
func BenchmarkPreparedLRUConcurrentAccess(b *testing.B) {
	for _, numGoroutines := range []int{1, 4, 16, 64, 256} {
		b.Run(fmt.Sprintf("goroutines-%d", numGoroutines), func(b *testing.B) {
			cache := newPreparedLRU(1000)

			// Pre-populate
			for i := 0; i < 100; i++ {
				key := cache.keyFor("host", "ks", fmt.Sprintf("stmt%d", i))
				cache.set(key, &preparedStatment{id: []byte{byte(i)}})
			}

			keys := make([]preparedKey, 100)
			for i := 0; i < 100; i++ {
				keys[i] = cache.keyFor("host", "ks", fmt.Sprintf("stmt%d", i))
			}

			b.ResetTimer()
			b.ReportAllocs()
			b.SetParallelism(numGoroutines)

			var counter uint64
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					idx := atomic.AddUint64(&counter, 1) % 100
					cache.get(keys[idx])
				}
			})
		})
	}
}

// BenchmarkKeyGeneration measures cache key construction overhead
func BenchmarkKeyGeneration(b *testing.B) {
	cache := newPreparedLRU(1)

	hostID := "550e8400-e29b-41d4-a716-446655440000"
	keyspace := "my_keyspace"
	statement := "SELECT id, name, email FROM users WHERE id = ?"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = cache.keyFor(hostID, keyspace, statement)
	}
}

// BenchmarkMutexContention directly measures mutex contention for comparison
func BenchmarkMutexContention(b *testing.B) {
	var mu sync.Mutex
	var counter int

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			counter++
			mu.Unlock()
		}
	})
}
