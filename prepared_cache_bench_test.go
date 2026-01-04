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
	"sync/atomic"
	"testing"
)

// BenchmarkPreparedCacheConcurrent measures prepared statement cache throughput
// under high concurrency. This establishes baseline performance for the
// preparedLRU implementation.
func BenchmarkPreparedCacheConcurrent(b *testing.B) {
	const workers = 64

	cluster := createCluster()
	cluster.NumConns = 4
	session := createSessionFromCluster(cluster, b)
	defer session.Close()

	if err := createTable(session, "CREATE TABLE IF NOT EXISTS prepared_cache_bench (id int primary key, val text)"); err != nil {
		b.Fatal(err)
	}

	// Pre-warm with a few queries
	for i := 0; i < 10; i++ {
		if err := session.Query("INSERT INTO prepared_cache_bench (id, val) VALUES (?, ?)", i, "warmup").Exec(); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	var counter uint64
	writer := func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddUint64(&counter, 1)
			// Same statement - hits cache after first prepare
			if err := session.Query("INSERT INTO prepared_cache_bench (id, val) VALUES (?, ?)", id, "test").Exec(); err != nil {
				b.Error(err)
				return
			}
		}
	}

	b.SetParallelism(workers)
	b.RunParallel(writer)
}

// BenchmarkPreparedCacheMultiStatement measures cache performance with multiple
// distinct statements, testing cache lookup overhead.
func BenchmarkPreparedCacheMultiStatement(b *testing.B) {
	const workers = 32
	const numStatements = 100

	cluster := createCluster()
	cluster.NumConns = 4
	session := createSessionFromCluster(cluster, b)
	defer session.Close()

	// Create tables for different statements
	for i := 0; i < numStatements; i++ {
		tableName := fmt.Sprintf("prepared_multi_%d", i)
		if err := createTable(session, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id int primary key)", tableName)); err != nil {
			b.Fatal(err)
		}
	}

	// Pre-compile statements
	statements := make([]string, numStatements)
	for i := 0; i < numStatements; i++ {
		statements[i] = fmt.Sprintf("INSERT INTO prepared_multi_%d (id) VALUES (?)", i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	var counter uint64
	writer := func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddUint64(&counter, 1)
			stmtIdx := int(id % numStatements)
			if err := session.Query(statements[stmtIdx], id).Exec(); err != nil {
				b.Error(err)
				return
			}
		}
	}

	b.SetParallelism(workers)
	b.RunParallel(writer)
}
