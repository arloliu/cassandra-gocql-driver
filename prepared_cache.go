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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"bytes"
	"context"

	"github.com/maypok86/otter/v2"
)

const defaultMaxPreparedStmts = 1000

// preparedKey is the cache key for prepared statements.
// Using a struct avoids string concatenation allocations on the hot path.
type preparedKey struct {
	hostID    string
	keyspace  string
	statement string
}

// preparedLRU is the prepared statement cache using otter for high-performance
// concurrent access without global mutex contention.
type preparedLRU struct {
	cache *otter.Cache[preparedKey, *preparedStatment]
}

// newPreparedLRU creates a new prepared statement cache with the given maximum size.
func newPreparedLRU(maxSize int) *preparedLRU {
	return &preparedLRU{
		cache: otter.Must(&otter.Options[preparedKey, *preparedStatment]{
			MaximumSize: maxSize,
		}),
	}
}

// keyFor constructs a cache key from host, keyspace, and statement.
func (p *preparedLRU) keyFor(hostID, keyspace, statement string) preparedKey {
	return preparedKey{
		hostID:    hostID,
		keyspace:  keyspace,
		statement: statement,
	}
}

// get retrieves a prepared statement from the cache.
// This is a lock-free read operation.
func (p *preparedLRU) get(key preparedKey) (*preparedStatment, bool) {
	return p.cache.GetIfPresent(key)
}

// getOrLoad retrieves a prepared statement from the cache, loading it if not present.
// Otter handles deduplication of concurrent loads for the same key.
func (p *preparedLRU) getOrLoad(ctx context.Context, key preparedKey, loader otter.Loader[preparedKey, *preparedStatment]) (*preparedStatment, error) {
	return p.cache.Get(ctx, key, loader)
}

// set adds or updates a prepared statement in the cache.
func (p *preparedLRU) set(key preparedKey, val *preparedStatment) {
	p.cache.Set(key, val)
}

// delete removes a prepared statement from the cache.
func (p *preparedLRU) delete(key preparedKey) {
	p.cache.Invalidate(key)
}

// evictPreparedID atomically removes an entry only if its ID matches.
// This prevents removing a re-prepared statement that replaced the failed one.
func (p *preparedLRU) evictPreparedID(key preparedKey, id []byte) {
	p.cache.ComputeIfPresent(key, func(val *preparedStatment) (*preparedStatment, otter.ComputeOp) {
		if val == nil || bytes.Equal(id, val.id) {
			return nil, otter.InvalidateOp
		}
		return val, otter.CancelOp
	})
}

// clear removes all entries from the cache.
func (p *preparedLRU) clear() {
	p.cache.InvalidateAll()
}
