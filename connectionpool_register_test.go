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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// registerPool is the half of the old addHost that runs under p.mu.
// Everything that decides whether a host may be admitted still happens there;
// only the fill moved out,
// so that a caller holding another mutex can register without dialling under it.
//
// These tests pin the contract the split has to preserve.
// Getting the second return value wrong is the way to break it silently:
// if it were "the pool was newly built" rather than "the caller owes this pool a fill",
// a registered pool that lost every connection would never be refilled,
// and nothing in the admission paths would report it.

// TestRegisterPool_ExistingPoolStillOwesAFill: registering the same object
// twice returns the same pool both times and asks for a fill both times.
func TestRegisterPool_ExistingPoolStillOwesAFill(t *testing.T) {
	f := newOwnershipFixture(t)

	first, ok := f.session.pool.getPoolFor(f.host)
	require.True(t, ok, "the fixture host is admitted")

	pool, needFill := f.session.pool.registerPool(f.host)
	require.Same(t, first, pool, "an owned host with a matching pool keeps it")
	require.True(t, needFill, "a caller that gets a pool back always owes it a fill")
}

// TestRegisterPool_RejectedHostOwesNothing: the ownership gate and the closed
// gate both return no pool, so a caller's deferred fill has nothing to run.
func TestRegisterPool_RejectedHostOwesNothing(t *testing.T) {
	t.Run("not owned", func(t *testing.T) {
		f := newOwnershipFixture(t)
		stale := f.host
		f.replacement(t) // takes the host ID; stale is no longer the ring's object

		pool, needFill := f.session.pool.registerPool(stale)
		require.Nil(t, pool, "an object the ring no longer owns is not admitted")
		require.False(t, needFill, "no pool means no fill obligation")
	})

	t.Run("pool closed", func(t *testing.T) {
		f := newOwnershipFixture(t)
		f.session.pool.Close()

		pool, needFill := f.session.pool.registerPool(f.host)
		require.Nil(t, pool, "a closed pool admits nobody")
		require.False(t, needFill, "no pool means no fill obligation")
	})
}

// TestRegisterPool_ReplacesAStalePool: a pool built for an object refreshRing
// has replaced is unregistered and closed, and the replacement gets its own.
//
// This is the case that makes getPoolFor's pointer check usable as an "is this
// object admitted" question: without the eviction, the stale pool would keep
// answering for the ID and the replacement would never be registered.
func TestRegisterPool_ReplacesAStalePool(t *testing.T) {
	f := newOwnershipFixture(t)

	stalePool, ok := f.session.pool.getPoolFor(f.host)
	require.True(t, ok, "the fixture host is admitted")

	b := f.replacement(t)
	pool, needFill := f.session.pool.registerPool(b)
	require.True(t, needFill, "the replacement owes its new pool a fill")
	require.NotSame(t, stalePool, pool, "the superseded object's pool is not reused")
	require.Same(t, b, pool.host, "the new pool belongs to the replacement")

	registered, ok := f.session.pool.getPoolFor(b)
	require.True(t, ok, "the replacement's pool is the one registered for the ID")
	require.Same(t, pool, registered)

	require.Eventually(t, func() bool { return isPoolClosed(stalePool) }, ownershipBudget, 5*time.Millisecond,
		"the superseded object's pool is closed")
}
