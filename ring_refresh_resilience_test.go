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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

const resiliencePeerHostID = "33333333-0000-4000-8000-00000000beef"

// TestRingRefreshSurvivesAPanicAfterItChangedTheRing pins the resilience half of M2.
//
// A panic out of refreshRing used to reach refreshDebouncer.flusher, whose
// recover-and-stop is terminal: the flusher exits, every later refresh fails fast
// with "refreshDebouncer is stopped", and the ring is frozen at whatever it last
// held for the rest of the session's life.
//
// runRingRefresh now turns that panic into a failed round. The guarantee is
// deliberately narrow: the flusher stays alive and the next round still runs. It is
// NOT that the ring was repaired. The panic below lands after addHostIfMissing has
// already inserted the host, and the next round sees an unchanged endpoint and only
// calls HostInfo.update, so the pool and policy publication the panic skipped is not
// made up. Closing that gap is separate work.
func TestRingRefreshSurvivesAPanicAfterItChangedTheRing(t *testing.T) {
	var panicOnFill atomic.Bool
	var done atomic.Pointer[[]error]
	done.Store(&[]error{})

	script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
		cluster.testStartPoolFillStart = func(host *HostInfo) {
			if host.HostID() == resiliencePeerHostID && panicOnFill.CompareAndSwap(true, false) {
				panic("scripted panic after the ring was changed")
			}
		}
		cluster.testRingRefreshDone = func(err error) {
			prev := *done.Load()
			next := append(append([]error{}, prev...), err)
			done.Store(&next)
		}
	})

	// The new peer is admitted - addHostIfMissing inserts it - and the panic then
	// lands inside the publication that follows, so the ring has already changed.
	script.setPeers([]peerRow{newPeerRow(resiliencePeerHostID, "127.0.0.2")})
	panicOnFill.Store(true)

	err := session.refreshRing()
	require.Error(t, err, "the panicking round must be reported as a failed round")
	require.ErrorContains(t, err, "panicked", "the error must name the panic")

	results := *done.Load()
	require.NotEmpty(t, results, "the completion hook must have been called for the panicking round")
	require.Error(t, results[len(results)-1], "the completion hook must have seen the same failure")

	// The whole point: the refresher is still running.
	require.NoError(t, session.refreshRing(), "the next round must still run")
	_, ok := session.ring.getHost(resiliencePeerHostID)
	require.True(t, ok, "the host admitted before the panic is still in the ring")
}

// TestRingRefreshSurvivesAPanicFromItsOwnReporting pins the two isolated boundaries
// runRingRefresh reports through.
//
// Reporting a failed round calls user code twice - the logger, then the completion
// hook - and both run in the deferred handler that produced the error. A panic from
// either would escape that handler and reach the flusher's recover-and-stop, which
// is the outcome the recover above exists to prevent. Each is therefore isolated on
// its own.
func TestRingRefreshSurvivesAPanicFromItsOwnReporting(t *testing.T) {
	t.Run("the logger panics while reporting the failure", func(t *testing.T) {
		var panicOnFill atomic.Bool
		var logger *panicOnWarningLogger
		var lastDone atomic.Pointer[error]

		script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
			base := cluster.Logger
			if base == nil {
				base = &defaultLogger{}
			}
			logger = &panicOnWarningLogger{StructuredLogger: base}
			cluster.Logger = logger
			cluster.testStartPoolFillStart = func(host *HostInfo) {
				if host.HostID() == resiliencePeerHostID && panicOnFill.CompareAndSwap(true, false) {
					panic("scripted panic after the ring was changed")
				}
			}
			cluster.testRingRefreshDone = func(err error) { lastDone.Store(&err) }
		})

		script.setPeers([]peerRow{newPeerRow(resiliencePeerHostID, "127.0.0.2")})
		panicOnFill.Store(true)
		logger.armed.Store(true)

		err := session.refreshRing()
		require.Error(t, err, "the failure must still reach the caller")
		require.NotNil(t, lastDone.Load(), "the completion hook must still run")
		require.Error(t, *lastDone.Load(), "the completion hook must still see the failure")
		require.NoError(t, session.refreshRing(), "the next round must still run")
	})

	t.Run("the completion hook panics", func(t *testing.T) {
		var panicOnDone atomic.Bool
		var seen atomic.Int64

		_, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
			cluster.testRingRefreshDone = func(error) {
				seen.Add(1)
				if panicOnDone.CompareAndSwap(true, false) {
					panic("scripted panic from the completion hook")
				}
			}
		})

		panicOnDone.Store(true)
		require.NoError(t, session.refreshRing(), "a panicking completion hook must not fail the round")
		require.NoError(t, session.refreshRing(), "the next round must still run")
		require.GreaterOrEqual(t, seen.Load(), int64(2), "both rounds must have reported completion")
	})
}
