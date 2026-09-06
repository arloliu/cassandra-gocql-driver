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
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestSessionCloseCancelsRingFlusherReconnectDial proves Session.Close cancels a
// reconnect dial that the ring flusher is parked in: Close cancels the session
// context before it joins the ring refresher, so the dial returns Canceled, the
// reconnect stops without dialling the contact points, and no host is convicted.
func TestSessionCloseCancelsRingFlusherReconnectDial(t *testing.T) {
	conviction := &recordingConvictionPolicy{onFailure: func(*HostInfo) bool { return false }}
	var fallbacks atomic.Int64
	f := newSnapshotFixture(t, func(cluster *ClusterConfig) {
		cluster.ConvictionPolicy = conviction
		cluster.testControlBeforeFallback = func() { fallbacks.Add(1) }
	})
	f.drain()

	// Fail the control connection's peers write so the ring flusher enters
	// reconnect on its own goroutine, and park the reconnect's dial.
	f.dialer.arm(controlFaultConn(t, f.session))
	parked := f.dialer.parkNextDials()
	f.session.ringRefresher.trigger()

	select {
	case <-parked:
	case <-time.After(lifecycleBudget):
		t.Fatal("the ring flusher's reconnect dial did not park")
	}
	require.Zero(t, f.dialer.dialCanceled.Load(), "no dial may be cancelled before Close")
	attemptsAtPark := f.dialer.dialAttempts.Load()

	closed := asyncClose(f.session)
	select {
	case <-closed:
	case <-time.After(closeBudget):
		t.Fatalf("Session.Close did not return within %v with a reconnect dial parked", closeBudget)
	}

	require.NotZero(t, f.dialer.dialCanceled.Load(), "the parked dial must have been cancelled by Close")
	require.Equal(t, attemptsAtPark, f.dialer.dialAttempts.Load(), "no further host may be dialled after the cancel")
	require.Empty(t, conviction.recorded(), "a cancelled control dial must not convict any host")
	require.Zero(t, fallbacks.Load(), "a cancelled reconnect must not admit the contact-point fallback")
	f.dialer.releaseDials()
}

// TestSessionCloseCancelsSchemaFlusherReconnectDial is the schema-flusher analog:
// the schema flusher's control query fails on write, its reconnect dial parks,
// and Session.Close cancels it.
func TestSessionCloseCancelsSchemaFlusherReconnectDial(t *testing.T) {
	conviction := &recordingConvictionPolicy{onFailure: func(*HostInfo) bool { return false }}
	var fallbacks atomic.Int64
	f := newSchemaFixture(t, func(cluster *ClusterConfig) {
		cluster.ConvictionPolicy = conviction
		cluster.testControlBeforeFallback = func() { fallbacks.Add(1) }
	})

	// Fail the schema refresh's keyspaces write so the schema flusher enters
	// reconnect on its own goroutine, and park the reconnect's dial.
	f.session.stmtsLRU.clear()
	f.dialer.setFailingStatement("system_schema.keyspaces")
	f.dialer.arm(controlFaultConn(t, f.session))
	parked := f.dialer.parkNextDials()
	f.session.schemaDescriber.schemaRefresher.trigger()

	select {
	case <-parked:
	case <-time.After(lifecycleBudget):
		t.Fatal("the schema flusher's reconnect dial did not park")
	}
	attemptsAtPark := f.dialer.dialAttempts.Load()

	closed := asyncClose(f.session)
	select {
	case <-closed:
	case <-time.After(closeBudget):
		t.Fatalf("Session.Close did not return within %v with a reconnect dial parked", closeBudget)
	}

	require.NotZero(t, f.dialer.dialCanceled.Load(), "the parked dial must have been cancelled by Close")
	require.Equal(t, attemptsAtPark, f.dialer.dialAttempts.Load(), "no further host may be dialled after the cancel")
	require.Empty(t, conviction.recorded(), "a cancelled control dial must not convict any host")
	require.Zero(t, fallbacks.Load(), "a cancelled reconnect must not admit the contact-point fallback")
	f.dialer.releaseDials()
}

// TestSessionCloseCancelsSchemaAgreementWait proves the schema agreement wait now
// observes the session context, on the schema-flusher path: a real schema refresh
// runs the wait, a divergent peer keeps it iterating, and Close cancels the
// session context so the refresh completes in well under a second.
func TestSessionCloseCancelsSchemaAgreementWait(t *testing.T) {
	f := newSchemaFixture(t, nil)

	// A peer on a different schema_version keeps the agreement loop from
	// converging, so it iterates (reading peers each time) until cancelled.
	peer := newPeerRow(peerIDOne, "127.0.0.7")
	peer.schemaVersion = "99999999-0000-4000-8000-0000000000ff"
	f.script.setPeers([]peerRow{peer})
	require.NoError(t, f.session.refreshRing(), "admit the divergent peer")
	f.drainSchema()

	// Run the wait through a real schema refresh (the schema flusher), not a
	// direct wrapper call.
	readsBefore := f.dialer.peersReads.Load()
	f.session.schemaDescriber.schemaRefresher.trigger()
	f.awaitSchemaEntered(t, "the schema refresh to start")

	// A second peers read past this point means the loop read peers, compared the
	// divergent versions, slept its retry interval, and read again: a full
	// iteration was processed, not merely one write issued.
	require.Eventually(t, func() bool {
		return f.dialer.peersReads.Load() >= readsBefore+2
	}, lifecycleBudget, time.Millisecond, "the agreement loop must process at least one divergent iteration")
	select {
	case <-f.schemaDone:
		t.Fatal("the refresh completed before cancellation; the loop was not iterating")
	default:
	}

	// Cancel via Close and time the refresh's completion; its agreement wait
	// returns on cancellation.
	start := time.Now()
	closed := asyncClose(f.session)
	select {
	case <-f.schemaDone:
		require.Less(t, time.Since(start), time.Second, "the cancelled agreement wait must return in under a second")
	case <-time.After(closeBudget):
		t.Fatalf("the schema refresh did not complete within %v of Close", closeBudget)
	}
	select {
	case <-closed:
	case <-time.After(lifecycleBudget):
		t.Fatal("Session.Close did not return")
	}
}

// TestFillCancelledByCloseDoesNotConvict proves a pool fill cancelled by
// Session.Close does not convict its host: the fill dial is cancelled with the
// session context, and fillingStopped skips the conviction (and the user
// HostDown callback it would fire) because the context is cancelled.
func TestFillCancelledByCloseDoesNotConvict(t *testing.T) {
	conviction := &recordingConvictionPolicy{}
	f := newSnapshotFixture(t, func(cluster *ClusterConfig) {
		cluster.ConvictionPolicy = conviction
	})
	f.drain()

	// Add a peer to the ring directly (no fill yet), then park every dial and
	// start its fill: the first connection is dialled synchronously and parks,
	// so the pool stays empty. A fill that then fails on an empty pool is exactly
	// what fillingStopped would convict on.
	peer, err := NewHostInfoFromAddrPort(net.ParseIP("127.0.0.7"), 9042)
	require.NoError(t, err)
	peer.setHostID(peerIDOne)
	peer, _ = f.session.ring.addOrUpdate(peer)

	parked := f.dialer.parkNextDials()
	fillReturned := make(chan struct{})
	go func() {
		f.session.startPoolFill(peer)
		close(fillReturned)
	}()

	select {
	case <-parked:
	case <-time.After(lifecycleBudget):
		t.Fatal("the peer's fill dial did not park")
	}
	pool, ok := f.session.pool.getPool(peer)
	require.True(t, ok, "the peer must have a pool")
	require.Zero(t, pool.Size(), "the peer's fill must be parked with an empty pool")

	// Close cancels the session context; the parked dial then returns Canceled and
	// the fill fails on the empty pool.
	closed := asyncClose(f.session)
	select {
	case <-closed:
	case <-time.After(closeBudget):
		t.Fatalf("Session.Close did not return within %v", closeBudget)
	}

	// Join the fill goroutine (Session.Close does not join it) before asserting.
	select {
	case <-fillReturned:
	case <-time.After(lifecycleBudget):
		t.Fatal("the peer's fill did not finish")
	}

	for _, h := range conviction.recorded() {
		require.NotEqual(t, peer.HostID(), h.HostID(), "a fill cancelled by Close must not convict its host")
	}
	require.NotEqual(t, NodeDown, peer.State(), "the peer must not be marked down by a cancelled fill")
	f.dialer.releaseDials()
}

// TestQueryErrorsDuringClose exercises the error classes a query observes as the
// session is closed.
//
// The plan lists four in-flight stages, plus the post-Close fast path.
// Three are gated here with stage evidence:
// an in-flight request (a PREPARE the node parks and never answers) returns one of
// the Close error classes; a query waiting on a pool fill returns ErrSessionClosed
// when the session context is cancelled; and a query issued after Close returns
// fast with the same error.
// The in-flight request is a genuine competing branch, confirmed empirically here:
// Session.Close both cancels the session context and closes the connection, and the
// waiting request observes context.Canceled or ErrConnectionClosed depending on
// which wins, so the test accepts either.
// The two stages left as documentation are the same competition seen from other
// entry points: a PREPARE cancelled in the window before its write, and a coalesced
// write already queued that surfaces io.EOF only when the coalescer's flush loses
// the race to the socket close.
// Neither can be pinned to one error without a racy fixture.
func TestQueryErrorsDuringClose(t *testing.T) {
	t.Run("in-flight request returns a Close error class", func(t *testing.T) {
		script, srv, _ := startLocalHostServer(t)
		cluster := newLocalHostCluster(t, "", srv.Address, nil)
		session, err := cluster.CreateSession()
		require.NoError(t, err, "CreateSession")
		t.Cleanup(session.Close)

		// The node never answers this query's PREPARE: it signals and then blocks,
		// so the request is provably sent and waiting for a response when Close runs.
		parked := script.setHangQuery("hang_for_test")
		result := make(chan error, 1)
		go func() {
			result <- session.Query("SELECT hang_for_test FROM x WHERE k = ?", 1).RetryPolicy(nil).Exec()
		}()
		awaitSignal(t, parked, "the request to reach the server and park")

		// Close both cancels the session context and closes the connection under the
		// waiting request, and these two race: the request observes context.Canceled
		// when the context wins and ErrConnectionClosed when the socket close wins.
		// Both are correct Close-time error classes, so either is accepted; this is
		// the competing-branch behaviour the plan documents for an in-flight request.
		closed := asyncClose(session)
		select {
		case err := <-result:
			ok := errors.Is(err, ErrConnectionClosed) || errors.Is(err, context.Canceled)
			require.True(t, ok,
				"an in-flight request during Close must report ErrConnectionClosed or context.Canceled, got %v", err)
		case <-time.After(closeBudget):
			t.Fatal("the waiting request did not return after Close")
		}
		select {
		case <-closed:
		case <-time.After(lifecycleBudget):
			t.Fatal("Session.Close did not return")
		}
	})

	t.Run("query waiting on a fill returns ErrSessionClosed", func(t *testing.T) {
		harness := newFillHarness(t, 1, nil)
		host := harness.hosts[0]

		// Empty the host's pool and block every fill, so a query drawn to it
		// schedules a fill and then waits for it in awaitFill.
		harness.dialer.arm(nil)
		detachPoolConn(t, harness.pool(t, host))

		waiting := make(chan struct{}, 1)
		harness.session.executor.testBeforeWait = func() {
			select {
			case waiting <- struct{}{}:
			default:
			}
		}

		result := harness.query(t.Context(), func(qry *Query) {
			qry.RetryPolicy(nil)
		})
		awaitSignal(t, waiting, "the query to wait for the fill")

		// Cancel the wait by closing the session; awaitFill returns ErrSessionClosed.
		closed := asyncClose(harness.session)
		require.ErrorIs(t, awaitQuery(t, result), ErrSessionClosed,
			"a query waiting on a fill must report ErrSessionClosed once the session context is cancelled")
		harness.dialer.releaseAll()
		select {
		case <-closed:
		case <-time.After(lifecycleBudget):
			t.Fatal("Session.Close did not return")
		}
	})

	t.Run("query issued after Close fast-fails with ErrSessionClosed", func(t *testing.T) {
		f := newSnapshotFixture(t, nil)
		f.drain()
		requireClosesWithinBudget(t, f.session)

		iter := f.session.Query("SELECT * FROM system.local").Iter()
		require.ErrorIs(t, iter.Close(), ErrSessionClosed,
			"a query issued after Close must fast-fail with ErrSessionClosed")
	})
}

// TestPoolAdmissionRejectedAfterClose proves the pool admission guard still holds
// independently of the context cancel: after Close, addHost builds no pool.
func TestPoolAdmissionRejectedAfterClose(t *testing.T) {
	f := newSnapshotFixture(t, nil)
	f.drain()
	host := controlHost(t, f)

	requireClosesWithinBudget(t, f.session)

	f.session.pool.addHost(host)
	_, ok := f.session.pool.getPool(host)
	require.False(t, ok, "no pool may be built for a host admitted after Close")
}
