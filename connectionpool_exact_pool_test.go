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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Exact-pool publication: a fill cycle's result may only act on the pool the cycle
// ran on.
//
// The neighbouring boundary - a replacement built as a NEW *HostInfo - is covered by
// connectionpool_ownership_test.go, where ring.owns rejects the stale cycle by pointer.
// These tests cover the case where the replacement reuses the SAME object, which owns()
// cannot see: only the pool identity distinguishes the retired cycle from the live one.
//
// Three checks stand behind the fill's failure path, and each test names the one it
// drives:
//
//   - the advisory gate in fillingStopped, which keeps a failure already visible as
//     stale out of the ConvictionPolicy;
//   - D1, in markHostDownFromPool, which authorises the whole DOWN transition;
//   - D2, in removeHostPool, which authorises the deletion under p.mu.

// quiesceReconnects stops the scheduler from filling a pool behind the test's back.
//
// A DOWN host opens an outage, and the first scheduled retry is due one second later
// however large ReconnectInterval is: baseRetryInterval caps the backoff at a second
// (session.go). Several fixtures here leave a host DOWN with an empty replacement and
// then assert that nothing touched it; on a slow machine the sweep can fill that
// replacement, which - with the fixture's dialer still failing - convicts it for real
// and fails a correct build. Zero removes the reconnect phase outright.
func quiesceReconnects(cluster *ClusterConfig) {
	cluster.ReconnectInterval = 0
}

// stateListener records notifications from the public HostStateChangeListener API.
//
// The fill harness installs its collector as the selection policy only, so the listener
// side is invisible there; a test that wants it installs one of these through tune.
//
// The two publications are separate on purpose. Both handlers decide with a bool
// returned from a withOwnedHost closure, and only a true reaches hostListeners; the
// policy call happens inside. So a refusal that wrongly returned true would leave every
// policy, ledger, state and pool assertion satisfied and still call the application
// back. Both directions are asserted on their own.
type stateListener struct {
	down chan *HostInfo
	up   chan *HostInfo
}

var _ HostStatusChangeListener = (*stateListener)(nil)

// newStateListener returns a listener installed on cluster.
func newStateListener(cluster *ClusterConfig) *stateListener {
	l := &stateListener{down: make(chan *HostInfo, 64), up: make(chan *HostInfo, 64)}
	cluster.Metadata.HostListener.HostStateChangeListener = l
	return l
}

// OnHostUp records an UP notification.
func (l *stateListener) OnHostUp(event HostUpEvent) {
	select {
	case l.up <- event.Host:
	default:
	}
}

// OnHostDown records a DOWN notification.
func (l *stateListener) OnHostDown(event HostDownEvent) {
	select {
	case l.down <- event.Host:
	default:
	}
}

// requireNoDown fails the test if a DOWN notification is waiting.
func (l *stateListener) requireNoDown(t *testing.T, what string) {
	t.Helper()

	select {
	case host := <-l.down:
		t.Fatalf("the application listener saw a DOWN for %v: %s", host, what)
	default:
	}
}

// drainUp discards the UP notifications published so far, and keeps draining until the
// channel has been quiet for a short run of intervals.
//
// A single pass is not enough, and the reason is worth stating because it cannot be
// fixed by waiting for the initial UP instead. newFillHarness joins the initial fill's
// POLICY UP and its claim release, and neither orders the public callback:
// handleNodeConnected calls hostListeners.OnHostUp after releasing hostPublishMu, so
// when the harness returns the initial handler can be sitting between those two
// statements. Nor can a test simply wait for that UP to arrive, because
// internalHostListeners suppresses every notification while session.initialized() is
// false and the initial fill normally completes inside CreateSession - so usually it is
// never delivered at all, and a wait would hang.
//
// The quiet run absorbs a late initial UP either way. It bounds a window a few
// instructions wide on one goroutine; closing it outright would need a production seam
// installed before CreateSession, which this change does not add. The residual is that
// window, and it can only ever cause a false FAILURE, never a false pass.
func (l *stateListener) drainUp() {
	quiet := 0
	for quiet < 5 {
		select {
		case <-l.up:
			quiet = 0
		default:
			quiet++
			time.Sleep(time.Millisecond)
		}
	}
}

// requireNoUp fails the test if an UP notification is waiting.
//
// U1's refusal returns false from handleNodeConnected's closure; a refusal that wrongly
// returned true would fire OnHostUp while leaving host state, the ledger, the policy and
// the replacement pool exactly as the UP tests expect them.
func (l *stateListener) requireNoUp(t *testing.T, what string) {
	t.Helper()

	select {
	case host := <-l.up:
		t.Fatalf("the application listener saw an UP for %v: %s", host, what)
	default:
	}
}

// requireNoPolicyDown fails the test if the selection policy saw a DOWN.
//
// The collector's channel is buffered and its writer never blocks, so this is a
// complete reading of what has been published at the moment it is called. Every
// caller therefore runs it only after a barrier that joins the cycle which would
// have published.
//
// Parameters:
//   - t: the test
//   - collector: the harness collector
//   - what: what the absence proves, for the failure message
func requireNoPolicyDown(t *testing.T, collector *hostStateCollector, what string) {
	t.Helper()

	select {
	case host := <-collector.down:
		t.Fatalf("the policy saw a DOWN for %v: %s", host, what)
	default:
	}
}

// TestFillingStopped_RetiredPoolsFailureIsNotConvicted drives the ADVISORY gate.
//
// A pool is retired and a replacement is registered for the SAME *HostInfo while the
// retired pool's cycle is still parked in its dial. When the dial then fails, the
// cycle finds its own pool unregistered before it reaches the conviction policy, so
// the policy is never consulted and the replacement is left alone.
//
// Before exact-pool publication this test's assertions all held the other way: the
// retired cycle convicted the host, markHostDown removed the replacement, and the
// replacement's healthy connection was closed with it.
//
// The barrier is the retired pool's own claim release, not a DOWN event: under the
// fix no DOWN happens, so a test that waited for one could never pass.
func TestFillingStopped_RetiredPoolsFailureIsNotConvicted(t *testing.T) {
	conviction := &recordingConvictionPolicy{}
	var listener *stateListener
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.ConvictionPolicy = conviction
		quiesceReconnects(cluster)
		listener = newStateListener(cluster)
	})
	host := harness.hosts[0]
	poolA := harness.pool(t, host)

	// Pool A empties and starts a cycle that parks in the dialer.
	harness.dialer.arm(nil)
	detached := detachPoolConn(t, poolA)
	require.True(t, poolA.claimFill(), "the fixture must be able to claim a fill")
	go poolA.runFill()
	harness.dialer.awaitStarted(t)

	// A is retired while its cycle is still in flight, and a replacement is
	// registered for the SAME host object. This is what the reconnect sweep or an
	// UP event produces after a conviction: same ring entry, new pool.
	harness.session.pool.removeHost(host)
	poolB, needFill := harness.session.pool.registerPool(host)
	require.NotNil(t, poolB, "the replacement must register")
	require.True(t, needFill, "a freshly registered pool wants a fill")
	require.NotSame(t, poolA, poolB, "the replacement must be a different pool")
	attachPoolConn(poolB, detached)
	require.Equal(t, 1, poolB.Size(), "the replacement must be healthy")

	// A's parked dial now fails. A is empty, so before the fix its cycle convicted
	// the host and took the replacement down with it.
	harness.dialer.setErr(func(string) error { return errFillTestDialRefused })
	harness.dialer.releaseAll()
	awaitNoPendingFills(t, harness.session.pool, poolA)

	require.Empty(t, conviction.recorded(),
		"a failure already visible as stale must not reach the conviction policy")
	requireNoPolicyDown(t, harness.collector, "a retired pool's failure must not convict its host")
	listener.requireNoDown(t, "and must not notify the application listener")

	registered, ok := harness.session.pool.getPoolFor(host)
	require.True(t, ok, "the replacement pool must still be registered")
	require.Same(t, poolB, registered, "and it must still be the replacement")
	require.Equal(t, NodeUp, host.State(), "the host must not have been marked DOWN")
	require.Equal(t, 1, poolB.Size(), "the replacement's connection must not have been closed")
}

// TestFillingStopped_StaleDownIsRefusedAfterTheConvictionPolicy drives D1.
//
// The advisory gate cannot catch every stale failure: it releases pool.mu before it
// reads the registration, and the conviction policy is application code that can take
// arbitrarily long. This test passes the advisory gate legitimately - A is still
// registered when it is read - and only then retires A and registers a replacement,
// while the cycle is blocked inside AddFailure. The conviction is therefore recorded,
// and D1 is what must refuse the DOWN it asks for.
//
// This is the only schedule that reaches D1 with a live replacement, and it is
// production-reachable: fillingStopped has released pool.mu (connectionpool.go) and has
// not taken hostPublishMu at the point it calls the policy.
//
// It does not exercise D2: with D1 intact the removal is never reached. D2 has its own
// test.
func TestFillingStopped_StaleDownIsRefusedAfterTheConvictionPolicy(t *testing.T) {
	var (
		harness         *fillHarness
		host            *HostInfo
		poolA           *hostConnPool
		poolB           *hostConnPool
		replacementConn *Conn
		listener        *stateListener
	)

	// The replacement is installed from inside AddFailure, so the advisory gate has
	// already passed and the cycle is committed to asking for a DOWN.
	replaced := make(chan struct{})
	conviction := &recordingConvictionPolicy{
		onFailure: func(*HostInfo) bool {
			select {
			case <-replaced:
				// A later call must not swap the pool a second time.
				return true
			default:
			}
			defer close(replaced)

			harness.session.pool.removeHost(host)
			var needFill bool
			poolB, needFill = harness.session.pool.registerPool(host)
			if poolB == nil || !needFill || poolB == poolA {
				t.Errorf("the replacement must register as a distinct pool, got %v (needFill=%v)", poolB, needFill)
				return true
			}
			// B is made healthy here, not after the cycle finishes: D1 must refuse
			// a stale DOWN for a replacement that is already serving traffic, which
			// is the state the defect destroyed.
			attachPoolConn(poolB, replacementConn)
			return true
		},
	}

	harness = newFillHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.ConvictionPolicy = conviction
		quiesceReconnects(cluster)
		listener = newStateListener(cluster)
	})
	host = harness.hosts[0]
	poolA = harness.pool(t, host)

	// A empties and starts a cycle that parks in the dialer; A is still the
	// registered pool, so the advisory gate will pass.
	harness.dialer.arm(func(string) error { return errFillTestDialRefused })
	replacementConn = detachPoolConn(t, poolA)
	require.True(t, poolA.claimFill(), "the fixture must be able to claim a fill")
	go poolA.runFill()
	harness.dialer.awaitStarted(t)
	harness.dialer.releaseAll()

	awaitSignal(t, replaced, "the replacement to be installed from inside AddFailure")
	awaitNoPendingFills(t, harness.session.pool, poolA)

	require.Len(t, conviction.recorded(), 1,
		"the failure passed the advisory gate legitimately, so the policy must have seen it exactly once")
	requireNoPolicyDown(t, harness.collector,
		"D1 must refuse the DOWN once the originating pool is no longer registered")
	listener.requireNoDown(t, "and must not notify the application listener")

	registered, ok := harness.session.pool.getPoolFor(host)
	require.True(t, ok, "the replacement pool must still be registered")
	require.Same(t, poolB, registered, "and it must still be the replacement")
	require.Equal(t, NodeUp, host.State(), "the host must not have been marked DOWN")

	// The replacement was healthy before D1 ran, and is untouched by it. Size alone
	// would not prove that: attachPoolConn appends without consulting pool.closed, so
	// a closed pool can still report a connection. The closed flag is the real oracle.
	require.False(t, isPoolClosed(poolB), "the replacement must not have been closed")
	require.Equal(t, 1, poolB.Size(), "and it must still hold its connection")
}

// TestFillingStopped_StaleDownPreservesALegitimateOutage proves that refusing a stale
// DOWN leaves the outage ledger exactly as it found it.
//
// The refusal is a return out of a closure that also arms a panic-safety defer for the
// ledger: if it were not the closure's FIRST act, the stale return would unwind through
// that defer and outageRemove the entry a legitimate DOWN had just added. The host would
// then be down with nothing holding its outage open, and the next DOWN would start a
// fresh generation.
//
// The schedule is the ordinary one. A's cycle fails and blocks inside AddFailure; a real
// DOWN convicts the host, opens an outage and removes A; recovery registers B, which is
// still empty because it has not finished connecting; A resumes into D1. What must be
// true afterwards is not only that A changed nothing, but that it removed nothing.
//
// No other delivered test asserts on the ledger, so this is the only one that can fail
// if the ordering inside the closure regresses.
func TestFillingStopped_StaleDownPreservesALegitimateOutage(t *testing.T) {
	var (
		harness  *fillHarness
		host     *HostInfo
		poolA    *hostConnPool
		poolB    *hostConnPool
		legit    ledgerState
		listener *stateListener
	)

	replaced := make(chan struct{})
	conviction := &recordingConvictionPolicy{
		onFailure: func(*HostInfo) bool {
			select {
			case <-replaced:
				return true
			default:
			}
			defer close(replaced)

			// A real DOWN, taken synchronously so that everything it does - the
			// state change, the ledger entry, the policy callback and the pool
			// removal - has landed before this returns. Reading driver state off
			// the DOWN callback instead would race the removal that follows it.
			harness.session.markHostDown(host)
			legit = readLedger(harness.session, host.HostID())
			if !legit.holds {
				t.Errorf("the legitimate DOWN must have opened an outage for %s", host.HostID())
			}

			var needFill bool
			poolB, needFill = harness.session.pool.registerPool(host)
			if poolB == nil || !needFill || poolB == poolA {
				t.Errorf("the replacement must register as a distinct pool, got %v (needFill=%v)", poolB, needFill)
			}
			return true
		},
	}

	harness = newFillHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.ConvictionPolicy = conviction
		quiesceReconnects(cluster)
		listener = newStateListener(cluster)
	})
	host = harness.hosts[0]
	poolA = harness.pool(t, host)

	harness.dialer.arm(func(string) error { return errFillTestDialRefused })
	detachPoolConn(t, poolA).Close()
	require.True(t, poolA.claimFill(), "the fixture must be able to claim a fill")
	go poolA.runFill()
	harness.dialer.awaitStarted(t)
	harness.dialer.releaseAll()

	awaitSignal(t, replaced, "the legitimate DOWN and the replacement to be installed")
	awaitNoPendingFills(t, harness.session.pool, poolA)

	after := readLedger(harness.session, host.HostID())
	require.True(t, after.holds, "the refused stale DOWN must not remove the legitimate outage entry")
	require.Equal(t, legit.gen, after.gen, "and must not move the outage generation")
	require.Equal(t, legit.startedAt, after.startedAt, "and must not restart the outage clock")
	require.Equal(t, legit.members, after.members, "and must not change the outage membership")

	// Exactly one DOWN reached the policy: the legitimate one. A's stale request
	// must not have produced a second.
	require.Len(t, conviction.recorded(), 1, "the failure passed the advisory gate, so the policy saw it once")
	require.Len(t, awaitHost(t, harness.collector.down, host, "the legitimate DOWN"), 0,
		"no DOWN may precede the legitimate one")
	requireNoPolicyDown(t, harness.collector, "D1 must not publish a second DOWN for the retired pool")
	require.Len(t, awaitHost(t, listener.down, host, "the legitimate listener DOWN"), 0,
		"no listener DOWN may precede the legitimate one")
	listener.requireNoDown(t, "and D1 must not fire a second listener DOWN")

	registered, ok := harness.session.pool.getPoolFor(host)
	require.True(t, ok, "the replacement must still be registered")
	require.Same(t, poolB, registered, "and it must still be the replacement")
	require.False(t, isPoolClosed(poolB), "and it must not have been closed")
	require.Equal(t, NodeDown, host.State(), "the host stays DOWN because the legitimate DOWN put it there")
}

// TestFillingStopped_RetiredPoolDischargesItsDeferredRefill covers the fifth discharge
// case the advisory gate creates for the fill gate's obligation ledger.
//
// The predecessor workstream's contract says an accepted removal obligation is either
// satisfied by a later cycle or explicitly discharged by the cycle that owned the gate.
// It listed four ways to discharge. The advisory gate adds a fifth: a cycle can clear
// the obligation because it predicts a conviction, and then decline to convict because
// its own pool is no longer registered - a disposition none of the four describes.
//
// **A must be unregistered and still OPEN when the cycle completes.** Retirement
// unregisters synchronously and closes asynchronously, so a test that merely calls
// removeHost and waits races its own closer: with pool.closed already true the
// obligation is discharged by case 3, and every assertion below would pass without case
// 5 being reached at all. The fixture therefore holds retirement at the seam between the
// unregistration and the close, which is where the two cases are distinguishable.
//
// That the obligation is discharged is the intended outcome, not an accident. The
// obligation dies with the pool; reserving a successor HERE, after the pool is already
// unregistered, would put a cycle on a pool no query can reach. What must not happen is
// a leaked claim, a successor reserved at this point, or a conviction. The claim count
// is sampled AT the owner's release, not polled afterwards: a wrongly reserved successor
// could be refused by the closed pool and release before a later poll could see it.
//
// This says nothing against successors in general. One admitted BEFORE retirement is
// legitimate and must run; that is T-GATE's subject, not this one's.
func TestFillingStopped_RetiredPoolDischargesItsDeferredRefill(t *testing.T) {
	conviction := &recordingConvictionPolicy{}
	var listener *stateListener
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.NumConns = 2
		cluster.ConvictionPolicy = conviction
		// Nothing but this test may fill a pool for this host; see quiesceReconnects.
		quiesceReconnects(cluster)
		listener = newStateListener(cluster)
	})
	host := harness.hosts[0]
	poolA := harness.pool(t, host)

	// An owning cycle parked in its continuation, holding one connection; that
	// connection then dies, so the cycle owes a refill it cannot serve itself.
	survivor, _ := startPartialCycle(t, harness, poolA)
	killDeferred(t, poolA, survivor)
	require.True(t, poolRefillPending(poolA), "the fixture must leave a real obligation outstanding")

	// Retirement is held between the unregistration and the close, so A is
	// unregistered and demonstrably open for the whole of its cycle's completion.
	// Without this the closer wins the race and case 3 discharges the obligation.
	// The seam fires for every parent-level removal, Close included, so the hold is
	// taken once and released once however often it runs.
	var hold, release sync.Once
	holdClose := make(chan struct{})
	unregistered := make(chan struct{})
	releaseClose := func() { release.Do(func() { close(holdClose) }) }
	t.Cleanup(releaseClose)
	harness.session.pool.testAfterParentNotify = func() {
		hold.Do(func() {
			close(unregistered)
			<-holdClose
		})
	}
	go harness.session.pool.removeHost(host)
	awaitSignal(t, unregistered, "A to be unregistered")
	require.False(t, isPoolClosed(poolA), "A must still be open, or this tests case 3 instead of case 5")

	poolB, _ := harness.session.pool.registerPool(host)
	require.NotNil(t, poolB, "the replacement must register")
	require.NotSame(t, poolA, poolB, "the replacement must be a different pool")

	// The claim count is read on the releasing goroutine, at the release itself.
	base := harness.events.count(poolFillDone)
	sampled := sampleFirstRelease(harness.events, poolA, base)

	// The parked dial fails over an empty pool, which is the shape that predicts a
	// conviction and therefore discharges the obligation before the gate is reached.
	//
	// releaseOne, not releaseAll. The owner has exactly one parked attempt, so this
	// releases that and nothing else: a successor wrongly reserved here finds the
	// dialer still armed and parks with its claim outstanding, which the owner's
	// sample cannot miss. Under releaseAll it would dial, fail, be refused by the
	// advisory gate and release - and its checkpoint could beat the owner's, so the
	// first sample would read ordinal 1, pending 0 and the reservation would go
	// unseen. The decrement and the checkpoint are separate operations.
	harness.dialer.setErr(func(string) error { return errFillTestDialRefused })
	harness.dialer.releaseOne()

	sample := awaitReleaseSample(t, sampled)
	require.Equal(t, 1, sample.ordinal, "the owning cycle's release must be the first of this episode")
	require.Zero(t, sample.pending, "no successor may be reserved on a retired pool")
	require.False(t, isPoolClosed(poolA), "A must still have been open when its cycle completed")

	awaitNoPendingFills(t, harness.session.pool, poolA)
	require.False(t, poolRefillPending(poolA), "the obligation must not outlive the retired pool")
	require.Empty(t, conviction.recorded(), "a retired pool's failure must not reach the conviction policy")
	requireNoPolicyDown(t, harness.collector, "and must not convict the host")
	listener.requireNoDown(t, "and must not notify the application listener")

	registered, ok := harness.session.pool.getPoolFor(host)
	require.True(t, ok, "the replacement must still be registered")
	require.Same(t, poolB, registered, "and it must still be the replacement")
	require.Equal(t, NodeUp, host.State(), "the host must not have been marked DOWN")

	releaseClose()
}

// TestRemoveHostPool_RefusesAPoolItDoesNotOwn drives D2 directly.
//
// D1 already refuses the transition that would reach a wrong removal, so no
// integration schedule can reach D2 with a mismatched pool: registerPool returns the
// pool already registered for the same object rather than replacing it, and every
// other removal takes hostPublishMu. D2 is kept because it makes the removal primitive
// exact on its own terms, for the next caller rather than this one - so it is tested on
// its own terms too.
//
// The three cases are independent subtests on purpose. Run in sequence, the third would
// find an empty map and return through !ok without ever evaluating pool.host != host.
func TestRemoveHostPool_RefusesAPoolItDoesNotOwn(t *testing.T) {
	// replaceRegisteredPool retires the harness pool for host and registers a fresh
	// one for the same object, as the recovery path does.
	replaceRegisteredPool := func(t *testing.T, harness *fillHarness, host *HostInfo) (before, after *hostConnPool) {
		t.Helper()

		before = harness.pool(t, host)
		harness.session.pool.removeHost(host)
		after, _ = harness.session.pool.registerPool(host)
		require.NotNil(t, after, "the replacement must register")
		require.NotSame(t, before, after, "the replacement must be a different pool")
		return before, after
	}

	t.Run("a pool the caller did not act on is left alone", func(t *testing.T) {
		harness := newFillHarness(t, 1, nil)
		host := harness.hosts[0]
		poolA, poolB := replaceRegisteredPool(t, harness, host)

		harness.session.pool.removeHostPool(host, poolA)

		registered, ok := harness.session.pool.getPoolFor(host)
		require.True(t, ok, "the replacement must survive a removal authorised by the retired pool")
		require.Same(t, poolB, registered, "and it must still be the replacement")
	})

	t.Run("a superseded host object removes nothing", func(t *testing.T) {
		harness := newFillHarness(t, 1, nil)
		host := harness.hosts[0]
		_, poolB := replaceRegisteredPool(t, harness, host)

		// Same host ID, different object: the pool is registered under the ID but
		// was built for host, so the host pointer check must refuse it. This is the
		// nil path, where there is no want to compare against.
		superseded := newReplacementHost(t, host)
		require.NotSame(t, host, superseded, "the superseded object must be a different pointer")
		require.Equal(t, host.HostID(), superseded.HostID(), "and must carry the same host ID")

		harness.session.pool.removeHostPool(superseded, nil)

		registered, ok := harness.session.pool.getPoolFor(host)
		require.True(t, ok, "a pool built for another object must not be removed through this one")
		require.Same(t, poolB, registered, "and it must still be the replacement")
	})

	t.Run("nil removes whatever this object has", func(t *testing.T) {
		harness := newFillHarness(t, 1, nil)
		host := harness.hosts[0]
		_, poolB := replaceRegisteredPool(t, harness, host)

		harness.session.pool.removeHostPool(host, nil)

		_, ok := harness.session.pool.getPoolFor(host)
		require.False(t, ok, "the membership form must remove the registered pool")
		require.NotNil(t, poolB, "the replacement is the pool that was removed")
	})
}

// upWatch joins the asynchronous handleNodeConnected callbacks of a harness session.
//
// handleNodeConnected runs on a goroutine fill spawns, so a test that asserts "no UP
// was published" without joining it passes for the wrong reason - it observes the
// absence before the goroutine had a chance to produce one. The seam is host-keyed, so
// a watch cannot tell two pools' handlers apart by argument; a test that has more than
// one in play consumes the earlier one while the later one is provably still parked.
type upWatch struct {
	connected chan *HostInfo
	collector *hostStateCollector
}

// watchNodeConnected installs the join seam and drains whatever the fixture already
// published, so every later observation belongs to the test.
//
// It is installed after newFillHarness has joined the initial fill's UP event and its
// claim release, which orders this write after the initial handler's read of the field.
func watchNodeConnected(t *testing.T, harness *fillHarness) *upWatch {
	t.Helper()

	w := &upWatch{connected: make(chan *HostInfo, 8), collector: harness.collector}
	harness.session.testAfterNodeConnected = func(host *HostInfo) {
		select {
		case w.connected <- host:
		default:
		}
	}
	w.drainUp()
	return w
}

// await blocks until one handleNodeConnected call has returned.
func (w *upWatch) await(t *testing.T, what string) {
	t.Helper()

	select {
	case <-w.connected:
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for %s", fillEventBudget, what)
	}
}

// drainUp discards the UP transitions published so far.
func (w *upWatch) drainUp() {
	for {
		select {
		case <-w.collector.up:
		default:
			return
		}
	}
}

// requireNoPolicyUp fails when the selection policy saw an UP since the last drain.
func (w *upWatch) requireNoPolicyUp(t *testing.T, what string) {
	t.Helper()

	select {
	case host := <-w.collector.up:
		t.Fatalf("the policy saw an UP for %v: %s", host, what)
	default:
	}
}

// retireAndReplace marks host DOWN - which removes and closes its pool - and registers
// a fresh pool for the SAME object, leaving it empty.
//
// This is the state a retired cycle's success must not disturb: the host is DOWN
// because something already convicted it, and the replacement holds nothing, so an UP
// published on the strength of the retired pool's connection would be a lie the
// replacement cannot back.
//
// Returns:
//   - *hostConnPool: the replacement pool
func retireAndReplace(t *testing.T, harness *fillHarness, host *HostInfo) *hostConnPool {
	t.Helper()

	harness.session.markHostDown(host)
	require.Equal(t, NodeDown, host.State(), "the fixture must leave the host DOWN")

	replacement, needFill := harness.session.pool.registerPool(host)
	require.NotNil(t, replacement, "the replacement must register")
	require.True(t, needFill, "a freshly registered pool wants a fill")
	require.Zero(t, replacement.Size(), "the replacement must start empty")
	return replacement
}

// TestFill_RetiredPoolDoesNotPublishUpFromTheInitialFill drives U1 from the
// synchronous fill site, which runs only when the cycle started over an empty pool.
//
// A cycle parks in its first dial, its pool is retired, a replacement is registered
// for the SAME object while the host is DOWN, and the dial then succeeds. connect
// closes the connection it just established because the pool is closed, but returns
// nil, so the cycle still reaches the UP notification - with a connection that exists
// nowhere. U1 is what refuses it.
func TestFill_RetiredPoolDoesNotPublishUpFromTheInitialFill(t *testing.T) {
	var listener *stateListener
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
		quiesceReconnects(cluster)
		listener = newStateListener(cluster)
	})
	host := harness.hosts[0]
	poolA := harness.pool(t, host)
	watch := watchNodeConnected(t, harness)

	// startCount == 0: the cycle takes the synchronous branch.
	harness.dialer.arm(nil)
	detachPoolConn(t, poolA).Close()
	require.True(t, poolA.claimFill(), "the fixture must be able to claim a fill")
	go poolA.runFill()
	harness.dialer.awaitStarted(t)

	poolB := retireAndReplace(t, harness, host)
	watch.drainUp()
	listener.drainUp()

	harness.dialer.releaseAll()
	watch.await(t, "the retired pool's UP notification to be decided")

	watch.requireNoPolicyUp(t, "a retired pool's fill must not publish UP for its replacement")
	listener.requireNoUp(t, "and must not notify the application listener")
	require.Equal(t, NodeDown, host.State(), "the host must stay DOWN")

	registered, ok := harness.session.pool.getPoolFor(host)
	require.True(t, ok, "the replacement must still be registered")
	require.Same(t, poolB, registered, "and it must still be the replacement")
	require.Zero(t, poolB.Size(), "the replacement must still be empty")
}

// TestFill_RetiredPoolDoesNotPublishUpFromTheContinuation drives U1 from the
// asynchronous fill site, which runs only when the cycle started over a pool that
// already held a connection.
//
// The two sites have mutually exclusive conditions - startCount == 0 against
// startCount > 0 - so one fixture cannot reach both, and one mutation cannot stand
// for both.
func TestFill_RetiredPoolDoesNotPublishUpFromTheContinuation(t *testing.T) {
	var listener *stateListener
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.NumConns = 2
		quiesceReconnects(cluster)
		listener = newStateListener(cluster)
	})
	host := harness.hosts[0]
	poolA := harness.pool(t, host)
	watch := watchNodeConnected(t, harness)

	// startCount == 1: the cycle skips the synchronous branch and goes straight to
	// the asynchronous continuation.
	harness.dialer.arm(nil)
	detachPoolConn(t, poolA).Close()
	require.Equal(t, 1, poolA.Size(), "the cycle must start over a pool that holds one connection")
	require.True(t, poolA.claimFill(), "the fixture must be able to claim a fill")
	go poolA.runFill()
	harness.dialer.awaitStarted(t)

	poolB := retireAndReplace(t, harness, host)
	watch.drainUp()
	listener.drainUp()

	harness.dialer.releaseAll()
	watch.await(t, "the retired pool's UP notification to be decided")

	watch.requireNoPolicyUp(t, "a retired pool's continuation must not publish UP for its replacement")
	listener.requireNoUp(t, "and must not notify the application listener")
	require.Equal(t, NodeDown, host.State(), "the host must stay DOWN")

	registered, ok := harness.session.pool.getPoolFor(host)
	require.True(t, ok, "the replacement must still be registered")
	require.Same(t, poolB, registered, "and it must still be the replacement")
	require.Zero(t, poolB.Size(), "the replacement must still be empty")
}

// TestFill_SuccessorAdmittedBeforeRetirementDoesNotAffectTheReplacement is the one
// place this change meets the fill gate.
//
// A successor admitted BEFORE its pool is retired is not a bug and must not be
// refused: requiring that would re-import the synchronous retirement the design
// rejected. What must hold is that its result, like any other cycle's, cannot act on
// a pool it did not run on.
//
// Two barriers carry the test, and neither is the obvious one:
//
//   - admission is proven by the successor's PARKED DIAL, not by its arrival at
//     poolFillAdmission. That checkpoint fires before fill's second admission check,
//     so a successor held there while its pool closes would refuse admission and never
//     reach a publication at all - the test would assert nothing.
//   - completion is joined through testAfterNodeConnected, which is host-keyed and so
//     cannot distinguish the owner's handler from the successor's. The owner's is
//     consumed while the successor is still parked, when nothing else can be in
//     flight, so the second one observed is unambiguously the successor's.
func TestFill_SuccessorAdmittedBeforeRetirementDoesNotAffectTheReplacement(t *testing.T) {
	var listener *stateListener
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.NumConns = 2
		quiesceReconnects(cluster)
		listener = newStateListener(cluster)
	})
	host := harness.hosts[0]
	poolA := harness.pool(t, host)
	watch := watchNodeConnected(t, harness)

	// An owning cycle parked in its continuation, holding one connection.
	survivor, _ := startPartialCycle(t, harness, poolA)
	// That connection dies, so the owner owes a refill it cannot serve itself.
	killDeferred(t, poolA, survivor)

	// The owner's dial succeeds: it lands a connection, ends below size, and hands
	// the obligation to a successor. Because it started over a non-empty pool it also
	// publishes an UP of its own - the one the next step consumes.
	base := harness.events.count(poolFillDone)
	harness.dialer.releaseOne()
	awaitClaimReleases(t, harness.events, base, 1)

	// The successor is admitted and parked in its own dial. Only now is it certain
	// that it passed fill's second admission check.
	harness.dialer.awaitStarted(t)
	awaitGate(t, poolA, true, "the successor to take the gate")

	// Consume the owner's completion while the successor is demonstrably parked.
	watch.await(t, "the owning cycle's UP notification")

	poolB := retireAndReplace(t, harness, host)
	watch.drainUp()
	listener.drainUp()

	harness.dialer.releaseAll()
	watch.await(t, "the successor's UP notification to be decided")

	watch.requireNoPolicyUp(t, "a successor admitted before retirement must not publish UP for the replacement")
	listener.requireNoUp(t, "and must not notify the application listener")
	require.Equal(t, NodeDown, host.State(), "the host must stay DOWN")

	registered, ok := harness.session.pool.getPoolFor(host)
	require.True(t, ok, "the replacement must still be registered")
	require.Same(t, poolB, registered, "and it must still be the replacement")
	require.Zero(t, poolB.Size(), "the replacement must still be empty")
	require.False(t, poolRefillPending(poolA), "no obligation may outlive the retired pool")
}
