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
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ledgerState is a coherent read of the outage ledger, taken the way the
// scheduler will take it: gen and startedAt under one hold of the lock.
type ledgerState struct {
	gen       uint64
	startedAt time.Time
	members   int
	holds     bool
}

// readLedger snapshots the session's outage ledger.
//
// Parameters:
//   - id: a host ID whose membership to report
//
// Returns:
//   - ledgerState: the generation, its start, the set size and whether id is a member
func readLedger(s *Session, id string) ledgerState {
	s.outage.mu.Lock()
	defer s.outage.mu.Unlock()

	_, holds := s.outage.downSet[id]
	return ledgerState{
		gen:       s.outage.gen,
		startedAt: s.outage.startedAt,
		members:   len(s.outage.downSet),
		holds:     holds,
	}
}

// takeNudge consumes a pending nudge without blocking.
//
// Returns:
//   - bool: true when a nudge was waiting
func takeNudge(s *Session) bool {
	select {
	case <-s.outage.nudge:
		return true
	default:
		return false
	}
}

// addRingHost builds a host at addr and puts it in the ring, so the DOWN and UP
// producers will act on it.
//
// Parameters:
//   - addr: the loopback address to give the host
//
// Returns:
//   - *HostInfo: the ring's object for the new host
func addRingHost(t *testing.T, s *Session, addr string) *HostInfo {
	t.Helper()

	host, err := NewHostInfoFromAddrPort(net.ParseIP(addr), 9042)
	require.NoError(t, err)
	host.setHostID(MustRandomUUID().String())
	_, existed := s.ring.addHostIfMissing(host)
	require.False(t, existed, "the ring must not already hold %s", addr)
	require.True(t, host.IsUp(), "a fresh host starts UP, which is what the DOWN transition needs")
	return host
}

// newFilteredLedgerSession builds a quiescent session with filter installed at
// construction, so no test ever writes cfg while event handlers read it.
//
// Parameters:
//   - filter: the HostFilter to install
//
// Returns:
//   - *Session: a session with the reconnect scheduler disabled
func newFilteredLedgerSession(t *testing.T, filter HostFilter) *Session {
	t.Helper()

	session, _, _ := newPauseRecoverySession(t, func(cluster *ClusterConfig) {
		cluster.ReconnectInterval = 0
		cluster.HostFilter = filter
	})
	return session
}

// TestOutageLedger_FirstDownOpensAnOutage: an empty ledger gaining its first
// member advances the generation, stamps its start and nudges the scheduler.
func TestOutageLedger_FirstDownOpensAnOutage(t *testing.T) {
	f := newOwnershipFixture(t)
	host := addRingHost(t, f.session, "10.0.0.1")

	before := readLedger(f.session, host.HostID())
	require.False(t, takeNudge(f.session), "no nudge may be pending before the first outage")

	f.session.markHostDown(host)

	after := readLedger(f.session, host.HostID())
	require.Equal(t, before.gen+1, after.gen, "the first member of an empty ledger opens an outage")
	require.True(t, after.holds, "the host must be a member")
	require.False(t, after.startedAt.IsZero(), "the outage must be stamped with its start")
	require.True(t, takeNudge(f.session), "opening an outage must nudge the scheduler")
}

// TestOutageLedger_FailedFillRecordsThroughTheRealPath: the producers are
// reached through the driver's own failure path, not only by calling them.
//
// A pool that loses its last connection runs fill, fillingStopped,
// handleHostDown and markHostDown in turn, and it is markHostDown's resolved
// object that must reach the ledger. A test that only calls the producer
// directly would not notice that chain resolving a different object, or not
// arriving at all.
func TestOutageLedger_FailedFillRecordsThroughTheRealPath(t *testing.T) {
	f := newTickFixture(t, nil)

	before := readLedger(f.session, f.host.HostID())
	require.False(t, before.holds, "a healthy host is not a member")
	require.False(t, takeNudge(f.session), "no nudge may be pending on a healthy ring")

	f.driveDown(t)

	after := readLedger(f.session, f.host.HostID())
	require.True(t, after.holds, "the host the failed fill convicted must be a member")
	require.Equal(t, before.gen+1, after.gen, "the failure must open an outage")
	require.False(t, after.startedAt.IsZero(), "the outage must be stamped")
	require.True(t, takeNudge(f.session), "the failure must nudge the scheduler")
}

// TestOutageLedger_JoiningAnOutageChangesNothingElse: a second host failing while
// one is already down joins the outage under way. It must not advance the
// generation, restamp its start, or nudge - all three would let a cluster with
// hosts trickling out reset a backoff or push a pending retry out for ever.
func TestOutageLedger_JoiningAnOutageChangesNothingElse(t *testing.T) {
	f := newOwnershipFixture(t)
	first := addRingHost(t, f.session, "10.0.0.1")
	second := addRingHost(t, f.session, "10.0.0.2")

	f.session.markHostDown(first)
	opened := readLedger(f.session, first.HostID())
	require.True(t, takeNudge(f.session), "the first DOWN nudges")

	f.session.markHostDown(second)

	joined := readLedger(f.session, second.HostID())
	require.Equal(t, opened.gen, joined.gen, "joining an outage must not advance the generation")
	require.Equal(t, opened.startedAt, joined.startedAt, "joining an outage must not restamp its start")
	require.Equal(t, 2, joined.members, "both hosts must be members")
	require.False(t, takeNudge(f.session), "joining an outage must not nudge")
}

// TestOutageLedger_RepeatedDownReportIsIdempotent: markHostDown sets the state
// unconditionally, so it cannot tell a first DOWN from a repeat. The set makes
// the ledger idempotent instead.
func TestOutageLedger_RepeatedDownReportIsIdempotent(t *testing.T) {
	f := newOwnershipFixture(t)
	host := addRingHost(t, f.session, "10.0.0.1")

	f.session.markHostDown(host)
	opened := readLedger(f.session, host.HostID())
	require.True(t, takeNudge(f.session), "the first DOWN nudges")

	f.session.markHostDown(host)

	repeated := readLedger(f.session, host.HostID())
	require.Equal(t, opened.gen, repeated.gen, "a repeated DOWN report must not open a second outage")
	require.Equal(t, opened.startedAt, repeated.startedAt, "a repeated DOWN report must not restamp the outage")
	require.Equal(t, 1, repeated.members, "the set makes the repeat a no-op")
	require.False(t, takeNudge(f.session), "a repeated DOWN report must not nudge")
}

// TestOutageLedger_EmptyingDoesNotAdvance: a host coming up leaves the ledger,
// and emptying it is not itself a generation change - there is no outage left to
// schedule, so the reset waits for the next empty-to-non-empty transition.
func TestOutageLedger_EmptyingDoesNotAdvance(t *testing.T) {
	f := newOwnershipFixture(t)

	f.session.markHostDown(f.host)
	opened := readLedger(f.session, f.host.HostID())
	require.True(t, opened.holds, "the fixture host must be a member while down")

	f.session.pool.addHost(f.host)
	f.hooks.await(t, poolFillDone, f.host, "the fill that brings the host back up")
	require.Eventually(t, func() bool { return f.host.IsUp() }, ownershipBudget, 5*time.Millisecond,
		"the fill must report the host UP")

	emptied := readLedger(f.session, f.host.HostID())
	require.False(t, emptied.holds, "an UP host must leave the ledger")
	require.Zero(t, emptied.members, "the ledger must be empty")
	require.Equal(t, opened.gen, emptied.gen, "emptying the ledger must not advance the generation")
}

// TestOutageLedger_SingleHostFlapOpensEachTime pins accepted behaviour, so it is
// not later "fixed" by mistake: one host cycling UP and DOWN empties the ledger
// each time, so every failure opens a new outage and resets whatever backoff the
// previous one had reached. A successful UP is real evidence the cluster
// changed; the cost is that a host in a crash loop gets no damping at all.
func TestOutageLedger_SingleHostFlapOpensEachTime(t *testing.T) {
	f := newOwnershipFixture(t)
	host := addRingHost(t, f.session, "10.0.0.1")

	f.session.markHostDown(host)
	first := readLedger(f.session, host.HostID())
	require.True(t, takeNudge(f.session), "the first DOWN nudges")

	// Bring it up the way handleNodeConnected does, which needs a registered pool.
	f.session.pool.registerPool(host)
	f.session.handleNodeConnected(host)
	require.False(t, readLedger(f.session, host.HostID()).holds, "an UP host leaves the ledger")

	f.session.markHostDown(host)

	second := readLedger(f.session, host.HostID())
	require.Equal(t, first.gen+1, second.gen, "a flap opens a new outage every time")
	require.True(t, takeNudge(f.session), "a flap nudges every time")
}

// TestOutageLedger_FilteredHostIsNotRelevant: a host the application excluded is
// not something the scheduler will reconnect, so it must not hold an outage open.
func TestOutageLedger_FilteredHostIsNotRelevant(t *testing.T) {
	// The filter is installed at construction and armed through its own atomic:
	// cfg is read by event handlers on other goroutines, so a test must never
	// write it after the session exists.
	filter := &flippableHostFilter{}
	session := newFilteredLedgerSession(t, filter)

	host := addRingHost(t, session, "10.0.0.1")
	rejected := host.HostID()
	filter.reject.Store(&rejected)

	before := readLedger(session, host.HostID())
	session.markHostDown(host)

	after := readLedger(session, host.HostID())
	require.False(t, after.holds, "a filtered host must not be a member")
	require.Equal(t, before.gen, after.gen, "a filtered host must not open an outage")
	require.False(t, takeNudge(session), "a filtered host must not nudge")
	require.Equal(t, NodeDown, host.State(), "the state change itself is unchanged: it happens before the filter")
}

// TestOutageLedger_FilterPanicRecordsIrrelevant: when the HostFilter returns by
// panicking there is no answer to record, so the host is recorded as irrelevant.
//
// That direction is the safe one - the scheduler recomputes membership every
// round, so the cost is a delayed retry, never a host wrongly held in an outage.
// The case that needs the deferred write is a host that is already a member: a
// repeat DOWN report whose filter panics must not leave the stale membership
// behind, because nothing else will take it out while the filter keeps failing.
func TestOutageLedger_FilterPanicRecordsIrrelevant(t *testing.T) {
	t.Run("not yet a member", func(t *testing.T) {
		filter := &panickingHostFilter{}
		session := newFilteredLedgerSession(t, filter)

		host := addRingHost(t, session, "10.0.0.1")
		before := readLedger(session, host.HostID())
		filter.arm()

		require.Panics(t, func() { session.markHostDown(host) }, "the filter panic propagates to the caller")

		after := readLedger(session, host.HostID())
		require.False(t, after.holds, "a host whose relevance is unknown must not be a member")
		require.Equal(t, before.gen, after.gen, "an unanswered filter must not open an outage")

		// The mutex must not have been left locked by the panic.
		done := make(chan struct{})
		go func() {
			session.withOwnedHost(host, func() bool { return true })
			close(done)
		}()
		awaitDone(t, done, "a later transition after the filter panic")
	})

	t.Run("already a member", func(t *testing.T) {
		filter := &panickingHostFilter{}
		session := newFilteredLedgerSession(t, filter)

		host := addRingHost(t, session, "10.0.0.1")
		session.markHostDown(host)
		require.True(t, readLedger(session, host.HostID()).holds, "the host must be a member before the filter breaks")

		filter.arm()
		require.Panics(t, func() { session.markHostDown(host) }, "the filter panic propagates to the caller")

		require.False(t, readLedger(session, host.HostID()).holds,
			"a repeat report whose filter panicked must drop the membership, not leave it behind")
	})
}

// TestOutageLedger_StaleObjectIsNotRecorded: an object the ring has already
// replaced is not a member, because the whole transition is gated on ownership.
func TestOutageLedger_StaleObjectIsNotRecorded(t *testing.T) {
	f := newOwnershipFixture(t)

	stale := f.host
	before := readLedger(f.session, stale.HostID())
	f.replacement(t)

	f.session.markHostDown(stale)

	after := readLedger(f.session, stale.HostID())
	require.False(t, after.holds, "a superseded object must not enter the ledger")
	require.Equal(t, before.gen, after.gen, "a superseded object must not open an outage")
}

// TestOutageLedger_RemovalAfterAnotherDownKeepsTheOutage covers removal order
// (a): the real membership never passed through empty, so neither may the
// ledger.
func TestOutageLedger_RemovalAfterAnotherDownKeepsTheOutage(t *testing.T) {
	f := newOwnershipFixture(t)
	first := addRingHost(t, f.session, "10.0.0.1")
	second := addRingHost(t, f.session, "10.0.0.2")

	f.session.markHostDown(first)
	f.session.markHostDown(second)
	opened := readLedger(f.session, second.HostID())
	require.Equal(t, 2, opened.members)

	f.session.removeHost(first)

	after := readLedger(f.session, second.HostID())
	require.Equal(t, opened.gen, after.gen, "the set never emptied, so the outage must be the same one")
	require.Equal(t, opened.startedAt, after.startedAt, "the outage's start must not be restamped")
	require.Equal(t, 1, after.members, "only the removed host leaves")
	require.True(t, after.holds, "the host that is still down stays a member")
}

// TestOutageLedger_DownAfterRemovalOpensANewOutage covers removal order (b): the
// removal emptied the ledger, so the next failure is a new outage.
func TestOutageLedger_DownAfterRemovalOpensANewOutage(t *testing.T) {
	f := newOwnershipFixture(t)
	first := addRingHost(t, f.session, "10.0.0.1")
	second := addRingHost(t, f.session, "10.0.0.2")

	f.session.markHostDown(first)
	opened := readLedger(f.session, first.HostID())
	require.True(t, takeNudge(f.session), "the first DOWN nudges")

	f.session.removeHost(first)
	require.Zero(t, readLedger(f.session, first.HostID()).members, "the removal must empty the ledger")

	f.session.markHostDown(second)

	after := readLedger(f.session, second.HostID())
	require.Equal(t, opened.gen+1, after.gen, "a failure after the ledger emptied opens a new outage")
	require.True(t, takeNudge(f.session), "the new outage nudges")
}

// TestOutageLedger_RemovalTransactionOrdersAReplacementsDown covers removal order
// (c), which is the defect the transaction exists to close.
//
// With the ring removal outside the mutex, the host ID is free while the removal
// waits for hostPublishMu. A replacement inserted under that ID - setupConn takes
// only the ring's lock - could then transition DOWN and be recorded, and the
// removal's bookkeeping, which deletes by ID, would erase it. The real membership
// would hold a down host that the ledger did not, so the scheduler would never
// arm a retry for it.
//
// Inside the transaction the ID stays taken until the bookkeeping has run, and
// every DOWN queues behind it.
func TestOutageLedger_RemovalTransactionOrdersAReplacementsDown(t *testing.T) {
	f := newOwnershipFixture(t)
	host := f.host

	f.session.markHostDown(host)
	opened := readLedger(f.session, host.HostID())
	require.True(t, opened.holds, "the host must be a member before it is removed")
	require.True(t, takeNudge(f.session), "the first DOWN nudges")

	// Hold the mutex so the removal has to wait for it, which is the window the
	// ring removal must not be able to run ahead in.
	f.session.hostPublishMu.Lock()
	removed := make(chan struct{})
	go func() {
		f.session.removeHost(host)
		close(removed)
	}()
	requireNotReturned(t, removed, "removeHost while the test holds hostPublishMu")

	require.True(t, f.session.ring.owns(host),
		"the ring removal is inside the transaction, so the entry must survive until the removal gets the mutex")
	replacement := newReplacementHost(t, host)
	_, existed := f.session.ring.addHostIfMissing(replacement)
	require.True(t, existed, "the host ID must still be taken while the removal waits")

	f.session.hostPublishMu.Unlock()
	awaitDone(t, removed, "removeHost")
	require.Zero(t, readLedger(f.session, host.HostID()).members, "the removal must empty the ledger")

	// Only now can the replacement take the ID, and its own DOWN is recorded
	// after the removal's bookkeeping rather than being erased by it.
	_, existed = f.session.ring.addHostIfMissing(replacement)
	require.False(t, existed, "the freed host ID must be available once the transaction has finished")
	f.session.markHostDown(replacement)

	after := readLedger(f.session, replacement.HostID())
	require.True(t, after.holds, "the replacement's failure must be recorded")
	require.Equal(t, opened.gen+1, after.gen, "the replacement's failure is a new outage")
}
