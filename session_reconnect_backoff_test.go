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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// reopenOutage makes the ledger record a new outage for id, which is what every
// DOWN host coming back UP and then id failing again does to it.
//
// The whole ledger has to be emptied, not only id's own entry: a generation
// advances on the empty-to-non-empty transition, so a host taken out and put
// back while another is still down joins the outage under way instead of opening
// one. The producers themselves are pinned by the outage ledger's own tests;
// what the tests here need is the transition, applied at a moment they choose.
//
// Parameters:
//   - s: the session whose ledger to move on
//   - id: the host ID the new outage opens with
func reopenOutage(s *Session, id string) {
	s.hostPublishMu.Lock()
	defer s.hostPublishMu.Unlock()

	s.outage.mu.Lock()
	members := make([]string, 0, len(s.outage.downSet))
	for member := range s.outage.downSet {
		members = append(members, member)
	}
	s.outage.mu.Unlock()

	for _, member := range members {
		s.outageRemove(member)
	}
	s.outageAdd(id)
}

// admitGate observes Session.scheduledAdmit and can move the outage ledger on
// from inside it, which is the window the generation gate exists to close.
type admitGate struct {
	// session is the session whose ledger the trap moves on.
	session atomic.Pointer[Session]
	// trap names the host ID whose admission should find the ledger moved on.
	// It is cleared when it fires, so the trap springs once.
	trap atomic.Pointer[string]

	mu sync.Mutex
	// seen is every host the gate was consulted for, in order.
	seen []string
}

// hook is installed as ClusterConfig.testScheduledAdmitStart.
//
// Parameters:
//   - host: the host the scheduler chose to admit
func (g *admitGate) hook(host *HostInfo) {
	g.mu.Lock()
	g.seen = append(g.seen, host.HostID())
	g.mu.Unlock()

	trap, session := g.trap.Load(), g.session.Load()
	if trap == nil || session == nil || *trap != host.HostID() {
		return
	}
	g.trap.Store(nil)
	reopenOutage(session, host.HostID())
}

// armTrap makes the admission of the host with this ID find a newer outage.
//
// Parameters:
//   - id: the host ID to trap
func (g *admitGate) armTrap(id string) { g.trap.Store(&id) }

// consulted returns the host IDs the gate was consulted for, in order.
//
// Returns:
//   - []string: the admissions attempted
func (g *admitGate) consulted() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.seen...)
}

// scanFlipper accepts every host and moves the outage ledger on from inside the
// eligibility scan, which is the window between the scheduler's two reads of it.
type scanFlipper struct {
	session atomic.Pointer[Session]
	id      atomic.Pointer[string]
	// armed makes every scan move the ledger on, so a scheduler that resampled
	// until the two reads agreed would never finish the round.
	armed atomic.Bool
	scans atomic.Int64
}

var _ HostFilter = (*scanFlipper)(nil)

// Accept counts the scan, moves the ledger on when armed, and accepts.
//
// Returns:
//   - bool: always true
func (f *scanFlipper) Accept(*HostInfo) bool {
	f.scans.Add(1)
	if !f.armed.Load() {
		return true
	}
	session, id := f.session.Load(), f.id.Load()
	if session != nil && id != nil {
		reopenOutage(session, *id)
	}
	return true
}

// TestScheduledAdmit_StaleGenerationIsRejected pins I1 at the gate itself.
//
// The authorization for a scheduled admission is the generation in force when
// the pool is registered, not the one that was read when the round chose this
// host. A host that went UP and DOWN again in between belongs to a newer outage
// whose own first retry has not come due, and readmitting it here would jump
// that queue. Nothing else stops it: the host is the same ring object throughout,
// and the pool's guards compare closed state and object identity.
func TestScheduledAdmit_StaleGenerationIsRejected(t *testing.T) {
	gate := &admitGate{}
	f := newTickFixture(t, func(cluster *ClusterConfig) {
		cluster.testScheduledAdmitStart = gate.hook
	})
	gate.session.Store(f.session)
	f.driveDown(t)

	authGen, _ := f.session.outageSnapshot()
	gate.armTrap(f.host.HostID())

	require.False(t, f.session.scheduledAdmit(f.host, authGen),
		"an admission authorized by a superseded outage must be rejected")

	newGen, _ := f.session.outageSnapshot()
	require.Equal(t, authGen+1, newGen, "the trap must have opened a new outage")
	_, pooled := f.session.pool.getPoolFor(f.host)
	require.False(t, pooled, "a rejected admission must register no pool")
	require.Equal(t, []string{f.host.HostID()}, gate.consulted())
}

// TestScheduledAdmit_CurrentGenerationIsAdmitted is the control for the gate: an
// admission the ledger still authorizes registers a pool and fills it, so the
// rejection above is the gate's doing and not an admission path that stopped
// working.
func TestScheduledAdmit_CurrentGenerationIsAdmitted(t *testing.T) {
	f := newTickFixture(t, nil)
	f.driveDown(t)

	authGen, _ := f.session.outageSnapshot()
	require.True(t, f.session.scheduledAdmit(f.host, authGen), "the current generation must authorize the admission")
	f.hooks.await(t, poolFillDone, f.host, "the admitted host's fill")
}

// TestReconnectSweep_AbandonsTheBatchWhenTheGenerationMoves pins the rest of I1:
// the gate is checked per host, and the first rejection ends the batch.
//
// The remaining hosts describe a membership the ledger has already left, so
// carrying on would spend the new outage's first retry on the old outage's list.
// The next round reconciles against the current generation instead.
func TestReconnectSweep_AbandonsTheBatchWhenTheGenerationMoves(t *testing.T) {
	gate := &admitGate{}
	f := newTickFixture(t, func(cluster *ClusterConfig) {
		cluster.testScheduledAdmitStart = gate.hook
	})
	gate.session.Store(f.session)
	f.driveDown(t)

	// A second host joins the outage under way, so the batch has two entries and
	// the generation does not move.
	second := addRingHost(t, f.session, "10.0.0.77")
	f.session.markHostDown(second)

	authGen, _ := f.session.outageSnapshot()
	eligible := []*HostInfo{f.host, second}
	require.Equal(t, eligible, f.session.eligibleDownHosts(f.session.ring.allHosts()),
		"the fixture must present both hosts to the sweep, in this order")

	gate.armTrap(second.HostID())
	f.hooks.drain()

	require.False(t, f.session.reconnectDownedHostsOnce(eligible, authGen),
		"a sweep whose generation moved must report the batch abandoned")

	require.Equal(t, []string{f.host.HostID(), second.HostID()}, gate.consulted(),
		"the gate is checked per host, so the batch stops at the first rejection")
	for _, rec := range f.hooks.drain() {
		require.NotSame(t, second, rec.host, "the rejected host must not have been registered or dialled")
	}
}

// TestHostScheduler_ResamplesTheLedgerOnlyOnce pins the sampling bound in I1.
//
// The generation and the eligibility scan are separate reads, so the pair can
// describe two different outages. One resample is taken to avoid obvious wasted
// work; retrying until they agree would let a single host flapping faster than
// the scan turn a correctness question into a livelock, and the gate at
// registration is what actually keeps a stale round from admitting anything.
func TestHostScheduler_ResamplesTheLedgerOnlyOnce(t *testing.T) {
	flipper := &scanFlipper{}
	f := newTickFixture(t, func(cluster *ClusterConfig) {
		cluster.HostFilter = flipper
	})
	f.driveDown(t)
	require.Len(t, f.session.ring.allHosts(), 1, "the fixture ring must hold exactly one host, so scans are countable")

	flipper.session.Store(f.session)
	id := f.host.HostID()
	flipper.id.Store(&id)

	before, _ := f.session.outageSnapshot()
	clock := newFakeSchedulerClock()
	w := newSessionScheduler(f.session, clock, time.Minute)

	flipper.scans.Store(0)
	flipper.armed.Store(true)
	w.serveReconnect(clock.now())
	flipper.armed.Store(false)

	require.Equal(t, int64(2), flipper.scans.Load(),
		"the round must scan twice: once for the first sample and once for the single resample")

	after, _ := f.session.outageSnapshot()
	require.Equal(t, before+2, after, "each scan moved the ledger on, so it advanced twice")
	require.Equal(t, before+1, w.observedGen,
		"the round must reconcile against the sample it took, not the generation that arrived after it")
}

// TestHostScheduler_SecondDisagreementDoesNotAuthorizeTheOldDeadline pins the
// pairing half of I1: a generation and an outage start are one authorization.
//
// Swapping only the generation onto a deadline the previous outage had already
// passed would let the gate's equality check succeed for an outage whose first
// retry has not arrived. The round must carry the pair it sampled, deadline
// included.
func TestHostScheduler_SecondDisagreementDoesNotAuthorizeTheOldDeadline(t *testing.T) {
	flipper := &scanFlipper{}
	f := newTickFixture(t, func(cluster *ClusterConfig) {
		cluster.HostFilter = flipper
	})
	f.driveDown(t)
	flipper.session.Store(f.session)
	id := f.host.HostID()
	flipper.id.Store(&id)

	firstGen, firstStartedAt := f.session.outageSnapshot()
	clock := newFakeSchedulerClock()
	clock.rebase(firstStartedAt)
	w := newSessionScheduler(f.session, clock, time.Minute)

	// The state a round that has already served this outage leaves behind: the
	// generation observed, and a deadline that has come and gone.
	w.observedGen = firstGen
	w.backoff = time.Second
	w.reconnectDeadline = clock.now().Add(-time.Second)

	f.hooks.drain()
	flipper.armed.Store(true)
	w.serveReconnect(clock.now())
	flipper.armed.Store(false)

	ledgerGen, _ := f.session.outageSnapshot()
	require.Equal(t, firstGen+2, ledgerGen, "both scans must have moved the ledger on")
	require.Equal(t, firstGen+1, w.observedGen,
		"the round must record the outage it sampled, not the one that arrived while it worked")
	require.True(t, w.reconnectDeadline.After(clock.now()),
		"the newer outage's first retry must be armed ahead, not inherited from the expired deadline")
	require.Empty(t, f.hooks.drain(), "no host may be admitted before the newer outage's first retry is due")
}

// TestHostScheduler_ExpiredDeadlineStillServesItsOwnGeneration is the control
// for the test above: with the ledger left alone, the same expired deadline does
// authorize a sweep. Without it the assertion above would also pass on a
// scheduler that had simply stopped reconnecting.
func TestHostScheduler_ExpiredDeadlineStillServesItsOwnGeneration(t *testing.T) {
	f := newTickFixture(t, nil)
	f.driveDown(t)

	gen, startedAt := f.session.outageSnapshot()
	clock := newFakeSchedulerClock()
	clock.rebase(startedAt)
	w := newSessionScheduler(f.session, clock, time.Minute)

	w.observedGen = gen
	w.backoff = time.Second
	w.reconnectDeadline = clock.now().Add(-time.Second)

	w.serveReconnect(clock.now())

	f.hooks.await(t, poolFillDone, f.host, "the retry an expired deadline of the current outage owes")
	require.Equal(t, 2*time.Second, w.backoff, "a served round must leave on the next step")
}

// TestHostScheduler_PanicBeforeReconcileStillAdvances pins I6 on the case the
// invariant was added for.
//
// A HostFilter that panics during the eligibility scan fails the round before
// the reconciliation has set any backoff, so the backoff is still zero. Doubling
// zero is zero, so an advance without a floor would arm a deadline that is
// already due: the loop would wake immediately, call the application's filter
// again, and spin. The floor is what makes every failed round leave with a
// strictly positive delay.
func TestHostScheduler_PanicBeforeReconcileStillAdvances(t *testing.T) {
	const intv = time.Minute

	filter := &panickingHostFilter{}
	f := newTickFixture(t, func(cluster *ClusterConfig) {
		cluster.HostFilter = filter
	})
	f.driveDown(t)
	filter.arm()

	clock := newFakeSchedulerClock()
	w := newSessionScheduler(f.session, clock, intv)
	require.Zero(t, w.backoff, "the round below must start from the state the floor exists for")
	// The phase arms no deadline until a round reconciles one, and every round
	// below fails before it can. The loop drives the clock off the deadline, so
	// it needs a starting instant on the clock's own timeline.
	w.reconnectDeadline = clock.now()

	want := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second}
	for round, expected := range want {
		clock.advance(w.reconnectDeadline.Sub(clock.now()))
		w.serveReconnect(clock.now())
		require.Equal(t, expected, w.backoff, "round %d must leave on a strictly positive delay", round)
		require.Equal(t, clock.now().Add(expected), w.reconnectDeadline,
			"round %d must arm the delay it chose", round)
	}
	require.Equal(t, len(want), filter.count(), "each round must consult the filter exactly once, so the loop is not spinning")
}

// TestHostScheduler_PanicAfterAnEmptyRoundStillAdvances pins the other origin of
// a zero backoff: a round that found nothing to reconnect clears it deliberately.
// A failure on the next round starts from that zero, so the floor has to hold
// there too.
func TestHostScheduler_PanicAfterAnEmptyRoundStillAdvances(t *testing.T) {
	const intv = time.Minute

	filter := &panickingHostFilter{}
	f := newTickFixture(t, func(cluster *ClusterConfig) {
		cluster.HostFilter = filter
	})

	clock := newFakeSchedulerClock()
	w := newSessionScheduler(f.session, clock, intv)
	// A round on a healthy ring: nothing to reconnect, so the phase is cleared.
	w.backoff = 30 * time.Second
	w.reconnectDeadline = clock.now().Add(30 * time.Second)
	w.serveReconnect(clock.now())
	require.Zero(t, w.backoff, "a round with nothing to reconnect must clear the backoff")
	require.True(t, w.reconnectDeadline.IsZero(), "a round with nothing to reconnect must clear the deadline")

	// Something for the next round to scan, and only then a filter that fails on
	// it: the conviction itself consults the filter.
	f.driveDown(t)
	filter.arm()
	w.serveReconnect(clock.now())
	require.Equal(t, 2*time.Second, w.backoff, "a failure from the cleared state must still leave a positive delay")
	require.Equal(t, clock.now().Add(2*time.Second), w.reconnectDeadline)
}

// TestHostScheduler_JoiningAnOutageDoesNotResetTheRhythm pins the reconciliation
// rule F3 rests on.
//
// A host that fails while another is already down joins the outage under way. It
// must neither reset the backoff to the first step nor move the retry that is
// already pending: on a cluster where nodes keep entering and leaving, either
// would put the delay back to one second for ever and defer the retry every
// time, so there would be no exponential backoff at all.
func TestHostScheduler_JoiningAnOutageDoesNotResetTheRhythm(t *testing.T) {
	const intv = time.Minute

	f := newTickFixture(t, nil)
	f.driveDown(t)
	gen, startedAt := f.session.outageSnapshot()

	clock := newFakeSchedulerClock()
	clock.rebase(startedAt)
	w := newSessionScheduler(f.session, clock, intv)

	// Ramp the phase up a few steps.
	w.serveReconnect(clock.now())
	for range 3 {
		clock.advance(w.reconnectDeadline.Sub(clock.now()))
		w.serveReconnect(clock.now())
	}
	require.Equal(t, 8*time.Second, w.backoff, "the ramp must have reached its fourth step")
	deadline := w.reconnectDeadline

	second := addRingHost(t, f.session, "10.0.0.78")
	f.session.markHostDown(second)
	joinedGen, _ := f.session.outageSnapshot()
	require.Equal(t, gen, joinedGen, "joining an outage must not open a new one")

	// A round that is not yet due: it reconciles and finds nothing to change.
	w.serveReconnect(clock.now())
	require.Equal(t, 8*time.Second, w.backoff, "a host joining the outage must not reset the backoff")
	require.Equal(t, deadline, w.reconnectDeadline, "a host joining the outage must not move the pending retry")
}

// TestHostScheduler_RecoveryClearsThePhaseAndTheNextOutageStartsOver pins the
// other end of the rhythm: the backoff returns to the first step only once there
// is nothing left to reconnect, and the outage after that is armed from its own
// start.
func TestHostScheduler_RecoveryClearsThePhaseAndTheNextOutageStartsOver(t *testing.T) {
	const intv = time.Minute

	f := newTickFixture(t, nil)
	f.driveDown(t)
	_, startedAt := f.session.outageSnapshot()

	clock := newFakeSchedulerClock()
	clock.rebase(startedAt)
	w := newSessionScheduler(f.session, clock, intv)

	w.serveReconnect(clock.now())
	for range 3 {
		clock.advance(w.reconnectDeadline.Sub(clock.now()))
		w.serveReconnect(clock.now())
	}
	require.Equal(t, 8*time.Second, w.backoff)

	// The host is back, so the scan finds nothing to reconnect.
	f.host.setState(NodeUp)
	w.serveReconnect(clock.now())
	require.Zero(t, w.backoff, "a recovered ring must clear the backoff")
	require.True(t, w.reconnectDeadline.IsZero(), "a recovered ring must leave no reconnect deadline")

	// The next outage is a new generation, armed from its own start.
	f.session.markHostDown(f.host)
	nextGen, nextStartedAt := f.session.outageSnapshot()
	clock.rebase(nextStartedAt)
	w.serveReconnect(clock.now())

	require.Equal(t, nextGen, w.observedGen)
	require.Equal(t, time.Second, w.backoff, "a new outage must start at the base interval again")
	require.Equal(t, nextStartedAt.Add(time.Second), w.reconnectDeadline,
		"a new outage's first retry must be armed from its own start")
}

// TestHostScheduler_DriftRemedyArmsFromNow covers the reconciliation's third
// case: hosts to retry with no outage recorded for them.
//
// A HostFilter is application code with no way to announce that its answer
// changed, so a host it starts accepting can be eligible while the ledger, which
// only its producers may write, holds nothing. The scheduler arms a deadline of
// its own rather than correcting the ledger - a worker that wrote there would
// erase whatever a producer recorded while it was sampling.
func TestHostScheduler_DriftRemedyArmsFromNow(t *testing.T) {
	const intv = time.Minute

	f := newTickFixture(t, nil)
	// DOWN in the ring but never through a producer, which is the shape a filter
	// that started accepting a host leaves behind.
	f.session.pool.removeHost(f.host)
	f.host.setState(NodeDown)

	gen, _ := f.session.outageSnapshot()
	require.Zero(t, gen, "the ledger must hold no outage for this host")

	clock := newFakeSchedulerClock()
	w := newSessionScheduler(f.session, clock, intv)
	w.serveReconnect(clock.now())

	require.Equal(t, time.Second, w.backoff, "the remedy must arm one base interval")
	require.Equal(t, clock.now().Add(time.Second), w.reconnectDeadline,
		"a host with no recorded outage has no start to arm from, so the remedy is measured from the round")

	clock.advance(time.Second)
	w.serveReconnect(clock.now())
	f.hooks.await(t, poolFillDone, f.host, "the remedy's retry")
}

// TestHostScheduler_SubSecondIntervalDegeneratesToAFixedRhythm pins the direct
// consequence of capping the base interval by ReconnectInterval.
//
// A configuration that asks for retries faster than once a second is honoured
// rather than overridden, which means its first step is already the cap and the
// exponential ramp collapses into the fixed rhythm it asked for.
func TestHostScheduler_SubSecondIntervalDegeneratesToAFixedRhythm(t *testing.T) {
	const intv = 200 * time.Millisecond

	f := newTickFixture(t, nil)
	f.driveDown(t)
	_, startedAt := f.session.outageSnapshot()

	clock := newFakeSchedulerClock()
	clock.rebase(startedAt)
	w := newSessionScheduler(f.session, clock, intv)
	require.Equal(t, intv, w.baseRetryInterval(), "an interval under a second must cap the base")

	w.serveReconnect(clock.now())
	require.Equal(t, startedAt.Add(intv), w.reconnectDeadline)

	for round := range 4 {
		clock.advance(w.reconnectDeadline.Sub(clock.now()))
		w.serveReconnect(clock.now())
		require.Equal(t, intv, w.backoff, "round %d must stay on the configured rhythm", round)
	}
}

// TestHostScheduler_NudgeWakesTheLoop proves the announcement side of the ledger
// is connected: an outage that opens while the loop is parked on a long timer
// must not wait for that timer to expire.
//
// Without it a session whose only armed deadline is the five-minute safety net
// would take up to five minutes to notice a node went down, whatever
// ReconnectInterval said.
func TestHostScheduler_NudgeWakesTheLoop(t *testing.T) {
	const intv = time.Minute

	f := newTickFixture(t, nil)
	clock := newFakeSchedulerClock()
	clock.rebase(time.Now())
	w := newSessionScheduler(f.session, clock, intv)
	require.False(t, takeNudge(f.session), "the fixture must start with no nudge pending")

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.run()
	}()

	// Parked on the safety net: nothing else is armed on a healthy ring.
	first := awaitArmed(t, clock, "the first timer")
	require.Equal(t, ringFullRefreshInterval, first.delay,
		"a quiet session must be parked on the safety net alone")

	f.driveDown(t)

	// The nudge, not the timer, is what ends the round: the timer above is never
	// fired.
	second := awaitArmed(t, clock, "the timer armed after the nudge")
	require.Less(t, second.delay, ringFullRefreshInterval,
		"the round the nudge started must have reconciled the new outage and armed its first retry")
	require.Positive(t, second.delay, "the retry the nudge produced must be armed ahead, not already due")
	require.LessOrEqual(t, second.delay, 2*time.Second,
		"the first retry is one base interval after the outage began")

	f.session.cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the scheduler did not exit after the session context was cancelled")
	}
}

// awaitArmed waits for the scheduler's next timer without firing it.
//
// Parameters:
//   - clock: the fake clock the scheduler arms against
//   - what: what the test is waiting for, for the failure message
//
// Returns:
//   - *armedTimer: the timer the scheduler armed
func awaitArmed(t *testing.T, clock *fakeSchedulerClock, what string) *armedTimer {
	t.Helper()

	select {
	case timer := <-clock.armed:
		return timer
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

// TestHostScheduler_NewOutageSurvivesAFailedRound pins the last enumerated I6
// case: a generation that arrives while a round is failing keeps its own
// absolute deadline.
//
// The failure completion arms now plus the next backoff step, which for a fresh
// outage is later than the deadline that outage is owed. Nothing in the failure
// path knows about the new generation, so what has to hold is that the next
// round's reconciliation overrides it - the round observed no generation, so
// case one still fires and re-derives the deadline from the outage's own start.
func TestHostScheduler_NewOutageSurvivesAFailedRound(t *testing.T) {
	const intv = time.Minute

	filter := &panickingHostFilter{}
	f := newTickFixture(t, func(cluster *ClusterConfig) {
		cluster.HostFilter = filter
	})

	// A first outage, convicted while the filter still answers - markHostDown
	// consults it, so it cannot be armed yet.
	f.session.markHostDown(f.host)
	firstGen, firstStartedAt := f.session.outageSnapshot()
	require.Positive(t, firstGen, "the conviction must have opened an outage")

	clock := newFakeSchedulerClock()
	clock.rebase(firstStartedAt)
	w := newSessionScheduler(f.session, clock, intv)
	w.reconnectDeadline = clock.now()

	filter.arm()
	w.serveReconnect(clock.now())
	require.Zero(t, w.observedGen, "a round that failed in the scan must not record a generation")
	require.Equal(t, clock.now().Add(2*time.Second), w.reconnectDeadline,
		"the failure completion must arm a retry of its own")

	// A newer outage arrives while the phase is held back by that failure.
	reopenOutage(f.session, f.host.HostID())
	gen, startedAt := f.session.outageSnapshot()
	require.Equal(t, firstGen+1, gen, "the ledger must have opened a second outage")

	// The next round reaches the reconciliation, which must derive the deadline
	// from the outage rather than inherit the one the failed round armed.
	filter.disarm()
	w.serveReconnect(clock.now())

	require.Equal(t, gen, w.observedGen)
	require.Equal(t, startedAt.Add(time.Second), w.reconnectDeadline,
		"the new outage's absolute deadline must win over the one the failed round armed")
	require.Equal(t, time.Second, w.backoff, "the new outage must start at the base interval")
}

// TestHostScheduler_DriftIntoAnOutageInheritsItsBackoff pins the worse of the
// two service bounds in I4, so it is not mistaken for a bug later.
//
// A host that becomes eligible while another is already down does not make the
// ledger go from empty to non-empty, so no generation opens and the scheduler
// keeps the backoff the outage under way had reached. With a long
// ReconnectInterval that is a long wait - far longer than the bound an idle
// scheduler gives - and it is accepted rather than fixed: the remedy would be a
// mechanism whose only customer is a HostFilter that changed its answer with no
// event to announce it.
func TestHostScheduler_DriftIntoAnOutageInheritsItsBackoff(t *testing.T) {
	const intv = 30 * time.Minute

	filter := &flippableHostFilter{}
	f := newTickFixture(t, func(cluster *ClusterConfig) {
		cluster.HostFilter = filter
	})

	// B is excluded before it goes down, so no producer records it.
	second := addRingHost(t, f.session, "10.0.0.79")
	rejected := second.HostID()
	filter.reject.Store(&rejected)
	f.session.markHostDown(second)
	require.False(t, readLedger(f.session, second.HostID()).holds,
		"an excluded host must not be recorded as relevant")

	// A is down and its retries have reached the cap.
	f.driveDown(t)
	gen, startedAt := f.session.outageSnapshot()
	clock := newFakeSchedulerClock()
	clock.rebase(startedAt)
	w := newSessionScheduler(f.session, clock, intv)
	w.observedGen = gen
	w.backoff = intv
	w.reconnectDeadline = clock.now().Add(intv)

	// The filter starts accepting B. The ledger does not move, because only its
	// producers may write it and none of them ran.
	filter.reject.Store(nil)
	driftGen, _ := f.session.outageSnapshot()
	require.Equal(t, gen, driftGen, "a filter changing its answer must not open an outage")

	w.serveReconnect(clock.now())
	require.Equal(t, intv, w.backoff, "the drifted host inherits the outage's backoff, not a fresh base interval")
	require.Equal(t, clock.now().Add(intv), w.reconnectDeadline,
		"the drifted host waits out the pending retry rather than being served inside a safety-net period")

	// It is served when that retry comes due, so the bound is long, not absent.
	clock.advance(intv)
	f.hooks.drain()
	w.serveReconnect(clock.now())
	f.hooks.await(t, poolFillDone, second, "the drifted host's retry once the outage's own deadline came due")
}
