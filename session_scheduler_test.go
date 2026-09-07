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

// schedulerEpoch is the fake clock's starting instant. Any fixed time works;
// a round number keeps failure messages readable.
var schedulerEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// armedTimer is one timer the scheduler asked the fake clock for.
type armedTimer struct {
	// delay is the duration the scheduler chose, which is the scheduling
	// decision under test.
	delay time.Duration
	// fired receives when the test releases the round.
	fired chan time.Time
}

// fakeSchedulerClock is a virtual clock that lets a test release the
// scheduler's rounds one at a time and read back the delay it chose for each.
//
// Time only moves when a timer is released, so a round's deadline arithmetic is
// exact and nothing depends on wall-clock timing.
type fakeSchedulerClock struct {
	mu      sync.Mutex
	current time.Time
	delays  []time.Duration
	armed   chan *armedTimer
}

var _ schedulerClock = (*fakeSchedulerClock)(nil)

// newFakeSchedulerClock builds a clock starting at schedulerEpoch.
//
// Returns:
//   - *fakeSchedulerClock: a clock with no timer armed
func newFakeSchedulerClock() *fakeSchedulerClock {
	return &fakeSchedulerClock{current: schedulerEpoch, armed: make(chan *armedTimer, 64)}
}

// now returns the virtual time.
//
// Returns:
//   - time.Time: the clock's current instant
func (c *fakeSchedulerClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.current
}

// newTimer records the chosen delay and publishes the timer to the test.
//
// Parameters:
//   - d: the delay the scheduler chose
//
// Returns:
//   - <-chan time.Time: fires when the test releases the round
//   - func(): a no-op stop; the fake timer holds no resources
func (c *fakeSchedulerClock) newTimer(d time.Duration) (<-chan time.Time, func()) {
	timer := &armedTimer{delay: d, fired: make(chan time.Time, 1)}
	c.mu.Lock()
	c.delays = append(c.delays, d)
	c.mu.Unlock()
	c.armed <- timer
	return timer.fired, func() {}
}

// rebase moves the clock onto another timeline, so a scheduler driven by it
// shares one with the wall-clock instants the outage ledger stamps.
//
// The ledger's producers read the wall clock, and the deadline a new outage arms
// is absolute - the outage's own start plus one base interval - so a test that
// left the fake clock on its own epoch would find that deadline months away and
// never due.
//
// Parameters:
//   - at: the instant the clock should now read
func (c *fakeSchedulerClock) rebase(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = at
}

// advance moves virtual time forward without firing any timer, for tests that
// drive phases directly instead of running the loop.
//
// Parameters:
//   - d: how far to move the clock
func (c *fakeSchedulerClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = c.current.Add(d)
}

// release waits for the scheduler to arm its next timer, advances virtual time
// by the delay it chose and fires it.
//
// Waiting for the timer is the test's round barrier: the scheduler cannot arm
// round N+1's timer until round N has been served.
//
// Parameters:
//   - what: what the test is waiting for, for the failure message
//
// Returns:
//   - time.Duration: the delay the scheduler chose for the released round
func (c *fakeSchedulerClock) release(t *testing.T, what string) time.Duration {
	t.Helper()

	select {
	case timer := <-c.armed:
		c.mu.Lock()
		c.current = c.current.Add(timer.delay)
		fired := c.current
		c.mu.Unlock()
		timer.fired <- fired
		return timer.delay
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return 0
	}
}

// panickingHostFilter accepts every host until it is armed, then panics on
// every call, standing in for application code that fails inside the reconnect
// sweep. It starts inert so session setup can run.
type panickingHostFilter struct {
	armed atomic.Bool

	mu    sync.Mutex
	calls int
}

var _ HostFilter = (*panickingHostFilter)(nil)

// arm makes every later call panic.
func (f *panickingHostFilter) arm() { f.armed.Store(true) }

// disarm makes the filter accept again, so a test can assert that what it was
// holding back resumes.
func (f *panickingHostFilter) disarm() { f.armed.Store(false) }

// Accept accepts until armed, and panics after that.
//
// Returns:
//   - bool: true while the filter is inert
func (f *panickingHostFilter) Accept(*HostInfo) bool {
	if !f.armed.Load() {
		return true
	}
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	panic("gocql: test induced HostFilter panic")
}

// count returns how many times the filter was consulted.
//
// Returns:
//   - int: the call count
func (f *panickingHostFilter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// newTestScheduler builds a scheduler over a fixture session and a fake clock.
//
// Parameters:
//   - session: the session the scheduler drives
//   - clock: the fake clock
//   - intv: the reconnect interval
//
// Returns:
//   - *hostScheduler: a scheduler with neither phase armed
func newTestScheduler(session *Session, clock schedulerClock, intv time.Duration) *hostScheduler {
	return &hostScheduler{session: session, clock: clock, reconnectInterval: intv}
}

// newSessionScheduler builds the scheduler the way a session does - the periodic
// refresh armed, the reconnect phase armed only when intv is positive - and
// re-bases the refresh deadline onto the fake clock's epoch.
//
// Parameters:
//   - session: the session the scheduler drives
//   - clock: the fake clock to drive it with
//   - intv: the reconnect interval
//
// Returns:
//   - *hostScheduler: a scheduler whose deadlines are on the fake clock's timeline
func newSessionScheduler(session *Session, clock *fakeSchedulerClock, intv time.Duration) *hostScheduler {
	w := session.newHostScheduler(intv)
	w.clock = clock
	// Re-base onto the fake epoch. The period the constructor itself chose is
	// asserted directly in TestHostScheduler_ConstructionArmsOnlyWhatIsEnabled,
	// because this line would otherwise hide it.
	w.fullRefreshDeadline = clock.now().Add(ringFullRefreshInterval)
	return w
}

// pinAtBackoffCap puts the reconnect phase where its rhythm is one fixed
// interval again: the outage already observed, so no round reconciles a new one,
// and the backoff already at the cap, where min(max(intv, base)*2, intv) is intv.
//
// Tests about the loop's structure use it so the exponential ramp does not have
// to be replayed in every one of them; the ramp itself is pinned by
// TestHostScheduler_BackoffDoublesToTheCap.
//
// Parameters:
//   - w: the scheduler to pin
//   - session: the session whose ledger it is reconciling against
func pinAtBackoffCap(w *hostScheduler, session *Session) {
	w.observedGen = session.outageGen()
	w.backoff = w.reconnectInterval
	w.reconnectDeadline = w.clock.now().Add(w.reconnectInterval)
}

// TestHostScheduler_NextWaitPicksEarliestArmedDeadline pins the timer choice:
// an unarmed phase takes no part, the earliest armed deadline wins, and a
// deadline already in the past yields zero rather than a negative delay.
func TestHostScheduler_NextWaitPicksEarliestArmedDeadline(t *testing.T) {
	now := schedulerEpoch

	tests := []struct {
		name      string
		reconnect time.Time
		refresh   time.Time
		floor     time.Time
		wantWait  time.Duration
		wantArmed bool
	}{
		{
			name:      "nothing armed",
			wantArmed: false,
		},
		{
			name:      "only reconnect",
			reconnect: now.Add(time.Minute),
			wantWait:  time.Minute,
			wantArmed: true,
		},
		{
			name:      "only refresh",
			refresh:   now.Add(2 * time.Minute),
			wantWait:  2 * time.Minute,
			wantArmed: true,
		},
		{
			name:      "reconnect is earlier",
			reconnect: now.Add(time.Minute),
			refresh:   now.Add(2 * time.Minute),
			wantWait:  time.Minute,
			wantArmed: true,
		},
		{
			name:      "refresh is earlier",
			reconnect: now.Add(3 * time.Minute),
			refresh:   now.Add(2 * time.Minute),
			wantWait:  2 * time.Minute,
			wantArmed: true,
		},
		{
			name:      "a passed deadline waits zero, not a negative delay",
			reconnect: now.Add(-time.Minute),
			wantWait:  0,
			wantArmed: true,
		},
		{
			name:      "the retry floor holds the refresh phase back",
			refresh:   now.Add(-time.Minute),
			floor:     now.Add(time.Second),
			wantWait:  time.Second,
			wantArmed: true,
		},
		{
			name:      "a floor on an unarmed refresh arms nothing",
			floor:     now.Add(time.Second),
			wantArmed: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := &hostScheduler{
				reconnectDeadline:   tc.reconnect,
				fullRefreshDeadline: tc.refresh,
				refreshRetryFloor:   tc.floor,
			}

			wait, armed := w.nextWait(now)
			require.Equal(t, tc.wantArmed, armed)
			if tc.wantArmed {
				require.Equal(t, tc.wantWait, wait)
			}
		})
	}
}

// TestHostScheduler_BackoffDoublesToTheCap pins F3 on the shape the default
// configuration has: a 60s ReconnectInterval against a five-minute refresh
// period, so the whole ramp happens inside one safety-net period and nothing in
// the assertion depends on the two phases sharing a rhythm.
//
// The delays asserted are the ones the scheduler chose, read back from the
// clock. They are not the moments a dial was observed: a pool fill dials its
// first connection synchronously, with the reconnection policy's own retries, so
// an unreachable host would push every later observation out.
//
// The first delay is not part of the sequence read here - it is armed from the
// outage's own start, which TestHostScheduler_NewOutageArmsFromItsStart pins -
// so the ramp below begins at the first doubling.
func TestHostScheduler_BackoffDoublesToTheCap(t *testing.T) {
	const intv = time.Minute

	f := newTickFixture(t, nil)
	f.driveDown(t)
	_, startedAt := f.session.outageSnapshot()

	clock := newFakeSchedulerClock()
	clock.rebase(startedAt)
	w := newSessionScheduler(f.session, clock, intv)

	// The ramp is read from serveReconnect directly: the loop's own timer choice
	// is the minimum of both phases, and the safety net would win the later
	// rounds once the backoff reaches the cap.
	require.Equal(t, time.Second, w.baseRetryInterval(), "the default interval must give a one second base")

	// The first round only reconciles. The outage's first retry is owed at its own
	// start plus one base interval, which has not arrived yet.
	w.serveReconnect(clock.now())
	require.Equal(t, time.Second, w.backoff, "a new outage starts at the base interval")
	require.Equal(t, startedAt.Add(time.Second), w.reconnectDeadline,
		"the first retry must be armed from the outage's own start")

	want := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second,
		32 * time.Second, intv, intv}
	for round, expected := range want {
		clock.advance(w.reconnectDeadline.Sub(clock.now()))
		w.serveReconnect(clock.now())
		require.Equal(t, expected, w.backoff, "round %d must double towards the cap and stop there", round)
		require.Equal(t, clock.now().Add(expected), w.reconnectDeadline,
			"round %d must arm the delay it chose", round)
	}
}

// TestHostScheduler_NewOutageArmsFromItsStart pins the first step of the ramp
// and where it is measured from.
//
// A new outage's first retry is owed at a fixed instant - the outage's own start
// plus one base interval - and not one base interval after whenever the loop
// happened to wake. Arming it from now would let a round held up elsewhere push
// the retry it was late for further out, and would make the delay depend on the
// loop's timing rather than the outage's.
func TestHostScheduler_NewOutageArmsFromItsStart(t *testing.T) {
	const intv = time.Minute

	f := newTickFixture(t, nil)
	f.driveDown(t)
	_, startedAt := f.session.outageSnapshot()

	clock := newFakeSchedulerClock()
	clock.rebase(startedAt)
	w := newSessionScheduler(f.session, clock, intv)
	require.Zero(t, w.observedGen, "a fresh scheduler has observed no outage")
	require.True(t, w.reconnectDeadline.IsZero(), "a fresh scheduler arms no reconnect deadline")

	// The loop wakes well after the outage began, which is what a slow round or a
	// late nudge looks like.
	clock.advance(5 * time.Second)
	w.serveReconnect(clock.now())

	gen, _ := f.session.outageSnapshot()
	require.Equal(t, gen, w.observedGen, "the round must record the outage it reconciled")
	require.Equal(t, 2*time.Second, w.backoff,
		"an overdue first retry must be served on arrival, and leave on the second step")
	require.Equal(t, clock.now().Add(2*time.Second), w.reconnectDeadline)

	// The deadline the round served was the outage's own, not one measured from
	// the round: the retry actually ran rather than being deferred a full step.
	// The pool itself cannot be asserted on - the fill fails against the closed
	// gate and markHostDown unregisters it again before the round returns.
	f.hooks.await(t, poolFillDone, f.host, "the overdue retry's fill")
}

// TestHostScheduler_QuietSessionArmsNoReconnect pins the other half of the
// deadline's ownership: it is produced by the reconciliation from an outage the
// ledger recorded, and by nothing else.
//
// A session with every host UP owes no reconnect at all, so it must keep no
// reconnect deadline - the safety net is the only thing left to wake it. A
// deadline armed on construction would be a retry for an outage that never
// happened, and would consult the application's HostFilter on a rhythm the
// cluster gave no reason for.
func TestHostScheduler_QuietSessionArmsNoReconnect(t *testing.T) {
	const intv = time.Minute

	filter := &countingHostFilter{}
	f := newTickFixture(t, func(cluster *ClusterConfig) {
		cluster.HostFilter = filter
	})

	clock := newFakeSchedulerClock()
	w := newSessionScheduler(f.session, clock, intv)

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.run()
	}()

	for round := range 3 {
		require.Equal(t, ringFullRefreshInterval, clock.release(t, "the next round's timer"),
			"round %d must be scheduled by the safety net alone", round)
		f.awaitEntered(t, "the periodic refresh")
		require.Error(t, awaitRefreshDone(t, f.done, "the periodic refresh to finish"))
	}

	// The next timer is armed only once the round has finished both phases, so
	// waiting for it makes the worker's own state readable.
	select {
	case <-clock.armed:
	case <-time.After(5 * time.Second):
		t.Fatal("the scheduler did not arm a timer after the last round")
	}

	f.session.cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the scheduler did not exit after the session context was cancelled")
	}

	require.True(t, w.reconnectDeadline.IsZero(), "a session with nothing DOWN must keep no reconnect deadline")
	require.Zero(t, w.backoff, "a session with nothing DOWN must keep no backoff")
	require.Positive(t, filter.count(), "the eligibility scan is what establishes there is nothing to reconnect")
}

// TestHostScheduler_UnarmedRefreshRequestsNothing pins the phase's zero value as
// a defensive invariant: a refresh phase with no deadline never comes due, so it
// requests nothing and takes no part in choosing the rhythm.
//
// No session produces this state - the periodic refresh is armed at construction
// - but the guard is what stops a missing deadline from reading as "due now",
// which would request a refresh every round.
func TestHostScheduler_UnarmedRefreshRequestsNothing(t *testing.T) {
	const intv = 90 * time.Second

	f := newTickFixture(t, nil)
	clock := newFakeSchedulerClock()
	w := newTestScheduler(f.session, clock, intv)
	require.True(t, w.fullRefreshDeadline.IsZero(), "the fixture must start with the periodic refresh unarmed")

	// Driven synchronously: with the refresh unarmed and nothing DOWN, no phase
	// is armed at all, so the loop would park on cancellation and never wake.
	for round := range 3 {
		_, armed := w.nextWait(clock.now())
		require.False(t, armed, "round %d: an unarmed refresh must not schedule a wake-up on its own", round)

		clock.advance(intv)
		w.serve()
	}

	f.requireNoRefresh(t, 2*ringRefreshDebounceTime, "an unarmed periodic refresh on a healthy ring")
	require.True(t, w.fullRefreshDeadline.IsZero(), "an unarmed periodic refresh must stay unarmed")
	require.True(t, w.refreshRetryFloor.IsZero(), "an unarmed periodic refresh must not take a retry floor")
}

// TestHostScheduler_ReconnectRoundsDoNotPostponeTheSafetyNet pins rule two: the
// periodic refresh deadline moves only when that phase is served.
//
// This is the shape the default configuration has - a reconnect interval well
// under the refresh period - and it is the only one where a served reconnect
// round could push the refresh out. Recomputing the refresh deadline at the end
// of any round would postpone the safety net for ever on such a cluster, which
// is the same error as letting an unrelated event push a pending reconnect out.
func TestHostScheduler_ReconnectRoundsDoNotPostponeTheSafetyNet(t *testing.T) {
	const intv = time.Minute

	f := newTickFixture(t, nil)
	f.driveDown(t)

	clock := newFakeSchedulerClock()
	w := newSessionScheduler(f.session, clock, intv)
	armedAt := w.fullRefreshDeadline

	// Driven synchronously: the test owns the clock and reads the worker's own
	// state, so there is no second goroutine to race with. The phase is pinned at
	// its cap so every round is one interval apart, which is the shape where a
	// reconnect round could push the safety net out.
	pinAtBackoffCap(w, f.session)

	rounds := int(ringFullRefreshInterval / intv)
	require.Greater(t, rounds, 1, "the fixture must give several reconnect rounds inside one refresh period")

	for round := 1; round <= rounds; round++ {
		wait, armed := w.nextWait(clock.now())
		require.True(t, armed)
		require.Equal(t, intv, wait, "round %d must be scheduled by the reconnect phase", round)

		clock.advance(wait)
		w.serve()

		if round < rounds {
			require.Equal(t, armedAt, w.fullRefreshDeadline,
				"round %d served only the reconnect phase, so the refresh deadline must not move", round)
		}
	}

	require.Equal(t, armedAt.Add(ringFullRefreshInterval), w.fullRefreshDeadline,
		"the refresh must advance exactly one period, and only once it was served")
}

// TestHostScheduler_RunExitsOnContextCancel proves a scheduler parked on its
// timer leaves as soon as the session's context is cancelled.
func TestHostScheduler_RunExitsOnContextCancel(t *testing.T) {
	f := newTickFixture(t, nil)
	clock := newFakeSchedulerClock()
	w := newSessionScheduler(f.session, clock, time.Hour)

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.run()
	}()

	select {
	case <-clock.armed:
	case <-time.After(5 * time.Second):
		t.Fatal("the scheduler never armed its first timer")
	}

	f.session.cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the scheduler did not exit on cancellation")
	}
}

// TestHostScheduler_RunExitsWithNothingArmed covers the state the reconnect
// phase's ownership made reachable: with the refresh unarmed and no outage
// recorded, no phase is armed and the loop parks on a nil channel.
//
// A loop that treated "nothing armed" as a zero delay would spin instead, so the
// exit has to be proved from that state and not only from a parked timer.
func TestHostScheduler_RunExitsWithNothingArmed(t *testing.T) {
	f := newTickFixture(t, nil)
	clock := newFakeSchedulerClock()
	w := newTestScheduler(f.session, clock, time.Hour)

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.run()
	}()

	// Nothing may be armed, so the absence of a timer is the state under test.
	select {
	case <-clock.armed:
		t.Fatal("a scheduler with no armed phase must not arm a timer")
	case <-time.After(50 * time.Millisecond):
	}

	f.session.cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a scheduler with no armed phase did not exit on cancellation")
	}
}

// TestHostScheduler_ReconnectPanicKeepsRefreshAndRhythm pins I3 and I6 together
// on the phase that runs application code.
//
// A HostFilter that panics on every sweep must not stop the loop, must not stop
// the periodic ring refresh, and must not turn the loop into a spin: the failed
// phase consumes its interval exactly as a completed one does.
func TestHostScheduler_ReconnectPanicKeepsRefreshAndRhythm(t *testing.T) {
	// Both phases advance by the same period, so every round makes both due.
	const intv = ringFullRefreshInterval

	filter := &panickingHostFilter{}
	f := newTickFixture(t, func(cluster *ClusterConfig) {
		cluster.HostFilter = filter
	})
	f.driveDown(t)
	filter.arm()

	clock := newFakeSchedulerClock()
	w := newTestScheduler(f.session, clock, intv)
	// Arm the periodic refresh on the same rhythm so every round makes both
	// phases due, which is the case where one phase can starve the other.
	w.fullRefreshDeadline = clock.now().Add(intv)
	// The sweep panics inside the eligibility scan, before any reconciliation, so
	// the backoff is whatever the previous round left. Pinning it at the cap makes
	// the rhythm one interval per round and the assertion below readable; that a
	// panic from the zero state still leaves a positive delay is pinned by
	// TestHostScheduler_PanicBeforeReconcileStillAdvances.
	pinAtBackoffCap(w, f.session)
	// The conviction above left a nudge pending. Consuming it here keeps every
	// round in this test timer-driven: a nudge-woken round leaves its timer
	// unread, and the next release would then fire a timer nothing is waiting on.
	takeNudge(f.session)

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.run()
	}()

	const rounds = 3
	for round := range rounds {
		require.Equal(t, intv, clock.release(t, "the next round's timer"),
			"round %d must wait a full interval even though the sweep panicked", round)
		f.awaitEntered(t, "the periodic ring refresh to run despite the panicking sweep")
		require.Error(t, awaitRefreshDone(t, f.done, "the periodic refresh to finish"),
			"the fixture has no control connection, so the refresh must fail and be seen to")
	}

	// The refresh phase runs first, so seeing its request says nothing about the
	// sweep in the same round. The next timer is armed only once the round has
	// finished both phases, so waiting for it is the barrier that makes the sweep
	// count readable.
	select {
	case <-clock.armed:
	case <-time.After(5 * time.Second):
		t.Fatal("the scheduler did not arm a timer after the last round")
	}
	require.GreaterOrEqual(t, filter.count(), rounds,
		"every round must have reached the sweep, so the refreshes above were not simply the sweep's own trigger")

	f.session.cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the scheduler did not exit after the session context was cancelled")
	}
}

// TestHostScheduler_DeliveredRefreshAdvancesAFullInterval proves a served
// refresh moves its own deadline one full period out, clears any retry floor,
// and leaves the reconnect phase alone.
func TestHostScheduler_DeliveredRefreshAdvancesAFullInterval(t *testing.T) {
	f := newTickFixture(t, nil)
	clock := newFakeSchedulerClock()
	w := newTestScheduler(f.session, clock, time.Minute)

	now := clock.now()
	w.fullRefreshDeadline = now
	w.refreshRetryFloor = now
	w.reconnectDeadline = now.Add(time.Hour)

	w.serveRingRefresh(now)

	require.Equal(t, now.Add(ringFullRefreshInterval), w.fullRefreshDeadline,
		"a delivered request must move the periodic deadline one full period out")
	require.True(t, w.refreshRetryFloor.IsZero(), "a delivered request must clear the retry floor")
	require.Equal(t, now.Add(time.Hour), w.reconnectDeadline, "the refresh phase must not touch the reconnect deadline")

	f.awaitEntered(t, "the refresh the phase requested")
}

// TestHostScheduler_RefreshFailureFloorIsPhaseScoped pins the other half of I3
// and I6: a refresh that fails to deliver keeps its deadline and takes a retry
// floor, and that floor must hold back only the refresh phase - a reconnect
// that comes due inside the floor is still served on time.
func TestHostScheduler_RefreshFailureFloorIsPhaseScoped(t *testing.T) {
	f := newTickFixture(t, nil)
	clock := newFakeSchedulerClock()
	w := newTestScheduler(f.session, clock, time.Minute)

	// A nil refresher makes the delivery panic, which is the failure the phase
	// boundary exists to absorb.
	refresher := f.session.ringRefresher
	f.session.ringRefresher = nil

	now := clock.now()
	w.fullRefreshDeadline = now
	w.serveRingRefresh(now)

	require.Equal(t, now, w.fullRefreshDeadline, "a request that was not delivered must not consume the deadline")
	require.Equal(t, now.Add(ringRefreshRetryDelay), w.refreshRetryFloor, "a failed delivery must take a retry floor")

	// The refresher is restored before the round below: what must hold the phase
	// back there is its floor, not a delivery that would fail anyway.
	f.session.ringRefresher = refresher
	// The reconnect phase needs something to serve, pinned at its cap so the
	// round's own rhythm is the configured interval.
	f.driveDown(t)
	pinAtBackoffCap(w, f.session)

	// The reconnect phase comes due well inside the refresh's floor.
	w.reconnectDeadline = now.Add(ringRefreshRetryDelay / 4)
	wait, armed := w.nextWait(now)
	require.True(t, armed)
	require.Equal(t, ringRefreshRetryDelay/4, wait, "the floor must not delay a reconnect that is due sooner")

	clock.advance(ringRefreshRetryDelay / 4)
	w.serve()
	require.Equal(t, clock.now().Add(time.Minute), w.reconnectDeadline,
		"the reconnect phase must have run on time inside the refresh's floor")
	require.Equal(t, now.Add(ringRefreshRetryDelay), w.refreshRetryFloor,
		"the floored refresh phase must not have run, so its floor is unchanged")
}

// TestHostScheduler_SafetyNetIsIndependentOfReconnectInterval: the periodic ring
// refresh is armed at construction on its own fixed period. A long reconnect
// interval must not stretch it, because it is a safety net for lost topology
// events and not a reconnection setting.
func TestHostScheduler_SafetyNetIsIndependentOfReconnectInterval(t *testing.T) {
	const intv = 30 * time.Minute

	f := newTickFixture(t, nil)
	clock := newFakeSchedulerClock()
	w := newSessionScheduler(f.session, clock, intv)

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.run()
	}()

	require.Equal(t, ringFullRefreshInterval, clock.release(t, "the first round's timer"),
		"the safety net, not the 30-minute reconnect interval, must decide the first wake-up")
	f.awaitEntered(t, "the periodic refresh")
	require.Error(t, awaitRefreshDone(t, f.done, "the periodic refresh to finish"))

	require.Equal(t, ringFullRefreshInterval, clock.release(t, "the second round's timer"),
		"the safety net must re-arm on its own period")

	f.session.cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the scheduler did not exit after the session context was cancelled")
	}
}

// TestHostScheduler_ConstructionArmsOnlyWhatIsEnabled pins what a session hands
// the scheduler: the periodic refresh always, the reconnect phase only when
// ReconnectInterval is positive.
func TestHostScheduler_ConstructionArmsOnlyWhatIsEnabled(t *testing.T) {
	f := newTickFixture(t, nil)

	// The period is the safety net's own, whatever the reconnect interval is:
	// a long interval must not stretch it and a zero interval must not remove it.
	for _, intv := range []time.Duration{time.Minute, 30 * time.Minute, 0} {
		before := time.Now()
		w := f.session.newHostScheduler(intv)
		after := time.Now()

		require.Equal(t, intv > 0, w.reconnectEnabled(), "reconnect interval %s", intv)
		require.True(t, w.reconnectDeadline.IsZero(), "run arms the reconnect phase, not the constructor")
		require.WithinRange(t, w.fullRefreshDeadline,
			before.Add(ringFullRefreshInterval), after.Add(ringFullRefreshInterval),
			"the safety net must be armed one of its own periods out, with a reconnect interval of %s", intv)
	}
}

// TestHostScheduler_SessionAlwaysStartsTheScheduler: the safety net is not
// something ReconnectInterval turns off, so the worker runs even at zero - which
// is the case where it used not to exist at all.
func TestHostScheduler_SessionAlwaysStartsTheScheduler(t *testing.T) {
	started := make(chan time.Duration, 1)
	f := newTickFixture(t, func(cluster *ClusterConfig) {
		cluster.ReconnectInterval = 0
		cluster.testSchedulerStarted = func(intv time.Duration) {
			select {
			case started <- intv:
			default:
			}
		}
	})
	require.NotNil(t, f.session)

	select {
	case intv := <-started:
		require.Zero(t, intv, "the scheduler must have been started with the configured zero interval")
	case <-time.After(5 * time.Second):
		t.Fatal("the session did not start the host scheduler with ReconnectInterval at zero")
	}
}

// TestHostScheduler_ZeroIntervalHasNoReconnectPhase pins I2.
//
// A zero ReconnectInterval must mean the phase does not exist, not that it is
// produced and then declined. A deadline that is due and never served is a
// wake-up with no delay: the loop would spin, and every round would call the
// application's HostFilter. The assertion is therefore on the wake-ups and the
// filter calls, not only on the absence of a reconnect.
func TestHostScheduler_ZeroIntervalHasNoReconnectPhase(t *testing.T) {
	filter := &countingHostFilter{}
	f := newTickFixture(t, func(cluster *ClusterConfig) {
		cluster.HostFilter = filter
	})
	f.driveDown(t)
	before := filter.count()

	clock := newFakeSchedulerClock()
	w := newSessionScheduler(f.session, clock, 0)

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.run()
	}()

	for round := range 3 {
		require.Equal(t, ringFullRefreshInterval, clock.release(t, "the next round's timer"),
			"round %d must be scheduled by the safety net alone, on a strictly positive delay", round)
		f.awaitEntered(t, "the periodic refresh")
		require.Error(t, awaitRefreshDone(t, f.done, "the periodic refresh to finish"))
	}

	f.session.cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a scheduler with no reconnect phase did not exit on cancellation")
	}

	require.True(t, w.reconnectDeadline.IsZero(), "no reconnect deadline may ever be produced")
	require.Equal(t, before, filter.count(),
		"a scheduler with no reconnect phase must never consult the HostFilter, even with a host down")
	_, pooled := f.session.pool.getPoolFor(f.host)
	require.False(t, pooled, "no scheduled admission may happen with reconnects disabled")
}

// countingHostFilter accepts every host and counts the calls, so a test can
// assert a path never consults the application's filter.
type countingHostFilter struct {
	calls atomic.Int64
}

var _ HostFilter = (*countingHostFilter)(nil)

// Accept counts the call and accepts.
//
// Returns:
//   - bool: always true
func (f *countingHostFilter) Accept(*HostInfo) bool {
	f.calls.Add(1)
	return true
}

// count returns how many times the filter was consulted.
//
// Returns:
//   - int64: the call count
func (f *countingHostFilter) count() int64 { return f.calls.Load() }

// TestRefreshRing_LogsChangesNotTheWholeRing pins the 5e log fix.
//
// The closing line used to dump every host in the ring at Info on every refresh.
// Once the periodic safety net runs for ever that becomes one multi-kilobyte
// line per period on a cluster that is not changing at all. A round that changed
// nothing must say so at Debug; a round that changed something must report the
// change, not the whole ring.
func TestRefreshRing_LogsChangesNotTheWholeRing(t *testing.T) {
	const peerHostID = "dddddddd-0000-4000-8000-00000000cafe"

	logger := &historyLogger{}
	script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
		cluster.Logger = logger
	})

	t.Run("an unchanged ring says nothing at Info", func(t *testing.T) {
		require.NoError(t, session.refreshRing(), "settle the ring")
		before := len(logger.withMessage("Refreshed ring."))
		infoBefore := countRefreshedRingAtInfo(logger)

		require.NoError(t, session.refreshRing(), "a second refresh that changes nothing")

		records := logger.withMessage("Refreshed ring.")
		require.Greater(t, len(records), before, "the refresh must still be logged")
		require.Equal(t, infoBefore, countRefreshedRingAtInfo(logger),
			"a round that changed nothing must not write an Info line")
		require.Equal(t, LogLevelDebug, records[len(records)-1].level)
	})

	t.Run("a changed ring reports the change", func(t *testing.T) {
		infoBefore := countRefreshedRingAtInfo(logger)
		script.setPeers([]peerRow{newPeerRow(peerHostID, "127.0.0.2")})

		require.NoError(t, session.refreshRing(), "adopt the peer")

		records := logger.withMessage("Refreshed ring.")
		last := records[len(records)-1]
		require.Equal(t, LogLevelInfo, last.level, "a round that changed the ring must be logged at Info")
		require.Equal(t, infoBefore+1, countRefreshedRingAtInfo(logger), "exactly one Info line for the change")

		changes := last.fieldString("changes")
		require.Contains(t, changes, peerHostID, "the line must name what changed")
		require.Contains(t, changes, "added", "the line must say what kind of change it was")
		require.Empty(t, last.fieldString("ring"), "the line must not carry a dump of the whole ring")

		hosts := session.ring.allHosts()
		require.Len(t, hosts, 2, "the fixture must now hold two hosts, so a whole-ring dump would be longer than the change")
		for _, h := range hosts {
			if h.HostID() != peerHostID {
				require.NotContains(t, changes, h.HostID(), "an unchanged host must not appear in the line")
			}
		}
	})
}

// countRefreshedRingAtInfo counts the closing refresh lines written at Info.
//
// Returns:
//   - int: how many "Refreshed ring." records are Info level
func countRefreshedRingAtInfo(logger *historyLogger) int {
	n := 0
	for _, r := range logger.withMessage("Refreshed ring.") {
		if r.level == LogLevelInfo {
			n++
		}
	}
	return n
}

// TestZeroReconnectInterval_StillAdmitsANewlyDiscoveredHost pins the other half
// of what zero means, so an implementation cannot satisfy
// TestHostScheduler_ZeroIntervalHasNoReconnectPhase by suppressing every dial.
//
// Zero disables scheduled retries for DOWN hosts the driver already knows about
// and whose description has not changed. It does not disable connecting: a host
// the metadata refresh has just discovered is admitted and dialled as usual.
func TestZeroReconnectInterval_StillAdmitsANewlyDiscoveredHost(t *testing.T) {
	const peerHostID = "dddddddd-0000-4000-8000-0000000000aa"

	script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
		cluster.ReconnectInterval = 0
	})

	_, pooled := session.pool.getPoolByHostID(peerHostID)
	require.False(t, pooled, "the peer must not be known before it is described")

	script.setPeers([]peerRow{newPeerRow(peerHostID, "127.0.0.2")})
	require.NoError(t, session.refreshRing(), "adopt the peer")

	_, ok := session.ring.getHost(peerHostID)
	require.True(t, ok, "the peer must be in the ring")
	_, pooled = session.pool.getPoolByHostID(peerHostID)
	require.True(t, pooled, "a newly discovered host must still be admitted with reconnects disabled")
}
