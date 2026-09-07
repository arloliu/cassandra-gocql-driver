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

// TestHostScheduler_ReconnectKeepsItsInterval proves the deadline loop replaced
// the ticker without changing the rhythm: with only the reconnect phase armed
// the scheduler waits exactly one interval before every round.
func TestHostScheduler_ReconnectKeepsItsInterval(t *testing.T) {
	const intv = 90 * time.Second

	f := newTickFixture(t, nil)
	clock := newFakeSchedulerClock()
	w := newTestScheduler(f.session, clock, intv)

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.run()
	}()

	for round := range 3 {
		require.Equal(t, intv, clock.release(t, "the reconnect phase to arm its timer"),
			"round %d must wait exactly one reconnect interval", round)
	}

	f.session.cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the scheduler did not exit after the session context was cancelled")
	}
}

// TestHostScheduler_UnarmedRefreshRequestsNothing pins the phase's zero value:
// until the periodic refresh is armed it must never come due, so a round on a
// healthy ring requests no refresh at all and the reconnect phase alone decides
// the rhythm.
func TestHostScheduler_UnarmedRefreshRequestsNothing(t *testing.T) {
	const intv = 90 * time.Second

	f := newTickFixture(t, nil)
	clock := newFakeSchedulerClock()
	w := newTestScheduler(f.session, clock, intv)
	require.True(t, w.fullRefreshDeadline.IsZero(), "the fixture must start with the periodic refresh unarmed")

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.run()
	}()

	for round := range 3 {
		require.Equal(t, intv, clock.release(t, "the next round's timer"),
			"round %d must be scheduled by the reconnect phase alone", round)
	}

	f.session.cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the scheduler did not exit after the session context was cancelled")
	}

	f.requireNoRefresh(t, 2*ringRefreshDebounceTime, "an unarmed periodic refresh on a healthy ring")
	require.True(t, w.fullRefreshDeadline.IsZero(), "an unarmed periodic refresh must stay unarmed")
	require.True(t, w.refreshRetryFloor.IsZero(), "an unarmed periodic refresh must not take a retry floor")
}

// TestHostScheduler_RunExitsOnContextCancel proves a scheduler parked on its
// timer leaves as soon as the session's context is cancelled.
func TestHostScheduler_RunExitsOnContextCancel(t *testing.T) {
	f := newTickFixture(t, nil)
	clock := newFakeSchedulerClock()
	w := newTestScheduler(f.session, clock, time.Hour)

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
	// boundary exists to absorb; the fixture host is UP, so the reconnect sweep
	// never reaches the refresher itself.
	refresher := f.session.ringRefresher
	f.session.ringRefresher = nil
	t.Cleanup(func() { f.session.ringRefresher = refresher })

	now := clock.now()
	w.fullRefreshDeadline = now
	w.serveRingRefresh(now)

	require.Equal(t, now, w.fullRefreshDeadline, "a request that was not delivered must not consume the deadline")
	require.Equal(t, now.Add(ringRefreshRetryDelay), w.refreshRetryFloor, "a failed delivery must take a retry floor")

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
