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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// schemaRetryBudget bounds every real-time wait in the schema-retry phase tests.
const schemaRetryBudget = 5 * time.Second

// schemaRetryFixture drives the host scheduler's schema-retry phase on a fake clock over a bare session.
//
// No NewSession runs, so no second, wall-clock scheduler serves the same debt.
// The scheduler's schema requests are counted instead of delivered,
// so every assertion is about the requests the phase made, not about rounds that ran.
// The debt is written through begin and finish, the recorder methods runSchemaRefresh calls.
type schemaRetryFixture struct {
	session  *Session
	clock    *fakeSchedulerClock
	w        *hostScheduler
	requests atomic.Int32
	// ringRuns receives once per ring refresh the scheduler delivered.
	ringRuns chan struct{}
}

// newSchemaRetryFixture builds the fixture.
//
// Parameters:
//   - t: the test, which cancels the session context and stops the ring refresher when it ends
//   - intv: the scheduler's reconnect interval
//   - logger: the session's logger
//
// Returns:
//   - *schemaRetryFixture: a scheduler with no phase armed over a session that owes nothing
func newSchemaRetryFixture(t *testing.T, intv time.Duration, logger StructuredLogger) *schemaRetryFixture {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	f := &schemaRetryFixture{clock: newFakeSchedulerClock(), ringRuns: make(chan struct{}, 64)}
	f.session = &Session{logger: logger, ctx: ctx, cancel: cancel}
	f.session.schemaDebt.nudge = make(chan struct{}, 1)
	f.session.ringRefresher = newRefreshDebouncer(time.Hour, func() error {
		f.ringRuns <- struct{}{}
		return nil
	}, logger)
	t.Cleanup(f.session.ringRefresher.stop)

	f.w = newTestScheduler(f.session, f.clock, intv)
	f.w.requestSchemaRefresh = func() { f.requests.Add(1) }
	return f
}

// fail records one failed round, as runSchemaRefresh does.
func (f *schemaRetryFixture) fail() {
	f.session.schemaDebt.begin()
	f.session.schemaDebt.finish(true)
}

// succeed records one successful round, as runSchemaRefresh does.
func (f *schemaRetryFixture) succeed() {
	f.session.schemaDebt.begin()
	f.session.schemaDebt.finish(false)
}

// serveAfter advances the fake clock and serves one round of every phase.
//
// Parameters:
//   - d: how far to move the clock first
func (f *schemaRetryFixture) serveAfter(d time.Duration) {
	f.clock.advance(d)
	f.w.serve()
}

// step returns the delay the schema phase's deadline stands at, or zero when it has none.
//
// Returns:
//   - time.Duration: the armed deadline minus the clock's current time
func (f *schemaRetryFixture) step() time.Duration {
	if f.w.schemaDeadline.IsZero() {
		return 0
	}
	return f.w.schemaDeadline.Sub(f.clock.now())
}

// failAndServeStep records a failure, serves it, and then serves the retry it armed once that is due.
//
// Parameters:
//   - t: the test
//
// Returns:
//   - time.Duration: the step the failure armed
func (f *schemaRetryFixture) failAndServeStep(t *testing.T) time.Duration {
	t.Helper()

	f.fail()
	f.serveAfter(0)
	step := f.step()
	before := f.requests.Load()
	f.serveAfter(step)
	require.Equal(t, before+1, f.requests.Load(), "the %v step, once due, must request one round", step)
	return step
}

// awaitArmed waits for the running loop to arm its next timer, without firing it.
//
// Parameters:
//   - t: the test
//   - what: what the test is waiting for, for the failure message
//
// Returns:
//   - *armedTimer: the timer the loop armed
func (f *schemaRetryFixture) awaitArmed(t *testing.T, what string) *armedTimer {
	t.Helper()
	select {
	case timer := <-f.clock.armed:
		return timer
	case <-time.After(schemaRetryBudget):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

// fire advances the fake clock by a timer's delay and fires it.
//
// Parameters:
//   - timer: a timer awaitArmed returned
func (f *schemaRetryFixture) fire(timer *armedTimer) {
	f.clock.advance(timer.delay)
	timer.fired <- f.clock.now()
}

// runLoop runs the scheduler's loop until the test ends.
//
// Parameters:
//   - t: the test, which cancels the loop and waits for it to exit when it ends
func (f *schemaRetryFixture) runLoop(t *testing.T) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.w.run()
	}()
	t.Cleanup(func() {
		f.session.cancel()
		select {
		case <-done:
		case <-time.After(schemaRetryBudget):
			t.Error("the scheduler did not exit after the session context was cancelled")
		}
	})
}

// TestSchemaRetry_NothingOwedArmsNothing pins P1: with no debt the phase takes no part in the loop.
func TestSchemaRetry_NothingOwedArmsNothing(t *testing.T) {
	f := newSchemaRetryFixture(t, 0, &defaultLogger{})
	f.w.fullRefreshDeadline = f.clock.now().Add(ringFullRefreshInterval)

	f.serveAfter(0)

	require.True(t, f.w.schemaDeadline.IsZero(), "schema deadline %v with nothing owed, want none", f.w.schemaDeadline)
	require.EqualValues(t, 0, f.requests.Load(), "requests %d with nothing owed, want 0", f.requests.Load())
	wait, armed := f.w.nextWait(f.clock.now())
	require.True(t, armed)
	require.Equal(t, ringFullRefreshInterval, wait, "the other phases alone must decide the next wake-up")
}

// TestSchemaRetry_FirstFailureArmsOneBaseStep pins P2: the first failure arms one base step and requests nothing yet;
// once due it requests exactly one round, and no later wake-up requests another before that round's outcome is recorded.
func TestSchemaRetry_FirstFailureArmsOneBaseStep(t *testing.T) {
	f := newSchemaRetryFixture(t, 0, &defaultLogger{})

	f.fail()
	f.serveAfter(0)
	require.EqualValues(t, 0, f.requests.Load(), "requests %d before the step is due, want 0", f.requests.Load())
	if !f.w.schemaDeadline.IsZero() {
		require.Equal(t, time.Second, f.step(), "the first failure must arm one base step")
	}

	f.serveAfter(time.Second)
	require.EqualValues(t, 1, f.requests.Load(), "requests %d, want 1", f.requests.Load())
	require.True(t, f.w.schemaDeadline.IsZero(), "schema deadline %v after the request, want none", f.w.schemaDeadline)

	// The requested round has not entered, so nothing has been recorded: the phase waits for an outcome.
	for range 3 {
		f.serveAfter(5 * time.Second)
	}
	require.EqualValues(t, 1, f.requests.Load(), "requests %d before the requested round's outcome, want 1", f.requests.Load())
	require.True(t, f.w.schemaDeadline.IsZero(), "schema deadline %v before the requested round's outcome, want none", f.w.schemaDeadline)
}

// TestSchemaRetry_RampDoublesToTheCap pins P3: failure after failure the step doubles up to ReconnectInterval.
// The interval is not a power-of-two multiple of a second, so the cap is reached by the comparison, not by a doubling.
func TestSchemaRetry_RampDoublesToTheCap(t *testing.T) {
	const intv = 45 * time.Second
	f := newSchemaRetryFixture(t, intv, &defaultLogger{})

	ordinals := []string{"first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eighth"}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, 32 * time.Second, intv, intv}
	for i, w := range want {
		got := f.failAndServeStep(t)
		require.Equal(t, w, got, "%s step %v, want %v", ordinals[i], got, w)
	}
}

// TestSchemaRetry_ObservedDischargeResets pins P4: a success the phase observes clears the deadline and the backoff,
// and a later failure starts again at the base.
func TestSchemaRetry_ObservedDischargeResets(t *testing.T) {
	f := newSchemaRetryFixture(t, 0, &defaultLogger{})
	f.failAndServeStep(t)
	f.fail()
	f.serveAfter(0)
	require.Equal(t, 2*time.Second, f.step(), "the second failure must arm the second step")

	// The discharge is recorded while that deadline is armed and not yet due.
	f.succeed()
	f.serveAfter(0)
	require.True(t, f.w.schemaDeadline.IsZero(), "deadline not cleared after the discharge")
	require.Zero(t, f.w.schemaBackoff, "backoff %v not cleared after the discharge", f.w.schemaBackoff)

	f.fail()
	f.serveAfter(0)
	require.Equal(t, time.Second, f.step(), "first step after an observed discharge %v, want 1s", f.step())
}

// TestSchemaRetry_UnobservedDischargeResets pins P4b: a discharge the phase never saw still resets the step,
// because okGen moved even though every snapshot the phase took read owed.
func TestSchemaRetry_UnobservedDischargeResets(t *testing.T) {
	f := newSchemaRetryFixture(t, 0, &defaultLogger{})
	for range 5 {
		f.failAndServeStep(t)
	}
	f.fail()
	f.serveAfter(0)
	require.Equal(t, 32*time.Second, f.step(), "the ramp must stand at 32s")

	f.succeed()
	f.fail()
	f.serveAfter(0)
	require.Equal(t, time.Second, f.step(), "first step after a discharge %v, want 1s", f.step())
}

// TestSchemaRetry_NoRequestAgainstARunningRound pins P5 (S7): a due deadline requests nothing while a round runs,
// and the running round's recorded failure re-arms from the next step.
func TestSchemaRetry_NoRequestAgainstARunningRound(t *testing.T) {
	f := newSchemaRetryFixture(t, 0, &defaultLogger{})
	f.fail()
	f.serveAfter(0)

	f.session.schemaDebt.begin()
	f.serveAfter(time.Second)
	require.EqualValues(t, 0, f.requests.Load(), "requests %d, want 0 while a round is running", f.requests.Load())
	require.True(t, f.w.schemaDeadline.IsZero(), "schema deadline %v while a round is running, want none", f.w.schemaDeadline)

	f.session.schemaDebt.finish(true)
	f.serveAfter(0)
	require.Equal(t, 2*time.Second, f.step(), "the running round's failure must re-arm from the next step")
}

// TestSchemaRetry_ExistsWithoutReconnectInterval pins P6 (S3):
// with ReconnectInterval zero or negative the phase still exists, its first step is one second and its cap sixty,
// while the reconnect phase stays absent and never consults the HostFilter.
func TestSchemaRetry_ExistsWithoutReconnectInterval(t *testing.T) {
	for _, intv := range []time.Duration{0, -time.Second} {
		t.Run(intv.String(), func(t *testing.T) {
			f := newSchemaRetryFixture(t, intv, &defaultLogger{})
			filter := &panickingHostFilter{}
			filter.arm()
			f.session.cfg.HostFilter = filter

			first := f.failAndServeStep(t)
			require.Equal(t, time.Second, first, "first step %v, want 1s", first)
			var last time.Duration
			for range 7 {
				last = f.failAndServeStep(t)
				require.True(t, f.w.reconnectDeadline.IsZero(), "the reconnect phase must arm no deadline")
			}
			require.Equal(t, schemaRetryFallbackCap, last, "cap %v, want %v", last, schemaRetryFallbackCap)
			require.Zero(t, filter.count(), "the HostFilter must never be consulted")
		})
	}
}

// TestSchemaRetry_PanicLeavesAPositiveDelay pins P7 (S4, S6):
// a phase that panics before it has decided leaves a strictly positive delay, requests nothing,
// does not stop the ring refresh due in the same round,
// and does not stop the loop.
func TestSchemaRetry_PanicLeavesAPositiveDelay(t *testing.T) {
	logger := &panicOnDebugLogger{StructuredLogger: &defaultLogger{}}
	f := newSchemaRetryFixture(t, 0, logger)
	f.w.fullRefreshDeadline = f.clock.now().Add(time.Second)
	f.fail()
	f.serveAfter(0)
	require.Equal(t, time.Second, f.step(), "the failure must arm one base step, due with the ring refresh")
	// The failure left a nudge pending.
	// Consuming it keeps every round below timer driven:
	// a nudge-woken round would leave the timer the test is about to fire unread.
	select {
	case <-f.session.schemaDebt.nudge:
	default:
		t.Fatal("the failure must nudge the scheduler")
	}

	logger.armed.Store(true)
	f.runLoop(t)
	due := f.awaitArmed(t, "the round in which both phases are due")
	require.Equal(t, time.Second, due.delay)
	f.fire(due)

	next := f.awaitArmed(t, "the timer after the phase panicked")
	require.False(t, logger.armed.Load(), "the phase's Debug must have been reached and panicked")
	require.Greater(t, next.delay, time.Duration(0), "schema delay %v after the phase panicked, want > 0", next.delay)
	require.EqualValues(t, 0, f.requests.Load(), "requests %d after the phase panicked, want 0", f.requests.Load())
	select {
	case <-f.ringRuns:
	case <-time.After(schemaRetryBudget):
		t.Fatal("the ring refresh due in the same round was not delivered")
	}

	// The loop keeps running, and the retry the panic postponed is requested on its next round.
	f.fire(next)
	f.awaitArmed(t, "the timer after the postponed retry")
	require.EqualValues(t, 1, f.requests.Load(), "requests %d after the postponed retry, want 1", f.requests.Load())
}

// TestSchemaRetry_NudgeWakesTheLoop pins P8:
// a failure recorded while the loop sleeps towards the ring deadline wakes it,
// with ReconnectInterval zero so the outage nudge is not selected.
// The five-minute timer is never fired: a loop with no nudge case would otherwise see the failure anyway.
func TestSchemaRetry_NudgeWakesTheLoop(t *testing.T) {
	f := newSchemaRetryFixture(t, 0, &defaultLogger{})
	f.w.fullRefreshDeadline = f.clock.now().Add(ringFullRefreshInterval)
	f.runLoop(t)

	parked := f.awaitArmed(t, "the ring deadline's timer")
	require.Equal(t, ringFullRefreshInterval, parked.delay)

	f.fail()
	retry := f.awaitArmed(t, "the retry timer")
	require.Equal(t, time.Second, retry.delay, "the nudged round must arm one base step")
}

// TestRefreshDebouncer_TriggerAndPendingDebounceRunAsOneRound pins P9:
// a retry and a pending debounced request outstanding when the flusher wakes run as one round,
// and the debounce timer is stopped by that round.
//
// It characterises the flusher's existing drain, which the schema retry relies on; it is not a regression test.
func TestRefreshDebouncer_TriggerAndPendingDebounceRunAsOneRound(t *testing.T) {
	var runs atomic.Int32
	entered := make(chan struct{}, 2)
	gate := make(chan struct{})
	d := newRefreshDebouncer(time.Hour, func() error {
		runs.Add(1)
		entered <- struct{}{}
		<-gate
		return nil
	}, &defaultLogger{})
	t.Cleanup(d.stop)

	d.debounce()
	d.trigger()
	select {
	case <-entered:
	case <-time.After(schemaRetryBudget):
		t.Fatal("the triggered round never ran")
	}
	// The flusher drained the debounce timer before it ran the round, so stopping it now finds it stopped.
	stoppedNow := d.timer.Stop()
	close(gate)
	require.False(t, stoppedNow, "the pending debounce was still armed after the round began")

	// A round requested afterwards is the second execution, so nothing was left queued behind the first.
	select {
	case err := <-d.refreshNow():
		require.NoError(t, err)
	case <-time.After(schemaRetryBudget):
		t.Fatal("the follow-up round never resolved")
	}
	require.EqualValues(t, 2, runs.Load(), "executions %d, want 2: the trigger and the debounce ran as one round", runs.Load())
}

// panicOnDebugLogger panics once on the first Debug after it is armed.
type panicOnDebugLogger struct {
	StructuredLogger
	armed atomic.Bool
}

// Debug panics once when armed, and forwards otherwise.
func (l *panicOnDebugLogger) Debug(msg string, fields ...LogField) {
	if l.armed.CompareAndSwap(true, false) {
		panic("scripted logger panic in Debug")
	}
	l.StructuredLogger.Debug(msg, fields...)
}
