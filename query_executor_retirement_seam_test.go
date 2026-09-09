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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// seamFixture is the four-runner shape the coordinator seam cases share.
//
// Two hosts and four runners give one of each classification without any further
// arrangement: the first two runners draw the two hosts and retire with an attempt behind
// them, and the last two find the enumeration spent and retire without one.
type seamFixture struct {
	*retirementFixture
	// attempted are the two hosts the first two runners draw, in order.
	attempted []*HostInfo
}

// newSeamFixture starts the shared shape with the first host failing and the second
// succeeding.
//
// Parameters:
//   - t: the test
//   - park: the 1-based runRetired arrivals to hold
//
// Returns:
//   - *seamFixture: the fixture, with both hosts armed
func newSeamFixture(t *testing.T, park ...int) *seamFixture {
	t.Helper()

	// Only the first host fails, so the second runner's result is the decisive outcome
	// every case but the cancellation one ends on.
	return newSeamFixtureWith(t, []retirementFailure{{host: 0, warnings: []string{"w-first"}}}, park...)
}

// newSeamFixtureWith is newSeamFixture with the failing hosts named.
//
// Parameters:
//   - t: the test
//   - failures: the hosts that answer with an error
//   - park: the 1-based runRetired arrivals to hold
//
// Returns:
//   - *seamFixture: the fixture, with both hosts armed
func newSeamFixtureWith(t *testing.T, failures []retirementFailure, park ...int) *seamFixture {
	t.Helper()

	fixture := newRetirementFixture(t, retirementOpts{
		hosts:    2,
		holdsAt:  runRetired,
		park:     park,
		failures: failures,
	})
	first, second := fixture.harness.hosts[0], fixture.harness.hosts[1]
	fixture.pinHosts(first, second)
	fixture.armAll()
	return &seamFixture{retirementFixture: fixture, attempted: []*HostInfo{first, second}}
}

// startAll starts the two attempting runners and waits for the other two to be launched.
//
// The last two are left parked at runEntered, which is what keeps them outstanding until
// the test wants them.
func (f *seamFixture) startAll(t *testing.T) {
	t.Helper()

	f.startAt(t, f.attempted[0], "the first runner")
	f.startAt(t, f.attempted[1], "the second runner")
	f.stages.await(t, runEntered, "the third runner to start")
	f.stages.await(t, runEntered, "the fourth runner to start")
}

// failFirst releases the failing host's response and waits for its retirement to be
// counted.
func (f *seamFixture) failFirst(t *testing.T, consumed int) {
	t.Helper()

	f.gate.release(hostIP(f.attempted[0]))
	f.consumes.await(t, consumed, "the first runner's retirement")
}

// retireUnattempted lets one parked runner out and waits for its retirement to be counted.
func (f *seamFixture) retireUnattempted(t *testing.T, consumed int, what string) {
	t.Helper()

	f.entered <- struct{}{}
	f.consumes.await(t, consumed, what)
}

// TestSpeculative_SeamPanicsDoNotOrphanIterators proves a panic out of any coordinator or
// drain seam leaves the query answered and every iterator reclaimed.
//
// The seams are test-only, so this is not about production behaviour reaching them: it is
// about the isolation those calls are wrapped in. coordinate is a synchronous call from
// executeQuery with no recover of its own, so an unwinding seam would take the caller's
// goroutine with it, and a seam that unwound past its Close would leak the framer the
// close was there to give back.
func TestSpeculative_SeamPanicsDoNotOrphanIterators(t *testing.T) {
	// The consume seam runs on coordinate's goroutine after the count and before the
	// handling, once per message, on either branch.
	t.Run("consume seam on the retired branch", func(t *testing.T) {
		fixture := newSeamFixture(t)
		fixture.panicOnConsume(1)

		result := iterAsync(fixture.query(t, &SimpleRetryPolicy{NumRetries: 5}, 3))
		fixture.startAll(t)

		fixture.failFirst(t, 1)
		fixture.gate.release(hostIP(fixture.attempted[1]))

		won := awaitIter(t, result, "the query to return despite the seam panic")
		require.NoError(t, won.err, "the second runner's attempt succeeded")
		require.Same(t, fixture.attempted[1], won.Host(), "the caller gets the decisive result")
		require.NoError(t, won.Close(), "the winning iterator is the caller's")

		// The retirement was still held and closed, and the two parked runners are the
		// drain's.
		require.Equal(t, 1, fixture.retired.count(), "the held retirement was closed")
		fixture.retired.requireClosed(t)
		fixture.drainRemaining(t, 2)
	})

	t.Run("consume seam on the results branch", func(t *testing.T) {
		fixture := newSeamFixture(t)
		// The second message is the winner's, and by then a retirement is held.
		fixture.panicOnConsume(2)

		result := iterAsync(fixture.query(t, &SimpleRetryPolicy{NumRetries: 5}, 3))
		fixture.startAll(t)

		fixture.failFirst(t, 1)
		fixture.gate.release(hostIP(fixture.attempted[1]))

		won := awaitIter(t, result, "the query to return despite the seam panic")
		require.Same(t, fixture.attempted[1], won.Host(), "the caller gets the decisive result")
		require.NoError(t, won.Close(), "the winning iterator is the caller's")

		require.Equal(t, 1, fixture.retired.count(), "the held retirement was closed on the way out")
		fixture.retired.requireClosed(t)
		fixture.drainRemaining(t, 2)
	})

	// The close seam runs once per coordinator-side close, and the close must happen
	// whether or not it panicked.
	t.Run("close seam replacing the held candidate", func(t *testing.T) {
		fixture := newSeamFixture(t)
		fixture.retired.panicOn(1)

		result := iterAsync(fixture.query(t, &SimpleRetryPolicy{NumRetries: 5}, 3))
		fixture.startAll(t)

		// An unattempted candidate first, then the attempted one that replaces it.
		fixture.retireUnattempted(t, 1, "the third runner's unattempted retirement")
		fixture.failFirst(t, 2)
		fixture.gate.release(hostIP(fixture.attempted[1]))

		won := awaitIter(t, result, "the query to return despite the seam panic")
		require.Same(t, fixture.attempted[1], won.Host(), "the caller gets the decisive result")
		require.NoError(t, won.Close(), "the winning iterator is the caller's")

		require.Equal(t, 2, fixture.retired.count(), "the displaced candidate and the held one were closed")
		fixture.retired.requireClosed(t)
		fixture.drainRemaining(t, 1)
	})

	t.Run("close seam discarding the arriving candidate", func(t *testing.T) {
		fixture := newSeamFixture(t)
		fixture.retired.panicOn(1)

		result := iterAsync(fixture.query(t, &SimpleRetryPolicy{NumRetries: 5}, 3))
		fixture.startAll(t)

		// The attempted candidate first, so the unattempted one that follows loses.
		fixture.failFirst(t, 1)
		fixture.retireUnattempted(t, 2, "the third runner's unattempted retirement")
		fixture.gate.release(hostIP(fixture.attempted[1]))

		won := awaitIter(t, result, "the query to return despite the seam panic")
		require.Same(t, fixture.attempted[1], won.Host(), "the caller gets the decisive result")
		require.NoError(t, won.Close(), "the winning iterator is the caller's")

		require.Equal(t, 2, fixture.retired.count(), "the discarded candidate and the held one were closed")
		fixture.retired.requireClosed(t)
		fixture.drainRemaining(t, 1)
	})

	t.Run("close seam in the deferred cleanup", func(t *testing.T) {
		fixture := newSeamFixture(t)
		fixture.retired.panicOn(1)

		result := iterAsync(fixture.query(t, &SimpleRetryPolicy{NumRetries: 5}, 3))
		fixture.startAll(t)

		fixture.failFirst(t, 1)
		fixture.gate.release(hostIP(fixture.attempted[1]))

		won := awaitIter(t, result, "the query to return despite the seam panic")
		require.Same(t, fixture.attempted[1], won.Host(), "the caller gets the decisive result")
		require.NoError(t, won.Close(), "the winning iterator is the caller's")

		require.Equal(t, 1, fixture.retired.count(), "the deferred cleanup closed the held candidate")
		fixture.retired.requireClosed(t)
		fixture.drainRemaining(t, 2)
	})

	// The recheck runs only when every launched runner has retired, so nothing is left
	// outstanding and no drain starts at all.
	t.Run("close seam after the cancellation recheck", func(t *testing.T) {
		// Both hosts fail, so every runner retires and the query has no decisive
		// outcome to end on: the recheck is only reached when every launched runner
		// has retired.
		fixture := newSeamFixtureWith(t, []retirementFailure{
			{host: 0, warnings: []string{"w-first"}},
			{host: 1, warnings: []string{"w-second"}},
		})
		// Four closes: the second retirement displaces the first, the two unattempted
		// ones are discarded, and the recheck closes what is still held.
		fixture.retired.panicOn(4)

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		consumes := fixture.consumes
		fixture.harness.session.executor.testAfterConsume = func(consumed int) {
			consumes.hook(consumed)
			if consumed == 4 {
				cancel()
			}
		}

		result := iterAsync(fixture.queryCtx(ctx, &SimpleRetryPolicy{NumRetries: 5}, 3, speculativeTick))
		fixture.startAll(t)

		fixture.failFirst(t, 1)
		fixture.gate.release(hostIP(fixture.attempted[1]))
		fixture.consumes.await(t, 2, "the second runner's retirement")
		fixture.retireUnattempted(t, 3, "the third runner's unattempted retirement")
		fixture.retireUnattempted(t, 4, "the fourth runner's unattempted retirement")

		got := awaitIter(t, result, "the cancellation to end the query")
		requireSameError(t, context.Canceled, got.err, "the recheck outranks the last retirement")
		requireSameError(t, context.Canceled, got.Close(), "Close reports the same error")

		require.Equal(t, 4, fixture.retired.count(), "every candidate was closed exactly once")
		fixture.retired.iters(t)
		fixture.retired.requireClosed(t)
		require.Equal(t, 0, fixture.drain.count(), "nothing was outstanding, so no drain ran")
		require.Equal(t, fixture.baseline, drainGoroutines(), "nothing was outstanding, so no drain ran")
		awaitRunnersExited(t, fixture.stages, 4)
	})

	// The drain's own seam: a panic must not strand the messages behind the one it
	// happened on.
	t.Run("drain seam on the retired branch", func(t *testing.T) {
		fixture := newSeamFixture(t, 2, 3)
		fixture.drain.panicOnRelease()

		result := iterAsync(fixture.query(t, &SimpleRetryPolicy{NumRetries: 5}, 3))
		fixture.startAll(t)

		// The first retirement is consumed; the other two are held before publishing.
		fixture.failFirst(t, 1)
		fixture.entered <- struct{}{}
		fixture.stages.await(t, runRetired, "the third runner to reach its retirement")
		fixture.entered <- struct{}{}
		fixture.stages.await(t, runRetired, "the fourth runner to reach its retirement")

		fixture.gate.release(hostIP(fixture.attempted[1]))
		won := awaitIter(t, result, "the second runner's result to win")
		require.NoError(t, won.Close(), "the winning iterator is the caller's")

		awaitDrainGoroutines(t, fixture.baseline+1, "the drain to wait for the held retirements")
		fixture.holds.releaseAll()

		// Both drained messages are collected before anything is read off them: the
		// seam publishes each iterator before it panics, so the pair is what the
		// drain actually reached, and they must be two distinct iterators.
		first := fixture.drain.await(t, "the drain to reach the first held retirement")
		second := fixture.drain.await(t, "the drain to reach the second held retirement")
		require.NotSame(t, first, second, "the drain reached two distinct iterators")

		// Neither of these two is an attempted retirement:
		// both runners found the selection spent and retired without reaching a host,
		// so their err-iters never held a response framer at all
		// and framer == nil is true of them from birth.
		// The atomic closed flag is the only cleanup evidence here,
		// exactly as it is for T-F2-7a's unattempted retirement.
		requireSameError(t, ErrNoConnections, first.err, "the first drained message retired without an attempt")
		requireSameError(t, ErrNoConnections, second.err, "the second drained message retired without an attempt")

		awaitDrainGoroutines(t, fixture.baseline, "the drain to finish despite the seam panics")
		require.Equal(t, int32(1), atomic.LoadInt32(&first.closed),
			"the seam panic must not have cost the first iterator its Close")
		require.Equal(t, int32(1), atomic.LoadInt32(&second.closed),
			"the seam panic must not have cost the second iterator its Close")
		require.Equal(t, 2, fixture.drain.count(),
			"the message behind the panicking one must still have been processed")
		awaitRunnersExited(t, fixture.stages, 4)
	})

	t.Run("drain seam on the results branch", func(t *testing.T) {
		fixture := newRetirementFixture(t, retirementOpts{
			hosts:   3,
			holdsAt: runResult,
			// The first two publications are held, so the third is the winner and the
			// held two are what the drain is owed.
			park:      []int{1, 2},
			parkDrain: true,
		})
		fixture.pinHosts(fixture.harness.hosts...)
		fixture.drain.panicOnRelease()

		result := iterAsync(fixture.query(t, &SimpleRetryPolicy{NumRetries: 5}, 2))

		// Every runner must be launched before any of them publishes: the ticker stops
		// once coordinate returns, so a winner that published early would leave the
		// later launches to never happen at all.
		for range 3 {
			fixture.stages.await(t, runEntered, "a runner to start")
		}

		// Every host succeeds; one runner at a time, so the ordinals are the test's.
		for i := range 3 {
			fixture.entered <- struct{}{}
			if i < 2 {
				fixture.stages.await(t, runResult, "a runner to reach publication")
			}
		}

		won := awaitIter(t, result, "the first runner's result to win")
		require.NoError(t, won.err, "every host answers successfully here")
		require.NoError(t, won.Close(), "the winning iterator is the caller's")

		awaitDrainGoroutines(t, fixture.baseline+1, "the drain to wait for the held results")
		fixture.holds.releaseAll()

		first := fixture.drain.await(t, "the drain to reach the first held result")
		require.NotNil(t, first.framer, "a successful result still holds its response framer")
		fixture.drain.releaseAll()

		// The second arrival is published on the drain's own goroutine,
		// after the first iterator's Close returned,
		// so receiving it is what orders the read below against that close.
		second := fixture.drain.await(t, "the drain to reach the second held result")
		require.NotSame(t, first, second, "the drain reached two distinct iterators")
		require.Nil(t, first.framer,
			"the seam panic must not have cost the first iterator its framer release")

		awaitDrainGoroutines(t, fixture.baseline, "the drain to finish despite the seam panics")
		require.Equal(t, int32(1), atomic.LoadInt32(&first.closed),
			"the seam panic must not have cost the first iterator its Close")
		// The last iterator has no later arrival to order a framer read against,
		// and the drain's exit is a stack poll rather than a happens-before edge,
		// so only the atomic closed flag Close sets is read for it.
		require.Equal(t, int32(1), atomic.LoadInt32(&second.closed),
			"the seam panic must not have cost the second iterator its Close")
		require.Equal(t, 2, fixture.drain.count(),
			"the message behind the panicking one must still have been processed")
		awaitRunnersExited(t, fixture.stages, 3)
	})
}

// panicOnConsume makes the nth consume seam call panic, after recording it.
//
// Recording first is what lets the test wait for the count the panic happens on.
func (f *seamFixture) panicOnConsume(n int) {
	consumes := f.consumes
	f.harness.session.executor.testAfterConsume = func(consumed int) {
		consumes.hook(consumed)
		if consumed == n {
			panic(callbackPanic{where: "consume seam"})
		}
	}
}

// drainRemaining releases the parked runners and waits for the drain to reclaim their
// messages.
//
// Parameters:
//   - t: the test
//   - want: how many messages were left outstanding
func (f *seamFixture) drainRemaining(t *testing.T, want int) {
	t.Helper()

	awaitDrainGoroutines(t, f.baseline+1, "the drain to wait for the parked runners")
	for range want {
		f.entered <- struct{}{}
	}
	awaitDrainGoroutines(t, f.baseline, "the drain to finish")
	require.Equal(t, want, f.drain.count(), "every outstanding message was reclaimed")
	awaitRunnersExited(t, f.stages, 4)
}

// bindingStmt is a statement the driver prepares, so its binding callback runs.
//
// The callback is invoked after the PREPARE succeeded and before the EXECUTE is sent,
// which makes it the one place a test can hold an attempt open without a wire gate and
// can decide what error an attempt fails with.
// The fixture server answers this prepare with an id and no bind markers.
const bindingStmt = "SELECT nometadata"

// bindingCallback is the controlled binding callback T-F2-16 drives.
//
// The first call announces itself and blocks: that runner is in flight inside execute.
// The second call fails with ErrNoConnections, which is the error value the old
// classification sniffed for.
type bindingCallback struct {
	// entered receives one token as the first call parks.
	entered chan struct{}
	// release lets the first call finish.
	release chan struct{}
	once    sync.Once

	calls atomic.Int32
	// secondErr records what the second call returned.
	secondErr atomic.Value
}

// newBindingCallback returns a callback ready to be passed to Session.Bind.
//
// Returns:
//   - *bindingCallback: the callback
func newBindingCallback() *bindingCallback {
	return &bindingCallback{entered: make(chan struct{}, 1), release: make(chan struct{})}
}

// bind is the func Session.Bind takes.
//
// Returns:
//   - []interface{}: no values; the fixture's prepared statement has no bind markers
//   - error: nil for the first call, ErrNoConnections for every later one
func (c *bindingCallback) bind(*QueryInfo) ([]interface{}, error) {
	if c.calls.Add(1) == 1 {
		select {
		case c.entered <- struct{}{}:
		default:
		}
		<-c.release
		return nil, nil
	}

	c.secondErr.Store(errValue{err: ErrNoConnections})
	return nil, ErrNoConnections
}

// releaseFirst lets the parked first call finish.
func (c *bindingCallback) releaseFirst() {
	c.once.Do(func() { close(c.release) })
}

// errValue boxes an error for atomic.Value, which needs one concrete type.
type errValue struct {
	err error
}

// TestSpeculative_SameErrorValueClassifiedByOutcomeInRun proves run classifies by the
// outcome do reports, not by the iterator's error.
//
// ErrNoConnections is the value the old sniffing keyed on, and a binding callback can
// produce it from an attempt that did reach a host. Under Rethrow that attempt ends the
// query; under RetryNextHost with the selection spent it retires and waits. The old code
// could not tell the two apart at all.
func TestSpeculative_SameErrorValueClassifiedByOutcomeInRun(t *testing.T) {
	// start builds the fixture and drives both runners to the point where the sibling's
	// attempt has failed with ErrNoConnections and the main runner is parked inside its
	// own binding callback.
	start := func(t *testing.T, rt RetryPolicy, holdsAt runStage) (*retirementFixture, *bindingCallback, <-chan *Iter) {
		t.Helper()

		fixture := newRetirementFixture(t, retirementOpts{hosts: 2, holdsAt: holdsAt, parkDrain: true})
		fixture.pinHosts(fixture.harness.hosts...)

		callback := newBindingCallback()
		t.Cleanup(callback.releaseFirst)
		qry := fixture.harness.session.Bind(bindingStmt, callback.bind).
			WithContext(t.Context()).RetryPolicy(rt).Observer(fixture.attempts)
		speculative(1, speculativeTick)(qry)
		result := iterAsync(qry)

		// The main runner is let go first, so it is the call that parks; only once it
		// has parked is the sibling allowed to start.
		fixture.stages.await(t, runEntered, "the main runner to start")
		fixture.entered <- struct{}{}
		awaitSignal(t, callback.entered, "the main runner to park inside its binding callback")

		fixture.stages.await(t, runEntered, "the speculative runner to start")
		fixture.entered <- struct{}{}
		return fixture, callback, result
	}

	// requireSecondCallFailed pins the premise: the error under test is the callback's,
	// not a PREPARE failure that never reached the callback at all.
	requireSecondCallFailed := func(t *testing.T, callback *bindingCallback) {
		t.Helper()

		require.Equal(t, int32(2), callback.calls.Load(), "one binding callback call per runner")
		boxed, ok := callback.secondErr.Load().(errValue)
		require.True(t, ok, "the second call must have recorded its error")
		requireSameError(t, ErrNoConnections, boxed.err, "the second call failed with ErrNoConnections itself")
	}

	t.Run("rethrow ends the query", func(t *testing.T) {
		policy := &scriptedRetryPolicy{answers: []retryAnswer{{attempt: true, retryType: Rethrow}}}
		fixture, callback, result := start(t, policy, runResult)

		got := awaitIter(t, result, "the rethrown error to end the query")
		requireSameError(t, ErrNoConnections, got.err, "Rethrow keeps the attempt's own error")
		require.Same(t, fixture.harness.hosts[1], got.Host(), "the attempt did reach a host")
		requireSameError(t, ErrNoConnections, got.Close(), "Close reports the same error")
		requireSecondCallFailed(t, callback)
		require.Equal(t, 0, fixture.stages.count(runRetired), "Rethrow is decisive, not a retirement")

		// The main runner was still inside its callback when the query ended.
		callback.releaseFirst()
		fixture.drain.await(t, "the drain to reach the main runner's iterator")
		fixture.drain.releaseAll()
		awaitDrainGoroutines(t, fixture.baseline, "the drain to finish")
		require.Equal(t, 1, fixture.drain.count(), "the main runner's message was the outstanding one")
		awaitRunnersExited(t, fixture.stages, 2)
	})

	t.Run("retry next host retires and waits", func(t *testing.T) {
		fixture, callback, result := start(t, &SimpleRetryPolicy{NumRetries: 5}, runRetired)

		fixture.consumes.await(t, 1, "coordinate to consume the speculative runner's retirement")
		requireSecondCallFailed(t, callback)
		require.Equal(t, 1, fixture.stages.count(runRetired), "the same error retires when a retry was asked for")
		require.Equal(t, 0, fixture.stages.count(runResult), "the retirement did not publish a result")
		requireStillRunning(t, result)

		callback.releaseFirst()
		won := awaitIter(t, result, "the main runner's result to win")
		require.NoError(t, won.err, "the main runner's attempt succeeded")
		require.Same(t, fixture.harness.hosts[0], won.Host(), "the main runner ran on the first host")
		require.NoError(t, won.Close(), "the winning iterator is the caller's")

		require.Equal(t, 1, fixture.retired.count(), "the held retirement was closed on the way out")
		require.Same(t, fixture.harness.hosts[1], fixture.retired.records()[0].host,
			"the retirement names the host its attempt reached")
		fixture.retired.requireClosed(t)
		require.Equal(t, 0, fixture.drain.count(), "both runners published before coordinate returned")
		awaitRunnersExited(t, fixture.stages, 2)
	})
}
