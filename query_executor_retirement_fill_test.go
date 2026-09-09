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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fillRetirement is the two-host shape the awaitFill retirement cases share.
//
// The main runner draws the first host and is held at the wire; the speculative runner
// draws the second, whose pool is empty with a fill in flight, and ends up inside
// awaitFill. What awaitFill then returns is what each case varies.
type fillRetirement struct {
	*retirementFixture
	// main is the host the first runner draws and is held on.
	main *HostInfo
	// filling is the host whose pool is empty with a fill in flight.
	filling *HostInfo
	// waiting receives one token each time a runner is about to block in awaitFill.
	waiting chan struct{}
	// waitRelease is closed to let a blocked testBeforeWait return; already closed
	// unless the case asked for a blocking wait seam.
	waitRelease chan struct{}
}

// newFillRetirement starts the shared shape.
//
// Parameters:
//   - t: the test
//   - park: the 1-based runRetired arrivals to hold
//   - blockWait: park every runner at the testBeforeWait seam until released
//
// Returns:
//   - *fillRetirement: the fixture
func newFillRetirement(t *testing.T, park []int, blockWait bool) *fillRetirement {
	t.Helper()

	fixture := newRetirementFixture(t, retirementOpts{
		hosts:     2,
		holdsAt:   runRetired,
		park:      park,
		failures:  []retirementFailure{{host: 1, warnings: []string{"w-filling"}}},
		parkDrain: true,
	})
	main, filling := fixture.harness.hosts[0], fixture.harness.hosts[1]
	fixture.pinHosts(main, filling)
	fixture.armAll()

	// The second host's pool is emptied and its dial is held, so a pick there reports
	// a fill in flight and the runner that drew it waits instead of giving up.
	pool := fixture.harness.pool(t, filling)
	fixture.harness.dialer.arm(nil)
	detached := detachPoolConn(t, pool)
	t.Cleanup(func() { detached.Close() })

	f := &fillRetirement{
		retirementFixture: fixture,
		main:              main,
		filling:           filling,
		waiting:           make(chan struct{}, 4),
		waitRelease:       make(chan struct{}),
	}
	if !blockWait {
		close(f.waitRelease)
	}
	fixture.harness.session.executor.testBeforeWait = func() {
		select {
		case f.waiting <- struct{}{}:
		default:
		}
		<-f.waitRelease
	}
	t.Cleanup(f.releaseWait)
	return f
}

// releaseWait lets every runner blocked at the wait seam through.
func (f *fillRetirement) releaseWait() {
	select {
	case <-f.waitRelease:
	default:
		close(f.waitRelease)
	}
}

// startBoth starts the main runner on its host and the speculative runner on the filling
// one, and waits until the latter is inside awaitFill.
func (f *fillRetirement) startBoth(t *testing.T) {
	t.Helper()

	f.startAt(t, f.main, "the main runner")
	f.stages.await(t, runEntered, "the speculative runner to start")
	f.entered <- struct{}{}
	awaitSignal(t, f.waiting, "the speculative runner to wait for the fill")
}

// TestSpeculative_AwaitFillRetirementDoesNotEndTheQuery proves what awaitFill reports
// decides whether the runner retires or ends the query.
//
// awaitFill has one error that means "there is nothing left to wait for" -
// ErrNoConnections, from either of its two return points - and two that are decisive: the
// caller's cancellation and a closed session.
// The first must protect the sibling exactly as an exhausted selector does;
// the other two must not.
func TestSpeculative_AwaitFillRetirementDoesNotEndTheQuery(t *testing.T) {
	// The candidate stops being worth waiting for: the pool is unregistered while the
	// runner is inside the wait.
	t.Run("candidate disappears", func(t *testing.T) {
		fixture := newFillRetirement(t, []int{1}, false)
		result := iterAsync(fixture.query(t, &SimpleRetryPolicy{NumRetries: 5}, 1))
		fixture.startBoth(t)

		fixture.harness.session.handleHostDown(fixture.filling)
		fixture.retireAt(t, 1, 1, "the speculative runner's retirement")
		requireStillRunning(t, result)

		fixture.finishWithMain(t, result)
	})

	// The wait's own timeout expires.
	t.Run("wait times out", func(t *testing.T) {
		fixture := newFillRetirement(t, []int{1}, false)
		// awaitFill reads Session.Timeout when it is called, so setting it here only
		// affects the waits this query makes; the connections were built with the
		// timeout the session started with.
		fixture.harness.session.cfg.Timeout = 20 * time.Millisecond

		result := iterAsync(fixture.query(t, &SimpleRetryPolicy{NumRetries: 5}, 1))
		fixture.startBoth(t)

		fixture.retireAt(t, 1, 1, "the speculative runner's retirement")
		requireStillRunning(t, result)

		fixture.finishWithMain(t, result)
	})

	// A cancellation out of the wait is decisive, and ends the query on its own.
	t.Run("caller cancels during the wait", func(t *testing.T) {
		fixture := newFillRetirement(t, nil, false)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		result := iterAsync(fixture.queryCtx(ctx, &SimpleRetryPolicy{NumRetries: 5}, 1, speculativeTick))
		fixture.startBoth(t)

		cancel()
		got := awaitIter(t, result, "the cancellation to end the query")
		requireSameError(t, context.Canceled, got.err, "a cancelled wait is decisive")
		requireSameError(t, context.Canceled, got.Close(), "Close reports the same error")
		// The coordinator has its own cancellation exit,
		// so the query can return before either runner has classified its outcome.
		// Both publications are observed first,
		// which is what makes the classification counts below say anything at all:
		// the sibling's cancelled wait and the main runner's attempt,
		// brought back by executeQuery cancelling the runner context,
		// are each a decisive result rather than a retirement.
		fixture.stages.await(t, runResult, "a runner to publish its result")
		fixture.stages.await(t, runResult, "the other runner to publish its result")
		require.Equal(t, 2, fixture.stages.count(runResult), "both runners published a decisive result")
		require.Equal(t, 0, fixture.stages.count(runRetired), "a cancelled wait is not a retirement")
		require.Equal(t, 0, fixture.retired.count(), "nothing was ever held")

		// The main runner's attempt comes back on the runner context.
		fixture.drain.await(t, "the drain to reach the main runner's iterator")
		fixture.drain.releaseAll()
		fixture.gate.release(hostIP(fixture.main))
		awaitDrainGoroutines(t, fixture.baseline, "the drain to finish")
		awaitRunnersExited(t, fixture.stages, 2)
	})

	// A fill that succeeds puts the runner back on the ordinary path, where a failing
	// attempt retires the way any other does.
	t.Run("fill succeeds and the attempt then fails", func(t *testing.T) {
		fixture := newFillRetirement(t, []int{1}, false)
		result := iterAsync(fixture.query(t, &SimpleRetryPolicy{NumRetries: 5}, 1))
		fixture.startBoth(t)

		fixture.harness.dialer.releaseAll()
		fixture.gate.awaitStartedAt(t, hostIP(fixture.filling), "the speculative runner to send its request")
		fixture.gate.release(hostIP(fixture.filling))
		fixture.retireAt(t, 1, 1, "the speculative runner's retirement")
		requireStillRunning(t, result)

		require.Positive(t, fixture.attempts.hosts()[fixture.filling],
			"the speculative runner did reach the filled host")
		fixture.finishWithMain(t, result)
	})
}

// finishWithMain releases the main runner and asserts it wins the query.
//
// Parameters:
//   - t: the test
//   - result: the channel the query's iterator arrives on
func (f *fillRetirement) finishWithMain(t *testing.T, result <-chan *Iter) {
	t.Helper()

	f.gate.release(hostIP(f.main))
	won := awaitIter(t, result, "the main runner's result to win")
	require.NoError(t, won.err, "the main runner's attempt succeeded")
	require.Same(t, f.main, won.Host(), "the main runner ran on its own host")
	require.NoError(t, won.Close(), "the winning iterator is the caller's")
	awaitRunnersExited(t, f.stages, 2)
}

// TestSpeculative_SessionCloseDuringAwaitFillEndsTheQuery proves a closed session ends the
// query from inside awaitFill rather than retiring the runner that saw it.
//
// ErrSessionClosed is the one awaitFill error that is neither a cancellation nor "nothing
// left to wait for": it says the session is gone, so waiting for a sibling would be
// waiting for something that can no longer happen.
func TestSpeculative_SessionCloseDuringAwaitFillEndsTheQuery(t *testing.T) {
	fixture := newFillRetirement(t, nil, true)
	// Zero timeout: awaitFill arms no fill timer at all, so the seam can hold the
	// runner for as long as Session.Close takes without the wait expiring underneath
	// it and turning this into an ErrNoConnections retirement.
	fixture.harness.session.cfg.Timeout = 0
	require.Zero(t, fixture.harness.session.cfg.Timeout, "the wait must not be able to time out here")

	// Session.Close closes the connections, so the main runner is held inside its
	// binding callback rather than at the wire.
	callback := newBindingCallback()
	t.Cleanup(callback.releaseFirst)
	qry := fixture.harness.session.Bind(bindingStmt, callback.bind).
		WithContext(context.Background()).RetryPolicy(&SimpleRetryPolicy{NumRetries: 5}).
		Observer(fixture.attempts)
	speculative(1, speculativeTick)(qry)
	result := iterAsync(qry)

	fixture.stages.await(t, runEntered, "the main runner to start")
	fixture.entered <- struct{}{}
	awaitSignal(t, callback.entered, "the main runner to park inside its binding callback")

	fixture.stages.await(t, runEntered, "the speculative runner to start")
	fixture.entered <- struct{}{}
	awaitSignal(t, fixture.waiting, "the speculative runner to reach the wait seam")

	// The seam is still holding the runner, so the close completes first and the wait
	// that follows can only see a closed session.
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		fixture.harness.session.Close()
	}()
	awaitSignal(t, closed, "the session to close")
	fixture.releaseWait()

	got := awaitIter(t, result, "the closed session to end the query")
	requireSameError(t, ErrSessionClosed, got.err, "a closed session is decisive")
	require.Equal(t, 0, fixture.stages.count(runRetired), "a closed session is not a retirement")
	require.Equal(t, 0, fixture.retired.count(), "nothing was ever held")
	requireSameError(t, ErrSessionClosed, got.Close(), "Close reports the same error")

	// The main runner is still inside its callback: its late message is the drain's.
	// executeQuery cancelled the runner context on the way out,
	// so the released callback's attempt reads that cancellation
	// before it ever reaches the closed connection.
	callback.releaseFirst()
	drained := fixture.drain.await(t, "the drain to reach the main runner's iterator")
	requireSameError(t, context.Canceled, drained.err,
		"the main runner's late attempt ends on the cancelled runner context")
	fixture.drain.releaseAll()
	awaitDrainGoroutines(t, fixture.baseline, "the drain to finish")
	require.Equal(t, 1, fixture.drain.count(), "the main runner's message was the outstanding one")
	awaitRunnersExited(t, fixture.stages, 2)
}
