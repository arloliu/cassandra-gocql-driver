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

// TestSpeculative_CancelledRunnerIterIsDrained proves the iterator a checkpoint hands back
// under a cancelled context is still reclaimed when nobody is left to receive it.
//
// The checkpoint's whole point is that the iterator is the attempt's own, framer and all,
// so a coordinator that has already answered the caller with its own cancellation iterator
// has to reclaim that framer through the drain.
func TestSpeculative_CancelledRunnerIterIsDrained(t *testing.T) {
	fixture := newRetirementFixture(t, retirementOpts{
		hosts:     1,
		holdsAt:   runResult,
		park:      []int{1},
		failures:  []retirementFailure{{host: 0, warnings: []string{"w-only"}}},
		parkDrain: true,
	})
	only := fixture.harness.hosts[0]
	fixture.pinHosts(only)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// The cancellation lands inside Attempt, so the checkpoint after it is the exit,
	// and the exit hands over the attempt's own iterator.
	policy := &countingCancelPolicy{
		cancelIn:      cancelInAttempt,
		cancel:        cancel,
		attemptAnswer: true,
		retryType:     Retry,
	}
	// An hour between launches: only the main runner ever exists, so the one message
	// the drain is owed is its held publication.
	result := iterAsync(fixture.queryCtx(ctx, policy, 1, time.Hour))
	// Nothing to order here: the one runner is let out of runEntered straight away.
	fixture.entered <- struct{}{}

	// The runner reaches its publication and is held there; coordinate meanwhile takes
	// the cancellation exit and answers the caller itself.
	fixture.stages.await(t, runResult, "the main runner to reach publication")
	got := awaitIter(t, result, "the cancellation to end the query")
	requireSameError(t, context.Canceled, got.err, "the query ends on the cancellation")
	require.Nil(t, got.Host(), "coordinate's own cancellation iterator names no host")
	requireSameError(t, context.Canceled, got.Close(), "Close reports the same error")

	awaitDrainGoroutines(t, fixture.baseline+1, "the drain to wait for the held publication")
	fixture.holds.release(1)

	drained := fixture.drain.await(t, "the drain to reach the runner's iterator")
	require.Same(t, only, drained.Host(), "the drained iterator is the attempt's own")
	requireSameError(t, context.Canceled, drained.err, "the checkpoint rewrote the error to the context's")
	require.NotNil(t, drained.framer, "the attempt's iterator still holds its response framer")

	fixture.drain.releaseAll()
	awaitDrainGoroutines(t, fixture.baseline, "the drain to finish")
	// closed is the evidence, and it is read atomically: this is the only outstanding
	// message, so nothing publishes after the close to order a plain read of the
	// iterator's own fields against the drain goroutine that wrote them.
	require.Equal(t, int32(1), atomic.LoadInt32(&drained.closed), "the drain must have closed it")
	awaitRunnersExited(t, fixture.stages, 1)
}

// TestSpeculative_HeldRetirementIsClosedWhenCallerCancels proves the retirement candidate
// coordinate is holding is reclaimed when the caller's cancellation ends the query
// instead.
//
// A cancellation is one of the three ways the holding stops, and it is the one with no
// message of its own to hang the cleanup off: the candidate is only reachable through
// coordinate's deferred cleanup.
func TestSpeculative_HeldRetirementIsClosedWhenCallerCancels(t *testing.T) {
	fixture := newRetirementFixture(t, retirementOpts{
		hosts:    2,
		holdsAt:  runRetired,
		park:     []int{1},
		failures: []retirementFailure{{host: 1, warnings: []string{"w-sibling"}, oversized: true}},
	})
	main, sibling := fixture.harness.hosts[0], fixture.harness.hosts[1]
	fixture.pinHosts(main, sibling)
	fixture.armAll()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := iterAsync(fixture.queryCtx(ctx, &SimpleRetryPolicy{NumRetries: 5}, 1, speculativeTick))

	fixture.startAt(t, main, "the main runner")
	fixture.startAt(t, sibling, "the speculative runner")

	fixture.gate.release(hostIP(sibling))
	fixture.retireAt(t, 1, 1, "the speculative runner's retirement")
	requireStillRunning(t, result)

	cancel()
	got := awaitIter(t, result, "the cancellation to end the query")
	requireSameError(t, context.Canceled, got.err, "the query ends on the cancellation, not on the retirement")
	requireSameError(t, context.Canceled, got.Close(), "Close reports the same error")

	// Read the release evidence before the main runner's response is let through: a
	// released framer goes back to the global pool, where the next reader may take it.
	require.Equal(t, 1, fixture.retired.count(), "the held retirement is the only thing coordinate closed")
	records := fixture.retired.records()
	require.Same(t, sibling, records[0].host, "the retirement closed is the sibling's own attempt")
	fixture.retired.requireClosed(t)
	requireReleased(t, records[0])

	// The main runner's attempt comes back on the cancellation and goes through the
	// drain, which is the other half of the accounting.
	drained := fixture.drain.await(t, "the drain to reach the main runner's iterator")
	require.Same(t, main, drained.Host(), "the drained iterator is the main runner's")
	fixture.gate.release(hostIP(main))
	awaitDrainGoroutines(t, fixture.baseline, "the drain to finish")
	require.Equal(t, 1, fixture.drain.count(), "one runner was still outstanding")
	awaitRunnersExited(t, fixture.stages, 2)
}

// TestSpeculative_LateAttemptedRetirementIsDrained proves the drain closes a retirement
// that arrives after coordinate returned, framer and all.
//
// It is the attempted arm of the retirement drain: the iterator carries a real response,
// so unlike the unattempted one there is a framer to give back, and the oversized payload
// is what makes that observable.
//
// Two retirements are left outstanding rather than one because the framer is only
// readable after the close through the drain's next arrival: every arrival is on the
// drain's own goroutine, so receiving the second is what orders the first close against
// the test.
func TestSpeculative_LateAttemptedRetirementIsDrained(t *testing.T) {
	fixture := newRetirementFixture(t, retirementOpts{
		hosts:   3,
		holdsAt: runRetired,
		park:    []int{1, 2},
		failures: []retirementFailure{
			{host: 1, warnings: []string{"w-first"}, oversized: true},
			{host: 2, warnings: []string{"w-second"}},
		},
		parkDrain: true,
	})
	main, late, later := fixture.harness.hosts[0], fixture.harness.hosts[1], fixture.harness.hosts[2]
	fixture.pinHosts(main, late, later)
	fixture.armAll()

	result := iterAsync(fixture.query(t, &SimpleRetryPolicy{NumRetries: 5}, 2))

	fixture.startAt(t, main, "the main runner")
	fixture.startAt(t, late, "the first speculative runner")
	fixture.startAt(t, later, "the second speculative runner")

	// Both siblings retire but are held before publishing, so coordinate never sees
	// either of them.
	fixture.gate.release(hostIP(late))
	fixture.stages.await(t, runRetired, "the first speculative runner to reach its retirement")
	fixture.gate.release(hostIP(later))
	fixture.stages.await(t, runRetired, "the second speculative runner to reach its retirement")

	fixture.gate.release(hostIP(main))
	won := awaitIter(t, result, "the main runner's result to win")
	require.NoError(t, won.err, "the main runner's attempt succeeded")
	require.NoError(t, won.Close(), "the winning iterator is the caller's")
	require.Equal(t, 0, fixture.retired.count(), "coordinate never consumed a retirement")

	// launched 3 - consumed 1 = 2 outstanding, and both are held retirements.
	awaitDrainGoroutines(t, fixture.baseline+1, "the drain to wait for the held retirements")
	fixture.holds.release(1)

	drained := fixture.drain.await(t, "the drain to reach the first late retirement")
	require.Same(t, late, drained.Host(), "the late retirement carries its own attempt's iterator")
	require.NotNil(t, drained.framer, "an attempted retirement still holds its response framer")
	framer := drained.framer
	require.Greater(t, cap(framer.readBuffer), maxPooledBufSize,
		"the fixture must produce a response larger than any pooled buffer")

	fixture.holds.release(2)
	fixture.drain.releaseAll()

	// The second arrival is on the drain's goroutine, after the first close returned.
	second := fixture.drain.await(t, "the drain to reach the second late retirement")
	require.Same(t, later, second.Host(), "the second retirement is the other sibling's")
	require.Nil(t, drained.framer, "the drained iterator must have let go of its framer")
	require.LessOrEqual(t, cap(framer.readBuffer), defaultBufSize,
		"the drain's close must have released the oversized read buffer")

	awaitDrainGoroutines(t, fixture.baseline, "the drain to finish")
	require.Equal(t, 2, fixture.drain.count(), "the drain closed exactly the outstanding messages")
	require.Equal(t, int32(1), atomic.LoadInt32(&drained.closed), "the drain must have closed it")
	awaitRunnersExited(t, fixture.stages, 3)
}

// TestSpeculative_CancelledLoserIsDrainedAsAResult proves a losing runner whose attempt
// comes back on the runner context still travels the results arm of the drain.
//
// It is deliberately not the same evidence as the retirement arm above: a cancelled
// attempt is a decisive outcome, so a change that broke only the retirement arm would
// leave this green.
func TestSpeculative_CancelledLoserIsDrainedAsAResult(t *testing.T) {
	fixture := newRetirementFixture(t, retirementOpts{
		hosts:     2,
		holdsAt:   runRetired,
		failures:  []retirementFailure{{host: 1, warnings: []string{"w-sibling"}}},
		parkDrain: true,
	})
	main, sibling := fixture.harness.hosts[0], fixture.harness.hosts[1]
	fixture.pinHosts(main, sibling)
	fixture.armAll()

	result := iterAsync(fixture.query(t, &SimpleRetryPolicy{NumRetries: 5}, 1))

	fixture.startAt(t, main, "the main runner")
	fixture.startAt(t, sibling, "the speculative runner")

	// The sibling stays in flight; the winner's return cancels the runner context, which
	// is what brings the sibling's attempt back.
	fixture.gate.release(hostIP(main))
	won := awaitIter(t, result, "the main runner's result to win")
	require.NoError(t, won.err, "the main runner's attempt succeeded")
	require.NoError(t, won.Close(), "the winning iterator is the caller's")

	drained := fixture.drain.await(t, "the drain to reach the losing runner's iterator")
	requireSameError(t, context.Canceled, drained.err, "the loser's attempt ends on the runner context")
	fixture.drain.releaseAll()
	awaitDrainGoroutines(t, fixture.baseline, "the drain to finish")

	require.Equal(t, 1, fixture.drain.count(), "one runner was still outstanding")
	require.Equal(t, 0, fixture.retired.count(), "a cancelled attempt is a decisive result, not a retirement")
	require.Equal(t, 0, fixture.stages.count(runRetired), "the loser never retired")
	fixture.gate.release(hostIP(sibling))
	awaitRunnersExited(t, fixture.stages, 2)
}

// TestSpeculative_TwoAttemptedRetirementsCloseTheLoser proves the candidate is replaced,
// and the replaced one closed, every time a further attempted retirement arrives.
//
// Three runners are the smallest shape with a middle candidate: one that was held, then
// displaced, and so is neither the first nor the one returned.
func TestSpeculative_TwoAttemptedRetirementsCloseTheLoser(t *testing.T) {
	fixture := newRetirementFixture(t, retirementOpts{
		hosts:   3,
		holdsAt: runRetired,
		park:    []int{1, 2, 3},
		failures: []retirementFailure{
			{host: 0, warnings: []string{"w-first"}},
			{host: 1, warnings: []string{"w-middle"}, oversized: true},
			{host: 2, warnings: []string{"w-last"}},
		},
	})
	first, middle, last := fixture.harness.hosts[0], fixture.harness.hosts[1], fixture.harness.hosts[2]
	fixture.pinHosts(first, middle, last)
	fixture.armAll()

	result := iterAsync(fixture.query(t, &SimpleRetryPolicy{NumRetries: 5}, 2))

	fixture.startAt(t, first, "the main runner")
	fixture.startAt(t, middle, "the first speculative runner")
	fixture.startAt(t, last, "the second speculative runner")

	fixture.gate.release(hostIP(first))
	fixture.retireAt(t, 1, 1, "the main runner's retirement")
	requireStillRunning(t, result)

	fixture.gate.release(hostIP(middle))
	fixture.retireAt(t, 2, 2, "the first speculative runner's retirement")
	requireStillRunning(t, result)

	fixture.gate.release(hostIP(last))
	fixture.retireAt(t, 3, 3, "the second speculative runner's retirement")

	got := awaitIter(t, result, "the last retirement to be returned")
	require.Same(t, last, got.Host(), "the retirement consumed last is the one returned")
	require.Equal(t, fixture.warningsAt(2), got.Warnings(),
		"the returned retirement carries its own response's warnings")
	// All three hosts fail with their own message,
	// so this names which of the three errors the caller was given.
	require.EqualError(t, got.err, retirementErrMsg(2),
		"the returned retirement reports the last host's own error")
	require.Equal(t, got.err, got.Close(), "Close reports the same error")

	// Both displaced candidates were closed, each exactly once.
	require.Equal(t, 2, fixture.retired.count(), "the two displaced candidates were closed")
	iters := fixture.retired.iters(t)
	require.NotContains(t, iters, got, "the returned iterator must not have been closed")
	records := fixture.retired.records()
	require.Same(t, first, records[0].host, "the first candidate is displaced first")
	require.Same(t, middle, records[1].host, "the second candidate is displaced by the third")
	fixture.retired.requireClosed(t)
	// Only the last close is attributable: the first happened while a further response
	// was still to arrive, and that reader may take the framer it released.
	requireReleased(t, records[1])

	require.Equal(t, 0, fixture.drain.count(), "every launched runner published before coordinate returned")
	require.Equal(t, fixture.baseline, drainGoroutines(), "no drain was needed")
	awaitRunnersExited(t, fixture.stages, 3)
}

// TestSpeculative_BudgetRetirementReturnsBeforeTheNextTick proves a query whose only
// launched runner retires returns at once rather than waiting for a launch that has not
// happened.
//
// The stop rule is "every launched runner", not "every runner the policy will ever
// launch": with an hour between launches, waiting for the next one would outlive
// Session.Timeout, and Session.Close cannot release a query whose context is the default
// background one.
func TestSpeculative_BudgetRetirementReturnsBeforeTheNextTick(t *testing.T) {
	fixture := newRetirementFixture(t, retirementOpts{
		hosts:    1,
		holdsAt:  runRetired,
		failures: []retirementFailure{{host: 0, warnings: []string{"w-only"}}},
	})
	only := fixture.harness.hosts[0]
	fixture.pinHosts(only)

	// One speculative attempt an hour away: launched stays 1 for the whole test.
	result := iterAsync(fixture.queryCtx(t.Context(), &SimpleRetryPolicy{NumRetries: 0}, 1, time.Hour))
	// Nothing to order here: the one runner is let out of runEntered straight away.
	fixture.entered <- struct{}{}

	got := awaitIter(t, result, "the only runner's retirement to be returned")
	require.Same(t, only, got.Host(), "the retirement carries the attempt's own iterator")
	require.Equal(t, fixture.warningsAt(0), got.Warnings(), "the retirement carries its response's warnings")
	require.Error(t, got.err, "the only execution failed, so the query fails")
	require.Equal(t, got.err, got.Close(), "Close reports the same error")

	require.Equal(t, 1, fixture.stages.count(runEntered), "the second launch was still an hour away")
	require.Equal(t, 0, fixture.retired.count(), "the only retirement went to the caller, not to a close")
	require.Equal(t, 0, fixture.drain.count(), "nothing was left outstanding")
	awaitRunnersExited(t, fixture.stages, 1)
}

// TestSpeculative_TwoUnattemptedRetirementsKeepTheLater proves the later of two
// retirements of the same kind is the one returned.
//
// Both reached no host, so the attempted-beats-unattempted rule cannot decide between
// them and the consumption order is all that is left.
func TestSpeculative_TwoUnattemptedRetirementsKeepTheLater(t *testing.T) {
	fixture := newRetirementFixture(t, retirementOpts{
		hosts:   1,
		holdsAt: runRetired,
		park:    []int{1, 2},
	})
	only := fixture.harness.hosts[0]
	// The host stays up but loses its pool, so it is drawn and then yields no
	// connection: both runners run out of hosts without ever attempting.
	unpoolHost(fixture.harness, only)

	result := iterAsync(fixture.query(t, &SimpleRetryPolicy{NumRetries: 5}, 1))

	// Both runners must be launched before either retires, or the first retirement
	// would already be the last one.
	fixture.stages.await(t, runEntered, "the main runner to start")
	fixture.stages.await(t, runEntered, "the speculative runner to start")

	fixture.entered <- struct{}{}
	fixture.retireAt(t, 1, 1, "the first runner's retirement")
	requireStillRunning(t, result)

	fixture.entered <- struct{}{}
	fixture.retireAt(t, 2, 2, "the second runner's retirement")

	got := awaitIter(t, result, "the later retirement to be returned")
	requireSameError(t, ErrNoConnections, got.err, "an unattempted retirement reports ErrNoConnections")
	require.Nil(t, got.Host(), "an unattempted retirement names no host")
	requireSameError(t, ErrNoConnections, got.Close(), "Close reports the same error")

	require.Equal(t, 1, fixture.retired.count(), "the earlier candidate was displaced and closed")
	records := fixture.retired.records()
	require.NotSame(t, got, records[0].iter, "the returned iterator is not the one that was closed")
	requireSameError(t, ErrNoConnections, records[0].iter.err, "the displaced candidate reports the same error")
	fixture.retired.requireClosed(t)

	require.Equal(t, 0, fixture.drain.count(), "every launched runner published before coordinate returned")
	awaitRunnersExited(t, fixture.stages, 2)
}

// TestSpeculative_LastRetirementLosesToCancellation proves the recheck at the last
// retirement, not the select, decides between a cancellation and that retirement.
//
// The cancellation is made to land inside the consume seam, after coordinate has taken
// the last retirement off the channel and before it decides what to return, so the two
// are ready together by construction rather than by chance.
func TestSpeculative_LastRetirementLosesToCancellation(t *testing.T) {
	fixture := newRetirementFixture(t, retirementOpts{
		hosts:    1,
		holdsAt:  runRetired,
		park:     []int{1, 2},
		failures: []retirementFailure{{host: 0, warnings: []string{"w-only"}}},
	})
	only := fixture.harness.hosts[0]
	fixture.pinHosts(only)
	fixture.armAll()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// The seam runs on coordinate's own goroutine, after the count and before the
	// handling, so cancelling here is ordered before the recheck.
	consumes := fixture.consumes
	fixture.harness.session.executor.testAfterConsume = func(consumed int) {
		if consumed == 2 {
			cancel()
		}
		consumes.hook(consumed)
	}

	result := iterAsync(fixture.queryCtx(ctx, &SimpleRetryPolicy{NumRetries: 5}, 1, speculativeTick))

	fixture.startAt(t, only, "the main runner")
	fixture.stages.await(t, runEntered, "the speculative runner to start")

	fixture.gate.release(hostIP(only))
	fixture.retireAt(t, 1, 1, "the main runner's attempted retirement")
	requireStillRunning(t, result)

	fixture.entered <- struct{}{}
	fixture.retireAt(t, 2, 2, "the speculative runner's unattempted retirement")

	got := awaitIter(t, result, "the cancellation to end the query")
	requireSameError(t, context.Canceled, got.err,
		"the cancellation outranks the last retirement")
	require.Nil(t, got.Host(), "coordinate's own cancellation iterator names no host")
	requireSameError(t, context.Canceled, got.Close(), "Close reports the same error")

	// Both retirements were the coordinator's to close: the unattempted one lost the
	// reconciliation, the attempted one was still held at the recheck.
	require.Equal(t, 2, fixture.retired.count(), "both retirements were closed by the coordinator")
	fixture.retired.requireClosed(t)
	hosts := []*HostInfo{fixture.retired.records()[0].host, fixture.retired.records()[1].host}
	require.Contains(t, hosts, only, "the attempted retirement was among them")
	require.Contains(t, hosts, (*HostInfo)(nil), "the unattempted retirement was among them")

	require.Equal(t, 0, fixture.drain.count(), "every launched runner published before coordinate returned")
	awaitRunnersExited(t, fixture.stages, 2)
}

// TestSpeculative_CancellationBeforeLastRetirement proves the held candidate is still
// reclaimed when the cancellation arrives through coordinate's own select rather than
// through the recheck.
//
// It is the other cancellation path, and not a substitute for the recheck test: here the
// last retirement never reaches coordinate at all.
func TestSpeculative_CancellationBeforeLastRetirement(t *testing.T) {
	fixture := newRetirementFixture(t, retirementOpts{
		hosts:     1,
		holdsAt:   runRetired,
		park:      []int{2},
		failures:  []retirementFailure{{host: 0, warnings: []string{"w-only"}}},
		parkDrain: true,
	})
	only := fixture.harness.hosts[0]
	fixture.pinHosts(only)
	fixture.armAll()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := iterAsync(fixture.queryCtx(ctx, &SimpleRetryPolicy{NumRetries: 5}, 1, speculativeTick))

	fixture.startAt(t, only, "the main runner")
	fixture.stages.await(t, runEntered, "the speculative runner to start")

	fixture.gate.release(hostIP(only))
	fixture.retireAt(t, 1, 1, "the main runner's attempted retirement")
	requireStillRunning(t, result)

	// The sibling retires too, but is held before publishing.
	fixture.entered <- struct{}{}
	fixture.stages.await(t, runRetired, "the speculative runner to reach its retirement")

	cancel()
	got := awaitIter(t, result, "the cancellation to end the query")
	requireSameError(t, context.Canceled, got.err, "the query ends on the cancellation")
	requireSameError(t, context.Canceled, got.Close(), "Close reports the same error")

	require.Equal(t, 1, fixture.retired.count(), "the held candidate was the coordinator's to close")
	require.Same(t, only, fixture.retired.records()[0].host, "the held candidate is the attempted retirement")
	fixture.retired.requireClosed(t)

	// The held retirement is the outstanding message, and the drain reclaims it.
	awaitDrainGoroutines(t, fixture.baseline+1, "the drain to wait for the held retirement")
	fixture.holds.release(2)
	drained := fixture.drain.await(t, "the drain to reach the late retirement")
	requireSameError(t, ErrNoConnections, drained.err, "the late retirement reached no host")
	fixture.drain.releaseAll()
	awaitDrainGoroutines(t, fixture.baseline, "the drain to finish")
	require.Equal(t, 1, fixture.drain.count(), "one runner was still outstanding")
	awaitRunnersExited(t, fixture.stages, 2)
}
