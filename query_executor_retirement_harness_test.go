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

// retirementStmt is the statement the retirement fixtures run.
//
// It is one word, so it is never prepared (shouldPrepare only claims the five statement
// types), and the servers that are not made to fail answer it with a void result.
const retirementStmt = "void"

// retirementErrCode is the CQL error code the failing servers answer with.
const retirementErrCode = 0x1001

// notReturnedWindow is how long a "the query has not ended" assertion waits.
//
// It is a negative assertion, so it cannot be event-driven: nothing is signalled by the
// query not returning.
// It is kept short because it is paid on every passing run.
const notReturnedWindow = 50 * time.Millisecond

// retirementFixture is the harness shape every F-failover-2 acceptance test builds on.
//
// It gives the test complete control over arrival order: each runner is let out of
// runEntered one at a time, its request is held at the server until the test releases it,
// and its publication is held at the run stage the holds were built for.
type retirementFixture struct {
	harness *fillHarness
	// gate holds each host's requests until the test releases them.
	gate *requestGate
	// failing is the set of servers answering with an error; set after the harness
	// exists, because the addresses are only known then.
	failing *hostSet
	stages  *runStageRecorder
	// entered releases one runner from runEntered per token.
	entered chan struct{}
	// holds parks the publications the test named.
	holds *publicationHolds
	// consumes acknowledges coordinate's accounting.
	consumes *consumeRecorder
	// retired records the coordinator-side closes.
	retired *retiredSeam
	// drain records the drain's closes.
	drain *drainSeam
	// attempts records every attempt every runner made.
	attempts *attemptRecorder
	// baseline is the drain goroutine count before the fixture ran.
	baseline int
}

// retirementOpts tunes newRetirementFixture.
type retirementOpts struct {
	// hosts is how many servers to start.
	hosts int
	// holdsAt is the run stage the publication holds park at.
	holdsAt runStage
	// park is the 1-based arrival numbers to hold at that stage.
	park []int
	// warnings is the warning list the failing servers' error carries.
	warnings []string
	// oversized makes the failing servers' error carry a payload larger than any
	// pooled buffer, so a release is observable on it.
	oversized bool
}

// newRetirementFixture starts a harness wired for arrival-ordered retirement tests.
//
// The protocol is pinned to v4 because an oversized custom payload is how these fixtures
// make a release observable, and the v5 segment encoder rejects a body that large.
// Both halves of the heartbeat are turned off, so no other framer user exists for the
// duration and a trimmed read buffer is attributable to the close under test.
//
// Parameters:
//   - t: the test
//   - opts: the fixture's shape
//
// Returns:
//   - *retirementFixture: the fixture, with every seam installed
func newRetirementFixture(t *testing.T, opts retirementOpts) *retirementFixture {
	t.Helper()

	baseline := drainGoroutines()
	gate := newRequestGate()
	t.Cleanup(gate.releaseAll)
	failing := newHostSet()

	var payload map[string][]byte
	if opts.oversized {
		payload = oversizedPayload()
	}
	harness := newFillHarnessOpts(t, opts.hosts, fillHarnessOpts{
		proto:    protoVersion4,
		recvHook: gate.hook,
		respHook: errorRespHook(failing, retirementErrCode, "retirement", opts.warnings, payload),
		tune: func(cluster *ClusterConfig) {
			noHeartbeat(cluster)
			cluster.heartbeatPhase = func(time.Duration) time.Duration { return time.Hour }
		},
	})

	stages := newRunStageRecorder()
	entered := stages.gate(runEntered)
	t.Cleanup(releaseStageGate(entered))
	holds := newPublicationHoldsAt(opts.holdsAt, opts.park...)
	t.Cleanup(holds.releaseAll)
	harness.session.executor.testRunHook = holds.wrap(stages.hook)

	consumes := newConsumeRecorder()
	harness.session.executor.testAfterConsume = consumes.hook
	retired := newRetiredSeam()
	harness.session.executor.testBeforeRetiredClose = retired.hook
	drain := newDrainSeam()
	drain.releaseAll()
	harness.session.executor.testBeforeDrainClose = drain.hook

	return &retirementFixture{
		harness:  harness,
		gate:     gate,
		failing:  failing,
		stages:   stages,
		entered:  entered,
		holds:    holds,
		consumes: consumes,
		retired:  retired,
		drain:    drain,
		attempts: newAttemptRecorder(),
		baseline: baseline,
	}
}

// pinHosts makes the shared selector enumerate hosts in exactly this order.
//
// One iterator serves every runner, so the order it enumerates in is the order the
// runners draw in; a replacement Pick past the script yields nothing, which is what makes
// the selection exhaustible.
func (f *retirementFixture) pinHosts(hosts ...*HostInfo) {
	f.harness.session.executor.policy = &scriptedIterPolicy{
		HostSelectionPolicy: f.harness.session.executor.policy,
		script:              [][]*HostInfo{hosts},
	}
}

// fail makes the servers at hosts answer with the fixture's error response.
func (f *retirementFixture) fail(hosts ...*HostInfo) {
	ips := make([]string, 0, len(hosts))
	for _, host := range hosts {
		ips = append(ips, hostIP(host))
	}
	f.failing.set(ips...)
}

// armAll holds the requests of every host in the harness.
func (f *retirementFixture) armAll() {
	for _, host := range f.harness.hosts {
		f.gate.arm(hostIP(host))
	}
}

// query returns the fixture's statement as a speculative, idempotent query.
//
// Parameters:
//   - t: the test
//   - rt: the retry policy
//   - attempts: how many speculative runners to add to the main one
//
// Returns:
//   - *Query: the query
func (f *retirementFixture) query(t *testing.T, rt RetryPolicy, attempts int) *Query {
	t.Helper()

	qry := f.harness.session.Query(retirementStmt).WithContext(t.Context()).
		RetryPolicy(rt).Observer(f.attempts)
	speculative(attempts, speculativeTick)(qry)
	return qry
}

// startAt lets the next runner out of runEntered and waits until its request is in flight
// at host.
//
// This is the ordering primitive the whole file rests on: only one runner is running at a
// time, so the host it draws and the ordinal its publication claims are both the test's
// own choice.
func (f *retirementFixture) startAt(t *testing.T, host *HostInfo, what string) {
	t.Helper()

	f.stages.await(t, runEntered, what+" to start")
	f.entered <- struct{}{}
	f.gate.awaitStartedAt(t, hostIP(host), what+" to send its request")
}

// startWithoutHost lets the next runner out of runEntered without waiting for a request.
//
// It is for a runner that finds the selection already spent and retires without reaching
// any host at all.
func (f *retirementFixture) startWithoutHost(t *testing.T, what string) {
	t.Helper()

	f.stages.await(t, runEntered, what+" to start")
	f.entered <- struct{}{}
}

// retireAt releases the nth held publication and waits for coordinate to account for it.
//
// The consume acknowledgement is what makes the next step safe to take: coordinate runs
// one message at a time on its own goroutine, so a counted message is a handled one.
func (f *retirementFixture) retireAt(t *testing.T, hold, consumed int, what string) {
	t.Helper()

	f.stages.await(t, runRetired, what+" to reach its retirement")
	f.holds.release(hold)
	f.consumes.await(t, consumed, "coordinate to consume "+what)
}

// requireStillRunning asserts the query has not returned.
//
// It is the sibling protection itself: a retirement that ended the query would deliver a
// result here.
func requireStillRunning(t *testing.T, result <-chan *Iter) {
	t.Helper()

	select {
	case iter := <-result:
		t.Fatalf("the query ended while a sibling was still running: %v", iter.err)
	case <-time.After(notReturnedWindow):
	}
}

// requireReleased asserts the framer the seam recorded before a close was oversized then
// and has been trimmed since.
//
// It may only be claimed for the last iterator closed in the test, and only before any
// later response is let through: a released framer goes back to the global pool, where the
// next reader takes it and grows it again.
func requireReleased(t *testing.T, record retiredRecord) {
	t.Helper()

	require.Greater(t, record.bufCap, maxPooledBufSize,
		"the fixture must produce a response larger than any pooled buffer")
	require.LessOrEqual(t, cap(record.framer.readBuffer), defaultBufSize,
		"the close must have released the oversized read buffer")
}

// TestSpeculative_RetiredSiblingDoesNotEndTheQuery proves a runner that ran out of hosts
// waits for its sibling instead of ending the query with its own error.
//
// This is F-failover-2 itself: before it, the sibling's exhausted-selector exit produced
// an error iterator that won the race against a main execution that was still perfectly
// healthy.
func TestSpeculative_RetiredSiblingDoesNotEndTheQuery(t *testing.T) {
	fixture := newRetirementFixture(t, retirementOpts{
		hosts:     2,
		holdsAt:   runRetired,
		park:      []int{1},
		warnings:  []string{"w-b"},
		oversized: true,
	})
	main, sibling := fixture.harness.hosts[0], fixture.harness.hosts[1]
	fixture.pinHosts(main, sibling)
	fixture.fail(sibling)
	fixture.armAll()

	qry := fixture.query(t, &SimpleRetryPolicy{NumRetries: 5}, 1)
	result := iterAsync(qry)

	fixture.startAt(t, main, "the main runner")
	fixture.startAt(t, sibling, "the speculative runner")

	// The sibling's attempt fails, the selection is spent, and it retires.
	fixture.gate.release(hostIP(sibling))
	fixture.retireAt(t, 1, 1, "the speculative runner's retirement")

	requireStillRunning(t, result)

	fixture.gate.release(hostIP(main))
	won := awaitIter(t, result, "the main runner's result to win")
	require.NoError(t, won.err, "the main runner's attempt succeeded")
	require.Same(t, main, won.Host(), "the winner is the runner that reached a host successfully")

	require.Equal(t, map[*HostInfo]int{main: 1, sibling: 1}, fixture.attempts.hosts(),
		"one attempt per runner")

	// The retirement was coordinate's to close, and it closed exactly it.
	require.Equal(t, 1, fixture.retired.count(), "the coordinator closed the one retirement it held")
	require.NotContains(t, fixture.retired.iters(t), won,
		"the winning iterator must not have been closed by the coordinator")
	records := fixture.retired.records()
	require.Same(t, sibling, records[0].host, "the retirement closed is the sibling's own attempt")
	fixture.retired.requireClosed(t)
	requireReleased(t, records[0])

	require.Equal(t, 0, fixture.drain.count(), "every launched runner published before coordinate returned")
	require.Equal(t, fixture.baseline, drainGoroutines(), "no drain was needed")

	require.NoError(t, won.Close(), "the winning iterator is the caller's")
	awaitRunnersExited(t, fixture.stages, 2)
}

// TestSpeculative_AllRetiredReturnsLastAttemptedErrorWithHost proves that when every
// execution has retired the caller receives the last attempt's own iterator, and that it
// is the one the coordinator consumed last.
//
// It is also the F-EH-9 half of this change: the error the caller sees now names its host
// and carries the response's warnings, which the synthetic error iterator the exhausted
// selector used to build had neither of.
func TestSpeculative_AllRetiredReturnsLastAttemptedErrorWithHost(t *testing.T) {
	t.Run("query", func(t *testing.T) {
		fixture := newRetirementFixture(t, retirementOpts{
			hosts:    2,
			holdsAt:  runRetired,
			park:     []int{1, 2},
			warnings: []string{"w-retired"},
		})
		first, last := fixture.harness.hosts[0], fixture.harness.hosts[1]
		fixture.pinHosts(first, last)
		fixture.fail(first, last)
		fixture.armAll()

		qry := fixture.query(t, &SimpleRetryPolicy{NumRetries: 5}, 1)
		result := iterAsync(qry)

		fixture.startAt(t, first, "the main runner")
		fixture.startAt(t, last, "the speculative runner")

		fixture.gate.release(hostIP(first))
		fixture.retireAt(t, 1, 1, "the main runner's retirement")
		requireStillRunning(t, result)

		fixture.gate.release(hostIP(last))
		fixture.retireAt(t, 2, 2, "the speculative runner's retirement")

		got := awaitIter(t, result, "the last retirement to be returned")
		requireRetirementReturned(t, fixture, got, first, last)
	})

	t.Run("batch", func(t *testing.T) {
		fixture := newRetirementFixture(t, retirementOpts{
			hosts:    2,
			holdsAt:  runRetired,
			park:     []int{1, 2},
			warnings: []string{"w-retired"},
		})
		first, last := fixture.harness.hosts[0], fixture.harness.hosts[1]
		fixture.pinHosts(first, last)
		fixture.fail(first, last)
		fixture.armAll()

		batch := fixture.harness.session.Batch(LoggedBatch).WithContext(t.Context()).
			RetryPolicy(&SimpleRetryPolicy{NumRetries: 5})
		batch.Entries = append(batch.Entries, BatchEntry{Stmt: retirementStmt, Idempotent: true})
		batch.SpeculativeExecutionPolicy(&SimpleSpeculativeExecution{NumAttempts: 1, TimeoutDelay: speculativeTick})
		result := batchIterAsync(batch)

		fixture.startAt(t, first, "the main runner")
		fixture.startAt(t, last, "the speculative runner")

		fixture.gate.release(hostIP(first))
		fixture.retireAt(t, 1, 1, "the main runner's retirement")
		requireStillRunning(t, result)

		fixture.gate.release(hostIP(last))
		fixture.retireAt(t, 2, 2, "the speculative runner's retirement")

		got := awaitIter(t, result, "the last retirement to be returned")
		requireRetirementReturned(t, fixture, got, first, last)
	})
}

// batchIterAsync runs the batch on its own goroutine and delivers its iterator.
//
// Returns:
//   - <-chan *Iter: receives the iterator once
func batchIterAsync(batch *Batch) <-chan *Iter {
	result := make(chan *Iter, 1)
	go func() { result <- batch.Iter() }()
	return result
}

// requireRetirementReturned asserts the caller got the last-consumed retirement and the
// coordinator closed the one it displaced.
//
// Parameters:
//   - t: the test
//   - fixture: the fixture the query ran on
//   - got: the iterator the caller received
//   - displaced: the host whose retirement was replaced
//   - kept: the host whose retirement was returned
func requireRetirementReturned(t *testing.T, fixture *retirementFixture, got *Iter, displaced, kept *HostInfo) {
	t.Helper()

	// Read the response metadata before Close: the framer it lives on is what Close
	// gives back.
	require.Same(t, kept, got.Host(), "the retirement consumed last is the one returned")
	require.Equal(t, []string{"w-retired"}, got.Warnings(),
		"the returned retirement carries its response's warnings")
	require.Error(t, got.err, "every execution failed, so the query fails")
	require.Equal(t, got.err, got.Close(), "Close reports the same error")

	require.Equal(t, 1, fixture.retired.count(), "the displaced retirement is the only one closed")
	records := fixture.retired.records()
	require.Same(t, displaced, records[0].host, "the retirement closed is the displaced one")
	require.NotSame(t, got, records[0].iter, "the returned iterator must not have been closed")
	fixture.retired.requireClosed(t)

	require.Equal(t, 0, fixture.drain.count(), "every launched runner published before coordinate returned")
	awaitRunnersExited(t, fixture.stages, 2)
}

// TestSpeculative_AttemptedRetirementBeatsUnattempted proves a retirement that reached a
// host outranks one that did not, whichever order they are consumed in.
//
// The two subtests are the two orders, and they are not interchangeable: the first proves
// a later attempted retirement displaces an earlier unattempted one, the second proves an
// unattempted one arriving last does not displace what is already held.
func TestSpeculative_AttemptedRetirementBeatsUnattempted(t *testing.T) {
	t.Run("unattempted first", func(t *testing.T) {
		fixture := newRetirementFixture(t, retirementOpts{
			hosts:    1,
			holdsAt:  runRetired,
			park:     []int{1, 2},
			warnings: []string{"w-only"},
		})
		only := fixture.harness.hosts[0]
		fixture.pinHosts(only)
		fixture.fail(only)
		fixture.armAll()

		qry := fixture.query(t, &SimpleRetryPolicy{NumRetries: 5}, 1)
		result := iterAsync(qry)

		fixture.startAt(t, only, "the main runner")
		// The sibling finds the one host already drawn and retires without attempting.
		fixture.startWithoutHost(t, "the speculative runner")
		fixture.retireAt(t, 1, 1, "the speculative runner's unattempted retirement")
		requireStillRunning(t, result)

		fixture.gate.release(hostIP(only))
		fixture.retireAt(t, 2, 2, "the main runner's attempted retirement")

		got := awaitIter(t, result, "the attempted retirement to be returned")
		requireAttemptedWon(t, fixture, got, only)
	})

	t.Run("attempted first", func(t *testing.T) {
		fixture := newRetirementFixture(t, retirementOpts{
			hosts:    1,
			holdsAt:  runRetired,
			park:     []int{1, 2},
			warnings: []string{"w-only"},
		})
		only := fixture.harness.hosts[0]
		fixture.pinHosts(only)
		fixture.fail(only)
		fixture.armAll()

		qry := fixture.query(t, &SimpleRetryPolicy{NumRetries: 5}, 1)
		result := iterAsync(qry)

		fixture.startAt(t, only, "the main runner")
		// The sibling is launched but held at runEntered: launched is already 2, so the
		// main runner's retirement cannot be the last one.
		fixture.stages.await(t, runEntered, "the speculative runner to start")

		fixture.gate.release(hostIP(only))
		fixture.retireAt(t, 1, 1, "the main runner's attempted retirement")
		requireStillRunning(t, result)

		fixture.entered <- struct{}{}
		fixture.retireAt(t, 2, 2, "the speculative runner's unattempted retirement")

		got := awaitIter(t, result, "the attempted retirement to be returned")
		requireAttemptedWon(t, fixture, got, only)
	})
}

// requireAttemptedWon asserts the attempted retirement reached the caller and the
// unattempted one was closed by the coordinator.
//
// Parameters:
//   - t: the test
//   - fixture: the fixture the query ran on
//   - got: the iterator the caller received
//   - host: the host the attempted retirement ran on
func requireAttemptedWon(t *testing.T, fixture *retirementFixture, got *Iter, host *HostInfo) {
	t.Helper()

	require.Same(t, host, got.Host(), "the attempted retirement is the one returned")
	require.Equal(t, []string{"w-only"}, got.Warnings(), "the returned retirement carries its response's warnings")
	require.Error(t, got.err, "every execution failed, so the query fails")
	require.Equal(t, got.err, got.Close(), "Close reports the same error")

	require.Equal(t, 1, fixture.retired.count(), "the unattempted retirement is the only one closed")
	records := fixture.retired.records()
	requireSameError(t, ErrNoConnections, records[0].iter.err,
		"the retirement closed is the one that reached no host")
	require.Nil(t, records[0].framer, "an unattempted retirement never held a response framer")
	fixture.retired.requireClosed(t)

	require.Equal(t, 0, fixture.drain.count(), "every launched runner published before coordinate returned")
	awaitRunnersExited(t, fixture.stages, 2)
}

// TestSpeculative_AttemptBudgetRetirementDoesNotEndTheQuery proves the second way to run
// out - the retry policy refusing a further attempt - protects a sibling just as running
// out of hosts does.
//
// NumRetries: 0 is the common configuration this reaches: the attempt budget is shared
// across runners and counted as attempts complete, so the first runner to come back with
// a failure has already spent it, and before this change its iterator ended the query.
func TestSpeculative_AttemptBudgetRetirementDoesNotEndTheQuery(t *testing.T) {
	fixture := newRetirementFixture(t, retirementOpts{
		hosts:     2,
		holdsAt:   runRetired,
		park:      []int{1},
		warnings:  []string{"w-b"},
		oversized: true,
	})
	main, sibling := fixture.harness.hosts[0], fixture.harness.hosts[1]
	fixture.pinHosts(main, sibling)
	fixture.fail(sibling)
	fixture.armAll()

	qry := fixture.query(t, &SimpleRetryPolicy{NumRetries: 0}, 1)
	result := iterAsync(qry)

	fixture.startAt(t, main, "the main runner")
	fixture.startAt(t, sibling, "the speculative runner")

	fixture.gate.release(hostIP(sibling))
	fixture.retireAt(t, 1, 1, "the speculative runner's retirement")

	requireStillRunning(t, result)

	fixture.gate.release(hostIP(main))
	won := awaitIter(t, result, "the main runner's result to win")
	require.NoError(t, won.err, "the main runner's attempt succeeded")
	require.Same(t, main, won.Host(), "the winner is the runner that reached a host successfully")

	require.Equal(t, map[*HostInfo]int{main: 1, sibling: 1}, fixture.attempts.hosts(),
		"one attempt per runner")

	require.Equal(t, 1, fixture.retired.count(), "the coordinator closed the one retirement it held")
	records := fixture.retired.records()
	require.Same(t, sibling, records[0].host, "the retirement closed is the sibling's own attempt")
	fixture.retired.requireClosed(t)
	requireReleased(t, records[0])

	require.Equal(t, 0, fixture.drain.count(), "every launched runner published before coordinate returned")
	require.Equal(t, fixture.baseline, drainGoroutines(), "no drain was needed")

	require.NoError(t, won.Close(), "the winning iterator is the caller's")
	awaitRunnersExited(t, fixture.stages, 2)
}

// TestSpeculative_RethrowStillEndsTheQuery proves Rethrow stays decisive.
//
// The distinction the whole design rests on is that "the policy said stop" is not "this
// execution cannot go on": the same failing sibling that waits under RetryNextHost must
// still end the query here, while the main runner is untouched and in flight.
func TestSpeculative_RethrowStillEndsTheQuery(t *testing.T) {
	fixture := newRetirementFixture(t, retirementOpts{
		hosts:    2,
		holdsAt:  runResult,
		warnings: []string{"w-b"},
	})
	main, sibling := fixture.harness.hosts[0], fixture.harness.hosts[1]
	fixture.pinHosts(main, sibling)
	fixture.fail(sibling)
	fixture.armAll()

	policy := &scriptedRetryPolicy{answers: []retryAnswer{{attempt: true, retryType: Rethrow}}}
	qry := fixture.query(t, policy, 1)
	result := iterAsync(qry)

	fixture.startAt(t, main, "the main runner")
	fixture.startAt(t, sibling, "the speculative runner")

	// The sibling publishes a decisive result, not a retirement.
	fixture.gate.release(hostIP(sibling))
	fixture.stages.await(t, runResult, "the speculative runner to publish its result")

	got := awaitIter(t, result, "the rethrown error to end the query")
	require.Same(t, sibling, got.Host(), "the query ends on the runner the policy told to stop")
	require.Error(t, got.err, "Rethrow keeps the attempt's error")
	require.Equal(t, 0, fixture.retired.count(), "nothing retired, so the coordinator held nothing")
	require.Equal(t, got.err, got.Close(), "Close reports the same error")

	// The main runner was still in flight when the query ended, so its result is the
	// drain's.
	// executeQuery cancels the runner context on the way out, which is what brings that
	// attempt back without the server ever answering it.
	drained := fixture.drain.await(t, "the drain to reach the main runner's iterator")
	require.Same(t, main, drained.Host(), "the drained iterator is the main runner's")
	fixture.gate.release(hostIP(main))
	awaitDrainGoroutines(t, fixture.baseline, "the drain to finish")

	require.Equal(t, 1, fixture.drain.count(), "the main runner's iterator went through the drain")
	awaitRunnersExited(t, fixture.stages, 2)
}

// TestSpeculative_RunnerContextIsVisibleToThePolicy proves a RetryPolicy consulted inside
// a speculative runner sees that runner's own context, so a backoff nap ends when a
// sibling wins.
//
// Without it the policy reads the caller's context, which nothing cancels when a sibling
// wins, and an ExponentialBackoffRetryPolicy naps out its full Max with the query already
// answered.
func TestSpeculative_RunnerContextIsVisibleToThePolicy(t *testing.T) {
	run := func(t *testing.T, batch bool) {
		t.Helper()

		fixture := newRetirementFixture(t, retirementOpts{hosts: 2, holdsAt: runResult})
		main, sibling := fixture.harness.hosts[0], fixture.harness.hosts[1]
		fixture.pinHosts(main, sibling)
		fixture.fail(sibling)
		fixture.armAll()

		// An hour of backoff: only the runner context can end this nap inside the
		// test's budget.
		policy := newCtxRecordingPolicy(&ExponentialBackoffRetryPolicy{
			NumRetries: 5,
			Min:        time.Hour,
			Max:        time.Hour,
		})

		callerCtx := t.Context()
		var result <-chan *Iter
		if batch {
			b := fixture.harness.session.Batch(LoggedBatch).WithContext(callerCtx).RetryPolicy(policy)
			b.Entries = append(b.Entries, BatchEntry{Stmt: retirementStmt, Idempotent: true})
			b.SpeculativeExecutionPolicy(&SimpleSpeculativeExecution{
				NumAttempts:  1,
				TimeoutDelay: speculativeTick,
			})
			result = batchIterAsync(b)
		} else {
			result = iterAsync(fixture.query(t, policy, 1))
		}

		fixture.startAt(t, main, "the main runner")
		fixture.startAt(t, sibling, "the speculative runner")

		// The sibling fails and enters the nap; only then is the winner released, so
		// the nap is demonstrably in progress when the cancellation arrives.
		fixture.gate.release(hostIP(sibling))
		awaitSignal(t, policy.entered, "the speculative runner to enter its backoff")

		fixture.gate.release(hostIP(main))
		won := awaitIter(t, result, "the main runner's result to win")
		require.NoError(t, won.err, "the main runner's attempt succeeded")
		require.NoError(t, won.Close(), "the winning iterator is the caller's")

		// The nap only ends inside this budget if it was watching the runner context.
		awaitRunnersExited(t, fixture.stages, 2)

		seen := policy.recorded()
		require.Len(t, seen, 1, "only the failing runner reached the retry policy")
		require.NotSame(t, callerCtx, seen[0], "the runner is given a context of its own")
		requireSameError(t, context.Canceled, seen[0].Err(),
			"the runner context is cancelled once a sibling wins")

		wantDeadline, wantOK := callerCtx.Deadline()
		gotDeadline, gotOK := seen[0].Deadline()
		require.Equal(t, wantOK, gotOK, "the runner context keeps the caller's deadline")
		require.Equal(t, wantDeadline, gotDeadline, "the runner context keeps the caller's deadline")
	}

	t.Run("query", func(t *testing.T) { run(t, false) })
	t.Run("batch", func(t *testing.T) { run(t, true) })
}
