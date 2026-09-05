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

// oneShotPolicy wraps a host selection policy so that Pick yields at most one host,
// standing in for hostpool.HostPoolHostPolicy.
//
// That policy's Pick carries a `used` guard (hostpool/hostpool.go:127) added by
// f1ff52e to stop the #1259 CPU spin, which makes every iterator it returns
// one-shot. Package hostpool imports gocql, so an in-package test cannot use the
// real thing and reproduces the shape instead.
type oneShotPolicy struct {
	HostSelectionPolicy

	// picks counts how many iterators were handed out.
	picks atomic.Int32
	// calls counts raw iterator calls across every iterator handed out.
	calls atomic.Int32
}

var _ HostSelectionPolicy = (*oneShotPolicy)(nil)

// Pick returns an iterator that yields the wrapped policy's first host and then
// reports exhaustion.
func (p *oneShotPolicy) Pick(qry ExecutableStatement) NextHost {
	p.picks.Add(1)
	inner := p.HostSelectionPolicy.Pick(qry)
	used := false
	return func() SelectedHost {
		p.calls.Add(1)
		if used {
			return nil
		}
		used = true
		return inner()
	}
}

// installOneShotPolicy makes the session's executor draw hosts from a one-shot
// iterator, leaving the cluster policy that the harness barriers observe in place.
//
// Returns:
//   - *oneShotPolicy: the installed policy, for its pick count
func installOneShotPolicy(h *fillHarness) *oneShotPolicy {
	policy := &oneShotPolicy{HostSelectionPolicy: h.session.executor.policy}
	h.session.executor.policy = policy
	return policy
}

// TestRetryNextHost_OneShotPolicyReachesEveryHost proves RetryNextHost keeps advancing
// when the selection policy hands out a one-shot iterator.
//
// Before the #812 fix the second hostIter() call returned nil, the loop head at
// query_executor.go:250 saw a non-nil lastErr and broke, and the query failed after a
// single attempt with its retry budget untouched.
//
// The fix does not hand a one-shot policy more attempts than a multi-host one would get:
// two hosts are up, so the query is attempted twice and stops, exactly as it would under
// RoundRobin. The retry budget is deliberately larger than that, to show the host bound
// and not the budget is what ends it.
func TestRetryNextHost_OneShotPolicyReachesEveryHost(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	policy := installOneShotPolicy(harness)

	qry := harness.session.Query("kill").
		Idempotent(true).
		RetryPolicy(&SimpleRetryPolicy{NumRetries: 5})

	iter := qry.Iter()
	require.Error(t, iter.Close(), "the test server answers kill with an error")

	require.Equal(t, 2, iter.Attempts(),
		"one attempt per ring host, the same as a policy whose iterator enumerates them")
	require.Equal(t, int32(2), policy.picks.Load(),
		"each exhausted one-shot iterator is replaced until the ring is covered")
}

// TestRetryNextHost_MultiHostPolicyNeverRePicks proves the fix is a no-op for a policy
// whose iterator already enumerates the ring.
//
// The ring bound is false by the time such an iterator runs dry, so no second selection
// round happens and the pre-#812 behaviour is preserved.
func TestRetryNextHost_MultiHostPolicyNeverRePicks(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	policy := &countingPickPolicy{HostSelectionPolicy: harness.session.executor.policy}
	harness.session.executor.policy = policy

	qry := harness.session.Query("kill").
		Idempotent(true).
		RetryPolicy(&SimpleRetryPolicy{NumRetries: 5})

	iter := qry.Iter()
	require.Error(t, iter.Close(), "the test server answers kill with an error")

	require.Equal(t, 2, iter.Attempts(), "the round robin iterator yields both ring hosts")
	require.Equal(t, int32(1), policy.picks.Load(),
		"a policy that enumerates the ring must never be asked for a second iterator")
}

// TestRetryNextHost_ZeroBudgetNeverRePicks proves no selection round is wasted when the
// retry policy permits no retries at all.
//
// attemptsReached is computed before the retry switch but enforced after it
// (query_executor.go:296 and :318), so an ungated re-pick would sample the policy once
// more and throw the result away.
func TestRetryNextHost_ZeroBudgetNeverRePicks(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	policy := installOneShotPolicy(harness)

	qry := harness.session.Query("kill").
		Idempotent(true).
		RetryPolicy(&SimpleRetryPolicy{NumRetries: 0})

	iter := qry.Iter()
	require.Error(t, iter.Close(), "the test server answers kill with an error")

	require.Equal(t, 1, iter.Attempts(), "no retry is permitted")
	require.Equal(t, int32(1), policy.picks.Load(),
		"the terminal pass must not draw an iterator it will discard")
}

// countingPickPolicy counts Pick calls and otherwise leaves the wrapped policy alone.
type countingPickPolicy struct {
	HostSelectionPolicy

	// picks counts how many iterators were handed out.
	picks atomic.Int32
}

var _ HostSelectionPolicy = (*countingPickPolicy)(nil)

// Pick forwards to the wrapped policy and records the call.
func (p *countingPickPolicy) Pick(qry ExecutableStatement) NextHost {
	p.picks.Add(1)
	return p.HostSelectionPolicy.Pick(qry)
}

// scriptedOneShotPolicy is a oneShotPolicy whose Nth Pick yields the Nth scripted host,
// so a test can say exactly which host a re-pick lands on.
//
// The last entry repeats once the script runs out.
type scriptedOneShotPolicy struct {
	HostSelectionPolicy

	// script is the host each successive Pick yields.
	script []*HostInfo

	// picks counts how many iterators were handed out.
	picks atomic.Int32
}

var _ HostSelectionPolicy = (*scriptedOneShotPolicy)(nil)

// Pick returns a one-shot iterator over the next scripted host.
func (p *scriptedOneShotPolicy) Pick(_ ExecutableStatement) NextHost {
	n := int(p.picks.Add(1)) - 1
	if n >= len(p.script) {
		n = len(p.script) - 1
	}
	host := p.script[n]

	used := false
	return func() SelectedHost {
		if used {
			return nil
		}
		used = true
		return (*selectedHost)(host)
	}
}

// TestRetryNextHost_PinnedQueryNeverRePicks proves a query pinned with SetHostID stays
// on its host.
//
// The pinned iterator at query_executor.go:196-207 does not come from the selection
// policy, so replacing it would silently move the query to another coordinator.
func TestRetryNextHost_PinnedQueryNeverRePicks(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	policy := installOneShotPolicy(harness)

	pinned := harness.hosts[0]
	qry := harness.session.Query("kill").
		Idempotent(true).
		RetryPolicy(&SimpleRetryPolicy{NumRetries: 2}).
		SetHostID(pinned.HostID())

	iter := qry.Iter()
	require.Error(t, iter.Close(), "the test server answers kill with an error")

	require.Equal(t, int32(0), policy.picks.Load(),
		"a pinned query must never draw a host from the selection policy")
	require.Equal(t, 1, iter.Attempts(),
		"the pinned iterator yields one host and there is no replacement to draw")
}

// TestRetryNextHost_RePickedHostWithEmptyPoolDoesNotSpin proves the re-pick terminates
// when the host it lands on has no connection to offer.
//
// This is the shape D2's no-spin claim is about: the first attempt fails with a
// retryable error, so RetryNextHost is reached and a fresh iterator is drawn, and the
// host that iterator yields has an empty pool with a fill in flight. Control reaches
// query_executor.go:269, the replacement iterator is already exhausted, and the loop
// head breaks on the non-nil lastErr instead of re-picking again.
func TestRetryNextHost_RePickedHostWithEmptyPoolDoesNotSpin(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	live, drained := harness.hosts[0], harness.hosts[1]

	// Hold every later dial open so the drained pool's refill stays in flight.
	harness.dialer.arm(nil)
	detachPoolConn(t, harness.pool(t, drained))

	policy := &scriptedOneShotPolicy{
		HostSelectionPolicy: harness.session.executor.policy,
		script:              []*HostInfo{live, drained},
	}
	harness.session.executor.policy = policy

	qry := harness.session.Query("kill").
		Idempotent(true).
		RetryPolicy(&SimpleRetryPolicy{NumRetries: 2})

	done := make(chan *Iter, 1)
	go func() { done <- qry.Iter() }()

	var iter *Iter
	select {
	case iter = <-done:
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v: do did not terminate after re-picking an empty pool",
			fillEventBudget)
	}
	require.Error(t, iter.Close(), "the test server answers kill with an error")

	require.Equal(t, int32(2), policy.picks.Load(),
		"exactly one re-pick: the replacement iterator is exhausted before a second")
	require.Equal(t, 1, iter.Attempts(),
		"only the live host could be attempted; the re-picked host had nothing to offer")
}

// TestRetryNextHost_DownHostDoesNotWidenTheBound proves the re-pick bound follows the hosts
// a policy can actually yield, not the hosts the ring remembers.
//
// roundRobbin skips hosts that are not up (policies.go:919-923), so a two-host ring with one
// host down hands an enumerating policy a single host. The ring still holds both, so a bound
// taken from the ring would see 1 < 2 and grant a selection round that the same query does
// not get today.
func TestRetryNextHost_DownHostDoesNotWidenTheBound(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	policy := &countingPickPolicy{HostSelectionPolicy: harness.session.executor.policy}
	harness.session.executor.policy = policy

	// markHostDown is what a DOWN event runs, and every step of it is synchronous.
	harness.session.markHostDown(harness.hosts[1])
	require.Equal(t, 1, harness.session.pool.upHostCount(), "only the live host may remain")

	qry := harness.session.Query("kill").
		Idempotent(true).
		RetryPolicy(&SimpleRetryPolicy{NumRetries: 5})

	iter := qry.Iter()
	require.Error(t, iter.Close(), "the test server answers kill with an error")

	require.Equal(t, 1, iter.Attempts(), "only one host is up, so only one attempt is possible")
	require.Equal(t, int32(1), policy.picks.Load(),
		"a host the policy cannot yield must not buy a second selection round")
}

// TestRetryNextHost_TokenAwareOverEnumeratingFallbackNeverRePicks proves token-aware over an
// enumerating fallback is unaffected.
//
// Scope, deliberately narrow: the test server carries no token metadata and the statement
// has no routing key, so Pick delegates straight to the fallback (policies.go:697-706). What
// it pins is that composing token-aware over RoundRobin does not introduce a second
// selection round. The replica-first path is argued rather than tested: the replica phase
// records each host it yields in `used` and the fallback phase then yields every remaining
// up host not in that set (policies.go:834-841), so the two together drain at the fallback's
// own count. Exercising it for real needs a token ring the unit harness has no server to
// build.
//
// Token-aware over a one-shot fallback is the composition that *is* affected, and that is
// the fix, not a regression.
func TestRetryNextHost_TokenAwareOverEnumeratingFallbackNeverRePicks(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	policy := &countingPickPolicy{
		HostSelectionPolicy: TokenAwareHostPolicy(RoundRobinHostPolicy()),
	}
	for _, host := range harness.hosts {
		policy.HostSelectionPolicy.AddHost(host)
		policy.HostSelectionPolicy.HostUp(host)
	}
	harness.session.executor.policy = policy

	qry := harness.session.Query("kill").
		Idempotent(true).
		RetryPolicy(&SimpleRetryPolicy{NumRetries: 5})

	iter := qry.Iter()
	require.Error(t, iter.Close(), "the test server answers kill with an error")

	require.Equal(t, 2, iter.Attempts(), "the token aware iterator yields both up hosts")
	require.Equal(t, int32(1), policy.picks.Load(),
		"token aware must never be asked for a second iterator")
}

// TestRetryNextHost_ReconnectingHostDoesNotWidenTheBound proves a pool registered for a host
// that is not up yet cannot buy an extra selection round.
//
// This is the window reconnectDownedHosts opens: it calls startPoolFill for a host that is
// still down (session.go:491), and startPoolFill registers the pool before adding the host
// to the policy (events.go:236-238). A bound taken from the raw pool count would see two
// entries and grant a selection round that no iterator can use, for as long as the fill
// takes. Filtering on up closes it, because a host only reaches NodeUp in
// handleNodeConnected, after both the pool and the policy already hold it.
func TestRetryNextHost_ReconnectingHostDoesNotWidenTheBound(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	policy := &countingPickPolicy{HostSelectionPolicy: harness.session.executor.policy}
	harness.session.executor.policy = policy

	// Hold the refill open so the host stays registered but down for the whole query.
	harness.dialer.arm(nil)

	reconnecting := harness.hosts[1]
	harness.session.markHostDown(reconnecting)

	// addHost fills synchronously, and the armed dialer parks that dial, so run the
	// reconnect the way reconnectDownedHosts does - in its own goroutine - and wait until
	// the dial is in flight. The pool is registered by then and the host is still down.
	go harness.session.startPoolFill(reconnecting)
	harness.dialer.awaitStarted(t)

	_, registered := harness.session.pool.getPool(reconnecting)
	require.True(t, registered, "the reconnecting host must hold a registered pool")
	require.False(t, reconnecting.IsUp(), "and its host must still be down")
	require.Equal(t, 1, harness.session.pool.upHostCount(),
		"so it must not count towards the re-pick bound")

	qry := harness.session.Query("kill").
		Idempotent(true).
		RetryPolicy(&SimpleRetryPolicy{NumRetries: 5})

	iter := qry.Iter()
	require.Error(t, iter.Close(), "the test server answers kill with an error")

	require.Equal(t, 1, iter.Attempts(), "only the up host can be attempted")
	require.Equal(t, int32(1), policy.picks.Load(),
		"a pool whose host is still down must not buy a second selection round")
}

// preAttemptQuery is the query the pre-attempt tests run: idempotent, no retry policy,
// so only the pre-attempt site can move it to another host.
//
// Returns:
//   - *Query: the query
func preAttemptQuery(h *fillHarness, stmt string) *Query {
	return h.session.Query(stmt).Idempotent(true).RetryPolicy(nil)
}

// TestPreAttempt_OneShotDownSampleThenServes proves a one-shot sample of a DOWN host is
// replaced before the first attempt instead of ending the query with no attempt at all.
//
// Only the live host is up and pooled, so the budget is one: the DOWN sample is skipped
// for free, the replacement lands on the live host, and the query is served.
func TestPreAttempt_OneShotDownSampleThenServes(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	down, live := harness.hosts[0], harness.hosts[1]
	harness.session.markHostDown(down)

	policy := &scriptedOneShotPolicy{HostSelectionPolicy: harness.session.executor.policy, script: []*HostInfo{down, live}}
	harness.session.executor.policy = policy

	iter := preAttemptQuery(harness, "void").Iter()
	require.NoError(t, iter.Close(), "the live host must serve the query")
	require.Equal(t, 1, iter.Attempts(), "one attempt, on the live host")
	require.Equal(t, int32(2), policy.picks.Load(), "the DOWN sample is replaced once")
}

// TestPreAttempt_NonIdempotentUnchanged proves a non-idempotent query keeps today's
// behaviour: its first sample is taken raw, rejected, and nothing replaces it.
func TestPreAttempt_NonIdempotentUnchanged(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	down, live := harness.hosts[0], harness.hosts[1]
	harness.session.markHostDown(down)

	policy := &scriptedOneShotPolicy{HostSelectionPolicy: harness.session.executor.policy, script: []*HostInfo{down, live}}
	harness.session.executor.policy = policy

	iter := harness.session.Query("void").Idempotent(false).RetryPolicy(nil).Iter()
	require.ErrorIs(t, iter.Close(), ErrNoConnections, "a non-idempotent query is not moved to another host")
	require.Equal(t, 0, iter.Attempts())
	require.Equal(t, int32(1), policy.picks.Load(), "no replacement for a non-idempotent query")
}

// TestPreAttempt_NonIdempotentEmptyFirstUnchanged proves a non-idempotent query whose
// first sample has an empty pool with a fill in flight still waits for that fill, with
// speculation configured and no replacement drawn.
func TestPreAttempt_NonIdempotentEmptyFirstUnchanged(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	empty, other := harness.hosts[0], harness.hosts[1]

	harness.dialer.arm(nil)
	detachPoolConn(t, harness.pool(t, empty))

	policy := &scriptedOneShotPolicy{HostSelectionPolicy: harness.session.executor.policy, script: []*HostInfo{empty, other}}
	harness.session.executor.policy = policy

	waiting := make(chan struct{}, 1)
	harness.session.executor.testBeforeWait = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}

	recorder := newAttemptRecorder()
	result := harness.query(t.Context(), func(qry *Query) {
		speculative(1, time.Hour)(qry)
		// The speculative helper marks the query idempotent; this test is about the
		// non-idempotent path.
		qry.Idempotent(false).RetryPolicy(nil).Observer(recorder)
	})
	awaitSignal(t, waiting, "the query to wait for the fill")
	harness.dialer.releaseAll()

	require.NoError(t, awaitQuery(t, result), "the fill must serve the query")
	require.Equal(t, 1, recorder.count())
	require.Equal(t, int32(1), policy.picks.Load(), "no replacement for a non-idempotent query")
}

// TestPreAttempt_PinnedEmptyFirstUnchanged proves a pinned query whose host has an empty
// pool with a fill in flight waits for that fill and never asks the policy for a host.
func TestPreAttempt_PinnedEmptyFirstUnchanged(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	pinned := harness.hosts[0]

	harness.dialer.arm(nil)
	detachPoolConn(t, harness.pool(t, pinned))
	policy := installOneShotPolicy(harness)

	waiting := make(chan struct{}, 1)
	harness.session.executor.testBeforeWait = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}

	recorder := newAttemptRecorder()
	result := harness.query(t.Context(), func(qry *Query) {
		speculative(1, time.Hour)(qry)
		qry.SetHostID(pinned.HostID()).RetryPolicy(nil).Observer(recorder)
	})
	awaitSignal(t, waiting, "the query to wait for the fill")
	harness.dialer.releaseAll()

	require.NoError(t, awaitQuery(t, result), "the fill must serve the query")
	require.Equal(t, 1, recorder.count())
	require.Equal(t, int32(0), policy.picks.Load(), "a pinned query never asks the policy")
}

// TestPreAttempt_RepeatedDownSampleIsBounded proves a policy that keeps sampling a DOWN
// host is asked for at most 1+maxHosts iterators: skipped samples are free, so the
// re-pick cap is what ends the search.
func TestPreAttempt_RepeatedDownSampleIsBounded(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	down := harness.hosts[0]
	harness.session.markHostDown(down)

	policy := &scriptedOneShotPolicy{HostSelectionPolicy: harness.session.executor.policy, script: []*HostInfo{down}}
	harness.session.executor.policy = policy

	iter := preAttemptQuery(harness, "void").Iter()
	require.ErrorIs(t, iter.Close(), ErrNoConnections)
	require.Equal(t, 0, iter.Attempts())
	require.Equal(t, int32(2), policy.picks.Load(), "one host is up, so one replacement and no more")
}

// TestPreAttempt_ZeroBoundNeverRePicks proves a query whose snapshot holds no up, pooled
// host draws its first iterator and nothing else.
func TestPreAttempt_ZeroBoundNeverRePicks(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	for _, host := range harness.hosts {
		harness.session.markHostDown(host)
	}

	policy := &scriptedOneShotPolicy{HostSelectionPolicy: harness.session.executor.policy, script: []*HostInfo{harness.hosts[0]}}
	harness.session.executor.policy = policy

	iter := preAttemptQuery(harness, "void").Iter()
	require.ErrorIs(t, iter.Close(), ErrNoConnections)
	require.Equal(t, 0, iter.Attempts())
	require.Equal(t, int32(1), policy.picks.Load(), "a zero budget never replaces")
}

// TestPreAttempt_ZeroSnapshotRecoveryServedByFirstIterator proves the first iterator is
// never capped: a host that comes up after a zero snapshot and is yielded by the first
// iterator is served as it always was, directly and with speculation configured.
func TestPreAttempt_ZeroSnapshotRecoveryServedByFirstIterator(t *testing.T) {
	for _, spec := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "speculative"}[spec], func(t *testing.T) {
			harness := newFillHarness(t, 2, nil)
			for _, host := range harness.hosts {
				harness.session.markHostDown(host)
			}
			recovering := harness.hosts[1]

			policy := &scriptedOneShotPolicy{HostSelectionPolicy: harness.session.executor.policy, script: []*HostInfo{recovering}}
			harness.session.executor.policy = policy

			// Bring the host back between the snapshot and the first draw; the UP
			// transition runs on a goroutine, so wait for its publication.
			harness.session.executor.testAfterSnapshot = sync.OnceFunc(func() {
				harness.session.startPoolFill(recovering)
				awaitHost(t, harness.collector.up, recovering, "the recovering host to reach UP")
			})

			qry := preAttemptQuery(harness, "void")
			if spec {
				speculative(1, time.Hour)(qry)
			}
			iter := qry.Iter()
			require.NoError(t, iter.Close(), "the first iterator's host must be served")
			require.Equal(t, 1, iter.Attempts())
			require.Equal(t, int32(1), policy.picks.Load(), "a zero budget never replaces")
		})
	}
}

// TestPreAttempt_RepeatedSaturatedSampleIsBounded proves an up, pooled host with no
// stream to offer spends budget each time it is sampled, so the search ends after
// maxHosts selections with no retry policy involved at all.
func TestPreAttempt_RepeatedSaturatedSampleIsBounded(t *testing.T) {
	harness := newFillHarness(t, 2, noHeartbeat)
	saturated := harness.hosts[0]
	saturatePool(t, harness, harness.pool(t, saturated))

	policy := &scriptedOneShotPolicy{HostSelectionPolicy: harness.session.executor.policy, script: []*HostInfo{saturated}}
	harness.session.executor.policy = policy

	iter := preAttemptQuery(harness, "void").Iter()
	require.ErrorIs(t, iter.Close(), ErrNoConnections)
	require.Equal(t, 0, iter.Attempts())
	require.Equal(t, int32(2), policy.picks.Load(), "two up hosts: two selections, then the budget is spent")
}

// TestPreAttempt_EnumeratingEmptyPoolsWaitForFill proves an enumerating policy whose
// every host has an empty pool with a fill in flight spends the whole budget in its first
// round and waits for a fill without a second Pick.
func TestPreAttempt_EnumeratingEmptyPoolsWaitForFill(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	policy := &countingPickPolicy{HostSelectionPolicy: harness.session.executor.policy}
	harness.session.executor.policy = policy

	harness.dialer.arm(nil)
	for _, host := range harness.hosts {
		detachPoolConn(t, harness.pool(t, host))
	}

	waiting := make(chan struct{}, 1)
	harness.session.executor.testBeforeWait = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}

	recorder := newAttemptRecorder()
	result := harness.query(t.Context(), func(qry *Query) { qry.Idempotent(true).RetryPolicy(nil).Observer(recorder) })
	awaitSignal(t, waiting, "the query to wait for a fill")
	require.Equal(t, int32(1), policy.picks.Load(), "both empty pools were selections; no replacement before waiting")

	harness.dialer.releaseAll()
	require.NoError(t, awaitQuery(t, result))
	require.Equal(t, 1, recorder.count())
	require.Equal(t, int32(1), policy.picks.Load())
}

// TestPreAttempt_EnumeratingSaturatedFirstNoSecondRound proves an enumerating policy takes
// one selection round even when the host it visits first has no usable connection.
//
// 2.4.1-otter took a second round here, because its guard counted attempts:
// the saturated host produced none, so the failing host was attempted twice.
// A saturated host is a selection, and two selections spend a two-host budget.
func TestPreAttempt_EnumeratingSaturatedFirstNoSecondRound(t *testing.T) {
	harness := newFillHarness(t, 2, noHeartbeat)
	saturated, failing := harness.hosts[0], harness.hosts[1]
	saturatePool(t, harness, harness.pool(t, saturated))

	round := []*HostInfo{saturated, failing}
	policy := &scriptedIterPolicy{HostSelectionPolicy: harness.session.executor.policy, script: [][]*HostInfo{round, round}}
	harness.session.executor.policy = policy

	iter := harness.session.Query("kill").Idempotent(true).RetryPolicy(&SimpleRetryPolicy{NumRetries: 5}).Iter()
	require.Error(t, iter.Close(), "the test server answers kill with an error")
	require.Equal(t, 1, iter.Attempts(), "the failing host is attempted once")
	require.Equal(t, int32(1), policy.picks.Load(), "one round covered both up hosts")
}

// TestPreAttempt_EnumeratingFailingFirstOneRound is the control for the visit order that
// already took one round before: the failing host first, then the saturated one.
func TestPreAttempt_EnumeratingFailingFirstOneRound(t *testing.T) {
	harness := newFillHarness(t, 2, noHeartbeat)
	saturated, failing := harness.hosts[0], harness.hosts[1]
	saturatePool(t, harness, harness.pool(t, saturated))

	round := []*HostInfo{failing, saturated}
	policy := &scriptedIterPolicy{HostSelectionPolicy: harness.session.executor.policy, script: [][]*HostInfo{round, round}}
	harness.session.executor.policy = policy

	iter := harness.session.Query("kill").Idempotent(true).RetryPolicy(&SimpleRetryPolicy{NumRetries: 5}).Iter()
	require.Error(t, iter.Close(), "the test server answers kill with an error")
	require.Equal(t, 1, iter.Attempts())
	require.Equal(t, int32(1), policy.picks.Load())
}

// TestPreAttempt_EnumeratingMembershipChangeMidRound pins the accepted transient: a host
// counted in the budget that goes DOWN before the round reaches it leaves the round one
// selection short, and the selector draws replacements until a cap ends it.
//
// The scripted policy answers every replacement with an empty iterator, so the re-pick cap
// is what ends the search: three Picks in total, no attempt.
func TestPreAttempt_EnumeratingMembershipChangeMidRound(t *testing.T) {
	harness := newFillHarness(t, 2, noHeartbeat)
	saturated, leaving := harness.hosts[0], harness.hosts[1]
	saturatePool(t, harness, harness.pool(t, saturated))

	policy := &scriptedIterPolicy{HostSelectionPolicy: harness.session.executor.policy, script: [][]*HostInfo{{saturated, leaving}}}
	harness.session.executor.policy = policy

	// The hook runs after the saturated host's nil Pick, before the round reaches the
	// other host.
	harness.session.executor.testBeforeSnapshot = sync.OnceFunc(func() { harness.session.markHostDown(leaving) })

	iter := preAttemptQuery(harness, "void").Iter()
	require.ErrorIs(t, iter.Close(), ErrNoConnections)
	require.Equal(t, 0, iter.Attempts())
	require.Equal(t, int32(3), policy.picks.Load(), "the first round plus maxHosts empty replacements")
}

// TestPreAttempt_PostFailureDownSampleThenReaches proves the search after a failed attempt
// keeps going past a DOWN sample: the DOWN host is skipped for free and the next sample is
// attempted.
func TestPreAttempt_PostFailureDownSampleThenReaches(t *testing.T) {
	testPostFailureDownSampleThenReaches(t, &SimpleRetryPolicy{NumRetries: 5})
}

// TestPreAttempt_AlwaysNextHostDownSampleThenReaches is the same search under a retry
// policy that never stops: both sites draw from one budget, which ends it.
func TestPreAttempt_AlwaysNextHostDownSampleThenReaches(t *testing.T) {
	testPostFailureDownSampleThenReaches(t, alwaysNextHostPolicy{})
}

// testPostFailureDownSampleThenReaches runs the [live, DOWN, live] script under rt and
// asserts both live hosts were attempted through three Picks.
func testPostFailureDownSampleThenReaches(t *testing.T, rt RetryPolicy) {
	t.Helper()

	harness := newFillHarness(t, 3, nil)
	first, down, last := harness.hosts[0], harness.hosts[2], harness.hosts[1]
	harness.session.markHostDown(down)

	policy := &scriptedOneShotPolicy{HostSelectionPolicy: harness.session.executor.policy, script: []*HostInfo{first, down, last}}
	harness.session.executor.policy = policy

	iter := harness.session.Query("kill").Idempotent(true).RetryPolicy(rt).Iter()
	require.Error(t, iter.Close(), "the test server answers kill with an error")
	require.Equal(t, 2, iter.Attempts(), "both up hosts are attempted")
	require.Equal(t, int32(3), policy.picks.Load(), "the DOWN sample costs a Pick but no budget")
}

// TestPreAttempt_AlwaysNextHostOneShotTerminates proves a retry policy that never stops
// is bounded by the selection budget alone.
func TestPreAttempt_AlwaysNextHostOneShotTerminates(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	policy := installOneShotPolicy(harness)

	iter := harness.session.Query("kill").Idempotent(true).RetryPolicy(alwaysNextHostPolicy{}).Iter()
	require.Error(t, iter.Close(), "the test server answers kill with an error")
	require.Equal(t, 2, iter.Attempts())
	require.Equal(t, int32(2), policy.picks.Load())
}

// TestPreAttempt_PostFailureSaturatedSampleThenReaches proves a replacement that lands on
// an up, pooled host with nothing to offer is handed out, spends budget, and is followed
// by another replacement through the pre-attempt site.
func TestPreAttempt_PostFailureSaturatedSampleThenReaches(t *testing.T) {
	harness := newFillHarness(t, 3, noHeartbeat)
	first, saturated, last := harness.hosts[0], harness.hosts[1], harness.hosts[2]
	saturatePool(t, harness, harness.pool(t, saturated))

	policy := &scriptedOneShotPolicy{HostSelectionPolicy: harness.session.executor.policy, script: []*HostInfo{first, saturated, last}}
	harness.session.executor.policy = policy

	iter := harness.session.Query("kill").Idempotent(true).RetryPolicy(&SimpleRetryPolicy{NumRetries: 5}).Iter()
	require.Error(t, iter.Close(), "the test server answers kill with an error")
	require.Equal(t, 2, iter.Attempts(), "the saturated host is a selection but not an attempt")
	require.Equal(t, int32(3), policy.picks.Load())
}

// TestPreAttempt_ReplacementThenFillStillAwaited proves a fill candidate recorded before
// a replacement is still waited for once the budget is spent.
func TestPreAttempt_ReplacementThenFillStillAwaited(t *testing.T) {
	harness := newFillHarness(t, 2, noHeartbeat)
	empty, saturated := harness.hosts[0], harness.hosts[1]

	harness.dialer.arm(nil)
	detachPoolConn(t, harness.pool(t, empty))
	saturatePool(t, harness, harness.pool(t, saturated))

	policy := &scriptedOneShotPolicy{HostSelectionPolicy: harness.session.executor.policy, script: []*HostInfo{empty, saturated}}
	harness.session.executor.policy = policy

	waiting := make(chan struct{}, 1)
	harness.session.executor.testBeforeWait = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}

	recorder := newAttemptRecorder()
	result := harness.query(t.Context(), func(qry *Query) { qry.Idempotent(true).RetryPolicy(nil).Observer(recorder) })
	awaitSignal(t, waiting, "the query to wait for the fill")
	require.Equal(t, int32(2), policy.picks.Load(), "the saturated replacement was drawn before waiting")

	harness.dialer.releaseAll()
	require.NoError(t, awaitQuery(t, result), "the candidate's fill must serve the query")
	require.Equal(t, 1, recorder.count())
	require.Equal(t, map[*HostInfo]int{empty: 1}, recorder.hosts())
}

// TestPreAttempt_ReplacementFailureAbandonsFillCandidate pins the priority between a
// retained fill candidate and a failed attempt on a replacement: the failure ends the
// query, even though the candidate's fill has completed by then.
func TestPreAttempt_ReplacementFailureAbandonsFillCandidate(t *testing.T) {
	gate := newRequestGate()
	harness := newFillHarnessOpts(t, 2, fillHarnessOpts{recvHook: gate.hook})
	t.Cleanup(gate.releaseAll)
	empty, failing := harness.hosts[0], harness.hosts[1]

	gate.arm(hostIP(failing))
	harness.dialer.arm(nil)
	detachPoolConn(t, harness.pool(t, empty))
	appended := onFreshConnAppended(harness, empty)

	policy := &scriptedOneShotPolicy{HostSelectionPolicy: harness.session.executor.policy, script: []*HostInfo{empty, failing}}
	harness.session.executor.policy = policy

	waiting := make(chan struct{}, 1)
	harness.session.executor.testBeforeWait = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}

	recorder := newAttemptRecorder()
	result := execAsync(harness.session.Query("kill").WithContext(t.Context()).
		Idempotent(true).RetryPolicy(&SimpleRetryPolicy{NumRetries: 5}).Observer(recorder))

	// Either the replacement's request is in flight, or - on a driver without the
	// replacement - the query is waiting for the fill.
	select {
	case <-gate.started:
	case <-waiting:
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for the query to reach the gate or the fill wait", fillEventBudget)
	}

	// Complete the candidate's fill before the replacement is answered.
	harness.dialer.releaseAll()
	awaitSignal(t, appended, "the candidate's pool to receive its connection")
	gate.releaseAll()

	require.Error(t, awaitQuery(t, result), "the test server answers kill with an error")
	require.Equal(t, 1, recorder.count(), "the replacement's failure ends the query")
	require.Equal(t, map[*HostInfo]int{failing: 1}, recorder.hosts(), "the usable candidate is not attempted")
	require.Equal(t, int32(2), policy.picks.Load())
}

// TestPreAttempt_TerminalPassAdvancesOnce proves the retry policy's terminal pass makes
// exactly one raw iterator call: a token-aware iterator whose next replica is up but has
// no pool is advanced past that replica and no fallback Pick happens.
func TestPreAttempt_TerminalPassAdvancesOnce(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	first, unpooled := harness.hosts[0], harness.hosts[1]

	fallback := &countingPickPolicy{HostSelectionPolicy: RoundRobinHostPolicy()}
	outer := tokenAwareOver(t, harness, fallback, 2, []*HostInfo{first, unpooled})
	unpoolHost(harness, unpooled)

	iter := routed(harness.session.Query("kill")).Idempotent(true).RetryPolicy(&SimpleRetryPolicy{NumRetries: 0}).Iter()
	require.Error(t, iter.Close(), "the test server answers kill with an error")
	require.Equal(t, 1, iter.Attempts())
	require.Equal(t, int32(1), outer.picks.Load())
	require.Equal(t, int32(0), fallback.picks.Load(), "the terminal pass consumed the replica, not the fallback")
}

// TestRetryNextHost_TokenAwareReplicaPhaseNeverRePicks proves token-aware over an
// enumerating fallback spends the budget in its replica phase and is never re-picked.
func TestRetryNextHost_TokenAwareReplicaPhaseNeverRePicks(t *testing.T) {
	harness := newFillHarness(t, 2, nil)

	fallback := &countingPickPolicy{HostSelectionPolicy: RoundRobinHostPolicy()}
	outer := tokenAwareOver(t, harness, fallback, 2, []*HostInfo{harness.hosts[0], harness.hosts[1]})

	iter := routed(harness.session.Query("kill")).Idempotent(true).RetryPolicy(&SimpleRetryPolicy{NumRetries: 5}).Iter()
	require.Error(t, iter.Close(), "the test server answers kill with an error")
	require.Equal(t, 2, iter.Attempts(), "both replicas are attempted")
	require.Equal(t, int32(1), outer.picks.Load())
	require.Equal(t, int32(1), fallback.picks.Load(), "the fallback is drawn lazily and contributes nothing")
}

// TestRetryNextHost_TokenAwareOverOneShotCapsMidReplicaPhase proves a replacement iterator
// that yields several hosts - token-aware over a one-shot fallback - is capped at the
// budget before it yields more.
//
// The first round spends two of three units on the replicas and the fallback's sample is
// a replica already yielded, so a replacement is drawn; it yields the first replica again,
// spending the last unit, and is capped before the second replica or the fallback.
func TestRetryNextHost_TokenAwareOverOneShotCapsMidReplicaPhase(t *testing.T) {
	harness := newFillHarness(t, 3, nil)
	replicaA, replicaB, other := harness.hosts[0], harness.hosts[1], harness.hosts[2]

	fallback := &scriptedOneShotPolicy{HostSelectionPolicy: RoundRobinHostPolicy(), script: []*HostInfo{replicaA, other}}
	outer := tokenAwareOver(t, harness, fallback, 2, []*HostInfo{replicaA, replicaB})

	recorder := newAttemptRecorder()
	iter := routed(harness.session.Query("kill")).Idempotent(true).RetryPolicy(&SimpleRetryPolicy{NumRetries: 5}).Observer(recorder).Iter()
	require.Error(t, iter.Close(), "the test server answers kill with an error")
	require.Equal(t, 3, iter.Attempts(), "three up hosts: three selections")
	require.Equal(t, map[*HostInfo]int{replicaA: 2, replicaB: 1}, recorder.hosts(), "the replacement is capped after the first replica")
	require.Equal(t, int32(2), outer.picks.Load())
	require.Equal(t, int32(1), fallback.picks.Load(), "the replacement never reaches its fallback")
}

// TestRetryNextHost_TokenAwareFallbackHostCoveredInFirstRound proves a non-replica host the
// fallback yields is a selection of the first round, so the round covers the budget.
func TestRetryNextHost_TokenAwareFallbackHostCoveredInFirstRound(t *testing.T) {
	harness := newFillHarness(t, 3, nil)

	fallback := &countingPickPolicy{HostSelectionPolicy: RoundRobinHostPolicy()}
	outer := tokenAwareOver(t, harness, fallback, 2, []*HostInfo{harness.hosts[0], harness.hosts[1]})

	iter := routed(harness.session.Query("kill")).Idempotent(true).RetryPolicy(&SimpleRetryPolicy{NumRetries: 5}).Iter()
	require.Error(t, iter.Close(), "the test server answers kill with an error")
	require.Equal(t, 3, iter.Attempts(), "two replicas and the fallback's host")
	require.Equal(t, int32(1), outer.picks.Load())
	require.Equal(t, int32(1), fallback.picks.Load())
}
