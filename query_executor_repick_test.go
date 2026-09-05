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
}

var _ HostSelectionPolicy = (*oneShotPolicy)(nil)

// Pick returns an iterator that yields the wrapped policy's first host and then
// reports exhaustion.
func (p *oneShotPolicy) Pick(qry ExecutableStatement) NextHost {
	p.picks.Add(1)
	inner := p.HostSelectionPolicy.Pick(qry)
	used := false
	return func() SelectedHost {
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
