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

	"github.com/stretchr/testify/require"
)

// retirementWarnings is the warning list the retirement fixtures' responses carry.
//
// It is what tells the attempt's own iterator from a synthetic error iterator: only the
// real one has a framer to read a warning off.
var retirementWarnings = []string{"gocql_test: retiring"}

// newRetirementIter returns a scripted attempt result carrying a readable warning.
//
// Parameters:
//   - err: the error the attempt reports
//
// Returns:
//   - *heldIter: the scripted result
func newRetirementIter(err error) *heldIter {
	return newHeldIter(err).withWarnings(retirementWarnings)
}

// hookedRetryPolicy answers every round the same way and runs a hook from inside
// GetRetryType.
//
// The hook is how a test changes the world between do's decision and its next pick -
// taking the host's connection away, for instance - at the one point where do is holding
// an attempt's iterator and has already been told to retry.
type hookedRetryPolicy struct {
	// retryType is the answer every round gives.
	retryType RetryType
	// onGetRetryType runs inside GetRetryType, before the answer.
	onGetRetryType func()
}

var _ RetryPolicy = (*hookedRetryPolicy)(nil)

// Attempt always permits another attempt.
//
// Returns:
//   - bool: true
func (p *hookedRetryPolicy) Attempt(RetryableQuery) bool { return true }

// GetRetryType runs the hook and answers.
//
// Returns:
//   - RetryType: the configured answer
func (p *hookedRetryPolicy) GetRetryType(error) RetryType {
	if p.onGetRetryType != nil {
		p.onGetRetryType()
	}
	return p.retryType
}

// requireRetiredWith asserts do retired carrying held's own iterator, with everything the
// attempt gathered still readable and still the caller's to close.
//
// Parameters:
//   - t: the test
//   - held: the scripted attempt result the retirement must carry
//   - result: what do returned
//   - outcome: the classification do returned alongside it
//   - host: the host the attempt ran on
func requireRetiredWith(t *testing.T, held *heldIter, result *Iter, outcome doOutcome, host *HostInfo) {
	t.Helper()

	require.Same(t, held.iter, result, "the retirement carries the last attempt's own iterator")
	require.Equal(t, outcomeRetiredAttempted, outcome, "an execution that reached a host retires as attempted")
	require.Same(t, host, result.Host(), "the retirement still names the host the attempt ran on")
	require.Equal(t, retirementWarnings, result.Warnings(), "the response's warnings survive the retirement")
	require.False(t, held.released(), "the retirement handed to the caller keeps its framer")
	requireSameError(t, errScriptedAttempt, result.Close(), "Close reports the attempt's own error")
	require.True(t, held.released(), "the caller's Close releases the framer")
}

// TestDo_ExhaustedSelectorHandsOverTheLastAttemptIter proves an execution that wanted a
// further attempt and could not have one retires with the last attempt's own iterator
// rather than ending on a synthetic error.
//
// The iterator is what makes the retirement useful: it names the host, and it still holds
// the response framer the warnings and the custom payload live in, which the error
// iterator the exhausted-selector exit used to build had neither of.
func TestDo_ExhaustedSelectorHandsOverTheLastAttemptIter(t *testing.T) {
	t.Run("selector exhausted after several attempts", func(t *testing.T) {
		fixture := newDoFixture()
		iters := []*heldIter{
			newRetirementIter(errScriptedAttempt),
			newRetirementIter(errScriptedAttempt),
			newRetirementIter(errScriptedAttempt),
		}
		qry := newScriptedQuery(alwaysNextHostPolicy{}, nil, iters)
		sel := fixture.selector(fixture.pinned(), fixture.pinned(), fixture.pinned())

		result, outcome := fixture.executor.do(context.Background(), qry, sel)

		require.Equal(t, len(iters), qry.served, "every selection must have been attempted")
		for i, held := range iters[:len(iters)-1] {
			require.True(t, held.released(), "the superseded iter %d must have released its framer", i)
		}
		requireRetiredWith(t, iters[len(iters)-1], result, outcome, fixture.host)
	})

	// The iterator is held across hosts that yield no connection: the retirement must
	// still carry the attempt that ran, however many fruitless draws came after it.
	t.Run("later hosts yield no connection", func(t *testing.T) {
		fixture := newDoFixture()
		held := newRetirementIter(errScriptedAttempt)
		qry := newScriptedQuery(alwaysNextHostPolicy{}, nil, []*heldIter{held})
		sel := fixture.selector(fixture.pinned(), fixture.hostWithoutConn(), fixture.hostWithoutConn())

		result, outcome := fixture.executor.do(context.Background(), qry, sel)

		require.Equal(t, 1, qry.served, "a host without a connection must not be attempted")
		requireRetiredWith(t, held, result, outcome, fixture.host)
	})

	// Retry names the same host, and that host can lose its connection between the
	// answer and the pick: the same window, reached without drawing at all.
	t.Run("same host loses its connection", func(t *testing.T) {
		fixture := newDoFixture()
		held := newRetirementIter(errScriptedAttempt)
		policy := &hookedRetryPolicy{retryType: Retry, onGetRetryType: fixture.dropConn}
		qry := newScriptedQuery(policy, nil, []*heldIter{held})

		result, outcome := fixture.executor.do(context.Background(), qry, fixture.selector(fixture.pinned()))

		require.Equal(t, 1, qry.served, "the host has no connection left to attempt on")
		requireRetiredWith(t, held, result, outcome, fixture.host)
	})

	// A fill candidate collected on the way must not resurrect the wait: an execution
	// that already attempted has something to retire with and does not wait for a fill.
	t.Run("fill candidate is not waited for", func(t *testing.T) {
		fixture := newDoFixture()
		waits := 0
		fixture.executor.testAfterWake = func() { waits++ }
		held := newRetirementIter(errScriptedAttempt)
		qry := newScriptedQuery(alwaysNextHostPolicy{}, nil, []*heldIter{held})
		sel := fixture.selector(fixture.pinned(), fixture.pendingFillHost())

		result, outcome := fixture.executor.do(context.Background(), qry, sel)

		require.Equal(t, 1, qry.served, "the pending host has no connection to attempt on")
		require.Zero(t, waits, "an execution that already attempted must not wait for a fill")
		requireRetiredWith(t, held, result, outcome, fixture.host)
	})

	// The other way to run out: the policy refuses the budget for an error it still
	// wants retried.
	t.Run("attempt budget reached", func(t *testing.T) {
		fixture := newDoFixture()
		held := newRetirementIter(errScriptedAttempt)
		qry := newScriptedQuery(&retrySameHost{left: 0}, nil, []*heldIter{held})

		result, outcome := fixture.executor.do(context.Background(), qry, fixture.selector(fixture.pinned()))

		require.Equal(t, 1, qry.served, "a refused budget must not drive a second attempt")
		requireRetiredWith(t, held, result, outcome, fixture.host)
	})

	t.Run("no host was ever reached", func(t *testing.T) {
		fixture := newDoFixture()
		qry := newScriptedQuery(alwaysNextHostPolicy{}, nil, nil)

		result, outcome := fixture.executor.do(context.Background(), qry, fixture.selector())

		require.Equal(t, 0, qry.served, "there was no host to attempt on")
		require.Equal(t, outcomeRetiredUnattempted, outcome, "an execution that reached no host retires unattempted")
		requireSameError(t, ErrNoConnections, result.err, "an unattempted retirement reports ErrNoConnections")
		require.Nil(t, result.Host(), "an unattempted retirement names no host")
		requireSameError(t, ErrNoConnections, result.Close(), "Close reports the same error")
	})

	// Rethrow and an unknown retry type are decisive, not retirements: the policy
	// answered "stop", which is not the same as "I cannot go on".
	t.Run("rethrow is decisive", func(t *testing.T) {
		fixture := newDoFixture()
		held := newRetirementIter(errScriptedAttempt)
		policy := &scriptedRetryPolicy{answers: []retryAnswer{{attempt: true, retryType: Rethrow}}}
		qry := newScriptedQuery(policy, nil, []*heldIter{held})

		result, outcome := fixture.executor.do(context.Background(), qry, fixture.selector(fixture.pinned()))

		require.Same(t, held.iter, result, "Rethrow hands the attempt's iterator to the caller")
		require.Equal(t, outcomeResult, outcome, "Rethrow decides the query")
		require.False(t, held.released(), "the iterator handed to the caller keeps its framer")
	})

	t.Run("unknown retry type is decisive", func(t *testing.T) {
		fixture := newDoFixture()
		held := newRetirementIter(errScriptedAttempt)
		qry := newScriptedQuery(unknownRetryTypePolicy{}, nil, []*heldIter{held})

		result, outcome := fixture.executor.do(context.Background(), qry, fixture.selector(fixture.pinned()))

		require.Equal(t, outcomeResult, outcome, "an unknown retry type decides the query")
		requireSameError(t, ErrUnknownRetryType, result.err, "the caller gets a fresh error iterator")
		require.NotSame(t, held.iter, result, "the attempt's iterator is not the one returned")
		require.True(t, held.released(), "the discarded iterator must have released its framer")
	})

	// The iterator is now held across the draw and the pick, so a callback that panics
	// in that window has to leave it reclaimed all the same.
	t.Run("callbacks panicking while the iterator is held", func(t *testing.T) {
		cases := []struct {
			name string
			// wire returns the selector do runs against.
			wire func(f *doFixture, value any) *hostSelector
		}{
			{
				// planRetry's own draw, with the iterator held.
				name: "next host draw",
				wire: func(f *doFixture, value any) *hostSelector {
					draws := &panicOnDraw{hosts: []SelectedHost{f.pinned()}, after: 2, value: value}
					return &hostSelector{iter: draws.next()}
				},
			},
			{
				// pickForHost's first question to a fresh selection.
				name: "selected host info",
				wire: func(f *doFixture, value any) *hostSelector {
					return f.selector(f.pinned(), panicOnInfo{value: value})
				},
			},
			{
				// the selector's replacement Pick, reached when the first iterator
				// runs dry with budget left.
				name: "replacement pick",
				wire: func(f *doFixture, value any) *hostSelector {
					return &hostSelector{
						iter:     f.selector(f.pinned()).iter,
						pick:     func() NextHost { panic(value) },
						eligible: f.executor.pooledUp,
						maxHosts: 2,
					}
				},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				fixture := newDoFixture()
				held := newRetirementIter(errScriptedAttempt)
				qry := newScriptedQuery(alwaysNextHostPolicy{}, nil, []*heldIter{held})
				value := callbackPanic{where: tc.name}
				sel := tc.wire(fixture, value)

				require.PanicsWithValue(t, value, func() {
					fixture.executor.do(context.Background(), qry, sel)
				}, "do must not recover a callback's panic")

				require.Equal(t, 1, qry.served, "the panic lands after the one attempt")
				require.True(t, held.released(), "the held iterator must have released its framer")
			})
		}
	})
}

// TestDo_SameErrorDifferentOutcome proves the outcome, not the error value, is what
// classifies an execution.
//
// ErrNoConnections is the value the old sniffing keyed on, so it is the one that has to
// carry both classifications: the retry policy's answer decides, and the same error is
// decisive under Rethrow and a retirement under RetryNextHost.
func TestDo_SameErrorDifferentOutcome(t *testing.T) {
	t.Run("rethrow makes it decisive", func(t *testing.T) {
		fixture := newDoFixture()
		held := newRetirementIter(ErrNoConnections)
		policy := &scriptedRetryPolicy{answers: []retryAnswer{{attempt: true, retryType: Rethrow}}}
		qry := newScriptedQuery(policy, nil, []*heldIter{held})

		result, outcome := fixture.executor.do(context.Background(), qry, fixture.selector(fixture.pinned()))

		require.Equal(t, outcomeResult, outcome, "the policy's answer, not the error value, decides")
		require.Same(t, held.iter, result)
		requireSameError(t, ErrNoConnections, result.Close(), "Close reports the attempt's own error")
	})

	t.Run("retry next host makes it a retirement", func(t *testing.T) {
		fixture := newDoFixture()
		held := newRetirementIter(ErrNoConnections)
		qry := newScriptedQuery(alwaysNextHostPolicy{}, nil, []*heldIter{held})

		result, outcome := fixture.executor.do(context.Background(), qry, fixture.selector(fixture.pinned()))

		require.Equal(t, outcomeRetiredAttempted, outcome,
			"the same error value retires when the policy asked for another host")
		require.Same(t, held.iter, result)
		require.Same(t, fixture.host, result.Host(),
			"an attempted retirement names its host, unlike the unattempted one carrying the same error")
		requireSameError(t, ErrNoConnections, result.Close(), "Close reports the attempt's own error")
	})
}
