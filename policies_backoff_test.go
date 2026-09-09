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

// backoffQuery is the smallest RetryableQuery an ExponentialBackoffRetryPolicy needs.
//
// The context comes from a function rather than a field so a case can make Context
// itself panic, which is how the budget guard's position - before any context read -
// is pinned.
type backoffQuery struct {
	attempts int
	ctx      func() context.Context
}

var _ RetryableQuery = (*backoffQuery)(nil)

// Attempts returns the scripted attempt count.
//
// Returns:
//   - int: the count
func (q *backoffQuery) Attempts() int { return q.attempts }

// SetConsistency is never called by the backoff policy.
func (q *backoffQuery) SetConsistency(Consistency) {}

// GetConsistency is never called by the backoff policy.
//
// Returns:
//   - Consistency: a fixed level
func (q *backoffQuery) GetConsistency() Consistency { return Quorum }

// Context returns the scripted context.
//
// Returns:
//   - context.Context: the context, which may be nil
func (q *backoffQuery) Context() context.Context { return q.ctx() }

// fixedCtx returns a Context function handing out ctx.
//
// Parameters:
//   - ctx: the context to hand out, possibly nil
//
// Returns:
//   - func() context.Context: the function
func fixedCtx(ctx context.Context) func() context.Context {
	return func() context.Context { return ctx }
}

// TestExponentialBackoff_AttemptReturnsWhenContextEnds proves the backoff nap watches the
// query's context instead of sleeping the interval out.
//
// The napping cases use Min = Max = one hour so that a policy that still slept would not
// merely be slow, it would hang past the test budget: the assertion is that Attempt came
// back at all, and the value it came back with.
func TestExponentialBackoff_AttemptReturnsWhenContextEnds(t *testing.T) {
	// attemptWithin runs Attempt on its own goroutine and fails the test if it does not
	// return inside the event budget, which is what a policy that ignored the context
	// would do.
	attemptWithin := func(t *testing.T, e *ExponentialBackoffRetryPolicy, q RetryableQuery) bool {
		t.Helper()

		answers := make(chan bool, 1)
		go func() { answers <- e.Attempt(q) }()

		select {
		case answer := <-answers:
			return answer
		case <-time.After(fillEventBudget):
			t.Fatalf("Attempt did not return within %v; the nap ignored the context", fillEventBudget)
			return false
		}
	}

	t.Run("context already cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		policy := &ExponentialBackoffRetryPolicy{NumRetries: 5, Min: time.Hour, Max: time.Hour}
		query := &backoffQuery{attempts: 1, ctx: fixedCtx(ctx)}

		require.False(t, attemptWithin(t, policy, query),
			"a query whose context is already done must not be attempted again")
	})

	t.Run("context ends during the nap", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
		defer cancel()

		policy := &ExponentialBackoffRetryPolicy{NumRetries: 5, Min: time.Hour, Max: time.Hour}
		query := &backoffQuery{attempts: 1, ctx: fixedCtx(ctx)}

		require.False(t, attemptWithin(t, policy, query),
			"the nap must end with the context rather than run the interval out")
	})

	t.Run("spent budget never reads the context", func(t *testing.T) {
		policy := &ExponentialBackoffRetryPolicy{NumRetries: 5, Min: time.Hour, Max: time.Hour}
		query := &backoffQuery{attempts: 6, ctx: func() context.Context {
			panic("gocql_test: Attempt read the context after the retry budget was spent")
		}}

		require.NotPanics(t, func() {
			require.False(t, policy.Attempt(query), "a spent retry budget still ends the retries")
		}, "the budget guard must run before any context read")
	})

	t.Run("nil context naps on the timer alone", func(t *testing.T) {
		policy := &ExponentialBackoffRetryPolicy{NumRetries: 5, Min: time.Millisecond, Max: time.Millisecond}
		query := &backoffQuery{attempts: 1, ctx: fixedCtx(nil)}

		require.True(t, attemptWithin(t, policy, query),
			"a third-party RetryableQuery without a context keeps the plain timed nap")
	})

	t.Run("live context naps the full interval", func(t *testing.T) {
		// Min grows past Max long before the twentieth attempt, so napTime returns Max
		// exactly and the elapsed time is a lower bound the test can assert on.
		policy := &ExponentialBackoffRetryPolicy{NumRetries: 20, Min: time.Millisecond, Max: 5 * time.Millisecond}
		query := &backoffQuery{attempts: 20, ctx: fixedCtx(t.Context())}

		start := time.Now()
		answer := attemptWithin(t, policy, query)
		elapsed := time.Since(start)

		require.True(t, answer, "a live context must not shorten the backoff")
		require.GreaterOrEqual(t, elapsed, 5*time.Millisecond, "the nap must have run its interval")
	})

	t.Run("expired timer and cancellation together", func(t *testing.T) {
		// Both select cases are ready on every iteration:
		// fire is a closed channel, so it never drains,
		// and the context is cancelled before the call.
		// The recheck after the select is the only thing that decides the answer
		// when the runtime picks fire.
		//
		// This case is probabilistic by construction.
		// A correct napUntil returns false every time;
		// one with the recheck removed returns true whenever the runtime picks fire,
		// which it does about half the time,
		// so 200 rounds miss it with probability 2^-200.
		// It is a supplement to the deterministic cases above, not a proof on its own.
		fire := make(chan time.Time)
		close(fire)

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		for round := range 200 {
			require.False(t, napUntil(ctx, fire),
				"round %d: a cancelled context outranks an expired nap", round)
		}
	})
}
