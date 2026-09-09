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

// cancelPoint names the retry-policy callback a countingCancelPolicy cancels from.
type cancelPoint int

const (
	// cancelNever leaves the context alone; the test cancels it itself.
	cancelNever cancelPoint = iota
	// cancelInAttempt cancels from inside Attempt, so the checkpoint between Attempt
	// and GetRetryType is the one that must see it.
	cancelInAttempt
	// cancelInGetRetryType cancels from inside GetRetryType, so the checkpoint after
	// it is the one that must see it.
	cancelInGetRetryType
)

// countingCancelPolicy counts the retry-policy callbacks do makes and cancels the query's
// context from inside one of them.
//
// The counts are the whole point: after a cancellation the executor must stop consulting
// the policy, and "stopped consulting" is only observable as a callback that never ran.
type countingCancelPolicy struct {
	// cancelIn names the callback that cancels, if any.
	cancelIn cancelPoint
	// cancel is the query context's cancel function.
	cancel context.CancelFunc
	// attemptAnswer is what Attempt reports.
	attemptAnswer bool
	// retryType is what GetRetryType reports.
	retryType RetryType

	attempts   atomic.Int32
	retryTypes atomic.Int32
}

var _ RetryPolicy = (*countingCancelPolicy)(nil)

// Attempt counts the call, cancels if this is its cancellation point, and answers.
//
// Returns:
//   - bool: the configured answer
func (p *countingCancelPolicy) Attempt(RetryableQuery) bool {
	p.attempts.Add(1)
	if p.cancelIn == cancelInAttempt {
		p.cancel()
	}
	return p.attemptAnswer
}

// GetRetryType counts the call, cancels if this is its cancellation point, and answers.
//
// Returns:
//   - RetryType: the configured answer
func (p *countingCancelPolicy) GetRetryType(error) RetryType {
	p.retryTypes.Add(1)
	if p.cancelIn == cancelInGetRetryType {
		p.cancel()
	}
	return p.retryType
}

// cancelWarnings is the warning list every cancellation fixture's response carries.
var cancelWarnings = []string{"gocql_test: scripted warning"}

// cancelPayload is the custom payload every cancellation fixture's response carries.
var cancelPayload = map[string][]byte{"gocql_test": []byte("payload")}

// newCancelIter returns a scripted attempt result carrying response metadata.
//
// The warnings and the payload are what proves the checkpoint hands over the attempt's
// own iterator rather than a synthetic error iterator: a fresh one has neither.
//
// Parameters:
//   - err: the error the attempt reports
//
// Returns:
//   - *heldIter: the scripted result
func newCancelIter(err error) *heldIter {
	return newHeldIter(err).withWarnings(cancelWarnings).withCustomPayload(cancelPayload)
}

// scriptedKind is one of the two statement shapes do treats alike.
//
// The checkpoints live in do, which is shared, so every case that is not about a
// Query-only field runs against both.
type scriptedKind struct {
	name  string
	build func(rt RetryPolicy, ctx context.Context, idempotent bool, iters []*heldIter) internalRequest
	// served reports how many attempts the request actually answered, which is how a
	// test tells "the executor stopped" from "the executor tried again".
	served func(req internalRequest) int
}

// scriptedKinds returns the Query and Batch shapes the do cases run against.
//
// Returns:
//   - []scriptedKind: the two shapes
func scriptedKinds() []scriptedKind {
	return []scriptedKind{
		{
			name: "query",
			build: func(rt RetryPolicy, ctx context.Context, idempotent bool, iters []*heldIter) internalRequest {
				qry := &Query{stmt: "SELECT cancellation FROM t", rt: rt, idempotent: idempotent}
				return &scriptedQuery{internalQuery: newInternalQuery(qry, ctx), iters: iters}
			},
			served: func(req internalRequest) int { return req.(*scriptedQuery).served },
		},
		{
			name: "batch",
			build: func(rt RetryPolicy, ctx context.Context, idempotent bool, iters []*heldIter) internalRequest {
				// A batch's idempotency comes from its entries, and an empty batch is
				// idempotent, so the non-idempotent case needs an entry to carry it.
				batch := &Batch{rt: rt, Entries: []BatchEntry{{
					Stmt:       "INSERT INTO t (id) VALUES (?)",
					Idempotent: idempotent,
				}}}
				return &scriptedBatch{internalBatch: newInternalBatch(batch, ctx), iters: iters}
			},
			served: func(req internalRequest) int { return req.(*scriptedBatch).served },
		},
	}
}

// requireSameError asserts got is want itself, not an error that merely wraps or matches it.
//
// Comparison is by == rather than errors.Is on purpose: the contract is that do stores the
// context's error bare, and errors.Is would be satisfied by a wrapped one.
//
// Parameters:
//   - t: the test
//   - want: the error the code under test must have stored
//   - got: the error it did store
//   - msg: what the assertion is about
func requireSameError(t *testing.T, want, got error, msg string) {
	t.Helper()

	require.True(t, want == got, "%s: want %v (%T), got %v (%T)", msg, want, want, got, got)
}

// requireHandedOver asserts the checkpoint handed the attempt's own iterator to the
// caller under wantErr, with everything the attempt gathered still readable.
//
// The metadata is read before Close on purpose: Close drops the framer, and the framer is
// where the warnings and the custom payload live.
//
// Parameters:
//   - t: the test
//   - req: the request whose metrics the iterator must still carry
//   - held: the scripted attempt result the caller must have received
//   - result: what do returned
//   - host: the host the attempt ran on
//   - wantErr: the error the iterator must report, by identity
func requireHandedOver(t *testing.T, req internalRequest, held *heldIter, result *Iter,
	host *HostInfo, wantErr error,
) {
	t.Helper()

	require.Same(t, held.iter, result, "the caller receives the attempt's own iterator")
	requireSameError(t, wantErr, result.err, "the iterator reports the context's error, unwrapped")
	require.Same(t, host, result.Host(), "the iterator still names the host it ran on")
	require.Equal(t, cancelWarnings, result.Warnings(), "the response's warnings survive the hand-over")
	require.Equal(t, cancelPayload, result.GetCustomPayload(), "the custom payload survives the hand-over")
	require.Same(t, req.getQueryMetrics(), result.metrics, "the iterator keeps the execution's metrics")

	require.False(t, held.released(), "the iterator handed to the caller keeps its framer")
	requireSameError(t, wantErr, result.Close(), "Close reports the same error")
	require.True(t, held.released(), "the caller's Close releases the framer")
}

// TestDo_CancelledBeforeAttemptSkipsPolicy proves a context that is already done when the
// attempt comes back stops the execution before the retry policy is consulted at all.
//
// The iterator the caller receives is the attempt's own, not a synthetic error iterator:
// its host, its warnings, its custom payload and the execution's metrics are all still
// readable, and it is the caller's Close that returns the framer to the pool.
func TestDo_CancelledBeforeAttemptSkipsPolicy(t *testing.T) {
	for _, kind := range scriptedKinds() {
		t.Run(kind.name, func(t *testing.T) {
			fixture := newDoFixture()
			held := newCancelIter(errScriptedAttempt)
			ctx, cancel := context.WithCancel(t.Context())
			policy := &countingCancelPolicy{cancelIn: cancelNever, cancel: cancel, attemptAnswer: true, retryType: Retry}
			req := kind.build(policy, ctx, true, []*heldIter{held})

			cancel()
			result, outcome := fixture.executor.do(ctx, req, fixture.selector(fixture.pinned()))

			require.Zero(t, policy.attempts.Load(), "a cancelled execution must not ask whether to retry")
			require.Zero(t, policy.retryTypes.Load(), "a cancelled execution must not ask how to retry")
			require.Equal(t, 1, kind.served(req), "the cancellation must not have driven a second attempt")

			require.Equal(t, outcomeResult, outcome, "a checkpoint ends the execution decisively, it does not retire it")
			requireHandedOver(t, req, held, result, fixture.host, context.Canceled)
		})
	}
}

// TestDo_CancelInsideAttemptSkipsGetRetryType proves a cancellation that lands while the
// policy's Attempt is running stops the execution before GetRetryType is asked.
//
// Both answers Attempt can give are covered: a spent budget already stops the loop for
// its own reasons, so only the permitting answer would otherwise prove the checkpoint,
// and the refusing one pins that the checkpoint runs regardless of the answer.
func TestDo_CancelInsideAttemptSkipsGetRetryType(t *testing.T) {
	answers := []struct {
		name    string
		attempt bool
	}{
		{name: "attempt permitted", attempt: true},
		{name: "attempt refused", attempt: false},
	}

	for _, kind := range scriptedKinds() {
		for _, answer := range answers {
			t.Run(kind.name+" "+answer.name, func(t *testing.T) {
				fixture := newDoFixture()
				held := newCancelIter(errScriptedAttempt)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				policy := &countingCancelPolicy{
					cancelIn:      cancelInAttempt,
					cancel:        cancel,
					attemptAnswer: answer.attempt,
					retryType:     Retry,
				}
				req := kind.build(policy, ctx, true, []*heldIter{held})

				result, outcome := fixture.executor.do(ctx, req, fixture.selector(fixture.pinned()))

				require.Equal(t, int32(1), policy.attempts.Load(), "Attempt runs before the cancellation lands")
				require.Zero(t, policy.retryTypes.Load(), "GetRetryType must not be asked after the cancellation")
				require.Equal(t, 1, kind.served(req), "the cancellation must not have driven a second attempt")

				require.Equal(t, outcomeResult, outcome, "a checkpoint ends the execution decisively, it does not retire it")
				requireHandedOver(t, req, held, result, fixture.host, context.Canceled)
			})
		}
	}
}

// TestDo_CancelInsideGetRetryTypeSkipsNextAttempt proves a cancellation that lands while
// GetRetryType is running stops the execution before the answer is acted on.
//
// The answer is RetryNextHost, which would otherwise draw a replacement host and send a
// second attempt, so the selector's draw count and the script's served count are both
// evidence: neither the host nor the attempt may move on.
func TestDo_CancelInsideGetRetryTypeSkipsNextAttempt(t *testing.T) {
	for _, kind := range scriptedKinds() {
		t.Run(kind.name, func(t *testing.T) {
			fixture := newDoFixture()
			iters := []*heldIter{newCancelIter(errScriptedAttempt), newCancelIter(errScriptedAttempt)}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			policy := &countingCancelPolicy{
				cancelIn:      cancelInGetRetryType,
				cancel:        cancel,
				attemptAnswer: true,
				retryType:     RetryNextHost,
			}
			req := kind.build(policy, ctx, true, iters)

			// The raw iterator calls are counted rather than the selections, because the
			// move the checkpoint must prevent - planRetry's draw - is a raw call.
			var draws atomic.Int32
			sel := &hostSelector{iter: scriptedIterator([]*HostInfo{fixture.host, fixture.host}, &draws)}

			result, outcome := fixture.executor.do(ctx, req, sel)

			require.Equal(t, int32(1), policy.attempts.Load(), "Attempt runs before the cancellation lands")
			require.Equal(t, int32(1), policy.retryTypes.Load(), "GetRetryType runs once and is not asked again")
			require.Equal(t, int32(1), draws.Load(), "the cancelled execution must not draw a replacement host")
			require.Equal(t, 1, kind.served(req), "the cancelled execution must not send a second attempt")

			require.Equal(t, outcomeResult, outcome, "a checkpoint ends the execution decisively, it does not retire it")
			requireHandedOver(t, req, iters[0], result, fixture.host, context.Canceled)

			require.False(t, iters[1].released(), "the unused scripted iterator must be untouched")
			require.Zero(t, atomic.LoadInt32(&iters[1].iter.closed), "the unused scripted iterator must be untouched")
		})
	}
}

// TestDo_CancelledAfterAnyAnswerStillReturnsContextError proves the checkpoint after
// GetRetryType outranks every answer the policy can give, including the ones do would
// otherwise treat as decisive.
//
// The unknown retry type is the load-bearing case: it is the one answer whose handling
// destroys the attempt's iterator and builds a fresh one, so it only stays a hand-over if
// the checkpoint runs before planRetry rather than after it.
func TestDo_CancelledAfterAnyAnswerStillReturnsContextError(t *testing.T) {
	answers := []struct {
		name      string
		retryType RetryType
		attempt   bool
	}{
		{name: "rethrow", retryType: Rethrow, attempt: true},
		{name: "ignore", retryType: Ignore, attempt: true},
		{name: "unknown", retryType: RetryType(255), attempt: true},
		{name: "retry with budget spent", retryType: Retry, attempt: false},
		{name: "retry next host with budget spent", retryType: RetryNextHost, attempt: false},
	}

	for _, kind := range scriptedKinds() {
		for _, answer := range answers {
			t.Run(kind.name+" "+answer.name, func(t *testing.T) {
				fixture := newDoFixture()
				held := newCancelIter(errScriptedAttempt)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				policy := &countingCancelPolicy{
					cancelIn:      cancelInGetRetryType,
					cancel:        cancel,
					attemptAnswer: answer.attempt,
					retryType:     answer.retryType,
				}
				req := kind.build(policy, ctx, true, []*heldIter{held})

				result, outcome := fixture.executor.do(ctx, req, fixture.selector(fixture.pinned()))

				require.Equal(t, 1, kind.served(req), "the cancelled execution must not send a second attempt")
				require.Equal(t, outcomeResult, outcome, "a checkpoint ends the execution decisively, it does not retire it")
				requireHandedOver(t, req, held, result, fixture.host, context.Canceled)
			})
		}
	}
}

// TestDo_SuccessUnderCancelledContextIsStillSuccess proves the checkpoints sit behind the
// three exits that never reach the retry policy.
//
// A cancelled context must not rewrite a result do would have returned without consulting
// the policy at all: an attempt that succeeded, a statement that is not idempotent, and a
// statement with no retry policy each keep exactly the outcome they had before.
func TestDo_SuccessUnderCancelledContextIsStillSuccess(t *testing.T) {
	cases := []struct {
		name       string
		attemptErr error
		idempotent bool
		withPolicy bool
	}{
		{name: "successful attempt", attemptErr: nil, idempotent: true, withPolicy: true},
		{name: "not idempotent", attemptErr: errScriptedAttempt, idempotent: false, withPolicy: true},
		{name: "no retry policy", attemptErr: errScriptedAttempt, idempotent: true, withPolicy: false},
	}

	for _, kind := range scriptedKinds() {
		for _, tc := range cases {
			t.Run(kind.name+" "+tc.name, func(t *testing.T) {
				fixture := newDoFixture()
				held := newCancelIter(tc.attemptErr)
				ctx, cancel := context.WithCancel(t.Context())

				var policy *countingCancelPolicy
				var rt RetryPolicy
				if tc.withPolicy {
					policy = &countingCancelPolicy{cancelIn: cancelNever, cancel: cancel, attemptAnswer: true, retryType: Retry}
					rt = policy
				}
				req := kind.build(rt, ctx, tc.idempotent, []*heldIter{held})

				cancel()
				result, outcome := fixture.executor.do(ctx, req, fixture.selector(fixture.pinned()))

				if policy != nil {
					require.Zero(t, policy.attempts.Load(), "this exit never reaches the retry policy")
					require.Zero(t, policy.retryTypes.Load(), "this exit never reaches the retry policy")
				}
				require.Equal(t, 1, kind.served(req), "one attempt decides these exits")
				require.Same(t, held.iter, result, "the caller receives the attempt's own iterator")
				require.Equal(t, outcomeResult, outcome, "these exits decide the query, they do not retire")
				if tc.attemptErr == nil {
					require.NoError(t, result.err, "the cancellation must not rewrite this outcome")
				} else {
					requireSameError(t, tc.attemptErr, result.err, "the cancellation must not rewrite this outcome")
				}
			})
		}
	}
}

// TestExecuteQuery_PinnedIdempotentQueryStillHitsTheCheckpoints proves a query pinned to
// one host with SetHostID reaches the checkpoints like any other.
//
// It runs through executeQuery rather than calling do directly, because the pinned branch
// is executeQuery's: it builds a one-shot selector of its own and skips the host selection
// policy entirely, and handing do a pinned selector would prove nothing about it.
// The host id and the context are copied into the request's options when the internal
// query is built, so both are set on the public Query first - and WithContext returns a
// copy, so it is that copy the request is built from.
func TestExecuteQuery_PinnedIdempotentQueryStillHitsTheCheckpoints(t *testing.T) {
	// build wires a pinned, idempotent query to rt and ctx and scripts its attempts.
	build := func(f *doFixture, rt RetryPolicy, ctx context.Context, iters []*heldIter) *scriptedQuery {
		pub := (&Query{stmt: "SELECT pinned FROM t", rt: rt, idempotent: true}).SetHostID(f.host.HostID())
		pub = pub.WithContext(ctx)
		return &scriptedQuery{internalQuery: newInternalQuery(pub, nil), iters: iters}
	}

	t.Run("cancelled inside attempt", func(t *testing.T) {
		fixture := newDoFixture().withResolvableHost()
		held := newCancelIter(errScriptedAttempt)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		policy := &countingCancelPolicy{
			cancelIn:      cancelInAttempt,
			cancel:        cancel,
			attemptAnswer: true,
			retryType:     Retry,
		}
		req := build(fixture, policy, ctx, []*heldIter{held})

		require.Equal(t, fixture.host.HostID(), req.GetHostID(), "the request must be pinned to the fixture's host")
		require.Same(t, ctx, req.Context(), "the request must carry the test's context")

		result, err := fixture.executor.executeQuery(req)
		require.NoError(t, err, "a resolvable pinned host must not fail before the attempt")

		require.Equal(t, int32(1), policy.attempts.Load(), "Attempt runs before the cancellation lands")
		require.Zero(t, policy.retryTypes.Load(), "GetRetryType must not be asked after the cancellation")
		require.Equal(t, 1, req.served, "the cancellation must not have driven a second attempt")

		requireHandedOver(t, req, held, result, fixture.host, context.Canceled)
	})

	t.Run("deadline already passed", func(t *testing.T) {
		fixture := newDoFixture().withResolvableHost()
		held := newCancelIter(errScriptedAttempt)
		ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Hour))
		defer cancel()
		policy := &countingCancelPolicy{cancelIn: cancelNever, cancel: cancel, attemptAnswer: true, retryType: Retry}
		req := build(fixture, policy, ctx, []*heldIter{held})

		result, err := fixture.executor.executeQuery(req)
		require.NoError(t, err, "a resolvable pinned host must not fail before the attempt")

		require.Zero(t, policy.attempts.Load(), "an expired deadline must not reach the retry policy")
		require.Zero(t, policy.retryTypes.Load(), "an expired deadline must not reach the retry policy")

		requireHandedOver(t, req, held, result, fixture.host, context.DeadlineExceeded)
	})
}

// withResolvableHost gives the fixture's executor a session whose ring resolves the
// fixture host by id, which is what executeQuery's pinned branch looks the host up in.
//
// Returns:
//   - *doFixture: the receiver, for chaining
func (f *doFixture) withResolvableHost() *doFixture {
	session := &Session{}
	session.ring.hosts = map[string]*HostInfo{f.host.HostID(): f.host}
	f.executor.pool.session = session
	return f
}
