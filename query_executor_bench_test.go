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

// benchRequest is a request whose attempts return prepared iterators in order.
//
// The scripted fixtures the tests use panic past the end of their script
// and would have to be rebuilt per iteration,
// which would measure the rebuild rather than do.
// One iterator per scripted attempt is the smallest thing that still models the real workload:
// do closes the iterator an attempt supersedes,
// so a benchmark that drives two attempts against a single shared iterator
// would spend every iteration but the first on the already-closed CAS path
// instead of the one production takes.
type benchRequest struct {
	*internalQuery

	// iters is one iterator per scripted attempt, in order.
	iters []*Iter
	// served is how many attempts this iteration has already answered.
	served int
}

var _ internalRequest = (*benchRequest)(nil)

// newBenchRequest builds an idempotent query whose attempts answer with errs in order.
//
// Parameters:
//   - rt: the retry policy do consults between attempts
//   - errs: one entry per attempt; nil is a successful attempt
//
// Returns:
//   - *benchRequest: the request, armed for its first iteration
func newBenchRequest(rt RetryPolicy, errs ...error) *benchRequest {
	qry := &Query{stmt: "SELECT bench FROM t", rt: rt, idempotent: true}
	request := &benchRequest{internalQuery: newInternalQuery(qry, context.Background())}
	metrics := request.getQueryMetrics()
	request.iters = make([]*Iter, len(errs))
	for i, err := range errs {
		request.iters[i] = &Iter{err: err, metrics: metrics}
	}
	return request
}

// rearm puts the request back in the state its first benchmark iteration found it in.
//
// Iter.Close is a one-way CAS and do closes every superseded attempt iterator, so without
// this every iteration after the first would measure a different code path.
// It allocates nothing, so it can stay inside the timed loop.
func (r *benchRequest) rearm() {
	r.served = 0
	for _, iter := range r.iters {
		atomic.StoreInt32(&iter.closed, 0)
	}
}

// execute returns the iterator scripted for this attempt.
//
// Returns:
//   - *Iter: the attempt's iterator; the last scripted one once the script is spent
func (r *benchRequest) execute(context.Context, *Conn) *Iter {
	iter := r.iters[len(r.iters)-1]
	if r.served < len(r.iters) {
		iter = r.iters[r.served]
	}
	r.served++
	return iter
}

// snapshotForRunner keeps the wrapper on the runner's own copy.
//
// Parameters:
//   - ctx: the runner context the snapshot answers Context() with
//
// Returns:
//   - internalRequest: the wrapped snapshot, sharing this request's scripted iterators
func (r *benchRequest) snapshotForRunner(ctx context.Context) internalRequest {
	snap, _ := r.internalQuery.snapshotForRunner(ctx).(*internalQuery)
	return &benchRequest{internalQuery: snap, iters: r.iters}
}

// BenchmarkDo_SingleAttempt measures the cheapest path through do:
// one attempt that succeeds and is handed straight to the caller.
func BenchmarkDo_SingleAttempt(b *testing.B) {
	fixture := newDoFixture()
	request := newBenchRequest(&SimpleRetryPolicy{NumRetries: 3}, nil)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		request.rearm()
		iter, _ := fixture.executor.do(ctx, request, fixture.selector(fixture.pinned()))
		_ = iter
	}
	b.StopTimer()
	require.Equal(b, 1, request.served, "the measured workload must be a single attempt")
}

// BenchmarkDo_RetryOnce measures the retry path:
// one failing attempt, a Retry on the same host,
// and the hand-over of the second attempt's successful iterator.
func BenchmarkDo_RetryOnce(b *testing.B) {
	fixture := newDoFixture()
	request := newBenchRequest(retryOncePolicy{}, errScriptedAttempt, nil)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		request.rearm()
		iter, _ := fixture.executor.do(ctx, request, fixture.selector(fixture.pinned()))
		_ = iter
	}
	b.StopTimer()
	// The name is the claim: without this the benchmark could quietly become a
	// one-attempt workload again and still report a number.
	require.Equal(b, 2, request.served, "the measured workload must be exactly two attempts")
	require.Equal(b, int32(1), atomic.LoadInt32(&request.iters[0].closed),
		"the superseded attempt's iterator must have been closed by the last iteration")
}

// retryOncePolicy permits a further attempt and keeps it on the host that just failed.
//
// The script ends the loop, not the policy: the second attempt succeeds, so do hands it
// over without consulting the policy again.
type retryOncePolicy struct{}

var _ RetryPolicy = retryOncePolicy{}

// Attempt always permits another attempt.
//
// Returns:
//   - bool: true
func (retryOncePolicy) Attempt(RetryableQuery) bool { return true }

// GetRetryType always retries on the same host.
//
// Returns:
//   - RetryType: Retry
func (retryOncePolicy) GetRetryType(error) RetryType { return Retry }

// BenchmarkCoordinate_MainWins measures the coordinator against a real session
// when the main runner answers before any speculative launch is due.
//
// An hour between launches means only the main runner ever starts,
// so what is measured is coordinate's own per-query cost
// (the two channels, the snapshot, the ticker and the cleanup defers)
// on top of one ordinary round trip.
// The harness and the query are built once, outside the timed loop.
func BenchmarkCoordinate_MainWins(b *testing.B) {
	harness := newFillHarness(b, 1, nil)
	qry := harness.session.Query("void").WithContext(context.Background())
	speculative(1, time.Hour)(qry)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		require.NoError(b, qry.Iter().Close(), "the fixture server must answer the statement")
	}
}
