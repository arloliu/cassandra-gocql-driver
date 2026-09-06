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

// prepareGateStmt is a statement that both shouldPrepare and the fixture server
// accept: shouldPrepare keys on the leading verb, and the test server's PREPARE
// handler keys on the first word after it, answering "nometadata" with a prepared
// id its EXECUTE branch also serves. A query using it therefore issues a real
// PREPARE and, once prepared, a successful EXECUTE.
const prepareGateStmt = "SELECT nometadata FROM prepare_gate"

// callerBudget is how long a caller's own context is given in these tests, and
// promptBudget is the wall-clock ceiling a caller-context return must respect.
//
// The ceiling is deliberately close to the deadline rather than merely below the
// session timeout: a ceiling of a few seconds would only prove the old behaviour is
// gone, while this one proves the caller returns on its own deadline.
const (
	callerBudget  = 200 * time.Millisecond
	promptBudget  = time.Second
	shortSessionT = 300 * time.Millisecond
)

// newPrepareGateHarness starts a fill harness whose servers park and count PREPARE
// requests.
//
// Parameters:
//   - t: the test; the gate is released on cleanup so no parked handler outlives it
//   - hosts: how many test servers to start
//   - tune: optional cluster tweaks applied before CreateSession
//
// Returns:
//   - *fillHarness: the connected harness
//   - *prepareGate: the gate, armed for nothing yet
func newPrepareGateHarness(t *testing.T, hosts int, tune func(*ClusterConfig)) (*fillHarness, *prepareGate) {
	t.Helper()

	gate := newPrepareGate()
	harness := newFillHarnessOpts(t, hosts, fillHarnessOpts{tune: tune, recvHook: gate.hook})
	t.Cleanup(gate.releaseAll)

	return harness, gate
}

// runPrepareQuery runs prepareGateStmt in the background and reports its error.
//
// Parameters:
//   - h: the harness whose session runs the query
//   - ctx: the caller's context
//   - tune: optional query tweaks
//
// Returns:
//   - <-chan error: receives the query's error exactly once
func runPrepareQuery(h *fillHarness, ctx context.Context, tune func(*Query)) <-chan error {
	result := make(chan error, 1)
	qry := h.session.Query(prepareGateStmt).WithContext(ctx)
	if tune != nil {
		tune(qry)
	}
	go func() { result <- qry.Exec() }()
	return result
}

// hostAt returns the harness host listening on ip.
//
// Returns:
//   - *HostInfo: the matching ring host
func hostAt(t *testing.T, h *fillHarness, ip string) *HostInfo {
	t.Helper()

	for _, host := range h.hosts {
		if hostIP(host) == ip {
			return host
		}
	}
	t.Fatalf("no fixture host listens on %s", ip)
	return nil
}

// TestPrepare_CallerContextBoundsTheWait proves a caller waiting on a prepare stops
// on its own deadline instead of on the session timeout.
//
// Both assertions are load-bearing. Asserting only the error class would also pass
// for an implementation that waits the full session timeout and reports
// DeadlineExceeded at the end of it, which is exactly the behaviour this change
// removes.
func TestPrepare_CallerContextBoundsTheWait(t *testing.T) {
	harness, gate := newPrepareGateHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.Timeout = 30 * time.Second
		noHeartbeat(cluster)
	})
	gate.parkStatements(prepareGateStmt)

	ctx, cancel := context.WithTimeout(context.Background(), callerBudget)
	defer cancel()

	start := time.Now()
	err := awaitQuery(t, runPrepareQuery(harness, ctx, nil))
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded, "the caller must see its own context error")
	require.Less(t, elapsed, promptBudget,
		"the caller must return on its own deadline, not on the session timeout")
}

// TestPrepare_CallerCancellationReturnsPromptly is TestPrepare_CallerContextBoundsTheWait
// for an explicit cancel rather than a deadline, so both context errors a caller can
// see are covered.
func TestPrepare_CallerCancellationReturnsPromptly(t *testing.T) {
	harness, gate := newPrepareGateHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.Timeout = 30 * time.Second
		noHeartbeat(cluster)
	})
	gate.parkStatements(prepareGateStmt)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result := runPrepareQuery(harness, ctx, nil)
	gate.awaitParked(t, "the caller's PREPARE to park server-side")

	start := time.Now()
	cancel()
	err := awaitQuery(t, result)

	require.ErrorIs(t, err, context.Canceled, "the caller must see its own context error")
	require.Less(t, time.Since(start), promptBudget, "the caller must return on its cancellation")
}

// TestPrepare_AbandonedLoadStillFeedsTheCache proves the whole point of bounding the
// waiter rather than the load: the prepare a caller gave up on still completes, so
// the next caller hits the cache instead of issuing a second PREPARE.
func TestPrepare_AbandonedLoadStillFeedsTheCache(t *testing.T) {
	harness, gate := newPrepareGateHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.Timeout = 30 * time.Second
		noHeartbeat(cluster)
	})
	gate.parkStatements(prepareGateStmt)

	ctx, cancel := context.WithTimeout(context.Background(), callerBudget)
	defer cancel()

	abandoned := runPrepareQuery(harness, ctx, nil)
	gate.awaitParked(t, "the abandoned caller's PREPARE to park server-side")
	require.ErrorIs(t, awaitQuery(t, abandoned), context.DeadlineExceeded,
		"the first caller must give up on its own deadline")

	gate.releaseAll()

	// Wait for the abandoned load itself to land, so the next caller is asserted
	// against a warm cache rather than racing the load it would otherwise join.
	cacheKey := harness.session.stmtsLRU.keyFor(harness.hosts[0].HostID(), "", prepareGateStmt)
	require.Eventually(t, func() bool {
		_, ok := harness.session.stmtsLRU.get(cacheKey)
		return ok
	}, fillEventBudget, time.Millisecond, "the abandoned load must still populate the cache")

	require.NoError(t, awaitQuery(t, runPrepareQuery(harness, context.Background(), nil)),
		"the next caller must succeed from the cache")
	require.Equal(t, 1, gate.count(prepareGateStmt),
		"the server must see exactly one PREPARE across both callers")
}

// TestPrepare_AbandonedFollowerLeavesTheSharedLoad proves a caller that merely shares
// someone else's in-flight prepare is released by its own context too.
//
// Otter parks followers in a context-free WaitGroup, so this is the case the fast
// path and the leader-side tests cannot reach. The order matters: the follower's
// return is asserted before the leader is released, so it cannot have been freed by
// the load completing.
func TestPrepare_AbandonedFollowerLeavesTheSharedLoad(t *testing.T) {
	harness, gate := newPrepareGateHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.Timeout = 30 * time.Second
		noHeartbeat(cluster)
	})
	gate.parkStatements(prepareGateStmt)

	leader := runPrepareQuery(harness, context.Background(), nil)
	gate.awaitParked(t, "the leader's PREPARE to park server-side")

	followerCtx, cancel := context.WithTimeout(context.Background(), callerBudget)
	defer cancel()

	start := time.Now()
	err := awaitQuery(t, runPrepareQuery(harness, followerCtx, nil))
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded, "the follower must see its own context error")
	require.Less(t, elapsed, promptBudget, "the follower must return on its own deadline")

	gate.releaseAll()
	require.NoError(t, awaitQuery(t, leader), "the leader's prepare must still complete")
	require.Equal(t, 1, gate.count(prepareGateStmt),
		"the shared load must issue exactly one PREPARE")
}

// TestPrepare_CallerCancellationStaysOnOneHost proves a caller's own cancellation is
// classified as a logical error: the executor returns it rather than spending the
// query's retry budget on another node.
//
// The assertion holds under a retry policy, an idempotent query and a second live
// host, so "it did not move on" can only come from the error's classification.
// It is the strict complement of TestPrepare_BackgroundTimeoutKeepsItsErrorClass.
func TestPrepare_CallerCancellationStaysOnOneHost(t *testing.T) {
	recorder := newAttemptRecorder()
	convictions := &recordingConvictionPolicy{onFailure: func(*HostInfo) bool { return false }}
	harness, gate := newPrepareGateHarness(t, 2, func(cluster *ClusterConfig) {
		cluster.Timeout = 30 * time.Second
		cluster.QueryObserver = recorder
		cluster.ConvictionPolicy = convictions
		noHeartbeat(cluster)
	})
	gate.parkStatements(prepareGateStmt)

	ctx, cancel := context.WithTimeout(context.Background(), callerBudget)
	defer cancel()

	start := time.Now()
	err := awaitQuery(t, runPrepareQuery(harness, ctx, func(qry *Query) {
		qry.Idempotent(true).RetryPolicy(&SimpleRetryPolicy{NumRetries: 2})
	}))
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded, "the caller must see its own context error")
	require.Less(t, elapsed, promptBudget, "the caller must return on its own deadline")
	require.Equal(t, 1, recorder.count(), "a cancelled caller must not spend a second attempt")
	require.Len(t, recorder.hosts(), 1, "a cancelled caller must not reach the second host")
	require.Empty(t, convictions.recorded(), "a cancelled caller must not fail a host")
}

// TestPrepare_BackgroundTimeoutKeepsItsErrorClass proves the bound put on the shared
// load reports a node timeout as a node timeout.
//
// The load runs under a context deadline, so it fails with context.DeadlineExceeded
// unless that is translated back. Without the translation this test is the only one
// that fails: otter shares the load's error with every waiter, the executor reads a
// bare context error as a caller's cancellation, and a node that really stopped
// answering is silently treated as a caller that gave up.
//
// The follower is pinned to the leader's host so it genuinely shares that load; the
// leader stays unpinned so its retry budget can reach the second host.
func TestPrepare_BackgroundTimeoutKeepsItsErrorClass(t *testing.T) {
	recorder := newAttemptRecorder()
	harness, gate := newPrepareGateHarness(t, 2, func(cluster *ClusterConfig) {
		cluster.Timeout = shortSessionT
		cluster.QueryObserver = recorder
		noHeartbeat(cluster)
	})
	gate.parkStatements(prepareGateStmt)

	leader := runPrepareQuery(harness, context.Background(), func(qry *Query) {
		qry.Idempotent(true).RetryPolicy(&SimpleRetryPolicy{NumRetries: 1})
	})
	leaderHost := hostAt(t, harness, gate.awaitParked(t, "the leader's PREPARE to park server-side"))

	follower := runPrepareQuery(harness, context.Background(), func(qry *Query) {
		qry.SetHostID(leaderHost.HostID())
	})

	require.ErrorIs(t, awaitQuery(t, follower), ErrTimeoutNoResponse,
		"a follower must see the node timeout, not a context error")
	require.ErrorIs(t, awaitQuery(t, leader), ErrTimeoutNoResponse,
		"the leader must see the node timeout, not a context error")

	require.Greater(t, len(recorder.hosts()), 1,
		"a node timeout must be classified as a host failure and reach the next host")

	// The count can only be read once the leader's host has drained. Its reader is
	// parked inside the gate, so a PREPARE the follower wrote on that same
	// connection would sit unread and uncounted: asserting before the drain would
	// pass whether or not the follower actually shared the load.
	gate.releaseAll()
	drainHost(t, harness, leaderHost)
	require.Equal(t, 2, gate.count(prepareGateStmt),
		"the follower must share the leader's load rather than issue its own PREPARE")
}

// drainHost round-trips an unprepared statement on host's connection, so every frame
// written to it earlier has been read by the time this returns.
//
// The socket is FIFO and each fixture host holds a single connection, so a request
// that answers is proof that the requests queued ahead of it were consumed.
func drainHost(t *testing.T, h *fillHarness, host *HostInfo) {
	t.Helper()

	qry := h.session.Query("void").WithContext(context.Background()).SetHostID(host.HostID())
	result := make(chan error, 1)
	go func() { result <- qry.Exec() }()
	require.NoError(t, awaitQuery(t, result), "the drain request must answer")
}

// TestPrepare_ZeroSessionTimeoutStillPrepares pins the guard that keeps a zero
// session timeout from deriving an already-expired deadline for the shared load.
//
// Without the guard the load's context is expired before the frame is written and
// every prepare fails outright, so the assertion has to be that the query succeeds.
func TestPrepare_ZeroSessionTimeoutStillPrepares(t *testing.T) {
	harness, gate := newPrepareGateHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.Timeout = 0
		noHeartbeat(cluster)
	})

	require.NoError(t, awaitQuery(t, runPrepareQuery(harness, context.Background(), nil)),
		"a zero session timeout must not make prepare fail")
	require.Equal(t, 1, gate.count(prepareGateStmt), "the statement must be prepared once")
}

// TestPrepare_AbandonedLoadStillTracesItsPrepare pins the callback lifetime the shared
// load implies: a caller that gives up still has its Tracer invoked, from the request
// that outlived it, after the query returned.
//
// The Tracer documentation states this because it cannot be avoided without giving up
// result sharing. The test exists so the behaviour is not quietly changed back.
func TestPrepare_AbandonedLoadStillTracesItsPrepare(t *testing.T) {
	harness, gate := newPrepareGateHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.Timeout = 30 * time.Second
		noHeartbeat(cluster)
	})
	gate.parkStatements(prepareGateStmt)

	tracer := &countingTracer{traced: make(chan struct{}, 1)}
	ctx, cancel := context.WithTimeout(context.Background(), callerBudget)
	defer cancel()

	abandoned := runPrepareQuery(harness, ctx, func(qry *Query) { qry.Trace(tracer) })
	gate.awaitParked(t, "the abandoned caller's PREPARE to park server-side")
	require.ErrorIs(t, awaitQuery(t, abandoned), context.DeadlineExceeded,
		"the caller must give up on its own deadline")
	require.Zero(t, tracer.count(), "the parked PREPARE cannot have been traced yet")

	gate.releaseAll()
	awaitSignal(t, tracer.traced, "the abandoned load to trace its PREPARE")
}

// countingTracer records how many times it was called and signals each call.
type countingTracer struct {
	// traced receives once per call, so a test can wait for one without polling.
	traced chan struct{}

	calls atomic.Int64
}

var _ Tracer = (*countingTracer)(nil)

// Trace records the call.
func (tr *countingTracer) Trace(_ []byte) {
	tr.calls.Add(1)
	select {
	case tr.traced <- struct{}{}:
	default:
	}
}

// count returns how many times Trace was called.
//
// Returns:
//   - int64: the number of calls
func (tr *countingTracer) count() int64 {
	return tr.calls.Load()
}
