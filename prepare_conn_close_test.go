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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// closedPrepareStmt is prepareGateStmt's shape under its own name,
// so the gate below parks only these statements.
const closedPrepareStmt = "SELECT nometadata FROM prepare_closed"

// TestPrepare_ClosedConnectionIsNotACallerCancellation proves a prepare on a connection
// that has already closed reports the connection closing, not a context error.
//
// The shared load runs on a context derived from the connection's own,
// so a closed connection makes the load fail with context.Canceled.
// The executor reads a bare context error as the caller giving up and never retries it,
// so a caller whose own context is alive would get a terminal cancellation for a
// connection loss its retry policy should have absorbed (soak finding F-soak-4).
func TestPrepare_ClosedConnectionIsNotACallerCancellation(t *testing.T) {
	harness, _ := newPrepareGateHarness(t, 1, noHeartbeat)
	conn := harness.pickAnyConn(t, harness.pool(t, harness.hosts[0]))
	conn.Close()

	_, err := conn.prepareStatement(context.Background(), closedPrepareStmt+" /* closed */", nil, "")

	require.ErrorIs(t, err, ErrConnectionClosed, "a closed connection must be reported as closed")
	require.NotErrorIs(t, err, context.Canceled, "a live caller must never see a context error it did not cause")
}

// isPrepare reports whether a request is a PREPARE.
func isPrepare(req frameBuilder) bool {
	_, ok := req.(*writePrepareFrame)
	return ok
}

// TestPrepare_CloseBeforeTheWaitIsNotACallerCancellation closes the connection after the
// PREPARE was written and before its caller waits, so every close arm is ready at once.
//
// The PREPARE is parked server-side, so no successful response can compete.
// The forced runs take the load context's arm deterministically (forceAbandonArm);
// the random runs let the wait pick among the connection's arm and the load context's arm,
// which with a zero session timeout are one channel, since the load context is then the
// connection's own.
func TestPrepare_CloseBeforeTheWaitIsNotACallerCancellation(t *testing.T) {
	for _, timeout := range []time.Duration{30 * time.Second, 0} {
		for i := range 21 {
			forced := i == 0
			t.Run(fmt.Sprintf("timeout=%s/forced=%v/%d", timeout, forced, i), func(t *testing.T) {
				var target atomic.Pointer[Conn]
				var armed atomic.Bool
				armed.Store(true)
				hooks := &connTestHooks{beforeCallerRecv: func(req frameBuilder) {
					if isPrepare(req) && armed.CompareAndSwap(true, false) {
						target.Load().Close()
					}
				}}
				if forced {
					hooks.forceAbandonArm = isPrepare
				}
				gate := newPrepareGate()
				harness := newFillHarnessOpts(t, 1, fillHarnessOpts{recvHook: gate.hook, hooks: hooks, tune: func(cluster *ClusterConfig) {
					cluster.Timeout = timeout
					noHeartbeat(cluster)
				}})
				t.Cleanup(gate.releaseAll)
				gate.parkStatements(closedPrepareStmt)
				conn := harness.pickAnyConn(t, harness.pool(t, harness.hosts[0]))
				target.Store(conn)

				_, err := conn.prepareStatement(context.Background(), closedPrepareStmt, nil, "")

				require.NotErrorIs(t, err, context.Canceled,
					"a connection closing under the load must not reach a live caller as a cancellation")
				require.ErrorIs(t, err, ErrConnectionClosed)
			})
		}
	}
}

// TestPrepare_CloseNotificationBeforeCancelIsNotACallerCancellation delivers the close
// to the waiting PREPARE before the connection's context is cancelled.
//
// closeWithError hands its error to every outstanding call first and cancels the
// connection's context afterwards. A custom transport whose reader fails with
// context.Canceled therefore reaches the load as that error while the connection's
// context is still alive, so the translation must also key on the connection being closed.
// The cancel is held until the result is in, and the test checks it was, so the
// translation cannot pass by seeing the cancellation instead.
func TestPrepare_CloseNotificationBeforeCancelIsNotACallerCancellation(t *testing.T) {
	holdCancel := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(holdCancel) }) }
	// A defer, not t.Cleanup: cleanups run after this function returns, in reverse order,
	// so the harness's session.Close would run first and block in the held closer
	// whenever an assertion below fails before the explicit release.
	defer release()
	hooks := &connTestHooks{closerBeforeCancel: func() { <-holdCancel }}
	gate := newPrepareGate()
	harness := newFillHarnessOpts(t, 1, fillHarnessOpts{recvHook: gate.hook, hooks: hooks, tune: func(cluster *ClusterConfig) {
		cluster.Timeout = 30 * time.Second
		noHeartbeat(cluster)
	}})
	t.Cleanup(gate.releaseAll)
	gate.parkStatements(closedPrepareStmt)
	conn := harness.pickAnyConn(t, harness.pool(t, harness.hosts[0]))

	result := make(chan error, 1)
	go func() {
		_, err := conn.prepareStatement(context.Background(), closedPrepareStmt, nil, "")
		result <- err
	}()
	gate.awaitParked(t, "the PREPARE to park server-side")
	closed := make(chan struct{})
	go func() {
		conn.closeWithError(context.Canceled)
		close(closed)
	}()

	err := awaitQuery(t, result)
	require.NoError(t, conn.ctx.Err(), "the connection's context must still be alive when the load returns")
	release()
	<-closed
	require.NotErrorIs(t, err, context.Canceled,
		"a close notification carrying context.Canceled must not reach a live caller as its own cancellation")
	require.ErrorIs(t, err, ErrConnectionClosed)
}

// TestPrepare_ClosedConnectionRetriesOnTheNextHost is the executor-level consequence:
// an idempotent query with a retry policy whose attempt lands on a closed connection
// must move to the next host instead of returning a cancellation.
//
// Conn.Close leaves the connection registered in its pool, which is the window between
// a close and the pool dropping the connection, so round robin reaches it deterministically.
func TestPrepare_ClosedConnectionRetriesOnTheNextHost(t *testing.T) {
	recorder := newAttemptRecorder()
	harness := newFillHarnessOpts(t, 2, fillHarnessOpts{tune: func(cluster *ClusterConfig) {
		cluster.Timeout = 30 * time.Second
		cluster.QueryObserver = recorder
		noHeartbeat(cluster)
	}})
	closedHost := harness.hosts[0]
	harness.pickAnyConn(t, harness.pool(t, closedHost)).Close()

	for i := range 4 {
		qry := harness.session.Query(fmt.Sprintf("%s /* %d */", closedPrepareStmt, i)).
			Idempotent(true).RetryPolicy(&SimpleRetryPolicy{NumRetries: 1})
		result := make(chan error, 1)
		go func() { result <- qry.Exec() }()
		require.NoError(t, awaitQuery(t, result), "query %d must succeed on the open host", i)
	}
	require.Positive(t, recorder.hosts()[closedHost], "round robin must have tried the closed connection")
}
