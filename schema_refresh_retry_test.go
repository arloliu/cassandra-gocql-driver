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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// initSchemaWarning is the warning init logs when its schema refresh fails.
const initSchemaWarning = "Failed to initialize schema metadata. " +
	"Token-aware routing will fall back to the configured fallback policy. " +
	"Attempts to retrieve keyspace metadata will fail with ErrKeyspaceDoesNotExist until schema refresh succeeds."

// schemaRound is one schema refresh round's result, with the debt as the round's done hook saw it.
type schemaRound struct {
	err error
	// debt is meaningful only when observed is true:
	// the rounds that run before the test holds the session, init's among them, cannot read it.
	debt     schemaDebtSnapshot
	observed bool
}

// schemaRoundObserver reports every schema round's result together with the debt its done hook saw.
type schemaRoundObserver struct {
	session atomic.Pointer[Session]
	rounds  chan schemaRound
}

// newSchemaRoundObserver builds an observer with room for every round a test runs.
//
// Returns:
//   - *schemaRoundObserver: an observer with no session attached yet
func newSchemaRoundObserver() *schemaRoundObserver {
	return &schemaRoundObserver{rounds: make(chan schemaRound, 64)}
}

// wrap chains the observer in front of the config's existing done hook.
//
// Parameters:
//   - cluster: the config whose testSchemaRefreshDone is wrapped
func (o *schemaRoundObserver) wrap(cluster *ClusterConfig) {
	prev := cluster.testSchemaRefreshDone
	cluster.testSchemaRefreshDone = func(err error) {
		round := schemaRound{err: err}
		if s := o.session.Load(); s != nil {
			round.debt = s.schemaDebt.snapshot()
			round.observed = true
		}
		o.rounds <- round
		if prev != nil {
			prev(err)
		}
	}
}

// attach hands the observer the session and discards the rounds that ran before it.
//
// Parameters:
//   - s: the session whose debt the done hook reads from now on
func (o *schemaRoundObserver) attach(s *Session) {
	o.session.Store(s)
	for {
		select {
		case <-o.rounds:
		default:
			return
		}
	}
}

// await returns the next round.
//
// Parameters:
//   - t: the test, failed if no round finishes within lifecycleBudget
//   - what: what the test is waiting for, for the failure message
//
// Returns:
//   - schemaRound: the round's result
func (o *schemaRoundObserver) await(t *testing.T, what string) schemaRound {
	t.Helper()
	select {
	case round := <-o.rounds:
		return round
	case <-time.After(lifecycleBudget):
		t.Fatalf("timed out waiting for %s", what)
		return schemaRound{}
	}
}

// TestSchemaRefresh_FailedRoundIsRetried pins F-schema-1's flusher half on the production path.
//
// A schema round that fails with an ordinary error has consumed its request,
// and the schema refresher has no periodic request to fall back on.
// Nothing but the host scheduler serving the debt the round recorded asks for another round,
// so the next round below is the retry.
// The node fails every EXECUTE, which fails fetchAllSchema while the control connection stays usable:
// the ledger's mechanism, with no panic and no hook involved.
//
// Promoted from _repro/zz_repro_round4_test.go's TestRepro_FailedRefreshLeavesNoRetryObligation,
// which measures the bare refreshDebouncer and stays reproduced by design.
// The debt checks inside the done hooks do not stop the test, so a round that never comes is reported by its timeout.
func TestSchemaRefresh_FailedRoundIsRetried(t *testing.T) {
	observer := newSchemaRoundObserver()
	f := newSchemaFixture(t, observer.wrap)
	observer.attach(f.session)

	f.script.failExecutes.Store(true)
	f.session.schemaDescriber.schemaRefresher.trigger()
	failed := observer.await(t, "the failed schema refresh")
	require.ErrorContains(t, failed.err, "scripted execute failure")
	assert.True(t, failed.observed && failed.debt.owed, "the failed round's done hook saw no debt owed")

	f.script.failExecutes.Store(false)
	retried := observer.await(t, "the retried schema refresh")
	require.NoError(t, retried.err, "the retried round must succeed")
	assert.True(t, retried.observed && !retried.debt.owed, "the retried round's done hook saw the debt still owed")
}

// TestSchemaRefresh_RepeatedFailuresAreRetried pins that the session's own scheduler keeps serving repeated failures.
//
// It asserts reachability only.
// The record and its nudge precede the done hook,
// so the gap between one round's done hook and the next round's start can be shorter than the step on correct code;
// the rhythm is pinned on the fake clock.
func TestSchemaRefresh_RepeatedFailuresAreRetried(t *testing.T) {
	observer := newSchemaRoundObserver()
	f := newSchemaFixture(t, observer.wrap)
	observer.attach(f.session)

	f.script.failExecutes.Store(true)
	f.session.schemaDescriber.schemaRefresher.trigger()
	for i, what := range []string{"the failed schema refresh", "the first retry", "the second retry"} {
		round := observer.await(t, what)
		require.ErrorContains(t, round.err, "scripted execute failure", "round %d must fail while EXECUTE fails", i)
		assert.True(t, round.observed && round.debt.owed, "round %d's done hook saw no debt owed", i)
	}

	f.script.failExecutes.Store(false)
	discharged := observer.await(t, "the retry that discharges the debt")
	require.NoError(t, discharged.err, "the retry after the node recovered must succeed")
	assert.True(t, discharged.observed && !discharged.debt.owed, "the discharging round's done hook saw the debt still owed")
}

// TestNewSession_FailedInitialSchemaRefreshIsRetried pins F-schema-1's init half.
//
// init runs the first schema round itself and on failure only logs.
// For a stable schema, with no later schema event and no control reconnect,
// the session would otherwise run for its whole life with no replica maps.
// The round after the failure is asserted to be one init did not request,
// and one no control reconnect requested: the control connection is still the one init published.
func TestNewSession_FailedInitialSchemaRefreshIsRetried(t *testing.T) {
	script, srv, _ := startLocalHostServer(t)
	script.failExecutes.Store(true)

	logger := &historyLogger{}
	var entered atomic.Int32
	observer := newSchemaRoundObserver()
	cluster := newLocalHostCluster(t, "", srv.Address, func(cluster *ClusterConfig, _ string) {
		cluster.Logger = logger
		cluster.Metadata.CacheMode = KeyspaceOnly
		cluster.schemaRefreshDebounce = time.Hour
		cluster.testSchemaRefreshHook = func() { entered.Add(1) }
		observer.wrap(cluster)
	})
	session, err := cluster.CreateSession()
	require.NoError(t, err, "CreateSession must succeed although its schema refresh failed")
	t.Cleanup(session.Close)
	published := session.control.getConn()

	initRound := observer.await(t, "init's schema refresh")
	require.ErrorContains(t, initRound.err, "scripted execute failure")
	require.Len(t, logger.withMessage(initSchemaWarning), 1, "init must log its failed schema refresh once")

	script.failExecutes.Store(false)
	retried := observer.await(t, "the retried schema refresh")
	require.NoError(t, retried.err, "the retried round must succeed")
	require.EqualValues(t, 2, entered.Load(), "the retry must be the second round")
	require.Same(t, published, session.control.getConn(), "no control reconnect may have requested the retry")
}

// TestSchemaRetry_NoRequestAgainstARoundTheWriterMarkedRunning pins the production running write against the phase.
//
// The real runSchemaRefresh, on a real refresher, is driven together with a fake-clock scheduler
// whose requests are counted and whose loop never runs.
// Round 2 is held inside its hook, after the writer has published running,
// and the scheduler is served synchronously inside that hold, with its retry already due.
// Nothing here depends on timing: the serve happens strictly inside the hold, and the count is read before the release.
//
// The reconnect interval is zero, so the reconnect phase does not exist
// and the bare session's missing ring and pool are never read; the schema phase is unaffected.
func TestSchemaRetry_NoRequestAgainstARoundTheWriterMarkedRunning(t *testing.T) {
	session, refresher := newSchemaRefreshFixture(t, &defaultLogger{})
	entered := make(chan struct{}, 1)
	gate := make(chan struct{})
	var rounds atomic.Int32
	session.cfg.testSchemaRefreshHook = func() {
		switch rounds.Add(1) {
		case 1:
			panic("scripted failure of round 1")
		case 2:
			entered <- struct{}{}
			<-gate
		}
	}

	clock := newFakeSchedulerClock()
	w := newTestScheduler(session, clock, 0)
	var requests atomic.Int32
	w.requestSchemaRefresh = func() { requests.Add(1) }

	// 1. Round 1 fails, and the phase arms one base step.
	require.ErrorContains(t, schemaRefreshRound(t, refresher), "schema refresh panicked")
	w.serve()
	require.Equal(t, clock.now().Add(time.Second), w.schemaDeadline, "the failure must arm one base step")

	// 2. Round 2 enters and is held.
	result := refresher.refreshNow()
	released := false
	release := func() {
		if !released {
			released = true
			close(gate)
		}
	}
	t.Cleanup(release)
	select {
	case <-entered:
	case <-time.After(schemaRefreshRoundBudget):
		t.Fatal("round 2 never entered")
	}

	// 3. The retry is due while round 2 is held.
	clock.advance(2 * time.Second)
	w.serve()
	held := requests.Load()
	heldDeadline := w.schemaDeadline
	release()
	require.EqualValues(t, 0, held, "requests %d, want 0 while round 2 is held", held)
	require.True(t, heldDeadline.IsZero(), "schema deadline %v while round 2 is held, want none", heldDeadline)

	// 4. Round 2 discharges the debt, and the phase asks for nothing more.
	select {
	case err := <-result:
		require.NoError(t, err, "round 2 must succeed")
	case <-time.After(schemaRefreshRoundBudget):
		t.Fatal("round 2 never resolved")
	}
	w.serve()
	require.EqualValues(t, 0, requests.Load(), "requests %d after the discharge, want 0", requests.Load())
	require.True(t, w.schemaDeadline.IsZero(), "schema deadline %v after the discharge, want none", w.schemaDeadline)
}
