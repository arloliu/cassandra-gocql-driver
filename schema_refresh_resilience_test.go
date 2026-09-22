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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// schemaRefreshRoundBudget bounds the wait for one requested refresh round to resolve.
const schemaRefreshRoundBudget = 5 * time.Second

// TestRunSchemaRefresh_PanicIsAFailedRound pins the schema half of the refresh-resilience guarantee.
//
// refreshSchemas runs schema listeners and policy callbacks on the schema refresher's flusher.
// A panic out of the refresh function reaches that flusher's recover-and-stop, which is terminal:
// every later schema refresh fails fast, so the session's schema is frozen for the rest of its life.
// runSchemaRefresh turns the panic into a failed round instead, as runRingRefresh does for the ring.
//
// The guarantee is narrow.
// It is that the flusher survives and the next round runs,
// not that the notifications the panicking round skipped are made again.
//
// Each case drives a real refreshDebouncer, since the flusher's fate is what is asserted.
// The metadata cache is Disabled so that a round which does not panic succeeds:
// refreshSchemas then returns before it needs the control connection this bare session lacks.
// The per-round assertions do not stop the test,
// so a flusher that died is reported by the round count at the end and not by an earlier message.
func TestRunSchemaRefresh_PanicIsAFailedRound(t *testing.T) {
	t.Run("the refresh panics", func(t *testing.T) {
		var entries atomic.Int32
		session, refresher := newSchemaRefreshFixture(t, &defaultLogger{})
		session.cfg.testSchemaRefreshHook = func() {
			if entries.Add(1) == 1 {
				panic("scripted panic from the schema refresh")
			}
		}

		first := schemaRefreshRound(t, refresher)
		second := schemaRefreshRound(t, refresher)

		assert.ErrorContains(t, first, "schema refresh panicked", "the panic must reach the caller as the round's error")
		assert.NoError(t, second, "the next round must succeed")
		require.EqualValues(t, 2, entries.Load(), "expected 2 rounds, actual %d", entries.Load())
	})

	t.Run("the logger panics while reporting the failure", func(t *testing.T) {
		var entries atomic.Int32
		var lastDone atomic.Pointer[error]
		logger := &panicOnWarningLogger{StructuredLogger: &defaultLogger{}}
		session, refresher := newSchemaRefreshFixture(t, logger)
		session.cfg.testSchemaRefreshHook = func() {
			if entries.Add(1) == 1 {
				panic("scripted panic from the schema refresh")
			}
		}
		session.cfg.testSchemaRefreshDone = func(err error) { lastDone.Store(&err) }
		logger.armed.Store(true)

		first := schemaRefreshRound(t, refresher)
		reported := lastDone.Load()
		second := schemaRefreshRound(t, refresher)

		assert.False(t, logger.armed.Load(), "the failure must have been reported through the panicking logger")
		assert.ErrorContains(t, first, "schema refresh panicked", "the failure must still reach the caller")
		if assert.NotNil(t, reported, "the completion hook must still run") {
			assert.Error(t, *reported, "the completion hook must still see the failure")
		}
		assert.NoError(t, second, "the next round must succeed")
		require.EqualValues(t, 2, entries.Load(), "expected 2 rounds, actual %d", entries.Load())
	})

	t.Run("the completion hook panics", func(t *testing.T) {
		var completions atomic.Int32
		session, refresher := newSchemaRefreshFixture(t, &defaultLogger{})
		session.cfg.testSchemaRefreshDone = func(error) {
			if completions.Add(1) == 1 {
				panic("scripted panic from the completion hook")
			}
		}

		first := schemaRefreshRound(t, refresher)
		second := schemaRefreshRound(t, refresher)

		assert.NoError(t, first, "a panicking completion hook must not fail the round")
		assert.NoError(t, second, "the next round must succeed")
		require.EqualValues(t, 2, completions.Load(), "expected 2 rounds, actual %d", completions.Load())
	})
}

// newSchemaRefreshFixture builds a session that can run schema refresh rounds with no server.
//
// Parameters:
//   - t: the test, which stops the refresher when it ends
//   - logger: the session's logger
//
// Returns:
//   - *Session: a bare session with the metadata cache Disabled; set its test hooks before the first round
//   - *refreshDebouncer: a running refresher whose refresh function is the session's runSchemaRefresh
func newSchemaRefreshFixture(t *testing.T, logger StructuredLogger) (*Session, *refreshDebouncer) {
	t.Helper()

	session := &Session{logger: logger}
	session.cfg.Metadata.CacheMode = Disabled

	refresher := newRefreshDebouncer(time.Hour, session.runSchemaRefresh, logger)
	t.Cleanup(refresher.stop)
	return session, refresher
}

// schemaRefreshRound requests one refresh round and waits for it to resolve.
//
// A refresher whose flusher has died resolves the request at once with its stopped error,
// so this returns for a dead flusher too and the caller's round count decides.
//
// Parameters:
//   - t: the test, failed if the round does not resolve within schemaRefreshRoundBudget
//   - refresher: the refresher to ask
//
// Returns:
//   - error: the round's result
func schemaRefreshRound(t *testing.T, refresher *refreshDebouncer) error {
	t.Helper()

	select {
	case err := <-refresher.refreshNow():
		return err
	case <-time.After(schemaRefreshRoundBudget):
		t.Fatalf("a requested schema refresh round did not resolve within %v", schemaRefreshRoundBudget)
		return nil
	}
}

// TestRunSchemaRefresh_RecordsTheOutcomeBeforeReporting pins the schema debt's writer:
// runSchemaRefresh marks a round running before anything can observe it,
// and records its outcome before any application code in its deferred function runs.
//
// Every observation is taken from inside a hook the round itself calls,
// so the order is decided on the flusher's goroutine and not by a race against the test.
// The panic is only the injection that reaches the recovery log and the panic value's methods;
// the ordinary-error path through the production writer is pinned by the schema refresh retry tests.
// The assertions do not stop the test, so a mutation that breaks several orderings reports each of them.
func TestRunSchemaRefresh_RecordsTheOutcomeBeforeReporting(t *testing.T) {
	var atHook, atLog, atDone debtProbe
	logger := &errorObservingLogger{StructuredLogger: &defaultLogger{}}
	session, refresher := newSchemaRefreshFixture(t, logger)
	debt := &session.schemaDebt
	logger.onError = func() { atLog.observe(debt) }

	// Each injected panic fires in one round only: the hook takes it.
	var panicWith atomic.Pointer[scriptedPanic]
	session.cfg.testSchemaRefreshHook = func() {
		atHook.observe(debt)
		if p := panicWith.Swap(nil); p != nil {
			panic(p.value)
		}
	}
	session.cfg.testSchemaRefreshDone = func(error) { atDone.observe(debt) }

	// 1. A failed round: running inside the round, owed before the recovery log and the done hook.
	panicWith.Store(&scriptedPanic{value: "scripted panic from the schema refresh"})
	assert.ErrorContains(t, schemaRefreshRound(t, refresher), "schema refresh panicked")
	hook, log, done := atHook.take(), atLog.take(), atDone.take()
	assert.True(t, hook.running, "testSchemaRefreshHook saw running == false")
	assert.True(t, log.owed, "the recovery log saw owed == false")
	assert.EqualValues(t, 1, log.failGen, "the recovery log saw failGen %d, want 1", log.failGen)
	assert.True(t, done.owed, "testSchemaRefreshDone saw owed == false")
	assert.EqualValues(t, 1, done.failGen, "testSchemaRefreshDone saw failGen %d, want 1", done.failGen)

	// 2. A success while owed discharges the debt.
	assert.NoError(t, schemaRefreshRound(t, refresher))
	done = atDone.take()
	assert.False(t, done.owed, "testSchemaRefreshDone saw owed == true after a success")
	assert.False(t, done.running, "testSchemaRefreshDone saw running == true after a success")
	assert.EqualValues(t, 1, done.okGen, "testSchemaRefreshDone saw okGen %d after the discharge, want 1", done.okGen)

	// 3. okGen counts discharges, not successes.
	assert.NoError(t, schemaRefreshRound(t, refresher))
	done = atDone.take()
	assert.EqualValues(t, 1, done.okGen, "a second success advanced okGen to %d, want 1", done.okGen)

	// 4. Formatting the panic value runs its methods, which are application code too.
	value := &debtStringer{debt: debt}
	panicWith.Store(&scriptedPanic{value: value})
	assert.ErrorContains(t, schemaRefreshRound(t, refresher), "schema refresh panicked")
	if assert.NotNil(t, value.first, "the panic value's String was never called") {
		assert.True(t, value.first.owed, "the panic value's String saw owed == false")
		assert.EqualValues(t, 2, value.first.failGen, "the panic value's String saw failGen %d, want 2", value.first.failGen)
		assert.False(t, value.first.running, "the panic value's String saw running == true")
	}
}

// scriptedPanic carries the value a hook is to panic with.
type scriptedPanic struct {
	value any
}

// debtProbe keeps the last snapshot of a schema debt taken from inside a hook.
//
// The hooks run on the flusher's goroutine and the test reads the probe after the round resolves,
// so the mutex is what makes the hand-over visible to the race detector.
type debtProbe struct {
	mu   sync.Mutex
	last schemaDebtSnapshot
}

// observe snapshots the debt.
//
// Parameters:
//   - debt: the debt to read
func (p *debtProbe) observe(debt *schemaRefreshDebt) {
	snap := debt.snapshot()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.last = snap
}

// take returns the last snapshot observed.
//
// Returns:
//   - schemaDebtSnapshot: the snapshot, or the zero value if nothing was observed
func (p *debtProbe) take() schemaDebtSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.last
}

// errorObservingLogger calls onError before every Error it forwards.
//
// handleRecoveredPanic reports through Error, so this is the recovery log's view of the round.
type errorObservingLogger struct {
	StructuredLogger
	onError func()
}

// Error runs onError and forwards the call.
func (l *errorObservingLogger) Error(msg string, fields ...LogField) {
	if l.onError != nil {
		l.onError()
	}
	l.StructuredLogger.Error(msg, fields...)
}

// debtStringer is a panic value whose String method snapshots the debt on its first call only.
//
// The round formats the value more than once;
// the first call is the one that decides whether any of them ran before the record.
type debtStringer struct {
	debt  *schemaRefreshDebt
	once  sync.Once
	first *schemaDebtSnapshot
}

// String snapshots the debt the first time it is called.
//
// Returns:
//   - string: a fixed description of the scripted panic
func (v *debtStringer) String() string {
	v.once.Do(func() {
		snap := v.debt.snapshot()
		v.first = &snap
	})
	return "scripted panic value that reads the schema debt"
}
