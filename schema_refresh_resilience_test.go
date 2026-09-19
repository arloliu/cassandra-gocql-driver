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
