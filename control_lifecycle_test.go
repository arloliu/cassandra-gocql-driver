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

// lifecycleBudget bounds every positive wait in these tests.
const lifecycleBudget = 10 * time.Second

// closeBudget bounds Session.Close in these tests: the plan's acceptance for the
// schema-refresh deadlock is a Close that returns within five seconds.
const closeBudget = 5 * time.Second

// schemaFixture is a snapshotFixture whose session caches keyspace metadata,
// so a schema refresh issues control queries, with the schema refresher's runs
// and the control reconnects observable.
//
// The schema debounce is set to an hour: a debounced refresh only runs when the
// test expires the timer through expireDebounce, so "the refresh has not run yet"
// is a deterministic statement and not a race against the clock.
type schemaFixture struct {
	*snapshotFixture

	schemaEntered  chan struct{}
	schemaDone     chan error
	reconnectDone  chan struct{}
	schemaDebounce time.Duration
}

// newSchemaFixture builds the fixture; tune may adjust the config further.
func newSchemaFixture(t *testing.T, tune func(*ClusterConfig)) *schemaFixture {
	t.Helper()

	f := &schemaFixture{
		schemaEntered:  make(chan struct{}, 64),
		schemaDone:     make(chan error, 64),
		reconnectDone:  make(chan struct{}, 64),
		schemaDebounce: time.Hour,
	}
	f.snapshotFixture = newSnapshotFixture(t, func(cluster *ClusterConfig) {
		cluster.Metadata.CacheMode = KeyspaceOnly
		cluster.schemaRefreshDebounce = f.schemaDebounce
		cluster.testSchemaRefreshHook = func() {
			select {
			case f.schemaEntered <- struct{}{}:
			default:
			}
		}
		cluster.testSchemaRefreshDone = func(err error) { f.schemaDone <- err }
		cluster.testControlReconnectDone = func() {
			select {
			case f.reconnectDone <- struct{}{}:
			default:
			}
		}
		if tune != nil {
			tune(cluster)
		}
	})
	f.drainSchema()
	return f
}

// drainSchema discards queued schema refresh and reconnect events, such as the
// ones session initialization produced.
func (f *schemaFixture) drainSchema() {
	for {
		select {
		case <-f.schemaEntered:
		case <-f.schemaDone:
		case <-f.reconnectDone:
		default:
			return
		}
	}
}

// awaitSchemaEntered waits for a schema refresh to start.
func (f *schemaFixture) awaitSchemaEntered(t *testing.T, what string) {
	t.Helper()
	select {
	case <-f.schemaEntered:
	case <-time.After(lifecycleBudget):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// awaitSchemaDone returns the next schema refresh result.
func (f *schemaFixture) awaitSchemaDone(t *testing.T, what string) error {
	t.Helper()
	select {
	case err := <-f.schemaDone:
		return err
	case <-time.After(lifecycleBudget):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

// awaitReconnectDone waits for a control reconnect to return.
func (f *schemaFixture) awaitReconnectDone(t *testing.T, what string) {
	t.Helper()
	select {
	case <-f.reconnectDone:
	case <-time.After(lifecycleBudget):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// requireNoSchemaRefresh asserts no schema refresh has started.
//
// With the hour-long debounce this is deterministic: the only way a debounced
// refresh runs is through expireDebounce.
func (f *schemaFixture) requireNoSchemaRefresh(t *testing.T, msg string) {
	t.Helper()
	select {
	case <-f.schemaEntered:
		t.Fatal(msg)
	default:
	}
}

// expireDebounce fires the debouncer's timer now, as if the interval had elapsed.
//
// Returns:
//   - bool: whether a debounced refresh was pending, i.e. the timer was armed
func expireDebounce(d *refreshDebouncer) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.stopped {
		return false
	}
	return d.timer.Reset(0)
}

// requireClosesWithinBudget asserts session.Close returns inside closeBudget.
func requireClosesWithinBudget(t *testing.T, session *Session) {
	t.Helper()
	closed := make(chan struct{})
	go func() {
		session.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(closeBudget):
		t.Fatalf("Session.Close did not return within %v", closeBudget)
	}
}

// TestControlConnReconnectDuringSchemaRefreshDoesNotDeadlock drives the schema
// side of the reconnect deadlock.
//
// The schema flusher's own control query fails on write, closeWithError calls
// HandleError synchronously, and reconnect runs on the flusher's goroutine.
// setupConn used to wait for the schema flusher from there, i.e. for itself:
// reconnect never returned, its reconnecting claim was never released, so every
// later reconnect short-circuited, and Session.Close hung in schemaRefresher.stop.
// Now setupConn debounces the refresh and returns; the reconnect completes, the
// claim is released, the debounced refresh runs when its timer fires, and Close
// returns.
func TestControlConnReconnectDuringSchemaRefreshDoesNotDeadlock(t *testing.T) {
	f := newSchemaFixture(t, nil)
	fc := controlFaultConn(t, f.session)
	before := f.session.control.getConn()

	// The keyspaces statement was prepared during initialization; forgetting it
	// makes the refresh prepare it again, on a write the dialer can name.
	f.session.stmtsLRU.clear()
	f.dialer.setFailingStatement("system_schema.keyspaces")
	f.dialer.arm(fc)

	f.session.schemaDescriber.schemaRefresher.trigger()
	f.awaitSchemaEntered(t, "the schema refresh whose keyspaces write fails")

	f.awaitReconnectDone(t, "the reconnect the failed write started on the schema flusher")
	require.Zero(t, atomic.LoadInt32(&f.session.control.reconnecting), "the reconnecting claim must be released")
	require.NotSame(t, before, f.session.control.getConn(), "the write failure must have reconnected the control connection")

	failures := f.dialer.recordedFailures()
	require.Len(t, failures, 1)
	require.Same(t, fc, failures[0].conn, "the failed write must be on the armed connection")
	require.Contains(t, string(failures[0].statement), "system_schema.keyspaces")

	// The interrupted refresh finishes on its own, then the refresh the reconnect
	// requested runs when its debounce expires.
	f.awaitSchemaDone(t, "the interrupted refresh's result")
	require.True(t, expireDebounce(f.session.schemaDescriber.schemaRefresher),
		"the reconnect must have left a schema refresh pending")
	f.awaitSchemaEntered(t, "the debounced schema refresh")
	require.NoError(t, f.awaitSchemaDone(t, "the debounced schema refresh's result"),
		"the refresh on the reconnected control connection must succeed")

	requireClosesWithinBudget(t, f.session)
}

// TestSchemaRefreshAfterReconnectIsDebounced pins the new shape of the
// post-reconnect schema refresh: setupConn requests it and returns, and the
// refresh runs when the debounce expires.
func TestSchemaRefreshAfterReconnectIsDebounced(t *testing.T) {
	f := newSchemaFixture(t, nil)
	before := f.session.control.getConn()

	f.session.control.reconnect()
	require.NotSame(t, before, f.session.control.getConn(), "reconnect must have replaced the control connection")
	f.requireNoSchemaRefresh(t, "setupConn must not run the schema refresh before returning")

	require.True(t, expireDebounce(f.session.schemaDescriber.schemaRefresher),
		"the reconnect must have left a schema refresh pending")
	f.awaitSchemaEntered(t, "the debounced schema refresh")
	require.NoError(t, f.awaitSchemaDone(t, "the debounced schema refresh's result"))
}

// TestSchemaRefreshFailureLogsWarning proves a schema refresh that nobody waits
// for still logs its failure: the node fails the keyspace metadata fetch, the
// refresh fails inside the flusher, and the warning is the only trace.
func TestSchemaRefreshFailureLogsWarning(t *testing.T) {
	f := newSchemaFixture(t, nil)
	const warning = "Schema refresh failed. " +
		"Schema might be stale or missing, causing token-aware routing to fall back to the configured fallback policy. " +
		"Keyspace metadata queries might fail with ErrKeyspaceDoesNotExist until schema refresh succeeds."
	require.Empty(t, f.logger.withMessage(warning), "a successful refresh must not warn")

	f.script.failExecutes.Store(true)
	f.session.schemaDescriber.schemaRefresher.trigger()
	err := f.awaitSchemaDone(t, "the failed schema refresh's result")
	require.ErrorContains(t, err, "scripted execute failure")

	records := f.logger.withMessage(warning)
	require.Len(t, records, 1, "the failure must be logged once")
	require.Equal(t, LogLevelWarn, records[0].level)
	require.Contains(t, records[0].fieldString("err"), "scripted execute failure")
}
