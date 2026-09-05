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
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// logRecord is one call made to a historyLogger.
type logRecord struct {
	level  LogLevel
	msg    string
	fields []LogField
}

// historyLogger records every log call, unlike captureLogger which keeps only the last.
type historyLogger struct {
	mu      sync.Mutex
	records []logRecord
}

var _ StructuredLogger = (*historyLogger)(nil)

func (l *historyLogger) record(level LogLevel, msg string, fields ...LogField) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, logRecord{level: level, msg: msg, fields: append([]LogField(nil), fields...)})
}

// Debug records a debug-level call.
func (l *historyLogger) Debug(msg string, fields ...LogField) {
	l.record(LogLevelDebug, msg, fields...)
}

// Info records an info-level call.
func (l *historyLogger) Info(msg string, fields ...LogField) { l.record(LogLevelInfo, msg, fields...) }

// Warning records a warning-level call.
func (l *historyLogger) Warning(msg string, fields ...LogField) {
	l.record(LogLevelWarn, msg, fields...)
}

// Error records an error-level call.
func (l *historyLogger) Error(msg string, fields ...LogField) {
	l.record(LogLevelError, msg, fields...)
}

// withMessage returns every record whose message equals msg.
//
// Returns:
//   - []logRecord: the matching records in call order
func (l *historyLogger) withMessage(msg string) []logRecord {
	l.mu.Lock()
	defer l.mu.Unlock()

	var out []logRecord
	for _, r := range l.records {
		if r.msg == msg {
			out = append(out, r)
		}
	}
	return out
}

// fieldString returns the string form of the named field, or "" when absent.
//
// Returns:
//   - string: the field's String(), or "" when the record has no such field
func (r logRecord) fieldString(name string) string {
	for _, f := range r.fields {
		if f.Name == name {
			return f.Value.String()
		}
	}
	return ""
}

// awaitRefreshDone receives one refresh result or fails the test.
//
// Returns:
//   - error: the result the refresh reported
func awaitRefreshDone(t *testing.T, done <-chan error, what string) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

// TestRingRefreshFailure_IsLogged proves a ring refresh that nobody waits for
// still logs its failure: the fixture has no control connection,
// so a triggered refresh fails with errNoControl and must say so.
func TestRingRefreshFailure_IsLogged(t *testing.T) {
	logger := &historyLogger{}
	done := make(chan error, 8)
	session, _, _ := newPauseRecoverySession(t, func(cluster *ClusterConfig) {
		cluster.Logger = logger
		cluster.testRingRefreshDone = func(err error) { done <- err }
	})

	session.ringRefresher.trigger()

	err := awaitRefreshDone(t, done, "the triggered refresh to finish")
	require.ErrorIs(t, err, errNoControl, "without a control connection the refresh must fail with errNoControl")

	warnings := logger.withMessage("Ring refresh failed.")
	require.Len(t, warnings, 1, "exactly one warning for the failed refresh")
	require.Equal(t, LogLevelWarn, warnings[0].level)
	require.Equal(t, errNoControl.Error(), warnings[0].fieldString("err"))
}

// tickFixture is a pause-recovery session with the reconnect ticker disabled,
// so the test owns every tick, and with the ring refresh's start observable.
type tickFixture struct {
	session   *Session
	host      *HostInfo
	collector *hostStateCollector
	hooks     *poolHooks
	gate      *gatedDialer
	logger    *historyLogger
	entered   chan struct{}
	done      chan error
}

// newTickFixture builds the fixture and waits for the initial fill to complete.
func newTickFixture(t *testing.T, tune func(*ClusterConfig)) *tickFixture {
	t.Helper()

	f := &tickFixture{
		hooks:   newPoolHooks(),
		gate:    &gatedDialer{},
		logger:  &historyLogger{},
		entered: make(chan struct{}, 64),
		done:    make(chan error, 64),
	}
	f.session, f.host, f.collector = newPauseRecoverySession(t, func(cluster *ClusterConfig) {
		cluster.ReconnectInterval = 0
		cluster.Dialer = f.gate
		cluster.ReconnectionPolicy = &ConstantReconnectionPolicy{MaxRetries: 1, Interval: time.Millisecond}
		cluster.Logger = f.logger
		cluster.testPoolHook = f.hooks.hook
		cluster.testRingRefreshHook = func() {
			select {
			case f.entered <- struct{}{}:
			default:
			}
		}
		cluster.testRingRefreshDone = func(err error) { f.done <- err }
		if tune != nil {
			tune(cluster)
		}
	})
	t.Cleanup(f.hooks.releaseDial)
	f.hooks.await(t, poolFillDone, f.host, "the fixture host's initial fill to complete")
	return f
}

// driveDown makes the host unreachable and waits until it is DOWN, its pool is
// unregistered and the failed fill has completed, so no fill is in flight.
func (f *tickFixture) driveDown(t *testing.T) {
	t.Helper()

	pool, ok := f.session.pool.getPoolFor(f.host)
	require.True(t, ok, "the fixture host must have a pool")
	conn := pool.Pick()
	require.NotNil(t, conn, "the fixture host must have a connection to lose")

	f.gate.close()
	conn.closeWithError(errors.New("gocql: test induced connection failure"))

	awaitHost(t, f.collector.down, f.host, "the host to be convicted after the failed fill cycle")
	require.Eventually(t, func() bool {
		_, ok := f.session.pool.getPoolFor(f.host)
		return !ok
	}, 5*time.Second, 5*time.Millisecond, "the convicted host's pool must be unregistered")
	f.hooks.await(t, poolFillDone, f.host, "the failed fill to complete")
	require.Equal(t, NodeDown, f.host.State())
}

// awaitEntered waits for a refresh to start.
func (f *tickFixture) awaitEntered(t *testing.T, what string) {
	t.Helper()
	select {
	case <-f.entered:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// requireNoRefresh asserts no refresh starts within window.
func (f *tickFixture) requireNoRefresh(t *testing.T, window time.Duration, what string) {
	t.Helper()
	select {
	case <-f.entered:
		t.Fatalf("unexpected ring refresh: %s", what)
	case <-time.After(window):
	}
}

// TestReconnectTick_AllUpNoRequest: a tick on a healthy ring requests nothing.
func TestReconnectTick_AllUpNoRequest(t *testing.T) {
	f := newTickFixture(t, nil)

	f.session.reconnectDownedHostsOnce()

	f.requireNoRefresh(t, 2*ringRefreshDebounceTime, "a tick with every host UP")
}

// TestReconnectTick_HostDownRequestsBeforeDialCompletes: the refresh is requested
// before the tick's synchronous dial of the DOWN host, so an unreachable host
// cannot delay discovery.
func TestReconnectTick_HostDownRequestsBeforeDialCompletes(t *testing.T) {
	f := newTickFixture(t, nil)
	f.driveDown(t)

	f.hooks.blockDial(f.host)
	ticked := make(chan struct{})
	go func() {
		f.session.reconnectDownedHostsOnce()
		close(ticked)
	}()
	f.hooks.awaitDialBlocked(t)

	// The dial is parked; the refresh must already have been requested and started.
	f.awaitEntered(t, "the refresh while the dial is blocked")

	f.hooks.releaseDial()
	select {
	case <-ticked:
	case <-time.After(5 * time.Second):
		t.Fatal("the tick did not return after the dial was released")
	}
}

// TestReconnectTick_HostDownRequestsEveryTick: the request is per tick, not once.
func TestReconnectTick_HostDownRequestsEveryTick(t *testing.T) {
	f := newTickFixture(t, nil)
	f.driveDown(t)

	f.session.reconnectDownedHostsOnce()
	f.awaitEntered(t, "the first tick's refresh")
	require.Error(t, awaitRefreshDone(t, f.done, "the first refresh to finish"))
	f.hooks.await(t, poolFillDone, f.host, "the first tick's failed fill")

	f.session.reconnectDownedHostsOnce()
	f.awaitEntered(t, "the second tick's refresh")
	require.Error(t, awaitRefreshDone(t, f.done, "the second refresh to finish"))
}

// TestReconnectTick_DisabledControlWarnsPerTick: without a control connection every
// tick's refresh fails with errNoControl and says so, once per tick.
func TestReconnectTick_DisabledControlWarnsPerTick(t *testing.T) {
	f := newTickFixture(t, nil)
	f.driveDown(t)

	for i := range 2 {
		f.session.reconnectDownedHostsOnce()
		require.ErrorIs(t, awaitRefreshDone(t, f.done, "the refresh to finish"), errNoControl)
		f.hooks.await(t, poolFillDone, f.host, "the tick's failed fill")
		warnings := f.logger.withMessage("Ring refresh failed.")
		require.Len(t, warnings, i+1, "one warning per tick")
		require.Equal(t, errNoControl.Error(), warnings[i].fieldString("err"))
	}
}

// TestReconnectTick_ZeroIntervalStartsNoTicker: with ReconnectInterval zero no
// sweep runs, so a DOWN host produces no refresh on its own.
func TestReconnectTick_ZeroIntervalStartsNoTicker(t *testing.T) {
	f := newTickFixture(t, nil)
	f.driveDown(t)

	f.requireNoRefresh(t, 2*ringRefreshDebounceTime, "a DOWN host with the ticker disabled")
}

// TestReconnectTick_TickerRequestsRefreshWhileDown: with a real interval the
// sweep itself requests the refresh, without any help from the test.
func TestReconnectTick_TickerRequestsRefreshWhileDown(t *testing.T) {
	f := newTickFixture(t, func(cluster *ClusterConfig) {
		cluster.ReconnectInterval = 200 * time.Millisecond
	})
	f.driveDown(t)

	f.awaitEntered(t, "a refresh requested by the reconnect sweep")
}

// TestHasUnfilteredDownHost covers the trigger predicate on hand-built hosts.
func TestHasUnfilteredDownHost(t *testing.T) {
	mk := func(addr string, state nodeState) *HostInfo {
		h, err := NewHostInfoFromAddrPort(net.ParseIP(addr), 9042)
		require.NoError(t, err)
		h.setHostID(MustRandomUUID().String())
		h.setState(state)
		return h
	}
	up := mk("10.0.0.1", NodeUp)
	down := mk("10.0.0.2", NodeDown)

	accept := &Session{cfg: ClusterConfig{}}
	reject := &Session{cfg: ClusterConfig{HostFilter: HostFilterFunc(func(host *HostInfo) bool {
		return host != down
	})}}

	require.False(t, accept.hasUnfilteredDownHost(nil), "an empty ring has no DOWN host")
	require.False(t, accept.hasUnfilteredDownHost([]*HostInfo{up}), "an UP host does not trigger")
	require.True(t, accept.hasUnfilteredDownHost([]*HostInfo{up, down}), "an accepted DOWN host triggers")
	require.False(t, reject.hasUnfilteredDownHost([]*HostInfo{up, down}), "a filtered DOWN host does not trigger")
}
