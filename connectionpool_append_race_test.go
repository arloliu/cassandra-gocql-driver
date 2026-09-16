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
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A connection is already serving when connect appends it,
// so it can die and reach HandleError before it is in pool.conns.
// HandleError then finds nothing,
// and before the fix connect appended the dead connection:
// it counted toward the pool's size, so no fill was ever due again,
// the host stayed UP, and every query that picked it failed.
//
// These tests pin both orderings of that death against connect's append check and
// assert the removal is accounted for either way: the dead connection never enters the
// pool, and a successor cycle replaces it without the host ever being convicted.

// appendRaceDialer hands the test the socket of the dial it is armed for.
type appendRaceDialer struct {
	inner  HostDialer
	armed  atomic.Bool
	dialed chan net.Conn
}

var _ HostDialer = (*appendRaceDialer)(nil)

// DialHost dials for real and, once armed, publishes that one socket.
//
// Returns:
//   - *DialedHost: the dialed connection
//   - error: the underlying dial error
func (d *appendRaceDialer) DialHost(ctx context.Context, host *HostInfo) (*DialedHost, error) {
	dialed, err := d.inner.DialHost(ctx, host)
	if err == nil && d.armed.CompareAndSwap(true, false) {
		d.dialed <- dialed.Conn
	}
	return dialed, err
}

// appendRaceFixture is a fill harness whose pool is one connection short, with the
// dialer armed for the connection the next cycle establishes.
type appendRaceFixture struct {
	harness *fillHarness
	dialer  *appendRaceDialer
	host    *HostInfo
	pool    *hostConnPool
}

// newAppendRaceFixture builds the fixture.
//
// The detached connection is closed before the dialer is armed, so its teardown can
// neither take the dialer's socket nor reach a checkpoint the test is waiting for.
//
// Parameters:
//   - numConns: the pool size
//   - opts: harness options; the dialer is installed over whatever tune sets
//
// Returns:
//   - *appendRaceFixture: the fixture, with the dialer armed
func newAppendRaceFixture(t *testing.T, numConns int, opts fillHarnessOpts) *appendRaceFixture {
	t.Helper()

	dialer := &appendRaceDialer{
		inner:  &defaultHostDialer{dialer: &net.Dialer{}},
		dialed: make(chan net.Conn, 1),
	}
	tune := opts.tune
	opts.tune = func(c *ClusterConfig) {
		c.NumConns = numConns
		if tune != nil {
			tune(c)
		}
		c.HostDialer = dialer
	}
	harness := newFillHarnessOpts(t, 1, opts)
	host := harness.hosts[0]
	pool := harness.pool(t, host)

	detachPoolConn(t, pool).Close()
	dialer.armed.Store(true)

	return &appendRaceFixture{harness: harness, dialer: dialer, host: host, pool: pool}
}

// killBeforeAppend makes the armed connection die before connect appends it.
//
// It runs inside connect, on the first poolConnBeforeAppend. That is the armed
// connection's checkpoint: no other connection is being established, and the
// successor's checkpoint cannot be reached while this one is still running.
//
// Parameters:
//   - before: runs first, inside connect; nil for none
//   - park: waits until the death is far enough along for the ordering under test
func (f *appendRaceFixture) killBeforeAppend(t *testing.T, before func(), park func()) {
	var fired atomic.Bool
	f.harness.events.on(poolConnBeforeAppend, func(*HostInfo) {
		if !fired.CompareAndSwap(false, true) {
			return
		}
		if before != nil {
			before()
		}
		select {
		case sock := <-f.dialer.dialed:
			_ = sock.Close()
		case <-time.After(fillEventBudget):
			t.Error("the armed dial never published its socket")
			return
		}
		park()
	})
}

// awaitFillReleases blocks until the pool has released n more fill claims than base.
//
// A timeout reports the pool as it stands, so a failure says whether the removal was
// lost, the dead connection kept, or the host convicted, not only that a release is missing.
func (f *appendRaceFixture) awaitFillReleases(t *testing.T, base, n int, what string) {
	t.Helper()

	deadline := time.After(fillEventBudget)
	for f.harness.events.count(poolFillDone) < base+n {
		select {
		case <-time.After(time.Millisecond):
		case <-deadline:
			t.Fatalf("timed out after %v waiting for %s (%d of %d releases); pool: %s",
				fillEventBudget, what, f.harness.events.count(poolFillDone)-base, n, f.describePool())
		}
	}
}

// describePool renders the pool's accounting state for a failure message.
//
// Returns:
//   - string: the connections and their liveness, the gate, the pending removal and claims,
//     and the host state
func (f *appendRaceFixture) describePool() string {
	f.pool.mu.RLock()
	closed := make([]bool, len(f.pool.conns))
	for i, conn := range f.pool.conns {
		closed[i] = conn.Closed()
	}
	filling, refill, pending := f.pool.filling, f.pool.refillPending, f.pool.fillsPending
	f.pool.mu.RUnlock()

	return fmt.Sprintf("conns closed=%v filling=%v refillPending=%v fillsPending=%d host=%v policyDownEvents=%d",
		closed, filling, refill, pending, f.host.State(), len(f.harness.collector.down))
}

// runClaimedFill claims a fill and runs it, as a Pick or a HandleError would.
func (f *appendRaceFixture) runClaimedFill(t *testing.T) {
	t.Helper()

	require.True(t, f.pool.claimFill(), "the pool must accept a fill claim")
	go f.pool.runFill()
}

// requireReplaced asserts the pool is back at size with only live connections, and
// that the policy never saw the host go DOWN.
func (f *appendRaceFixture) requireReplaced(t *testing.T) {
	t.Helper()

	f.pool.mu.RLock()
	conns := append([]*Conn(nil), f.pool.conns...)
	size, filling, refill, pending := f.pool.size, f.pool.filling, f.pool.refillPending, f.pool.fillsPending
	f.pool.mu.RUnlock()

	// Conviction is checked first: a convicted host's pool is replaced, so the size
	// check would otherwise report the symptom rather than the cause.
	require.Empty(t, drainHosts(f.harness.collector.down), "the host must never have been marked DOWN")
	require.Equal(t, NodeUp, f.host.State(), "the host must be UP")
	require.Len(t, conns, size, "the pool must be back at size")
	for i, conn := range conns {
		require.False(t, conn.Closed(), "connection %d of the pool must be live", i)
	}
	require.False(t, filling, "no cycle may still hold the gate")
	require.False(t, refill, "no removal may still be pending")
	require.Zero(t, pending, "no fill claim may still be published")
}

// awaitNotOurs is a park for killBeforeAppend: it returns once HandleError has scanned
// the pool and not found the connection, so connect's append check comes after it.
func (f *appendRaceFixture) awaitNotOurs(t *testing.T) func() {
	return func() {
		if _, err := f.harness.events.awaitErr(poolHandleErrorNotOurs, "HandleError to miss the dying connection"); err != nil {
			t.Error(err)
		}
	}
}

// TestConnect_DeathBeforeAppendOnInitialFillIsReplaced drives the synchronous branch:
// the pool is empty, so the dying connection is the cycle's first.
func TestConnect_DeathBeforeAppendOnInitialFillIsReplaced(t *testing.T) {
	f := newAppendRaceFixture(t, 1, fillHarnessOpts{})
	f.killBeforeAppend(t, nil, f.awaitNotOurs(t))

	base := f.harness.events.count(poolFillDone)
	f.runClaimedFill(t)

	// The owning cycle, then the successor it hands the removal to.
	f.awaitFillReleases(t, base, 2, "the owning cycle and its successor")
	f.requireReplaced(t)

	require.NoError(t, awaitQuery(t, f.harness.query(t.Context(), nil)),
		"a query must be served by the replacement")
}

// TestConnect_DeathBeforeAppendOnContinuationIsReplaced drives the asynchronous branch:
// the pool still holds a connection, so the dying one is dialed by connectMany.
//
// This is the branch where returning the death as an error would neither convict nor
// leave an obligation, so it separates a recorded removal from a connect failure.
func TestConnect_DeathBeforeAppendOnContinuationIsReplaced(t *testing.T) {
	f := newAppendRaceFixture(t, 2, fillHarnessOpts{})
	f.killBeforeAppend(t, nil, f.awaitNotOurs(t))

	base := f.harness.events.count(poolFillDone)
	f.runClaimedFill(t)

	f.awaitFillReleases(t, base, 2, "the owning cycle and its successor")
	f.requireReplaced(t)
}

// TestConnect_DeathRecordedBeforeHandleErrorIsNotRecordedTwice drives the other
// ordering: connect's check sees the connection closed before its HandleError has
// scanned the pool.
// HandleError must then find nothing and record nothing,
// because connect already recorded the removal.
func TestConnect_DeathRecordedBeforeHandleErrorIsNotRecordedTwice(t *testing.T) {
	var (
		armed   atomic.Bool
		parked  = make(chan struct{})
		release = make(chan struct{})
	)
	hooks := &connTestHooks{
		// closeWithError marks the connection closed before this runs,
		// and calls HandleError only after it returns.
		// It carries no connection identity,
		// so the one-shot selects the first closer after arming,
		// and that closer blocks outside the CAS rather than holding any lock another closer needs.
		closerBeforeCancel: func() {
			if !armed.CompareAndSwap(true, false) {
				return
			}
			close(parked)
			<-release
		},
	}
	var releaseOnce sync.Once
	releaseCloser := func() { releaseOnce.Do(func() { close(release) }) }
	// Deferred as well as registered: a deferred release runs before the fixture's
	// session cleanup, which can itself become the parked closer if the schedule failed.
	t.Cleanup(releaseCloser)
	defer releaseCloser()

	f := newAppendRaceFixture(t, 1, fillHarnessOpts{
		hooks: hooks,
		// No heartbeat may close a connection inside the schedule.
		tune: func(c *ClusterConfig) { c.heartbeatInterval = time.Hour },
	})

	type sample struct {
		pending int
		refill  bool
	}
	samples := make(chan sample, 1)
	f.harness.events.on(poolHandleErrorNotOurs, func(*HostInfo) {
		// Read directly under one read lock, then publish outside it: this runs
		// before HandleError would spawn anything, so a claim it published is
		// still visible here.
		f.pool.mu.RLock()
		s := sample{pending: f.pool.fillsPending, refill: f.pool.refillPending}
		f.pool.mu.RUnlock()
		samples <- s
	})

	f.killBeforeAppend(t, func() { armed.Store(true) }, func() {
		select {
		case <-parked:
		case <-time.After(fillEventBudget):
			t.Error("the dying connection's closer never parked")
		}
	})

	base := f.harness.events.count(poolFillDone)
	f.runClaimedFill(t)

	// Both cycles are joined before HandleError is let run, so nothing the fill
	// machinery owes is outstanding when it samples the pool.
	f.awaitFillReleases(t, base, 2, "the owning cycle and its successor")
	f.requireReplaced(t)

	releaseCloser()

	select {
	case s := <-samples:
		require.Zero(t, s.pending, "HandleError must not claim a fill for a removal connect recorded")
		require.False(t, s.refill, "HandleError must not record a removal connect recorded")
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for HandleError to miss the dying connection", fillEventBudget)
	}
	require.Equal(t, base+2, f.harness.events.count(poolFillDone), "no further fill may have run")
}

// TestConnect_DeathBeforeAppendDoesNotStrandWaiter parks a query in awaitFill on the
// cycle whose connection dies, and requires the successor to serve it.
func TestConnect_DeathBeforeAppendDoesNotStrandWaiter(t *testing.T) {
	f := newAppendRaceFixture(t, 1, fillHarnessOpts{})

	waiting := make(chan struct{})
	var waitOnce sync.Once
	f.harness.session.executor.testBeforeWait = func() {
		waitOnce.Do(func() { close(waiting) })
	}

	f.killBeforeAppend(t, func() {
		select {
		case <-waiting:
		case <-time.After(fillEventBudget):
			t.Error("the query never parked on the fill")
		}
	}, f.awaitNotOurs(t))

	// Pick on the empty pool claims the fill, and the query then waits on it.
	result := f.harness.query(t.Context(), nil)

	require.NoError(t, awaitQuery(t, result), "the waiter must be served by the successor")
	require.Empty(t, drainHosts(f.harness.collector.down), "the host must never have been marked DOWN")
}

// TestConnect_RefusesPolicyWithNoAttempt proves a reconnection policy that allows no
// attempt fails the connect instead of reaching the append with no connection.
func TestConnect_RefusesPolicyWithNoAttempt(t *testing.T) {
	for _, retries := range []int{0, -1} {
		t.Run(time.Duration(retries).String(), func(t *testing.T) {
			session := &Session{}
			session.cfg.ReconnectionPolicy = &ConstantReconnectionPolicy{MaxRetries: retries}
			pool := &hostConnPool{
				session: session,
				host:    &HostInfo{},
				size:    1,
				logger:  newTestLogger(LogLevelDebug),
			}

			var err error
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("connect panicked: %v", r)
					}
				}()
				err = pool.connect()
			}()

			require.ErrorContains(t, err, "GetMaxRetries", "connect must fail for a policy with no attempt")
			require.Empty(t, pool.conns, "nothing may be appended")
			awaitPoolLock(t, pool)
		})
	}
}
