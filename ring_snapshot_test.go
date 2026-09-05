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
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// snapshotBudget bounds every positive wait in these tests.
const snapshotBudget = 15 * time.Second

var errInjectedWrite = errors.New("gocql: test injected write failure")

// writeFailure records one write the faulting dialer failed.
type writeFailure struct {
	conn      *faultConn
	statement []byte
}

// faultingDialer redirects every dial to one address, like redirectHostDialer,
// and wraps each connection so a test can fail exactly the writes it names:
// QUERY frames whose statement mentions system.peers, on the connections it armed.
//
// OPTIONS, STARTUP, REGISTER and system.local writes always pass, so heartbeats,
// connection setup and the control connection's own setup keep working; only a
// ring refresh's peers read fails, which is the write that used to deadlock the
// control reconnect.
type faultingDialer struct {
	target string

	mu        sync.Mutex
	refuse    bool
	armNew    bool
	armed     map[*faultConn]bool
	conns     []*faultConn
	dialTimes []time.Time
	failures  []writeFailure

	// heartbeats counts OPTIONS frames written through after a connection's
	// STARTUP, on any connection: connection setup sends OPTIONS before STARTUP,
	// so only the control heartbeat sends one afterwards.
	heartbeats atomic.Int64
}

var _ HostDialer = (*faultingDialer)(nil)

func newFaultingDialer(target string) *faultingDialer {
	return &faultingDialer{target: target, armed: map[*faultConn]bool{}}
}

// DialHost dials the fixed target and wraps the connection.
func (d *faultingDialer) DialHost(ctx context.Context, host *HostInfo) (*DialedHost, error) {
	d.mu.Lock()
	refuse := d.refuse
	d.mu.Unlock()
	if refuse {
		return nil, errGatedDialerClosed
	}

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", d.target)
	if err != nil {
		return nil, err
	}
	fc := &faultConn{Conn: conn, dialer: d}

	d.mu.Lock()
	d.conns = append(d.conns, fc)
	d.dialTimes = append(d.dialTimes, time.Now())
	if d.armNew {
		d.armed[fc] = true
	}
	d.mu.Unlock()
	return &DialedHost{Conn: fc}, nil
}

// dials returns when each connection was dialled, in order.
func (d *faultingDialer) dials() []time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]time.Time(nil), d.dialTimes...)
}

// arm makes peers writes on fc fail.
func (d *faultingDialer) arm(fc *faultConn) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.armed[fc] = true
}

// setArmNew arms (or stops arming) every connection dialled from now on.
func (d *faultingDialer) setArmNew(on bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.armNew = on
}

// disarmAll lets every write through again.
func (d *faultingDialer) disarmAll() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.armNew = false
	d.armed = map[*faultConn]bool{}
}

// setRefuse makes DialHost fail (or succeed again).
func (d *faultingDialer) setRefuse(on bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.refuse = on
}

// dialCount returns how many connections were dialled so far.
func (d *faultingDialer) dialCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.conns)
}

// recordedFailures returns a copy of the failed writes.
func (d *faultingDialer) recordedFailures() []writeFailure {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]writeFailure(nil), d.failures...)
}

// shouldFail decides whether a write on fc is one to fail, and records it if so.
func (d *faultingDialer) shouldFail(fc *faultConn, p []byte) bool {
	// Protocol v4, uncompressed: a 9-byte header whose fifth byte is the opcode,
	// then for QUERY a 4-byte length and the statement.
	if len(p) < 9 || frameOp(p[4]) != opQuery || !bytes.Contains(p, []byte("system.peers")) {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.armed[fc] {
		return false
	}
	d.failures = append(d.failures, writeFailure{conn: fc, statement: append([]byte(nil), p[9:]...)})
	return true
}

// faultConn is a net.Conn whose Write can be failed by its dialer.
type faultConn struct {
	net.Conn
	dialer *faultingDialer

	// started is set once STARTUP was written on this connection.
	started atomic.Bool
}

// Write fails the writes the dialer names and counts post-STARTUP OPTIONS writes.
func (c *faultConn) Write(p []byte) (int, error) {
	if c.dialer.shouldFail(c, p) {
		return 0, errInjectedWrite
	}
	n, err := c.Conn.Write(p)
	if err == nil && len(p) >= 9 {
		switch frameOp(p[4]) {
		case opStartup:
			c.started.Store(true)
		case opOptions:
			if c.started.Load() {
				c.dialer.heartbeats.Add(1)
			}
		}
	}
	return n, err
}

// controlFaultConn returns the faultConn behind the published control connection.
func controlFaultConn(t *testing.T, session *Session) *faultConn {
	t.Helper()

	ch := session.control.getConn()
	require.NotNil(t, ch, "the session must hold a control connection")
	w, ok := ch.conn.w.(*deadlineContextWriter)
	require.True(t, ok, "the control connection's writer must be a deadlineContextWriter, got %T", ch.conn.w)
	fc, ok := w.w.(*faultConn)
	require.True(t, ok, "the control connection must have been dialled by the faulting dialer, got %T", w.w)
	return fc
}

// snapshotFixture is a control-enabled session against the scripted fake node,
// dialled through a faultingDialer, with the ring refresh's results observable.
type snapshotFixture struct {
	script  *localHostServer
	session *Session
	dialer  *faultingDialer
	logger  *historyLogger
	done    chan error
	entered chan struct{}
	// parked is signalled by a refresh that found holdRefresh set and is now
	// blocked on refreshGate; only that proves the refresh is in flight and held.
	parked chan struct{}

	holdRefresh atomic.Bool
	refreshGate chan struct{}
	releaseOnce sync.Once
}

// newSnapshotFixture builds the fixture; tune may adjust the config further.
func newSnapshotFixture(t *testing.T, tune func(*ClusterConfig)) *snapshotFixture {
	t.Helper()

	script, srv, _ := startLocalHostServer(t)
	f := &snapshotFixture{
		script:      script,
		dialer:      newFaultingDialer(srv.Address),
		logger:      &historyLogger{},
		done:        make(chan error, 64),
		entered:     make(chan struct{}, 64),
		parked:      make(chan struct{}, 64),
		refreshGate: make(chan struct{}),
	}

	cluster := newLocalHostCluster(t, "", srv.Address, func(cluster *ClusterConfig, _ string) {
		cluster.HostDialer = f.dialer
		cluster.Logger = f.logger
		cluster.testRingRefreshHook = func() {
			select {
			case f.entered <- struct{}{}:
			default:
			}
			if f.holdRefresh.Load() {
				select {
				case f.parked <- struct{}{}:
				default:
				}
				<-f.refreshGate
			}
		}
		cluster.testRingRefreshDone = func(err error) { f.done <- err }
		if tune != nil {
			tune(cluster)
		}
	})
	session, err := cluster.CreateSession()
	require.NoError(t, err, "CreateSession")
	// Bounded teardown: these tests exist to catch a Close that hangs behind the
	// ring flusher, and a failed assertion must not turn into a hung test binary.
	t.Cleanup(func() {
		closed := make(chan struct{})
		go func() {
			session.Close()
			close(closed)
		}()
		select {
		case <-closed:
		case <-time.After(snapshotBudget):
			t.Errorf("Session.Close hung during teardown")
		}
	})
	// Registered after the close, so it runs before it: Close waits for the
	// flusher, which must not be left parked on the gate.
	t.Cleanup(f.releaseRefresh)
	f.session = session
	return f
}

// awaitParked waits for a held refresh to block on the gate.
func (f *snapshotFixture) awaitParked(t *testing.T, what string) {
	t.Helper()
	select {
	case <-f.parked:
	case <-time.After(snapshotBudget):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// releaseRefresh opens the refresh gate; safe to call twice.
func (f *snapshotFixture) releaseRefresh() {
	f.releaseOnce.Do(func() { close(f.refreshGate) })
}

// awaitDone returns the next refresh result.
func (f *snapshotFixture) awaitDone(t *testing.T, what string) error {
	t.Helper()
	select {
	case err := <-f.done:
		return err
	case <-time.After(snapshotBudget):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

// awaitEntered waits for a refresh to start.
func (f *snapshotFixture) awaitEntered(t *testing.T, what string) {
	t.Helper()
	select {
	case <-f.entered:
	case <-time.After(snapshotBudget):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// drain discards queued refresh events.
func (f *snapshotFixture) drain() {
	for {
		select {
		case <-f.done:
		case <-f.entered:
		default:
			return
		}
	}
}

// refreshAsync runs session.refreshRing on a goroutine.
//
// Returns:
//   - <-chan error: the refresh's result
func (f *snapshotFixture) refreshAsync() <-chan error {
	errCh := make(chan error, 1)
	go func() { errCh <- f.session.refreshRing() }()
	return errCh
}

// awaitErr receives a refresh result or fails.
func awaitErr(t *testing.T, errCh <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-errCh:
		return err
	case <-time.After(snapshotBudget):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

// requireClosesWithin asserts session.Close returns inside the budget.
func requireClosesWithin(t *testing.T, session *Session) {
	t.Helper()
	closed := make(chan struct{})
	go func() {
		session.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(snapshotBudget):
		t.Fatal("Session.Close did not return")
	}
}

const (
	peerIDOne = "11111111-0000-4000-8000-000000000001"
	peerIDTwo = "22222222-0000-4000-8000-000000000002"
)

// TestDuplicateHostID covers the pure duplicate check.
func TestDuplicateHostID(t *testing.T) {
	mk := func(id, addr string) *HostInfo {
		h, err := NewHostInfoFromAddrPort(net.ParseIP(addr), 9042)
		require.NoError(t, err)
		h.setHostID(id)
		return h
	}
	a := mk(peerIDOne, "10.0.0.1")
	b := mk(peerIDTwo, "10.0.0.2")
	aAgain := mk(peerIDOne, "10.0.0.3")

	_, _, dup := duplicateHostID(nil)
	require.False(t, dup)
	_, _, dup = duplicateHostID([]*HostInfo{a, b})
	require.False(t, dup)

	first, again, dup := duplicateHostID([]*HostInfo{a, b, aAgain})
	require.True(t, dup)
	require.Same(t, a, first, "the earlier occurrence comes first")
	require.Same(t, aAgain, again)

	first, again, dup = duplicateHostID([]*HostInfo{a, aAgain, b})
	require.True(t, dup)
	require.Same(t, a, first)
	require.Same(t, aAgain, again)
}

// TestRefreshRing_RejectsDuplicateSnapshot: a snapshot that lists the local
// host's id again at another address is rejected whole; a distinct peer is admitted.
func TestRefreshRing_RejectsDuplicateSnapshot(t *testing.T) {
	f := newSnapshotFixture(t, nil)
	original := f.session.ring.allHosts()
	require.Len(t, original, 1)

	f.script.setPeers([]peerRow{newPeerRow(f.script.hostID, "127.0.0.7")})
	err := f.session.refreshRing()
	require.ErrorIs(t, err, errDuplicateHostID)
	hosts := f.session.ring.allHosts()
	require.Len(t, hosts, 1, "the ring must be untouched")
	require.Same(t, original[0], hosts[0], "the ring object must be untouched")
	require.Len(t, f.logger.withMessage("Ring refresh failed."), 1, "the rejection is logged once")

	f.script.setPeers([]peerRow{newPeerRow(peerIDOne, "127.0.0.7")})
	require.NoError(t, f.session.refreshRing(), "a distinct peer id must be admitted through the same path")
	require.Len(t, f.session.ring.allHosts(), 2)
}

// TestRefreshRing_DuplicateLimitationTwoPhase: while one id is duplicated the whole
// ring is frozen, including another host's address change; once the duplicate
// clears, the next triggered refresh applies that change.
func TestRefreshRing_DuplicateLimitationTwoPhase(t *testing.T) {
	for _, dupFirst := range []bool{true, false} {
		name := "DuplicateLast"
		if dupFirst {
			name = "DuplicateFirst"
		}
		t.Run(name, func(t *testing.T) {
			f := newSnapshotFixture(t, nil)

			f.script.setPeers([]peerRow{newPeerRow(peerIDOne, "127.0.0.7")})
			require.NoError(t, f.session.refreshRing())
			c, ok := f.session.ring.getHost(peerIDOne)
			require.True(t, ok, "the peer must have been admitted")
			require.Eventually(t, func() bool { return c.IsUp() }, snapshotBudget, 10*time.Millisecond, "the peer's fill must succeed")
			f.session.handleHostDown(c)
			require.Equal(t, NodeDown, c.State())

			dup := newPeerRow(f.script.hostID, "127.0.0.9")
			moved := newPeerRow(peerIDOne, "127.0.0.8")
			if dupFirst {
				f.script.setPeers([]peerRow{dup, moved})
			} else {
				f.script.setPeers([]peerRow{moved, dup})
			}
			require.ErrorIs(t, f.session.refreshRing(), errDuplicateHostID)
			still, ok := f.session.ring.getHost(peerIDOne)
			require.True(t, ok)
			require.Same(t, c, still, "the moved host must keep its object while the snapshot is rejected")
			require.Equal(t, "127.0.0.7", c.ConnectAddress().String(), "the move must not be applied from a rejected snapshot")

			// C is DOWN, so the reconnect tick is what requests the next refresh.
			f.drain()
			f.script.setPeers([]peerRow{moved})
			f.session.reconnectDownedHostsOnce()
			require.NoError(t, f.awaitDone(t, "the refresh the tick requested after the duplicate cleared"))
			after, ok := f.session.ring.getHost(peerIDOne)
			require.True(t, ok)
			require.Equal(t, "127.0.0.8", after.ConnectAddress().String(), "the move must be applied once the duplicate cleared")
		})
	}
}

// TestSnapshot_FailsWholeOnControlSwitch: a control switch between the local and
// the peers read fails the whole refresh; nothing is reconciled from the mix.
func TestSnapshot_FailsWholeOnControlSwitch(t *testing.T) {
	hook := make(chan struct{}, 1)
	gate := make(chan struct{})
	var release sync.Once
	var hold atomic.Bool
	f := newSnapshotFixture(t, func(cluster *ClusterConfig) {
		cluster.testRingSnapshotHook = func() {
			if hold.Load() {
				hook <- struct{}{}
				<-gate
			}
		}
	})
	// After the fixture's cleanups, so the gate opens before Session.Close.
	t.Cleanup(func() { release.Do(func() { close(gate) }) })
	original := f.session.ring.allHosts()
	require.Len(t, original, 1)
	f.script.setPeers([]peerRow{newPeerRow(peerIDOne, "127.0.0.7")})

	hold.Store(true)
	errCh := f.refreshAsync()
	select {
	case <-hook:
	case <-time.After(snapshotBudget):
		t.Fatal("the refresh did not reach the point between its two reads")
	}
	before := f.session.control.getConn()
	// On a goroutine with a bound: with the old synchronous refresh this call
	// would wait on the parked flusher and never return.
	reconnected := make(chan struct{})
	go func() {
		f.session.control.reconnect()
		close(reconnected)
	}()
	select {
	case <-reconnected:
	case <-time.After(snapshotBudget):
		t.Fatal("reconnect waited on the parked ring flusher")
	}
	require.NotSame(t, before, f.session.control.getConn(), "the control connection must have switched")
	hold.Store(false)
	release.Do(func() { close(gate) })

	err := awaitErr(t, errCh, "the interrupted refresh")
	require.Error(t, err, "the peers read on the closed connection must fail the whole refresh")
	hosts := f.session.ring.allHosts()
	require.Len(t, hosts, 1, "nothing may be reconciled from a snapshot whose reads crossed a switch")
	require.Same(t, original[0], hosts[0])

	f.drain()
	require.NoError(t, f.session.refreshRing(), "the next refresh, on one connection, succeeds")
	require.Len(t, f.session.ring.allHosts(), 2)
}

// TestSnapshot_FailsOnClosedControlConn: a retained closed connection fails the
// refresh without reconciling; the heartbeat then reconnects and a refresh runs.
func TestSnapshot_FailsOnClosedControlConn(t *testing.T) {
	f := newSnapshotFixture(t, nil)
	original := f.session.ring.allHosts()
	f.script.setPeers([]peerRow{newPeerRow(peerIDOne, "127.0.0.7")})

	f.drain()
	f.session.control.getConn().conn.Close()

	require.Error(t, f.session.refreshRing(), "a query on the closed connection must fail the refresh")
	require.Len(t, f.session.ring.allHosts(), 1)
	require.Same(t, original[0], f.session.ring.allHosts()[0])

	// The first result is that failure; the heartbeat's reconnect then debounces
	// a refresh that succeeds.
	require.Error(t, f.awaitDone(t, "the failed refresh's result"))
	require.NoError(t, f.awaitDone(t, "the reconnect-driven refresh"))
	require.Len(t, f.session.ring.allHosts(), 2)
}

// TestSnapshot_FailsWhenAcquisitionExhausted: with no published connection and
// every dial refused, withConnHost gives up with errNoControl and nothing changes.
func TestSnapshot_FailsWhenAcquisitionExhausted(t *testing.T) {
	f := newSnapshotFixture(t, nil)
	original := f.session.ring.allHosts()
	f.script.setPeers([]peerRow{newPeerRow(peerIDOne, "127.0.0.7")})

	f.drain()
	refreshedBefore := len(f.logger.withMessage("Refreshed ring."))
	f.dialer.setRefuse(true)
	// The #1297 state: the control connection has gone and was not re-established.
	f.session.control.conn.Store((*connHost)(nil))

	err := f.session.refreshRing()
	require.ErrorIs(t, err, errNoControl)
	require.Len(t, f.session.ring.allHosts(), 1)
	require.Same(t, original[0], f.session.ring.allHosts()[0])
	require.Len(t, f.logger.withMessage("Refreshed ring."), refreshedBefore, "no reconciliation may have run")

	f.dialer.setRefuse(false)
	require.Error(t, f.awaitDone(t, "the failed refresh's result"))
	require.NoError(t, f.awaitDone(t, "the refresh after the heartbeat reconnected"))
	require.Len(t, f.session.ring.allHosts(), 2)
}

// TestControlReconnect_AfterRefreshWriteFailure drives the original deadlock chain:
// the refresh's peers write fails on the control connection, closeWithError calls
// HandleError synchronously, reconnect runs on the flusher's goroutine and used to
// wait for the flusher itself. Now it debounces a refresh and returns.
func TestControlReconnect_AfterRefreshWriteFailure(t *testing.T) {
	f := newSnapshotFixture(t, nil)
	f.script.setPeers([]peerRow{newPeerRow(peerIDOne, "127.0.0.7")})
	fc := controlFaultConn(t, f.session)
	before := f.session.control.getConn()

	f.drain()
	f.dialer.arm(fc)

	err := awaitErr(t, f.refreshAsync(), "the refresh whose peers write fails")
	require.Error(t, err, "the refresh must report its failed write")
	require.NotSame(t, before, f.session.control.getConn(), "the write failure must have reconnected the control connection")

	failures := f.dialer.recordedFailures()
	require.Len(t, failures, 1)
	require.Same(t, fc, failures[0].conn, "the failed write must be on the armed connection")
	require.Contains(t, string(failures[0].statement), "system.peers")

	require.Error(t, f.awaitDone(t, "the failed refresh's result"))
	require.NoError(t, f.awaitDone(t, "the debounced refresh after the reconnect"))
	require.Len(t, f.session.ring.allHosts(), 2)

	requireClosesWithin(t, f.session)
}

// TestControlReconnect_RepeatedRefreshFailures: every reconnected connection fails
// its peers write again, so refresh and reconnect cycle - paced by the debounce,
// with heartbeats passing - until the fault is lifted.
func TestControlReconnect_RepeatedRefreshFailures(t *testing.T) {
	f := newSnapshotFixture(t, nil)
	f.script.setPeers([]peerRow{newPeerRow(peerIDOne, "127.0.0.7")})

	f.drain()
	f.dialer.setArmNew(true)
	f.dialer.arm(controlFaultConn(t, f.session))
	dialsBefore := len(f.dialer.dials())
	heartbeatsBefore := f.dialer.heartbeats.Load()
	f.session.ringRefresher.trigger()

	for range 3 {
		require.Error(t, f.awaitDone(t, "a failed refresh"))
	}
	// Each failed refresh reconnected once: the reconnect dials are the source-time
	// events, and consecutive ones are at least a debounce apart.
	dials := f.dialer.dials()[dialsBefore:]
	require.GreaterOrEqual(t, len(dials), 3, "every failed refresh must have reconnected")
	for i := 1; i < len(dials); i++ {
		require.GreaterOrEqual(t, dials[i].Sub(dials[i-1]), ringRefreshDebounceTime-100*time.Millisecond,
			"consecutive reconnects must be paced by the debounce")
	}
	for _, failure := range f.dialer.recordedFailures() {
		require.Contains(t, string(failure.statement), "system.peers", "only peers writes may have been failed")
	}
	// The control heartbeat kept running across the cycle: OPTIONS were written
	// after STARTUP on the live connections, and none of them was reported failed.
	require.Greater(t, f.dialer.heartbeats.Load(), heartbeatsBefore, "heartbeat OPTIONS writes must have kept passing")
	require.Empty(t, f.logger.withMessage("Control connection failed to send heartbeat."), "no heartbeat write may have failed")
	require.Empty(t, f.logger.withMessage("Control connection heartbeat failed."), "no heartbeat response may have failed")

	f.dialer.disarmAll()
	var err error
	for range 3 {
		if err = f.awaitDone(t, "a refresh after the fault was lifted"); err == nil {
			break
		}
	}
	require.NoError(t, err, "a refresh on an unarmed connection must succeed")
	require.Len(t, f.session.ring.allHosts(), 2)

	requireClosesWithin(t, f.session)
}

// TestClose_DuringContinuingRefreshFailure: with the fault still armed and a
// refresh in flight, Session.Close returns.
func TestClose_DuringContinuingRefreshFailure(t *testing.T) {
	f := newSnapshotFixture(t, nil)
	f.script.setPeers([]peerRow{newPeerRow(peerIDOne, "127.0.0.7")})

	f.drain()
	f.dialer.setArmNew(true)
	f.dialer.arm(controlFaultConn(t, f.session))
	f.session.ringRefresher.trigger()
	for range 2 {
		require.Error(t, f.awaitDone(t, "a failed refresh"))
	}

	// Hold the next refresh inside its callback so Close is issued while it is
	// in flight, and release it only once shutdown has demonstrably started.
	f.drain()
	f.holdRefresh.Store(true)
	f.awaitParked(t, "the third refresh to park on the gate")

	closed := make(chan struct{})
	go func() {
		f.session.Close()
		close(closed)
	}()
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&f.session.control.state) == controlConnClosing
	}, snapshotBudget, 5*time.Millisecond, "Close must have started shutting the control connection down")
	f.holdRefresh.Store(false)
	f.releaseRefresh()

	select {
	case <-closed:
	case <-time.After(snapshotBudget):
		t.Fatal("Session.Close did not return with a refresh in flight and the fault armed")
	}
	dials := f.dialer.dialCount()
	time.Sleep(2 * ringRefreshDebounceTime)
	require.Equal(t, dials, f.dialer.dialCount(), "no reconnect may follow Close")
}
