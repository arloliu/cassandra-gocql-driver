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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// handleErrorDecision is one decision controlConn.HandleError reported.
type handleErrorDecision struct {
	conn      *Conn
	reconnect bool
}

// handleErrorDecisions records every decision reported through testControlHandleErrorDecision.
// Recording only appends and signals, so the hook never blocks the HandleError that calls it.
type handleErrorDecisions struct {
	mu       sync.Mutex
	list     []handleErrorDecision
	recorded chan struct{}
}

func newHandleErrorDecisions() *handleErrorDecisions {
	return &handleErrorDecisions{recorded: make(chan struct{}, 1)}
}

// record is the testControlHandleErrorDecision hook.
func (d *handleErrorDecisions) record(conn *Conn, reconnect bool) {
	d.mu.Lock()
	d.list = append(d.list, handleErrorDecision{conn: conn, reconnect: reconnect})
	d.mu.Unlock()
	select {
	case d.recorded <- struct{}{}:
	default:
	}
}

// lookup returns the first decision reported for conn.
func (d *handleErrorDecisions) lookup(conn *Conn) (reconnect, found bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, dec := range d.list {
		if dec.conn == conn {
			return dec.reconnect, true
		}
	}
	return false, false
}

// awaitFor waits until HandleError has reported a decision for conn and returns it.
func (d *handleErrorDecisions) awaitFor(t *testing.T, conn *Conn) bool {
	t.Helper()

	deadline := time.After(lifecycleBudget)
	for {
		if reconnect, found := d.lookup(conn); found {
			return reconnect
		}
		select {
		case <-d.recorded:
		case <-deadline:
			t.Fatalf("HandleError reported no decision for %s within %v", conn.addr, lifecycleBudget)
		}
	}
}

// heartbeatPark holds the control heartbeat in testControlBeforeHeartbeatStart,
// before its Starting to Started CAS, until released.
// It registers its own release with t.Cleanup,
// so a failed assertion cannot leave the heartbeat parked.
type heartbeatPark struct {
	arrived chan struct{}
	gate    chan struct{}
	once    sync.Once
}

func newHeartbeatPark(t *testing.T) *heartbeatPark {
	p := &heartbeatPark{arrived: make(chan struct{}, 1), gate: make(chan struct{})}
	t.Cleanup(p.release)
	return p
}

// hook is the testControlBeforeHeartbeatStart hook.
func (p *heartbeatPark) hook() {
	select {
	case p.arrived <- struct{}{}:
	default:
	}
	<-p.gate
}

// awaitParked waits until the heartbeat has reached the hook.
func (p *heartbeatPark) awaitParked(t *testing.T) {
	t.Helper()
	select {
	case <-p.arrived:
	case <-time.After(lifecycleBudget):
		t.Fatalf("the control heartbeat did not park within %v", lifecycleBudget)
	}
}

// release lets the parked heartbeat run its CAS; safe to call twice.
func (p *heartbeatPark) release() {
	p.once.Do(func() { close(p.gate) })
}

// onlyCandidate returns the single control candidate in flight.
func onlyCandidate(t *testing.T, c *controlConn) *Conn {
	t.Helper()
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	require.Len(t, c.candidates, 1, "exactly one control candidate must be in flight")
	for conn := range c.candidates {
		return conn
	}
	return nil
}

// requireAllTransportsClosed is the orphan oracle:
// every transport the dialer produced, pool connections included, must have had Close invoked on it.
//
// Call it once Session.Close (or NewSession's own failure path) has returned,
// and before fixture cleanup, which would close an orphan from the server side.
// It checks the driver's close obligation, not the peer's teardown.
func requireAllTransportsClosed(t *testing.T, dialer *faultingDialer) {
	t.Helper()

	dialer.mu.Lock()
	conns := append([]*faultConn(nil), dialer.conns...)
	dialer.mu.Unlock()
	// A transport left open would otherwise outlive the test.
	t.Cleanup(func() {
		for _, fc := range conns {
			_ = fc.Close()
		}
	})

	open := 0
	for _, fc := range conns {
		if !assert.True(t, fc.transportCloseInvoked.Load(),
			"a control transport was left open after Session.Close: %s", fc.LocalAddr()) {
			open++
		}
	}
	t.Logf("dialled transports: %d, left open: %d", len(conns), open)
	if open > 0 {
		t.FailNow()
	}
}

// awaitSessionClose runs Session.Close and bounds the wait for it.
func awaitSessionClose(t *testing.T, session *Session) {
	t.Helper()
	select {
	case <-asyncClose(session):
	case <-time.After(closeBudget):
		t.Fatal("Session.Close did not return")
	}
}

// TestControlInit_DyingCandidateLeavesNoOrphan_TwoContactPoints proves that
// when init's first candidate dies during its setup, its HandleError leaves it to init:
// no reconnect runs beside init's connect loop,
// so no second control connection is published and displaced,
// and Session.Close closes every transport.
func TestControlInit_DyingCandidateLeavesNoOrphan_TwoContactPoints(t *testing.T) {
	captured := &sessionCapturePolicy{HostSelectionPolicy: RoundRobinHostPolicy()}
	decisions := newHandleErrorDecisions()
	_, srv, _ := startLocalHostServer(t)
	dialer := newFaultingDialer(srv.Address)
	parked := make(chan *faultConn, 8)
	dialer.setGateRegisterNew(parked)
	t.Cleanup(dialer.releaseRegisterGates)
	cluster := newLocalHostCluster(t, "", srv.Address, func(cluster *ClusterConfig, _ string) {
		cluster.Hosts = []string{"127.0.0.1", "127.0.0.2"}
		cluster.HostDialer = dialer
		cluster.Logger = &historyLogger{}
		cluster.PoolConfig.HostSelectionPolicy = captured
		cluster.testControlHandleErrorDecision = decisions.record
	})

	type result struct {
		s   *Session
		err error
	}
	created := make(chan result, 1)
	go func() {
		s, err := cluster.CreateSession()
		created <- result{s, err}
	}()

	awaitParked := func(what string) {
		t.Helper()
		select {
		case <-parked:
		case <-time.After(lifecycleBudget):
			t.Fatalf("timed out waiting for %s to park in REGISTER", what)
		}
	}
	awaitParked("init's first candidate")
	control := captured.session.Load().control
	first := onlyCandidate(t, control)

	// The node drops the connection: serve reports a read error from its own goroutine.
	go first.closeWithError(errors.New("injected: init candidate died during REGISTER"))

	if decisions.awaitFor(t, first) {
		// HandleError started a reconnect beside init's loop.
		// Hold both candidates in setup, so that each publishes a healthy connection once released.
		awaitParked("init's second candidate or the reconnect's")
		awaitParked("the other of init's second candidate and the reconnect's")
	}
	dialer.releaseRegisterGates()

	var res result
	select {
	case res = <-created:
	case <-time.After(lifecycleBudget):
		t.Fatal("CreateSession did not return")
	}
	require.NoError(t, res.err, "CreateSession")
	t.Cleanup(res.s.Close)
	require.Eventually(t, func() bool { return candidateCount(control) == 0 }, lifecycleBudget, time.Millisecond,
		"every control candidate must have been published or released")

	awaitSessionClose(t, res.s)
	requireAllTransportsClosed(t, dialer)
}

// TestControlInit_CandidateDeadBeforePublishLeavesNoOrphan proves that
// an init candidate dying after REGISTER and before its publish starts no reconnect of its own.
// That reconnect would publish a live connection that init's publish then displaced,
// and nothing would ever close it.
//
// Init still publishes the dead candidate (N-ctrl-1),
// so whether CreateSession succeeds is logged, not asserted.
func TestControlInit_CandidateDeadBeforePublishLeavesNoOrphan(t *testing.T) {
	var calls atomic.Int32
	captured := &sessionCapturePolicy{HostSelectionPolicy: RoundRobinHostPolicy()}
	_, srv, _ := startLocalHostServer(t)
	dialer := newFaultingDialer(srv.Address)
	cluster := newLocalHostCluster(t, "", srv.Address, func(cluster *ClusterConfig, _ string) {
		cluster.HostDialer = dialer
		cluster.Logger = &historyLogger{}
		cluster.PoolConfig.HostSelectionPolicy = captured
		cluster.testControlBeforePublish = func(*HostInfo) {
			if calls.Add(1) != 1 {
				return
			}
			// Init's first candidate dies with an error after REGISTER.
			// In production serve reports it from its own goroutine;
			// inline here so the order is fixed: a reconnect started by its HandleError publishes before init does.
			c := captured.session.Load().control
			c.lifecycleMu.Lock()
			var cand *Conn
			for conn := range c.candidates {
				cand = conn
			}
			c.lifecycleMu.Unlock()
			cand.closeWithError(errors.New("injected: init candidate died before publish"))
		}
	})

	session, err := cluster.CreateSession()
	t.Logf("CreateSession: err=%v", err)
	require.GreaterOrEqual(t, calls.Load(), int32(1), "init's candidate must have reached its publish")
	if err == nil {
		t.Cleanup(session.Close)
		awaitSessionClose(t, session)
	}
	// On failure NewSession has already run Session.Close.
	requireAllTransportsClosed(t, dialer)
}

// TestControlStarting_PublishedDeathIsRepairedAtOnce proves that
// the published control connection is repaired by its HandleError while the state is still Starting,
// without waiting for the heartbeat to take over.
func TestControlStarting_PublishedDeathIsRepairedAtOnce(t *testing.T) {
	park := newHeartbeatPark(t)
	decisions := newHandleErrorDecisions()
	_, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
		cluster.testControlBeforeHeartbeatStart = park.hook
		cluster.testControlHandleErrorDecision = decisions.record
	})
	park.awaitParked(t)
	control := session.control
	published := control.getConn()
	require.NotNil(t, published, "CreateSession must have published a control connection")
	require.False(t, published.conn.Closed(), "the published control connection must be live")

	// serve would report a read error from its own goroutine; inline, HandleError
	// and any reconnect it starts have run when this returns.
	published.conn.closeWithError(errors.New("injected: published control connection died"))

	require.True(t, decisions.awaitFor(t, published.conn),
		"the decision hook reported reconnect == false for the published connection")
	require.Eventually(t, func() bool {
		ch := control.getConn()
		return ch != nil && ch.conn != published.conn && !ch.conn.Closed()
	}, lifecycleBudget, time.Millisecond, "a different, live control connection must be published")
	require.Equal(t, int32(controlConnStarting), atomic.LoadInt32(&control.state),
		"the heartbeat must still be parked, so the repair did not wait for it")
	park.release()
}

// TestControlStarting_SchemaDialectProbeSurvivesAWriteFailure proves that
// under DisableInitialHostLookup the system_schema probe survives a write failure
// that closes the published control connection while the state is Starting:
// its HandleError reconnects at once, and the probe's retry runs on the replacement.
func TestControlStarting_SchemaDialectProbeSurvivesAWriteFailure(t *testing.T) {
	park := newHeartbeatPark(t)
	decisions := newHandleErrorDecisions()
	captured := &sessionCapturePolicy{HostSelectionPolicy: RoundRobinHostPolicy()}
	var first atomic.Pointer[Conn]
	var firstTransport atomic.Pointer[faultConn]
	_, srv, _ := startLocalHostServer(t)
	dialer := newFaultingDialer(srv.Address)
	// Covers the probe's PREPARE as well as a QUERY.
	dialer.setFailingStatement("system_schema.keyspaces")
	cluster := newLocalHostCluster(t, "", srv.Address, func(cluster *ClusterConfig, _ string) {
		cluster.HostDialer = dialer
		cluster.DisableInitialHostLookup = true
		cluster.PoolConfig.HostSelectionPolicy = captured
		cluster.testControlBeforeHeartbeatStart = park.hook
		cluster.testControlHandleErrorDecision = decisions.record
		cluster.testControlBeforePublish = func(*HostInfo) {
			// Arm only init's candidate; the reconnect's replacement stays unarmed.
			if first.Load() != nil {
				return
			}
			c := captured.session.Load().control
			c.lifecycleMu.Lock()
			var cand *Conn
			for conn := range c.candidates {
				cand = conn
			}
			c.lifecycleMu.Unlock()
			w, ok := cand.w.(*deadlineContextWriter)
			if !ok {
				return
			}
			fc, ok := w.w.(*faultConn)
			if !ok {
				return
			}
			first.Store(cand)
			firstTransport.Store(fc)
			dialer.arm(fc)
		}
	})

	session, err := cluster.CreateSession()
	require.NoError(t, err, "CreateSession")
	t.Cleanup(session.Close)
	require.NotNil(t, firstTransport.Load(), "init's candidate transport must have been armed")

	require.True(t, session.useSystemSchema, "useSystemSchema is false: the probe's retry did not reach a live connection")
	failedOnFirst := false
	for _, f := range dialer.recordedFailures() {
		if f.conn == firstTransport.Load() {
			failedOnFirst = true
		}
	}
	require.True(t, failedOnFirst, "the probe's write must have failed on init's published connection")
	require.True(t, decisions.awaitFor(t, first.Load()),
		"the decision hook reported reconnect == false for the published connection")
	park.awaitParked(t)
	require.Equal(t, int32(controlConnStarting), atomic.LoadInt32(&session.control.state),
		"the heartbeat must still be parked")
}

// TestControlStarted_PublishedDeathReconnectsFromHandleError proves that
// once the heartbeat has taken over, the published connection's HandleError itself still decides to reconnect.
// The heartbeat's own reconnect could also supply a replacement,
// so the decision, not the replacement, is what this checks first.
func TestControlStarted_PublishedDeathReconnectsFromHandleError(t *testing.T) {
	decisions := newHandleErrorDecisions()
	_, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
		cluster.testControlHandleErrorDecision = decisions.record
	})
	control := session.control
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&control.state) == controlConnStarted
	}, lifecycleBudget, time.Millisecond, "the heartbeat must move the state to Started")
	published := control.getConn()
	require.NotNil(t, published, "CreateSession must have published a control connection")

	published.conn.closeWithError(errors.New("injected: published control connection died"))

	require.True(t, decisions.awaitFor(t, published.conn),
		"the decision hook reported reconnect == false for the published connection")
	require.Eventually(t, func() bool {
		ch := control.getConn()
		return ch != nil && ch.conn != published.conn && !ch.conn.Closed()
	}, lifecycleBudget, time.Millisecond, "a different, live control connection must be published")
}
