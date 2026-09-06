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

// candidateCount returns how many control candidates are in flight.
func candidateCount(c *controlConn) int {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	return len(c.candidates)
}

// asyncClose runs session.Close on a goroutine and returns a channel closed when
// it returns, so a test can drive the shutdown and still bound the wait.
func asyncClose(session *Session) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		session.Close()
		close(done)
	}()
	return done
}

// awaitClosing waits until close() has latched the terminal state.
func awaitClosing(t *testing.T, c *controlConn) {
	t.Helper()
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&c.state) == controlConnClosing
	}, lifecycleBudget, time.Millisecond, "Close must latch the control connection closing")
}

// TestControlConnCandidateClosedWhenCloseWins proves that when Session.Close
// latches between a candidate's setup and its publication, shutdown owns the
// candidate: it closes the transport and the publish is refused.
func TestControlConnCandidateClosedWhenCloseWins(t *testing.T) {
	gate := make(chan struct{})
	parked := make(chan struct{}, 1)
	var armed atomic.Bool
	policy := newAddHostGatePolicy()
	fillStarted := make(chan *HostInfo, 4)
	f := newSnapshotFixture(t, func(cluster *ClusterConfig) {
		cluster.PoolConfig.HostSelectionPolicy = policy
		cluster.testStartPoolFillStart = func(host *HostInfo) { fillStarted <- host }
		cluster.testControlBeforePublish = func(*HostInfo) {
			if armed.Load() {
				parked <- struct{}{}
				<-gate
			}
		}
	})
	f.drain()
	addsBefore := policy.addHostCount()

	before := f.session.control.getConn()
	dialsBefore := f.dialer.dialCount()
	armed.Store(true)

	// Independent reconnect: Session.Close joins the ring flusher, so a
	// flusher-driven reconnect held past Close would deadlock the test itself.
	reconnected := make(chan struct{})
	go func() {
		f.session.control.reconnect()
		close(reconnected)
	}()

	select {
	case <-parked:
	case <-time.After(lifecycleBudget):
		t.Fatal("the reconnect did not reach the pre-publish gate")
	}
	require.Greater(t, f.dialer.dialCount(), dialsBefore, "the reconnect must have dialled a candidate")
	candidate := f.dialer.lastConn()
	require.NotNil(t, candidate)
	require.Equal(t, 1, candidateCount(f.session.control), "the candidate must be in flight")

	// Close runs to completion while the reconnect is still parked at the gate,
	// because Close does not join this independent reconnect.
	// Once Close has returned, its snapshot has already closed the in-flight
	// candidate, so the assertion is on a settled state, before the gate opens.
	closed := asyncClose(f.session)
	select {
	case <-closed:
	case <-time.After(closeBudget):
		t.Fatalf("Session.Close did not return within %v", closeBudget)
	}
	require.True(t, candidate.transportCloseInvoked.Load(),
		"Close must have closed the in-flight candidate's transport before it returned")

	close(gate)
	select {
	case <-reconnected:
	case <-time.After(lifecycleBudget):
		t.Fatal("reconnect did not return after the gate opened")
	}

	require.Same(t, before, f.session.control.getConn(),
		"a candidate whose publish lost to Close must not have replaced the control connection")
	require.Zero(t, candidateCount(f.session.control), "no candidate may be left in flight")

	// A publish that lost to Close must not schedule a fill or publish the host.
	select {
	case <-fillStarted:
		t.Fatal("a rejected publish must not schedule a pool fill")
	default:
	}
	require.Equal(t, addsBefore, policy.addHostCount(), "the host must not be published to the policy")
}

// TestControlConnCandidateClosedOnSetupFailure proves that when Session.Close
// latches in the window after a candidate's setup failed and before its caller
// closes it, shutdown owns the candidate: it closes the transport, and the
// caller's own cleanup that follows is idempotent.
func TestControlConnCandidateClosedOnSetupFailure(t *testing.T) {
	var reject atomic.Bool
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var armed atomic.Bool
	f := newSnapshotFixture(t, func(cluster *ClusterConfig) {
		cluster.HostFilter = HostFilterFunc(func(*HostInfo) bool {
			return !reject.Load()
		})
		cluster.testControlAfterSetupFailure = func() {
			if armed.Load() {
				entered <- struct{}{}
				<-release
			}
		}
	})
	f.drain()

	dialsBefore := f.dialer.dialCount()
	reject.Store(true)
	armed.Store(true)

	// Independent reconnect: its setup fails at the host filter, and its cleanup
	// parks in the after-setup-failure window with the candidate still in the set.
	reconnected := make(chan struct{})
	go func() {
		f.session.control.reconnect()
		close(reconnected)
	}()

	select {
	case <-entered:
	case <-time.After(lifecycleBudget):
		t.Fatal("the reconnect did not reach the after-setup-failure window")
	}
	require.Greater(t, f.dialer.dialCount(), dialsBefore, "the reconnect must have dialled a candidate")
	candidate := f.dialer.lastConn()
	require.NotNil(t, candidate)
	require.Equal(t, 1, candidateCount(f.session.control), "the failed candidate must still be in the set")
	require.False(t, candidate.transportCloseInvoked.Load(), "the candidate must not be closed before shutdown")

	// Close snapshots the still-in-set candidate and closes it.
	closed := asyncClose(f.session)
	select {
	case <-closed:
	case <-time.After(closeBudget):
		t.Fatalf("Session.Close did not return within %v", closeBudget)
	}
	require.True(t, candidate.transportCloseInvoked.Load(),
		"Close must have closed the candidate it snapshotted in the failure window")

	// Release the caller's cleanup; its Close is idempotent and it releases.
	close(release)
	select {
	case <-reconnected:
	case <-time.After(lifecycleBudget):
		t.Fatal("reconnect did not return")
	}
	require.Zero(t, candidateCount(f.session.control), "the failed candidate must be released")
}

// TestControlConnCandidateInBlockedWriteIsClosedByShutdown proves a candidate
// parked inside its REGISTER write is closed by Session.Close: the write does
// not consult the context, so only the transport close unblocks it.
func TestControlConnCandidateInBlockedWriteIsClosedByShutdown(t *testing.T) {
	f := newSnapshotFixture(t, nil)
	f.drain()

	// Gate the REGISTER write of the next dialled connection, so the reconnect
	// blocks inside setupConn with the candidate registered and in flight.
	parkedCh := make(chan *faultConn, 1)
	f.dialer.setGateRegisterNew(parkedCh)
	t.Cleanup(f.dialer.releaseRegisterGates)

	reconnected := make(chan struct{})
	go func() {
		f.session.control.reconnect()
		close(reconnected)
	}()

	var candidate *faultConn
	select {
	case candidate = <-parkedCh:
	case <-time.After(lifecycleBudget):
		t.Fatal("the reconnect did not park in the REGISTER write")
	}
	require.Equal(t, 1, candidateCount(f.session.control), "the candidate must be in flight")
	require.False(t, candidate.transportCloseInvoked.Load(), "the candidate must not be closed before shutdown")

	// Close closes the candidate as part of its snapshot, which unblocks the
	// parked write; Close does not join this independent reconnect, so it returns.
	// Joining Close makes the transport-close assertion a settled-state check.
	closed := asyncClose(f.session)
	select {
	case <-closed:
	case <-time.After(closeBudget):
		t.Fatalf("Session.Close did not return within %v", closeBudget)
	}
	require.True(t, candidate.transportCloseInvoked.Load(),
		"Close must have closed the candidate blocked in its write before it returned")

	select {
	case <-reconnected:
	case <-time.After(lifecycleBudget):
		t.Fatal("reconnect did not return")
	}
	require.Zero(t, candidateCount(f.session.control), "no candidate may be left in flight after Close")
}

// TestControlConnInitialDialKeepsDisableCoalesce proves the init dial path keeps
// disabling coalescing on the control connection, and the reconnect path uses the
// session conn config, even though both now flow through dialCandidate.
func TestControlConnInitialDialKeepsDisableCoalesce(t *testing.T) {
	f := newSnapshotFixture(t, func(cluster *ClusterConfig) {
		// Coalescing is only selected when this is positive and disableCoalesce
		// is false, so the two writer types differ only if the flag is honoured.
		cluster.WriteCoalesceWaitTime = 10 * time.Millisecond
	})
	f.drain()

	ch := f.session.control.getConn()
	require.NotNil(t, ch)
	_, ok := ch.conn.w.(*deadlineContextWriter)
	require.True(t, ok, "the init control connection must not coalesce, got %T", ch.conn.w)

	before := ch.conn
	f.session.control.reconnect()
	after := f.session.control.getConn()
	require.NotSame(t, before, after.conn, "the reconnect must have replaced the control connection")
	// The reconnect dials the session conn config, which does not disable
	// coalescing, so with a positive wait time it selects the coalescer. This is
	// the difference dialCandidate must preserve: the init copy sets the flag, the
	// reconnect config does not.
	_, ok = after.conn.w.(*writeCoalescer)
	require.True(t, ok, "the reconnected control connection must use the session config's coalescer, got %T", after.conn.w)
}

// TestControlConnCandidateCleanedUpOnSetupPanic proves the per-attempt deferred
// cleanup closes and releases the candidate when setup panics while unwinding,
// so no connection is leaked even though the panic aborts the reconnect.
func TestControlConnCandidateCleanedUpOnSetupPanic(t *testing.T) {
	var armed atomic.Bool
	f := newSnapshotFixture(t, func(cluster *ClusterConfig) {
		cluster.testControlBeforePublish = func(*HostInfo) {
			if armed.Load() {
				panic("injected setup panic")
			}
		}
	})
	f.drain()

	dialsBefore := f.dialer.dialCount()
	armed.Store(true)

	require.PanicsWithValue(t, "injected setup panic", func() {
		f.session.control.attemptReconnect()
	}, "the injected panic must unwind the reconnect")

	require.Greater(t, f.dialer.dialCount(), dialsBefore, "a candidate must have been dialled")
	candidate := f.dialer.lastConn()
	require.NotNil(t, candidate)
	require.True(t, candidate.transportCloseInvoked.Load(), "the deferred cleanup must have closed the candidate during unwinding")
	require.Zero(t, candidateCount(f.session.control), "the candidate must have been released during unwinding")
}

// TestControlConnInitSetupFailureClosesCandidate proves a failure of the init
// setup closes the candidate and leaves nothing in flight, and CreateSession
// reports the failure.
func TestControlConnInitSetupFailureClosesCandidate(t *testing.T) {
	script, srv, _ := startLocalHostServer(t)
	dialer := newFaultingDialer(srv.Address)
	cluster := newLocalHostCluster(t, "", srv.Address, func(cluster *ClusterConfig, _ string) {
		cluster.HostDialer = dialer
		cluster.HostFilter = HostFilterFunc(func(*HostInfo) bool { return false })
	})
	_ = script

	session, err := cluster.CreateSession()
	if session != nil {
		t.Cleanup(session.Close)
	}
	require.Error(t, err, "CreateSession must fail when the control host is filtered out")

	require.NotZero(t, dialer.dialCount(), "the init must have dialled a candidate")
	for _, fc := range func() []*faultConn {
		dialer.mu.Lock()
		defer dialer.mu.Unlock()
		return append([]*faultConn(nil), dialer.conns...)
	}() {
		require.True(t, fc.transportCloseInvoked.Load(), "every init candidate must be closed on failure")
	}
}

// TestControlConnInitSetupPanicClosesCandidate proves the init path's per-attempt
// cleanup runs when setup panics: the candidate transport is closed while the
// panic unwinds, before CreateSession propagates it.
//
// NewSession calls Session.Close only when init returns an error, not when it
// panics, so this is the case setupCandidate's deferred cleanup exists for.
func TestControlConnInitSetupPanicClosesCandidate(t *testing.T) {
	_, srv, _ := startLocalHostServer(t)
	dialer := newFaultingDialer(srv.Address)
	// Capture the initializing session through the policy's Init, so the candidate
	// set can be inspected after the panic unwinds (CreateSession returns nothing).
	capture := &sessionCapturePolicy{HostSelectionPolicy: RoundRobinHostPolicy()}
	cluster := newLocalHostCluster(t, "", srv.Address, func(cluster *ClusterConfig, _ string) {
		cluster.HostDialer = dialer
		cluster.PoolConfig.HostSelectionPolicy = capture
		// Panic in the init setup, after the candidate is dialled and registered
		// and before it is published.
		cluster.testControlBeforePublish = func(*HostInfo) { panic("injected init setup panic") }
	})

	require.PanicsWithValue(t, "injected init setup panic", func() {
		_, _ = cluster.CreateSession()
	}, "the init setup panic must propagate out of CreateSession")

	conns := func() []*faultConn {
		dialer.mu.Lock()
		defer dialer.mu.Unlock()
		return append([]*faultConn(nil), dialer.conns...)
	}()
	require.NotEmpty(t, conns, "the init must have dialled a candidate")
	for _, fc := range conns {
		require.True(t, fc.transportCloseInvoked.Load(),
			"the init candidate must be closed by the deferred cleanup during the panic unwind")
	}
	session := capture.session.Load()
	require.NotNil(t, session, "the policy must have captured the initializing session")
	require.Zero(t, candidateCount(session.control),
		"the init candidate must have been released from the set during the panic unwind")
}

// sessionCapturePolicy captures the session passed to Init so a test can inspect
// session state after initialization fails.
type sessionCapturePolicy struct {
	HostSelectionPolicy
	session atomic.Pointer[Session]
}

// Init records the session, then forwards to the wrapped policy.
func (p *sessionCapturePolicy) Init(s *Session) {
	p.session.Store(s)
	p.HostSelectionPolicy.Init(s)
}

// TestControlConnCloseBeforeHeartbeatStart proves that when close() latches
// before the heartbeat goroutine starts, the heartbeat's CAS fails and it
// returns at once, the state stays Closing, and reconnect short-circuits.
func TestControlConnCloseBeforeHeartbeatStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &Session{
		ctx:    ctx,
		cancel: cancel,
		logger: &defaultLogger{},
		cfg:    ClusterConfig{},
	}
	c := createControlConn(session)

	// Latch Closing before the heartbeat goroutine ever runs.
	snap := c.latchAndSnapshot()
	require.Nil(t, snap.published)
	require.Empty(t, snap.candidates)
	require.Equal(t, int32(controlConnClosing), atomic.LoadInt32(&c.state))

	// heartBeat's CAS Starting->Started must fail and return immediately.
	done := make(chan struct{})
	go func() {
		c.heartBeat()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(lifecycleBudget):
		t.Fatal("heartBeat did not return after close latched Closing")
	}
	require.Equal(t, int32(controlConnClosing), atomic.LoadInt32(&c.state), "the terminal state must stand")

	// reconnect must short-circuit on the closing state.
	c.reconnect()
	require.Zero(t, atomic.LoadInt32(&c.reconnecting), "reconnect must not have claimed the reconnecting flag")
}

// TestReconnectShutdownOnLastHostSetupFailure proves that a shutdown observed
// while the last host's setup is in flight stops the reconnect with the sentinel:
// it does not fall back to the contact points and does not log the failure.
func TestReconnectShutdownOnLastHostSetupFailure(t *testing.T) {
	var resolved atomic.Int64
	f := newSnapshotFixture(t, func(cluster *ClusterConfig) {
		cluster.testControlBeforeResolve = func() { resolved.Add(1) }
	})
	f.drain()

	// Gate the REGISTER write so the setup of the only ring host parks in flight.
	parkedCh := make(chan *faultConn, 1)
	f.dialer.setGateRegisterNew(parkedCh)
	t.Cleanup(f.dialer.releaseRegisterGates)

	result := make(chan error, 1)
	go func() {
		_, err := f.session.control.attemptReconnect()
		result <- err
	}()

	var candidate *faultConn
	select {
	case candidate = <-parkedCh:
	case <-time.After(lifecycleBudget):
		t.Fatal("the reconnect did not park in the last host's setup")
	}
	attemptsAtPark := f.dialer.dialAttempts.Load()

	// Latch Closing and close the in-flight candidate: its parked REGISTER write
	// then fails, so setupConn returns a real wrapped register error while the
	// controller is closing, and the reconnect stops with the sentinel.
	f.session.control.close()
	f.dialer.releaseRegisterGates()
	require.True(t, candidate.transportCloseInvoked.Load(), "the candidate must have been closed by the shutdown")

	select {
	case err := <-result:
		require.ErrorIs(t, err, errControlConnClosing, "the reconnect must report the sentinel")
	case <-time.After(lifecycleBudget):
		t.Fatal("attemptReconnect did not return")
	}
	require.Equal(t, attemptsAtPark, f.dialer.dialAttempts.Load(), "no contact-point fallback may be dialled")
	require.Zero(t, resolved.Load(), "the contact points must not be resolved once closing")
	require.Empty(t, f.logger.withMessage("During reconnection, control connection setup failed after connecting to host."),
		"a shutdown-driven setup failure must not be logged as a host failure")
}

// TestReconnectShutdownWithEmptyRing proves a reconnect on an empty ring after
// Close returns the sentinel and does not dial the contact points.
func TestReconnectShutdownWithEmptyRing(t *testing.T) {
	var resolved atomic.Int64
	f := newSnapshotFixture(t, func(cluster *ClusterConfig) {
		cluster.testControlBeforeResolve = func() { resolved.Add(1) }
	})
	f.drain()

	// Latch Closing, then empty the ring so attemptReconnect has no host to dial.
	f.session.control.latchAndSnapshot()
	for _, h := range f.session.ring.allHosts() {
		f.session.ring.removeHost(h.HostID())
	}
	attemptsBefore := f.dialer.dialAttempts.Load()

	_, err := f.session.control.attemptReconnect()
	require.ErrorIs(t, err, errControlConnClosing, "a reconnect while closing must report the sentinel")
	require.Equal(t, attemptsBefore, f.dialer.dialAttempts.Load(), "no contact point may be dialled once closing")
	require.Zero(t, resolved.Load(), "the contact points must not be resolved once closing")
}

// TestReconnectShutdownAtFallbackAdmission proves a shutdown seen after the ring
// walk failed and before the contact-point fallback stops the fallback: the
// sentinel is returned and no contact point is dialled.
func TestReconnectShutdownAtFallbackAdmission(t *testing.T) {
	var armed atomic.Bool
	fellBack := make(chan struct{}, 1)
	var resolved atomic.Int64
	var f *snapshotFixture
	// Install the hooks before the session exists so no goroutine reads the config
	// fields concurrently with a test write; gate the body on an atomic flag the
	// test sets later.
	f = newSnapshotFixture(t, func(cluster *ClusterConfig) {
		cluster.testControlBeforeFallback = func() {
			if !armed.Load() {
				return
			}
			select {
			case fellBack <- struct{}{}:
			default:
			}
			// Latch Closing in the admission window the pre-fallback check guards.
			f.session.control.latchAndSnapshot()
		}
		cluster.testControlBeforeResolve = func() { resolved.Add(1) }
	})
	f.drain()

	// Refuse every dial so the ring walk fails with an ordinary error.
	f.dialer.setRefuse(true)
	armed.Store(true)
	_, err := f.session.control.attemptReconnect()
	require.ErrorIs(t, err, errControlConnClosing, "a shutdown at the fallback admission must report the sentinel")
	select {
	case <-fellBack:
	default:
		t.Fatal("the pre-fallback hook did not run")
	}
	// The sentinel alone cannot distinguish "stopped before resolution" from
	// "resolved then stopped": both closing checks return it.
	// The resolve hook, placed after the pre-fallback check and before
	// addrsToHosts, proves the contact points were never resolved.
	require.Zero(t, resolved.Load(), "the contact points must not be resolved once closing")
}
