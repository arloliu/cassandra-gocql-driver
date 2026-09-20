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
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// capturingInitPolicy records the session it is initialised with, so a test can reach
// a session NewSession never returned, and optionally panics from Init.
type capturingInitPolicy struct {
	HostSelectionPolicy
	session     atomic.Pointer[Session]
	panicWith   any
	beforeRaise func(*Session)
}

func (p *capturingInitPolicy) Init(s *Session) {
	p.session.Store(s)
	p.HostSelectionPolicy.Init(s)
	if p.panicWith != nil {
		if p.beforeRaise != nil {
			p.beforeRaise(s)
		}
		panic(p.panicWith)
	}
}

// capturingReadyListener records the session and panics from OnSessionReady.
type capturingReadyListener struct {
	session     atomic.Pointer[Session]
	panicWith   any
	beforeRaise func(*Session)
}

func (l *capturingReadyListener) OnSessionReady(s *Session) {
	l.session.Store(s)
	if l.beforeRaise != nil {
		l.beforeRaise(s)
	}
	if l.panicWith != nil {
		panic(l.panicWith)
	}
}

// infoPanicOnceLogger panics on one Info message and works otherwise. It is used to
// panic on init's final line without disturbing the rest of initialisation.
type infoPanicOnceLogger struct {
	StructuredLogger
	match  string
	armed  atomic.Bool
	panics atomic.Int32
}

func (l *infoPanicOnceLogger) Info(msg string, fields ...LogField) {
	if msg == l.match && l.armed.CompareAndSwap(true, false) {
		l.panics.Add(1)
		panic("test: logger panic on init's final line")
	}
	l.StructuredLogger.Info(msg, fields...)
}

// requireSessionTornDown asserts the three workers Session.Close stops have stopped and
// that the session reports itself closed.
func requireSessionTornDown(t *testing.T, s *Session) {
	t.Helper()

	require.NotNil(t, s, "the test never captured the session NewSession did not return")
	for name, done := range map[string]chan struct{}{
		"the schema refresher": s.schemaDescriber.schemaRefresher.done,
		"the event debouncer":  s.nodeEvents.done,
		"the ring refresher":   s.ringRefresher.done,
	} {
		select {
		case <-done:
		case <-time.After(fillEventBudget):
			t.Fatalf("%d worker(s) still running after NewSession panicked: %s was not stopped", 3, name)
		}
	}
	require.True(t, s.Closed(), "the session must report itself closed")
}

// newGuardCluster returns a cluster pointed at a fixture server, with one connection so
// that the transport inventory is deterministic.
func newGuardCluster(t *testing.T, dialer *inventoryDialer, tune func(*ClusterConfig)) *ClusterConfig {
	t.Helper()

	srv := NewTestServer(t, defaultProto, testServerContext(t))
	t.Cleanup(srv.Stop)

	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = 1
	if dialer != nil {
		dialer.inner = &defaultHostDialer{dialer: &net.Dialer{Timeout: 5 * time.Second}}
		cluster.HostDialer = dialer
	}
	if tune != nil {
		tune(cluster)
	}
	return cluster
}

// TestNewSession_PanicDuringInitClosesTheSession is the inversion of
// _repro/zz_repro_round6_test.go's TestRepro_SessionReadyPanicLeavesLiveSessionUnowned
// (F-sess-2).
//
// NewSession closed the session only when init RETURNED an error. A panic out of
// policy.Init, out of buildPool or out of init itself unwound it with the three
// debouncers, the scheduler and the pools running and no handle returned, so nothing
// could ever close them.
//
// The oracle is the three workers' done channels and Closed(), not a process-wide
// goroutine count: testify's Eventually runs its condition on a new goroutine, so a
// total can neither be compared safely nor tell a leaked worker from an unrelated exit.
func TestNewSession_PanicDuringInitClosesTheSession(t *testing.T) {
	t.Run("a panicking SessionReadyListener", func(t *testing.T) {
		listener := &capturingReadyListener{panicWith: "test: OnSessionReady panic"}
		dialer := &inventoryDialer{}
		cluster := newGuardCluster(t, dialer, func(c *ClusterConfig) {
			c.Metadata.SessionReadyListener = listener
		})

		var captured *Session
		t.Cleanup(func() {
			if captured != nil {
				captured.Close()
			}
		})
		require.Panics(t, func() { _, _ = cluster.CreateSession() },
			"the listener's panic must still reach the caller")
		captured = listener.session.Load()

		requireSessionTornDown(t, captured)
		requireTransportsClosed(t, dialer)
	})

	t.Run("a panicking policy Init", func(t *testing.T) {
		policy := &capturingInitPolicy{
			HostSelectionPolicy: RoundRobinHostPolicy(),
			panicWith:           "test: policy.Init panic",
		}
		dialer := &inventoryDialer{}
		cluster := newGuardCluster(t, dialer, func(c *ClusterConfig) {
			c.PoolConfig.HostSelectionPolicy = policy
		})

		var captured *Session
		t.Cleanup(func() {
			if captured != nil {
				captured.Close()
			}
		})
		require.Panics(t, func() { _, _ = cluster.CreateSession() },
			"the policy's panic must still reach the caller")
		captured = policy.session.Load()

		requireSessionTornDown(t, captured)
	})

	t.Run("a panicking logger on init's final line", func(t *testing.T) {
		logger := &infoPanicOnceLogger{
			StructuredLogger: newTestLogger(LogLevelDebug),
			match:            "Session initialized successfully.",
		}
		listener := &capturingReadyListener{}
		dialer := &inventoryDialer{}
		cluster := newGuardCluster(t, dialer, func(c *ClusterConfig) {
			c.Logger = logger
			c.Metadata.SessionReadyListener = listener
		})
		logger.armed.Store(true)

		var captured *Session
		t.Cleanup(func() {
			if captured != nil {
				captured.Close()
			}
		})
		require.Panics(t, func() { _, _ = cluster.CreateSession() },
			"the logger's panic must still reach the caller")
		require.EqualValues(t, 1, logger.panics.Load(), "the final line must have been reached")
		captured = listener.session.Load()

		requireSessionTornDown(t, captured)
		requireTransportsClosed(t, dialer)
	})
}

// requireTransportsClosed freezes the dialer's inventory and asserts every transport in
// it is closed. It is called only after the workers' done channels have been received,
// which is the barrier: Session.Close stops them after it has closed the pools.
func requireTransportsClosed(t *testing.T, dialer *inventoryDialer) {
	t.Helper()

	dialer.mu.Lock()
	defer dialer.mu.Unlock()
	require.NotEmpty(t, dialer.conns, "the fixture must have dialled at least one transport")
	for i, conn := range dialer.conns {
		require.True(t, conn.closed.Load(), "transport %d was left open after the guard ran", i)
	}
}

// TestNewSession_SecondaryPanicDuringCleanup covers the guarantee the plan adopts: the
// original panic is preserved, the cleanup is attempted, and the driver's own teardown
// continues past the application callbacks the branch isolates.
//
// "A logger that panics during Close" does not by itself arrange a logger call:
// closeWithError reaches the error handler only when the transport's Close reports an
// error, and a closing host pool returns from its error handler before logging. The
// deterministic path is the control connection's error handler, whose warning the
// control-log isolation covers. The fixture server answers no system.local rows, so the
// control connection is built over a real dialled connection and published from the
// listener, immediately before it raises the original panic.
func TestNewSession_SecondaryPanicDuringCleanup(t *testing.T) {
	const originalPanic = "test: the original OnSessionReady panic"

	logger := &controlWarnPanicLogger{StructuredLogger: newTestLogger(LogLevelDebug)}
	listener := &capturingReadyListener{panicWith: originalPanic}
	dialer := &inventoryDialer{}

	srv := NewTestServer(t, defaultProto, testServerContext(t))
	t.Cleanup(srv.Stop)
	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = 1
	dialer.inner = &errorOnCloseDialer{inner: &defaultHostDialer{dialer: &net.Dialer{Timeout: 5 * time.Second}}}
	cluster.HostDialer = dialer
	cluster.Logger = logger
	cluster.Metadata.SessionReadyListener = listener

	listener.beforeRaise = func(s *Session) {
		control := createControlConn(s)
		cfg := *s.connCfg
		host := s.ring.allHosts()[0]
		conn, err := s.dial(s.ctx, host, &cfg, control)
		require.NoError(t, err, "dial the connection the control connection will own")
		control.conn.Store(&connHost{conn: conn, host: host})
		s.control = control
		logger.armed.Store(true)
	}

	// Registered before any fatal assertion, so a failing run cannot contaminate the
	// tests after it — but the resource assertions below run first, or this fallback
	// would hide what the guard left open.
	var captured *Session
	t.Cleanup(func() {
		if captured != nil {
			captured.Close()
		}
	})

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = cluster.CreateSession()
	}()
	captured = listener.session.Load()

	require.Equal(t, originalPanic, recovered,
		"the value the caller sees must be the original panic, not the cleanup's")
	require.EqualValues(t, 1, logger.panics.Load(),
		"the control error handler's log must have been reached and panicked")

	requireSessionTornDown(t, captured)
	requireTransportsClosed(t, dialer)
}

// TestNewSession_CleanupPanicDoesNotReplaceTheOriginal establishes preservation only:
// when the cleanup itself panics, the value the caller sees is still the one init
// raised.
//
// No application callback can produce that failure any more — the branch isolates every
// one the cleanup reaches — so the failure is injected through s.cancel, a function
// field a same-package test can replace. The wrapper is installed synchronously through
// the captured session, in policy.Init, immediately before the original panic is
// raised.
func TestNewSession_CleanupPanicDoesNotReplaceTheOriginal(t *testing.T) {
	const (
		originalPanic = "test: the original policy.Init panic"
		cleanupPanic  = "test: the injected cleanup panic"
	)

	var realCancel context.CancelFunc
	policy := &capturingInitPolicy{
		HostSelectionPolicy: RoundRobinHostPolicy(),
		panicWith:           originalPanic,
	}
	policy.beforeRaise = func(s *Session) {
		// Saved, so the fallback below can finish the cleanup this wrapper aborts.
		realCancel = s.cancel
		s.cancel = func() { panic(cleanupPanic) }
	}

	srv := NewTestServer(t, defaultProto, testServerContext(t))
	t.Cleanup(srv.Stop)
	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = 1
	cluster.PoolConfig.HostSelectionPolicy = policy

	// Registered first: the injected panic lands after Close has latched isClosing, so
	// restoring s.cancel and calling Close again cannot finish the cleanup. The
	// captured workers are stopped explicitly instead.
	var captured *Session
	t.Cleanup(func() {
		if captured == nil {
			return
		}
		if realCancel != nil {
			captured.cancel = realCancel
			realCancel()
		}
		captured.schemaDescriber.schemaRefresher.stop()
		captured.nodeEvents.stop()
		captured.ringRefresher.stop()
	})

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = cluster.CreateSession()
	}()
	captured = policy.session.Load()
	require.NotNil(t, captured, "the test never captured the session")

	if recovered != originalPanic {
		t.Fatalf("NewSession panicked with %v, want %v", recovered, originalPanic)
	}
}

// TestNewSession_PanicDoesNotHangOnAStrandedPolicyMutex is the hang regression the
// guard needs. The deciding outcome is that panic propagation completes rather than the
// test timing out.
//
// Before the token-aware policy's scoped unlock and its isolated warning landed, the
// guard turned a leak into a hang: a stranded t.mu blocks a worker Session.Close joins,
// and stop() has no deadline — cancelling the session context interrupts neither a mutex
// acquisition nor a callback already running.
//
// The schedule separates two events rather than racing them:
//
//  1. the real F-pol-1 warning path — an unsupported partitioner and a panicking logger
//     — is driven on a background policy operation the test owns, recovers and joins, so
//     the mutex question is decided by the token-aware policy's own code and nothing
//     else;
//  2. a ring refresh is started behind it, so a worker Close joins is the one waiting on
//     t.mu;
//  3. the panic is raised independently, from OnSessionReady, which is what unwinds
//     NewSession into the guard.
//
// OnSessionReady runs after initialisation's own synchronous work, so the sequence
// cannot be blocked by a refresh that init itself started.
func TestNewSession_PanicDoesNotHangOnAStrandedPolicyMutex(t *testing.T) {
	const peerHostID = "dddddddd-0000-4000-8000-00000000cafe"

	script, srv, _ := startLocalHostServer(t)
	logger := &armableWarnPanicLogger{StructuredLogger: newTestLogger(LogLevelDebug)}
	listener := &capturingReadyListener{panicWith: "test: OnSessionReady panic"}
	policy := TokenAwareHostPolicy(RoundRobinHostPolicy()).(*tokenAwareHostPolicy)
	refreshEntered := make(chan struct{}, 8)
	admissionReached := make(chan struct{}, 8)

	cluster := newLocalHostCluster(t, "", srv.Address, func(c *ClusterConfig, _ string) {
		c.Logger = logger
		c.PoolConfig.HostSelectionPolicy = policy
		c.Metadata.SessionReadyListener = listener
		c.testRingRefreshHook = func() {
			select {
			case refreshEntered <- struct{}{}:
			default:
			}
		}
		// The barrier is the round reaching the new peer's admission, not the round
		// starting: the start hook fires before the ring data is even fetched, so a
		// panic there races the round into its first query and the round dies on the
		// cancelled session context without ever touching the policy.
		c.testCompleteAdmissionStart = func(h *HostInfo) {
			if h.HostID() != peerHostID {
				return
			}
			select {
			case admissionReached <- struct{}{}:
			default:
			}
		}
	})

	listener.beforeRaise = func(s *Session) {
		// 1. The warning path, on a background operation. It panics, so it is
		//    recovered and joined here; whether it left t.mu held is the subject.
		policy.SetPartitioner("com.example.UnsupportedPartitioner")
		logger.armed.Store(true)
		background := make(chan struct{})
		go func() {
			defer close(background)
			defer func() { _ = recover() }()
			policy.AddHost((&HostInfo{
				connectAddress: net.IPv4(10, 0, 0, 9),
				tokens:         []string{"00"},
			}).withIdentity("background", "", ""))
		}()
		<-background
		require.EqualValues(t, 1, logger.panics.Load(), "the warning path must have panicked exactly once")

		// 2. A ring refresh behind it. Adopting a new peer takes the round into the
		//    policy, which is where t.mu is acquired.
		script.setPeers([]peerRow{newPeerRow(peerHostID, "127.0.0.2")})
		s.ringRefresher.trigger()
		select {
		case <-refreshEntered:
		case <-time.After(fillEventBudget):
			t.Error("the ring refresh never started")
		}
		select {
		case <-admissionReached:
		case <-time.After(fillEventBudget):
			t.Error("the ring refresh never reached the new peer's admission")
		}
	}

	// 3. The panic, and the deciding observation: the call returns at all.
	done := make(chan any, 1)

	// Registered before construction starts, so a run that fails the deciding
	// observation still tidies up rather than leaving the package to the timeout.
	// Every step is bounded: on a tree where t.mu can be stranded the constructor and
	// the refresher are blocked on a production mutex that no test can release, so the
	// most this can do is stop waiting for them and say so.
	t.Cleanup(func() {
		if captured := listener.session.Load(); captured != nil {
			closed := make(chan struct{})
			go func() {
				defer close(closed)
				defer func() { _ = recover() }()
				captured.Close()
			}()
			select {
			case <-closed:
			case <-time.After(2 * time.Second):
				t.Log("cleanup: Session.Close did not return; a stranded policy mutex holds it")
			}
		}
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Log("cleanup: the constructor goroutine never returned and is abandoned")
		}
	})

	go func() {
		defer func() { done <- recover() }()
		_, _ = cluster.CreateSession()
	}()

	select {
	case recovered := <-done:
		// Put back for the cleanup above, which reads it too.
		done <- recovered
		require.Equal(t, "test: OnSessionReady panic", recovered,
			"the original panic must still reach the caller")
	case <-time.After(10 * time.Second):
		t.Fatal("NewSession did not complete within 10s: the guard is waiting on a stranded policy mutex")
	}

	if captured := listener.session.Load(); captured != nil {
		requireSessionTornDown(t, captured)
	}
}
