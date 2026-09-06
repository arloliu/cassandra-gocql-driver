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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestHeartbeatPhase_WithinHalfInterval asserts that the default first-heartbeat phase
// always lies in [interval/2, interval).
func TestHeartbeatPhase_WithinHalfInterval(t *testing.T) {
	const interval = heartbeatInterval
	for range 1000 {
		phase := defaultHeartbeatPhase(interval)
		require.GreaterOrEqual(t, phase, interval/2)
		require.Less(t, phase, interval)
	}
}

// TestHeartBeat_FirstIntervalIsSteadyState asserts that the heartbeat cadence
// is the configured interval from the first heartbeat on,
// even when no heartbeat has ever succeeded.
// Every heartbeat is answered with an error frame;
// the old loop stayed at a 1 s cadence until the first supportedFrame,
// which would place the fourth heartbeat at about 4 s instead of under 1 s.
func TestHeartBeat_FirstIntervalIsSteadyState(t *testing.T) {
	_, done := newHeartbeatSession(t, 300*time.Millisecond, 4, func(respFrame *framer, stream int) {
		respFrame.writeHeader(0, opError, stream)
		respFrame.writeInt(0x0000) // ServerError code
		respFrame.writeString("synthetic heartbeat error")
	})

	waitSignal(t, done, 2*time.Second, "four heartbeats at a 300ms interval")
}

// TestHeartBeat_UsesInjectedFirstPhase asserts that a connection's first heartbeat wait
// is the value returned by the injected phase provider and that the phase is never re-applied.
//
// Rendezvous: both connections of a two-connection pool block inside the phase provider,
// which proves both startups are complete (heartBeat starts after init),
// so every OPTIONS counted after the release is a heartbeat.
// The provider returns 0 to the first caller and 2 s to the second;
// a loop that overwrote the phase with the 10 s interval would send nothing within 3 s.
func TestHeartBeat_UsesInjectedFirstPhase(t *testing.T) {
	const steadyInterval = 10 * time.Second

	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)

	var (
		phaseCalls  atomic.Int32
		gotInterval atomic.Int64
		counting    atomic.Bool
		heartbeats  atomic.Int32
	)
	first := make(chan struct{})
	second := make(chan struct{})

	srv := newTestServerOpts{
		addr:     "127.0.0.1:0",
		protocol: defaultProto,
		optionsRespFn: func(respFrame *framer, stream int) bool {
			if !counting.Load() {
				return false // startup negotiation; not a heartbeat
			}
			switch heartbeats.Add(1) {
			case 1:
				close(first)
			case 2:
				close(second)
			}
			return false // default opSupported reply
		},
	}.newServer(t, t.Context())
	t.Cleanup(srv.Stop)

	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = 2
	cluster.Timeout = 5 * time.Second
	cluster.heartbeatInterval = steadyInterval
	cluster.heartbeatPhase = func(interval time.Duration) time.Duration {
		gotInterval.Store(int64(interval))
		n := phaseCalls.Add(1)
		arrived <- struct{}{}
		<-release
		if n == 1 {
			return 0
		}
		return 2 * time.Second
	}
	db, err := cluster.CreateSession()
	require.NoError(t, err, "CreateSession")
	t.Cleanup(db.Close)

	waitSignal(t, arrived, 5*time.Second, "first connection to reach the phase provider")
	waitSignal(t, arrived, 5*time.Second, "second connection to reach the phase provider")
	require.Equal(t, steadyInterval, time.Duration(gotInterval.Load()))

	counting.Store(true)
	releaseOnce()

	waitSignal(t, first, 3*time.Second, "first heartbeat (phase 0)")

	negCtx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	select {
	case <-second:
		t.Fatal("second heartbeat fired within 500ms of the first; the 2s phase was not honoured")
	case <-negCtx.Done():
	}

	waitSignal(t, second, 5*time.Second, "second heartbeat (phase 2s)")
	require.EqualValues(t, 2, phaseCalls.Load())
}

// TestResolveHeartbeatTimeout_ZeroOrNegativeSelectsDefault pins the zero-value
// contract of ClusterConfig.HeartbeatTimeout: unset takes the default, and every
// positive value is used exactly as configured.
//
// Historical context (upstream issue #1919): this resolution used to be
// max(5s, Session.Timeout), a floor that stopped a sub-second Session.Timeout
// from capping heartbeat OPTIONS and tripping the six-strike threshold on a
// healthy connection. The heartbeat no longer reads Session.Timeout at all, so
// the floor is gone; a short HeartbeatTimeout is honoured and its risk is
// documented on the field instead of corrected here.
func TestResolveHeartbeatTimeout_ZeroOrNegativeSelectsDefault(t *testing.T) {
	cases := []struct {
		name       string
		configured time.Duration
		want       time.Duration
	}{
		{"zero selects the default", 0, heartbeatDefaultTimeout},
		{"negative selects the default", -1 * time.Second, heartbeatDefaultTimeout},
		{"sub-default 100ms is honoured", 100 * time.Millisecond, 100 * time.Millisecond},
		{"equal-to-default is unchanged", heartbeatDefaultTimeout, heartbeatDefaultTimeout},
		{"above-default 30s is honoured", 30 * time.Second, 30 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, resolveHeartbeatTimeout(tc.configured))
		})
	}
}

// TestNewCluster_HeartbeatTimeoutDefault asserts NewCluster exposes the effective
// default on the field, so a caller printing its ClusterConfig sees the value it
// will actually get rather than a bare zero.
func TestNewCluster_HeartbeatTimeoutDefault(t *testing.T) {
	require.Equal(t, heartbeatDefaultTimeout, NewCluster("127.0.0.1").HeartbeatTimeout)
}

// TestConnConfig_HeartbeatTimeoutIgnoresSessionTimeout is the regression that
// pins the property this change introduces: the heartbeat timeout no longer
// derives from Session.Timeout.
//
// The config is built by hand rather than through NewCluster precisely because
// NewCluster fills HeartbeatTimeout in; only a hand-built config exercises the
// zero-value resolution, which is the path a caller assembling a ClusterConfig
// literal takes.
func TestConnConfig_HeartbeatTimeoutIgnoresSessionTimeout(t *testing.T) {
	cfg := &ClusterConfig{Timeout: 30 * time.Second, ConnectTimeout: 30 * time.Second}

	connCfg, err := connConfig(cfg)
	require.NoError(t, err, "connConfig")
	require.Equal(t, heartbeatDefaultTimeout, connCfg.heartbeatTimeout,
		"a 30s Session.Timeout must not stretch the heartbeat timeout")
}

// TestConnConfig_HeartbeatTimeoutHonoursExplicitValue asserts a configured value
// reaches the connection unmodified, with no floor applied on the way.
func TestConnConfig_HeartbeatTimeoutHonoursExplicitValue(t *testing.T) {
	cfg := &ClusterConfig{Timeout: 30 * time.Second, HeartbeatTimeout: 200 * time.Millisecond}

	connCfg, err := connConfig(cfg)
	require.NoError(t, err, "connConfig")
	require.Equal(t, 200*time.Millisecond, connCfg.heartbeatTimeout)
}

// TestHeartBeat_ClosesAfterSixUnansweredOptions asserts the connection closes on
// the sixth unanswered heartbeat and that the wait between attempts is
// HeartbeatTimeout, not Session.Timeout.
//
// The discriminator is the pair of assertions, not either one alone. A build
// that reverted to Session.Timeout would still close on the sixth failure, so
// the count alone proves nothing; it would take 5*30s+30s = 180s to do it, so
// the bound is what separates the two. With HeartbeatTimeout at 50ms, a 10ms
// interval and a zero phase the expected close is about 300ms.
//
// The server stops answering OPTIONS only after startup negotiation, so
// CreateSession still completes and every swallowed frame is a heartbeat. The
// counter belongs to the first accepted connection: the pool refills after the
// close and the replacement heartbeats against its own counter.
func TestHeartBeat_ClosesAfterSixUnansweredOptions(t *testing.T) {
	srv := newTestServerOpts{addr: "127.0.0.1:0", protocol: defaultProto}.newServer(t, t.Context())
	t.Cleanup(srv.Stop)

	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = 1
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 5 * time.Second
	cluster.HeartbeatTimeout = 50 * time.Millisecond
	cluster.heartbeatInterval = 10 * time.Millisecond
	cluster.heartbeatPhase = zeroHeartbeatPhase

	session, err := cluster.CreateSession()
	require.NoError(t, err, "CreateSession")
	t.Cleanup(session.Close)

	hosts := session.ring.allHosts()
	require.Len(t, hosts, 1, "the fake server must be the only host")
	hostPool, ok := session.pool.getPool(hosts[0])
	require.True(t, ok, "the session must hold a pool for the fake server")
	conn := hostPool.Pick()
	require.NotNil(t, conn, "the pool must hold a connection")

	counter := srv.firstOptionCounter()
	require.NotNil(t, counter, "the server must have accepted a connection")

	srv.swallowPostStartupOptions.Store(true)

	require.Eventually(t, conn.Closed, 5*time.Second, 5*time.Millisecond,
		"the connection must close once its heartbeats go unanswered; "+
			"a heartbeat timeout still taken from Session.Timeout would need about 180s")
	require.EqualValues(t, 6, counter.Load(),
		"the connection must close on the sixth consecutive heartbeat failure")
}
