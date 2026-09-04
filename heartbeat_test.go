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

// Regression test for upstream issue #1919.
// Heartbeat OPTIONS round-trips must not be capped at a tight Session.Timeout
// (which is what c.r.GetTimeout() returns on an established connection); a
// sub-second cap turns transient jitter into the 6-strike failure threshold
// (failures > 5 in Conn.heartBeat) and closes a healthy connection.
// heartbeatTimeout enforces a per-attempt floor.
func TestHeartbeatTimeout_FloorsAtFiveSeconds(t *testing.T) {
	cases := []struct {
		name        string
		connTimeout time.Duration
		want        time.Duration
	}{
		{"zero is floored", 0, heartbeatMinTimeout},
		{"sub-floor 100ms is floored", 100 * time.Millisecond, heartbeatMinTimeout},
		{"sub-floor 1s is floored", 1 * time.Second, heartbeatMinTimeout},
		{"equal-to-floor is unchanged", heartbeatMinTimeout, heartbeatMinTimeout},
		{"above-floor 30s is respected", 30 * time.Second, 30 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := heartbeatTimeout(tc.connTimeout)
			if got != tc.want {
				t.Fatalf("heartbeatTimeout(%v) = %v, want %v", tc.connTimeout, got, tc.want)
			}
		})
	}
}
