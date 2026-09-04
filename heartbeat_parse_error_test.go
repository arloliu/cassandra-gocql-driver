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

// heartbeatTestInterval is the shortened steady-state heartbeat interval used by the
// event-driven heartbeat tests; the phase is zero so the first heartbeat fires at once.
const heartbeatTestInterval = 200 * time.Millisecond

// zeroHeartbeatPhase is a heartbeatPhase provider that makes the first heartbeat fire immediately.
func zeroHeartbeatPhase(time.Duration) time.Duration { return 0 }

// newHeartbeatSession starts a fake server and a one-connection session
// whose heartbeat OPTIONS responses are written by respond.
//
// With NumConns=1 and disableControlConn=true (testCluster default),
// session setup sends exactly one opOptions (the pool connection's startup negotiation)
// before the heartbeat goroutine starts,
// so the first opOptions is passed through untouched and every later one is a heartbeat handed to respond.
// The returned channel is closed when the nth heartbeat has been answered.
//
// Parameters:
//   - t: The test; the server and session are closed in t.Cleanup.
//   - interval: The steady-state heartbeat interval for the session.
//   - n: The heartbeat count at which the returned channel is closed.
//   - respond: Writes the response to a heartbeat OPTIONS frame.
//
// Returns:
//   - *Session: The connected session.
//   - <-chan struct{}: Closed once n heartbeats have been answered.
func newHeartbeatSession(t *testing.T, interval time.Duration, n uint64, respond func(respFrame *framer, stream int)) (*Session, <-chan struct{}) {
	t.Helper()

	var optionsReceived uint64
	done := make(chan struct{})
	srv := newTestServerOpts{
		addr:     "127.0.0.1:0",
		protocol: defaultProto,
		optionsRespFn: func(respFrame *framer, stream int) bool {
			seq := atomic.AddUint64(&optionsReceived, 1)
			if seq == 1 {
				return false // startup negotiation; pass through
			}
			respond(respFrame, stream)
			if seq-1 == n {
				close(done)
			}
			return true
		},
	}.newServer(t, t.Context())
	t.Cleanup(srv.Stop)

	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = 1
	cluster.Timeout = 5 * time.Second
	cluster.heartbeatInterval = interval
	cluster.heartbeatPhase = zeroHeartbeatPhase
	db, err := cluster.CreateSession()
	require.NoError(t, err, "CreateSession")
	t.Cleanup(db.Close)

	return db, done
}

// waitSignal blocks until ch yields (a send or a close), failing the test when the bound expires.
//
// Parameters:
//   - t: The test; t.Fatalf is called when the bound expires.
//   - ch: The channel to wait on; a send or a close ends the wait.
//   - timeout: The upper bound on the wait.
//   - what: What is being waited for, quoted in the failure message.
func waitSignal(t *testing.T, ch <-chan struct{}, timeout time.Duration, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatalf("timed out after %v waiting for %s", timeout, what)
	}
}

// TestHeartBeat_ParseErrorDoesNotFailConnection asserts that parseFrame
// errors during heartbeat OPTIONS responses do NOT count toward the
// connection's failure threshold: after more than five malformed
// heartbeat responses the connection still serves queries.
//
// The fake server's opOptions response is replaced with a body that
// looks structurally correct (right op code) but has an invalid string
// list (claims 1 entry then no payload), tripping parseSupportedFrame.
func TestHeartBeat_ParseErrorDoesNotFailConnection(t *testing.T) {
	db, done := newHeartbeatSession(t, heartbeatTestInterval, 6, func(respFrame *framer, stream int) {
		respFrame.writeHeader(0, opSupported, stream)
		respFrame.writeShort(1)
	})

	waitSignal(t, done, 10*time.Second, "six malformed heartbeat responses")

	require.NoError(t, db.Query("void").Exec(),
		"query after parse-error heartbeats failed; connection likely closed")
}

// TestHeartBeat_ErrorResponseDoesNotFailConnection asserts the sibling rule:
// a server replying with an error frame to OPTIONS does not count toward the failure threshold.
// The error is logged at Debug, not silently dropped.
func TestHeartBeat_ErrorResponseDoesNotFailConnection(t *testing.T) {
	db, done := newHeartbeatSession(t, heartbeatTestInterval, 6, func(respFrame *framer, stream int) {
		respFrame.writeHeader(0, opError, stream)
		respFrame.writeInt(0x0000) // ServerError code
		respFrame.writeString("synthetic heartbeat error")
	})

	waitSignal(t, done, 10*time.Second, "six error-frame heartbeat responses")

	require.NoError(t, db.Query("void").Exec(),
		"query after server-error heartbeats failed; connection likely closed")
}
