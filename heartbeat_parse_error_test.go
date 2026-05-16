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
)

// TestHeartBeat_ParseErrorDoesNotFailConnection asserts that parseFrame
// errors during heartbeat OPTIONS responses do NOT count toward the
// connection's failure threshold. Before the §8 fix, 5 malformed
// heartbeat responses within ~5 seconds would force-close the
// connection (failures > 5 path). After the fix, parse errors are
// logged as transient and the connection survives indefinitely.
//
// The test drives the parse-error path via the optionsRespFn hook:
// the fake server's opOptions response is replaced with a body that
// looks structurally correct (right op code) but has an invalid string
// list (claims 1 entry then no payload), tripping parseSupportedFrame.
func TestHeartBeat_ParseErrorDoesNotFailConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Counter pattern: with NumConns=1 and disableControlConn=true
	// (testCluster default), the session setup sends exactly 1 opOptions
	// (the pool conn's startup negotiation). All subsequent opOptions
	// come from the heartbeat. We let the first call through cleanly so
	// session setup succeeds, then corrupt every subsequent response.
	// This guarantees no clean heartbeat ever fires, so sleepTime stays
	// at 1s and we deterministically see 5+ heartbeats in a 7s window.
	var optionsReceived uint64
	var heartbeatCount uint64
	srv := newTestServerOpts{
		addr:     "127.0.0.1:0",
		protocol: defaultProto,
		optionsRespFn: func(respFrame *framer, stream int) bool {
			n := atomic.AddUint64(&optionsReceived, 1)
			if n == 1 {
				return false // first call is startup; pass through
			}
			atomic.AddUint64(&heartbeatCount, 1)
			// Heartbeats expect *supportedFrame. We send opSupported
			// with a malformed stringMultiMap body — claims 1 entry
			// then truncates. parseSupportedFrame errors.
			respFrame.writeHeader(0, opSupported, stream)
			respFrame.writeShort(1)
			return true
		},
	}.newServer(t, ctx)
	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = 1
	cluster.Timeout = 5 * time.Second
	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	defer db.Close()

	// Wait long enough for >5 heartbeats. The heartbeat timer is 1s on
	// failure (sleepTime never changes from 1s since no clean heartbeat
	// fires after the first startup OPTIONS). 7s gives us ~6 heartbeats
	// with margin.
	const waitDuration = 7 * time.Second
	time.Sleep(waitDuration)

	got := atomic.LoadUint64(&heartbeatCount)
	if got < 5 {
		t.Errorf("expected >=5 heartbeat opOptions calls in %v, got %d", waitDuration, got)
	}

	// The connection must still be alive: a query should succeed.
	if err := db.Query("void").Exec(); err != nil {
		t.Fatalf("query after parse-error heartbeats failed; connection likely closed: %v", err)
	}
}

// TestHeartBeat_ErrorResponseDoesNotFailConnection asserts the §8 sibling
// fix: a server replying with an error frame to OPTIONS does not count
// toward the failure threshold. The error is logged at Debug, not
// silently dropped (the prior TODO behavior).
func TestHeartBeat_ErrorResponseDoesNotFailConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Same counter pattern as the parse-error test above: let the first
	// opOptions through for startup, corrupt all subsequent (heartbeat)
	// responses. Guarantees deterministic 1s heartbeat cadence.
	var optionsReceived uint64
	var heartbeatCount uint64
	srv := newTestServerOpts{
		addr:     "127.0.0.1:0",
		protocol: defaultProto,
		optionsRespFn: func(respFrame *framer, stream int) bool {
			n := atomic.AddUint64(&optionsReceived, 1)
			if n == 1 {
				return false
			}
			atomic.AddUint64(&heartbeatCount, 1)
			// Reply with a structured error frame instead of
			// opSupported — exercises the `case error:` branch.
			respFrame.writeHeader(0, opError, stream)
			respFrame.writeInt(0x0000) // ServerError code
			respFrame.writeString("synthetic heartbeat error")
			return true
		},
	}.newServer(t, ctx)
	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = 1
	cluster.Timeout = 5 * time.Second
	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	defer db.Close()

	const waitDuration = 7 * time.Second
	time.Sleep(waitDuration)

	got := atomic.LoadUint64(&heartbeatCount)
	if got < 5 {
		t.Errorf("expected >=5 heartbeat opOptions calls in %v, got %d", waitDuration, got)
	}

	if err := db.Query("void").Exec(); err != nil {
		t.Fatalf("query after server-error heartbeats failed; connection likely closed: %v", err)
	}
}
