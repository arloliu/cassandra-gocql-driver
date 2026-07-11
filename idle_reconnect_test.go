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

// idleReconnectStartupCount stands up the fake server, opens a single pool
// connection with a small Session.Timeout, lets it sit idle well past several
// timeout windows, and returns how many STARTUP frames the server saw.
//
// Each (re)connect sends exactly one STARTUP, so the count is a direct,
// log-independent measure of reconnect churn: a healthy idle connection yields
// exactly 1, while the inherited v2.0.0 read-deadline regression (upstream
// CASSGO-125 / 590aabe) makes the connection trip its read deadline while
// waiting for the next frame and reconnect repeatedly, inflating the count.
func idleReconnectStartupCount(t *testing.T, proto protoVersion) int64 {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var startups atomic.Int64
	srv := newTestServerOpts{
		addr:     "127.0.0.1:0",
		protocol: uint8(proto),
		recvHook: func(f *framer) {
			if f.header.op == opStartup {
				startups.Add(1)
			}
		},
	}.newServer(t, ctx)
	defer srv.Stop()

	cluster := testCluster(proto, srv.Address)
	cluster.Timeout = 200 * time.Millisecond
	cluster.NumConns = 1
	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	defer db.Close()

	// Idle far longer than Session.Timeout so the read deadline, if it is
	// (wrongly) armed on idle frame reads, has many chances to fire.
	time.Sleep(1500 * time.Millisecond)

	// Snapshot the reconnect churn accumulated purely from idling, before the
	// query below (which would itself reconnect if the connection had died).
	idleStartups := startups.Load()

	// The connection that survived the idle period must still be usable: a query
	// after the long idle must succeed. This also fails loudly if the idle read
	// left the connection wedged (e.g. the read timeout stuck at 0 or unrestored).
	if err := db.Query("void").Exec(); err != nil {
		t.Fatalf("query after idle failed (connection wedged or dead): %v", err)
	}

	return idleStartups
}

// TestIdleConnectionDoesNotReconnectV4 exercises the pre-v5 processFrame read
// path. Regression guard for the "repeated Pool connection error" churn.
func TestIdleConnectionDoesNotReconnectV4(t *testing.T) {
	if got := idleReconnectStartupCount(t, protoVersion4); got != 1 {
		t.Fatalf("idle connection reconnected: saw %d STARTUP frames, want 1 (read deadline is firing on idle frame reads)", got)
	}
}

// TestIdleConnectionDoesNotReconnectV5 exercises the proto-v5 recvSegment read
// path (the path otter-cache uses in production).
func TestIdleConnectionDoesNotReconnectV5(t *testing.T) {
	if got := idleReconnectStartupCount(t, protoVersion5); got != 1 {
		t.Fatalf("idle connection reconnected: saw %d STARTUP frames, want 1 (read deadline is firing on idle segment reads)", got)
	}
}
