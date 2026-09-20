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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"net"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHostInfo_Lookup(t *testing.T) {
	hostLookupPreferV4 = true
	defer func() { hostLookupPreferV4 = false }()

	tests := [...]struct {
		addr string
		ip   net.IP
	}{
		{"127.0.0.1", net.IPv4(127, 0, 0, 1)},
		{"localhost", net.IPv4(127, 0, 0, 1)}, // TODO: this may be host dependant
	}

	for i, test := range tests {
		hosts, err := hostInfo(test.addr, 1)
		if err != nil {
			t.Errorf("%d: %v", i, err)
			continue
		}

		host := hosts[0]
		if !host.ConnectAddress().Equal(test.ip) {
			t.Errorf("expected ip %v got %v for addr %q", test.ip, host.ConnectAddress(), test.addr)
		}
	}
}

// onControlHeartbeatGoroutine reports whether the caller runs on the control
// connection's heartbeat goroutine. Matching the running frame, not the "created by"
// line: the goroutine is started from controlConn.connect, so a "created by" matcher
// would name the wrong function.
func onControlHeartbeatGoroutine() bool {
	buf := make([]byte, 16<<10)
	buf = buf[:runtime.Stack(buf, false)]
	return strings.Contains(string(buf), "(*controlConn).heartBeat")
}

// awaitSuccessfulHeartbeatRound waits until the fixture server has answered two more
// post-startup OPTIONS requests. Those are the control connection's heartbeats and
// nothing else, provided the pool's own heartbeat is turned off.
//
// Two, not one: the second OPTIONS can only be sent after the first round returned its
// interval and the loop re-armed the timer with it, so this is what establishes that the
// five-second interval — and not the initial one-second one — is in force. Without that
// precondition a mutant that keeps the previous interval on a panicking round is
// indistinguishable from one that resets it, which is exactly what a slow start made
// happen when this waited out a fixed two seconds instead.
func awaitSuccessfulHeartbeatRound(t *testing.T, script *localHostServer, mark int32) {
	t.Helper()

	deadline := time.Now().Add(lifecycleBudget)
	for script.heartbeatOptions.Load() < mark+2 {
		if time.Now().After(deadline) {
			t.Fatalf("no two successful heartbeat rounds within %v", lifecycleBudget)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// awaitDialAfter waits for the dialer's attempt counter to move past mark.
func awaitDialAfter(f *snapshotFixture, mark int64, budget time.Duration) bool {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if f.dialer.dialAttempts.Load() > mark {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// TestControl_HeartbeatSurvivesAPanickingRound is the inversion of
// _repro/zz_repro_control_test.go's TestRepro_ControlHeartbeatPanicLeavesNoRetryOwner
// (F-ctrl-1).
//
// The heartbeat goroutine is the only thing that retries a control reconnect after one
// has failed: HandleError reconnects once, acquireConn only while nothing is published,
// and a failed ring refresh does not reconnect at all. Ending the goroutine on the first
// recovered panic — raised by any of the callbacks reconnect reaches — therefore left
// the control connection with no retry owner.
//
// The evidence is a subsequent heartbeat-owned dial, not a stack match: the goroutine is
// created by controlConn.connect, so a "created by" oracle would fail correct code, and
// a running-frame match could be satisfied by another session in the same process.
func TestControl_HeartbeatSurvivesAPanickingRound(t *testing.T) {
	var armed, panicked atomic.Bool
	reconnectDone := make(chan struct{}, 16)

	f := newSnapshotFixture(t, func(cluster *ClusterConfig) {
		// The pool's own heartbeat would otherwise reach the same OPTIONS responder
		// and make the count below say nothing about the control connection.
		cluster.heartbeatInterval = time.Hour
		cluster.HostFilter = HostFilterFunc(func(*HostInfo) bool {
			if armed.Load() && onControlHeartbeatGoroutine() && panicked.CompareAndSwap(false, true) {
				panic("test: application HostFilter panics once")
			}
			return true
		})
		cluster.testControlReconnectDone = func() {
			select {
			case reconnectDone <- struct{}{}:
			default:
			}
		}
	})
	f.drain()

	// Healthy rounds first, observed rather than waited out: sleepTime starts at 1s
	// and only a successful round sets it to 5s.
	awaitSuccessfulHeartbeatRound(t, f.script, f.script.heartbeatOptions.Load())

	before := f.session.control.getConn()
	require.NotNil(t, before, "precondition: a control connection is published")

	armed.Store(true)
	// The control connection dies without an error reaching HandleError, so the
	// heartbeat is what notices: its next OPTIONS write fails and it reconnects on
	// its own goroutine, where the application's HostFilter panics.
	before.conn.Close()

	select {
	case <-reconnectDone:
	case <-time.After(lifecycleBudget):
		t.Fatal("the heartbeat never attempted a reconnect")
	}
	require.True(t, panicked.Load(), "the injected panic must have fired on the heartbeat goroutine")
	dialsAtPanic := f.dialer.dialAttempts.Load()

	// A failed round retries on the fast rhythm, not on the healthy one.
	retried := awaitDialAfter(f, dialsAtPanic, 2*time.Second)
	if !retried {
		t.Errorf("the retry did not come within 2s of the panicking round")
		if !awaitDialAfter(f, dialsAtPanic, 5*time.Second) {
			t.Fatal("no dial within 5s after the panicking round")
		}
	}

	require.Eventually(t, func() bool {
		ch := f.session.control.getConn()
		return ch != nil && !ch.conn.Closed()
	}, lifecycleBudget, 20*time.Millisecond, "the heartbeat must replace the control connection")
	require.Zero(t, atomic.LoadInt32(&f.session.control.reconnecting),
		"the reconnecting claim must be released")
}
