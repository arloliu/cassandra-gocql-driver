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
	"net"
	"testing"
	"time"
)

// TestHostConnPool_ConnectRetriesPerReconnectionPolicy asserts that
// hostConnPool.connect honors the user's configured MaxRetries on dial
// failures.
//
// Before the §7 fix, hostConnPool.connect contained a `(*net.OpError).Temporary()`
// early-break that silently capped retries at 1 for the common dial-failure
// classes (ECONNREFUSED, "no route to host", etc.), regardless of what the
// user configured in ReconnectionPolicy. After the fix, the policy's
// MaxRetries is respected: a fully-failing connect path logs MaxRetries
// "Pool failed to connect to host" warnings (one per failed attempt).
//
// We use an immediately-closed listener to produce a port that reliably
// refuses connections (ECONNREFUSED). The errno-class returns
// Temporary()==false under modern Go (pre-fix would early-break after
// attempt 1 with zero warnings; post-fix produces MaxRetries warnings).
func TestHostConnPool_ConnectRetriesPerReconnectionPolicy(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	refusedAddr := listener.Addr().String()
	listener.Close()

	logger := newChannelLogger()
	const wantAttempts = 3
	cluster := NewCluster(refusedAddr)
	cluster.Logger = logger
	cluster.ProtoVersion = int(defaultProto)
	cluster.ReconnectionPolicy = &ConstantReconnectionPolicy{
		MaxRetries: wantAttempts,
		Interval:   10 * time.Millisecond,
	}
	cluster.Timeout = 500 * time.Millisecond
	cluster.disableControlConn = true

	// CreateSession is expected to fail (no hosts reachable). We assert
	// the retry behavior, not the failure.
	if _, err := cluster.CreateSession(); err == nil {
		t.Fatal("expected CreateSession to fail with unreachable host")
	}

	got := logger.countWarnings("Pool failed to connect to host")
	if got != wantAttempts {
		t.Errorf("retry warnings = %d, want %d (pool.connect must respect ReconnectionPolicy.MaxRetries=%d)",
			got, wantAttempts, wantAttempts)
	}
}
