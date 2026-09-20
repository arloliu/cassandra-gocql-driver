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
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/internal/streams"
)

// TestConnConfigForwardsMaxStreams verifies MaxStreams flows ClusterConfig ->
// ConnConfig, and that the resulting stream table and callMap are sized from it:
// the zero-value default yields a 2048-entry table (16 KiB callMap) and -1 restores
// the protocol maximum.
func TestConnConfigForwardsMaxStreams(t *testing.T) {
	cases := []struct {
		name       string
		maxStreams int
		want       int
	}{
		{"default zero -> 2048", 0, 2048},
		{"negative -> proto max", -1, 32768},
		{"explicit value", 4096, 4096},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cc, err := connConfig(&ClusterConfig{ProtoVersion: protoVersion5, MaxStreams: tc.maxStreams})
			if err != nil {
				t.Fatalf("connConfig: %v", err)
			}
			if cc.MaxStreams != tc.maxStreams {
				t.Fatalf("ConnConfig.MaxStreams = %d, want %d", cc.MaxStreams, tc.maxStreams)
			}
			gen := streams.New(cc.ProtoVersion, cc.MaxStreams)
			if gen.NumStreams != tc.want {
				t.Fatalf("NumStreams = %d, want %d", gen.NumStreams, tc.want)
			}
			if cm := newCallMap(gen.NumStreams); len(cm.entries) != tc.want {
				t.Fatalf("callMap entries = %d, want %d", len(cm.entries), tc.want)
			}
		})
	}
}

// TestHostConnPoolPickSkipsSaturatedConns verifies Pick ignores connections with no
// available streams and returns nil when the whole pool is saturated — the receive-
// side counterpart to a smaller MaxStreams table.
func TestHostConnPoolPickSkipsSaturatedConns(t *testing.T) {
	saturated := func() *Conn {
		g := streams.New(protoVersion5, 128)
		for {
			if _, ok := g.GetStream(); !ok {
				break
			}
		}
		return &Conn{streams: g}
	}

	full := saturated()
	if full.AvailableStreams() != 0 {
		t.Fatalf("saturated conn reports %d available streams, want 0", full.AvailableStreams())
	}

	// All connections saturated -> Pick returns nil (size == pool.size, so no fill).
	allFull := &hostConnPool{conns: []*Conn{full}, size: 1}
	if got := allFull.Pick(); got != nil {
		t.Fatalf("Pick on a fully saturated pool = %v, want nil", got)
	}

	// A connection with capacity is chosen over the saturated one.
	open := &Conn{streams: streams.New(protoVersion5, 128)}
	if open.AvailableStreams() == 0 {
		t.Fatal("expected the open conn to report available streams")
	}
	pool := &hostConnPool{conns: []*Conn{full, open}, size: 2}
	if got := pool.Pick(); got != open {
		t.Fatalf("Pick = %v, want the open conn", got)
	}
}

func TestSetupTLSConfig(t *testing.T) {
	tests := []struct {
		name                       string
		opts                       *SslOptions
		expectedInsecureSkipVerify bool
	}{
		{
			name: "Config nil, EnableHostVerification false",
			opts: &SslOptions{
				EnableHostVerification: false,
			},
			expectedInsecureSkipVerify: true,
		},
		{
			name: "Config nil, EnableHostVerification true",
			opts: &SslOptions{
				EnableHostVerification: true,
			},
			expectedInsecureSkipVerify: false,
		},
		{
			name: "Config.InsecureSkipVerify false, EnableHostVerification false",
			opts: &SslOptions{
				EnableHostVerification: false,
				Config: &tls.Config{
					InsecureSkipVerify: false,
				},
			},
			expectedInsecureSkipVerify: false,
		},
		{
			name: "Config.InsecureSkipVerify true, EnableHostVerification false",
			opts: &SslOptions{
				EnableHostVerification: false,
				Config: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
			expectedInsecureSkipVerify: true,
		},
		{
			name: "Config.InsecureSkipVerify false, EnableHostVerification true",
			opts: &SslOptions{
				EnableHostVerification: true,
				Config: &tls.Config{
					InsecureSkipVerify: false,
				},
			},
			expectedInsecureSkipVerify: false,
		},
		{
			name: "Config.InsecureSkipVerify true, EnableHostVerification true",
			opts: &SslOptions{
				EnableHostVerification: true,
				Config: &tls.Config{
					InsecureSkipVerify: true,
				},
			},
			expectedInsecureSkipVerify: false,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			tlsConfig, err := setupTLSConfig(test.opts)
			if err != nil {
				t.Fatalf("unexpected error %q", err.Error())
			}
			if tlsConfig.InsecureSkipVerify != test.expectedInsecureSkipVerify {
				t.Fatalf("got %v, but expected %v", tlsConfig.InsecureSkipVerify,
					test.expectedInsecureSkipVerify)
			}
		})
	}
}

// TestPool_StreamObserverPanicDoesNotAbortPoolClose is the inversion of
// _repro/zz_repro_conn_stranded_test.go's
// TestRepro_C1b_PanickingStreamObserverAbortsPoolClose, the second face of F-conn-1:
// hostConnPool.Close closes its connections in a loop with no isolation, so an
// observer panic on the first one left the rest open forever — live sockets with a
// serve and a heartbeat goroutine each, in a pool nothing references any more.
func TestPool_StreamObserverPanicDoesNotAbortPoolClose(t *testing.T) {
	observer := &oneShotAbandonObserver{}
	gate := newRequestGate()
	harness := newFillHarnessOpts(t, 1, fillHarnessOpts{
		recvHook: gate.hook,
		tune: func(c *ClusterConfig) {
			c.NumConns = 2
			c.StreamObserver = observer
		},
	})
	host := harness.hosts[0]
	pool := harness.pool(t, host)

	gate.arm(host.ConnectAddress().String())
	inflight := harness.query(t.Context(), nil)
	gate.awaitStarted(t, "the query to be parked at the server")

	// Put the connection carrying the parked query first, so Close reaches it first.
	pool.mu.Lock()
	require.Len(t, pool.conns, 2)
	if len(pool.conns[0].calls.snapshot()) == 0 {
		pool.conns[0], pool.conns[1] = pool.conns[1], pool.conns[0]
	}
	conns := append([]*Conn(nil), pool.conns...)
	pool.mu.Unlock()
	require.NotEmpty(t, conns[0].calls.snapshot(), "the parked query must be in flight on the first connection")

	observer.armed.Store(true)
	// What closeAsync does: Close under a recoverGoroutine.
	func() {
		defer recoverGoroutine(harness.session.logger, "test.hostConnPool.Close", nil)
		pool.Close()
	}()
	gate.releaseAll()
	<-inflight

	require.EqualValues(t, 1, observer.panics.Load(), "the panic site must have been reached exactly once")

	time.Sleep(fillAbsenceWindow)
	var open int
	for _, c := range conns {
		if !c.Closed() {
			open++
		}
	}
	if open > 0 {
		t.Fatalf("%d of %d conns left open", open, len(conns))
	}
	for _, c := range conns {
		require.Error(t, c.ctx.Err(), "every closed connection's teardown must have run")
	}
}
