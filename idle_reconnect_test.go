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
	"bufio"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// assertNoIdleReconnect opens a single pool connection with a small
// Session.Timeout, then confirms the connection does not reconnect while idle.
//
// It is event-driven: the server's recvHook publishes onto startupCh for every
// STARTUP frame (one per (re)connect), so the test reacts the instant a
// reconnect happens instead of sleeping and sampling. A healthy idle connection
// produces exactly one STARTUP (the initial connect) and none thereafter; the
// inherited v2.0.0 read-deadline regression (upstream CASSGO-125 / 590aabe)
// would trip the read deadline while waiting for the next frame and reconnect,
// publishing more STARTUP frames.
func assertNoIdleReconnect(t *testing.T, proto protoVersion) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startupCh := make(chan struct{}, 16)
	srv := newTestServerOpts{
		addr:     "127.0.0.1:0",
		protocol: uint8(proto),
		recvHook: func(f *framer) {
			if f.header.op == opStartup {
				select {
				case startupCh <- struct{}{}:
				default:
				}
			}
		},
	}.newServer(t, ctx)
	defer srv.Stop()

	cluster := testCluster(proto, srv.Address)
	cluster.Timeout = 200 * time.Millisecond
	cluster.NumConns = 1
	db, err := cluster.CreateSession()
	require.NoError(t, err, "CreateSession")
	defer db.Close()

	// Drain the initial connection's STARTUP.
	select {
	case <-startupCh:
	case <-time.After(2 * time.Second):
		t.Fatal("initial connection never sent STARTUP")
	}

	// Watch for a second STARTUP over a window many Session.Timeouts long. With
	// the bug the idle read deadline fires roughly every Session.Timeout, closing
	// and re-establishing the connection; this select fails the moment that
	// happens rather than after a fixed sleep.
	const watch = 1500 * time.Millisecond
	select {
	case <-startupCh:
		t.Fatalf("idle connection reconnected: a second STARTUP arrived within %v (read deadline firing on idle frame/segment reads)", watch)
	case <-time.After(watch):
		// No reconnect observed.
	}

	// The surviving connection must still be usable, and no reconnect may race
	// this query (startupCh is still being collected).
	require.NoError(t, db.Query("void").Exec(), "query after idle")
	select {
	case <-startupCh:
		t.Fatal("connection reconnected around the post-idle query")
	default:
	}
}

// TestIdleConnectionDoesNotReconnectV4 exercises the pre-v5 processFrame read
// path.
func TestIdleConnectionDoesNotReconnectV4(t *testing.T) {
	assertNoIdleReconnect(t, protoVersion4)
}

// TestIdleConnectionDoesNotReconnectV5 exercises the proto-v5 recvSegment read
// path (the path otter-cache uses in production).
func TestIdleConnectionDoesNotReconnectV5(t *testing.T) {
	assertNoIdleReconnect(t, protoVersion5)
}

// uncompressedSegmentHeader builds a wire-valid 6-byte proto-v5 uncompressed
// segment header (3 length/flag bytes + CRC24) advertising the given payload
// length, without any payload.
func uncompressedSegmentHeader(payloadLen int, selfContained bool) []byte {
	headerInt := uint32(payloadLen) & maxSegmentPayloadSize
	if selfContained {
		headerInt |= 1 << 17
	}
	h := make([]byte, 6)
	h[0] = byte(headerInt)
	h[1] = byte(headerInt >> 8)
	h[2] = byte(headerInt >> 16)
	crc := Crc24(h[:3])
	h[3] = byte(crc)
	h[4] = byte(crc >> 8)
	h[5] = byte(crc >> 16)
	return h
}

// TestReadUncompressedSegmentBoundsPayloadAfterHeader is the regression guard
// for the proto-v5 half of the CASSGO-125 fix: recvSegment disables the read
// deadline only while idle-waiting for a segment to begin, then re-arms it via
// onSegmentHeader before the payload read. A peer that sends a valid segment
// header and then stalls mid-payload must therefore hit the deadline instead of
// hanging the receive loop.
//
// Without the onSegmentHeader restore the payload read would run with no
// deadline and this test would block until its own timeout fires.
func TestReadUncompressedSegmentBoundsPayloadAfterHeader(t *testing.T) {
	server, client, err := tcpConnPair()
	require.NoError(t, err)
	t.Cleanup(func() {
		server.Close()
		client.Close()
	})

	cr := &connReader{conn: client, r: bufio.NewReader(client)}
	// Idle: no read deadline while waiting for the segment to begin.
	cr.SetTimeout(0)

	// The peer sends a valid header advertising a 100-byte payload, then stalls
	// (never writes the payload).
	go func() {
		_, _ = server.Write(uncompressedSegmentHeader(100, true))
	}()

	const payloadDeadline = 100 * time.Millisecond
	restored := make(chan struct{})
	onSegmentHeader := func() {
		cr.SetTimeout(payloadDeadline)
		close(restored)
	}

	type readResult struct {
		bufPtr *[]byte
		err    error
	}
	done := make(chan readResult, 1)
	start := time.Now()
	go func() {
		_, bufPtr, _, err := readUncompressedSegment(cr, onSegmentHeader)
		done <- readResult{bufPtr, err}
	}()

	select {
	case res := <-done:
		if res.bufPtr != nil {
			releaseSegmentBuffer(res.bufPtr)
		}
		require.Error(t, res.err, "stalled payload read must fail on the re-armed deadline, not succeed")
		require.Less(t, time.Since(start), 2*time.Second, "payload read must be bounded by the re-armed deadline")
	case <-time.After(3 * time.Second):
		t.Fatal("readUncompressedSegment blocked past the re-armed deadline: the payload read is not bounded (onSegmentHeader restore missing?)")
	}

	select {
	case <-restored:
	default:
		t.Fatal("onSegmentHeader was not invoked after the segment header was read")
	}
}

// TestRecvSegmentBoundsPayloadAfterHeader is the end-to-end guard for the same
// property through the production wiring: it drives Conn.recvSegment (not the
// reader directly), so it fails if recvSegment ever stops passing its re-arming
// callback to the segment reader. The peer sends a valid uncompressed segment
// header then stalls; recvSegment must return a deadline error rather than
// blocking the receive loop.
func TestRecvSegmentBoundsPayloadAfterHeader(t *testing.T) {
	server, client, err := tcpConnPair()
	require.NoError(t, err)
	t.Cleanup(func() {
		server.Close()
		client.Close()
	})

	c := &Conn{
		r:       &connReader{conn: client, r: bufio.NewReader(client)},
		version: protoVersion5,
	}
	// A non-zero read timeout drives recvSegment's toggle-and-restore path.
	c.r.SetTimeout(150 * time.Millisecond)

	go func() {
		_, _ = server.Write(uncompressedSegmentHeader(100, true))
	}()

	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- c.recvSegment(context.Background()) }()

	select {
	case err := <-done:
		require.Error(t, err, "recvSegment must fail on the re-armed deadline when the peer stalls mid-payload")
		require.Less(t, time.Since(start), 2*time.Second, "recvSegment must be bounded by the re-armed read deadline")
	case <-time.After(3 * time.Second):
		t.Fatal("recvSegment blocked past the re-armed deadline: a proto-v5 idle-then-stall connection hangs the receive loop (recvSegment not passing its onSegmentHeader callback?)")
	}
}
