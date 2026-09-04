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
	"crypto/tls"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// dialTestTimeout is the ConnectTimeout the bounded cases dial with.
	// Small enough that a regression fails the test by blocking, large enough
	// not to fire on a loaded machine before the handshake is even attempted.
	dialTestTimeout = 250 * time.Millisecond

	// dialTestBudget bounds every wait that a correct implementation ends long before.
	dialTestBudget = 30 * time.Second
)

// hangingListener accepts TCP connections and then answers nothing,
// the way a SIGSTOP-paused node does:
// the kernel completes the TCP handshake while the stopped process replies to no byte of it.
//
// Accepted connections are retained until cleanup so the peer sees a live, silent
// socket rather than the EOF a dropped connection would deliver.
type hangingListener struct {
	listener net.Listener
	// accepted publishes every accepted connection so a test can gate on the
	// server side without polling.
	accepted chan net.Conn
	closeAll sync.Once
}

// newHangingListener starts a listener on loopback that accepts and never answers.
//
// Returns:
//   - *hangingListener: listener whose socket and accepted connections are closed by t.Cleanup
func newHangingListener(t *testing.T) *hangingListener {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "listen")

	hl := &hangingListener{
		listener: listener,
		accepted: make(chan net.Conn, 8),
	}
	t.Cleanup(hl.close)

	go func() {
		for {
			conn, err := hl.listener.Accept()
			if err != nil {
				return
			}
			hl.accepted <- conn
		}
	}()

	return hl
}

// newEchoTLSListener starts a TLS listener on loopback that echoes everything it reads.
//
// Returns:
//   - net.Listener: listener closed by t.Cleanup
func newEchoTLSListener(t *testing.T) net.Listener {
	t.Helper()

	cert, err := tls.LoadX509KeyPair("testdata/pki/cassandra.crt", "testdata/pki/cassandra.key")
	require.NoError(t, err, "LoadX509KeyPair")

	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	require.NoError(t, err, "tls.Listen")
	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// The first read drives the server side of the handshake.
			go func() {
				defer func() { _ = conn.Close() }()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	return listener
}

// addr returns the address the hanging listener is bound to.
func (hl *hangingListener) addr() net.Addr {
	return hl.listener.Addr()
}

// close shuts the listener down and closes every connection it accepted.
func (hl *hangingListener) close() {
	hl.closeAll.Do(func() {
		_ = hl.listener.Close()
		for {
			select {
			case conn := <-hl.accepted:
				_ = conn.Close()
			default:
				return
			}
		}
	})
}

// hostForAddr builds the HostInfo a dialer needs to reach addr.
//
// Parameters:
//   - addr: a resolved TCP address, typically a test listener's
//
// Returns:
//   - *HostInfo: host carrying addr's connect address and port
func hostForAddr(t *testing.T, addr net.Addr) *HostInfo {
	t.Helper()

	ip, portText, err := net.SplitHostPort(addr.String())
	require.NoError(t, err, "SplitHostPort")
	port, err := strconv.Atoi(portText)
	require.NoError(t, err, "Atoi")

	return &HostInfo{connectAddress: net.ParseIP(ip), port: port}
}

// TestDefaultHostDialerBoundsTLSHandshake proves a silent peer can no longer park
// the dialer in the handshake: the fill cycle that this defect kept open forever
// now ends in an error, which is what lets a paused host be convicted.
func TestDefaultHostDialerBoundsTLSHandshake(t *testing.T) {
	listener := newHangingListener(t)
	hd := &defaultHostDialer{
		dialer:         &net.Dialer{Timeout: dialTestTimeout},
		tlsConfig:      &tls.Config{InsecureSkipVerify: true},
		connectTimeout: dialTestTimeout,
	}

	started := time.Now()
	dialed, err := hd.DialHost(t.Context(), hostForAddr(t, listener.addr()))
	elapsed := time.Since(started)

	require.Error(t, err, "DialHost against a silent peer")
	require.Nil(t, dialed)
	// The deadline is the only thing that can end this handshake, so the error
	// distinguishes a real timeout from the peer having dropped the connection.
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, dialTestBudget, "DialHost should return on the ConnectTimeout")

	// The pool retries a failed fill indefinitely, so the TCP connection under a
	// timed-out handshake has to be released rather than leaked.
	serverConn := <-listener.accepted
	t.Cleanup(func() { _ = serverConn.Close() })
	require.NoError(t, serverConn.SetReadDeadline(time.Now().Add(dialTestBudget)), "SetReadDeadline")
	_, err = io.Copy(io.Discard, serverConn)
	require.NoError(t, err, "the server side should observe a clean close, not a stalled read")
}

// TestDefaultHostDialerZeroConnectTimeoutStaysUnbounded proves a zero ConnectTimeout
// keeps meaning "no timeout", exactly as it does for the TCP dial, so the fix
// introduces no default the operator did not ask for.
func TestDefaultHostDialerZeroConnectTimeoutStaysUnbounded(t *testing.T) {
	listener := newHangingListener(t)
	hd := &defaultHostDialer{
		dialer:         &net.Dialer{},
		tlsConfig:      &tls.Config{InsecureSkipVerify: true},
		connectTimeout: 0,
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	type result struct {
		dialed *DialedHost
		err    error
	}
	done := make(chan result, 1)
	go func() {
		dialed, err := hd.DialHost(ctx, hostForAddr(t, listener.addr()))
		done <- result{dialed: dialed, err: err}
	}()

	// Asserting an absence needs a window; this one is not waiting for state to
	// settle, it is the observation that no deadline of our own fires.
	select {
	case got := <-done:
		require.FailNowf(t, "DialHost returned", "a zero ConnectTimeout must not bound the handshake, got %v", got.err)
	case <-time.After(4 * dialTestTimeout):
	}

	cancel()

	select {
	case got := <-done:
		require.Error(t, got.err, "DialHost after cancellation")
		require.ErrorIs(t, got.err, context.Canceled)
	case <-time.After(dialTestBudget):
		require.FailNow(t, "DialHost did not observe the cancelled context")
	}
}

// TestDefaultHostDialerPlainConnectionUnaffected proves the non-TLS path is untouched:
// it keeps the dial bound it always had and gains no handshake deadline,
// so a silent peer still yields a usable connection.
func TestDefaultHostDialerPlainConnectionUnaffected(t *testing.T) {
	listener := newHangingListener(t)
	hd := &defaultHostDialer{
		dialer:         &net.Dialer{Timeout: dialTestTimeout},
		connectTimeout: dialTestTimeout,
	}

	dialed, err := hd.DialHost(t.Context(), hostForAddr(t, listener.addr()))

	require.NoError(t, err, "DialHost without TLS")
	require.NotNil(t, dialed)
	t.Cleanup(func() { _ = dialed.Conn.Close() })
	assert.False(t, dialed.DisableCoalesce, "a plain connection supports writev")

	_, err = dialed.Conn.Write([]byte("ping"))
	require.NoError(t, err, "write on the plain connection")
}

// TestDefaultHostDialerTLSSuccessRoundTrips proves the bound leaves a successful
// handshake alone: the returned connection carries traffic after the deadline the
// handshake ran under has been released.
func TestDefaultHostDialerTLSSuccessRoundTrips(t *testing.T) {
	listener := newEchoTLSListener(t)
	hd := &defaultHostDialer{
		dialer:         &net.Dialer{Timeout: dialTestBudget},
		tlsConfig:      &tls.Config{InsecureSkipVerify: true},
		connectTimeout: dialTestBudget,
	}

	dialed, err := hd.DialHost(t.Context(), hostForAddr(t, listener.Addr()))

	require.NoError(t, err, "DialHost against a TLS listener")
	require.NotNil(t, dialed)
	t.Cleanup(func() { _ = dialed.Conn.Close() })
	assert.True(t, dialed.DisableCoalesce, "a TLS connection cannot use writev")

	require.NoError(t, dialed.Conn.SetDeadline(time.Now().Add(dialTestBudget)), "SetDeadline")
	_, err = dialed.Conn.Write([]byte("ping"))
	require.NoError(t, err, "write on the TLS connection")

	echoed := make([]byte, 4)
	_, err = io.ReadFull(dialed.Conn, echoed)
	require.NoError(t, err, "read on the TLS connection")
	require.Equal(t, "ping", string(echoed))
}
