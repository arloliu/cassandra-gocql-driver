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
	"errors"
	"net"
	"testing"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/internal/streams"
)

// TestConn_CloseWithErrorNoDeadlock verifies that closeWithError does not
// deadlock when there are pending calls whose callers haven't started
// reading from the response channel yet.
//
// The race condition occurs when:
// 1. A caller creates a callReq and adds it to c.calls
// 2. The caller is blocked (e.g., in Write) before selecting on call.resp
// 3. closeWithError iterates calls and tries to send error to call.resp
// 4. With an unbuffered channel, this send blocks forever
//
// The fix is to use a buffered channel for call.resp.
func TestConn_CloseWithErrorNoDeadlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a minimal Conn with just the fields needed for closeWithError
	conn := &Conn{
		streams: streams.New(protoVersion4),
		calls:   newCallMap(64),
		ctx:     ctx,
		cancel:  cancel,
		r:       &mockConnReader{},
		errorHandler: connErrorHandlerFn(func(c *Conn, err error, closed bool) {
			// no-op for test
		}),
		logger: NewLogger(LogLevelNone),
	}

	// Create a call with a buffered response channel (simulating the fix)
	// If this were unbuffered, closeWithError would deadlock
	call := &callReq{
		streamID: 1,
		resp:     make(chan callResp, 1), // Buffered - the fix
		timeout:  make(chan struct{}),
	}

	conn.calls.tryStore(call.streamID, call)

	// closeWithError should complete without blocking
	// because the buffered channel allows the send to proceed
	done := make(chan struct{})
	go func() {
		conn.closeWithError(errors.New("test error"))
		close(done)
	}()

	select {
	case <-done:
		// Success - closeWithError completed without deadlocking
	case <-time.After(1 * time.Second):
		t.Fatal("closeWithError deadlocked - the resp channel is likely unbuffered")
	}

	// Verify the error was delivered to the call
	select {
	case resp := <-call.resp:
		if resp.err == nil || resp.err.Error() != "test error" {
			t.Errorf("expected 'test error', got %v", resp.err)
		}
	default:
		t.Error("expected error to be sent to call.resp")
	}
}

// mockConnReader implements ConnReader for testing
type mockConnReader struct {
	net.Conn // embed to satisfy Write, Read, etc. - we only need Close
}

func (m *mockConnReader) Close() error                       { return nil }
func (m *mockConnReader) LocalAddr() net.Addr                { return nil }
func (m *mockConnReader) RemoteAddr() net.Addr               { return nil }
func (m *mockConnReader) SetDeadline(t time.Time) error      { return nil }
func (m *mockConnReader) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConnReader) SetWriteDeadline(t time.Time) error { return nil }
func (m *mockConnReader) SetTimeout(timeout time.Duration)   {}
func (m *mockConnReader) GetTimeout() time.Duration          { return 0 }
func (m *mockConnReader) Read(p []byte) (n int, err error)   { return 0, nil }
func (m *mockConnReader) Write(p []byte) (n int, err error)  { return len(p), nil }
