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
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// channelLogger is a StructuredLogger that pushes Warning messages to a
// channel so the test can deterministically wait for the leak-detector
// finalizer to fire. Other levels are no-ops.
type channelLogger struct {
	warnings chan string
	count    int64
	mu       sync.Mutex
	captured []string
}

func newChannelLogger() *channelLogger {
	return &channelLogger{warnings: make(chan string, 8)}
}

func (l *channelLogger) Error(_ string, _ ...LogField)   {}
func (l *channelLogger) Info(_ string, _ ...LogField)    {}
func (l *channelLogger) Debug(_ string, _ ...LogField)   {}
func (l *channelLogger) Warning(msg string, _ ...LogField) {
	atomic.AddInt64(&l.count, 1)
	l.mu.Lock()
	l.captured = append(l.captured, msg)
	l.mu.Unlock()
	select {
	case l.warnings <- msg:
	default:
	}
}

func (l *channelLogger) hasLeakWarning() (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range l.captured {
		if strings.Contains(m, "garbage-collected without Close") {
			return m, true
		}
	}
	return "", false
}

// runQueryAndDropIter executes one query and drops the iter without
// calling Close. Wrapping this in its own function ensures the local
// variable is not kept alive by the test function's stack frame.
//
//go:noinline
func runQueryAndDropIter(t *testing.T, sess *Session) {
	t.Helper()
	iter := sess.Query("void").Iter()
	if iter == nil {
		t.Fatal("nil iter")
	}
	// Touch the iter so it is real — the goal is to let it become
	// unreachable on function return, not to be optimized away.
	_ = iter.NumRows()
}

// waitForFinalizer drives runtime.GC in a loop until the channel
// receives a value or the deadline expires. Two consecutive GCs per
// iteration drain pending finalizers (the first marks unreachable,
// the second sweeps and runs finalizers).
func waitForFinalizer(t *testing.T, ch <-chan string, timeout time.Duration) (string, bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		runtime.GC()
		runtime.GC()
		select {
		case msg := <-ch:
			return msg, true
		case <-time.After(20 * time.Millisecond):
		}
	}
	return "", false
}

// TestIter_LeakDetectorFiresWhenNotClosed asserts the SetFinalizer
// safety net fires when the user drops an Iter reference without
// calling Close(). The logger must receive a warning indicating the
// leak.
func TestIter_LeakDetectorFiresWhenNotClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv := NewTestServer(t, defaultProto, ctx)
	defer srv.Stop()

	logger := newChannelLogger()
	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = 1
	cluster.Logger = logger
	sess, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	defer sess.Close()

	runQueryAndDropIter(t, sess)

	msg, ok := waitForFinalizer(t, logger.warnings, 5*time.Second)
	if !ok {
		t.Fatal("finalizer did not fire within timeout — leak detector missing or iter retained")
	}
	if !strings.Contains(msg, "garbage-collected without Close") {
		t.Errorf("unexpected warning message: %q", msg)
	}
}

// TestIter_LeakDetectorDoesNotFireAfterClose asserts that Close()
// clears the finalizer, so a properly-closed iter does NOT warn on
// GC. This validates the SetFinalizer(iter, nil) call inside Close().
func TestIter_LeakDetectorDoesNotFireAfterClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv := NewTestServer(t, defaultProto, ctx)
	defer srv.Stop()

	logger := newChannelLogger()
	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = 1
	cluster.Logger = logger
	sess, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	defer sess.Close()

	// Run query, properly Close, then drop reference.
	func() {
		iter := sess.Query("void").Iter()
		if err := iter.Close(); err != nil {
			t.Fatalf("iter.Close: %v", err)
		}
	}()

	// Wait bounded time. The leak warning must NOT appear.
	deadline := time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) {
		runtime.GC()
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
	}

	if msg, found := logger.hasLeakWarning(); found {
		t.Fatalf("leak warning fired on a Closed iter: %q", msg)
	}
}

// TestIter_LeakDetectorNoFalseWarnOnScanContextNotFound asserts that
// when a helper like Query.ScanContext exits early due to ErrNotFound,
// the helper-owned iter is Closed before return — so no false leak
// warning fires. Without the iter.Close() in the early-return branch,
// the user-facing iter is dropped while still armed and the finalizer
// would warn about correct user code.
func TestIter_LeakDetectorNoFalseWarnOnScanContextNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv := NewTestServer(t, defaultProto, ctx)
	defer srv.Stop()

	logger := newChannelLogger()
	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = 1
	cluster.Logger = logger
	sess, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	defer sess.Close()

	// The fake server returns resultKindVoid (numRows=0) for "void",
	// which makes checkErrAndNotFound return ErrNotFound and ScanContext
	// take the early-return path.
	var dst int
	if err := sess.Query("void").ScanContext(ctx, &dst); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound from ScanContext on void query, got %v", err)
	}

	deadline := time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) {
		runtime.GC()
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
	}

	if msg, found := logger.hasLeakWarning(); found {
		t.Fatalf("false leak warning from ScanContext early-return path: %q", msg)
	}
}

// TestIter_LeakDetector_NotAttachedOnErrIter asserts that an iter
// returned via the error path (no framer) does not trigger the
// finalizer. attachLeakDetector short-circuits when framer is nil, so
// the iter has no finalizer and dropping its reference produces no
// warning.
func TestIter_LeakDetector_NotAttachedOnErrIter(t *testing.T) {
	logger := newChannelLogger()
	// Build a session-less iter directly via newErrIter. The
	// attachLeakDetector call must be a no-op (framer is nil).
	iter := newErrIter(ErrSessionClosed, &queryMetrics{}, "", nil, nil)
	iter.attachLeakDetector(logger)

	// Drop reference and force GC.
	iter = nil
	_ = iter

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		runtime.GC()
		runtime.GC()
		time.Sleep(50 * time.Millisecond)
	}

	if msg, found := logger.hasLeakWarning(); found {
		t.Fatalf("leak warning fired on framer-less err iter: %q", msg)
	}
}
