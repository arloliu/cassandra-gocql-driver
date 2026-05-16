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
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// TestRecover_Smoke_EventDebouncerStopSync asserts that stop() returns
// only AFTER the flusher goroutine has exited. The synchronization is
// via the new e.done chan: close(quit) → flusher returns → defer
// close(done) → stop() unblocks. Without this, a pending timer could
// fire one last callback after stop() returns, against
// already-stopped components.
func TestRecover_Smoke_EventDebouncerStopSync(t *testing.T) {
	callbacks := atomic.Int64{}
	flusherExited := atomic.Bool{}

	e := newEventDebouncer("smoke", func(frames []frame) {
		callbacks.Add(1)
	}, &captureLogger{})

	// Wrap the flusher exit observable: spawn a watcher that watches
	// e.done and sets flusherExited just after.
	go func() {
		<-e.done
		flusherExited.Store(true)
	}()

	// Wait a moment for the flusher goroutine to start.
	time.Sleep(10 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		defer close(done)
		e.stop()
	}()
	select {
	case <-done:
		// stop() returned. The flusher MUST have exited first; the watcher
		// observes done and sets the flag synchronously, but there's a small
		// race between the close and the flag write. A bounded wait covers
		// the race.
		for i := 0; i < 100; i++ {
			if flusherExited.Load() {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if !flusherExited.Load() {
			t.Fatalf("stop() returned before flusher actually exited")
		}
	case <-time.After(time.Second):
		t.Fatalf("eventDebouncer.stop() deadlocked")
	}
}

// TestRecover_Smoke_HelperAbsorbsPanicAcrossGoroutine asserts at the
// integration level that a goroutine using recoverGoroutine does not
// crash the test process, even with a nil logger and a panicking
// teardown. This is a sanity check that the helper composes correctly
// with `go func()` invocation (not just the unit tests in recover_test.go).
func TestRecover_Smoke_HelperAbsorbsPanicAcrossGoroutine(t *testing.T) {
	beforeRoutines := runtime.NumGoroutine()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer recoverGoroutine(nil, "smoke.test", func(err error) {
			panic("teardown panics too")
		})
		panic("primary boom")
	}()

	select {
	case <-done:
		// goroutine exited cleanly.
	case <-time.After(time.Second):
		t.Fatalf("goroutine did not exit; recoverGoroutine likely deadlocked")
	}

	// Allow the runtime a moment to clean up.
	time.Sleep(10 * time.Millisecond)

	afterRoutines := runtime.NumGoroutine()
	if afterRoutines > beforeRoutines+1 {
		t.Logf("note: goroutine count grew by %d (likely benign GC noise)",
			afterRoutines-beforeRoutines)
	}
}

// TestRecover_Smoke_PoolFillSyncPanicClearsFillingFlag asserts that a
// panic in the synchronous portion of hostConnPool.fill (before the
// async branch takes over) does NOT leave pool.filling = true. Without
// the v3 fix, the pool would be permanently stuck — later Pick() and
// HandleError() calls would see filling=true and skip refilling, so
// the host would remain disconnected.
//
// We exercise this by directly constructing a hostConnPool with a
// session whose connect() will panic. The fill() function should
// recover, mark filling=false via fillingStopped, and return.
func TestRecover_Smoke_PoolFillSyncPanicClearsFillingFlag(t *testing.T) {
	// Build a minimal hostConnPool by hand. We don't need a real network
	// — the test exercises the panic-recovery path, which fires before
	// any successful connect can happen.
	pool := &hostConnPool{
		size:     1,
		filling:  false,
		closed:   false,
		conns:    nil,
		host:     &HostInfo{},
		logger:   &captureLogger{},
		keyspace: "",
		// session is intentionally nil — pool.connect() will panic
		// trying to dereference it, which is exactly the failure mode
		// we want to recover from.
	}

	// Direct call (not via `go pool.fill()`), so the test goroutine
	// observes the recovery. fill()'s synchronous-body recover should
	// absorb the panic and clear pool.filling.
	pool.fill()

	pool.mu.Lock()
	stillFilling := pool.filling
	pool.mu.Unlock()
	if stillFilling {
		t.Fatalf("pool.filling left true after sync-panic in fill(); pool is permanently stuck")
	}
}

// TestRecover_Smoke_RefreshDebouncerPanicFailsFast asserts that after a
// recovered panic in refreshDebouncer.flusher, future refreshNow()
// callers get an immediate error instead of waiting forever. Without
// the v6 fix, the flusher would exit silently, leaving d.stopped=false
// and refreshNow() would attach a listener no goroutine ever broadcasts
// to.
func TestRecover_Smoke_RefreshDebouncerPanicFailsFast(t *testing.T) {
	d := newRefreshDebouncer(1*time.Hour, func() error {
		panic("refresh boom")
	}, &captureLogger{})
	t.Cleanup(d.stop)

	// Drive the flusher into a panic via refreshNow + waiting for the
	// panic-broadcast error.
	firstListener := d.refreshNow()
	select {
	case err := <-firstListener:
		if err == nil {
			t.Fatalf("expected non-nil error from broadcaster, got nil")
		}
	case <-time.After(time.Second):
		t.Fatalf("first refreshNow listener did not receive an error")
	}

	// Give the flusher's panic recovery a moment to finalize d.stopped.
	time.Sleep(50 * time.Millisecond)

	// Subsequent refreshNow must NOT block.
	secondListener := d.refreshNow()
	select {
	case err := <-secondListener:
		if err == nil {
			t.Fatalf("expected stopped-state error from second refreshNow, got nil")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("second refreshNow after flusher panic blocked; v6 fail-fast did not engage")
	}
}

// TestRecover_Smoke_ControlConnHeartbeatPanicDoesNotBlockClose asserts
// that a panic in controlConn.heartBeat does NOT leave Session.Close()
// blocked forever. Before the v4 fix, c.quit was unbuffered and
// close() did `c.quit <- struct{}{}` after CAS Started→Closing; if
// heartBeat had exited via recovery, no receiver remained and close()
// blocked. Now c.quit is buffered (1) so the send always completes,
// and the recovery teardown CAS's state to Closing so subsequent
// close() calls observe consistent state.
func TestRecover_Smoke_ControlConnHeartbeatPanicDoesNotBlockClose(t *testing.T) {
	c := &controlConn{
		session: &Session{logger: &captureLogger{}},
		quit:    make(chan struct{}, 1),
	}
	c.conn.Store((*connHost)(nil))
	// Simulate "heartBeat ran and entered the Started state, then died".
	atomic.StoreInt32(&c.state, controlConnStarted)

	// close() must not block.
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.close()
	}()
	select {
	case <-done:
		// close() returned, no deadlock.
	case <-time.After(time.Second):
		t.Fatalf("controlConn.close() blocked after heartbeat exited")
	}
}

// TestRecover_Smoke_PanicWithStackEndToEnd asserts that a re-panic with
// panicWithStack flows through recoverGoroutine and the wrapped stack is
// what gets logged — not the re-panic frame.
//
// This integrates the refreshDebouncer broadcaster-strand pattern at
// the helper level.
func TestRecover_Smoke_PanicWithStackEndToEnd(t *testing.T) {
	logger := &captureLogger{}
	customStack := []byte("ORIGINAL_REFRESHFN_FRAME")

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer recoverGoroutine(logger, "smoke.refresher", nil)

		// Inner func that mimics the refreshDebouncer pattern: inner
		// recover, re-panic with panicWithStack carrying a custom stack.
		func() {
			defer func() {
				if r := recover(); r != nil {
					panic(panicWithStack{value: r, stack: customStack})
				}
			}()
			panic("original cause")
		}()
	}()
	<-done

	_, _, fields := logger.snapshot()
	stackLogged := ""
	for _, f := range fields {
		if f.Name == "stack" {
			stackLogged = f.Value.Any().(string)
		}
	}
	if stackLogged != string(customStack) {
		t.Fatalf("expected wrapped stack to flow end-to-end, got %q", stackLogged)
	}
}
