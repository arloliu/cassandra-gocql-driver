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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEventDebounce(t *testing.T) {
	const eventCount = 150
	wg := &sync.WaitGroup{}
	wg.Add(1)

	eventsSeen := 0
	debouncer := newEventDebouncer("testDebouncer", func(events []frame) {
		defer wg.Done()
		eventsSeen += len(events)
	}, nil, &defaultLogger{})
	defer debouncer.stop()

	for i := 0; i < eventCount; i++ {
		debouncer.debounce(&statusChangeEventFrame{
			change: "UP",
			host:   net.IPv4(127, 0, 0, 1),
			port:   9042,
		})
	}

	wg.Wait()
	if eventCount != eventsSeen {
		t.Fatalf("expected to see %d events but got %d", eventCount, eventsSeen)
	}
}

// warnOncePanicLogger panics on the first Warning it sees and works afterwards.
type warnOncePanicLogger struct {
	StructuredLogger
	panics atomic.Int32
}

func (l *warnOncePanicLogger) Warning(msg string, fields ...LogField) {
	if l.panics.CompareAndSwap(0, 1) {
		panic("test: logger panic on the overflow line")
	}
	l.StructuredLogger.Warning(msg, fields...)
}

// mutexFreeWithin reports whether mu can be acquired at any point within d.
// A caller that runs while the mutex is held by its own goroutine never sees it
// free, however long it waits; a caller that merely races the flusher does.
func mutexFreeWithin(mu *sync.Mutex, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for {
		if mu.TryLock() {
			mu.Unlock()
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
}

// completeWithin runs fn on its own goroutine and returns a channel closed when it
// returns. A goroutine left blocked on a stranded mutex is leaked deliberately: the
// test has already failed at that point.
func completeWithin(fn func()) chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	return done
}

func awaitWithin(t *testing.T, name string, d time.Duration, done chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not return within %v", name, d)
	}
}

// fillEventBuffer pushes exactly eventBufferSize events, which is the point at
// which the next one overflows.
func fillEventBuffer(d *eventDebouncer) {
	ev := &statusChangeEventFrame{change: "DOWN", host: net.IPv4(127, 0, 0, 1), port: 9042}
	for i := 0; i < eventBufferSize; i++ {
		d.debounce(ev)
	}
}

// TestEventDebouncer_OverflowLogPanicDoesNotStrandMutex is the inversion of
// _repro/zz_repro_round2_test.go's TestRepro_EventDebouncerOverflowLogPanicStrandsMutex
// (F-evt-1). The overflow warning was logged under e.mu before a plain Unlock, so one
// panic there stranded the mutex — the flusher blocked, every later event blocked, and
// because stop() waits on the flusher's done, Session.Close never returned — and it
// also skipped the onOverflow compensation, which is the part a deferred unlock alone
// would not have restored.
func TestEventDebouncer_OverflowLogPanicDoesNotStrandMutex(t *testing.T) {
	ev := &statusChangeEventFrame{change: "DOWN", host: net.IPv4(127, 0, 0, 1), port: 9042}

	t.Run("the mutex survives and the compensation runs", func(t *testing.T) {
		var overflows atomic.Int32
		var ranUnderLock atomic.Bool
		var d *eventDebouncer

		logger := &warnOncePanicLogger{StructuredLogger: &defaultLogger{}}
		d = newEventDebouncer("test", func([]frame) {}, func() {
			overflows.Add(1)
			if !mutexFreeWithin(&d.mu, 500*time.Millisecond) {
				ranUnderLock.Store(true)
			}
		}, logger)

		fillEventBuffer(d)

		// Whether the panic escapes debounce is the next sub-test's subject.
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Logf("debounce propagated the logger panic: %v", r)
				}
			}()
			d.debounce(ev)
		}()
		if logger.panics.Load() != 1 {
			t.Fatalf("the overflow line was not reached")
		}
		// Snapshot before the probes below, which overflow the full buffer again.
		compensations := overflows.Load()
		underLock := ranUnderLock.Load()

		// The last debounce armed the timer for eventDebounceTime. Let it fire, so
		// that the flusher has reached its own e.mu acquisition: stop() waits on the
		// flusher's done, and a flusher still parked in its select would return from
		// a stranded mutex that it has not yet touched.
		time.Sleep(eventDebounceTime + 500*time.Millisecond)

		later := completeWithin(func() { d.debounce(ev) })
		stopped := completeWithin(func() { d.stop() })
		awaitWithin(t, "stop()", 2*time.Second, stopped)
		awaitWithin(t, "a later debounce", 2*time.Second, later)

		if compensations != 1 {
			t.Fatalf("onOverflow ran %d times for the panicking overflow, want 1", compensations)
		}
		if underLock {
			t.Fatal("onOverflow ran while e.mu was held")
		}
	})

	t.Run("the panicking warning is isolated", func(t *testing.T) {
		logger := &warnOncePanicLogger{StructuredLogger: &defaultLogger{}}
		d := newEventDebouncer("test", func([]frame) {}, func() {}, logger)
		defer d.stop()

		fillEventBuffer(d)
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("debounce panicked: %v", r)
				}
			}()
			d.debounce(ev)
		}()
		if logger.panics.Load() != 1 {
			t.Fatalf("the overflow line was not reached")
		}
	})
}

// onHandleNodeConnected reports whether the caller runs inside handleNodeConnected.
// The injection has to be keyed on the call site, not on "the first call": the
// reconnect sweep consults the filter while collecting DOWN hosts, and
// completeAdmission consults it again, both before the transition under test.
func onHandleNodeConnected() bool {
	buf := make([]byte, 16<<10)
	buf = buf[:runtime.Stack(buf, false)]
	return strings.Contains(string(buf), "(*Session).handleNodeConnected")
}

// hostUpPanicPolicy panics once from HostUp, when the caller is handleNodeConnected.
// armed and panics are the test's, so the filter variant and the policy variant share
// one arming protocol.
type hostUpPanicPolicy struct {
	HostSelectionPolicy
	armed  *atomic.Bool
	panics *atomic.Int32
	active bool
}

func (p *hostUpPanicPolicy) HostUp(h *HostInfo) {
	if p.active && p.armed.Load() && onHandleNodeConnected() && p.armed.CompareAndSwap(true, false) {
		p.panics.Add(1)
		panic("test: policy HostUp panic")
	}
	p.HostSelectionPolicy.HostUp(h)
}

// policyOffers reports whether the policy's iterator offers hostID.
func policyOffers(policy HostSelectionPolicy, hostID string) bool {
	for next := policy.Pick(nil); ; {
		selected := next()
		if selected == nil {
			return false
		}
		if selected.Info().HostID() == hostID {
			return true
		}
	}
}

// TestSession_HostUpPanicLeavesThePublicationRetryable covers N11.
//
// handleNodeConnected sets the host UP and then calls the application's filter and
// policy. Both round-robin policies remove a host from their list on DOWN and HostUp
// is the only thing that puts it back, so one panic there left a host that is UP,
// holds a full pool, and is in no policy list. Nothing repaired it: the executor only
// sees what Pick offers, the DOWN sweep skips it because it is UP, a full pool does
// not refill, completeAdmission skipped AddHost while the publication record still
// named the object, and the fill worker's recovery absorbed the panic.
func TestSession_HostUpPanicLeavesThePublicationRetryable(t *testing.T) {
	run := func(t *testing.T, inPolicy bool) {
		var armed atomic.Bool
		var panics atomic.Int32
		var holdRefresh atomic.Bool
		var releaseOnce sync.Once
		refreshGate := make(chan struct{})
		connected := make(chan struct{}, 8)

		release := func() {
			holdRefresh.Store(false)
			releaseOnce.Do(func() { close(refreshGate) })
		}
		t.Cleanup(release)

		policy := &hostUpPanicPolicy{armed: &armed, panics: &panics, active: inPolicy}
		f := newTickFixture(t, func(cluster *ClusterConfig) {
			// The fixture installs its state collector as the selection policy and
			// waits on it, so this wraps it rather than replacing it.
			policy.HostSelectionPolicy = cluster.PoolConfig.HostSelectionPolicy
			cluster.PoolConfig.HostSelectionPolicy = policy
			if !inPolicy {
				cluster.HostFilter = HostFilterFunc(func(*HostInfo) bool {
					if armed.Load() && onHandleNodeConnected() && armed.CompareAndSwap(true, false) {
						panics.Add(1)
						panic("test: HostFilter panic")
					}
					return true
				})
			}
			// An automatic refresh can republish between the panic and the
			// intermediate assertion and hide the defect, so it is held.
			prev := cluster.testRingRefreshHook
			cluster.testRingRefreshHook = func() {
				if prev != nil {
					prev()
				}
				if holdRefresh.Load() {
					<-refreshGate
				}
			}
		})
		f.session.testAfterNodeConnected = func(*HostInfo) {
			select {
			case connected <- struct{}{}:
			default:
			}
		}

		hostID := f.host.HostID()
		require.True(t, policyOffers(policy, hostID), "precondition: the policy offers the host")
		f.driveDown(t)
		require.False(t, policyOffers(policy, hostID), "precondition: a DOWN host is not offered")

		holdRefresh.Store(true)
		armed.Store(true)
		f.gate.open()
		sweepDownedHostsOnce(f.session)

		select {
		case <-connected:
		case <-time.After(fillEventBudget):
			t.Fatal("the refill never reached handleNodeConnected")
		}
		require.EqualValues(t, 1, panics.Load(), "the injected panic must have fired inside handleNodeConnected")

		require.Equal(t, NodeUp, f.host.State(), "the host is UP")
		pool, ok := f.session.pool.getPoolFor(f.host)
		require.True(t, ok, "and holds a registered pool")
		require.Positive(t, pool.Size(), "which is filled")
		require.False(t, policyOffers(policy, hostID),
			"the lost publication must leave the host out of the policy")

		// One ring-refresh round is the retry owner. completeAdmission is what that
		// round does for the canonical object (host_source.go:1772 is its only
		// caller), and it is driven directly here because this fixture runs with the
		// control connection disabled, so refreshRing itself returns errNoControl.
		release()
		f.session.completeAdmission(f.host)
		if !policyOffers(policy, hostID) {
			t.Fatal("host not offered by Pick after a ring-refresh round")
		}
	}

	t.Run("a panicking HostFilter", func(t *testing.T) { run(t, false) })
	t.Run("a panicking policy HostUp", func(t *testing.T) { run(t, true) })
}
