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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// triggerTestBudget bounds every positive wait in these tests.
const triggerTestBudget = 5 * time.Second

// awaitRun receives one refresh run or fails the test.
func awaitRun(t *testing.T, runs <-chan struct{}, what string) {
	t.Helper()

	select {
	case <-runs:
	case <-time.After(triggerTestBudget):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// requireNoRun asserts that no refresh run arrives within window.
//
// A wait for absence is unavoidable here: the property is that the timer did not fire.
func requireNoRun(t *testing.T, runs <-chan struct{}, window time.Duration, what string) {
	t.Helper()

	select {
	case <-runs:
		t.Fatalf("unexpected refresh run: %s", what)
	case <-time.After(window):
	}
}

// TestRefreshDebouncer_Trigger_RunsWithoutDebounceTimer proves a trigger runs the
// refresh promptly even when the debounce interval is far longer than the wait.
func TestRefreshDebouncer_Trigger_RunsWithoutDebounceTimer(t *testing.T) {
	runs := make(chan struct{}, 8)
	d := newRefreshDebouncer(10*time.Second, func() error {
		runs <- struct{}{}
		return nil
	}, nil)
	t.Cleanup(d.stop)

	d.trigger()

	awaitRun(t, runs, "the triggered refresh")
}

// TestRefreshDebouncer_Trigger_CoalescesWhileRunning proves that any number of
// triggers arriving during a refresh collapse into exactly one follow-up refresh.
func TestRefreshDebouncer_Trigger_CoalescesWhileRunning(t *testing.T) {
	const interval = 100 * time.Millisecond

	entered := make(chan struct{}, 8)
	gate := make(chan struct{})
	var first atomic.Bool
	first.Store(true)
	d := newRefreshDebouncer(interval, func() error {
		entered <- struct{}{}
		if first.CompareAndSwap(true, false) {
			<-gate
		}
		return nil
	}, nil)
	t.Cleanup(d.stop)
	t.Cleanup(func() { close(gate) })

	d.trigger()
	awaitRun(t, entered, "the first refresh to start")

	for range 5 {
		d.trigger()
	}
	gate <- struct{}{}

	awaitRun(t, entered, "exactly one follow-up refresh")
	requireNoRun(t, entered, 2*interval, "a third refresh after coalesced triggers")
}

// TestRefreshDebouncer_Trigger_DoesNotResetPendingDebounce proves a trigger runs
// ahead of a pending debounce and that the flusher drains that debounce,
// so the pair yields one refresh, not two.
func TestRefreshDebouncer_Trigger_DoesNotResetPendingDebounce(t *testing.T) {
	const interval = 400 * time.Millisecond

	runs := make(chan struct{}, 8)
	d := newRefreshDebouncer(interval, func() error {
		runs <- struct{}{}
		return nil
	}, nil)
	t.Cleanup(d.stop)

	d.debounce()
	d.trigger()

	select {
	case <-runs:
	case <-time.After(interval / 2):
		t.Fatal("the trigger must run before the debounce timer would have fired")
	}
	requireNoRun(t, runs, 2*interval, "the drained debounce must not run a second refresh")
}

// TestRefreshDebouncer_Trigger_AfterStopIsNoop proves a trigger after stop
// neither runs a refresh nor blocks.
func TestRefreshDebouncer_Trigger_AfterStopIsNoop(t *testing.T) {
	runs := make(chan struct{}, 8)
	d := newRefreshDebouncer(time.Second, func() error {
		runs <- struct{}{}
		return nil
	}, nil)
	d.stop()

	d.trigger()

	requireNoRun(t, runs, 200*time.Millisecond, "a refresh after stop")
}

// TestRefreshDebouncer_Stop_WithQueuedTrigger proves stop completes while a
// refresh is running with another trigger queued behind it.
func TestRefreshDebouncer_Stop_WithQueuedTrigger(t *testing.T) {
	entered := make(chan struct{}, 8)
	gate := make(chan struct{})
	var runs atomic.Int32
	d := newRefreshDebouncer(time.Second, func() error {
		runs.Add(1)
		entered <- struct{}{}
		if runs.Load() == 1 {
			<-gate
		}
		return nil
	}, nil)
	var closeGate sync.Once
	release := func() { closeGate.Do(func() { close(gate) }) }
	t.Cleanup(release)

	d.trigger()
	awaitRun(t, entered, "the first refresh to start")
	d.trigger()

	stopped := make(chan struct{})
	go func() {
		d.stop()
		close(stopped)
	}()
	release()

	select {
	case <-stopped:
	case <-time.After(triggerTestBudget):
		t.Fatal("stop did not return after the running refresh was released")
	}
	require.LessOrEqual(t, runs.Load(), int32(2), "at most the running refresh and one queued follow-up may run")
}
