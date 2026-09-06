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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestNextDebounceDeadlineTakesTheEarlierOfTheTwo pins the deadline arithmetic on a
// clock the test controls, because the flush itself runs on another goroutine and a
// wall-clock assertion about when it should have happened is inherently flaky.
func TestNextDebounceDeadlineTakesTheEarlierOfTheTwo(t *testing.T) {
	base := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name     string
		elapsed  time.Duration
		expected time.Duration
	}{
		{
			name:     "the quiet period is the earlier deadline at the start of a burst",
			elapsed:  0,
			expected: eventDebounceTime,
		},
		{
			name:     "still the quiet period while the hard deadline is further away",
			elapsed:  eventMaxDebounce - eventDebounceTime - time.Millisecond,
			expected: eventDebounceTime,
		},
		{
			name:     "the hard deadline takes over once it is the nearer one",
			elapsed:  eventMaxDebounce - eventDebounceTime/2,
			expected: eventDebounceTime / 2,
		},
		{
			name: "an event past the hard deadline arms an immediate flush, " +
				"rather than declining to extend an expiry an earlier event already pushed out",
			elapsed:  eventMaxDebounce + time.Second,
			expected: -time.Second,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := nextDebounceDeadline(base.Add(tc.elapsed), base)
			require.Equal(t, tc.expected, got)
		})
	}
}

// TestEventDebouncerFlushesUnderSustainedChurn pins F-AH-3 end to end.
//
// Every event reset the quiet period, so events arriving closer together than
// eventDebounceTime postponed the flush indefinitely. A rolling restart or a
// flapping node produces exactly that, and the topology changes those events
// describe went unhandled for as long as the churn lasted - on the one path that
// tells the driver a node appeared or left.
func TestEventDebouncerFlushesUnderSustainedChurn(t *testing.T) {
	var (
		mu        sync.Mutex
		delivered int
		flushes   int
	)
	done := make(chan struct{}, 1)

	d := newEventDebouncer("churn", func(events []frame) {
		mu.Lock()
		flushes++
		delivered += len(events)
		mu.Unlock()
		select {
		case done <- struct{}{}:
		default:
		}
	}, nil, &defaultLogger{})
	defer d.stop()

	// Churn for longer than the hard bound, spaced well under the quiet period.
	const (
		events  = 25
		spacing = 200 * time.Millisecond
	)
	require.Greater(t, events*spacing, eventMaxDebounce,
		"the churn must outlast the hard bound for this test to mean anything")

	for i := 0; i < events; i++ {
		d.debounce(&statusChangeEventFrame{
			change: "DOWN",
			host:   net.IPv4(127, 0, 0, byte(i%5+1)),
			port:   9042,
		})
		time.Sleep(spacing)
	}

	mu.Lock()
	duringChurn := flushes
	mu.Unlock()
	require.NotZero(t, duringChurn, "the debouncer must have flushed while the churn was still going")

	// Everything still arrives: the bound splits a burst into batches, it does not
	// drop any of it.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return delivered == events
	}, snapshotBudget, 20*time.Millisecond, "every event must be delivered once the churn stops")
}

// TestEventDebouncerArmsTheHardDeadline pins that the bound is armed rather than
// merely reached by chance, by watching the delays a real debouncer arms.
func TestEventDebouncerArmsTheHardDeadline(t *testing.T) {
	var (
		mu     sync.Mutex
		delays []time.Duration
	)
	var flushed atomic.Bool

	d := newEventDebouncer("bound", func([]frame) { flushed.Store(true) }, nil, &defaultLogger{})
	defer d.stop()
	d.onResetTimer = func(delay time.Duration) {
		mu.Lock()
		delays = append(delays, delay)
		mu.Unlock()
	}

	for i := 0; i < 25; i++ {
		d.debounce(&statusChangeEventFrame{change: "DOWN", host: net.IPv4(127, 0, 0, 1), port: 9042})
		time.Sleep(200 * time.Millisecond)
		if flushed.Load() {
			break
		}
	}

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, delays, "the debouncer must have armed at least one delay")
	var shortened bool
	for _, delay := range delays {
		require.LessOrEqual(t, delay, eventDebounceTime, "no delay may exceed the quiet period")
		if delay < eventDebounceTime {
			shortened = true
		}
	}
	require.True(t, shortened,
		"the hard deadline must have shortened at least one delay below the quiet period")
}

// TestEventBufferOverflowRequestsARingRefresh pins the compensation half of F-AH-3.
//
// An event that does not fit the buffer is dropped, and node events are the only
// path that reports a node appearing or leaving, so nothing else will ever mention
// what was dropped. A ring refresh is the compensation: it re-reads membership from
// the system tables directly.
//
// This test drives the real Session wiring on purpose. The refresher is built after
// the event debouncer, so a hand-assembled debouncer with a callback wired straight
// to the refresher would bypass the nil receiver a naive method binding captures -
// and would pass while the driver panicked on its first overflow.
func TestEventBufferOverflowRequestsARingRefresh(t *testing.T) {
	refreshes := make(chan error, 64)
	_, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
		cluster.testRingRefreshDone = func(err error) {
			select {
			case refreshes <- err:
			default:
			}
		}
	})

	drain := func() {
		for {
			select {
			case <-refreshes:
			default:
				return
			}
		}
	}
	overflow := func() {
		for i := 0; i <= eventBufferSize; i++ {
			session.nodeEvents.debounce(&statusChangeEventFrame{
				change: "DOWN", host: net.IPv4(127, 0, 0, 1), port: 9042,
			})
		}
	}

	drain()
	overflow()

	// A triggered refresh is immediate. A debounced one would wait out the debounce
	// interval, which is what makes this assertion discriminating.
	select {
	case <-refreshes:
	case <-time.After(ringRefreshDebounceTime / 2):
		t.Fatal("an overflowing event buffer must request a ring refresh at once")
	}

	// Sustained overflow must not postpone the compensation it is asking for. A
	// debounced request would push the timer out on every drop, so a cluster
	// overflowing continuously would never see the refresh it needs most.
	for round := 0; round < 3; round++ {
		drain()
		overflow()
		select {
		case <-refreshes:
		case <-time.After(ringRefreshDebounceTime / 2):
			t.Fatalf("round %d: repeated overflow must not postpone the compensating refresh", round)
		}
	}
}
