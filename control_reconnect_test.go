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

// controlTestBudget bounds every positive wait in these tests.
const controlTestBudget = 10 * time.Second

// TestControlReconnect_DoesNotWaitOnRingFlusher proves controlConn.reconnect
// returns while the ring refresher is busy.
//
// Before the fix, reconnect ended with a synchronous refreshRing, which waits for
// the ring flusher to run a refresh. A refresh whose control query fails on write
// reaches HandleError, and so reconnect, on the flusher's own goroutine; the wait
// was then on itself, and Session.Close hung behind it. Here the flusher is held
// inside a refresh while reconnect is called from another goroutine: with the
// synchronous refresh it never returns; with the debounced request it returns at
// once and the refresh it asked for runs after the flusher is released.
func TestControlReconnect_DoesNotWaitOnRingFlusher(t *testing.T) {
	entered := make(chan struct{}, 8)
	gate := make(chan struct{})
	var release sync.Once

	var hold atomic.Bool
	_, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
		cluster.testRingRefreshHook = func() {
			entered <- struct{}{}
			if hold.Load() {
				<-gate
			}
		}
	})
	// Registered after the fixture's Session.Close, so it runs before it:
	// Close waits for the flusher, which must not be left parked on the gate.
	t.Cleanup(func() { release.Do(func() { close(gate) }) })

	before := session.control.getConn()
	require.NotNil(t, before, "the fixture must hold a control connection")

	hold.Store(true)
	session.ringRefresher.trigger()
	select {
	case <-entered:
	case <-time.After(controlTestBudget):
		t.Fatal("the triggered refresh did not start")
	}

	returned := make(chan struct{})
	go func() {
		session.control.reconnect()
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(controlTestBudget):
		t.Fatal("reconnect waited on the ring flusher")
	}
	require.NotSame(t, before, session.control.getConn(), "reconnect must have replaced the control connection")

	hold.Store(false)
	release.Do(func() { close(gate) })

	select {
	case <-entered:
	case <-time.After(controlTestBudget):
		t.Fatal("the refresh requested by reconnect did not run after the flusher was released")
	}
}
