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

// addHostGatePolicy wraps a RoundRobin policy and records AddHost calls, with an
// optional per-call action so a test can make AddHost block or re-enter Close.
type addHostGatePolicy struct {
	HostSelectionPolicy

	mu       sync.Mutex
	addHosts []*HostInfo

	onAddHost func(host *HostInfo)
}

func newAddHostGatePolicy() *addHostGatePolicy {
	return &addHostGatePolicy{HostSelectionPolicy: RoundRobinHostPolicy()}
}

// AddHost records the host and runs onAddHost, if set, before forwarding.
func (p *addHostGatePolicy) AddHost(host *HostInfo) {
	p.mu.Lock()
	p.addHosts = append(p.addHosts, host)
	fn := p.onAddHost
	p.mu.Unlock()
	if fn != nil {
		fn(host)
	}
	p.HostSelectionPolicy.AddHost(host)
}

// setOnAddHost sets the per-call action.
func (p *addHostGatePolicy) setOnAddHost(fn func(host *HostInfo)) {
	p.mu.Lock()
	p.onAddHost = fn
	p.mu.Unlock()
}

// addHostCount returns how many times AddHost was called.
func (p *addHostGatePolicy) addHostCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.addHosts)
}

// controlHost returns the single ring host of a snapshotFixture.
func controlHost(t *testing.T, f *snapshotFixture) *HostInfo {
	t.Helper()
	hosts := f.session.ring.allHosts()
	require.Len(t, hosts, 1, "the fixture must hold exactly one ring host")
	return hosts[0]
}

// TestWithOwnedHostRejectsAfterClose proves the shutdown gate on withOwnedHost:
// once the flag is set a not-yet-admitted callback is rejected and the policy is
// not called, while a callback admitted before the flag was set still runs.
func TestWithOwnedHostRejectsAfterClose(t *testing.T) {
	t.Run("flag set before callback rejects it", func(t *testing.T) {
		f := newSnapshotFixture(t, nil)
		host := controlHost(t, f)

		f.session.hostPublishClosed.Store(true)
		called := false
		ok := f.session.withOwnedHost(host, func() bool {
			called = true
			return true
		})
		require.False(t, ok, "a transition must be rejected once the gate is closed")
		require.False(t, called, "the gated callback must not run")
	})

	t.Run("admitted callback held through Close still runs", func(t *testing.T) {
		f := newSnapshotFixture(t, nil)
		host := controlHost(t, f)

		// The callback passes the gate check while the flag is clear (admitted),
		// then blocks. Close runs to completion while it is held: Close sets the
		// flag but takes no hostPublishMu, so it does not wait on this callback.
		entered := make(chan struct{})
		release := make(chan struct{})
		result := make(chan bool, 1)
		go func() {
			result <- f.session.withOwnedHost(host, func() bool {
				close(entered)
				<-release
				return true
			})
		}()

		select {
		case <-entered:
		case <-time.After(lifecycleBudget):
			t.Fatal("the callback did not enter withOwnedHost")
		}

		closed := asyncClose(f.session)
		select {
		case <-closed:
		case <-time.After(lifecycleBudget):
			t.Fatal("Close waited on the admitted callback")
		}

		// Only now release it; an admitted callback runs to completion.
		close(release)
		select {
		case ok := <-result:
			require.True(t, ok, "an admitted callback must run to completion even across Close")
		case <-time.After(lifecycleBudget):
			t.Fatal("the admitted callback did not finish")
		}
	})

	t.Run("reentrant Close from an independent fill does not deadlock", func(t *testing.T) {
		policy := newAddHostGatePolicy()
		f := newSnapshotFixture(t, func(cluster *ClusterConfig) {
			cluster.PoolConfig.HostSelectionPolicy = policy
		})
		host := controlHost(t, f)

		// AddHost, run from an independent startPoolFill goroutine, synchronously
		// calls Session.Close. Because Close does not take hostPublishMu, the
		// mutex withOwnedHost holds around this call is not the one Close needs,
		// so there is no self-deadlock. (The ring-flusher reentrant path remains
		// a known limitation and is not exercised here.)
		var once sync.Once
		policy.setOnAddHost(func(*HostInfo) {
			once.Do(func() { f.session.Close() })
		})

		done := make(chan struct{})
		go func() {
			f.session.startPoolFill(host)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(lifecycleBudget):
			t.Fatal("startPoolFill deadlocked when AddHost re-entered Close")
		}
	})
}

// TestWithOwnedHostCallbackWaitingForCancellation proves Close does not wait on
// an already-admitted policy callback: the callback blocks until the session
// context is cancelled, and Close, which never takes hostPublishMu, cancels it.
func TestWithOwnedHostCallbackWaitingForCancellation(t *testing.T) {
	f := newSnapshotFixture(t, nil)
	host := controlHost(t, f)

	entered := make(chan struct{})
	callbackDone := make(chan struct{})
	go func() {
		f.session.withOwnedHost(host, func() bool {
			close(entered)
			<-f.session.ctx.Done()
			return true
		})
		close(callbackDone)
	}()

	select {
	case <-entered:
	case <-time.After(lifecycleBudget):
		t.Fatal("the callback did not enter withOwnedHost")
	}

	// Close must return even though an admitted callback still holds hostPublishMu,
	// because Close never takes that mutex; its context cancel then releases the
	// callback.
	closed := asyncClose(f.session)
	select {
	case <-closed:
	case <-time.After(lifecycleBudget):
		t.Fatal("Session.Close waited on the admitted callback")
	}
	select {
	case <-callbackDone:
	case <-time.After(lifecycleBudget):
		t.Fatal("the admitted callback did not finish after the context was cancelled")
	}
}

// TestControlConnPublishedConnClosedWhenPublishWins proves that when a control
// connection is published just before Session.Close, Close closes that published
// transport and the fill it scheduled does not re-publish the host: the fill's
// AddHost is rejected by the shutdown gate.
func TestControlConnPublishedConnClosedWhenPublishWins(t *testing.T) {
	policy := newAddHostGatePolicy()
	fillStarted := make(chan *HostInfo, 4)
	fillGate := make(chan struct{})
	fillDone := make(chan *HostInfo, 4)
	var armed atomic.Bool
	f := newSnapshotFixture(t, func(cluster *ClusterConfig) {
		cluster.PoolConfig.HostSelectionPolicy = policy
		cluster.testStartPoolFillStart = func(host *HostInfo) {
			if armed.Load() {
				fillStarted <- host
				<-fillGate
			}
		}
		cluster.testStartPoolFillDone = func(host *HostInfo) { fillDone <- host }
	})
	f.drain()

	addsBefore := policy.addHostCount()
	armed.Store(true)

	// Reconnect publishes a new control connection and schedules its fill, which
	// parks at its start before reaching the shutdown gate.
	before := f.session.control.getConn()
	reconnected := make(chan struct{})
	go func() {
		f.session.control.reconnect()
		close(reconnected)
	}()

	select {
	case <-fillStarted:
	case <-time.After(lifecycleBudget):
		t.Fatal("the reconnect did not schedule a pool fill")
	}
	published := f.session.control.getConn()
	require.NotSame(t, before, published, "the reconnect must have published a new control connection")
	publishedConn := f.dialer.lastConn()
	require.NotNil(t, publishedConn)

	// Close while the fill is parked: it closes the just-published transport and
	// sets the host-publication gate.
	closed := asyncClose(f.session)
	select {
	case <-closed:
	case <-time.After(closeBudget):
		t.Fatalf("Session.Close did not return within %v", closeBudget)
	}
	require.True(t, publishedConn.transportCloseInvoked.Load(),
		"Close must have closed the published control transport")

	// Release the fill; its AddHost must find the gate closed.
	close(fillGate)
	select {
	case <-reconnected:
	case <-time.After(lifecycleBudget):
		t.Fatal("reconnect did not return")
	}
	select {
	case <-fillDone:
	case <-time.After(lifecycleBudget):
		t.Fatal("the parked fill did not finish")
	}
	require.Equal(t, addsBefore, policy.addHostCount(),
		"a fill running after the gate closed must not re-publish its host")
}
