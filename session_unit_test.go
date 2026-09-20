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
	"context"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// rejectedSessions is how many times each case asks NewSession for a session it must refuse.
const rejectedSessions = 10

// debouncerWorkerCreator matches the "created by" line of a goroutine
// that one of the session's debouncer constructors started.
//
// The creator is matched rather than the worker's own frame because the creator is recorded
// by the go statement itself, while the frame appears only once the goroutine has been scheduled:
// a worker that has not run yet shows up as a compiler-generated wrapper.
var debouncerWorkerCreator = regexp.MustCompile(`created by \S+\.(newRefreshDebouncer|newEventDebouncer) `)

// TestNewSession_ConfigErrorStartsNoWorkers pins that a refused configuration leaves nothing running.
//
// NewSession has three ordinary error returns that follow its configuration checks.
// The debouncers start a goroutine in their constructors and only Session.Close stops them,
// so they must not be constructed before those returns: nobody holds a session to close.
//
// It is a top-level serial test and must stay one.
// The oracle identifies new workers of the two constructors, not the session that owns them,
// so a session constructed by a test running alongside would be counted here.
func TestNewSession_ConfigErrorStartsNoWorkers(t *testing.T) {
	t.Run("the oracle sees a worker of each constructor", func(t *testing.T) {
		before := debouncerWorkers()
		refresher := newRefreshDebouncer(time.Hour, func() error { return nil }, &defaultLogger{})
		t.Cleanup(refresher.stop)
		started := debouncerWorkersSince(before)
		require.Len(t, started, 1, "one refresh debouncer must be seen as one new worker")
		require.Contains(t, started[0], ".newRefreshDebouncer ")

		before = debouncerWorkers()
		events := newEventDebouncer("oracle control", func([]frame) {}, func() {}, &defaultLogger{})
		t.Cleanup(events.stop)
		started = debouncerWorkersSince(before)
		require.Len(t, started, 1, "one event debouncer must be seen as one new worker")
		require.Contains(t, started[0], ".newEventDebouncer ")
	})

	tests := []struct {
		name      string
		configure func(t *testing.T, cluster *ClusterConfig)
		wantErr   string
	}{
		{
			name: "schema listener with the metadata cache disabled",
			configure: func(_ *testing.T, cluster *ClusterConfig) {
				cluster.Metadata.CacheMode = Disabled
				cluster.Metadata.SchemaListener.KeyspaceChangeListener = &mockKeyspaceChangeListener{}
			},
			wantErr: "Disabled metadata cache mode",
		},
		{
			name: "table listener with a keyspace-only metadata cache",
			configure: func(_ *testing.T, cluster *ClusterConfig) {
				cluster.Metadata.CacheMode = KeyspaceOnly
				cluster.Metadata.SchemaListener.TableChangeListener = &mockTableChangeListener{}
			},
			wantErr: "KeyspaceOnly metadata cache mode",
		},
		{
			// HostDialer stays unset: a custom dialer bypasses the TLS configuration.
			name: "TLS options naming a CA file that does not exist",
			configure: func(t *testing.T, cluster *ClusterConfig) {
				cluster.SslOpts = &SslOptions{CaPath: filepath.Join(t.TempDir(), "missing-ca.pem")}
			},
			wantErr: "unable to open CA certs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := NewCluster("127.0.0.1")
			tt.configure(t, cluster)

			before := debouncerWorkers()
			for range rejectedSessions {
				_, err := NewSession(*cluster)
				require.ErrorContains(t, err, tt.wantErr, "the case must leave through the return it names")
			}

			// Counted at once: a leaked worker is visible from its go statement and never exits.
			survivors := debouncerWorkersSince(before)
			require.Zerof(t, len(survivors), "%d debouncer worker(s) survive a rejected NewSession:\n%s",
				len(survivors), strings.Join(survivors, "\n"))
		})
	}
}

func TestAsyncSessionInit(t *testing.T) {
	// Parallel: this test spends its time waiting out a real interval, and owns
	// every fixture it touches - its own server on its own port, its own
	// cluster. See "Parallel tests" in .agents/rules/300-testing.md.
	t.Parallel()

	// Build a 3 node cluster to test host metric mapping
	var addresses = []string{
		"127.0.0.1",
		"127.0.0.2",
		"127.0.0.3",
	}
	// only build 1 of the servers so that we can test not connecting to the last
	// one
	srv := NewTestServerWithAddress(addresses[0]+":0", t, defaultProto, context.Background())
	defer srv.Stop()

	// just choose any port
	cluster := testCluster(defaultProto, srv.Address, addresses[1]+":9999", addresses[2]+":9999")
	cluster.PoolConfig.HostSelectionPolicy = SingleHostReadyPolicy(RoundRobinHostPolicy())
	db, err := cluster.CreateSession()
	if err != nil {
		t.Fatalf("NewCluster: %v", err)
	}
	defer db.Close()

	// make sure the session works
	if err := db.Query("void").Exec(); err != nil {
		t.Fatalf("unexpected error from void")
	}
}

// debouncerWorkers lists the live goroutines that a debouncer constructor started.
//
// Returns:
//   - map[string]string: each worker's stack block, keyed by its "goroutine N" header
func debouncerWorkers() map[string]string {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}

	workers := make(map[string]string)
	for block := range strings.SplitSeq(string(buf), "\n\n") {
		if !debouncerWorkerCreator.MatchString(block) {
			continue
		}
		header, _, _ := strings.Cut(block, " [")
		workers[header] = block
	}
	return workers
}

// debouncerWorkersSince lists the debouncer workers that were not in an earlier listing.
//
// It compares identities rather than totals, so a worker of an earlier test that exits
// in between cannot hide a new one: an exit only ever removes an identity.
//
// Parameters:
//   - before: an earlier result of debouncerWorkers
//
// Returns:
//   - []string: the "created by" line of each worker started since
func debouncerWorkersSince(before map[string]string) []string {
	var started []string
	for id, block := range debouncerWorkers() {
		if _, ok := before[id]; ok {
			continue
		}
		started = append(started, id+": "+debouncerWorkerCreator.FindString(block))
	}
	return started
}

// removePanicPolicy panics once in RemoveHost, before delegating.
type removePanicPolicy struct {
	HostSelectionPolicy
	armed  atomic.Bool
	panics atomic.Int32
}

func (p *removePanicPolicy) RemoveHost(h *HostInfo) {
	if p.armed.CompareAndSwap(true, false) {
		p.panics.Add(1)
		panic("test: policy RemoveHost panic")
	}
	p.HostSelectionPolicy.RemoveHost(h)
}

// TestSession_RemoveHostPolicyPanicStillRemovesThePool is the inversion of
// _repro/zz_repro_round3_test.go's TestRepro_RemoveHostPolicyPanicStrandsPool (F-sess-1).
//
// removeHost took the host out of the ring, then called the application's RemoveHost,
// then removed the pool. runRingRefresh's guard turns a policy panic into a failed round,
// and no later round retries: the object is no longer in the ring, so it is in no
// snapshot's previous set and the DOWN-host sweep, which walks the ring, never reaches
// it. The departed host kept a registered pool that was still handing out a live
// connection.
func TestSession_RemoveHostPolicyPanicStillRemovesThePool(t *testing.T) {
	const departingHostID = "dddddddd-0000-4000-8000-00000000beef"
	const departingAddr = "127.0.0.2"

	inner := RoundRobinHostPolicy()
	policy := &removePanicPolicy{HostSelectionPolicy: inner}
	var listener *stateListener
	script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
		cluster.PoolConfig.HostSelectionPolicy = policy
		listener = newStateListener(cluster)
	})

	script.setPeers([]peerRow{newPeerRow(departingHostID, departingAddr)})
	require.NoError(t, session.refreshRing(), "adopt the peer")
	select {
	case <-listener.up:
	case <-time.After(fillEventBudget):
		t.Fatal("the peer never came up")
	}
	host, ok := session.ring.getHost(departingHostID)
	require.True(t, ok, "the peer must be in the ring")
	pool, ok := session.pool.getPoolFor(host)
	require.True(t, ok, "the peer must have a pool")
	require.Eventually(t, func() bool { return pool.Size() == 1 }, fillEventBudget, 5*time.Millisecond,
		"the peer's pool must be filled before the removal")
	departing := pool.Pick()
	require.NotNil(t, departing, "the peer's pool must hand out a connection before the removal")

	// The peer leaves the ring and the application's RemoveHost panics once.
	policy.armed.Store(true)
	script.setPeers(nil)
	err := session.refreshRing()
	require.Error(t, err, "the panicking round must still be reported as failed")
	require.EqualValues(t, 1, policy.panics.Load(), "the policy panic was not reached")

	_, inRing := session.ring.getHost(departingHostID)
	require.False(t, inRing, "the host left the ring")
	if _, registered := session.pool.getPoolByHostID(departingHostID); registered {
		t.Fatal("pool still registered after the panicking round")
	}

	// The physical close is asynchronous — removeHostPool schedules it — so this
	// waits for evidence rather than reading the flag straight after unregistration.
	//
	// The connection itself is the oracle, not pool.Pick(): Pick stops handing out
	// connections as soon as the pool is marked closed, which is before the loop that
	// closes them, so a teardown that never closed anything would satisfy it.
	require.Eventually(t, departing.Closed, fillEventBudget, 5*time.Millisecond,
		"the removed pool's connection must be closed")
}
