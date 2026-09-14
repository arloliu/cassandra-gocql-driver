//go:build all || ccm
// +build all ccm

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
	"testing"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/internal/ccm"
	"github.com/stretchr/testify/require"
)

// rollingEventBudget bounds how long the test waits for one host transition.
// Stopping and starting a real Cassandra node is slow, so it is generous.
const rollingEventBudget = 90 * time.Second

// inflightPinnedBudget bounds how long a read pinned to a host that is DOWN may
// take to come back with an error.
//
// It is not a latency target. The failure it exists to catch is the one #1582
// reports - a request that never returns - so anything short of the package
// timeout would do; this is only small enough to fail the test quickly.
const inflightPinnedBudget = 30 * time.Second

// rollingFixture is one session against the ccm cluster, observing host state.
type rollingFixture struct {
	session  *Session
	listener *pauseHostStateListener
	// nodes maps a host's connect address to its ccm node name.
	nodes map[string]string
}

// newRollingFixture connects a session against the ccm cluster.
//
// Parameters:
//   - serverEvents: whether the server's own STATUS_CHANGE and TOPOLOGY_CHANGE
//     notifications are delivered. With them off, the driver has to notice
//     everything itself - reconnectDownedHosts, the control connection's
//     reconnect, and its own ring refresh - which is the configuration #1582 and
//     #1884 actually describe, where the topology events "may not fire or are not
//     acted upon". Topology matters as much as status here: NEW_NODE is what would
//     otherwise hand the driver an address it has not seen before.
//
// Returns:
//   - *rollingFixture: fixture whose session is closed by t.Cleanup
func newRollingFixture(t *testing.T, clusterInfo *ccm.ClusterInfo, serverEvents bool) *rollingFixture {
	t.Helper()

	nodes := make(map[string]string, len(clusterInfo.Hosts))
	for _, node := range clusterInfo.Hosts {
		nodes[node.Addr] = node.Name
	}

	listener := newPauseHostStateListener()
	cluster := createCluster(func(cfg *ClusterConfig) {
		cfg.Hosts = clusterInfo.HostAddrs()
		cfg.ReconnectInterval = 500 * time.Millisecond
		cfg.ReconnectionPolicy = &ConstantReconnectionPolicy{MaxRetries: 3, Interval: time.Second}
		cfg.Metadata.HostListener.HostStateChangeListener = listener
		cfg.Events.DisableNodeStatusEvents = !serverEvents
		cfg.Events.DisableTopologyEvents = !serverEvents
	})

	session, err := cluster.CreateSession()
	require.NoError(t, err, "CreateSession")
	t.Cleanup(session.Close)

	return &rollingFixture{session: session, listener: listener, nodes: nodes}
}

// node returns the ccm node name backing host.
//
// Returns:
//   - string: the node name, e.g. "node2"
func (f *rollingFixture) node(t *testing.T, host *HostInfo) string {
	t.Helper()

	node := f.nodes[host.ConnectAddress().String()]
	require.NotEmpty(t, node, "no ccm node for host %s", host.ConnectAddress())
	return node
}

// awaitAllUp blocks until every ring host reports UP, polling the ring rather
// than the event channel so it cannot miss a transition it did not subscribe to
// in time.
func (f *rollingFixture) awaitAllUp(t *testing.T, want int, budget time.Duration) {
	t.Helper()

	deadline := time.After(budget)
	for {
		hosts := f.session.ring.allHosts()
		up := 0
		for _, host := range hosts {
			if host.IsUp() {
				up++
			}
		}
		if up == want && len(hosts) == want {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("timed out after %v with %d/%d ring hosts up: %s",
				budget, up, want, ringString(hosts))
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// TestRollingRestartRecovery restarts every node in turn and proves no host is
// left stranded DOWN.
//
// This is the symptom issue #1582 reports: after a rolling restart of the whole
// cluster the client "remains stuck with all hosts DOWN".
//
// Server events are switched off. #1582 says the server's topology events "may
// not fire or are not acted upon", so a pass that depends on STATUS_CHANGE would
// prove nothing about that complaint; with them off, recovery has to come from
// reconnectDownedHosts and the control connection's own reconnect, which is what
// the paused-node work put in place. This is strictly the harder configuration,
// and the events-enabled one was verified separately and also passes - running
// only this one keeps the ccm suite inside its timeout.
//
// Scope: the nodes keep their addresses. The issue's other half, a whole cluster
// coming back on new IPs behind unchanged hostnames, is served by the control
// connection's fallback to freshly resolved contact points (control.go, "Fallback
// to initial contact points"); reproducing that needs name resolution the test can
// steer, which ccm on fixed loopback addresses cannot provide.
func TestRollingRestartRecovery(t *testing.T) {
	require.NoError(t, ccm.AllUp())

	clusterInfo, err := ccm.CurrentClusterInfo()
	require.NoError(t, err)
	if len(clusterInfo.Hosts) < 3 {
		t.Skip("this test requires at least 3 nodes")
	}

	rollTheCluster(t, clusterInfo, false)
}

// rollTheCluster stops and starts every node in turn, checking after each one that
// the driver noticed both transitions and can serve a read pinned to that host,
// then that every host is back once the whole sweep is done.
func rollTheCluster(t *testing.T, clusterInfo *ccm.ClusterInfo, serverEvents bool) {
	require.NoError(t, ccm.AllUp())

	fixture := newRollingFixture(t, clusterInfo, serverEvents)
	hosts := fixture.session.ring.allHosts()
	require.Len(t, hosts, len(clusterInfo.Hosts), "the session must see the whole ring")

	for _, host := range hosts {
		node := fixture.node(t, host)
		hostID := host.HostID()

		t.Logf("restarting %s (%s)", node, host.ConnectAddress())
		require.NoError(t, ccm.NodeDown(node), "stop %s", node)
		t.Cleanup(func() { _ = ccm.NodeUp(node) })

		awaitHostEvent(t, fixture.listener.down, hostID, rollingEventBudget,
			"the driver to notice "+node+" went down")

		require.NoError(t, ccm.NodeUp(node), "start %s", node)
		awaitHostEvent(t, fixture.listener.up, hostID, rollingEventBudget,
			"the driver to recover "+node)

		// The host is only genuinely back when it can serve a read itself.
		require.Eventually(t, func() bool {
			return fixture.session.Query("SELECT key FROM system.local").
				Consistency(One).
				SetHostID(hostID).
				Exec() == nil
		}, rollingEventBudget, 500*time.Millisecond,
			"%s must serve a pinned read again after the restart", node)
	}

	// Every host, not just the last one: the failure #1582 reports is that hosts
	// restarted earlier in the sweep never come back.
	fixture.awaitAllUp(t, len(clusterInfo.Hosts), rollingEventBudget)

	for _, host := range fixture.session.ring.allHosts() {
		require.NoError(t, fixture.session.Query("SELECT key FROM system.local").
			Consistency(One).
			SetHostID(host.HostID()).
			Exec(),
			"host %s must serve a pinned read once the whole cluster has been rolled",
			host.ConnectAddress())
	}
}

// TestRollingRestartServesThroughOneRestart checks service during one restart.
//
// TestRollingRestartRecovery drives a node down and back up and only then asks
// anyone to serve a read (rollTheCluster below), so a driver that stalled every
// host for the duration of a restart would still pass it. This test issues
// queries while the node is down.
//
// Three things are checked, and they fail for different reasons:
//
//   - The untouched hosts keep serving, probed repeatedly with every probe
//     required to succeed. #1582's complaint is a client "stuck with all hosts
//     DOWN", and one node leaving must not take the others' pools with it.
//     There is no success-rate threshold on purpose: a threshold is a number
//     nobody can justify and every future flake hides behind.
//
//   - No untouched host is reported DOWN at any point, not even briefly and not
//     after the probes stop. Probes cannot see a blip that heals between two of
//     them, and awaitHostEvent discards other hosts' events - rightly, when a
//     test only cares about one host. The listener's own ledger answers this
//     instead, and it is read once at the end so the whole window is covered.
//
//   - A read pinned to the node that is down comes back with an error rather
//     than hanging. A pinned request is bound to one host and is never moved to
//     a replacement (query_executor.go, executeQuery), so the only correct
//     answers are "fails" and "fails"; "never returns" is the bug. The error's
//     identity is deliberately not asserted - which one surfaces depends on
//     where in the shutdown the request lands - only that one arrives.
//
// Every host is required to serve before anything is stopped, and the ledger is
// reset at that point. CreateSession returns once the aggregate pool is
// non-empty (session.go, ErrNoConnectionsStarted), so without both steps a host
// still recovering from a failed initial connection would either fail the first
// probe or leave a queued DOWN to be charged to the restart.
//
// Scope: one node, not a sweep. The checks exercise the same mechanism once,
// and "no host is left stranded after a full sweep" is already
// TestRollingRestartRecovery's verdict. Server events are off for the reason
// given on that test: it is the strictly harder configuration.
func TestRollingRestartServesThroughOneRestart(t *testing.T) {
	require.NoError(t, ccm.AllUp())

	clusterInfo, err := ccm.CurrentClusterInfo()
	require.NoError(t, err)
	if len(clusterInfo.Hosts) < 3 {
		t.Skip("this test requires at least 3 nodes")
	}

	ctx := t.Context()
	fixture := newRollingFixture(t, clusterInfo, false)

	// Restart a host that is not carrying the control connection. A control
	// host going down also forces a control reconnect, which is a different
	// recovery path - TestPauseResumeRecovery covers that one - and it would
	// blur what a failure here means.
	controlHost := fixture.session.control.getConn().host
	require.NotNil(t, controlHost, "the session must hold a control connection")

	var target *HostInfo
	var others []*HostInfo
	for _, host := range fixture.session.ring.allHosts() {
		if host.HostID() != controlHost.HostID() && target == nil {
			target = host
			continue
		}
		others = append(others, host)
	}
	require.NotNil(t, target, "expected a ring host that does not carry the control connection")
	require.Len(t, others, len(clusterInfo.Hosts)-1, "every other host must be watched")

	node := fixture.node(t, target)
	targetID := target.HostID()

	// The starting state has to be proven, not assumed: every probe after this
	// point is mandatory, so an unready host would be charged to the restart.
	for _, host := range fixture.session.ring.allHosts() {
		require.Eventually(t, func() bool {
			return fixture.session.Query("SELECT key FROM system.local").
				Consistency(One).
				SetHostID(host.HostID()).
				ExecContext(ctx) == nil
		}, rollingEventBudget, 500*time.Millisecond,
			"host %s must serve before the restart begins", host.ConnectAddress())
	}

	// Everything observed from here belongs to the restart.
	fixture.listener.resetDownSeen()

	t.Logf("restarting %s (%s) with queries in flight", node, target.ConnectAddress())
	require.NoError(t, ccm.NodeDown(node), "stop %s", node)
	t.Cleanup(func() { _ = ccm.NodeUp(node) })

	// Wait for the driver to notice before asserting anything. Until it has, a
	// request can still be sitting on a socket that is gone, and this test would
	// be measuring TCP behaviour rather than the driver's.
	awaitHostEvent(t, fixture.listener.down, targetID, rollingEventBudget,
		"the driver to notice "+node+" went down")

	// Repeatedly, not once: the requirement is that the others serve for the
	// whole time the node is down, and a single probe cannot tell a host that
	// kept working from one that happened to be between failures.
	for i := 0; i < 8; i++ {
		for _, host := range others {
			require.NoError(t, fixture.session.Query("SELECT key FROM system.local").
				Consistency(One).
				SetHostID(host.HostID()).
				ExecContext(ctx),
				"host %s must keep serving while %s is down (probe %d)",
				host.ConnectAddress(), node, i)
		}
		// Spacing between samples, not a wait for state: nothing here is being
		// given time to become true. Every probe above must already succeed,
		// and the gap only spreads them across the outage instead of firing
		// them back to back.
		time.Sleep(250 * time.Millisecond)
	}

	// The pinned read must come back. Run it off the test goroutine so a hang
	// fails the test here instead of stalling until the package timeout, where
	// it would look like an unrelated problem.
	pinned := make(chan error, 1)
	pinnedDone := make(chan struct{})
	go func() {
		defer close(pinnedDone)
		pinned <- fixture.session.Query("SELECT key FROM system.local").
			Consistency(One).
			SetHostID(targetID).
			ExecContext(ctx)
	}()

	// Registered after the cleanup that restarts the node, so it runs before it:
	// cleanups are LIFO. Cancelling the context only signals the query, so on the
	// watchdog path this bounded wait is what observes it finishing. The bound is
	// not a guarantee: if it expires the failure is reported and the node is
	// restarted anyway, so the query is reported as unconfirmed rather than
	// silently raced against the node coming back.
	t.Cleanup(func() {
		select {
		case <-pinnedDone:
		case <-time.After(inflightPinnedBudget):
			t.Errorf("the pinned read was still running %s after the test gave up on it; "+
				"the node is about to be restarted with its completion unconfirmed",
				inflightPinnedBudget)
		}
	})

	select {
	case err := <-pinned:
		require.Error(t, err,
			"a read pinned to %s must not succeed while it is down", node)
	case <-time.After(inflightPinnedBudget):
		t.Fatalf("a read pinned to %s did not return within %s while it was down; "+
			"a pinned request is never moved to another host, so it must fail rather than hang",
			node, inflightPinnedBudget)
	}

	require.NoError(t, ccm.NodeUp(node), "start %s", node)
	awaitHostEvent(t, fixture.listener.up, targetID, rollingEventBudget,
		"the driver to recover "+node)

	// And the restarted host serves again, so the test cannot pass by leaving
	// the cluster in a state where only the untouched hosts work.
	require.Eventually(t, func() bool {
		return fixture.session.Query("SELECT key FROM system.local").
			Consistency(One).
			SetHostID(targetID).
			ExecContext(ctx) == nil
	}, rollingEventBudget, 500*time.Millisecond,
		"%s must serve a pinned read again after the restart", node)

	fixture.awaitAllUp(t, len(clusterInfo.Hosts), rollingEventBudget)

	// Read last, so it covers the restart end to end - including anything that
	// went down and recovered after the probes stopped, which awaitAllUp cannot
	// see because it only looks at the state hosts are in now.
	require.Empty(t, fixture.listener.downSeenExcept(targetID),
		"no host other than %s may have been reported down during the restart", node)
}
