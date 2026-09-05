//go:build ccm
// +build ccm

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
//   - serverEvents: whether the server's own STATUS_CHANGE notifications are
//     delivered. With them off, recovery has to come from the driver alone -
//     reconnectDownedHosts and the control connection's reconnect - which is the
//     configuration #1582 actually describes, where the topology events "may not
//     fire or are not acted upon".
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
// Both subtests matter, and the second is the one with teeth. #1582 says the
// server's topology events "may not fire or are not acted upon", so a pass that
// depends on STATUS_CHANGE proves nothing about that complaint. WithoutServerEvents
// switches them off, leaving recovery entirely to the driver: reconnectDownedHosts
// and the control connection's own reconnect, which is what the paused-node work
// put in place.
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

	t.Run("WithServerEvents", func(t *testing.T) {
		rollTheCluster(t, clusterInfo, true)
	})

	t.Run("WithoutServerEvents", func(t *testing.T) {
		rollTheCluster(t, clusterInfo, false)
	})
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
