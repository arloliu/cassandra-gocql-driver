//go:build ccm && ccmtopology
// +build ccm,ccmtopology

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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/internal/ccm"
	"github.com/stretchr/testify/require"
)

// rejoinAddressBase is where the search for a free loopback alias starts.
//
// It is deliberately far above the cluster's own addresses. nextNodeSpec in
// host_events_ccm_test.go allocates a new node at 127.0.0.<highest octet + 1>,
// so anything just past the last node collides with it depending on which test
// runs first - which is exactly what happened the first time this ran inside the
// full suite. Starting at 200 puts the two allocators out of each other's way,
// and leaves nextNodeSpec's arithmetic intact if it ever runs while a node is
// parked up here.
const rejoinAddressBase = 200

// rejoinAddressFor picks a loopback alias no ring host currently holds.
//
// 127.0.0.0/8 is entirely local on Linux, so no interface setup is needed. The
// address is chosen rather than fixed because a run killed before its cleanup -
// a package timeout, say - leaves a node parked on the alias it was moved to,
// and the next run has to cope with that instead of failing on the fixture.
//
// Returns:
//   - string: an unused 127.0.0.x address
func rejoinAddressFor(t *testing.T, hosts []*HostInfo) string {
	t.Helper()

	taken := make(map[string]bool, len(hosts))
	for _, host := range hosts {
		taken[host.ConnectAddress().String()] = true
	}

	for i := rejoinAddressBase; i < 255; i++ {
		candidate := fmt.Sprintf("127.0.0.%d", i)
		if !taken[candidate] {
			return candidate
		}
	}

	t.Fatal("no free loopback alias for the rejoin address")
	return ""
}

// TestRejoinWithNewAddress moves a node to a new IP and checks whether the
// session follows it.
//
// This is issue #1884: a node leaves and rejoins the ring under the same host_id
// but at a different address - a recycled pod - and the session is reported to
// stop serving queries with "no hosts available in the pool" indefinitely.
//
// The node keeps its data directory, so it keeps its host_id and its tokens;
// only listen_address and rpc_address move. That is what makes this different
// from TestRollingRestartRecovery, where the address never changes.
//
// The two subtests disagree, and that is the result:
//
//   - WithServerEvents passes. A TOPOLOGY_CHANGE for the new address makes the
//     driver refresh its ring, it finds the same host_id at the new address,
//     rebuilds, and serves.
//
//   - WithoutServerEvents FAILS, and is skipped by default for that reason. It
//     is the configuration the issue actually describes - #1582 words it as the
//     topology events "may not fire or are not acted upon", which is routine in
//     k8s - and it is the reproducer.
//
// Root cause, read off events.go and control.go: the ring is refreshed from
// exactly three places. A TOPOLOGY_CHANGE, gated on DisableTopologyEvents
// (events.go:192-194); a STATUS_CHANGE UP naming an IP the ring does not know,
// gated on DisableNodeStatusEvents (events.go:219-222); and a control-connection
// reconnect (control.go:445). There is no periodic refresh. So when the events
// are lost and the control connection is undisturbed - which is the case when
// the node that moved is not the control host - nothing ever rediscovers the new
// address, and reconnectDownedHosts goes on dialling the stale one forever.
// That is the reported symptom exactly.
//
// Fixing it is a design change - some bounded periodic or triggered re-read of
// system.peers - and is deliberately not attempted here. This test is the
// evidence it is needed.
//
// It carries its own build tag because it is destructive to the shared fixture.
// Moving a node leaves the cluster's system.peers holding a row for the address
// it vacated, and putting the node back does not remove that row, so a later
// test sees a peer that will never answer. ccm.AddNode/RemoveNode do not have
// that problem because nodetool removenode cleans up after them; there is no
// equivalent for an address change. Run it on a cluster you are willing to
// rebuild:
//
//	make test-integration TEST_INTEGRATION_TAGS="ccm ccmtopology" \
//	    TEST_OPTS="-run TestRejoinWithNewAddress"
//
// and add GOCQL_RUN_KNOWN_FAILURES=1 to include the failing subtest.
func TestRejoinWithNewAddress(t *testing.T) {
	require.NoError(t, ccm.AllUp())

	clusterInfo, err := ccm.CurrentClusterInfo()
	require.NoError(t, err)
	if len(clusterInfo.Hosts) < 3 {
		t.Skip("this test requires at least 3 nodes")
	}

	t.Run("WithServerEvents", func(t *testing.T) {
		rejoinAtNewAddress(t, clusterInfo, true)
	})

	t.Run("WithoutServerEvents", func(t *testing.T) {
		if os.Getenv("GOCQL_RUN_KNOWN_FAILURES") == "" {
			t.Skip("known failure, issue #1884: with topology and status events off " +
				"nothing refreshes the ring, so the moved host is never rediscovered. " +
				"Set GOCQL_RUN_KNOWN_FAILURES=1 to run it.")
		}
		rejoinAtNewAddress(t, clusterInfo, false)
	})
}

// rejoinAtNewAddress moves one non-control host onto a fresh address and checks
// that the session ends up holding that host_id at the new address, with no
// stale entry left behind and every host still serving.
func rejoinAtNewAddress(t *testing.T, clusterInfo *ccm.ClusterInfo, serverEvents bool) {
	require.NoError(t, ccm.AllUp())

	fixture := newRollingFixture(t, clusterInfo, serverEvents)

	// Move a host that is not carrying the control connection. A control host
	// that moves would be recovered by the control reconnect's own ring refresh,
	// which is a different path and one TestPauseResumeRecovery already covers.
	controlHost := fixture.session.control.getConn().host
	require.NotNil(t, controlHost, "the session must hold a control connection")

	var target *HostInfo
	for _, host := range fixture.session.ring.allHosts() {
		if host.HostID() != controlHost.HostID() {
			target = host
			break
		}
	}
	require.NotNil(t, target, "expected a ring host that does not carry the control connection")

	node := fixture.node(t, target)
	hostID := target.HostID()
	oldAddress := target.ConnectAddress().String()
	rejoinAddress := rejoinAddressFor(t, fixture.session.ring.allHosts())

	t.Logf("moving %s from %s to %s, host_id %s", node, oldAddress, rejoinAddress, hostID)
	t.Cleanup(func() {
		if err := ccm.SetNodeAddress(node, oldAddress); err != nil {
			t.Errorf("failed to move %s back to %s: %v", node, oldAddress, err)
		}
	})
	require.NoError(t, ccm.SetNodeAddress(node, rejoinAddress), "move %s", node)

	// The driver must end up holding this host_id at the new address. Poll the
	// ring rather than waiting on an event: the rebuild removes and re-adds the
	// host, and which notifications that produces is exactly what is in doubt.
	require.Eventually(t, func() bool {
		host, ok := fixture.session.ring.getHost(hostID)
		return ok && host.IsUp() && host.ConnectAddress().String() == rejoinAddress
	}, rollingEventBudget, time.Second,
		"the ring must carry host_id %s at %s", hostID, rejoinAddress)

	// The stale entry must be gone, not merely shadowed: a leftover host at the
	// old address is a pool the driver will keep trying to fill forever.
	for _, host := range fixture.session.ring.allHosts() {
		require.NotEqual(t, oldAddress, host.ConnectAddress().String(),
			"the ring still holds the stale address for host_id %s", host.HostID())
	}

	// And it has to actually serve, which is the symptom the issue reports.
	require.Eventually(t, func() bool {
		return fixture.session.Query("SELECT key FROM system.local").
			Consistency(One).
			SetHostID(hostID).
			Exec() == nil
	}, rollingEventBudget, time.Second,
		"host_id %s must serve a pinned read at its new address", hostID)

	// Every other host must be unharmed by the reconciliation.
	fixture.awaitAllUp(t, len(clusterInfo.Hosts), rollingEventBudget)
	for _, host := range fixture.session.ring.allHosts() {
		require.NoError(t, fixture.session.Query("SELECT key FROM system.local").
			Consistency(One).
			SetHostID(host.HostID()).
			Exec(),
			"host %s must still serve a pinned read", host.ConnectAddress())
	}
}
