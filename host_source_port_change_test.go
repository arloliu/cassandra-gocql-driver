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
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// portRecordingDialer redirects every dial to one address and records the endpoint
// it was asked for, so a test can see which port the driver would really dial.
type portRecordingDialer struct {
	target string

	mu    sync.Mutex
	asked []string
}

var _ HostDialer = (*portRecordingDialer)(nil)

func (d *portRecordingDialer) DialHost(ctx context.Context, host *HostInfo) (*DialedHost, error) {
	d.mu.Lock()
	d.asked = append(d.asked, host.ConnectAddressAndPort())
	d.mu.Unlock()

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", d.target)
	if err != nil {
		return nil, err
	}
	return &DialedHost{Conn: conn}, nil
}

// askedFor reports whether the dialer was ever handed this endpoint.
func (d *portRecordingDialer) askedFor(endpoint string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, asked := range d.asked {
		if asked == endpoint {
			return true
		}
	}
	return false
}

// recordingTopologyListener counts the topology callbacks refreshRing makes.
type recordingTopologyListener struct {
	mu      sync.Mutex
	added   []string
	removed []string
}

var _ TopologyChangeListener = (*recordingTopologyListener)(nil)

func (l *recordingTopologyListener) OnNewHost(event NewHostEvent) {
	l.mu.Lock()
	l.added = append(l.added, event.Host.ConnectAddressAndPort())
	l.mu.Unlock()
}

func (l *recordingTopologyListener) OnRemovedHost(event RemovedHostEvent) {
	l.mu.Lock()
	l.removed = append(l.removed, event.Host.ConnectAddressAndPort())
	l.mu.Unlock()
}

func (l *recordingTopologyListener) snapshot() (added, removed []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string{}, l.added...), append([]string{}, l.removed...)
}

// TestRefreshRingTreatsAPortChangeAsAnEndpointChange pins M3.
//
// refreshRing compared a peer's new description with the ring entry by address
// alone. A node that moved only its native_transport_port therefore took the "no
// change" branch, and HostInfo.update adopts a port only when the entry has none, so
// the new port was dropped: the ring kept the old endpoint and every dial went to a
// port nothing was listening on, for as long as the node's addresses stayed put.
//
// The port is half of an endpoint, so a port change has to be reconciled the same
// way an address change is: remove the entry and re-add the new one, which is the
// only path on which a rebuilt endpoint reaches the ring, the pool and the policy.
func TestRefreshRingTreatsAPortChangeAsAnEndpointChange(t *testing.T) {
	const peerHostID = "44444444-0000-4000-8000-00000000beef"

	listener := &recordingTopologyListener{}
	dialer := &portRecordingDialer{}
	script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, serverAddress string) {
		dialer.target = serverAddress
		cluster.HostDialer = dialer
		cluster.Metadata.HostListener.TopologyChangeListener = listener
	})

	peer := newPeerRow(peerHostID, "127.0.0.2")
	peer.nativePort = 9042
	script.setPeers([]peerRow{peer})
	require.NoError(t, session.refreshRing(), "adopt the peer")

	before, ok := session.ring.getHost(peerHostID)
	require.True(t, ok, "the peer must be in the ring")
	require.Equal(t, "127.0.0.2:9042", before.ConnectAddressAndPort(), "the peer's initial endpoint")

	oldPool, ok := session.pool.getPoolFor(before)
	require.True(t, ok, "the peer must own a pool before the port moves")
	addedBefore, removedBefore := listener.snapshot()

	// Only the port moves. Both addresses stay exactly where they were.
	peer.nativePort = 19042
	script.setPeers([]peerRow{peer})
	require.NoError(t, session.refreshRing(), "reconcile the port change")

	after, ok := session.ring.getHost(peerHostID)
	require.True(t, ok, "the peer must still be in the ring")
	require.Equal(t, "127.0.0.2:19042", after.ConnectAddressAndPort(), "the ring must carry the new port")
	require.NotSame(t, before, after, "a port change must replace the ring entry, not update it")
	require.Equal(t, before.nodeToNodeAddress().String(), after.nodeToNodeAddress().String(),
		"the addresses must be unchanged - only the port moved")

	gotAdded, gotRemoved := listener.snapshot()
	require.Equal(t, append(append([]string{}, removedBefore...), "127.0.0.2:9042"), gotRemoved,
		"the reconciliation must report the old endpoint removed exactly once")
	require.Equal(t, append(append([]string{}, addedBefore...), "127.0.0.2:19042"), gotAdded,
		"the reconciliation must report the new endpoint added exactly once")

	// The pool follows the ring: the old pool is closed for good, the replaced object
	// no longer owns one, and the driver dials the new endpoint.
	_, state := oldPool.pickOrState()
	require.Equal(t, poolClosed, state, "the pool of the replaced entry must have been closed")
	_, ok = session.pool.getPoolFor(before)
	require.False(t, ok, "the replaced entry must no longer own a pool")
	require.Eventually(t, func() bool { return dialer.askedFor("127.0.0.2:19042") },
		snapshotBudget, 10*time.Millisecond, "the pool must dial the new port")
}

// TestASilentPortDoesNotReplaceAKnownEndpoint pins the boundary of the port
// comparison: only a port a source actually named counts as evidence.
//
// A peers table that carries no native_port column - legacy system.peers, which a
// control connection falls back to - or one that carries a NULL in it leaves the
// rebuilt host holding ClusterConfig.Port, a default nothing confirmed. Reading that
// silence as "the port changed to 9042" would remove a node reached on a non-default
// port and rebuild its pool against a port nothing is listening on, on the first
// refresh after a control-connection switch.
func TestASilentPortDoesNotReplaceAKnownEndpoint(t *testing.T) {
	const peerHostID = "ffffffff-0000-4000-8000-00000000beef"

	for _, translator := range []struct {
		name string
		impl AddressTranslator
		addr string
	}{
		{name: "no translator", addr: "127.0.0.2"},
		// A translator that hands both halves straight back, and one that really
		// rewrites the address and only the address. Neither names a port, so neither
		// may turn a defaulted one into evidence - and the second proves that
		// rewriting the address is not what promotes it.
		{name: "identity translator", impl: IdentityTranslator(), addr: "127.0.0.2"},
		{
			name: "address-only translator",
			impl: AddressTranslatorFunc(func(addr net.IP, port int) (net.IP, int) {
				return offsetLastOctet(addr), port
			}),
			addr: "127.0.0.3",
		},
	} {
		t.Run(translator.name, func(t *testing.T) {
			silentPortKeepsTheKnownEndpoint(t, peerHostID, translator.impl, translator.addr)
		})
	}
}

// silentPortKeepsTheKnownEndpoint drives TestASilentPortDoesNotReplaceAKnownEndpoint
// for one AddressTranslator configuration.
func silentPortKeepsTheKnownEndpoint(t *testing.T, peerHostID string, translator AddressTranslator, wantAddr string) {
	t.Helper()

	listener := &recordingTopologyListener{}
	script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
		cluster.Metadata.HostListener.TopologyChangeListener = listener
		cluster.AddressTranslator = translator
	})
	require.Equal(t, 9042, session.cfg.Port, "this test relies on the default cfg.Port")

	peer := newPeerRow(peerHostID, "127.0.0.2")
	peer.nativePort = 19042
	script.setPeers([]peerRow{peer})
	require.NoError(t, session.refreshRing(), "adopt the peer on a non-default port")

	before, ok := session.ring.getHost(peerHostID)
	require.True(t, ok, "the peer must be in the ring")
	require.Equal(t, wantAddr+":19042", before.ConnectAddressAndPort(), "the adopted endpoint")

	for _, tc := range []struct {
		name string
		row  peerRow
	}{
		{name: "the native_port column is absent", row: newPeerRow(peerHostID, "127.0.0.2")},
		{
			name: "the native_port column is NULL",
			row: func() peerRow {
				row := newPeerRow(peerHostID, "127.0.0.2")
				row.nullNativePort = true
				return row
			}(),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, removedBefore := listener.snapshot()

			script.setPeers([]peerRow{tc.row})
			require.NoError(t, session.refreshRing(), "refreshRing")

			after, ok := session.ring.getHost(peerHostID)
			require.True(t, ok, "the peer must still be in the ring")
			require.Same(t, before, after, "a port nothing named must not replace the ring entry")
			require.Equal(t, wantAddr+":19042", after.ConnectAddressAndPort(),
				"the known endpoint must be kept")

			_, removedAfter := listener.snapshot()
			require.Len(t, removedAfter, len(removedBefore), "no host may have been reported removed")
		})
	}
}

// remappingTranslator rewrites every system-table address to a port a test controls.
type remappingTranslator struct{ port atomic.Int64 }

var _ AddressTranslator = (*remappingTranslator)(nil)

func (tr *remappingTranslator) Translate(addr net.IP, _ int) (net.IP, int) {
	return addr, int(tr.port.Load())
}

// TestATranslatedPortChangeStillReplacesTheEndpoint pins the other side of the
// "a port nothing named" rule: a port the AddressTranslator named is named.
//
// A peers row without a native_port column leaves the port defaulted, but the
// translator then rewrites it, and both halves of what it returns are the endpoint
// the driver will dial. If a translator starts mapping a node somewhere else -
// a rebuilt tunnel, a re-published service port - that is a real endpoint change and
// the pool has to follow it, which is exactly what treating the port as unnamed
// would suppress.
func TestATranslatedPortChangeStillReplacesTheEndpoint(t *testing.T) {
	const peerHostID = "abababab-0000-4000-8000-00000000beef"

	translator := &remappingTranslator{}
	translator.port.Store(19042)
	script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
		cluster.AddressTranslator = translator
	})

	// No native_port column at all, so the port can only come from the translator.
	script.setPeers([]peerRow{newPeerRow(peerHostID, "127.0.0.2")})
	require.NoError(t, session.refreshRing(), "adopt the peer")

	before, ok := session.ring.getHost(peerHostID)
	require.True(t, ok, "the peer must be in the ring")
	require.Equal(t, "127.0.0.2:19042", before.ConnectAddressAndPort(), "the translated endpoint")

	translator.port.Store(29042)
	require.NoError(t, session.refreshRing(), "reconcile the remapped port")

	after, ok := session.ring.getHost(peerHostID)
	require.True(t, ok, "the peer must still be in the ring")
	require.NotSame(t, before, after, "a translated port change must replace the ring entry")
	require.Equal(t, "127.0.0.2:29042", after.ConnectAddressAndPort(), "the ring must carry the new port")
}
