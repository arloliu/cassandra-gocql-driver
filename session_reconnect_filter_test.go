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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// flippableHostFilter rejects one host id, once armed.
type flippableHostFilter struct {
	reject atomic.Pointer[string]
}

var _ HostFilter = (*flippableHostFilter)(nil)

func (f *flippableHostFilter) Accept(host *HostInfo) bool {
	rejected := f.reject.Load()
	return rejected == nil || host.HostID() != *rejected
}

// TestReconnectSkipsFilteredHosts pins F-AH-5.
//
// The reconnect tick dialled every host in the ring that was not UP, without asking
// the HostFilter. A filtered host can sit DOWN in the ring - the control connection
// adds its host before filtering, and a DOWN sets the state before the filter is
// consulted - so the driver kept dialling a node the application had explicitly
// excluded, and every attempt was wasted: the pool never admits it.
func TestReconnectSkipsFilteredHosts(t *testing.T) {
	const peerHostID = "dddddddd-0000-4000-8000-00000000beef"

	filter := &flippableHostFilter{}
	dialer := &portRecordingDialer{}
	script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, serverAddress string) {
		dialer.target = serverAddress
		cluster.HostDialer = dialer
		cluster.HostFilter = filter
	})

	script.setPeers([]peerRow{newPeerRow(peerHostID, "127.0.0.2")})
	require.NoError(t, session.refreshRing(), "adopt the peer")

	peer, ok := session.ring.getHost(peerHostID)
	require.True(t, ok, "the peer must be in the ring")

	// A host the filter now rejects, sitting DOWN in the ring with no pool: exactly
	// the state the reconnect tick exists to act on.
	session.pool.removeHost(peer)
	peer.setState(NodeDown)
	rejected := peerHostID
	filter.reject.Store(&rejected)

	// The sweep registers the pool synchronously before filling it,
	// so the answer is settled by the time the call returns.
	dialer.reset()
	sweepDownedHostsOnce(session)
	_, ok = session.pool.getPoolByHostID(peerHostID)
	require.False(t, ok, "a filtered host must not be admitted to the pool by the reconnect tick")
	require.False(t, dialer.askedFor(peer.ConnectAddressAndPort()),
		"a filtered host must not be dialled by the reconnect tick")

	// A host the filter accepts is still acted on, so the skip is the filter's doing
	// and not a reconnect that stopped working.
	filter.reject.Store(nil)
	sweepDownedHostsOnce(session)
	_, ok = session.pool.getPoolByHostID(peerHostID)
	require.True(t, ok, "an accepted host must still be admitted by the reconnect tick")
}
