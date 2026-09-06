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
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNativePortLosesToACallerSuppliedEndpoint pins the port half of the endpoint
// provenance rule.
//
// newHostInfoFromRow takes an address and a port from its caller. When that address
// is valid the pair is the logical dial target - the endpoint the driver is already
// connected through, and the one a custom HostDialer is handed next time - so the
// row cannot override either half of it. When it is not, the row is the only source
// of both, and native_port must be taken.
//
// The predicate is the provenance of the endpoint, not "the caller passed a port":
// peer rows are read with a nil address and ClusterConfig.Port as a mere default, so
// keying on a non-zero port would silently disable the port correction for peers,
// which is the whole point of reading native_port at all.
func TestNativePortLosesToACallerSuppliedEndpoint(t *testing.T) {
	row := func() map[string]interface{} {
		return map[string]interface{}{
			"rpc_address":     "10.0.0.2",
			"data_center":     "dc",
			"rack":            "rack",
			"host_id":         "7ce72fcc-0000-4000-8000-00000000babe",
			"tokens":          []string{"0"},
			"native_port":     19042,
			"release_version": "3.11.0",
		}
	}
	session := &Session{logger: &defaultLogger{}}

	peer, err := newHostInfoFromRow(session, nil, 9042, row())
	require.NoError(t, err, "build a peer from the row")
	require.Equal(t, 19042, peer.Port(),
		"a peer's endpoint comes from the row, so native_port must win over the default port")

	local, err := newHostInfoFromRow(session, net.ParseIP("10.0.0.9"), 9042, row())
	require.NoError(t, err, "build the control host from the row")
	require.Equal(t, 9042, local.Port(),
		"a caller-supplied endpoint is the dial target, so native_port must not override its port")
	require.Equal(t, "10.0.0.9", local.ConnectAddress().String(),
		"the caller-supplied address must be kept, as it already was")

	// A malformed native_port is still rejected on the path that ignores its value,
	// so the two paths agree on what a well-formed row is.
	bad := row()
	bad["native_port"] = "19042"
	_, err = newHostInfoFromRow(session, net.ParseIP("10.0.0.9"), 9042, bad)
	require.Error(t, err, "a native_port of the wrong type must be rejected even when it is not used")
}

// TestControlHostKeepsItsDialledPortAgainstNativePort pins the same rule end to end,
// on the object the driver actually dials with.
//
// The control connection's logical port and system.local's native_port can disagree:
// a HostDialer that redirects, or an AddressTranslator that offsets the port, makes
// the endpoint the driver dialled something the node does not report. Before the
// rule was explicit this was held only by a side effect - refreshRing saw no
// endpoint change and HostInfo.update keeps a non-zero port - which stops holding
// the moment refreshRing starts comparing ports.
func TestControlHostKeepsItsDialledPortAgainstNativePort(t *testing.T) {
	const dialTargetPort = 19042

	script, _, serverPort, session := startLocalHostFixture(t,
		net.JoinHostPort("127.0.0.1", strconv.Itoa(dialTargetPort)), nil)
	require.NotEqual(t, dialTargetPort, serverPort, "the fixture must not land on the dial target port")

	before, ok := session.ring.getHost(script.hostID)
	require.True(t, ok, "the control host must be in the ring after session init")
	require.Equal(t, dialTargetPort, before.Port(), "session init records the port the dial was made with")

	// The node now reports a port that contradicts the endpoint the driver dialled.
	script.setLocalNativePort(9042)
	require.NoError(t, session.refreshRing(), "refreshRing")

	after, ok := session.ring.getHost(script.hostID)
	require.True(t, ok, "the control host must still be in the ring")
	require.Same(t, before, after, "a native_port that only contradicts the dial target must not replace the entry")
	require.Equal(t, dialTargetPort, after.Port(), "the control host must keep the port it is dialled on")

	// And the same holds when the entry really is replaced, which is the only path a
	// rebuilt port reaches the ring on.
	script.setBroadcastAddress("127.0.0.4")
	require.NoError(t, session.refreshRing(), "refreshRing after the address change")

	replacement, ok := session.ring.getHost(script.hostID)
	require.True(t, ok, "the control host must still be in the ring after the replacement")
	require.NotSame(t, after, replacement, "the address change must have replaced the entry")
	require.Equal(t, dialTargetPort, replacement.Port(),
		"the replacement must keep the dialled port, not adopt native_port")
}
