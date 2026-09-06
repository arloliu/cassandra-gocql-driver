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
	"testing"

	"github.com/stretchr/testify/require"
)

// unconvertiblePeerRow returns a peer row that newHostInfoFromRow cannot turn into
// a HostInfo: every address it could pick a connect address from is unspecified,
// so the conversion ends at "invalid host address".
//
// The row is otherwise well formed, and in particular carries a readable host_id,
// so a caller can still tell which host it failed to parse.
//
// Parameters:
//   - hostID: the host_id the row reports
//
// Returns:
//   - peerRow: a row that fails conversion but carries identity
func unconvertiblePeerRow(hostID string) peerRow {
	row := newPeerRow(hostID, "0.0.0.0")
	return row
}

// TestRefreshRingSurvivesAnUnconvertiblePeerRow pins M2: a peers row that cannot be
// converted must not take the iterator down with it.
//
// getClusterPeerInfo used to close the iterator on a per-row conversion error and
// then continue reading from it. Close releases the framer and nils it, so the next
// MapScan dereferenced a nil framer and panicked on the ring refresher's flusher,
// which stopped the flusher for the lifetime of the session.
//
// The refresh must instead skip the row, keep reading, adopt the peers that follow
// it, and still run on the next round.
func TestRefreshRingSurvivesAnUnconvertiblePeerRow(t *testing.T) {
	const (
		badHostID  = "11111111-0000-4000-8000-00000000beef"
		goodHostID = "22222222-0000-4000-8000-00000000beef"
	)

	script, _, _, session := startLocalHostFixture(t, "", nil)

	// The unconvertible row comes first, so a refresh that gives up on it never
	// reaches the healthy peer behind it.
	script.setPeers([]peerRow{
		unconvertiblePeerRow(badHostID),
		newPeerRow(goodHostID, "127.0.0.2"),
	})

	require.NoError(t, session.refreshRing(), "the first refresh must report success")

	_, ok := session.ring.getHost(goodHostID)
	require.True(t, ok, "the peer behind the unconvertible row must have been adopted")
	_, ok = session.ring.getHost(badHostID)
	require.False(t, ok, "the unconvertible row must not have produced a ring entry")

	// The flusher has to have survived: a second debounced refresh still runs.
	require.NoError(t, session.refreshRing(), "the second refresh must still run")
	_, ok = session.ring.getHost(goodHostID)
	require.True(t, ok, "the healthy peer must still be in the ring")
}
