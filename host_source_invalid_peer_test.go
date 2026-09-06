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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	invalidPeerA = "aaaaaaaa-0000-4000-8000-00000000beef"
	invalidPeerB = "bbbbbbbb-0000-4000-8000-00000000beef"
)

// identifiableInvalidRow returns a row that converts into a HostInfo cleanly and is
// then rejected by isValidPeer, so its host_id is readable.
func identifiableInvalidRow(hostID, addr string) peerRow {
	row := newPeerRow(hostID, addr)
	row.nullRack = true
	return row
}

// addressOnlyInvalidRow returns a row whose host_id cannot be parsed, so the only
// identity it carries is its address.
func addressOnlyInvalidRow(addr string) peerRow {
	row := newPeerRow("", addr)
	return row
}

// requireRinged asserts the host is in the ring and still owns a registered pool.
//
// Asserting IsUp alone would prove nothing: an ordinary removal leaves the departed
// object's own IsUp reading true, so that assertion holds whether or not the host
// was evicted.
func requireRinged(t *testing.T, session *Session, hostID, msg string) {
	t.Helper()
	_, ok := session.ring.getHost(hostID)
	require.True(t, ok, "%s: host must still be in the ring", msg)
	_, ok = session.pool.getPoolByHostID(hostID)
	require.True(t, ok, "%s: host must still own a registered pool", msg)
}

func requireNotRinged(t *testing.T, session *Session, hostID, msg string) {
	t.Helper()
	_, ok := session.ring.getHost(hostID)
	require.False(t, ok, "%s: host must have been removed from the ring", msg)
}

// TestATransientlyInvalidPeerRowDoesNotEvictAHealthyHost pins F-AH-2.
//
// A peers row that fails isValidPeer - a null rack from a snitch that has not caught
// up, an empty token set on a node that is still joining - was dropped with a
// warning. The host then had no row in the snapshot at all, so the sweep at the end
// of refreshRing removed it, closed its pool and told the policy it was gone. On a
// healthy, quiet ring nothing looks at the peers table again on its own, so the node
// stayed evicted until some unrelated event happened to trigger another refresh.
//
// A single bad observation is now not enough. It takes invalidPeerRowGrace of them,
// with no valid one in between.
func TestATransientlyInvalidPeerRowDoesNotEvictAHealthyHost(t *testing.T) {
	script, _, _, session := startLocalHostFixture(t, "", nil)

	good := newPeerRow(invalidPeerA, "127.0.0.2")
	script.setPeers([]peerRow{good})
	require.NoError(t, session.refreshRing(), "adopt the peer")
	requireRinged(t, session, invalidPeerA, "after adoption")

	// One bad round is not evidence the node left.
	script.setPeers([]peerRow{identifiableInvalidRow(invalidPeerA, "127.0.0.2")})
	for round := 1; round < invalidPeerRowGrace; round++ {
		require.NoError(t, session.refreshRing(), "round %d", round)
		requireRinged(t, session, invalidPeerA, "while within the grace")
	}

	// The last one uses it up.
	require.NoError(t, session.refreshRing(), "the round that uses up the grace")
	requireNotRinged(t, session, invalidPeerA, "after the grace ran out")
}

// TestOneValidObservationResetsTheInvalidRowCount pins that the count is of
// consecutive bad observations, and that it is kept per host identity rather than
// per whatever identity a given row happened to carry.
//
// The sequence is deliberately invalid-by-address, invalid-by-address, valid-by-id,
// invalid-by-address. A count kept under two keys - one for the address, one for the
// host id - passes anything shorter: the valid round clears the id key while the
// address key still holds two, and the fourth round then evicts a host that was
// healthy one round ago.
func TestOneValidObservationResetsTheInvalidRowCount(t *testing.T) {
	script, _, _, session := startLocalHostFixture(t, "", nil)

	good := newPeerRow(invalidPeerA, "127.0.0.2")
	script.setPeers([]peerRow{good})
	require.NoError(t, session.refreshRing(), "adopt the peer")

	script.setPeers([]peerRow{addressOnlyInvalidRow("127.0.0.2")})
	require.NoError(t, session.refreshRing(), "first invalid round")
	require.NoError(t, session.refreshRing(), "second invalid round")
	requireRinged(t, session, invalidPeerA, "two invalid rounds")

	script.setPeers([]peerRow{good})
	require.NoError(t, session.refreshRing(), "a valid round")
	requireRinged(t, session, invalidPeerA, "after the valid round")

	script.setPeers([]peerRow{addressOnlyInvalidRow("127.0.0.2")})
	require.NoError(t, session.refreshRing(), "one invalid round after the valid one")
	requireRinged(t, session, invalidPeerA,
		"the valid round must have reset the count, whichever identity the bad rows carried")
}

// TestAValidRowWinsOverAnInvalidOneForTheSameHost pins the duplicate rule.
//
// The duplicate host_id check only ever sees rows that parsed and passed
// isValidPeer, so a valid row and an invalid row for one host reach the accepted
// snapshot together. The valid one is the stronger evidence.
func TestAValidRowWinsOverAnInvalidOneForTheSameHost(t *testing.T) {
	script, _, _, session := startLocalHostFixture(t, "", nil)

	good := newPeerRow(invalidPeerA, "127.0.0.2")
	script.setPeers([]peerRow{good})
	require.NoError(t, session.refreshRing(), "adopt the peer")

	// A mixed round must leave the count at zero, not at one. The difference only
	// shows once the host stops being described validly: with the mixed round counted,
	// the grace is one round shorter than it should be and the host is evicted while
	// it was still healthy a round ago.
	script.setPeers([]peerRow{good, identifiableInvalidRow(invalidPeerA, "127.0.0.2")})
	require.NoError(t, session.refreshRing(), "the mixed round")
	requireRinged(t, session, invalidPeerA, "a valid row for the same host must win")

	script.setPeers([]peerRow{identifiableInvalidRow(invalidPeerA, "127.0.0.2")})
	for round := 1; round < invalidPeerRowGrace; round++ {
		require.NoError(t, session.refreshRing(), "invalid round %d", round)
		requireRinged(t, session, invalidPeerA,
			"the mixed round must not have spent any of the grace")
	}
}

// TestARejectedSnapshotDoesNotCountAsAnObservation pins that the observation unit is
// an accepted snapshot.
//
// A snapshot listing one host_id twice is rejected outright: refreshRing never runs
// on it, and it carries no trustworthy membership information at all. Letting it
// increment the count would let a duplicate-emitting node spend another host's
// grace.
func TestARejectedSnapshotDoesNotCountAsAnObservation(t *testing.T) {
	script, _, _, session := startLocalHostFixture(t, "", nil)

	good := newPeerRow(invalidPeerA, "127.0.0.2")
	other := newPeerRow(invalidPeerB, "127.0.0.3")
	script.setPeers([]peerRow{good, other})
	require.NoError(t, session.refreshRing(), "adopt both peers")

	// Rejected snapshots that also carry an invalid row for A.
	duplicate := newPeerRow(invalidPeerB, "127.0.0.4")
	rejected := []peerRow{identifiableInvalidRow(invalidPeerA, "127.0.0.2"), other, duplicate}
	script.setPeers(rejected)
	for round := 0; round < invalidPeerRowGrace+2; round++ {
		require.Error(t, session.refreshRing(), "a duplicated host_id must reject the snapshot")
	}
	requireRinged(t, session, invalidPeerA, "rejected snapshots must not spend the grace")

	// And the grace is still whole: it takes the full count of accepted rounds.
	script.setPeers([]peerRow{identifiableInvalidRow(invalidPeerA, "127.0.0.2"), other})
	for round := 1; round < invalidPeerRowGrace; round++ {
		require.NoError(t, session.refreshRing(), "accepted round %d", round)
		requireRinged(t, session, invalidPeerA, "within the grace")
	}
	require.NoError(t, session.refreshRing(), "the round that uses up the grace")
	requireNotRinged(t, session, invalidPeerA, "after the grace ran out")
}

// TestAnUnattributableRowSuspendsTheAbsenceSweepOnly pins the conservative half of
// the rule, and its limit.
//
// An invalid row carrying neither a usable host_id nor an address that resolves to
// exactly one ring member tells us that something is wrong and nothing about which
// host it is wrong for. The count cannot be spent on anybody, but that alone would
// not protect the host in question: it is absent from the valid-host list, so the
// ordinary sweep would remove it anyway.
//
// So a snapshot carrying such a row is treated as incomplete membership information:
// the absence sweep is skipped for that round. Only that sweep. A valid peer whose
// endpoint changed is still reconciled in the same round, because that has full
// identity evidence, and suppressing it would undo the port-change fix.
func TestAnUnattributableRowSuspendsTheAbsenceSweepOnly(t *testing.T) {
	const movingPeer = "cccccccc-0000-4000-8000-00000000beef"

	listener := &recordingTopologyListener{}
	dialer := &portRecordingDialer{}
	script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, serverAddress string) {
		dialer.target = serverAddress
		cluster.HostDialer = dialer
		cluster.Metadata.HostListener.TopologyChangeListener = listener
	})

	// Every scripted row names a port, because the fixture serves that column for the
	// whole batch or for none of it.
	departing := newPeerRow(invalidPeerA, "127.0.0.2")
	departing.nativePort = 9042
	moving := newPeerRow(movingPeer, "127.0.0.3")
	moving.nativePort = 9042
	script.setPeers([]peerRow{departing, moving})
	require.NoError(t, session.refreshRing(), "adopt both peers")
	requireRinged(t, session, invalidPeerA, "after adoption")

	// One snapshot that: drops A entirely, carries a row nobody can be blamed for,
	// and moves the other peer's port.
	moving.nativePort = 19042
	orphan := addressOnlyInvalidRow("127.0.0.9") // an address no ring member holds
	orphan.nativePort = 9042
	script.setPeers([]peerRow{orphan, moving})
	require.NoError(t, session.refreshRing(), "the incomplete round must still succeed")

	requireRinged(t, session, invalidPeerA,
		"an absent host must be kept when the snapshot's membership is incomplete")

	after, ok := session.ring.getHost(movingPeer)
	require.True(t, ok, "the moving peer must still be in the ring")
	require.Equal(t, "127.0.0.3:19042", after.ConnectAddressAndPort(),
		"a valid peer's endpoint change must still be reconciled in an incomplete round")

	// Once membership is complete again, the absent host is removed as usual.
	script.setPeers([]peerRow{moving})
	require.NoError(t, session.refreshRing(), "a complete round")
	requireNotRinged(t, session, invalidPeerA, "a complete snapshot removes the absent host")
}

// TestAnInvalidRowForAnUnknownHostDoesNotSuspendTheSweep pins the boundary of the
// rule above.
//
// A row that fails isValidPeer while carrying a perfectly readable host_id is
// attributable - there is simply no ring entry to protect. A node that is still
// joining produces exactly that: its token set is empty, so isValidPeer rejects it,
// and it is not in the ring yet.
//
// Membership is still fully accounted for in such a round, so the sweep must run.
// Treating "not a ring member" as "unattributable" would keep every departed member
// in the ring for the whole duration of an unrelated node's bootstrap.
func TestAnInvalidRowForAnUnknownHostDoesNotSuspendTheSweep(t *testing.T) {
	const joiningHost = "eeeeeeee-0000-4000-8000-00000000beef"

	script, _, _, session := startLocalHostFixture(t, "", nil)

	departing := newPeerRow(invalidPeerA, "127.0.0.2")
	script.setPeers([]peerRow{departing})
	require.NoError(t, session.refreshRing(), "adopt the peer")
	requireRinged(t, session, invalidPeerA, "after adoption")

	// A snapshot that drops the known peer and describes a host the ring has never
	// seen with a row that cannot be accepted.
	script.setPeers([]peerRow{identifiableInvalidRow(joiningHost, "127.0.0.3")})
	require.NoError(t, session.refreshRing(), "the round must succeed")

	requireNotRinged(t, session, invalidPeerA,
		"an invalid row naming a host the ring does not hold must not suspend the sweep")
	requireNotRinged(t, session, joiningHost, "the host with the invalid row must not have been adopted")
}

// TestANullHostIDFallsBackToTheAddress pins that a NULL host_id is not an identity.
//
// host_id is a uuid column, and a NULL uuid scans as the zero UUID, whose rendered
// form is a perfectly well-formed identity string that nothing downstream would
// question. Taken at face value it names a host the ring has never held, so an
// invalid row carrying one is attributed to that phantom - and the real host the row
// was about, absent from the valid list, is swept on the very first bad snapshot.
//
// The row must instead fall back to its address, which does resolve to the host it
// is really about.
func TestANullHostIDFallsBackToTheAddress(t *testing.T) {
	script, _, _, session := startLocalHostFixture(t, "", nil)

	good := newPeerRow(invalidPeerA, "127.0.0.2")
	script.setPeers([]peerRow{good})
	require.NoError(t, session.refreshRing(), "adopt the peer")
	requireRinged(t, session, invalidPeerA, "after adoption")

	nullID := newPeerRow(invalidPeerA, "127.0.0.2")
	nullID.nullHostID = true
	nullID.nullRack = true
	script.setPeers([]peerRow{nullID})

	for round := 1; round < invalidPeerRowGrace; round++ {
		require.NoError(t, session.refreshRing(), "round %d", round)
		requireRinged(t, session, invalidPeerA,
			"a row with a NULL host_id must be charged to the host its address names")
	}
	require.NoError(t, session.refreshRing(), "the round that uses up the grace")
	requireNotRinged(t, session, invalidPeerA, "after the grace ran out")
}

// TestAnAmbiguousAddressSuspendsTheAbsenceSweep pins the other half of "attributable".
//
// Two ring members can share one node-to-node address, and the ring's address index
// cannot say so: it holds one host id per address. An invalid row carrying only such
// an address names two hosts, which is to say it names neither. Picking one of them
// would spend a healthy host's grace on evidence about some other host.
func TestAnAmbiguousAddressSuspendsTheAbsenceSweep(t *testing.T) {
	const shared = "127.0.0.5"

	script, _, _, session := startLocalHostFixture(t, "", nil)

	// Two peers reachable at different addresses that broadcast the same one. peer is
	// the node-to-node address a peers row reports, so both are indexed by it.
	first := newPeerRow(invalidPeerA, "127.0.0.2")
	first.peer = shared
	second := newPeerRow(invalidPeerB, "127.0.0.3")
	second.peer = shared
	script.setPeers([]peerRow{first, second})
	require.NoError(t, session.refreshRing(), "adopt both peers")
	requireRinged(t, session, invalidPeerA, "after adoption")
	requireRinged(t, session, invalidPeerB, "after adoption")

	// A snapshot that drops both and carries one invalid row naming only the shared
	// address. Neither host may be evicted on it.
	orphan := newPeerRow("", "127.0.0.4")
	orphan.peer = shared
	script.setPeers([]peerRow{orphan})
	require.NoError(t, session.refreshRing(), "the ambiguous round must succeed")

	requireRinged(t, session, invalidPeerA, "an ambiguous address must not spend anyone's grace")
	requireRinged(t, session, invalidPeerB, "an ambiguous address must not spend anyone's grace")
}

// TestRepeatedInvalidRowsForOneHostCountOnce pins that the unit of observation is a
// snapshot, not a row: a node emitting several bad rows for one host must not spend
// its whole grace in a single round.
func TestRepeatedInvalidRowsForOneHostCountOnce(t *testing.T) {
	script, _, _, session := startLocalHostFixture(t, "", nil)

	good := newPeerRow(invalidPeerA, "127.0.0.2")
	script.setPeers([]peerRow{good})
	require.NoError(t, session.refreshRing(), "adopt the peer")

	bad := identifiableInvalidRow(invalidPeerA, "127.0.0.2")
	repeated := make([]peerRow, invalidPeerRowGrace+2)
	for i := range repeated {
		repeated[i] = bad
	}
	script.setPeers(repeated)

	require.NoError(t, session.refreshRing(), "one round of many bad rows")
	requireRinged(t, session, invalidPeerA, "many rows in one snapshot are still one observation")
}

// TestAHostReadmittedBetweenSnapshotsStartsFromZero pins the count's lifetime.
//
// A host that used up its grace is removed, and a control connection setting itself
// up re-adds its own host directly, without a snapshot in between. The new ring entry
// is a different object with the same id, and it must not inherit the old count -
// otherwise its very first bad row evicts it again.
func TestAHostReadmittedBetweenSnapshotsStartsFromZero(t *testing.T) {
	script, _, _, session := startLocalHostFixture(t, "", nil)

	good := newPeerRow(invalidPeerA, "127.0.0.2")
	script.setPeers([]peerRow{good})
	require.NoError(t, session.refreshRing(), "adopt the peer")

	script.setPeers([]peerRow{identifiableInvalidRow(invalidPeerA, "127.0.0.2")})
	for round := 0; round < invalidPeerRowGrace; round++ {
		require.NoError(t, session.refreshRing(), "round %d", round)
	}
	requireNotRinged(t, session, invalidPeerA, "the grace must have run out")

	// Re-admitted outside a snapshot, as controlConn.setupConn does for its own host.
	readmitted, err := NewTestHostInfoFromRow(map[string]interface{}{
		"peer":            "127.0.0.2",
		"rpc_address":     "127.0.0.2",
		"data_center":     "dc1",
		"rack":            "rack1",
		"host_id":         invalidPeerA,
		"release_version": "3.11.0",
		"tokens":          []string{"-9223372036854775808"},
	})
	require.NoError(t, err, "build the re-admitted host")
	_, existed := session.ring.addHostIfMissing(readmitted)
	require.False(t, existed, "the host must have been re-admitted as a new entry")

	// Its first bad round must not evict it.
	require.NoError(t, session.refreshRing(), "the first round after re-admission")
	_, ok := session.ring.getHost(invalidPeerA)
	require.True(t, ok, "a re-admitted host must not inherit the old count")
}

// armableBadTranslator returns an unusable address once armed, which makes the
// conversion of a system-table row fail outright.
type armableBadTranslator struct{ armed atomic.Bool }

var _ AddressTranslator = (*armableBadTranslator)(nil)

func (tr *armableBadTranslator) Translate(addr net.IP, port int) (net.IP, int) {
	if tr.armed.Load() {
		return nil, port
	}
	return addr, port
}

// TestANullHostIDOnAnUnconvertibleRowFallsBackToTheAddress covers the same NULL
// host_id on the other producer of an invalid row.
//
// A row that never becomes a HostInfo at all has its identity dug out of the row map
// rather than read off an object, and that path has its own reading of a NULL uuid.
// Taking the zero UUID for an identity there attributes the row to a phantom exactly
// as it did on the other path, and the host the row was really about - absent from
// the valid list - is swept on the first bad snapshot.
//
// The conversion is made to fail the way it does in practice: an AddressTranslator
// that returns an address the driver cannot use. The control host is never
// translated, so only the peer row is affected.
func TestANullHostIDOnAnUnconvertibleRowFallsBackToTheAddress(t *testing.T) {
	translator := &armableBadTranslator{}
	script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
		cluster.AddressTranslator = translator
	})

	// peer is the address the ring indexes this host by, and it stays readable in the
	// row even when the conversion fails.
	good := newPeerRow(invalidPeerA, "127.0.0.2")
	script.setPeers([]peerRow{good})
	require.NoError(t, session.refreshRing(), "adopt the peer")
	requireRinged(t, session, invalidPeerA, "after adoption")

	unconvertible := newPeerRow(invalidPeerA, "127.0.0.2")
	unconvertible.nullHostID = true
	script.setPeers([]peerRow{unconvertible})
	translator.armed.Store(true)

	for round := 1; round < invalidPeerRowGrace; round++ {
		require.NoError(t, session.refreshRing(), "round %d", round)
		requireRinged(t, session, invalidPeerA,
			"an unconvertible row with a NULL host_id must be charged to the host its address names")
	}
	require.NoError(t, session.refreshRing(), "the round that uses up the grace")
	requireNotRinged(t, session, invalidPeerA, "after the grace ran out")
}

// TestAnIncompleteSnapshotAlsoSuspendsTheGraceEviction pins that an incomplete
// snapshot suspends the whole sweep, not only the removal of absent hosts.
//
// A host one observation short of its grace, described badly again in a round whose
// membership could not be accounted for, must survive. That round is not evidence
// about anybody: the row nobody could be blamed for might have been about this very
// host, in which case the earlier observations were charged to it wrongly.
func TestAnIncompleteSnapshotAlsoSuspendsTheGraceEviction(t *testing.T) {
	script, _, _, session := startLocalHostFixture(t, "", nil)

	good := newPeerRow(invalidPeerA, "127.0.0.2")
	script.setPeers([]peerRow{good})
	require.NoError(t, session.refreshRing(), "adopt the peer")

	// Spend all but the last of the grace.
	script.setPeers([]peerRow{identifiableInvalidRow(invalidPeerA, "127.0.0.2")})
	for round := 1; round < invalidPeerRowGrace; round++ {
		require.NoError(t, session.refreshRing(), "round %d", round)
		requireRinged(t, session, invalidPeerA, "within the grace")
	}

	// The round that would have used it up, in a snapshot carrying a row that names
	// nobody.
	orphan := newPeerRow("", "127.0.0.9")
	orphan.peer = "127.0.0.9"
	script.setPeers([]peerRow{identifiableInvalidRow(invalidPeerA, "127.0.0.2"), orphan})
	require.NoError(t, session.refreshRing(), "the incomplete round must succeed")
	requireRinged(t, session, invalidPeerA,
		"an incomplete snapshot must not evict a host whose grace it just used up")

	// A complete round settles it.
	script.setPeers([]peerRow{identifiableInvalidRow(invalidPeerA, "127.0.0.2")})
	require.NoError(t, session.refreshRing(), "a complete round")
	requireNotRinged(t, session, invalidPeerA, "a complete snapshot evicts once the grace is gone")
}

// TestAnUnusableAddressIsNotAnIdentity pins that the unspecified address cannot be
// used to attribute an invalid row.
//
// HostInfo.nodeToNodeAddress falls back to 0.0.0.0 when neither of its columns is
// usable, and a node can be perfectly reachable while reporting one - its rpc_address
// carries it. Indexing such a host under that fallback makes it the answer for every
// invalid row that also has no usable address, so a bad row about host A is charged
// to host B, membership reads as complete, and A is swept on the first bad snapshot
// with its whole grace unspent.
//
// Both producers of an invalid row have to agree on this, so both shapes are driven:
// a row that never converts, and one that converts and is then rejected.
func TestAnUnusableAddressIsNotAnIdentity(t *testing.T) {
	for _, tc := range []struct {
		name       string
		translator AddressTranslator
	}{
		{name: "the row is rejected after converting"},
		{name: "the row never converts", translator: &armableBadTranslator{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad, _ := tc.translator.(*armableBadTranslator)
			script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
				if tc.translator != nil {
					cluster.AddressTranslator = tc.translator
				}
			})

			// B is reachable through its rpc_address but reports no usable
			// node-to-node address, so the ring holds it under the fallback.
			reachable := newPeerRow(invalidPeerB, "127.0.0.3")
			reachable.peer = "0.0.0.0"
			departing := newPeerRow(invalidPeerA, "127.0.0.2")
			script.setPeers([]peerRow{departing, reachable})
			require.NoError(t, session.refreshRing(), "adopt both peers")
			requireRinged(t, session, invalidPeerA, "after adoption")
			requireRinged(t, session, invalidPeerB, "after adoption")

			// A snapshot that drops A and carries one invalid row with neither a usable
			// host_id nor a usable address. It names nobody, so nothing may be evicted.
			orphan := newPeerRow("", "127.0.0.4")
			orphan.peer = "0.0.0.0"
			orphan.nullHostID = true
			script.setPeers([]peerRow{orphan, reachable})
			if bad != nil {
				bad.armed.Store(true)
			}
			require.NoError(t, session.refreshRing(), "the round must succeed")

			requireRinged(t, session, invalidPeerA,
				"a row with no usable address must not be charged to a host that merely lacks one too")
			requireRinged(t, session, invalidPeerB, "the host it was wrongly charged to must be untouched")
		})
	}
}
