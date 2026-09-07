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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

const admissionPeerHostID = "44444444-0000-4000-8000-00000000beef"

// publishedRecord reads the publication ledger entry for a host ID.
func publishedRecord(s *Session, hostID string) *HostInfo {
	s.hostPublishMu.Lock()
	defer s.hostPublishMu.Unlock()
	return s.publishedHosts[hostID]
}

// TestRefreshRing_CompletesAdmissionForAnEntryItDidNotAdd pins the F-AH-8 repair.
//
// controlConn.setupConn inserts its host into the ring before it registers events,
// and that registration can fail.
// The entry it leaves behind defaults to UP, so the reconnect sweep skips it and no
// later refresh ever calls startPoolFill for it: it stayed in the ring with no pool
// and no policy publication for the rest of the session.
//
// This simulates that entry directly - an object in the ring that was never admitted
// - and asserts an ordinary refresh that sees no endpoint change repairs it.
func TestRefreshRing_CompletesAdmissionForAnEntryItDidNotAdd(t *testing.T) {
	script, _, _, session := startLocalHostFixture(t, "", nil)

	// A peer the driver knows about, admitted the ordinary way.
	script.setPeers([]peerRow{newPeerRow(admissionPeerHostID, "127.0.0.2")})
	require.NoError(t, session.refreshRing(), "the first refresh admits the peer")

	peer, ok := session.ring.getHost(admissionPeerHostID)
	require.True(t, ok, "the peer is in the ring")
	require.Same(t, peer, publishedRecord(session, admissionPeerHostID), "the peer was published")

	// Undo the admission behind the refresh's back, leaving the ring entry alone.
	// This is the state a failed setupConn leaves: in the ring, UP, unadmitted.
	session.pool.removeHost(peer)
	session.hostPublishMu.Lock()
	delete(session.publishedHosts, admissionPeerHostID)
	session.hostPublishMu.Unlock()
	_, pooled := session.pool.getPoolFor(peer)
	require.False(t, pooled, "the peer now has no pool")

	// An unchanged refresh: same endpoint, so the loop only calls HostInfo.update.
	require.NoError(t, session.refreshRing(), "the unchanged refresh must run")

	same, ok := session.ring.getHost(admissionPeerHostID)
	require.True(t, ok, "the peer is still in the ring")
	require.Same(t, peer, same, "the unchanged refresh must not replace the ring object")

	_, pooled = session.pool.getPoolFor(peer)
	require.True(t, pooled, "the unchanged refresh must register the missing pool")
	require.Same(t, peer, publishedRecord(session, admissionPeerHostID),
		"the unchanged refresh must make the missing policy publication")
}

// TestRefreshRing_DoesNotRepublishOnEveryRound: the repair is idempotent.
//
// completeAdmission runs for every host on every round, so the publication ledger is
// what stops a healthy ring from announcing all of its hosts to the policy again on
// each one.
//
// The assertion has to count policy.AddHost, not OnNewHost.
// OnNewHost is gated a second time by the branch that brought the host into the
// ring, so it stays flat on unchanged rounds whether the ledger works or not: a test
// watching it would pass with the ledger removed entirely.
// Republished AddHost calls are not harmless either - tokenAwareHostPolicy.AddHost
// rebuilds the token ring and every replica map when the host is new to its list.
func TestRefreshRing_DoesNotRepublishOnEveryRound(t *testing.T) {
	var newHosts atomic.Int64
	policy := &addHostCounter{HostSelectionPolicy: RoundRobinHostPolicy()}
	script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
		cluster.PoolConfig.HostSelectionPolicy = policy
		cluster.Metadata.HostListener.TopologyChangeListener = topologyCounter{onNew: func() { newHosts.Add(1) }}
	})

	script.setPeers([]peerRow{newPeerRow(admissionPeerHostID, "127.0.0.2")})
	require.NoError(t, session.refreshRing(), "the first refresh admits the peer")

	firstAdds := policy.count(admissionPeerHostID)
	require.Equal(t, int64(1), firstAdds, "the first refresh publishes the new peer exactly once")
	firstNew := newHosts.Load()
	require.Positive(t, firstNew, "the first refresh announces the new peer")

	for i := 0; i < 3; i++ {
		require.NoError(t, session.refreshRing(), "an unchanged refresh must run")
	}
	require.Equal(t, firstAdds, policy.count(admissionPeerHostID),
		"unchanged refreshes must not publish a host that is already published")
	require.Equal(t, firstNew, newHosts.Load(),
		"unchanged refreshes must not announce a host that is already admitted")
}

// TestPublicationLedger_RecordsEverySite: all three publication sites record.
//
// completeAdmission publishes any ring object the ledger does not already name, so
// a site that publishes without recording makes the first refresh publish that host
// a second time.
// Session.init's bulk publish and startPoolFill are the other two sites, and neither
// is exercised by the refresh-driven tests above: init publishes the contact point
// before any refresh runs, and startPoolFill is the UP-event and control-recovery
// path.
func TestPublicationLedger_RecordsEverySite(t *testing.T) {
	policy := &addHostCounter{HostSelectionPolicy: RoundRobinHostPolicy()}
	script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
		cluster.PoolConfig.HostSelectionPolicy = policy
	})

	t.Run("Session.init", func(t *testing.T) {
		local, ok := session.ring.getHost(script.localHostID())
		require.True(t, ok, "the control host is in the ring")
		require.Same(t, local, publishedRecord(session, script.localHostID()),
			"init must record the host it published")

		before := policy.count(script.localHostID())
		require.NoError(t, session.refreshRing(), "a refresh must run")
		require.Equal(t, before, policy.count(script.localHostID()),
			"the first refresh must not republish a host init already published")
	})

	t.Run("startPoolFill", func(t *testing.T) {
		script.setPeers([]peerRow{newPeerRow(admissionPeerHostID, "127.0.0.2")})
		require.NoError(t, session.refreshRing(), "the refresh admits the peer")
		peer, ok := session.ring.getHost(admissionPeerHostID)
		require.True(t, ok, "the peer is in the ring")

		// Retire the record without touching the pool, then drive the UP-event path.
		session.hostPublishMu.Lock()
		delete(session.publishedHosts, admissionPeerHostID)
		session.hostPublishMu.Unlock()

		before := policy.count(admissionPeerHostID)
		session.startPoolFill(peer)
		require.Equal(t, before+1, policy.count(admissionPeerHostID), "startPoolFill publishes")
		require.Same(t, peer, publishedRecord(session, admissionPeerHostID),
			"startPoolFill must record the host it published")

		require.NoError(t, session.refreshRing(), "a later refresh must run")
		require.Equal(t, before+1, policy.count(admissionPeerHostID),
			"a refresh must not republish a host startPoolFill already published")
	})
}

// TestPublicationLedger_RetirementIsPointerSensitive: a replacement under the same
// host ID does not have its record dropped by the removal of its predecessor.
//
// removeHost takes the ring entry out and un-publishes in one hostPublishMu
// transaction, but the entry is keyed by host ID and carries no pointer check, so a
// replacement can already hold the ID by the time the un-publication runs.
// Deleting the record by key alone would retire the replacement's publication, and
// the next refresh would announce it to the policy a second time.
func TestPublicationLedger_RetirementIsPointerSensitive(t *testing.T) {
	f := newOwnershipFixture(t)

	stale := f.host
	require.Same(t, stale, publishedRecord(f.session, stale.HostID()), "the fixture host is published")

	b := f.replacement(t)
	require.True(t, f.session.completeAdmission(b), "the replacement is published by the repair")
	require.Same(t, b, publishedRecord(f.session, b.HostID()), "the replacement holds the record")

	// The stale object's removal must not take the replacement's record with it.
	// The helper runs inside removeHost's critical section, so the test holds the
	// same mutex the caller would.
	func() {
		f.session.hostPublishMu.Lock()
		defer f.session.hostPublishMu.Unlock()
		f.session.unpublishHostLocked(stale)
	}()
	require.Same(t, b, publishedRecord(f.session, b.HostID()),
		"retiring a superseded object must leave the replacement's record alone")
}

// TestRefreshRing_AdoptsAnEntryInsertedAfterTheSnapshot forces the first of the two
// setupConn collisions: the ring gains this host ID after prevHosts was captured.
//
// refreshRing used to abandon the round with ErrCannotFindHost here.
// That left every host later in the snapshot unreconciled, and left this entry - which
// setupConn inserts before it registers events, a registration that can fail - with no
// pool and no policy publication.
// The round must now adopt the ring's object, complete its admission, and carry on.
func TestRefreshRing_AdoptsAnEntryInsertedAfterTheSnapshot(t *testing.T) {
	var inserted atomic.Pointer[HostInfo]
	var arm atomic.Bool
	var session *Session

	script, _, _, s := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
		cluster.testAfterPrevHostsSnapshot = func() {
			if !arm.CompareAndSwap(true, false) {
				return
			}
			// Stand in for setupConn: insert the peer straight into the ring, with
			// no pool and no policy publication, after prevHosts was taken.
			h, err := NewHostInfoFromAddrPort(net.ParseIP("127.0.0.2"), 9042)
			require.NoError(t, err, "NewHostInfoFromAddrPort")
			h.setHostID(admissionPeerHostID)
			owner, existed := session.ring.addHostIfMissing(h)
			require.False(t, existed, "the peer must not already be in the ring")
			inserted.Store(owner)
		}
	})
	session = s

	// The same peer is in this round's snapshot rows, so the loop reaches the
	// "already exists but not in prevHosts" branch.
	script.setPeers([]peerRow{newPeerRow(admissionPeerHostID, "127.0.0.2")})
	arm.Store(true)

	require.NoError(t, session.refreshRing(), "the round must complete, not abandon itself")
	require.False(t, arm.Load(), "the seam must have fired")

	owner := inserted.Load()
	require.NotNil(t, owner, "the stand-in insert must have happened")

	current, ok := session.ring.getHost(admissionPeerHostID)
	require.True(t, ok, "the inserted entry survives the round")
	require.Same(t, owner, current, "the ring keeps the object that won")

	_, pooled := session.pool.getPoolFor(owner)
	require.True(t, pooled, "the adopted entry must have its pool registered")
	require.Same(t, owner, publishedRecord(session, admissionPeerHostID),
		"the adopted entry must be published to the policy")

	// The sweep must not then remove it: the tail deletes it from prevHosts.
	require.NoError(t, session.refreshRing(), "a following round must run")
	_, ok = session.ring.getHost(admissionPeerHostID)
	require.True(t, ok, "the adopted entry must survive the sweep")
}

// TestRefreshRing_AdoptsTheWinnerWhenAnEndpointChangeLosesTheReAdd forces the second
// setupConn collision: the ID is re-added between refreshRing's removal of the old
// entry and its re-insert of the new one.
//
// refreshRing used to abandon the round with ErrHostAlreadyExists, leaving the ring
// holding the winner with no pool and no publication, and every later host in the
// snapshot unreconciled.
// The winner must be adopted instead.
//
// OnRemovedHost is the seam: it is dispatched synchronously between the two calls.
func TestRefreshRing_AdoptsTheWinnerWhenAnEndpointChangeLosesTheReAdd(t *testing.T) {
	var winner atomic.Pointer[HostInfo]
	var arm atomic.Bool
	var session *Session

	script, _, _, s := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
		cluster.Metadata.HostListener.TopologyChangeListener = topologyRemovedHook{
			onRemoved: func(ev RemovedHostEvent) {
				if ev.Host.HostID() != admissionPeerHostID || !arm.CompareAndSwap(true, false) {
					return
				}
				h, err := NewHostInfoFromAddrPort(net.ParseIP("127.0.0.3"), 9042)
				require.NoError(t, err, "NewHostInfoFromAddrPort")
				h.setHostID(admissionPeerHostID)
				owner, existed := session.ring.addHostIfMissing(h)
				require.False(t, existed, "the removal must have freed the host ID")
				winner.Store(owner)
			},
		}
	})
	session = s

	script.setPeers([]peerRow{newPeerRow(admissionPeerHostID, "127.0.0.2")})
	require.NoError(t, session.refreshRing(), "the first refresh admits the peer")

	// Move the peer's endpoint, so the loop takes the remove-then-re-add branch.
	script.setPeers([]peerRow{newPeerRow(admissionPeerHostID, "127.0.0.4")})
	arm.Store(true)

	require.NoError(t, session.refreshRing(), "the round must complete, not abandon itself")
	require.False(t, arm.Load(), "the seam must have fired")

	w := winner.Load()
	require.NotNil(t, w, "the stand-in re-add must have happened")

	current, ok := session.ring.getHost(admissionPeerHostID)
	require.True(t, ok, "the winner is in the ring")
	require.Same(t, w, current, "the ring keeps the object that won the re-add")

	_, pooled := session.pool.getPoolFor(w)
	require.True(t, pooled, "the winner must have its pool registered, not the loser")
	require.Same(t, w, publishedRecord(session, admissionPeerHostID),
		"the winner must be the object published to the policy")
}

// topologyRemovedHook runs a callback on OnRemovedHost.
type topologyRemovedHook struct {
	onRemoved func(RemovedHostEvent)
}

var _ TopologyChangeListener = topologyRemovedHook{}

func (h topologyRemovedHook) OnNewHost(NewHostEvent) {}

func (h topologyRemovedHook) OnRemovedHost(ev RemovedHostEvent) {
	if h.onRemoved != nil {
		h.onRemoved(ev)
	}
}

// addHostCounter counts AddHost calls per host ID.
type addHostCounter struct {
	HostSelectionPolicy

	mu   sync.Mutex
	adds map[string]int64
}

var _ HostSelectionPolicy = (*addHostCounter)(nil)

func (p *addHostCounter) AddHost(host *HostInfo) {
	p.mu.Lock()
	if p.adds == nil {
		p.adds = make(map[string]int64)
	}
	p.adds[host.HostID()]++
	p.mu.Unlock()
	p.HostSelectionPolicy.AddHost(host)
}

func (p *addHostCounter) count(hostID string) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.adds[hostID]
}

// TestCompleteAdmission_LeavesADownHostAlone: the repair must not undo a DOWN.
//
// markHostDown removes the pool on purpose, and the reconnect schedule owes that
// host a delay before it is dialled again.
// A repair that rebuilt the pool because "the ring owns it and it has none" would
// readmit the host immediately and defeat that delay - and it would do so on a quiet
// ring with no endpoint change and no reconnect configured at all.
func TestCompleteAdmission_LeavesADownHostAlone(t *testing.T) {
	script, _, _, session := startLocalHostFixture(t, "", nil)

	script.setPeers([]peerRow{newPeerRow(admissionPeerHostID, "127.0.0.2")})
	require.NoError(t, session.refreshRing(), "the first refresh admits the peer")

	peer, ok := session.ring.getHost(admissionPeerHostID)
	require.True(t, ok, "the peer is in the ring")

	session.markHostDown(peer)
	require.Equal(t, NodeDown, peer.State(), "the peer is DOWN")
	_, pooled := session.pool.getPoolFor(peer)
	require.False(t, pooled, "markHostDown removed the pool")

	require.False(t, session.completeAdmission(peer), "a DOWN host must not be published")

	_, pooled = session.pool.getPoolFor(peer)
	require.False(t, pooled, "a DOWN host's pool must not be rebuilt by the repair")
}

// TestCompleteAdmission_FilterRejectsBeforeAnythingIsRegistered.
//
// The filter is consulted on the canonical object, not on the snapshot object the
// apply loop already filtered, and it runs before the pool is registered.
// Registering first and filtering afterwards would leave a pool behind and, because
// the fill is armed by a defer, dial a host the application excluded.
func TestCompleteAdmission_FilterRejectsBeforeAnythingIsRegistered(t *testing.T) {
	filter := &flippableHostFilter{}
	script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
		cluster.HostFilter = filter
	})

	script.setPeers([]peerRow{newPeerRow(admissionPeerHostID, "127.0.0.2")})
	require.NoError(t, session.refreshRing(), "the first refresh admits the peer")

	peer, ok := session.ring.getHost(admissionPeerHostID)
	require.True(t, ok, "the peer is in the ring")

	// Undo the admission, then start rejecting the peer.
	session.pool.removeHost(peer)
	session.hostPublishMu.Lock()
	delete(session.publishedHosts, admissionPeerHostID)
	session.hostPublishMu.Unlock()
	rejected := admissionPeerHostID
	filter.reject.Store(&rejected)

	require.False(t, session.completeAdmission(peer), "a filtered host must not be published")

	_, pooled := session.pool.getPoolFor(peer)
	require.False(t, pooled, "a filtered host must not have a pool registered")
	require.Nil(t, publishedRecord(session, admissionPeerHostID), "a filtered host must not be published")
}

// topologyCounter counts OnNewHost callbacks.
type topologyCounter struct {
	onNew func()
}

var _ TopologyChangeListener = topologyCounter{}

func (c topologyCounter) OnRemovedHost(RemovedHostEvent) {}

func (c topologyCounter) OnNewHost(NewHostEvent) {
	if c.onNew != nil {
		c.onNew()
	}
}
