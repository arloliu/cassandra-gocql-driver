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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// F-ring-1: the built-in policies keyed membership by connect address.
//
// refreshRing admits a newcomer in its hosts loop and sweeps a departed entry only afterwards,
// and it reconciles a moved host as remove-old-then-add-new.
// With address-keyed membership,
// AddHost of a host at an address another listed host still held was a no-op,
// and the other host's RemoveHost then evicted whatever was listed at that address.
// These tests drive both shapes through a real session:
// a node replaced under a new host_id at the same address,
// and addresses rotating between host_ids.

const (
	ringIdentityOldID = "aaaaaaaa-0000-4000-8000-000000000001"
	ringIdentityNewID = "bbbbbbbb-0000-4000-8000-000000000002"
	ringIdentityAddr  = "127.0.0.2"

	ringIdentityX = "aaaaaaaa-0000-4000-8000-0000000000a1"
	ringIdentityY = "bbbbbbbb-0000-4000-8000-0000000000b2"
	ringIdentityZ = "cccccccc-0000-4000-8000-0000000000c3"
)

// orderingListener records each ring object's UP,
// and once armed keeps refreshRing in OnNewHost until that object's UP was published.
//
// OnNewHost runs synchronously in refreshRing's hosts loop,
// after the admission's fill and before the sweep,
// and OnHostUp runs after the policy's HostUp.
// So the hold orders the newcomer's UP ahead of the sweep with an ordinary application listener,
// a schedule the driver permits.
// A hold that times out is counted,
// because the refresh then runs unordered and an ordered test would pass without exercising its case.
type orderingListener struct {
	// hold arms the barrier.
	// It is set after the session is created, so the session's own startup is never held.
	hold atomic.Bool
	// timedOut counts the barriers that expired before the UP arrived.
	timedOut atomic.Int64

	mu sync.Mutex
	up map[*HostInfo]chan struct{}
}

func newOrderingListener() *orderingListener {
	return &orderingListener{up: map[*HostInfo]chan struct{}{}}
}

func (l *orderingListener) ch(h *HostInfo) chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	c, ok := l.up[h]
	if !ok {
		c = make(chan struct{})
		l.up[h] = c
	}
	return c
}

func (l *orderingListener) OnHostUp(ev HostUpEvent) {
	c := l.ch(ev.Host)
	select {
	case <-c:
	default:
		close(c)
	}
}

func (l *orderingListener) OnHostDown(HostDownEvent) {}

func (l *orderingListener) OnNewHost(ev NewHostEvent) {
	if !l.hold.Load() {
		return
	}
	select {
	case <-l.ch(ev.Host):
	case <-time.After(5 * time.Second):
		l.timedOut.Add(1)
	}
}

func (l *orderingListener) OnRemovedHost(RemovedHostEvent) {}

// awaitUp returns the ring's object for id once its UP was published.
func (l *orderingListener) awaitUp(t *testing.T, session *Session, id string) *HostInfo {
	t.Helper()
	h, ok := session.ring.getHost(id)
	require.True(t, ok, "host %s is in the ring", id)
	select {
	case <-l.ch(h):
	case <-time.After(5 * time.Second):
		t.Fatalf("host %s never came up", id)
	}
	return h
}

// requireOrdered fails an ordered test whose barrier expired.
func (l *orderingListener) requireOrdered(t *testing.T, ordered bool) {
	t.Helper()
	if ordered {
		require.Zero(t, l.timedOut.Load(), "the ordering barrier timed out; the ordered case did not run")
	}
}

// startRingIdentitySession starts a session on the scripted node with policy
// and listener installed.
func startRingIdentitySession(t *testing.T, policy HostSelectionPolicy, listener *orderingListener, ordered bool) (*localHostServer, *Session) {
	t.Helper()
	script, _, _, session := startLocalHostFixture(t, "", func(cluster *ClusterConfig, _ string) {
		cluster.PoolConfig.HostSelectionPolicy = policy
		cluster.Metadata.HostListener.HostStateChangeListener = listener
		cluster.Metadata.HostListener.TopologyChangeListener = listener
	})
	listener.hold.Store(ordered)
	return script, session
}

// peerRowWithToken is newPeerRow with its one token replaced.
func peerRowWithToken(hostID, addr, token string) peerRow {
	row := newPeerRow(hostID, addr)
	row.tokens = []string{token}
	return row
}

func ringIdentityIDs(hosts []*HostInfo) []string {
	ids := make([]string, 0, len(hosts))
	for _, h := range hosts {
		ids = append(ids, h.HostID())
	}
	return ids
}

// requireTokenRingOwners asserts that token-aware's token ring names each owner for every one of its tokens,
// and names no departed id at all.
func requireTokenRingOwners(t *testing.T, ta *tokenAwareHostPolicy, owners []*HostInfo, departed ...string) {
	t.Helper()
	meta := ta.getMetadataReadOnly()
	require.NotNil(t, meta, "token-aware holds no metadata")
	require.NotNil(t, meta.tokenRing, "token-aware holds no token ring; the fixture serves Murmur3")
	ring := meta.tokenRing

	for _, ht := range ring.tokens {
		require.NotContains(t, departed, ht.host.HostID(), "the token ring still names a departed host: %v", ht)
	}
	for _, owner := range owners {
		for _, tok := range owner.Tokens() {
			want := ring.partitioner.ParseString(tok).String()
			found := false
			for _, ht := range ring.tokens {
				if ht.token.String() == want && ht.host.HostID() == owner.HostID() {
					found = true
					break
				}
			}
			require.True(t, found, "the token ring must name %s for its token %s: %v", owner.HostID(), tok, ring.tokens)
		}
	}
}

// replaceAtSameAddress adopts a node,
// then serves the same address under a new host_id with the old one gone,
// and returns the replacement's ring object once it is UP with a full pool.
func replaceAtSameAddress(t *testing.T, policy HostSelectionPolicy, ordered bool) *HostInfo {
	t.Helper()

	listener := newOrderingListener()
	script, session := startRingIdentitySession(t, policy, listener, ordered)

	// Round 1: the original node.
	script.setPeers([]peerRow{peerRowWithToken(ringIdentityOldID, ringIdentityAddr, "-6000000000000000000")})
	require.NoError(t, session.refreshRing(), "adopt the original node")
	listener.requireOrdered(t, ordered)
	listener.awaitUp(t, session, ringIdentityOldID)

	// Round 2: the same address now answers under a new host_id and other tokens.
	script.setPeers([]peerRow{peerRowWithToken(ringIdentityNewID, ringIdentityAddr, "6000000000000000000")})
	require.NoError(t, session.refreshRing(), "reconcile the replacement")
	listener.requireOrdered(t, ordered)
	newHost := listener.awaitUp(t, session, ringIdentityNewID)

	_, ok := session.ring.getHost(ringIdentityOldID)
	require.False(t, ok, "the departed node left the ring")
	require.True(t, newHost.IsUp(), "the replacement is UP")
	pool, ok := session.pool.getPoolFor(newHost)
	require.True(t, ok, "the replacement has a registered pool")
	require.Eventually(t, func() bool { return pool.Size() == 1 }, 5*time.Second, 5*time.Millisecond,
		"the replacement's pool is full")
	return newHost
}

// TestRingIdentity_ReplacedAtSameAddress_RoundRobin orders the replacement's UP before the sweep.
// Before the fix its HostUp was a no-op beside the departed entry,
// and the departed node's RemoveHost then evicted it.
func TestRingIdentity_ReplacedAtSameAddress_RoundRobin(t *testing.T) {
	policy := RoundRobinHostPolicy()
	replaceAtSameAddress(t, policy, true)

	listed := ringIdentityIDs(policy.(*roundRobinHostPolicy).hosts.get())
	require.Contains(t, listed, ringIdentityNewID,
		"the UP, fully pooled replacement must be in the selection policy")
	require.NotContains(t, listed, ringIdentityOldID,
		"the departed node must not be in the selection policy")
}

// TestRingIdentity_ReplacedAtSameAddress_TokenAware covers token-aware,
// whose membership, and through it the token ring, changes only in AddHost and RemoveHost.
// Before the fix the replacement was lost whichever way the UP and the sweep raced.
func TestRingIdentity_ReplacedAtSameAddress_TokenAware(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ordered bool
	}{
		{"UpBeforeSweep", true},
		{"Racing", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := TokenAwareHostPolicy(RoundRobinHostPolicy())
			newHost := replaceAtSameAddress(t, policy, tc.ordered)

			ta := policy.(*tokenAwareHostPolicy)
			members := ringIdentityIDs(ta.hosts.get())
			fallback := ringIdentityIDs(ta.fallback.(*roundRobinHostPolicy).hosts.get())
			require.Contains(t, members, ringIdentityNewID, "the replacement must be in token-aware's membership")
			require.Contains(t, fallback, ringIdentityNewID, "the replacement must be in the fallback's membership")
			require.NotContains(t, members, ringIdentityOldID, "the departed node must not be in token-aware's membership")
			require.NotContains(t, fallback, ringIdentityOldID, "the departed node must not be in the fallback's membership")
			requireTokenRingOwners(t, ta, []*HostInfo{newHost}, ringIdentityOldID)
		})
	}
}

// TestRingIdentity_AddressRecycling rotates every address by one in a single snapshot,
// which is what a StatefulSet restarted while the control connection was away looks like:
// X moves .2 -> .3, Y .3 -> .4, Z .4 -> .2.
// refreshRing reconciles them one at a time,
// so each host's new address is still held by another host's old entry when it is added.
func TestRingIdentity_AddressRecycling(t *testing.T) {
	roundRobin := func() HostSelectionPolicy { return RoundRobinHostPolicy() }
	tokenAware := func() HostSelectionPolicy { return TokenAwareHostPolicy(RoundRobinHostPolicy()) }
	tokens := map[string]string{
		ringIdentityX: "-6000000000000000000",
		ringIdentityY: "0",
		ringIdentityZ: "6000000000000000000",
	}
	moved := map[string]string{
		ringIdentityX: "127.0.0.3",
		ringIdentityY: "127.0.0.4",
		ringIdentityZ: "127.0.0.2",
	}
	ids := []string{ringIdentityX, ringIdentityY, ringIdentityZ}

	for _, tc := range []struct {
		name    string
		ordered bool
		policy  func() HostSelectionPolicy
	}{
		{"RoundRobin/Ordered", true, roundRobin},
		{"RoundRobin/Racing", false, roundRobin},
		{"TokenAware/Ordered", true, tokenAware},
		{"TokenAware/Racing", false, tokenAware},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := tc.policy()
			listener := newOrderingListener()
			script, session := startRingIdentitySession(t, policy, listener, tc.ordered)

			listed := func() []*HostInfo {
				switch p := policy.(type) {
				case *roundRobinHostPolicy:
					return p.hosts.get()
				case *tokenAwareHostPolicy:
					return p.hosts.get()
				}
				t.Fatalf("unexpected policy %T", policy)
				return nil
			}

			script.setPeers([]peerRow{
				peerRowWithToken(ringIdentityX, "127.0.0.2", tokens[ringIdentityX]),
				peerRowWithToken(ringIdentityY, "127.0.0.3", tokens[ringIdentityY]),
				peerRowWithToken(ringIdentityZ, "127.0.0.4", tokens[ringIdentityZ]),
			})
			require.NoError(t, session.refreshRing(), "adopt the three peers")
			listener.requireOrdered(t, tc.ordered)
			for _, id := range ids {
				listener.awaitUp(t, session, id)
			}
			require.Len(t, listed(), 4, "control host plus three peers")

			// One snapshot, every address rotated by one.
			rows := make([]peerRow, 0, len(ids))
			for _, id := range ids {
				rows = append(rows, peerRowWithToken(id, moved[id], tokens[id]))
			}
			script.setPeers(rows)
			require.NoError(t, session.refreshRing(), "reconcile the rotation")
			listener.requireOrdered(t, tc.ordered)

			current := make([]*HostInfo, 0, len(ids))
			for _, id := range ids {
				h := listener.awaitUp(t, session, id)
				require.True(t, h.IsUp(), "host %s is UP", id)
				pool, ok := session.pool.getPoolFor(h)
				require.True(t, ok, "host %s has a registered pool", id)
				require.Eventually(t, func() bool { return pool.Size() == 1 }, 5*time.Second, 5*time.Millisecond,
					"host %s's pool is full", id)
				current = append(current, h)
			}

			hosts := listed()
			for _, id := range ids {
				require.Contains(t, ringIdentityIDs(hosts), id, "an UP, fully pooled host must be in the selection policy")
				var at []string
				for _, h := range hosts {
					if h.HostID() == id {
						at = append(at, h.ConnectAddress().String())
					}
				}
				require.Equal(t, []string{moved[id]}, at, "host %s must be listed once, at its new address", id)
			}

			if ta, ok := policy.(*tokenAwareHostPolicy); ok {
				fallback := ringIdentityIDs(ta.fallback.(*roundRobinHostPolicy).hosts.get())
				for _, id := range ids {
					require.Contains(t, fallback, id, "an UP, fully pooled host must be in the fallback's membership")
				}
				requireTokenRingOwners(t, ta, current)
			}
		})
	}
}
