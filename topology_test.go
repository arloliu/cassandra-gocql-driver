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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlacementStrategy_SimpleStrategy(t *testing.T) {
	host0 := (&HostInfo{}).withIdentity("0", "", "")
	host25 := (&HostInfo{}).withIdentity("25", "", "")
	host50 := (&HostInfo{}).withIdentity("50", "", "")
	host75 := (&HostInfo{}).withIdentity("75", "", "")

	tokens := []hostToken{
		{intToken(0), host0},
		{intToken(25), host25},
		{intToken(50), host50},
		{intToken(75), host75},
	}

	hosts := []*HostInfo{host0, host25, host50, host75}

	strat := newSimpleStrategy(2)
	tokenReplicas := strat.replicaMap(&tokenRing{hosts: hosts, tokens: tokens})
	if len(tokenReplicas) != len(tokens) {
		t.Fatalf("expected replica map to have %d items but has %d", len(tokens), len(tokenReplicas))
	}

	for _, replicas := range tokenReplicas {
		if len(replicas.hosts) != strat.rf {
			t.Errorf("expected to have %d replicas got %d for token=%v", strat.rf, len(replicas.hosts), replicas.token)
		}
	}

	for i, token := range tokens {
		ht := tokenReplicas.replicasFor(token.token)
		if ht.token != token.token {
			t.Errorf("token %v not in replica map: %v", token, ht.hosts)
		}

		for j, replica := range ht.hosts {
			exp := tokens[(i+j)%len(tokens)].host
			if exp != replica {
				t.Errorf("expected host %v to be a replica of %v got %v", exp.HostID(), token, replica.HostID())
			}
		}
	}
}

func TestPlacementStrategy_NetworkStrategy(t *testing.T) {
	const (
		totalDCs   = 3
		racksPerDC = 3
		hostsPerDC = 5
	)

	tests := []struct {
		name                   string
		strat                  *networkTopology
		expectedReplicaMapSize int
	}{
		{
			name: "full",
			strat: newNetworkTopology(map[string]int{
				"dc1": 1,
				"dc2": 2,
				"dc3": 3,
			}),
			expectedReplicaMapSize: hostsPerDC * totalDCs,
		},
		{
			name: "missing",
			strat: newNetworkTopology(map[string]int{
				"dc2": 2,
				"dc3": 3,
			}),
			expectedReplicaMapSize: hostsPerDC * 2,
		},
		{
			name: "zero",
			strat: newNetworkTopology(map[string]int{
				"dc1": 0,
				"dc2": 2,
				"dc3": 3,
			}),
			expectedReplicaMapSize: hostsPerDC * 2,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			var (
				hosts  []*HostInfo
				tokens []hostToken
			)
			dcRing := make(map[string][]hostToken, totalDCs)
			for i := 0; i < totalDCs; i++ {
				var dcTokens []hostToken
				dc := fmt.Sprintf("dc%d", i+1)

				for j := 0; j < hostsPerDC; j++ {
					rack := fmt.Sprintf("rack%d", (j%racksPerDC)+1)

					h := (&HostInfo{}).withIdentity(fmt.Sprintf("%s:%s:%d", dc, rack, j), dc, rack)

					token := hostToken{
						token: orderedToken([]byte(h.HostID())),
						host:  h,
					}

					tokens = append(tokens, token)
					dcTokens = append(dcTokens, token)

					hosts = append(hosts, h)
				}

				sort.Sort(&tokenRing{tokens: dcTokens})
				dcRing[dc] = dcTokens
			}

			if len(tokens) != hostsPerDC*totalDCs {
				t.Fatalf("expected %d tokens in the ring got %d", hostsPerDC*totalDCs, len(tokens))
			}
			sort.Sort(&tokenRing{tokens: tokens})

			var expReplicas int
			for _, rf := range test.strat.dcs {
				expReplicas += rf
			}

			tokenReplicas := test.strat.replicaMap(&tokenRing{hosts: hosts, tokens: tokens})
			if len(tokenReplicas) != test.expectedReplicaMapSize {
				t.Fatalf("expected replica map to have %d items but has %d", test.expectedReplicaMapSize,
					len(tokenReplicas))
			}
			if !sort.IsSorted(tokenReplicas) {
				t.Fatal("replica map was not sorted by token")
			}

			for token, replicas := range tokenReplicas {
				if len(replicas.hosts) != expReplicas {
					t.Fatalf("expected to have %d replicas got %d for token=%v", expReplicas, len(replicas.hosts), token)
				}
			}

			for dc, rf := range test.strat.dcs {
				if rf == 0 {
					continue
				}
				dcTokens := dcRing[dc]
				for i, th := range dcTokens {
					token := th.token
					allReplicas := tokenReplicas.replicasFor(token)
					if allReplicas.token != token {
						t.Fatalf("token %v not in replica map", token)
					}

					var replicas []*HostInfo
					for _, replica := range allReplicas.hosts {
						if replica.DataCenter() == dc {
							replicas = append(replicas, replica)
						}
					}

					if len(replicas) != rf {
						t.Fatalf("expected %d replicas in dc %q got %d", rf, dc, len(replicas))
					}

					var lastRack string
					for j, replica := range replicas {
						// expected is in the next rack
						var exp *HostInfo
						if lastRack == "" {
							// primary, first replica
							exp = dcTokens[(i+j)%len(dcTokens)].host
						} else {
							for k := 0; k < len(dcTokens); k++ {
								// walk around the ring from i + j to find the next host the
								// next rack
								p := (i + j + k) % len(dcTokens)
								h := dcTokens[p].host
								if h.Rack() != lastRack {
									exp = h
									break
								}
							}
							if exp.Rack() == lastRack {
								panic("no more racks")
							}
						}
						lastRack = replica.Rack()
					}
				}
			}
		})
	}
}

// Regression test for upstream issue #1947 / CASSGO-122 (adapted from PR #1948).
// When the token ring only contains hosts from a DC that has RF=0/unspecified
// for a keyspace, networkTopology.replicaMap must not panic; it should return
// an empty replica map.
func TestPlacementStrategy_NetworkStrategy_DoNotPanicWhenNoReplicasInRing(t *testing.T) {
	strat := newNetworkTopology(map[string]int{
		"dc1": 3, // replicated only in dc1
	})

	// Hosts in ring only from dc2, so no replicas should be returned.
	hosts := []*HostInfo{
		(&HostInfo{}).withIdentity("dc2:rack1:0", "dc2", "rack1"),
		(&HostInfo{}).withIdentity("dc2:rack2:1", "dc2", "rack2"),
		(&HostInfo{}).withIdentity("dc2:rack3:2", "dc2", "rack3"),
	}

	tokens := make([]hostToken, 0, len(hosts))
	for _, h := range hosts {
		tokens = append(tokens, hostToken{
			token: orderedToken(h.HostID()),
			host:  h,
		})
	}
	sort.Sort(&tokenRing{tokens: tokens})

	require.NotPanics(t, func() {
		replicas := strat.replicaMap(&tokenRing{hosts: hosts, tokens: tokens})
		require.Empty(t, replicas, "expected no replicas, got %d", len(replicas))
	})
}

// replicaIDs returns the host IDs of replicas, in order.
func replicaIDs(replicas []*HostInfo) []string {
	ids := make([]string, 0, len(replicas))
	for _, h := range replicas {
		ids = append(ids, h.HostID())
	}
	return ids
}

// replicaMapIDs flattens a replica map to token → ordered host IDs.
func replicaMapIDs(ring tokenRingReplicas) map[string][]string {
	out := make(map[string][]string, len(ring))
	for _, ht := range ring {
		out[ht.token.String()] = replicaIDs(ht.hosts)
	}
	return out
}

// orderedRing builds a sorted ring from (token, host) pairs.
func orderedRing(hosts []*HostInfo, pairs ...hostToken) *tokenRing {
	ring := &tokenRing{hosts: hosts, tokens: pairs}
	sort.Sort(ring)
	return ring
}

// Adjacent vnodes owned by the same host in a single rack must not put that
// host in a token's replica list twice (F-soak-3).
func TestPlacementStrategy_NetworkStrategy_AdjacentVnodesSingleRack(t *testing.T) {
	a := (&HostInfo{}).withIdentity("a", "dc1", "rack1")
	b := (&HostInfo{}).withIdentity("b", "dc1", "rack1")
	c := (&HostInfo{}).withIdentity("c", "dc1", "rack1")
	ring := orderedRing([]*HostInfo{a, b, c},
		hostToken{orderedToken("01"), a},
		hostToken{orderedToken("02"), a},
		hostToken{orderedToken("03"), b},
		hostToken{orderedToken("04"), c},
	)

	got := replicaMapIDs(newNetworkTopology(map[string]int{"dc1": 3}).replicaMap(ring))

	require.Equal(t, map[string][]string{
		"01": {"a", "b", "c"},
		"02": {"a", "b", "c"},
		"03": {"b", "c", "a"},
		"04": {"c", "a", "b"},
	}, got)
}

// A host skipped while racks were still unseen must not be drained into a
// replica list it is already in, nor drained twice.
func TestPlacementStrategy_NetworkStrategy_SkippedDrainNoRepeat(t *testing.T) {
	a := (&HostInfo{}).withIdentity("a", "dc1", "r1")
	b := (&HostInfo{}).withIdentity("b", "dc1", "r1")
	c := (&HostInfo{}).withIdentity("c", "dc1", "r2")
	ring := orderedRing([]*HostInfo{a, b, c},
		hostToken{orderedToken("01"), a},
		hostToken{orderedToken("02"), a},
		hostToken{orderedToken("03"), b},
		hostToken{orderedToken("04"), c},
	)

	got := replicaMapIDs(newNetworkTopology(map[string]int{"dc1": 3}).replicaMap(ring))

	require.Equal(t, []string{"a", "c", "b"}, got["01"])
	require.Equal(t, []string{"a", "c", "b"}, got["02"])
}

// A host queued twice while its rack is seen must drain once.
func TestPlacementStrategy_NetworkStrategy_SkippedTwiceDrainsOnce(t *testing.T) {
	a := (&HostInfo{}).withIdentity("a", "dc1", "r1")
	b := (&HostInfo{}).withIdentity("b", "dc1", "r1")
	c := (&HostInfo{}).withIdentity("c", "dc1", "r2")
	d := (&HostInfo{}).withIdentity("d", "dc1", "r1")
	ring := orderedRing([]*HostInfo{a, b, c, d},
		hostToken{orderedToken("01"), a},
		hostToken{orderedToken("02"), b},
		hostToken{orderedToken("03"), b},
		hostToken{orderedToken("04"), c},
		hostToken{orderedToken("05"), d},
	)

	got := replicaMapIDs(newNetworkTopology(map[string]int{"dc1": 4}).replicaMap(ring))

	require.Equal(t, []string{"a", "c", "b", "d"}, got["01"])
}

// A drained host met again later in the walk must not be appended again.
func TestPlacementStrategy_NetworkStrategy_DrainedHostMetAgain(t *testing.T) {
	a := (&HostInfo{}).withIdentity("a", "dc1", "r1")
	b := (&HostInfo{}).withIdentity("b", "dc1", "r1")
	c := (&HostInfo{}).withIdentity("c", "dc1", "r2")
	d := (&HostInfo{}).withIdentity("d", "dc1", "r1")
	ring := orderedRing([]*HostInfo{a, b, c, d},
		hostToken{orderedToken("01"), a},
		hostToken{orderedToken("02"), b},
		hostToken{orderedToken("03"), c},
		hostToken{orderedToken("04"), b},
		hostToken{orderedToken("05"), d},
	)

	got := replicaMapIDs(newNetworkTopology(map[string]int{"dc1": 4}).replicaMap(ring))

	require.Equal(t, []string{"a", "c", "b", "d"}, got["01"])
}

// Replicas of two data centers interleave in walk order, each host once.
func TestPlacementStrategy_NetworkStrategy_InterleavedDCs(t *testing.T) {
	a1 := (&HostInfo{}).withIdentity("a1", "dc1", "r1")
	b1 := (&HostInfo{}).withIdentity("b1", "dc1", "r1")
	a2 := (&HostInfo{}).withIdentity("a2", "dc2", "r1")
	b2 := (&HostInfo{}).withIdentity("b2", "dc2", "r1")
	ring := orderedRing([]*HostInfo{a1, b1, a2, b2},
		hostToken{orderedToken("01"), a1},
		hostToken{orderedToken("02"), a2},
		hostToken{orderedToken("03"), a1},
		hostToken{orderedToken("04"), b2},
		hostToken{orderedToken("05"), b1},
		hostToken{orderedToken("06"), a2},
	)

	got := replicaMapIDs(newNetworkTopology(map[string]int{"dc1": 2, "dc2": 2}).replicaMap(ring))

	require.Equal(t, []string{"a1", "a2", "b2", "b1"}, got["01"])
	require.Equal(t, []string{"a1", "b2", "b1", "a2"}, got["03"])
}

// A data center with fewer token-owning hosts than its replication factor
// gets every one of its hosts once.
func TestPlacementStrategy_NetworkStrategy_DCSmallerThanRF(t *testing.T) {
	a := (&HostInfo{}).withIdentity("a", "dc1", "rack1")
	b := (&HostInfo{}).withIdentity("b", "dc1", "rack1")
	ring := orderedRing([]*HostInfo{a, b},
		hostToken{orderedToken("01"), a},
		hostToken{orderedToken("02"), b},
		hostToken{orderedToken("03"), a},
		hostToken{orderedToken("04"), a},
		hostToken{orderedToken("05"), b},
	)

	got := replicaMapIDs(newNetworkTopology(map[string]int{"dc1": 3}).replicaMap(ring))

	require.Equal(t, map[string][]string{
		"01": {"a", "b"},
		"02": {"b", "a"},
		"03": {"a", "b"},
		"04": {"a", "b"},
		"05": {"b", "a"},
	}, got)
}

// cassandra3NaturalEndpoints is an independent port of Apache Cassandra
// NetworkTopologyStrategy.calculateNaturalEndpoints at cassandra-3.0 @ 9e04193c3fe7,
// used as a test oracle. Source:
// https://github.com/apache/cassandra/blob/9e04193c3fe7/src/java/org/apache/cassandra/locator/NetworkTopologyStrategy.java
// Replicas are kept with ordered-set semantics, as the LinkedHashSet there.
//
// Parameters:
//   - dcs: replication factor per data center
//   - tokens: the sorted ring
//   - start: index of the token to place
//
// Returns:
//   - []*HostInfo: the ordered replicas of tokens[start]
func cassandra3NaturalEndpoints(dcs map[string]int, tokens []hostToken, start int) []*HostInfo {
	allEndpoints := map[string]map[*HostInfo]bool{}
	racks := map[string]map[string]bool{}
	for _, th := range tokens {
		dc := th.host.DataCenter()
		if allEndpoints[dc] == nil {
			allEndpoints[dc] = map[*HostInfo]bool{}
			racks[dc] = map[string]bool{}
		}
		allEndpoints[dc][th.host] = true
		racks[dc][th.host.Rack()] = true
	}

	dcReplicas := map[string]map[*HostInfo]bool{}
	seenRacks := map[string]map[string]bool{}
	skipped := map[string][]*HostInfo{}
	skippedSet := map[string]map[*HostInfo]bool{}
	for dc := range dcs {
		dcReplicas[dc] = map[*HostInfo]bool{}
		seenRacks[dc] = map[string]bool{}
		skippedSet[dc] = map[*HostInfo]bool{}
	}

	var replicas []*HostInfo
	inReplicas := map[*HostInfo]bool{}
	add := func(dc string, h *HostInfo) {
		dcReplicas[dc][h] = true
		if !inReplicas[h] {
			inReplicas[h] = true
			replicas = append(replicas, h)
		}
	}
	sufficient := func(dc string) bool {
		return len(dcReplicas[dc]) >= min(len(allEndpoints[dc]), dcs[dc])
	}
	allSufficient := func() bool {
		for dc := range dcs {
			if !sufficient(dc) {
				return false
			}
		}
		return true
	}

	for j := 0; j < len(tokens) && !allSufficient(); j++ {
		ep := tokens[(start+j)%len(tokens)].host
		dc := ep.DataCenter()
		if _, ok := dcs[dc]; !ok || sufficient(dc) {
			continue
		}
		if len(seenRacks[dc]) == len(racks[dc]) {
			add(dc, ep)
			continue
		}
		rack := ep.Rack()
		if seenRacks[dc][rack] {
			if !skippedSet[dc][ep] {
				skippedSet[dc][ep] = true
				skipped[dc] = append(skipped[dc], ep)
			}
			continue
		}
		add(dc, ep)
		seenRacks[dc][rack] = true
		if len(seenRacks[dc]) == len(racks[dc]) {
			for _, sh := range skipped[dc] {
				if sufficient(dc) {
					break
				}
				add(dc, sh)
			}
		}
	}
	return replicas
}

// replicaMap must equal the Cassandra 3.0 placement, as an ordered list, on
// random rings with vnodes, racks, several data centers and odd factors.
func TestPlacementStrategy_NetworkStrategy_MatchesCassandraOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(20260924))
	for iter := range 400 {
		numDCs := 1 + rng.Intn(3)
		var hosts []*HostInfo
		var tokens []hostToken
		usedTokens := map[string]bool{}
		dcs := map[string]int{}
		for d := range numDCs {
			dc := fmt.Sprintf("dc%d", d)
			// Sometimes the strategy leaves the DC unspecified.
			if rng.Intn(4) > 0 {
				dcs[dc] = rng.Intn(5)
			}
			numRacks := 1 + rng.Intn(4)
			numHosts := 1 + rng.Intn(8)
			for i := range numHosts {
				// Rack names repeat across DCs on purpose.
				h := (&HostInfo{}).withIdentity(fmt.Sprintf("%s-h%d", dc, i), dc, fmt.Sprintf("r%d", rng.Intn(numRacks)))
				hosts = append(hosts, h)
				for v := 1 + rng.Intn(16); v > 0; v-- {
					var tok string
					for {
						tok = fmt.Sprintf("%08d", rng.Intn(100000000))
						if !usedTokens[tok] {
							break
						}
					}
					usedTokens[tok] = true
					tokens = append(tokens, hostToken{orderedToken(tok), h})
				}
			}
		}
		// A data center the strategy names but the ring lacks.
		if rng.Intn(4) == 0 {
			dcs["absent"] = 1 + rng.Intn(3)
		}
		ring := orderedRing(hosts, tokens...)
		strat := newNetworkTopology(dcs)

		var got tokenRingReplicas
		require.NotPanics(t, func() { got = strat.replicaMap(ring) }, "iter %d dcs %v", iter, dcs)

		// Stored entries: only tokens of replicated DCs, owner first, no repeats.
		stored := map[string]bool{}
		for _, ht := range got {
			stored[ht.token.String()] = true
			require.NotEmpty(t, ht.hosts, "iter %d token %v", iter, ht.token)
			seen := map[*HostInfo]bool{}
			for _, h := range ht.hosts {
				require.False(t, seen[h], "iter %d token %v repeats %s", iter, ht.token, h.HostID())
				seen[h] = true
			}
		}
		anyReplicated := false
		for _, th := range ring.tokens {
			replicated := dcs[th.host.DataCenter()] > 0
			anyReplicated = anyReplicated || replicated
			require.Equal(t, replicated, stored[th.token.String()], "iter %d token %v stored", iter, th.token)
			if replicated {
				require.Same(t, th.host, got.replicasFor(th.token).hosts[0], "iter %d token %v primary", iter, th.token)
			}
		}
		if !anyReplicated {
			require.Empty(t, got, "iter %d dcs %v", iter, dcs)
			require.Nil(t, got.replicasFor(ring.tokens[0].token))
			continue
		}

		// Lookups: every token, and a key strictly between it and the next one.
		for i, th := range ring.tokens {
			want := replicaIDs(cassandra3NaturalEndpoints(dcs, ring.tokens, i))
			require.Equal(t, want, replicaIDs(got.replicasFor(th.token).hosts),
				"iter %d token %v dcs %v", iter, th.token, dcs)

			between := orderedToken(string(th.token.(orderedToken)) + "5")
			wantNext := replicaIDs(cassandra3NaturalEndpoints(dcs, ring.tokens, (i+1)%len(ring.tokens)))
			require.Equal(t, wantNext, replicaIDs(got.replicasFor(between).hosts),
				"iter %d key %v dcs %v", iter, between, dcs)
		}
	}
}

// benchmarkNetworkTopologyRing builds a ring of numDCs data centers with
// hostsPerDC hosts each, spread over racks racks, each host owning vnodes tokens.
//
// Parameters:
//   - numDCs: number of data centers, named dc0, dc1, ...
//   - hostsPerDC: hosts per data center
//   - vnodes: tokens per host
//   - racks: racks per data center
//
// Returns:
//   - *tokenRing: the sorted ring
func benchmarkNetworkTopologyRing(numDCs, hostsPerDC, vnodes, racks int) *tokenRing {
	rng := rand.New(rand.NewSource(1))
	var hosts []*HostInfo
	var tokens []hostToken
	used := map[string]bool{}
	for d := range numDCs {
		for i := range hostsPerDC {
			h := (&HostInfo{}).withIdentity(fmt.Sprintf("dc%d-%d", d, i), fmt.Sprintf("dc%d", d), fmt.Sprintf("r%d", i%racks))
			hosts = append(hosts, h)
			for range vnodes {
				var tok string
				for {
					tok = fmt.Sprintf("%012d", rng.Int63n(1e12))
					if !used[tok] {
						break
					}
				}
				used[tok] = true
				tokens = append(tokens, hostToken{orderedToken(tok), h})
			}
		}
	}
	return orderedRing(hosts, tokens...)
}

// BenchmarkNetworkTopologyReplicaMap measures a full replica map build.
// The undersized and absent cases guard the effective replication factor cap:
// without it every token's walk covers the whole ring.
func BenchmarkNetworkTopologyReplicaMap(b *testing.B) {
	cases := []struct {
		name string
		ring *tokenRing
		dcs  map[string]int
	}{
		{"3dc-20hosts-256vnodes-3racks", benchmarkNetworkTopologyRing(3, 20, 256, 3), map[string]int{"dc0": 3, "dc1": 3, "dc2": 3}},
		{"3dc-20hosts-256vnodes-1rack", benchmarkNetworkTopologyRing(3, 20, 256, 1), map[string]int{"dc0": 3, "dc1": 3, "dc2": 3}},
		{"undersized-2hosts-2048vnodes-rf3", benchmarkNetworkTopologyRing(1, 2, 2048, 1), map[string]int{"dc0": 3}},
		{"absent-dc-3hosts-1024vnodes", benchmarkNetworkTopologyRing(1, 3, 1024, 1), map[string]int{"dc0": 3, "absent": 3}},
	}
	for _, c := range cases {
		strat := newNetworkTopology(c.dcs)
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = strat.replicaMap(c.ring)
			}
		})
	}
}
