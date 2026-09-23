//go:build all || unit
// +build all unit

// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
	"net"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Tests of the round-robin host selection policy implementation
func TestRoundRobbin(t *testing.T) {
	policy := RoundRobinHostPolicy()

	hosts := [...]*HostInfo{
		(&HostInfo{connectAddress: net.IPv4(0, 0, 0, 1)}).withIdentity("0", "", ""),
		(&HostInfo{connectAddress: net.IPv4(0, 0, 0, 2)}).withIdentity("1", "", ""),
	}

	for _, host := range hosts {
		policy.AddHost(host)
	}

	got := make(map[string]bool)
	it := policy.Pick(nil)
	for h := it(); h != nil; h = it() {
		id := h.Info().HostID()
		if got[id] {
			t.Fatalf("got duplicate host: %v", id)
		}
		got[id] = true
	}
	if len(got) != len(hosts) {
		t.Fatalf("expected %d hosts got %d", len(hosts), len(got))
	}
}

// Tests of the token-aware host selection policy implementation with a
// round-robin host selection policy fallback.
func TestHostPolicy_TokenAware_SimpleStrategy(t *testing.T) {
	const keyspace = "myKeyspace"
	policy := TokenAwareHostPolicy(RoundRobinHostPolicy(), DoNotShuffleReplicas())
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return keyspace }
	keyspaceMeta := &KeyspaceMetadata{
		Name:          keyspace,
		StrategyClass: "SimpleStrategy",
		StrategyOptions: map[string]interface{}{
			"class":              "SimpleStrategy",
			"replication_factor": 2,
		},
	}
	strategy := getStrategy(keyspaceMeta, nopLoggerSingleton)
	keyspaceMeta.placementStrategy = strategy
	policyInternal.getSchemaMeta = func() *schemaMeta {
		return &schemaMeta{
			keyspaceMeta: map[string]*KeyspaceMetadata{
				keyspace: keyspaceMeta,
			},
		}
	}
	query := &Query{}
	query.getKeyspace = func() string { return keyspace }

	iter := policy.Pick(nil)
	if iter == nil {
		t.Fatal("host iterator was nil")
	}
	actual := iter()
	if actual != nil {
		t.Fatalf("expected nil from iterator, but was %v", actual)
	}

	// set the hosts
	hosts := [...]*HostInfo{
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"00"}}).withIdentity("0", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"25"}}).withIdentity("1", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50"}}).withIdentity("2", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"75"}}).withIdentity("3", "", ""),
	}
	for _, host := range &hosts {
		policy.AddHost(host)
	}

	policy.SetPartitioner("OrderedPartitioner")

	// The SimpleStrategy above should generate the following replicas.
	// It's handy to have as reference here.
	assertDeepEqual(t, "replicas", map[string]tokenRingReplicas{
		strategy.strategyKey(): {
			{orderedToken("00"), []*HostInfo{hosts[0], hosts[1]}},
			{orderedToken("25"), []*HostInfo{hosts[1], hosts[2]}},
			{orderedToken("50"), []*HostInfo{hosts[2], hosts[3]}},
			{orderedToken("75"), []*HostInfo{hosts[3], hosts[0]}},
		},
	}, policyInternal.getMetadataReadOnly().replicas)

	// now the token ring is configured
	query.RoutingKey([]byte("20"))
	iter = policy.Pick(newInternalQuery(query, nil))
	// first token-aware hosts
	expectHosts(t, "hosts[0]", iter, "1")
	expectHosts(t, "hosts[1]", iter, "2")
	// then rest of the hosts
	expectHosts(t, "rest", iter, "0", "3")
	expectNoMoreHosts(t, iter)
}

func TestHostPolicy_RoundRobin_NilHostInfo(t *testing.T) {
	policy := RoundRobinHostPolicy()

	host := (&HostInfo{}).withIdentity("host-1", "", "")
	policy.AddHost(host)

	iter := policy.Pick(nil)
	next := iter()
	if next == nil {
		t.Fatal("got nil host")
	} else if v := next.Info(); v == nil {
		t.Fatal("got nil HostInfo")
	} else if v.HostID() != host.HostID() {
		t.Fatalf("expected host %v got %v", host, v)
	}

	next = iter()
	if next != nil {
		t.Errorf("expected to get nil host got %+v", next)
		if next.Info() == nil {
			t.Fatalf("HostInfo is nil")
		}
	}
}

func TestHostPolicy_TokenAware_NilHostInfo(t *testing.T) {
	policy := TokenAwareHostPolicy(RoundRobinHostPolicy())
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return "myKeyspace" }

	hosts := [...]*HostInfo{
		{connectAddress: net.IPv4(10, 0, 0, 0), tokens: []string{"00"}},
		{connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"25"}},
		{connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"50"}},
		{connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"75"}},
	}
	for _, host := range hosts {
		policy.AddHost(host)
	}
	policy.SetPartitioner("OrderedPartitioner")

	query := &Query{}
	query.getKeyspace = func() string { return "myKeyspace" }
	query.RoutingKey([]byte("20"))

	iter := policy.Pick(newInternalQuery(query, nil))
	next := iter()
	if next == nil {
		t.Fatal("got nil host")
	} else if v := next.Info(); v == nil {
		t.Fatal("got nil HostInfo")
	} else if !v.ConnectAddress().Equal(hosts[1].ConnectAddress()) {
		t.Fatalf("expected peer 1 got %v", v.ConnectAddress())
	}

	// Empty the hosts to trigger the panic when using the fallback.
	for _, host := range hosts {
		policy.RemoveHost(host)
	}

	next = iter()
	if next != nil {
		t.Errorf("expected to get nil host got %+v", next)
		if next.Info() == nil {
			t.Fatalf("HostInfo is nil")
		}
	}
}

func TestCOWList_Add(t *testing.T) {
	var cow cowHostList

	toAdd := [...]net.IP{net.IPv4(10, 0, 0, 1), net.IPv4(10, 0, 0, 2), net.IPv4(10, 0, 0, 3)}

	for _, addr := range toAdd {
		if !cow.add(&HostInfo{connectAddress: addr}) {
			t.Fatal("did not add peer which was not in the set")
		}
	}

	hosts := cow.get()
	if len(hosts) != len(toAdd) {
		t.Fatalf("expected to have %d hosts got %d", len(toAdd), len(hosts))
	}

	set := make(map[string]bool)
	for _, host := range hosts {
		set[string(host.ConnectAddress())] = true
	}

	for _, addr := range toAdd {
		if !set[string(addr)] {
			t.Errorf("addr was not in the host list: %q", addr)
		}
	}
}

// identityTestHost builds a bare host at addr, with id published when it is non-empty.
func identityTestHost(id string, addr net.IP) *HostInfo {
	h := &HostInfo{connectAddress: addr}
	if id != "" {
		h.withIdentity(id, "", "")
	}
	return h
}

// requireNoNilHost fails when the list holds a nil entry.
func requireNoNilHost(t *testing.T, cow *cowHostList) {
	t.Helper()
	for i, h := range cow.get() {
		require.NotNil(t, h, "remove left a nil entry at %d in %v", i, cow.get())
	}
}

// TestCOWList_IdentityKeyed pins that the list identifies a host by its host_id:
// two hosts at one address are two entries,
// and a distinct object with a listed host's id is that host wherever it is.
func TestCOWList_IdentityKeyed(t *testing.T) {
	addrA, addrB := net.IPv4(10, 0, 0, 1), net.IPv4(10, 0, 0, 2)
	a := identityTestHost("X", addrA)
	b := identityTestHost("Y", addrA)

	var cow cowHostList
	require.True(t, cow.add(a), "add(A) to an empty list returned false")
	require.True(t, cow.add(b), "B not added: add(B) returned false beside A at the same address")
	require.True(t, cow.remove(a), "remove(A) found nothing")
	requireNoNilHost(t, &cow)
	require.Equal(t, []*HostInfo{b}, cow.get(), "remove(A) also removed B")

	require.True(t, cow.add(a), "re-adding A returned false")
	aPrime := identityTestHost("X", addrB)
	require.False(t, cow.add(aPrime), "add(A') returned true: a distinct object with A's id is A")
	require.True(t, cow.remove(aPrime), "remove(A') found nothing: a distinct object with A's id is A")
	requireNoNilHost(t, &cow)
	require.Equal(t, []*HostInfo{b}, cow.get(), "remove(A') must remove A and only A")
}

// TestCOWList_AddressKey pins that hosts without a host_id are identified by their connect address,
// as every host was before.
func TestCOWList_AddressKey(t *testing.T) {
	addrA, addrB := net.IPv4(10, 0, 0, 1), net.IPv4(10, 0, 0, 2)
	first := identityTestHost("", addrA)
	second := identityTestHost("", addrA)
	elsewhere := identityTestHost("", addrB)
	identified := identityTestHost("Y", addrA)

	var cow cowHostList
	require.True(t, cow.add(first), "add to an empty list returned false")
	require.False(t, cow.add(second), "a second id-less host at one address was added")
	require.True(t, cow.add(elsewhere), "the id-less host at a different address was not added")
	require.True(t, cow.add(identified), "the identified host at an id-less host's address was not added")

	require.True(t, cow.remove(second), "removing an id-less host found nothing at its address")
	requireNoNilHost(t, &cow)
	require.Equal(t, []*HostInfo{elsewhere, identified}, cow.get(),
		"removing an id-less host must remove only the id-less entry at its address")
}

// TestCOWList_MixedPairsNeverMatch pins that a host with a host_id and one without are never the same host,
// so the id-less removal unpublishHostLocked can issue does not evict an identified host at the same address.
func TestCOWList_MixedPairsNeverMatch(t *testing.T) {
	addr := net.IPv4(10, 0, 0, 1)
	b := identityTestHost("Y", addr)
	c := identityTestHost("", addr)

	var cow cowHostList
	require.True(t, cow.add(b), "add(B) to an empty list returned false")
	require.True(t, cow.add(c), "add(C) returned false: an id-less host matched an identified one")
	require.True(t, cow.remove(c), "remove(C) found nothing")
	requireNoNilHost(t, &cow)
	require.Equal(t, []*HostInfo{b}, cow.get(), "remove(C) removed B")

	var onlyB cowHostList
	require.True(t, onlyB.add(b), "add(B) to an empty list returned false")
	require.False(t, onlyB.remove(identityTestHost("", addr)), "the bare removal on the list holding only B removed B")
	require.Equal(t, []*HostInfo{b}, onlyB.get(), "the bare removal on the list holding only B removed B")
}

// TestCOWList_RemoveAfterIdFilledInPlace covers a listed id-less host whose id HostInfo.update fills in place:
// it then shares a key with another entry, and one removal matches both.
// The list must shrink by two, not expose a nil slot.
func TestCOWList_RemoveAfterIdFilledInPlace(t *testing.T) {
	addr := net.IPv4(10, 0, 0, 1)
	c := identityTestHost("", addr)
	d := identityTestHost("Y", addr)

	var cow cowHostList
	require.True(t, cow.add(c), "add(C) to an empty list returned false")
	require.True(t, cow.add(d), "add(D) returned false: an id-less host matched an identified one")

	c.update(d)
	require.Equal(t, "Y", c.HostID(), "update must fill C's empty host_id from D")

	require.True(t, cow.remove(d), "remove(D) found nothing")
	requireNoNilHost(t, &cow)
	require.Empty(t, cow.get(), "remove(D) must remove both entries that carry D's id")
}

// TestTokenAware_ReplacedAtSameAddressReplicaMap covers a replacement at one address carrying the same token:
// token-aware's token ring holds both hosts until the departed one is removed,
// and afterwards no replica map entry names it.
func TestTokenAware_ReplacedAtSameAddressReplicaMap(t *testing.T) {
	const keyspace = "myKeyspace"
	policy := TokenAwareHostPolicy(RoundRobinHostPolicy())
	ta := policy.(*tokenAwareHostPolicy)
	keyspaceMeta := &KeyspaceMetadata{
		Name:          keyspace,
		StrategyClass: "SimpleStrategy",
		StrategyOptions: map[string]any{
			"class":              "SimpleStrategy",
			"replication_factor": 1,
		},
	}
	strategy := getStrategy(keyspaceMeta, nopLoggerSingleton)
	keyspaceMeta.placementStrategy = strategy
	ta.getSchemaMeta = func() *schemaMeta {
		return &schemaMeta{keyspaceMeta: map[string]*KeyspaceMetadata{keyspace: keyspaceMeta}}
	}
	policy.SetPartitioner("OrderedPartitioner")

	addr := net.IPv4(10, 0, 0, 1)
	a := (&HostInfo{connectAddress: addr, tokens: []string{"50"}}).withIdentity("X", "", "")
	b := (&HostInfo{connectAddress: addr, tokens: []string{"50"}}).withIdentity("Y", "", "")

	policy.AddHost(a)
	policy.AddHost(b)
	var ringIDs []string
	for _, ht := range ta.getMetadataReadOnly().tokenRing.tokens {
		ringIDs = append(ringIDs, ht.host.HostID())
	}
	require.ElementsMatch(t, []string{"X", "Y"}, ringIDs, "after AddHost(B) the token ring must name both A and B")

	policy.RemoveHost(a)
	replicas := ta.getMetadataReadOnly().replicas[strategy.strategyKey()]
	require.NotEmpty(t, replicas, "no replica map for the keyspace's strategy")
	var named []string
	for _, ht := range replicas {
		require.Equal(t, orderedToken("50"), ht.token, "the replica map holds a token no host owns: %v", replicas)
		for _, h := range ht.hosts {
			named = append(named, h.HostID())
		}
	}
	require.NotContains(t, named, "X", "the replica map for T still names A: %v", replicas)
	require.Contains(t, named, "Y", "the replica map for T must name B: %v", replicas)
}

// TestSimpleRetryPolicy makes sure that we only allow 1 + numRetries attempts
func TestSimpleRetryPolicy(t *testing.T) {
	q := newInternalQuery(&Query{}, nil)

	// this should allow a total of 3 tries.
	rt := &SimpleRetryPolicy{NumRetries: 2}

	cases := []struct {
		attempts int
		allow    bool
	}{
		{0, true},
		{1, true},
		{2, true},
		{3, false},
		{4, false},
		{5, false},
	}

	for _, c := range cases {
		q.metrics = &queryMetrics{totalAttempts: int64(c.attempts)}
		q.hostMetricsManager = preFilledHostMetricsMetricsManager(map[string]*hostMetrics{"127.0.0.1": {Attempts: c.attempts}})
		if c.allow && !rt.Attempt(q) {
			t.Fatalf("should allow retry after %d attempts", c.attempts)
		}
		if !c.allow && rt.Attempt(q) {
			t.Fatalf("should not allow retry after %d attempts", c.attempts)
		}
	}
}

func TestExponentialBackoffPolicy(t *testing.T) {
	// test with defaults
	sut := &ExponentialBackoffRetryPolicy{NumRetries: 2}

	cases := []struct {
		attempts int
		delay    time.Duration
	}{

		{1, 100 * time.Millisecond},
		{2, (2) * 100 * time.Millisecond},
		{3, (2 * 2) * 100 * time.Millisecond},
		{4, (2 * 2 * 2) * 100 * time.Millisecond},
	}
	for _, c := range cases {
		// test 100 times for each case
		for i := 0; i < 100; i++ {
			d := sut.napTime(c.attempts)
			if d < c.delay-(100*time.Millisecond)/2 {
				t.Fatalf("Delay %d less than jitter min of %d", d, c.delay-100*time.Millisecond/2)
			}
			if d > c.delay+(100*time.Millisecond)/2 {
				t.Fatalf("Delay %d greater than jitter max of %d", d, c.delay+100*time.Millisecond/2)
			}
		}
	}
}

func TestDowngradingConsistencyRetryPolicy(t *testing.T) {

	q := newInternalQuery(&Query{initialConsistency: LocalQuorum}, nil)

	rewt0 := &RequestErrWriteTimeout{
		Received:  0,
		WriteType: "SIMPLE",
	}

	rewt1 := &RequestErrWriteTimeout{
		Received:  1,
		WriteType: "BATCH",
	}

	rewt2 := &RequestErrWriteTimeout{
		WriteType: "UNLOGGED_BATCH",
	}

	rert := &RequestErrReadTimeout{}

	reu0 := &RequestErrUnavailable{
		Alive: 0,
	}

	reu1 := &RequestErrUnavailable{
		Alive: 1,
	}

	// this should allow a total of 3 tries.
	consistencyLevels := []Consistency{Three, Two, One}
	rt := &DowngradingConsistencyRetryPolicy{ConsistencyLevelsToTry: consistencyLevels}
	cases := []struct {
		attempts  int
		allow     bool
		err       error
		retryType RetryType
	}{
		{0, true, rewt0, Rethrow},
		{3, true, rewt1, Ignore},
		{1, true, rewt2, Retry},
		{2, true, rert, Retry},
		{4, false, reu0, Rethrow},
		{16, false, reu1, Retry},
	}

	for _, c := range cases {
		q.metrics = &queryMetrics{totalAttempts: int64(c.attempts)}
		q.hostMetricsManager = preFilledHostMetricsMetricsManager(map[string]*hostMetrics{"127.0.0.1": {Attempts: c.attempts}})
		if c.retryType != rt.GetRetryType(c.err) {
			t.Fatalf("retry type should be %v", c.retryType)
		}
		if c.allow && !rt.Attempt(q) {
			t.Fatalf("should allow retry after %d attempts", c.attempts)
		}
		if !c.allow && rt.Attempt(q) {
			t.Fatalf("should not allow retry after %d attempts", c.attempts)
		}
	}
}

// expectHosts makes sure that the next len(hostIDs) returned from iter is a permutation of hostIDs.
func expectHosts(t *testing.T, msg string, iter NextHost, hostIDs ...string) {
	t.Helper()

	expectedHostIDs := make(map[string]struct{}, len(hostIDs))
	for i := range hostIDs {
		expectedHostIDs[hostIDs[i]] = struct{}{}
	}

	expectedStr := func() string {
		keys := make([]string, 0, len(expectedHostIDs))
		for k := range expectedHostIDs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		return strings.Join(keys, ", ")
	}

	for len(expectedHostIDs) > 0 {
		host := iter()
		if host == nil || host.Info() == nil {
			t.Fatalf("%s: expected hostID one of {%s}, but got nil", msg, expectedStr())
		}
		hostID := host.Info().HostID()
		if _, ok := expectedHostIDs[hostID]; !ok {
			t.Fatalf("%s: expected host ID one of {%s}, but got %s", msg, expectedStr(), hostID)
		}
		delete(expectedHostIDs, hostID)
	}
}

func expectNoMoreHosts(t *testing.T, iter NextHost) {
	t.Helper()
	host := iter()
	if host == nil {
		// success
		return
	}
	info := host.Info()
	if info == nil {
		t.Fatalf("expected no more hosts, but got host with nil Info()")
		return
	}
	t.Fatalf("expected no more hosts, but got %s", info.HostID())
}

func TestHostPolicy_DCAwareRR(t *testing.T) {
	p := DCAwareRoundRobinPolicy("local")

	hosts := [...]*HostInfo{
		(&HostInfo{connectAddress: net.ParseIP("10.0.0.1")}).withIdentity("0", "local", ""),
		(&HostInfo{connectAddress: net.ParseIP("10.0.0.2")}).withIdentity("1", "local", ""),
		(&HostInfo{connectAddress: net.ParseIP("10.0.0.3")}).withIdentity("2", "remote", ""),
		(&HostInfo{connectAddress: net.ParseIP("10.0.0.4")}).withIdentity("3", "remote", ""),
	}

	for _, host := range hosts {
		p.AddHost(host)
	}

	got := make(map[string]bool, len(hosts))
	var dcs []string

	it := p.Pick(nil)
	for h := it(); h != nil; h = it() {
		id := h.Info().HostID()
		dc := h.Info().DataCenter()

		if got[id] {
			t.Fatalf("got duplicate host %s", id)
		}
		got[id] = true
		dcs = append(dcs, dc)
	}

	if len(got) != len(hosts) {
		t.Fatalf("expected %d hosts got %d", len(hosts), len(got))
	}

	var remote bool
	for _, dc := range dcs {
		if dc == "local" {
			if remote {
				t.Fatalf("got local dc after remote: %v", dcs)
			}
		} else {
			remote = true
		}
	}

}

// Tests of the token-aware host selection policy implementation with a
// DC aware round-robin host selection policy fallback
// with {"class": "NetworkTopologyStrategy", "a": 1, "b": 1, "c": 1} replication.
func TestHostPolicy_TokenAware(t *testing.T) {
	const keyspace = "myKeyspace"
	policy := TokenAwareHostPolicy(DCAwareRoundRobinPolicy("local"))
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return keyspace }

	query := &Query{}
	query.getKeyspace = func() string { return keyspace }

	iter := policy.Pick(nil)
	if iter == nil {
		t.Fatal("host iterator was nil")
	}
	actual := iter()
	if actual != nil {
		t.Fatalf("expected nil from iterator, but was %v", actual)
	}

	// set the hosts
	hosts := [...]*HostInfo{
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"05"}}).withIdentity("0", "remote1", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"10"}}).withIdentity("1", "local", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"15"}}).withIdentity("2", "remote2", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"20"}}).withIdentity("3", "remote1", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"25"}}).withIdentity("4", "local", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"30"}}).withIdentity("5", "remote2", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 7), tokens: []string{"35"}}).withIdentity("6", "remote1", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 8), tokens: []string{"40"}}).withIdentity("7", "local", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 9), tokens: []string{"45"}}).withIdentity("8", "remote2", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 10), tokens: []string{"50"}}).withIdentity("9", "remote1", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 11), tokens: []string{"55"}}).withIdentity("10", "local", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 12), tokens: []string{"60"}}).withIdentity("11", "remote2", ""),
	}
	for _, host := range hosts {
		policy.AddHost(host)
	}

	// the token ring is not setup without the partitioner, but the fallback
	// should work
	if actual := policy.Pick(nil)(); actual == nil {
		t.Fatal("expected to get host from fallback got nil")
	}

	query.RoutingKey([]byte("30"))
	if actual := policy.Pick(newInternalQuery(query, nil))(); actual == nil {
		t.Fatal("expected to get host from fallback got nil")
	}

	keyspaceMeta := &KeyspaceMetadata{
		Name:          keyspace,
		StrategyClass: "NetworkTopologyStrategy",
		StrategyOptions: map[string]interface{}{
			"class":   "NetworkTopologyStrategy",
			"local":   1,
			"remote1": 1,
			"remote2": 1,
		},
	}
	strategy := getStrategy(keyspaceMeta, nopLoggerSingleton)
	keyspaceMeta.placementStrategy = strategy
	policyInternal.getSchemaMeta = func() *schemaMeta {
		return &schemaMeta{
			keyspaceMeta: map[string]*KeyspaceMetadata{
				keyspace: keyspaceMeta,
			},
		}
	}

	policy.SetPartitioner("OrderedPartitioner")

	// The NetworkTopologyStrategy above should generate the following replicas.
	// It's handy to have as reference here.
	assertDeepEqual(t, "replicas", map[string]tokenRingReplicas{
		strategy.strategyKey(): {
			{orderedToken("05"), []*HostInfo{hosts[0], hosts[1], hosts[2]}},
			{orderedToken("10"), []*HostInfo{hosts[1], hosts[2], hosts[3]}},
			{orderedToken("15"), []*HostInfo{hosts[2], hosts[3], hosts[4]}},
			{orderedToken("20"), []*HostInfo{hosts[3], hosts[4], hosts[5]}},
			{orderedToken("25"), []*HostInfo{hosts[4], hosts[5], hosts[6]}},
			{orderedToken("30"), []*HostInfo{hosts[5], hosts[6], hosts[7]}},
			{orderedToken("35"), []*HostInfo{hosts[6], hosts[7], hosts[8]}},
			{orderedToken("40"), []*HostInfo{hosts[7], hosts[8], hosts[9]}},
			{orderedToken("45"), []*HostInfo{hosts[8], hosts[9], hosts[10]}},
			{orderedToken("50"), []*HostInfo{hosts[9], hosts[10], hosts[11]}},
			{orderedToken("55"), []*HostInfo{hosts[10], hosts[11], hosts[0]}},
			{orderedToken("60"), []*HostInfo{hosts[11], hosts[0], hosts[1]}},
		},
	}, policyInternal.getMetadataReadOnly().replicas)

	// now the token ring is configured
	query.RoutingKey([]byte("23"))
	iter = policy.Pick(newInternalQuery(query, nil))
	// first should be host with matching token from the local DC
	expectHosts(t, "matching token from local DC", iter, "4")
	// next are in non-deterministic order
	expectHosts(t, "rest", iter, "0", "1", "2", "3", "5", "6", "7", "8", "9", "10", "11")
	expectNoMoreHosts(t, iter)
}

// Tests of the token-aware host selection policy implementation with a
// DC aware round-robin host selection policy fallback
// with {"class": "NetworkTopologyStrategy", "a": 2, "b": 2, "c": 2} replication.
func TestHostPolicy_TokenAware_NetworkStrategy(t *testing.T) {
	const keyspace = "myKeyspace"
	policy := TokenAwareHostPolicy(DCAwareRoundRobinPolicy("local"), NonLocalReplicasFallback())
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return keyspace }

	query := &Query{}
	query.getKeyspace = func() string { return keyspace }

	iter := policy.Pick(nil)
	if iter == nil {
		t.Fatal("host iterator was nil")
	}
	actual := iter()
	if actual != nil {
		t.Fatalf("expected nil from iterator, but was %v", actual)
	}

	// set the hosts
	hosts := [...]*HostInfo{
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"05"}}).withIdentity("0", "remote1", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"10"}}).withIdentity("1", "local", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"15"}}).withIdentity("2", "remote2", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"20"}}).withIdentity("3", "remote1", ""), // 1
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"25"}}).withIdentity("4", "local", ""),   // 2
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"30"}}).withIdentity("5", "remote2", ""), // 3
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 7), tokens: []string{"35"}}).withIdentity("6", "remote1", ""), // 4
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 8), tokens: []string{"40"}}).withIdentity("7", "local", ""),   // 5
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 9), tokens: []string{"45"}}).withIdentity("8", "remote2", ""), // 6
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 10), tokens: []string{"50"}}).withIdentity("9", "remote1", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 11), tokens: []string{"55"}}).withIdentity("10", "local", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 12), tokens: []string{"60"}}).withIdentity("11", "remote2", ""),
	}
	for _, host := range hosts {
		policy.AddHost(host)
	}

	keyspaceMeta := &KeyspaceMetadata{
		Name:          keyspace,
		StrategyClass: "NetworkTopologyStrategy",
		StrategyOptions: map[string]interface{}{
			"class":   "NetworkTopologyStrategy",
			"local":   2,
			"remote1": 2,
			"remote2": 2,
		},
	}
	strategy := getStrategy(keyspaceMeta, nopLoggerSingleton)
	keyspaceMeta.placementStrategy = strategy
	policyInternal.getSchemaMeta = func() *schemaMeta {
		return &schemaMeta{
			keyspaceMeta: map[string]*KeyspaceMetadata{
				keyspace: keyspaceMeta,
			},
		}
	}

	policy.SetPartitioner("OrderedPartitioner")

	// The NetworkTopologyStrategy above should generate the following replicas.
	// It's handy to have as reference here.
	assertDeepEqual(t, "replicas", map[string]tokenRingReplicas{
		strategy.strategyKey(): {
			{orderedToken("05"), []*HostInfo{hosts[0], hosts[1], hosts[2], hosts[3], hosts[4], hosts[5]}},
			{orderedToken("10"), []*HostInfo{hosts[1], hosts[2], hosts[3], hosts[4], hosts[5], hosts[6]}},
			{orderedToken("15"), []*HostInfo{hosts[2], hosts[3], hosts[4], hosts[5], hosts[6], hosts[7]}},
			{orderedToken("20"), []*HostInfo{hosts[3], hosts[4], hosts[5], hosts[6], hosts[7], hosts[8]}},
			{orderedToken("25"), []*HostInfo{hosts[4], hosts[5], hosts[6], hosts[7], hosts[8], hosts[9]}},
			{orderedToken("30"), []*HostInfo{hosts[5], hosts[6], hosts[7], hosts[8], hosts[9], hosts[10]}},
			{orderedToken("35"), []*HostInfo{hosts[6], hosts[7], hosts[8], hosts[9], hosts[10], hosts[11]}},
			{orderedToken("40"), []*HostInfo{hosts[7], hosts[8], hosts[9], hosts[10], hosts[11], hosts[0]}},
			{orderedToken("45"), []*HostInfo{hosts[8], hosts[9], hosts[10], hosts[11], hosts[0], hosts[1]}},
			{orderedToken("50"), []*HostInfo{hosts[9], hosts[10], hosts[11], hosts[0], hosts[1], hosts[2]}},
			{orderedToken("55"), []*HostInfo{hosts[10], hosts[11], hosts[0], hosts[1], hosts[2], hosts[3]}},
			{orderedToken("60"), []*HostInfo{hosts[11], hosts[0], hosts[1], hosts[2], hosts[3], hosts[4]}},
		},
	}, policyInternal.getMetadataReadOnly().replicas)

	// now the token ring is configured
	query.RoutingKey([]byte("18"))
	iter = policy.Pick(newInternalQuery(query, nil))
	// first should be hosts with matching token from the local DC
	expectHosts(t, "matching token from local DC", iter, "4", "7")
	// rest should be hosts with matching token from remote DCs
	expectHosts(t, "matching token from remote DCs", iter, "3", "5", "6", "8")
	// followed by other hosts
	expectHosts(t, "rest", iter, "0", "1", "2", "9", "10", "11")
	expectNoMoreHosts(t, iter)
}

func TestHostPolicy_RackAwareRR(t *testing.T) {
	p := RackAwareRoundRobinPolicy("local", "b")

	hosts := [...]*HostInfo{
		(&HostInfo{connectAddress: net.ParseIP("10.0.0.1")}).withIdentity("0", "local", "a"),
		(&HostInfo{connectAddress: net.ParseIP("10.0.0.2")}).withIdentity("1", "local", "a"),
		(&HostInfo{connectAddress: net.ParseIP("10.0.0.3")}).withIdentity("2", "local", "b"),
		(&HostInfo{connectAddress: net.ParseIP("10.0.0.4")}).withIdentity("3", "local", "b"),
		(&HostInfo{connectAddress: net.ParseIP("10.0.0.5")}).withIdentity("4", "remote", "a"),
		(&HostInfo{connectAddress: net.ParseIP("10.0.0.6")}).withIdentity("5", "remote", "a"),
		(&HostInfo{connectAddress: net.ParseIP("10.0.0.7")}).withIdentity("6", "remote", "b"),
		(&HostInfo{connectAddress: net.ParseIP("10.0.0.8")}).withIdentity("7", "remote", "b"),
	}

	for _, host := range hosts {
		p.AddHost(host)
	}

	it := p.Pick(nil)

	// Must start with rack-local hosts
	expectHosts(t, "rack-local hosts", it, "3", "2")
	// Then dc-local hosts
	expectHosts(t, "dc-local hosts", it, "0", "1")
	// Then the remote hosts
	expectHosts(t, "remote hosts", it, "4", "5", "6", "7")
	expectNoMoreHosts(t, it)
}

// Tests of the token-aware host selection policy implementation with a
// DC & Rack aware round-robin host selection policy fallback
func TestHostPolicy_TokenAware_RackAware(t *testing.T) {
	const keyspace = "myKeyspace"
	policy := TokenAwareHostPolicy(RackAwareRoundRobinPolicy("local", "b"))
	policyWithFallback := TokenAwareHostPolicy(RackAwareRoundRobinPolicy("local", "b"), NonLocalReplicasFallback())

	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return keyspace }

	policyWithFallbackInternal := policyWithFallback.(*tokenAwareHostPolicy)
	policyWithFallbackInternal.getKeyspaceName = policyInternal.getKeyspaceName
	policyWithFallbackInternal.getKeyspaceMetadata = policyInternal.getKeyspaceMetadata

	query := &Query{}
	query.getKeyspace = func() string { return keyspace }

	iter := policy.Pick(nil)
	if iter == nil {
		t.Fatal("host iterator was nil")
	}
	actual := iter()
	if actual != nil {
		t.Fatalf("expected nil from iterator, but was %v", actual)
	}

	// set the hosts
	hosts := [...]*HostInfo{
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"05"}}).withIdentity("0", "remote", "a"),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"10"}}).withIdentity("1", "remote", "b"),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"15"}}).withIdentity("2", "local", "a"),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"20"}}).withIdentity("3", "local", "b"),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"25"}}).withIdentity("4", "remote", "a"),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"30"}}).withIdentity("5", "remote", "b"),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 7), tokens: []string{"35"}}).withIdentity("6", "local", "a"),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 8), tokens: []string{"40"}}).withIdentity("7", "local", "b"),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 9), tokens: []string{"45"}}).withIdentity("8", "remote", "a"),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 10), tokens: []string{"50"}}).withIdentity("9", "remote", "b"),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 11), tokens: []string{"55"}}).withIdentity("10", "local", "a"),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 12), tokens: []string{"60"}}).withIdentity("11", "local", "b"),
	}
	for _, host := range hosts {
		policy.AddHost(host)
		policyWithFallback.AddHost(host)
	}

	// the token ring is not setup without the partitioner, but the fallback
	// should work
	if actual := policy.Pick(nil)(); actual == nil {
		t.Fatal("expected to get host from fallback got nil")
	}

	query.RoutingKey([]byte("30"))
	if actual := policy.Pick(newInternalQuery(query, nil))(); actual == nil {
		t.Fatal("expected to get host from fallback got nil")
	}

	keyspaceMeta := &KeyspaceMetadata{
		Name:          keyspace,
		StrategyClass: "NetworkTopologyStrategy",
		StrategyOptions: map[string]interface{}{
			"class":  "NetworkTopologyStrategy",
			"local":  2,
			"remote": 2,
		},
	}
	strategy := getStrategy(keyspaceMeta, nopLoggerSingleton)
	keyspaceMeta.placementStrategy = strategy
	policyInternal.getSchemaMeta = func() *schemaMeta {
		return &schemaMeta{
			keyspaceMeta: map[string]*KeyspaceMetadata{
				keyspace: keyspaceMeta,
			},
		}
	}
	policyWithFallbackInternal.getSchemaMeta = policyInternal.getSchemaMeta

	policy.SetPartitioner("OrderedPartitioner")
	policyWithFallback.SetPartitioner("OrderedPartitioner")

	// The NetworkTopologyStrategy above should generate the following replicas.
	// It's handy to have as reference here.
	assertDeepEqual(t, "replicas", map[string]tokenRingReplicas{
		strategy.strategyKey(): {
			{orderedToken("05"), []*HostInfo{hosts[0], hosts[1], hosts[2], hosts[3]}},
			{orderedToken("10"), []*HostInfo{hosts[1], hosts[2], hosts[3], hosts[4]}},
			{orderedToken("15"), []*HostInfo{hosts[2], hosts[3], hosts[4], hosts[5]}},
			{orderedToken("20"), []*HostInfo{hosts[3], hosts[4], hosts[5], hosts[6]}},
			{orderedToken("25"), []*HostInfo{hosts[4], hosts[5], hosts[6], hosts[7]}},
			{orderedToken("30"), []*HostInfo{hosts[5], hosts[6], hosts[7], hosts[8]}},
			{orderedToken("35"), []*HostInfo{hosts[6], hosts[7], hosts[8], hosts[9]}},
			{orderedToken("40"), []*HostInfo{hosts[7], hosts[8], hosts[9], hosts[10]}},
			{orderedToken("45"), []*HostInfo{hosts[8], hosts[9], hosts[10], hosts[11]}},
			{orderedToken("50"), []*HostInfo{hosts[9], hosts[10], hosts[11], hosts[0]}},
			{orderedToken("55"), []*HostInfo{hosts[10], hosts[11], hosts[0], hosts[1]}},
			{orderedToken("60"), []*HostInfo{hosts[11], hosts[0], hosts[1], hosts[2]}},
		},
	}, policyInternal.getMetadataReadOnly().replicas)

	query.RoutingKey([]byte("23"))

	// now the token ring is configured
	// Test the policy with fallback
	iter = policyWithFallback.Pick(newInternalQuery(query, nil))

	// first should be host with matching token from the local DC & rack
	expectHosts(t, "matching token from local DC and local rack", iter, "7")
	// next should be host with matching token from local DC and other rack
	expectHosts(t, "matching token from local DC and non-local rack", iter, "6")
	// next should be hosts with matching token from other DC, in any order
	expectHosts(t, "matching token from non-local DC", iter, "4", "5")
	// then the local DC & rack that didn't match the token
	expectHosts(t, "non-matching token from local DC and local rack", iter, "3", "11")
	// then the local DC & other rack that didn't match the token
	expectHosts(t, "non-matching token from local DC and non-local rack", iter, "2", "10")
	// finally, the other DC that didn't match the token
	expectHosts(t, "non-matching token from non-local DC", iter, "0", "1", "8", "9")
	expectNoMoreHosts(t, iter)

	// Test the policy without fallback
	iter = policy.Pick(newInternalQuery(query, nil))

	// first should be host with matching token from the local DC & Rack
	expectHosts(t, "matching token from local DC and local rack", iter, "7")
	// next should be the other two hosts from local DC & rack
	expectHosts(t, "non-matching token local DC and local rack", iter, "3", "11")
	// then the three hosts from the local DC but other rack
	expectHosts(t, "local DC, non-local rack", iter, "2", "6", "10")
	// then the 6 hosts from the other DC
	expectHosts(t, "non-local DC", iter, "0", "1", "4", "5", "8", "9")
	expectNoMoreHosts(t, iter)
}

// TestHostPolicy_TokenAware_MultiKeyspace tests that token-aware routing works
// for queries to keyspaces other than the session's default keyspace.
func TestHostPolicy_TokenAware_MultiKeyspace(t *testing.T) {
	const sessionKeyspace = "ks1"
	const otherKeyspace = "ks2"

	policy := TokenAwareHostPolicy(RoundRobinHostPolicy())
	policyInternal := policy.(*tokenAwareHostPolicy)
	createKeyspaceMeta := func(name string) *KeyspaceMetadata {
		ksMeta := &KeyspaceMetadata{
			Name:          name,
			StrategyClass: "SimpleStrategy",
			StrategyOptions: map[string]interface{}{
				"class":              "SimpleStrategy",
				"replication_factor": 2,
			},
		}
		ksMeta.placementStrategy = getStrategy(ksMeta, nopLoggerSingleton)
		return ksMeta
	}

	sessionKeyspaceMeta := createKeyspaceMeta(sessionKeyspace)
	otherKeyspaceMeta := createKeyspaceMeta(otherKeyspace)
	policyInternal.getSchemaMeta = func() *schemaMeta {
		return &schemaMeta{
			keyspaceMeta: map[string]*KeyspaceMetadata{
				sessionKeyspace: sessionKeyspaceMeta,
				otherKeyspace:   otherKeyspaceMeta,
			},
		}
	}

	policy.SetPartitioner("OrderedPartitioner")

	// Add hosts with tokens
	hosts := [...]*HostInfo{
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"00"}}).withIdentity("0", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"25"}}).withIdentity("1", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50"}}).withIdentity("2", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"75"}}).withIdentity("3", "", ""),
	}
	for _, host := range &hosts {
		policy.AddHost(host)
	}

	// Verify both keyspaces are populated after SetPartitioner
	meta := policyInternal.getMetadataReadOnly()
	if meta.replicas[sessionKeyspaceMeta.placementStrategy.strategyKey()] == nil {
		t.Fatalf("session keyspace %s not in replica map", sessionKeyspace)
	}
	if meta.replicas[otherKeyspaceMeta.placementStrategy.strategyKey()] == nil {
		t.Fatalf("other keyspace %s not in replica map", otherKeyspace)
	}

	t.Run("SessionKeyspace", func(t *testing.T) {
		query := &Query{}
		query.getKeyspace = func() string { return sessionKeyspace }
		query.RoutingKey([]byte("20"))

		iter := policy.Pick(newInternalQuery(query, nil))

		// Should get token-aware hosts (token "20" → host with token "25")
		expectHosts(t, "session keyspace token-aware", iter, "1", "2")
		// Then fallback to remaining hosts
		expectHosts(t, "session keyspace fallback", iter, "0", "3")
		expectNoMoreHosts(t, iter)
	})

	t.Run("OtherKeyspace", func(t *testing.T) {
		query := &Query{}
		query.getKeyspace = func() string { return otherKeyspace }
		query.RoutingKey([]byte("60"))

		iter := policy.Pick(newInternalQuery(query, nil))

		// Should get token-aware hosts for otherKeyspace
		// token "60" → host with token "75"
		expectHosts(t, "other keyspace token-aware", iter, "3", "0")
		// Then fallback to remaining hosts
		expectHosts(t, "other keyspace fallback", iter, "1", "2")
		expectNoMoreHosts(t, iter)
	})
}

// TestHostPolicy_TokenAware_MultiKeyspace_WithShuffleReplicas tests that
// ShuffleReplicas option works correctly with proactively populated keyspaces.
func TestHostPolicy_TokenAware_MultiKeyspace_WithShuffleReplicas(t *testing.T) {
	const sessionKeyspace = "ks1"
	const otherKeyspace = "ks2"

	policy := TokenAwareHostPolicy(RoundRobinHostPolicy(), ShuffleReplicas())
	policyInternal := policy.(*tokenAwareHostPolicy)

	createKeyspaceMeta := func(name string) *KeyspaceMetadata {
		ksMeta := &KeyspaceMetadata{
			Name:          name,
			StrategyClass: "SimpleStrategy",
			StrategyOptions: map[string]interface{}{
				"class":              "SimpleStrategy",
				"replication_factor": 2,
			},
		}
		ksMeta.placementStrategy = getStrategy(ksMeta, nopLoggerSingleton)
		return ksMeta
	}

	sessionKeyspaceMeta := createKeyspaceMeta(sessionKeyspace)
	otherKeyspaceMeta := createKeyspaceMeta(otherKeyspace)
	policyInternal.getSchemaMeta = func() *schemaMeta {
		return &schemaMeta{
			keyspaceMeta: map[string]*KeyspaceMetadata{
				sessionKeyspace: sessionKeyspaceMeta,
				otherKeyspace:   otherKeyspaceMeta,
			},
		}
	}

	hosts := [...]*HostInfo{
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"00"}}).withIdentity("0", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"25"}}).withIdentity("1", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50"}}).withIdentity("2", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"75"}}).withIdentity("3", "", ""),
	}
	for _, host := range &hosts {
		policy.AddHost(host)
	}
	policy.SetPartitioner("OrderedPartitioner")

	// Query other keyspace with shuffle replicas enabled
	query := &Query{}
	query.getKeyspace = func() string { return otherKeyspace }
	query.RoutingKey([]byte("20"))

	// Execute Pick multiple times and collect first hosts
	firstHosts := make(map[string]int)
	for i := 0; i < 100; i++ {
		iter := policy.Pick(newInternalQuery(query, nil))
		host := iter()
		if host != nil {
			firstHosts[host.Info().HostID()]++
		}
	}

	// With ShuffleReplicas, we should see distribution across replicas
	// (not always the same host)
	if len(firstHosts) < 2 {
		t.Errorf("expected distribution across replicas with ShuffleReplicas, got only %d unique first hosts", len(firstHosts))
	}
}

// TestHostPolicy_TokenAware_TopologyChangeUpdatesAllKeyspaces verifies that
// when hosts are added or removed, replica maps are updated for ALL keyspaces,
// not just the session keyspace.
func TestHostPolicy_TokenAware_TopologyChangeUpdatesAllKeyspaces(t *testing.T) {
	const sessionKeyspace = "ks1"
	const otherKeyspace = "ks2"

	policy := TokenAwareHostPolicy(RoundRobinHostPolicy())
	policyInternal := policy.(*tokenAwareHostPolicy)

	createKeyspaceMeta := func(name string) *KeyspaceMetadata {
		ksMeta := &KeyspaceMetadata{
			Name:          name,
			StrategyClass: "SimpleStrategy",
			StrategyOptions: map[string]interface{}{
				"class":              "SimpleStrategy",
				"replication_factor": 2,
			},
		}
		ksMeta.placementStrategy = getStrategy(ksMeta, nopLoggerSingleton)
		return ksMeta
	}

	sessionKeyspaceMeta := createKeyspaceMeta(sessionKeyspace)
	otherKeyspaceMeta := createKeyspaceMeta(otherKeyspace)
	policyInternal.getSchemaMeta = func() *schemaMeta {
		return &schemaMeta{
			keyspaceMeta: map[string]*KeyspaceMetadata{
				sessionKeyspace: sessionKeyspaceMeta,
				otherKeyspace:   otherKeyspaceMeta,
			},
		}
	}

	// Initial topology: 3 hosts
	initialHosts := []*HostInfo{
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"00"}}).withIdentity("0", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"33"}}).withIdentity("1", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"66"}}).withIdentity("2", "", ""),
	}
	for _, host := range initialHosts {
		policy.AddHost(host)
	}
	policy.SetPartitioner("OrderedPartitioner")

	// Verify both keyspaces are in replica map
	meta := policyInternal.getMetadataReadOnly()
	if meta.replicas[sessionKeyspaceMeta.placementStrategy.strategyKey()] == nil {
		t.Fatalf("session keyspace %s not in replica map", sessionKeyspace)
	}
	if meta.replicas[otherKeyspaceMeta.placementStrategy.strategyKey()] == nil {
		t.Fatalf("other keyspace %s not in replica map", otherKeyspace)
	}

	// Test: Add a new host (topology change)
	t.Run("AddHost", func(t *testing.T) {
		newHost := (&HostInfo{
			connectAddress: net.IPv4(10, 0, 0, 4),
			tokens:         []string{"99"},
		}).withIdentity("3", "", "")
		policy.AddHost(newHost)

		// Verify: Get updated metadata
		metaAfterAdd := policyInternal.getMetadataReadOnly()

		// Check session keyspace was updated
		updatedSessionReplicas := metaAfterAdd.replicas[sessionKeyspaceMeta.placementStrategy.strategyKey()]
		if updatedSessionReplicas == nil {
			t.Fatal("session keyspace replica map is nil after AddHost")
		}

		// Check other keyspace was updated
		updatedOtherReplicas := metaAfterAdd.replicas[otherKeyspaceMeta.placementStrategy.strategyKey()]
		if updatedOtherReplicas == nil {
			t.Fatal("other keyspace replica map is nil after AddHost")
		}

		//Verify replica maps include new host
		// For session keyspace
		sessionHasNewHost := false
		for _, ht := range updatedSessionReplicas {
			for _, host := range ht.hosts {
				if host.HostID() == "3" {
					sessionHasNewHost = true
					break
				}
			}
		}
		if !sessionHasNewHost {
			t.Error("session keyspace replica map does not include new host")
		}

		// For other keyspace
		otherHasNewHost := false
		for _, ht := range updatedOtherReplicas {
			for _, host := range ht.hosts {
				if host.HostID() == "3" {
					otherHasNewHost = true
					break
				}
			}
		}
		if !otherHasNewHost {
			t.Error("other keyspace replica map does not include new host - replica map is STALE after topology change!")
		}
	})

	// Test: Remove host
	t.Run("RemoveHost", func(t *testing.T) {
		// Remove one of the original hosts
		hostToRemove := initialHosts[0]
		policy.RemoveHost(hostToRemove)

		metaAfterRemove := policyInternal.getMetadataReadOnly()

		// Verify session keyspace updated
		sessionReplicasAfterRemove := metaAfterRemove.replicas[sessionKeyspaceMeta.placementStrategy.strategyKey()]
		sessionStillHasHost := false
		for _, ht := range sessionReplicasAfterRemove {
			for _, host := range ht.hosts {
				if host.HostID() == "0" {
					sessionStillHasHost = true
					break
				}
			}
		}
		if sessionStillHasHost {
			t.Error("session keyspace still has removed host in replica map")
		}

		// Verify other keyspace updated
		otherReplicasAfterRemove := metaAfterRemove.replicas[otherKeyspaceMeta.placementStrategy.strategyKey()]
		otherStillHasHost := false
		for _, ht := range otherReplicasAfterRemove {
			for _, host := range ht.hosts {
				if host.HostID() == "0" {
					otherStillHasHost = true
					break
				}
			}
		}
		if otherStillHasHost {
			t.Error("other keyspace still has removed host in replica map - STALE after topology change!")
		}
	})
}

// shuffleTestKeyspace is shared by the shuffle regression tests below.
const shuffleTestKeyspace = "ks"

// setupShuffleTestPolicy wires a TokenAwareHostPolicy + SimpleStrategy keyspace
// of the given replication factor, with OrderedPartitioner so routing keys map
// directly to tokens. Returns the policy and a pre-built query targeting the
// shared keyspace -- callers add hosts and set RoutingKey on the query.
func setupShuffleTestPolicy(rf int, fallback HostSelectionPolicy, opts ...func(*tokenAwareHostPolicy)) (HostSelectionPolicy, *Query) {
	policy := TokenAwareHostPolicy(fallback, opts...)
	policyInternal := policy.(*tokenAwareHostPolicy)
	policyInternal.getKeyspaceName = func() string { return shuffleTestKeyspace }

	ksMeta := &KeyspaceMetadata{
		Name:          shuffleTestKeyspace,
		StrategyClass: "SimpleStrategy",
		StrategyOptions: map[string]interface{}{
			"class":              "SimpleStrategy",
			"replication_factor": rf,
		},
	}
	ksMeta.placementStrategy = getStrategy(ksMeta, nopLoggerSingleton)
	policyInternal.getSchemaMeta = func() *schemaMeta {
		return &schemaMeta{
			keyspaceMeta: map[string]*KeyspaceMetadata{shuffleTestKeyspace: ksMeta},
		}
	}

	query := &Query{}
	query.getKeyspace = func() string { return shuffleTestKeyspace }
	return policy, query
}

// firstHostDistribution drives policy.Pick(query) the given number of times
// and tallies how often each host appears as the first returned host. Fails
// the test if any iteration returns nil.
func firstHostDistribution(t *testing.T, policy HostSelectionPolicy, query *Query, iterations int) map[string]int {
	t.Helper()
	dist := make(map[string]int)
	for i := 0; i < iterations; i++ {
		iter := policy.Pick(newInternalQuery(query, nil))
		host := iter()
		if host == nil {
			t.Fatalf("expected non-nil first host on iteration %d", i)
		}
		dist[host.Info().HostID()]++
	}
	return dist
}

// TestHostPolicy_TokenAware_Shuffle_DownReplica verifies that when one of the
// replicas for a token is down, the remaining live replicas each receive
// roughly equal first-host traffic. Regression test: an earlier refactor
// rotated the starting offset across the raw replica list and then filtered
// out !IsUp hosts during iteration, which biased traffic toward whichever
// live replica immediately followed the down host (e.g., RF=3 [A,B,C] with
// A down → B receives 2/3 of first-host picks instead of 1/2). The rotation
// must happen over the filtered-live subset.
func TestHostPolicy_TokenAware_Shuffle_DownReplica(t *testing.T) {
	policy, query := setupShuffleTestPolicy(3, RoundRobinHostPolicy(), ShuffleReplicas())

	// 4 hosts at ring positions 10, 30, 50, 70. SimpleStrategy RF=3 on
	// OrderedPartitioner with routing key "05" walks the ring from token 10,
	// producing replicas [A@10, B@30, C@50].
	hosts := [...]*HostInfo{
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"10"}}).withIdentity("A", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"30"}}).withIdentity("B", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50"}}).withIdentity("C", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"70"}}).withIdentity("D", "", ""),
	}
	for _, host := range &hosts {
		policy.AddHost(host)
	}
	policy.SetPartitioner("OrderedPartitioner")

	// Mark A down. Live replicas for token "05" are now {B, C}.
	hosts[0].setState(NodeDown)
	query.RoutingKey([]byte("05"))

	const iterations = 600
	firstHosts := firstHostDistribution(t, policy, query, iterations)

	if firstHosts["A"] != 0 {
		t.Errorf("down replica A should never be first, got %d picks", firstHosts["A"])
	}
	// Rotation over live-locals gives exactly 50/50 (counter cycles 0,1,0,1).
	// Loose lower bound of 40% leaves slack but still catches the old
	// rotate-over-raw-list bias (which produces a 67/33 split).
	minRequired := iterations * 4 / 10
	if firstHosts["B"] < minRequired {
		t.Errorf("expected B to be first at least %d times, got %d (distribution: %v)",
			minRequired, firstHosts["B"], firstHosts)
	}
	if firstHosts["C"] < minRequired {
		t.Errorf("expected C to be first at least %d times, got %d (distribution: %v)",
			minRequired, firstHosts["C"], firstHosts)
	}
}

// TestHostPolicy_TokenAware_Shuffle_ClusteredLocalReplicas verifies that
// when local replicas cluster next to each other in the raw replica list
// (and remote replicas are filtered out by tier), each local replica still
// receives roughly equal first-host traffic. Regression test: rotating over
// the raw list and filtering during iteration biases toward whichever local
// replica immediately follows runs of remote replicas (e.g., [L,L,R,R] →
// 3:1 skew toward L0). The rotation must happen over the filtered-local
// subset.
func TestHostPolicy_TokenAware_Shuffle_ClusteredLocalReplicas(t *testing.T) {
	policy, query := setupShuffleTestPolicy(4, DCAwareRoundRobinPolicy("local"), ShuffleReplicas())

	// 6 hosts on the ring at tokens 10..60, with the first two in the local
	// DC and the rest remote. SimpleStrategy walks the ring without DC
	// awareness, so for routing key "05" (ring walk starts at token 10) and
	// RF=4 the replica list is [L0, L1, R0, R1] -- locals clustered at front.
	// DCAware fallback classifies L0, L1 as tier 0 and R0, R1 as tier 1.
	hosts := [...]*HostInfo{
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"10"}}).withIdentity("L0", "local", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"20"}}).withIdentity("L1", "local", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"30"}}).withIdentity("R0", "remote", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"40"}}).withIdentity("R1", "remote", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"50"}}).withIdentity("R2", "remote", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"60"}}).withIdentity("R3", "remote", ""),
	}
	for _, host := range &hosts {
		policy.AddHost(host)
	}
	policy.SetPartitioner("OrderedPartitioner")
	query.RoutingKey([]byte("05"))

	const iterations = 600
	firstHosts := firstHostDistribution(t, policy, query, iterations)

	// Remote replicas must never be picked first (they are tier 1 and
	// nonLocalReplicasFallback is not enabled).
	for _, id := range []string{"R0", "R1", "R2", "R3"} {
		if firstHosts[id] != 0 {
			t.Errorf("remote replica %s should never be first, got %d picks", id, firstHosts[id])
		}
	}
	// Rotation over [L0, L1] gives exactly 50/50; old rotate-over-raw-list
	// produces ~75/25 (L0 wins for 3 of 4 starts).
	minRequired := iterations * 4 / 10
	if firstHosts["L0"] < minRequired {
		t.Errorf("expected L0 to be first at least %d times, got %d (distribution: %v)",
			minRequired, firstHosts["L0"], firstHosts)
	}
	if firstHosts["L1"] < minRequired {
		t.Errorf("expected L1 to be first at least %d times, got %d (distribution: %v)",
			minRequired, firstHosts["L1"], firstHosts)
	}
}

// TestHostPolicy_TokenAware_Shuffle_RemoteFallback verifies that when local
// replicas are down/exhausted and NonLocalReplicasFallback is enabled, the
// remote-tier replicas also rotate across queries instead of always returning
// the same first remote replica. Regression test: an earlier version of the
// rotation refactor only rotated localLive and consumed remote tiers from
// index 0, so during a local-DC outage every query landed on the same remote
// coordinator -- the exact failover case where load distribution matters most.
func TestHostPolicy_TokenAware_Shuffle_RemoteFallback(t *testing.T) {
	policy, query := setupShuffleTestPolicy(4,
		DCAwareRoundRobinPolicy("local"),
		ShuffleReplicas(),
		NonLocalReplicasFallback(),
	)

	// SimpleStrategy RF=4, routing key "05" → replicas [L0, L1, R0, R1] by
	// ring walk. With DCAware("local") fallback: L0/L1 are tier 0, R0/R1 are
	// tier 1 and stashed for non-local fallback.
	hosts := [...]*HostInfo{
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"10"}}).withIdentity("L0", "local", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"20"}}).withIdentity("L1", "local", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"30"}}).withIdentity("R0", "remote", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"40"}}).withIdentity("R1", "remote", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"50"}}).withIdentity("R2", "remote", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"60"}}).withIdentity("R3", "remote", ""),
	}
	for _, host := range &hosts {
		policy.AddHost(host)
	}
	policy.SetPartitioner("OrderedPartitioner")

	// Simulate a local-DC outage: both local replicas down. The first host
	// returned by Pick must come from the remote replica fallback (R0/R1).
	hosts[0].setState(NodeDown)
	hosts[1].setState(NodeDown)
	query.RoutingKey([]byte("05"))

	const iterations = 600
	firstHosts := firstHostDistribution(t, policy, query, iterations)

	// L0, L1 are down → never first.
	for _, id := range []string{"L0", "L1"} {
		if firstHosts[id] != 0 {
			t.Errorf("down replica %s should never be first, got %d picks", id, firstHosts[id])
		}
	}
	// R2 and R3 are not replicas of the routing key's token; they should not
	// appear via the token-aware fallback path.
	for _, id := range []string{"R2", "R3"} {
		if firstHosts[id] != 0 {
			t.Errorf("non-replica %s should never be first, got %d picks", id, firstHosts[id])
		}
	}
	// Rotation over [R0, R1] gives exactly 50/50; without remote-tier
	// rotation, R0 wins every Pick.
	minRequired := iterations * 4 / 10
	if firstHosts["R0"] < minRequired {
		t.Errorf("expected R0 to be first at least %d times, got %d (distribution: %v)",
			minRequired, firstHosts["R0"], firstHosts)
	}
	if firstHosts["R1"] < minRequired {
		t.Errorf("expected R1 to be first at least %d times, got %d (distribution: %v)",
			minRequired, firstHosts["R1"], firstHosts)
	}
}

// TestHostPolicy_TokenAware_Shuffle_AllReplicasDown verifies that when every
// replica for a token is down and NonLocalReplicasFallback is disabled, the
// iterator falls through to the wrapped fallback policy. This exercises the
// `t.fallback.Pick(qry)` tail path and the `used[]` dedup that filters
// already-returned hosts from the fallback's output -- neither is touched by
// the other shuffle regression tests.
func TestHostPolicy_TokenAware_Shuffle_AllReplicasDown(t *testing.T) {
	policy, query := setupShuffleTestPolicy(3, RoundRobinHostPolicy(), ShuffleReplicas())

	// SimpleStrategy RF=3, routing key "05" → replicas [A@10, B@30, C@50].
	// D is not a replica for this token but should still appear via the
	// RoundRobin fallback once the (now-dead) replicas are exhausted.
	hosts := [...]*HostInfo{
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"10"}}).withIdentity("A", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"30"}}).withIdentity("B", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50"}}).withIdentity("C", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"70"}}).withIdentity("D", "", ""),
	}
	for _, host := range &hosts {
		policy.AddHost(host)
	}
	policy.SetPartitioner("OrderedPartitioner")

	// Mark all three replicas down so localLive is empty.
	hosts[0].setState(NodeDown)
	hosts[1].setState(NodeDown)
	hosts[2].setState(NodeDown)
	query.RoutingKey([]byte("05"))

	// Drive Pick once and walk the iterator to completion. Expectations:
	//   - No down replica is ever returned.
	//   - D (the non-replica) is returned at most once (via the fallback).
	//   - No host is returned twice (the `used[]` map must deduplicate
	//     between the empty replica iterator and the fallback's output).
	iter := policy.Pick(newInternalQuery(query, nil))
	seen := make(map[string]int)
	for h := iter(); h != nil; h = iter() {
		seen[h.Info().HostID()]++
	}

	for _, id := range []string{"A", "B", "C"} {
		if seen[id] != 0 {
			t.Errorf("down replica %s should not be returned, got %d times", id, seen[id])
		}
	}
	if seen["D"] != 1 {
		t.Errorf("expected fallback host D returned exactly once, got %d times (seen=%v)", seen["D"], seen)
	}
}

// TestHostPolicy_TokenAware_Shuffle_EmptyMiddleTier verifies that when an
// intermediate remote tier is empty (e.g. RackAware with replicas in the
// local rack and a remote DC but none in other local racks), the iterator
// correctly advances past the empty tier instead of halting. Regression
// test for a pre-existing bug in the prior remote-tier loop guard
// (for j < len(remote) && k < len(remote[j])), which would terminate the
// loop the first time it saw a zero-length tier and silently drop later
// non-empty tiers.
func TestHostPolicy_TokenAware_Shuffle_EmptyMiddleTier(t *testing.T) {
	policy, query := setupShuffleTestPolicy(3,
		RackAwareRoundRobinPolicy("local", "b"),
		NonLocalReplicasFallback(),
	)

	// RF=3 SimpleStrategy ring walk for routing key "05" picks [A, B, C]:
	//   A = local DC, rack b   (tier 0)
	//   B = local DC, rack b   (tier 0)
	//   C = remote DC          (tier 2)
	//   D is local DC, rack a but is NOT in this token's replica set.
	// So tier 1 (local DC, other rack) is empty within the replica set.
	hosts := [...]*HostInfo{
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"10"}}).withIdentity("A", "local", "b"),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"20"}}).withIdentity("B", "local", "b"),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"30"}}).withIdentity("C", "remote", "a"),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"40"}}).withIdentity("D", "local", "a"),
	}
	for _, host := range &hosts {
		policy.AddHost(host)
	}
	policy.SetPartitioner("OrderedPartitioner")

	// Take both tier-0 replicas down so the iterator must reach into the
	// remote fallback to find a coordinator.
	hosts[0].setState(NodeDown)
	hosts[1].setState(NodeDown)
	query.RoutingKey([]byte("05"))

	iter := policy.Pick(newInternalQuery(query, nil))
	first := iter()
	if first == nil {
		t.Fatal("expected non-nil host -- iterator halted on empty tier 1 instead of advancing to tier 2")
	}
	// C is the only tier-2 replica; it must be returned via the token-aware
	// remote-fallback path, ahead of any non-replica host from the rack-aware
	// policy fallback (which would otherwise come from the closure tail).
	if id := first.Info().HostID(); id != "C" {
		t.Errorf("expected remote replica C from tier 2, got %s (likely returned via policy fallback because the empty-tier guard skipped tier 2)", id)
	}
}

// TestHostPolicy_TokenAware_Shuffle_CrossPolicyIndependence verifies that
// two TokenAwareHostPolicy instances constructed back-to-back rotate
// independently. The rotation counter is seeded with rand.Uint64() at
// construction so multiple sessions or co-located policies do not all
// start at the same offset and produce identical pick sequences.
func TestHostPolicy_TokenAware_Shuffle_CrossPolicyIndependence(t *testing.T) {
	// Build N independent policies, take one Pick from each, and check that
	// the first-host distribution is not concentrated on a single host. With
	// 3 live replicas and N=60 policies, a deterministic seed would give 60
	// of one host and 0 of the other two; random seeding spreads the load.
	const (
		numPolicies = 60
		minDistinct = 2 // at least 2 of 3 replicas chosen as first across policies
	)
	hosts := []*HostInfo{
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"10"}}).withIdentity("A", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"30"}}).withIdentity("B", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50"}}).withIdentity("C", "", ""),
		(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"70"}}).withIdentity("D", "", ""),
	}

	firstHosts := make(map[string]int)
	for i := 0; i < numPolicies; i++ {
		policy, query := setupShuffleTestPolicy(3, RoundRobinHostPolicy())
		for _, h := range hosts {
			policy.AddHost(h)
		}
		policy.SetPartitioner("OrderedPartitioner")
		query.RoutingKey([]byte("05"))

		iter := policy.Pick(newInternalQuery(query, nil))
		h := iter()
		if h == nil {
			t.Fatalf("policy %d: unexpected nil host", i)
		}
		firstHosts[h.Info().HostID()]++
	}
	if len(firstHosts) < minDistinct {
		t.Errorf("expected at least %d distinct first hosts across %d policies (independent seeding), got %d: %v",
			minDistinct, numPolicies, len(firstHosts), firstHosts)
	}
}

// benchmarkShufflePolicyWith builds a token-aware policy with shuffle enabled
// over the given fallback, modelling a production-scale workload: 100 hosts
// with RF=5, so each token has 5 replicas.
// hostFn builds host i.
// The routing key is fixed so every Pick targets the same 5-host replica set,
// the steady-state hot path.
//
// The fallback decides how much of each HostInfo the classify loop reads:
// RoundRobinHostPolicy never touches the host, DCAwareRoundRobinPolicy reads
// DataCenter(), and RackAwareRoundRobinPolicy reads the data center and the
// rack through HostTier.
// Before the identity snapshot those were one and two RLocks per replica; now
// they are one atomic load.
func benchmarkShufflePolicyWith(b *testing.B, fallback HostSelectionPolicy, hostFn func(i int) *HostInfo) (HostSelectionPolicy, *internalQuery) {
	b.Helper()
	const (
		numHosts = 100
		rf       = 5
	)
	policy, query := setupShuffleTestPolicy(rf, fallback, ShuffleReplicas())
	for i := 0; i < numHosts; i++ {
		policy.AddHost(hostFn(i))
	}
	policy.SetPartitioner("OrderedPartitioner")
	// Routing key "005" walks the ring from token "005", yielding 5 replicas
	// at indices 5..9.
	query.RoutingKey([]byte("005"))
	return policy, newInternalQuery(query, nil)
}

// benchmarkShuffleHost builds a host with no DC and no rack,
// so it is only meaningful with RoundRobinHostPolicy,
// whose IsLocal is unconditionally true and never reads the host:
// that is what keeps all 5 replicas in tier 0.
// A topology-aware fallback would classify these hosts as remote
// (dcAwareRR.IsLocal compares "" against its local DC, rackAwareRR.HostTier
// returns 2) -- use benchmarkShuffleTopologyHost there.
func benchmarkShuffleHost(i int) *HostInfo {
	return (&HostInfo{
		connectAddress: net.IPv4(10, 0, byte(i/256), byte(i%256)),
		tokens:         []string{fmt.Sprintf("%03d", i)},
	}).withIdentity(fmt.Sprintf("%03d", i), "", "")
}

// benchmarkShuffleTopologyHost builds a host spread over 2 DCs x 2 racks,
// roughly 25 hosts per combination: DC flips every 2 ring positions, rack
// every 4.
// For the fixed routing key the 5 replicas at indices 5..9 land in dc1/r2,
// dc2/r2, dc2/r2, dc1/r1, dc1/r1 -- so against
// RackAwareRoundRobinPolicy("dc1", "r1") all three tiers are exercised and
// tier 0 holds more than one host (the rotation branch),
// and against DCAwareRoundRobinPolicy("dc1") three replicas are local.
// Spanning both DCs is what makes HostTier read rack as well as DC.
func benchmarkShuffleTopologyHost(i int) *HostInfo {
	h := benchmarkShuffleHost(i)
	return h.withIdentity(
		h.HostID(),
		[...]string{"dc1", "dc2"}[(i/2)%2],
		[...]string{"r1", "r2"}[(i/4)%2],
	)
}

// BenchmarkTokenAwareHostPolicy_PickShuffleSerial measures per-Pick cost on a
// single goroutine. Mostly captures allocation/classification overhead -- the
// global RNG mutex from the prior implementation is uncontended here, so this
// benchmark is the *least* favorable comparison for the lock-free rotation.
func BenchmarkTokenAwareHostPolicy_PickShuffleSerial(b *testing.B) {
	policy, iq := benchmarkShufflePolicyWith(b, RoundRobinHostPolicy(), benchmarkShuffleHost)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := policy.Pick(iq)
		if iter() == nil {
			b.Fatal("unexpected nil host")
		}
	}
}

// BenchmarkTokenAwareHostPolicy_PickShuffleParallel measures per-Pick cost
// under contention from GOMAXPROCS goroutines. This is the load shape the
// refactor targets: every concurrent query used to serialize on the global
// mutRandr mutex inside shuffleHosts.
func BenchmarkTokenAwareHostPolicy_PickShuffleParallel(b *testing.B) {
	policy, iq := benchmarkShufflePolicyWith(b, RoundRobinHostPolicy(), benchmarkShuffleHost)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			iter := policy.Pick(iq)
			iter()
		}
	})
}

// BenchmarkTokenAwareHostPolicy_PickShuffleRackAwareSerial measures per-Pick
// cost when the fallback is rack-aware,
// so the classify loop calls HostTier on every replica before IsUp().
// This was the Pick shape most exposed to the HostInfo lock: HostTier took two
// RLocks per replica and IsUp a third.
// HostTier is now one identity load and IsUp one atomic load.
func BenchmarkTokenAwareHostPolicy_PickShuffleRackAwareSerial(b *testing.B) {
	policy, iq := benchmarkShufflePolicyWith(b, RackAwareRoundRobinPolicy("dc1", "r1"), benchmarkShuffleTopologyHost)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := policy.Pick(iq)
		if iter() == nil {
			b.Fatal("unexpected nil host")
		}
	}
}

// BenchmarkTokenAwareHostPolicy_PickShuffleRackAwareParallel measures the same
// rack-aware Pick under contention from GOMAXPROCS goroutines.
// It is the primary before/after instrument for the identity snapshot: the two
// independent RLocks inside HostTier should collapse to one lock-free Load.
func BenchmarkTokenAwareHostPolicy_PickShuffleRackAwareParallel(b *testing.B) {
	policy, iq := benchmarkShufflePolicyWith(b, RackAwareRoundRobinPolicy("dc1", "r1"), benchmarkShuffleTopologyHost)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			iter := policy.Pick(iq)
			iter()
		}
	})
}

// BenchmarkTokenAwareHostPolicy_PickShuffleDCAwareSerial measures per-Pick
// cost when the fallback is DC-aware,
// so the classify loop calls IsLocal -- one DataCenter() read -- per replica.
// It sits between the round-robin and rack-aware benchmarks and isolates the
// cost of a single topology read.
func BenchmarkTokenAwareHostPolicy_PickShuffleDCAwareSerial(b *testing.B) {
	policy, iq := benchmarkShufflePolicyWith(b, DCAwareRoundRobinPolicy("dc1"), benchmarkShuffleTopologyHost)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := policy.Pick(iq)
		if iter() == nil {
			b.Fatal("unexpected nil host")
		}
	}
}

// BenchmarkTokenAwareHostPolicy_PickShuffleDCAwareParallel measures the same
// DC-aware Pick under contention from GOMAXPROCS goroutines.
// Before the identity snapshot the shared DataCenter() RLock serialized every
// concurrent classify loop on one cache line; now DataCenter() is a lock-free
// load and the benchmark shows what that removed.
func BenchmarkTokenAwareHostPolicy_PickShuffleDCAwareParallel(b *testing.B) {
	policy, iq := benchmarkShufflePolicyWith(b, DCAwareRoundRobinPolicy("dc1"), benchmarkShuffleTopologyHost)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			iter := policy.Pick(iq)
			iter()
		}
	})
}

// reproArmableWarnPanicLogger is a logger whose first Warning after arming panics.
// It models an application logger that fails on one call and not on the next.
type armableWarnPanicLogger struct {
	StructuredLogger
	armed  atomic.Bool
	panics atomic.Int32
}

func (l *armableWarnPanicLogger) Warning(msg string, fields ...LogField) {
	if l.armed.CompareAndSwap(true, false) {
		l.panics.Add(1)
		panic("test: logger panic in resetTokenRing")
	}
	l.StructuredLogger.Warning(msg, fields...)
}

// lockOrderFallbackPolicy records whether the token-aware policy's mutex was held
// when the fallback policy was entered. The token-aware policy has always called
// its fallback outside t.mu; a function-level deferred unlock would invert that.
type lockOrderFallbackPolicy struct {
	HostSelectionPolicy
	policy *tokenAwareHostPolicy
	under  atomic.Bool
	calls  atomic.Int32
}

func (f *lockOrderFallbackPolicy) observe() {
	f.calls.Add(1)
	if f.policy.mu.TryLock() {
		f.policy.mu.Unlock()
		return
	}
	f.under.Store(true)
}

func (f *lockOrderFallbackPolicy) AddHost(host *HostInfo) {
	f.observe()
	f.HostSelectionPolicy.AddHost(host)
}

func (f *lockOrderFallbackPolicy) RemoveHost(host *HostInfo) {
	f.observe()
	f.HostSelectionPolicy.RemoveHost(host)
}

// newPanicLoggerTokenAwarePolicy builds a token-aware policy over fallback with an
// unsupported partitioner already set, so that every later resetTokenRing reaches
// the failure Warning. The logger is returned disarmed.
func newPanicLoggerTokenAwarePolicy(fallback HostSelectionPolicy) (*tokenAwareHostPolicy, *armableWarnPanicLogger) {
	logger := &armableWarnPanicLogger{StructuredLogger: &defaultLogger{}}
	p := TokenAwareHostPolicy(fallback).(*tokenAwareHostPolicy)
	p.logger = logger
	p.SetPartitioner("com.example.UnsupportedPartitioner")
	return p, logger
}

// mustReturnWithin runs fn on its own goroutine and fails the test if it has not
// returned within the bounded wait. A goroutine left blocked on a stranded mutex
// is leaked deliberately: the test has already failed at that point.
func mustReturnWithin(t *testing.T, name string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s blocked for 2s after the logger panicked", name)
	}
}

// TestTokenAware_LogPanicDoesNotStrandPolicyMutex is the inversion of
// _repro/zz_repro_round6_test.go's TestRepro_TokenAwareLogPanicStrandsPolicyMutex
// (F-pol-1). AddHost, AddHosts and RemoveHost called resetTokenRing under t.mu and
// released it with a plain Unlock, so one panic out of the failure Warning stranded
// the mutex and every later topology update blocked forever.
func TestTokenAware_LogPanicDoesNotStrandPolicyMutex(t *testing.T) {
	h1 := (&HostInfo{connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"00"}}).withIdentity("h1", "", "")
	h2 := (&HostInfo{connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"25"}}).withIdentity("h2", "", "")

	t.Run("the mutex survives a panicking warning", func(t *testing.T) {
		p, logger := newPanicLoggerTokenAwarePolicy(RoundRobinHostPolicy())

		logger.armed.Store(true)
		func() {
			// Whether the panic escapes AddHost is the next sub-test's subject;
			// this one is only about what the mutex is in afterwards.
			defer func() {
				if r := recover(); r != nil {
					t.Logf("AddHost propagated the logger panic: %v", r)
				}
			}()
			p.AddHost(h1)
		}()
		if got := logger.panics.Load(); got != 1 {
			t.Fatalf("the logger panicked %d times, want exactly 1", got)
		}

		mustReturnWithin(t, "a later AddHost", func() { p.AddHost(h2) })
		mustReturnWithin(t, "a later RemoveHost", func() { p.RemoveHost(h1) })
		mustReturnWithin(t, "a later SetPartitioner", func() { p.SetPartitioner("com.example.OtherUnsupportedPartitioner") })

		// The ledger's split: these three never took t.mu and are the control.
		mustReturnWithin(t, "HostUp/HostDown/Pick", func() {
			p.HostUp(h2)
			p.HostDown(h2)
			_ = p.Pick(nil)
		})
	})

	t.Run("the panicking warning is isolated", func(t *testing.T) {
		p, logger := newPanicLoggerTokenAwarePolicy(RoundRobinHostPolicy())

		logger.armed.Store(true)
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("AddHosts panicked: %v", r)
				}
			}()
			p.AddHosts([]*HostInfo{h1, h2})
		}()
		if got := logger.panics.Load(); got != 1 {
			t.Fatalf("the logger panicked %d times, want exactly 1", got)
		}
	})

	t.Run("the fallback is still called outside t.mu", func(t *testing.T) {
		fallback := &lockOrderFallbackPolicy{HostSelectionPolicy: RoundRobinHostPolicy()}
		p, _ := newPanicLoggerTokenAwarePolicy(fallback)
		fallback.policy = p

		p.AddHost(h1)
		p.AddHosts([]*HostInfo{h2})
		p.RemoveHost(h1)

		if got := fallback.calls.Load(); got != 3 {
			t.Fatalf("the fallback was entered %d times, want 3", got)
		}
		if fallback.under.Load() {
			t.Fatal("fallback called under t.mu")
		}
	})
}
