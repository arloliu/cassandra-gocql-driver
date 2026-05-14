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
	"testing"
	"time"
)

// Tests of the round-robin host selection policy implementation
func TestRoundRobbin(t *testing.T) {
	policy := RoundRobinHostPolicy()

	hosts := [...]*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(0, 0, 0, 1)},
		{hostId: "1", connectAddress: net.IPv4(0, 0, 0, 2)},
	}

	for _, host := range hosts {
		policy.AddHost(host)
	}

	got := make(map[string]bool)
	it := policy.Pick(nil)
	for h := it(); h != nil; h = it() {
		id := h.Info().hostId
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
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"00"}},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"25"}},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50"}},
		{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"75"}},
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

	host := &HostInfo{hostId: "host-1"}
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
		{hostId: "0", connectAddress: net.ParseIP("10.0.0.1"), dataCenter: "local"},
		{hostId: "1", connectAddress: net.ParseIP("10.0.0.2"), dataCenter: "local"},
		{hostId: "2", connectAddress: net.ParseIP("10.0.0.3"), dataCenter: "remote"},
		{hostId: "3", connectAddress: net.ParseIP("10.0.0.4"), dataCenter: "remote"},
	}

	for _, host := range hosts {
		p.AddHost(host)
	}

	got := make(map[string]bool, len(hosts))
	var dcs []string

	it := p.Pick(nil)
	for h := it(); h != nil; h = it() {
		id := h.Info().hostId
		dc := h.Info().dataCenter

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
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"05"}, dataCenter: "remote1"},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"10"}, dataCenter: "local"},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"15"}, dataCenter: "remote2"},
		{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"20"}, dataCenter: "remote1"},
		{hostId: "4", connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"25"}, dataCenter: "local"},
		{hostId: "5", connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"30"}, dataCenter: "remote2"},
		{hostId: "6", connectAddress: net.IPv4(10, 0, 0, 7), tokens: []string{"35"}, dataCenter: "remote1"},
		{hostId: "7", connectAddress: net.IPv4(10, 0, 0, 8), tokens: []string{"40"}, dataCenter: "local"},
		{hostId: "8", connectAddress: net.IPv4(10, 0, 0, 9), tokens: []string{"45"}, dataCenter: "remote2"},
		{hostId: "9", connectAddress: net.IPv4(10, 0, 0, 10), tokens: []string{"50"}, dataCenter: "remote1"},
		{hostId: "10", connectAddress: net.IPv4(10, 0, 0, 11), tokens: []string{"55"}, dataCenter: "local"},
		{hostId: "11", connectAddress: net.IPv4(10, 0, 0, 12), tokens: []string{"60"}, dataCenter: "remote2"},
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
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"05"}, dataCenter: "remote1"},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"10"}, dataCenter: "local"},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"15"}, dataCenter: "remote2"},
		{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"20"}, dataCenter: "remote1"}, // 1
		{hostId: "4", connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"25"}, dataCenter: "local"},   // 2
		{hostId: "5", connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"30"}, dataCenter: "remote2"}, // 3
		{hostId: "6", connectAddress: net.IPv4(10, 0, 0, 7), tokens: []string{"35"}, dataCenter: "remote1"}, // 4
		{hostId: "7", connectAddress: net.IPv4(10, 0, 0, 8), tokens: []string{"40"}, dataCenter: "local"},   // 5
		{hostId: "8", connectAddress: net.IPv4(10, 0, 0, 9), tokens: []string{"45"}, dataCenter: "remote2"}, // 6
		{hostId: "9", connectAddress: net.IPv4(10, 0, 0, 10), tokens: []string{"50"}, dataCenter: "remote1"},
		{hostId: "10", connectAddress: net.IPv4(10, 0, 0, 11), tokens: []string{"55"}, dataCenter: "local"},
		{hostId: "11", connectAddress: net.IPv4(10, 0, 0, 12), tokens: []string{"60"}, dataCenter: "remote2"},
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
		{hostId: "0", connectAddress: net.ParseIP("10.0.0.1"), dataCenter: "local", rack: "a"},
		{hostId: "1", connectAddress: net.ParseIP("10.0.0.2"), dataCenter: "local", rack: "a"},
		{hostId: "2", connectAddress: net.ParseIP("10.0.0.3"), dataCenter: "local", rack: "b"},
		{hostId: "3", connectAddress: net.ParseIP("10.0.0.4"), dataCenter: "local", rack: "b"},
		{hostId: "4", connectAddress: net.ParseIP("10.0.0.5"), dataCenter: "remote", rack: "a"},
		{hostId: "5", connectAddress: net.ParseIP("10.0.0.6"), dataCenter: "remote", rack: "a"},
		{hostId: "6", connectAddress: net.ParseIP("10.0.0.7"), dataCenter: "remote", rack: "b"},
		{hostId: "7", connectAddress: net.ParseIP("10.0.0.8"), dataCenter: "remote", rack: "b"},
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
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"05"}, dataCenter: "remote", rack: "a"},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"10"}, dataCenter: "remote", rack: "b"},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"15"}, dataCenter: "local", rack: "a"},
		{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"20"}, dataCenter: "local", rack: "b"},
		{hostId: "4", connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"25"}, dataCenter: "remote", rack: "a"},
		{hostId: "5", connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"30"}, dataCenter: "remote", rack: "b"},
		{hostId: "6", connectAddress: net.IPv4(10, 0, 0, 7), tokens: []string{"35"}, dataCenter: "local", rack: "a"},
		{hostId: "7", connectAddress: net.IPv4(10, 0, 0, 8), tokens: []string{"40"}, dataCenter: "local", rack: "b"},
		{hostId: "8", connectAddress: net.IPv4(10, 0, 0, 9), tokens: []string{"45"}, dataCenter: "remote", rack: "a"},
		{hostId: "9", connectAddress: net.IPv4(10, 0, 0, 10), tokens: []string{"50"}, dataCenter: "remote", rack: "b"},
		{hostId: "10", connectAddress: net.IPv4(10, 0, 0, 11), tokens: []string{"55"}, dataCenter: "local", rack: "a"},
		{hostId: "11", connectAddress: net.IPv4(10, 0, 0, 12), tokens: []string{"60"}, dataCenter: "local", rack: "b"},
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
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"00"}},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"25"}},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50"}},
		{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"75"}},
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
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"00"}},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"25"}},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50"}},
		{hostId: "3", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"75"}},
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
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"00"}},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"33"}},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"66"}},
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
		newHost := &HostInfo{
			hostId:         "3",
			connectAddress: net.IPv4(10, 0, 0, 4),
			tokens:         []string{"99"},
		}
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
		{hostId: "A", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"10"}},
		{hostId: "B", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"30"}},
		{hostId: "C", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50"}},
		{hostId: "D", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"70"}},
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
		{hostId: "L0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"10"}, dataCenter: "local"},
		{hostId: "L1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"20"}, dataCenter: "local"},
		{hostId: "R0", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"30"}, dataCenter: "remote"},
		{hostId: "R1", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"40"}, dataCenter: "remote"},
		{hostId: "R2", connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"50"}, dataCenter: "remote"},
		{hostId: "R3", connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"60"}, dataCenter: "remote"},
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
		{hostId: "L0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"10"}, dataCenter: "local"},
		{hostId: "L1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"20"}, dataCenter: "local"},
		{hostId: "R0", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"30"}, dataCenter: "remote"},
		{hostId: "R1", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"40"}, dataCenter: "remote"},
		{hostId: "R2", connectAddress: net.IPv4(10, 0, 0, 5), tokens: []string{"50"}, dataCenter: "remote"},
		{hostId: "R3", connectAddress: net.IPv4(10, 0, 0, 6), tokens: []string{"60"}, dataCenter: "remote"},
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
		{hostId: "A", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"10"}},
		{hostId: "B", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"30"}},
		{hostId: "C", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50"}},
		{hostId: "D", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"70"}},
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
		{hostId: "A", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"10"}, dataCenter: "local", rack: "b"},
		{hostId: "B", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"20"}, dataCenter: "local", rack: "b"},
		{hostId: "C", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"30"}, dataCenter: "remote", rack: "a"},
		{hostId: "D", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"40"}, dataCenter: "local", rack: "a"},
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
		{hostId: "A", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"10"}},
		{hostId: "B", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"30"}},
		{hostId: "C", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"50"}},
		{hostId: "D", connectAddress: net.IPv4(10, 0, 0, 4), tokens: []string{"70"}},
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

// benchmarkShufflePolicy builds a token-aware policy with shuffle enabled,
// modelling a production-scale workload: 100 single-DC hosts with RF=5, so
// each token has 5 replicas. The routing key is fixed so every Pick targets
// the same 5-host replica set -- the steady-state hot path.
func benchmarkShufflePolicy(b *testing.B) (HostSelectionPolicy, *internalQuery) {
	b.Helper()
	const (
		numHosts = 100
		rf       = 5
	)
	policy, query := setupShuffleTestPolicy(rf, RoundRobinHostPolicy(), ShuffleReplicas())
	for i := 0; i < numHosts; i++ {
		policy.AddHost(&HostInfo{
			hostId:         fmt.Sprintf("%03d", i),
			connectAddress: net.IPv4(10, 0, byte(i/256), byte(i%256)),
			tokens:         []string{fmt.Sprintf("%03d", i)},
		})
	}
	policy.SetPartitioner("OrderedPartitioner")
	// Routing key "005" walks the ring from token "005", yielding 5 replicas
	// at indices 5..9.
	query.RoutingKey([]byte("005"))
	return policy, newInternalQuery(query, nil)
}

// BenchmarkTokenAwareHostPolicy_PickShuffleSerial measures per-Pick cost on a
// single goroutine. Mostly captures allocation/classification overhead -- the
// global RNG mutex from the prior implementation is uncontended here, so this
// benchmark is the *least* favorable comparison for the lock-free rotation.
func BenchmarkTokenAwareHostPolicy_PickShuffleSerial(b *testing.B) {
	policy, iq := benchmarkShufflePolicy(b)

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
	policy, iq := benchmarkShufflePolicy(b)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			iter := policy.Pick(iq)
			iter()
		}
	})
}
