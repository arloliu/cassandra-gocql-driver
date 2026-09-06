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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gocql

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

// tokenAwareEmptyRingFixture returns a token-aware policy over fallback with
// three local hosts, the ordered partitioner set, and a query carrying a
// routing key, ready for the hosts to be removed again.
//
// Parameters:
//   - t: the test
//   - fallback: the fallback policy under test
//   - withSchema: install SimpleStrategy keyspace metadata so Pick takes the
//     replica-map branch rather than the primary-token branch
//
// Returns:
//   - HostSelectionPolicy: the policy
//   - []*HostInfo: the three hosts added to it
//   - *Query: a query with keyspace and routing key set
func tokenAwareEmptyRingFixture(t *testing.T, fallback HostSelectionPolicy, withSchema bool) (HostSelectionPolicy, []*HostInfo, *Query) {
	t.Helper()
	const keyspace = "emptyRingKeyspace"

	policy := TokenAwareHostPolicy(fallback)
	internal := policy.(*tokenAwareHostPolicy)
	internal.getKeyspaceName = func() string { return keyspace }
	if withSchema {
		ksMeta := &KeyspaceMetadata{
			Name:          keyspace,
			StrategyClass: "SimpleStrategy",
			StrategyOptions: map[string]interface{}{
				"class":              "SimpleStrategy",
				"replication_factor": 3,
			},
		}
		ksMeta.placementStrategy = getStrategy(ksMeta, nopLoggerSingleton)
		internal.getSchemaMeta = func() *schemaMeta {
			return &schemaMeta{keyspaceMeta: map[string]*KeyspaceMetadata{keyspace: ksMeta}}
		}
	}

	hosts := []*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), tokens: []string{"10"}, dataCenter: "local", rack: "b"},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), tokens: []string{"20"}, dataCenter: "local", rack: "b"},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), tokens: []string{"30"}, dataCenter: "local", rack: "b"},
	}
	for _, h := range hosts {
		h.setState(NodeUp)
	}
	policy.SetPartitioner("OrderedPartitioner")
	for _, h := range hosts {
		policy.AddHost(h)
	}

	query := &Query{}
	query.getKeyspace = func() string { return keyspace }
	query.RoutingKey([]byte("23"))
	return policy, hosts, query
}

// countHosts drains iter and returns how many hosts it yielded.
func countHosts(iter NextHost) int {
	n := 0
	for iter() != nil {
		n++
	}
	return n
}

// TestHostPolicy_TokenAware_EmptyRingDoesNotPanic proves Pick does not dereference
// the nil primary that GetHostForToken returns for a ring with no tokens.
// DC- and rack-aware fallbacks classify replicas through HostInfo.DataCenter, which is not nil-safe.
// Round robin is the control case.
func TestHostPolicy_TokenAware_EmptyRingDoesNotPanic(t *testing.T) {
	fallbacks := map[string]func() HostSelectionPolicy{
		"dc":   func() HostSelectionPolicy { return DCAwareRoundRobinPolicy("local") },
		"rack": func() HostSelectionPolicy { return RackAwareRoundRobinPolicy("local", "b") },
		"rr":   RoundRobinHostPolicy,
	}
	for name, newFallback := range fallbacks {
		for _, withSchema := range []bool{false, true} {
			t.Run(name+"/schema="+map[bool]string{false: "off", true: "on"}[withSchema], func(t *testing.T) {
				policy, hosts, query := tokenAwareEmptyRingFixture(t, newFallback(), withSchema)

				// Removing one host leaves the other tokens in place: no change.
				policy.RemoveHost(hosts[0])
				require.Equal(t, 2, countHosts(policy.Pick(newInternalQuery(query, nil))),
					"two hosts must remain selectable after removing one of three")

				// Removing the last token holder empties the ring.
				policy.RemoveHost(hosts[1])
				policy.RemoveHost(hosts[2])
				var yielded int
				require.NotPanics(t, func() {
					yielded = countHosts(policy.Pick(newInternalQuery(query, nil)))
				}, "Pick on an empty token ring")
				require.Zero(t, yielded, "an empty ring has no hosts to yield")
			})
		}
	}
}

// TestHostPolicy_TokenAware_TokenlessHostsDoNotPanic proves hosts without tokens,
// which build a token ring with zero tokens, make Pick fall back rather than panic.
func TestHostPolicy_TokenAware_TokenlessHostsDoNotPanic(t *testing.T) {
	policy := TokenAwareHostPolicy(DCAwareRoundRobinPolicy("local"))
	internal := policy.(*tokenAwareHostPolicy)
	internal.getKeyspaceName = func() string { return "ks" }

	hosts := []*HostInfo{
		{hostId: "0", connectAddress: net.IPv4(10, 0, 0, 1), dataCenter: "local"},
		{hostId: "1", connectAddress: net.IPv4(10, 0, 0, 2), dataCenter: "local"},
		{hostId: "2", connectAddress: net.IPv4(10, 0, 0, 3), dataCenter: "local"},
	}
	for _, h := range hosts {
		h.setState(NodeUp)
	}
	policy.SetPartitioner("OrderedPartitioner")
	for _, h := range hosts {
		policy.AddHost(h)
	}

	query := &Query{}
	query.getKeyspace = func() string { return "ks" }
	query.RoutingKey([]byte("23"))

	var yielded int
	require.NotPanics(t, func() {
		yielded = countHosts(policy.Pick(newInternalQuery(query, nil)))
	}, "Pick with token-less hosts")
	require.Equal(t, 3, yielded, "the fallback must still offer every host")
}
