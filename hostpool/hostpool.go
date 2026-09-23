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

package hostpool

import (
	"slices"
	"sync"

	"github.com/hailocab/go-hostpool"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// HostPoolHostPolicy is a host policy which uses the bitly/go-hostpool library
// to distribute queries between hosts and prevent sending queries to
// unresponsive hosts. When creating the host pool that is passed to the policy
// use an empty slice of hosts as the hostpool will be populated later by gocql.
// See below for examples of usage:
//
//	// Create host selection policy using a simple host pool
//	cluster.PoolConfig.HostSelectionPolicy = HostPoolHostPolicy(hostpool.New(nil))
//
//	// Create host selection policy using an epsilon greedy pool
//	cluster.PoolConfig.HostSelectionPolicy = HostPoolHostPolicy(
//	    hostpool.NewEpsilonGreedy(nil, 0, &hostpool.LinearEpsilonValueCalculator{}),
//	)
//
// The pool's host strings are connect addresses, while the policy tracks hosts by host_id.
// When a second host takes an address, the address is served by the host admitted there last.
// Every membership change resets the pool's statistics, as it always has.
//
// Parameters:
//   - hp: the pool to select from; the policy replaces its hosts
//
// Returns:
//   - *hostPoolHostPolicy: the host selection policy
func HostPoolHostPolicy(hp hostpool.HostPool) *hostPoolHostPolicy {
	return &hostPoolHostPolicy{hostMap: map[string]*gocql.HostInfo{}, hp: hp}
}

type hostPoolHostPolicy struct {
	hp      hostpool.HostPool
	mu      sync.RWMutex
	members []*gocql.HostInfo          // admission order; no two share an identity
	hostMap map[string]*gocql.HostInfo // address -> the last-admitted member at it; derived from members
}

// sameHostIdentity reports whether a and b describe the same node.
// A host is identified by its host_id when it carries one and by its connect address when it does not.
// A host with a host_id and one without are never the same node.
func sameHostIdentity(a, b *gocql.HostInfo) bool {
	if a == b {
		return true
	}
	ida, idb := a.HostID(), b.HostID()
	if ida != "" || idb != "" {
		return ida == idb
	}
	return a.ConnectAddress().Equal(b.ConnectAddress())
}

func (r *hostPoolHostPolicy) Init(*gocql.Session)                       {}
func (r *hostPoolHostPolicy) KeyspaceChanged(gocql.KeyspaceUpdateEvent) {}
func (r *hostPoolHostPolicy) SetPartitioner(string)                     {}
func (r *hostPoolHostPolicy) IsLocal(*gocql.HostInfo) bool              { return true }

func (r *hostPoolHostPolicy) SetHosts(hosts []*gocql.HostInfo) {
	// The first occurrence of an identity wins; a later one is the same host.
	members := make([]*gocql.HostInfo, 0, len(hosts))
	for _, host := range hosts {
		if !slices.ContainsFunc(members, func(m *gocql.HostInfo) bool { return sameHostIdentity(host, m) }) {
			members = append(members, host)
		}
	}

	// The pool is the application's and may panic. The unlock is deferred rather
	// than the call moved out: the session publishes through AddHost, RemoveHost
	// and SetHosts concurrently, and Pick reads the pool and hostMap under one
	// read lock, so either write outside the section loses an update the adapter
	// would never recompute. The panic itself is left to propagate: what this
	// adapter owes is its mutex and its ordering, not the pool's success.
	r.mu.Lock()
	defer r.mu.Unlock()

	r.publish(members)
}

func (r *hostPoolHostPolicy) AddHost(host *gocql.HostInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// A member with host's identity is host, and keeps its place.
	// Moving it to newest would let a stale AddHost take its address back from a host admitted there since,
	// and the newer host would go unpicked until the stale one was removed.
	if slices.ContainsFunc(r.members, func(m *gocql.HostInfo) bool { return sameHostIdentity(host, m) }) {
		return
	}

	r.publish(append(slices.Clone(r.members), host))
}

func (r *hostPoolHostPolicy) RemoveHost(host *gocql.HostInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()

	same := func(m *gocql.HostInfo) bool { return sameHostIdentity(host, m) }
	if !slices.ContainsFunc(r.members, same) {
		return
	}

	// Another member at the same address keeps it: only host's identity leaves.
	r.publish(slices.DeleteFunc(slices.Clone(r.members), same))
}

func (r *hostPoolHostPolicy) HostUp(host *gocql.HostInfo) {
	r.AddHost(host)
}

func (r *hostPoolHostPolicy) HostDown(host *gocql.HostInfo) {
	r.RemoveHost(host)
}

func (r *hostPoolHostPolicy) Pick(qry gocql.ExecutableStatement) gocql.NextHost {
	// The NextHost contract is: callers iterate until they receive nil. Without
	// a one-shot guard this closure would re-sample the underlying go-hostpool
	// indefinitely (Get() never returns "exhausted") and burn 100% CPU when
	// composed with policies that drain to nil — e.g. TokenAwareHostPolicy as
	// the parent — because the fallback iterator never terminates. See #1259.
	used := false
	return func() gocql.SelectedHost {
		if used {
			return nil
		}
		used = true

		r.mu.RLock()
		defer r.mu.RUnlock()

		if len(r.hostMap) == 0 {
			return nil
		}

		hostR := r.hp.Get()
		host, ok := r.hostMap[hostR.Host()]
		if !ok {
			return nil
		}

		return selectedHostPoolHost{
			policy: r,
			info:   host,
			hostR:  hostR,
		}
	}
}

// selectedHostPoolHost is a host returned by the hostPoolHostPolicy and
// implements the SelectedHost interface
type selectedHostPoolHost struct {
	policy *hostPoolHostPolicy
	info   *gocql.HostInfo
	hostR  hostpool.HostPoolResponse
}

func (host selectedHostPoolHost) Info() *gocql.HostInfo {
	return host.info
}

func (host selectedHostPoolHost) Mark(err error) {
	ip := host.info.ConnectAddress().String()

	host.policy.mu.RLock()
	defer host.policy.mu.RUnlock()

	if current, ok := host.policy.hostMap[ip]; !ok || !sameHostIdentity(current, host.info) {
		// host was removed, or replaced at its address, between pick and mark:
		// what happened on it is not to be charged to the address's current host
		return
	}

	host.hostR.Mark(err)
}

// publish hands the pool the distinct addresses of members,
// then records members and the address map derived from them.
//
// The pool is called first and the record written second.
// Recording first would make AddHost's membership check swallow every later attempt,
// so a panic in the pool would leave the host recorded, unpublished and never selectable.
//
// The caller must hold r.mu.
//
// Parameters:
//   - members: the prospective members, in admission order
func (r *hostPoolHostPolicy) publish(members []*gocql.HostInfo) {
	peers := make([]string, 0, len(members))
	hostMap := make(map[string]*gocql.HostInfo, len(members))
	for _, member := range members {
		ip := member.ConnectAddress().String()
		if _, ok := hostMap[ip]; !ok {
			peers = append(peers, ip)
		}
		// A later member overwrites an earlier one:
		// an address is served by the member admitted there last.
		hostMap[ip] = member
	}

	r.hp.SetHosts(peers)

	r.members = members
	r.hostMap = hostMap
}
