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

package hostpool

import (
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/hailocab/go-hostpool"
	"github.com/stretchr/testify/require"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

func TestHostPolicy_HostPool(t *testing.T) {
	policy := HostPoolHostPolicy(hostpool.New(nil))

	//hosts := []*gocql.HostInfo{
	//	{hostId: "f1935733-af5f-4995-bd1e-94a7a3e67bfd", connectAddress: net.ParseIP("10.0.0.0")},
	//	{hostId: "93ca4489-b322-4fda-b5a5-12d4436271df", connectAddress: net.ParseIP("10.0.0.1")},
	//}
	firstHostId, err1 := gocql.ParseUUID("f1935733-af5f-4995-bd1e-94a7a3e67bfd")
	secondHostId, err2 := gocql.ParseUUID("93ca4489-b322-4fda-b5a5-12d4436271df")

	if err1 != nil || err2 != nil {
		t.Fatal(err1, err2)
	}

	firstHost, err := gocql.NewTestHostInfoFromRow(
		map[string]interface{}{
			"peer":        net.ParseIP("10.0.0.0"),
			"native_port": 9042,
			"host_id":     firstHostId})
	if err != nil {
		t.Errorf("Error creating first host: %v", err)
	}

	secHost, err := gocql.NewTestHostInfoFromRow(
		map[string]interface{}{
			"peer":        net.ParseIP("10.0.0.1"),
			"native_port": 9042,
			"host_id":     secondHostId})
	if err != nil {
		t.Errorf("Error creating second host: %v", err)
	}
	hosts := []*gocql.HostInfo{firstHost, secHost}
	// Using set host to control the ordering of the hosts as calling "AddHost" iterates the map
	// which will result in an unpredictable ordering
	policy.SetHosts(hosts)

	// Each Pick returns a one-shot iterator: one non-nil host, then nil. See
	// #1259 — the previous behavior of repeatedly sampling go-hostpool meant
	// the closure never returned nil, causing tokenAwareHostPolicy fallback
	// iteration to spin at 100% CPU. Callers that want to consider another
	// host call Pick again.
	iter := policy.Pick(nil)
	first := iter()
	if first == nil {
		t.Fatal("Pick().iter() returned nil on first call; expected a host")
	}
	if id := first.Info().HostID(); id != firstHostId.String() && id != secondHostId.String() {
		t.Errorf("Pick returned unknown host id %s", id)
	}
	first.Mark(nil)

	if next := iter(); next != nil {
		t.Errorf("iter() must return nil after first non-nil result; got host id %s", next.Info().HostID())
	}
	if next := iter(); next != nil {
		t.Errorf("iter() must continue to return nil after exhaustion; got host id %s", next.Info().HostID())
	}

	// A subsequent Pick gives a fresh one-shot iterator. Mark one host as
	// failing so hostpool's stats degrade it, then verify the closure still
	// terminates regardless of which host is selected.
	iter2 := policy.Pick(nil)
	second := iter2()
	if second == nil {
		t.Fatal("second Pick().iter() returned nil on first call")
	}
	second.Mark(fmt.Errorf("error"))
	if next := iter2(); next != nil {
		t.Errorf("second iter() must return nil after first non-nil result; got %s", next.Info().HostID())
	}
}

// panickingPool is a hostpool.HostPool whose SetHosts panics for the first
// panics calls and forwards to the embedded pool afterwards. The real pool is
// embedded because hostpool.HostPool has unexported methods and cannot be
// implemented from outside its own package.
type panickingPool struct {
	hostpool.HostPool
	remaining int
	calls     int
	lastHosts []string
}

func (p *panickingPool) SetHosts(hosts []string) {
	p.calls++
	p.lastHosts = hosts
	if p.remaining > 0 {
		p.remaining--
		panic("test: the application's host pool panicked in SetHosts")
	}
	p.HostPool.SetHosts(hosts)
}

func testHost(t *testing.T, addr, id string) *gocql.HostInfo {
	t.Helper()
	hostID, err := gocql.ParseUUID(id)
	if err != nil {
		t.Fatalf("ParseUUID: %v", err)
	}
	host, err := gocql.NewTestHostInfoFromRow(map[string]interface{}{
		"peer":        net.ParseIP(addr),
		"native_port": 9042,
		"host_id":     hostID,
	})
	if err != nil {
		t.Fatalf("NewTestHostInfoFromRow: %v", err)
	}
	return host
}

// mustReturnWithin runs fn on its own goroutine and fails the test if it has not
// returned within two seconds. A goroutine left blocked on a stranded mutex is
// leaked deliberately: the test has already failed at that point.
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
		t.Fatalf("%s blocked for 2s after SetHosts panicked", name)
	}
}

// TestHostPoolPolicy_SetHostsPanicDoesNotStrandMutex covers N9. SetHosts called
// the application's pool under r.mu and released it with a plain Unlock, so one
// panic in that pool stranded the adapter's own mutex: AddHost, RemoveHost and
// the selection path all blocked on it afterwards, and because the ring flusher
// reaches AddHost, so did Session.Close.
func TestHostPoolPolicy_SetHostsPanicDoesNotStrandMutex(t *testing.T) {
	pool := &panickingPool{HostPool: hostpool.New(nil), remaining: 1}
	policy := HostPoolHostPolicy(pool)

	first := testHost(t, "10.0.0.0", "f1935733-af5f-4995-bd1e-94a7a3e67bfd")
	second := testHost(t, "10.0.0.1", "93ca4489-b322-4fda-b5a5-12d4436271df")

	func() {
		// The panic is expected to reach the caller: this adapter owes its mutex
		// and its ordering, not the application pool's success. If a safely is
		// ever added here, this assertion goes red on purpose.
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("SetHosts swallowed the pool's panic")
			}
		}()
		policy.SetHosts([]*gocql.HostInfo{first, second})
	}()

	mustReturnWithin(t, "AddHost", func() { policy.AddHost(first) })
	mustReturnWithin(t, "RemoveHost", func() { policy.RemoveHost(first) })
	// Pick itself never takes r.mu; the iterator it returns does.
	mustReturnWithin(t, "the selection path", func() {
		iter := policy.Pick(nil)
		for iter() != nil { //revive:disable-line:empty-block
		}
	})
}

// TestHostPoolPolicy_AddHostPanicLeavesTheRetryOpen covers N10. AddHost recorded
// the host in r.hostMap before it published the new peer list to the application's
// pool, and its own early return makes every later AddHost for a recorded host a
// no-op. A panic in the pool therefore left the host recorded, unpublished and
// unreachable: completeAdmission's next round returns early, records the
// publication as done, and nothing retries.
func TestHostPoolPolicy_AddHostPanicLeavesTheRetryOpen(t *testing.T) {
	pool := &panickingPool{HostPool: hostpool.New(nil), remaining: 1}
	policy := HostPoolHostPolicy(pool)
	host := testHost(t, "10.0.0.0", "f1935733-af5f-4995-bd1e-94a7a3e67bfd")
	ip := host.ConnectAddress().String()

	addHostAndPanic := func(t *testing.T) {
		t.Helper()
		defer func() {
			if r := recover(); r == nil {
				t.Error("AddHost swallowed the pool's panic")
			}
		}()
		policy.AddHost(host)
	}

	t.Run("the failed publication is not recorded", func(t *testing.T) {
		addHostAndPanic(t)

		policy.mu.RLock()
		_, recorded := policy.hostMap[ip]
		policy.mu.RUnlock()
		if recorded {
			t.Fatal("the failed publication was recorded; every later AddHost returns early")
		}
	})

	t.Run("a later AddHost retries the publication", func(t *testing.T) {
		before := pool.calls
		policy.AddHost(host)
		if pool.calls == before {
			t.Fatal("second AddHost made no SetHosts call; host never published")
		}

		iter := policy.Pick(nil)
		selected := iter()
		if selected == nil {
			t.Fatal("the host is not selectable after the retry")
		}
		if got := selected.Info().ConnectAddress().String(); got != ip {
			t.Fatalf("selected %s, want %s", got, ip)
		}
		selected.Mark(nil)
	})
}

// recordingPool is a hostpool.HostPool that records what the adapter hands it.
//
// Get answers with the address in next when one is set,
// so a test can read which host the adapter serves an address from.
// Marks are recorded on the response,
// because a response marks the pool that issued it through methods a wrapper outside go-hostpool cannot override.
type recordingPool struct {
	hostpool.HostPool
	setHostsCalls int
	lastHosts     []string
	next          string
	marks         []error
}

// recordingResponse is the response recordingPool hands out.
type recordingResponse struct {
	hostpool.HostPoolResponse
	host string
	pool *recordingPool
}

func newRecordingPool() *recordingPool {
	return &recordingPool{HostPool: hostpool.New(nil)}
}

func (p *recordingPool) SetHosts(hosts []string) {
	p.setHostsCalls++
	p.lastHosts = append([]string(nil), hosts...)
	p.HostPool.SetHosts(hosts)
}

func (p *recordingPool) Get() hostpool.HostPoolResponse {
	for range len(p.lastHosts) + 1 {
		inner := p.HostPool.Get()
		if inner == nil {
			break
		}
		if p.next == "" || inner.Host() == p.next {
			return &recordingResponse{HostPoolResponse: inner, host: inner.Host(), pool: p}
		}
	}
	return &recordingResponse{host: p.next, pool: p}
}

func (r *recordingResponse) Host() string { return r.host }

func (r *recordingResponse) Mark(err error) {
	r.pool.marks = append(r.pool.marks, err)
	if r.HostPoolResponse != nil {
		r.HostPoolResponse.Mark(err)
	}
}

// pickAt picks through the policy with the pool answering addr,
// and returns the selection, or nil when the policy selected nothing.
func pickAt(policy *hostPoolHostPolicy, pool *recordingPool, addr string) gocql.SelectedHost {
	pool.next = addr
	return policy.Pick(nil)()
}

// pickedInfo is pickAt's host, or nil.
func pickedInfo(policy *hostPoolHostPolicy, pool *recordingPool, addr string) *gocql.HostInfo {
	if selected := pickAt(policy, pool, addr); selected != nil {
		return selected.Info()
	}
	return nil
}

// anonHost builds a host with no host_id at addr.
func anonHost(t *testing.T, addr string) *gocql.HostInfo {
	t.Helper()
	host, err := gocql.NewTestHostInfoFromRow(map[string]any{
		"peer":        net.ParseIP(addr),
		"native_port": 9042,
	})
	require.NoError(t, err, "NewTestHostInfoFromRow")
	require.Empty(t, host.HostID(), "an id-less host must carry no host_id")
	return host
}

const (
	hostIDX = "aaaaaaaa-0000-4000-8000-0000000000a1"
	hostIDY = "bbbbbbbb-0000-4000-8000-0000000000b2"
	hostIDZ = "cccccccc-0000-4000-8000-0000000000c3"
)

// TestHostPoolPolicy_ReplacedAtSameAddress covers F-ring-1's replacement:
// B takes A's address under a new host_id, then A is swept.
// The address must be served by B from its admission on, and removing A must not remove B.
func TestHostPoolPolicy_ReplacedAtSameAddress(t *testing.T) {
	const addr = "10.0.0.1"
	pool := newRecordingPool()
	policy := HostPoolHostPolicy(pool)
	a := testHost(t, addr, hostIDX)
	b := testHost(t, addr, hostIDY)

	policy.AddHost(a)
	policy.AddHost(b)
	got := pickedInfo(policy, pool, addr)
	require.NotNil(t, got, "Pick after AddHost(B) returned nil")
	require.Same(t, b, got, "Pick after AddHost(B) returned A: the replacement does not serve its address")

	policy.RemoveHost(a)
	got = pickedInfo(policy, pool, addr)
	require.NotNil(t, got, "Pick after RemoveHost(A) returned nil: removing A removed B")
	require.Same(t, b, got, "Pick after RemoveHost(A) must return B")
	require.Equal(t, []string{addr}, pool.Hosts(), "the pool must hold the address once")
}

// TestHostPoolPolicy_StaleAddAfterReplacement covers an UP handler that resolved A,
// paused while the refresh admitted B, and resumed:
// its AddHost(A) must not take the address back,
// or B would go unpicked until the sweep's RemoveHost(A).
func TestHostPoolPolicy_StaleAddAfterReplacement(t *testing.T) {
	const addr = "10.0.0.1"
	pool := newRecordingPool()
	policy := HostPoolHostPolicy(pool)
	a := testHost(t, addr, hostIDX)
	b := testHost(t, addr, hostIDY)

	policy.AddHost(a)
	policy.AddHost(b)
	policy.AddHost(a)
	require.Same(t, b, pickedInfo(policy, pool, addr), "the stale add of A took the address back")

	policy.RemoveHost(a)
	require.Same(t, b, pickedInfo(policy, pool, addr), "Pick after RemoveHost(A) must still return B")
}

// TestHostPoolPolicy_AddressRotation covers F-ring-1's recycling:
// X moves .2 -> .3, Y .3 -> .4 and Z .4 -> .2,
// reconciled one host at a time as refreshRing does, remove-old-then-add-new.
func TestHostPoolPolicy_AddressRotation(t *testing.T) {
	pool := newRecordingPool()
	policy := HostPoolHostPolicy(pool)
	x0, y0, z0 := testHost(t, "10.0.0.2", hostIDX), testHost(t, "10.0.0.3", hostIDY), testHost(t, "10.0.0.4", hostIDZ)
	x1, y1, z1 := testHost(t, "10.0.0.3", hostIDX), testHost(t, "10.0.0.4", hostIDY), testHost(t, "10.0.0.2", hostIDZ)

	policy.AddHost(x0)
	policy.AddHost(y0)
	policy.AddHost(z0)

	policy.RemoveHost(x0)
	policy.AddHost(x1)
	policy.RemoveHost(y0)
	policy.AddHost(y1)
	policy.RemoveHost(z0)
	policy.AddHost(z1)

	require.Same(t, z1, pickedInfo(policy, pool, "10.0.0.2"), "10.0.0.2 must be served by Z")
	require.Same(t, x1, pickedInfo(policy, pool, "10.0.0.3"), "10.0.0.3 must be served by X")
	require.Same(t, y1, pickedInfo(policy, pool, "10.0.0.4"), "10.0.0.4 must be served by Y")
	require.ElementsMatch(t, []string{"10.0.0.2", "10.0.0.3", "10.0.0.4"}, pool.Hosts(),
		"the pool must hold every address once")
}

// TestHostPoolPolicy_ReplacementResetsPool pins that B taking A's address republishes the peer list:
// go-hostpool rebuilds every entry on SetHosts,
// so A's dead mark and statistics do not carry over to B.
func TestHostPoolPolicy_ReplacementResetsPool(t *testing.T) {
	const addr = "10.0.0.1"
	pool := newRecordingPool()
	policy := HostPoolHostPolicy(pool)

	policy.AddHost(testHost(t, addr, hostIDX))
	before := pool.setHostsCalls
	policy.AddHost(testHost(t, addr, hostIDY))
	require.Equal(t, before+1, pool.setHostsCalls,
		"AddHost(B) over A made no SetHosts call: A's pool state carries over to B")
}

// TestHostPoolPolicy_MarkIdentity pins that a response picked for A and marked after B replaced A is not charged to B,
// while B's own mark still is.
func TestHostPoolPolicy_MarkIdentity(t *testing.T) {
	const addr = "10.0.0.1"
	pool := newRecordingPool()
	policy := HostPoolHostPolicy(pool)
	a := testHost(t, addr, hostIDX)
	b := testHost(t, addr, hostIDY)

	policy.AddHost(a)
	pickedA := pickAt(policy, pool, addr)
	require.NotNil(t, pickedA, "Pick returned nil")
	require.Same(t, a, pickedA.Info(), "Pick before the replacement must return A")

	policy.AddHost(b)
	pickedB := pickAt(policy, pool, addr)
	require.NotNil(t, pickedB, "Pick returned nil")
	require.Same(t, b, pickedB.Info(), "Pick after the replacement must return B")

	errA := errors.New("A's stale mark")
	errB := errors.New("B's mark")
	pickedA.Mark(errA)
	pickedB.Mark(errB)
	require.Contains(t, pool.marks, errB, "the recorder did not see B's mark")
	require.NotContains(t, pool.marks, errA, "the recorder saw A's stale mark")
}

// TestHostPoolPolicy_AddressKeyAndMixed pins the copied identity predicate on hosts without a host_id:
// they are identified by address, and never match a host that has one.
func TestHostPoolPolicy_AddressKeyAndMixed(t *testing.T) {
	const addrA, addrB = "10.0.0.1", "10.0.0.2"

	t.Run("id-less hosts at one address are one member", func(t *testing.T) {
		pool := newRecordingPool()
		policy := HostPoolHostPolicy(pool)
		first, second := anonHost(t, addrA), anonHost(t, addrA)

		policy.AddHost(first)
		before := pool.setHostsCalls
		policy.AddHost(second)
		require.Equal(t, before, pool.setHostsCalls, "the second id-less host at one address was added")
		require.Same(t, first, pickedInfo(policy, pool, addrA), "the second id-less host at one address was added")

		policy.RemoveHost(second)
		require.Empty(t, pool.Hosts(), "removing an id-less host must remove the one at its address")
	})

	t.Run("id-less hosts at different addresses are two members", func(t *testing.T) {
		pool := newRecordingPool()
		policy := HostPoolHostPolicy(pool)

		policy.AddHost(anonHost(t, addrA))
		policy.AddHost(anonHost(t, addrB))
		require.ElementsMatch(t, []string{addrA, addrB}, pool.Hosts(),
			"the id-less host at a different address was not added")
	})

	t.Run("a mixed pair at one address is two members", func(t *testing.T) {
		pool := newRecordingPool()
		policy := HostPoolHostPolicy(pool)
		identified, anon := testHost(t, addrA, hostIDY), anonHost(t, addrA)

		policy.AddHost(identified)
		policy.AddHost(anon)
		require.Same(t, anon, pickedInfo(policy, pool, addrA), "the mixed pair is one member")

		policy.RemoveHost(anon)
		require.Same(t, identified, pickedInfo(policy, pool, addrA),
			"removing the id-less host emptied the address")
	})

	t.Run("an id-less removal leaves the identified host", func(t *testing.T) {
		pool := newRecordingPool()
		policy := HostPoolHostPolicy(pool)
		identified := testHost(t, addrA, hostIDY)

		policy.AddHost(identified)
		policy.RemoveHost(anonHost(t, addrA))
		require.Same(t, identified, pickedInfo(policy, pool, addrA),
			"removing the id-less host emptied the address")
	})
}

// TestHostPoolPolicy_SameIdDistinctObject pins that the host_id decides:
// a distinct object with a member's id at another address is that member.
func TestHostPoolPolicy_SameIdDistinctObject(t *testing.T) {
	const addrA, addrB = "10.0.0.1", "10.0.0.2"
	pool := newRecordingPool()
	policy := HostPoolHostPolicy(pool)

	policy.AddHost(testHost(t, addrA, hostIDX))
	before := pool.setHostsCalls
	policy.AddHost(testHost(t, addrB, hostIDX))
	require.Equal(t, []string{addrA}, pool.Hosts(), "AddHost(A') added b")
	require.Equal(t, before, pool.setHostsCalls, "AddHost(A') republished the pool")

	policy.RemoveHost(testHost(t, addrB, hostIDX))
	require.Empty(t, pool.Hosts(), "RemoveHost(A') did not remove A")
}

// TestHostPoolPolicy_SetHostsByIdentity pins SetHosts' contract:
// it deduplicates by identity, the first occurrence winning,
// offers each address once,
// serves an address from the last host admitted there,
// and records every host it keeps as a member.
func TestHostPoolPolicy_SetHostsByIdentity(t *testing.T) {
	const addrA, addrC = "10.0.0.1", "10.0.0.3"
	pool := newRecordingPool()
	policy := HostPoolHostPolicy(pool)
	a := testHost(t, addrA, hostIDX)
	b := testHost(t, addrA, hostIDY)
	aPrime := testHost(t, addrC, hostIDX)

	policy.SetHosts([]*gocql.HostInfo{a, b, aPrime})
	require.Equal(t, []string{addrA}, pool.lastHosts,
		"the pool must receive exactly [a]: A' duplicates A, and a is offered once")
	require.Same(t, b, pickedInfo(policy, pool, addrA), "Pick must return B, the last admitted at a")

	before := pool.setHostsCalls
	policy.RemoveHost(a)
	require.Equal(t, before+1, pool.setHostsCalls, "RemoveHost(A) found no member: SetHosts did not record A")
	require.Same(t, b, pickedInfo(policy, pool, addrA), "Pick after RemoveHost(A) must still return B")

	policy.RemoveHost(b)
	require.Empty(t, pool.Hosts(), "RemoveHost(B) must empty the pool")
}
