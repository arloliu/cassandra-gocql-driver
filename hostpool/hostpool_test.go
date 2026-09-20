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
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/hailocab/go-hostpool"

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
