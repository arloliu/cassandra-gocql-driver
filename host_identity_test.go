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
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// The tests in this file pin the invariants the identity snapshot rests on:
//
//   - I1 a single observation of hostId/dataCenter/rack/missingRack always comes
//     from one publication, and one logical decision reads a host once
//   - I2 the two post-publication identity writers serialise, so neither loses
//     the other's fill
//   - I3 state is not part of the identity and no identity publication moves it
//   - I4 the fill rules are the ones the plain fields had
//   - I5 the read path allocates nothing and a no-op update publishes nothing
//   - I6 nothing keeps a parallel copy of a snapshotted field
//
// They are all in-memory and run under -race.
// The stress tests assert invariants, so they never report a false failure;
// what they actually detect is measured separately,
// by applying the planned mutations to the production code.

// ---------------------------------------------------------------------------
// shared concurrency skeleton
// ---------------------------------------------------------------------------

// identityStress is the reader/writer skeleton the concurrent identity tests
// share.
//
// Readers signal ready after their first observation, the writer waits for all
// of them before it starts, and closing done makes every reader take one last
// observation and leave.
// No reader waits to "eventually see" a particular value,
// so nothing here can hang: the only termination condition is done.
//
// Only the goroutine that owns the test may call t.Fatal, so readers record what
// they saw in counters and the main goroutine reports after wait.
type identityStress struct {
	done  chan struct{}
	once  sync.Once
	ready sync.WaitGroup
	wg    sync.WaitGroup
}

// newIdentityStress builds a skeleton whose done channel is closed on cleanup,
// so a failing test releases its goroutines instead of leaking them.
//
// Parameters:
//   - t: the test the skeleton belongs to
//
// Returns:
//   - *identityStress: a skeleton with no readers and no writer yet
func newIdentityStress(t *testing.T) *identityStress {
	s := &identityStress{done: make(chan struct{})}
	t.Cleanup(s.stop)
	return s
}

// stop closes done exactly once, whoever calls it.
func (s *identityStress) stop() {
	s.once.Do(func() { close(s.done) })
}

// readersEach starts n reader goroutines, each running its own observer.
//
// Each reader observes once, reports itself ready, then observes in a loop; the
// call made after done is closed is passed last, so a test can assert on the
// final, settled state.
// A per-reader observer is what lets a test track state
// across one reader's own observations.
//
// Parameters:
//   - n: how many readers to start
//   - newObserver: builds one reader's observer; called once per reader
func (s *identityStress) readersEach(n int, newObserver func() func(last bool)) {
	s.ready.Add(n)
	s.wg.Add(n)
	for i := 0; i < n; i++ {
		observe := newObserver()
		go func() {
			defer s.wg.Done()
			observe(false)
			s.ready.Done()
			for {
				select {
				case <-s.done:
					observe(true)
					return
				default:
					observe(false)
				}
			}
		}()
	}
}

// readers starts n reader goroutines sharing one observer.
//
// Parameters:
//   - n: how many readers to start
//   - observe: one observation; last is true only for the closing call
func (s *identityStress) readers(n int, observe func(last bool)) {
	s.readersEach(n, func() func(bool) { return observe })
}

// write starts the writer, which runs body once every reader is ready and then
// closes done.
//
// Parameters:
//   - body: the writes under test
func (s *identityStress) write(body func()) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.ready.Wait()
		body()
		s.stop()
	}()
}

// wait joins every reader and the writer.
func (s *identityStress) wait() {
	s.wg.Wait()
}

// publishIdentity stores id on host without any lock, which is what a fixture no
// other goroutine holds may do, and what a flipping writer needs in order to
// drive the snapshot directly rather than through update's fill rules.
//
// Parameters:
//   - host: the host to publish on
//   - id: the snapshot to publish; shared afterwards, so it must not be mutated
func publishIdentity(host *HostInfo, id *hostIdentity) {
	host.ident.Store(id)
}

// identityHost builds a fixture host carrying id, with connectAddress derived
// from last so distinct fixtures are distinct hosts to the policies.
//
// Parameters:
//   - last: the final octet of the host's 10.0.0.x connect address
//   - id: the identity to publish
//
// Returns:
//   - *HostInfo: the fixture
func identityHost(last byte, id hostIdentity) *HostInfo {
	h := &HostInfo{connectAddress: net.IPv4(10, 0, 0, last)}
	publishIdentity(h, &id)
	return h
}

// requireIdentity fails the test unless host's published identity is exactly want.
//
// Parameters:
//   - t: the test
//   - host: the host to inspect
//   - want: the expected snapshot
//   - context: what the caller was doing, for the failure message
func requireIdentity(t *testing.T, host *HostInfo, want hostIdentity, context string) {
	t.Helper()
	if got := *host.identity(); got != want {
		t.Fatalf("%s: identity = %+v, want %+v", context, got, want)
	}
}

// runConcurrentlyFunc starts both functions from a barrier and waits for both.
//
// Parameters:
//   - first: one racing write
//   - second: the other
func runConcurrentlyFunc(first, second func()) {
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		first()
	}()
	go func() {
		defer wg.Done()
		<-start
		second()
	}()
	close(start)
	wg.Wait()
}

// ---------------------------------------------------------------------------
// T1 - I1: HostTier never sees a torn identity
// ---------------------------------------------------------------------------

// identityFlips is how many A-then-B cycles the flipping writers drive.
// Each cycle is two publications,
// so a reader that loads the snapshot twice has a window on every one of them.
const identityFlips = 100000

// TestHostInfo_HostTierNeverObservesTornIdentity drives a host between two
// snapshots while rackAwareRR.HostTier classifies it, and asserts the tier is
// always one the host was actually in.
//
// With localDC "" and localRack "r", snapshot A ("", "", missing rack) is tier 1
// and snapshot B ("remote", "r") is tier 2. Tier 0 needs A's data center paired
// with B's rack, a combination no publication ever held: it can only come from
// reading the data center and the rack as two separate observations.
func TestHostInfo_HostTierNeverObservesTornIdentity(t *testing.T) {
	tierer, ok := RackAwareRoundRobinPolicy("", "r").(HostTierer)
	if !ok {
		t.Fatal("RackAwareRoundRobinPolicy must implement HostTierer")
	}

	snapA := &hostIdentity{missingRack: true}
	snapB := &hostIdentity{dataCenter: "remote", rack: "r"}

	host := &HostInfo{}
	publishIdentity(host, snapA)
	if got := tierer.HostTier(host); got != 1 {
		t.Fatalf("snapshot A must classify as tier 1, got %d", got)
	}
	publishIdentity(host, snapB)
	if got := tierer.HostTier(host); got != 2 {
		t.Fatalf("snapshot B must classify as tier 2, got %d", got)
	}
	publishIdentity(host, snapA)

	var torn, bogus, lastNotB, observations atomic.Int64

	s := newIdentityStress(t)
	s.readers(runtime.GOMAXPROCS(0), func(last bool) {
		tier := tierer.HostTier(host)
		observations.Add(1)
		switch tier {
		case 0:
			// A's data center with B's rack: the torn read.
			torn.Add(1)
		case 1, 2:
		default:
			bogus.Add(1)
		}
		if last && tier != 2 {
			lastNotB.Add(1)
		}
	})
	s.write(func() {
		for i := 0; i < identityFlips; i++ {
			publishIdentity(host, snapA)
			publishIdentity(host, snapB)
		}
	})
	s.wait()

	t.Logf("T1: %d HostTier observations over %d flips", observations.Load(), identityFlips)
	if n := torn.Load(); n != 0 {
		t.Errorf("HostTier returned the impossible tier 0 %d times over %d flips: "+
			"the data center and the rack came from different publications", n, identityFlips)
	}
	if n := bogus.Load(); n != 0 {
		t.Errorf("HostTier returned a tier outside {0,1,2} %d times", n)
	}
	if n := lastNotB.Load(); n != 0 {
		t.Errorf("%d readers settled on a tier other than 2 after the writer's last publication of B", n)
	}
	if got := tierer.HostTier(host); got != 2 {
		t.Fatalf("after the writer finished on B, HostTier = %d, want 2", got)
	}
}

// ---------------------------------------------------------------------------
// T2 - I2: concurrent updates keep both fills
// ---------------------------------------------------------------------------

// identityTrials is how many times the racing-writer tests repeat.
const identityTrials = 1000

// TestHostInfo_ConcurrentUpdatesPreserveComplementaryFills races two updates
// carrying complementary halves of an identity and asserts both land.
//
// from1 carries only the data center and must itself report missingRack: a from
// whose rack is present-but-empty legitimately fills the destination's rack with
// "" and closes the door on from2's rack, which is correct behaviour rather than
// a lost update.
// Whichever order the two updates take,
// the destination must end up with from2's host id and rack and from1's data center.
//
// The ring variant drives the same race through ring.addOrUpdate, which is how
// two concurrent ring refreshes reach one host: addHostIfMissing releases the
// ring lock before update runs, so the host's own lock is the only thing
// serialising them.
func TestHostInfo_ConcurrentUpdatesPreserveComplementaryFills(t *testing.T) {
	want := hostIdentity{hostId: "h", dataCenter: "dc1", rack: "r2"}

	t.Run("bare update", func(t *testing.T) {
		for trial := 0; trial < identityTrials; trial++ {
			dest := identityHost(1, hostIdentity{missingRack: true})
			from1 := identityHost(2, hostIdentity{dataCenter: "dc1", missingRack: true})
			from2 := identityHost(3, hostIdentity{hostId: "h", rack: "r2"})

			runConcurrentlyFunc(
				func() { dest.update(from1) },
				func() { dest.update(from2) },
			)

			requireIdentity(t, dest, want, fmt.Sprintf("trial %d", trial))
		}
	})

	t.Run("through the ring", func(t *testing.T) {
		for trial := 0; trial < identityTrials; trial++ {
			// The destination is already in the ring under host id "h", so both
			// refreshes take the update path rather than one of them inserting.
			dest := identityHost(1, hostIdentity{hostId: "h", missingRack: true})
			dest.peer = net.IPv4(10, 0, 0, 1)
			from1 := identityHost(2, hostIdentity{hostId: "h", dataCenter: "dc1", missingRack: true})
			from1.peer = net.IPv4(10, 0, 0, 2)
			from2 := identityHost(3, hostIdentity{hostId: "h", rack: "r2"})
			from2.peer = net.IPv4(10, 0, 0, 3)

			r := &ring{}
			if _, existed := r.addHostIfMissing(dest); existed {
				t.Fatalf("trial %d: the destination must be the ring's first entry", trial)
			}

			var existed1, existed2 atomic.Bool
			runConcurrentlyFunc(
				func() { _, e := r.addOrUpdate(from1); existed1.Store(e) },
				func() { _, e := r.addOrUpdate(from2); existed2.Store(e) },
			)

			if !existed1.Load() || !existed2.Load() {
				t.Fatalf("trial %d: both refreshes must have taken the update path, got %v and %v",
					trial, existed1.Load(), existed2.Load())
			}
			requireIdentity(t, dest, want, fmt.Sprintf("trial %d", trial))
		}
	})
}

// ---------------------------------------------------------------------------
// T3 - I2, I4: update and setHostID in both orders
// ---------------------------------------------------------------------------

// TestHostInfo_UpdateAndSetHostIDBothOrders pins how the two identity writers
// combine: setHostID overwrites unconditionally, update fills only what is
// empty, and neither loses the other's work whichever runs first.
//
// Each case starts from a destination that reports missingRack. A zero HostInfo
// would not: its rack is an empty rack that exists, which update correctly
// declines to fill, and the case would then fail for a reason that has nothing
// to do with the two writers.
func TestHostInfo_UpdateAndSetHostIDBothOrders(t *testing.T) {
	newDest := func() *HostInfo { return identityHost(1, hostIdentity{missingRack: true}) }
	newFrom := func() *HostInfo {
		return identityHost(2, hostIdentity{hostId: "old", dataCenter: "dc", rack: "r"})
	}
	want := hostIdentity{hostId: "new", dataCenter: "dc", rack: "r"}

	t.Run("setHostID then update", func(t *testing.T) {
		dest := newDest()
		dest.setHostID("new")
		dest.update(newFrom())
		requireIdentity(t, dest, want, "setHostID then update")
	})

	t.Run("update then setHostID", func(t *testing.T) {
		dest := newDest()
		dest.update(newFrom())
		requireIdentity(t, dest, hostIdentity{hostId: "old", dataCenter: "dc", rack: "r"},
			"update alone fills the empty host id from from")
		dest.setHostID("new")
		requireIdentity(t, dest, want, "update then setHostID")
	})

	t.Run("concurrently", func(t *testing.T) {
		for trial := 0; trial < identityTrials; trial++ {
			dest := newDest()
			from := newFrom()
			runConcurrentlyFunc(
				func() { dest.setHostID("new") },
				func() { dest.update(from) },
			)
			requireIdentity(t, dest, want, fmt.Sprintf("trial %d", trial))
		}
	})
}

// ---------------------------------------------------------------------------
// T4 - I3: identity publication never moves state
// ---------------------------------------------------------------------------

// TestHostInfo_IdentityPublicationDoesNotTouchState asserts state and identity
// are independent: setState is the only writer of state, and no identity
// publication carries a state back with it.
//
// State is asserted immediately after every setState as well as after every
// identity write, so a failure says which boundary broke - the setter and the
// read path, or only publication.
func TestHostInfo_IdentityPublicationDoesNotTouchState(t *testing.T) {
	newHost := func() *HostInfo { return identityHost(1, hostIdentity{missingRack: true}) }
	newFrom := func() *HostInfo {
		return identityHost(2, hostIdentity{hostId: "h", dataCenter: "dc", rack: "r"})
	}
	requireState := func(t *testing.T, host *HostInfo, want nodeState, context string) {
		t.Helper()
		if got := host.State(); got != want {
			t.Fatalf("%s: State() = %s, want %s", context, got, want)
		}
	}

	t.Run("down survives identity publication", func(t *testing.T) {
		host := newHost()
		host.setState(NodeDown)
		requireState(t, host, NodeDown, "right after setState(NodeDown)")

		host.update(newFrom())
		requireState(t, host, NodeDown, "after update published an identity")

		host.setHostID("other")
		requireState(t, host, NodeDown, "after setHostID published an identity")
	})

	t.Run("up after down survives identity publication", func(t *testing.T) {
		host := newHost()
		host.setState(NodeDown)
		requireState(t, host, NodeDown, "right after setState(NodeDown)")
		host.setState(NodeUp)
		requireState(t, host, NodeUp, "right after setState(NodeUp)")

		host.update(newFrom())
		requireState(t, host, NodeUp, "after update published an identity")

		host.setHostID("other")
		requireState(t, host, NodeUp, "after setHostID published an identity")
	})

	t.Run("concurrently", func(t *testing.T) {
		const reads = 200000

		host := newHost()
		host.setState(NodeDown)
		requireState(t, host, NodeDown, "right after setState(NodeDown)")

		done := make(chan struct{})
		var once sync.Once
		stop := func() { once.Do(func() { close(done) }) }
		t.Cleanup(stop)

		started := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; ; round++ {
				from := identityHost(2, hostIdentity{
					hostId:     fmt.Sprintf("h%d", round),
					dataCenter: "dc",
					rack:       "r",
				})
				host.update(from)
				host.setHostID(fmt.Sprintf("h%d", round))
				if round == 0 {
					close(started)
				}
				select {
				case <-done:
					return
				default:
				}
			}
		}()

		<-started
		var wrong int64
		for i := 0; i < reads; i++ {
			if host.State() != NodeDown {
				wrong++
			}
		}
		stop()
		wg.Wait()

		if wrong != 0 {
			t.Errorf("State() left NodeDown %d times out of %d reads while identities were published",
				wrong, reads)
		}
		requireState(t, host, NodeDown, "after the identity writer finished")
	})
}

// ---------------------------------------------------------------------------
// T5 - I4, I5: defaults, fill rules, and the read path's allocations
// ---------------------------------------------------------------------------

// TestHostInfo_IdentityDefaultsAndFillRules pins the values a host reports when
// nothing was published, the rules update fills by, and the two halves of I5:
// an update that changes nothing publishes nothing, and it allocates nothing.
func TestHostInfo_IdentityDefaultsAndFillRules(t *testing.T) {
	t.Run("a zero host reports the shared empty identity", func(t *testing.T) {
		host := &HostInfo{}
		if host.ident.Load() != nil {
			t.Fatal("a host that published nothing must hold a nil pointer, not a lazily allocated default")
		}
		if host.identity() != &emptyHostIdentity {
			t.Fatal("identity() on an unpublished host must return the shared emptyHostIdentity")
		}
		if got := *host.identity(); got != (hostIdentity{}) {
			t.Fatalf("emptyHostIdentity = %+v, want the zero value", got)
		}
		if host.HostID() != "" || host.DataCenter() != "" || host.Rack() != "" {
			t.Fatalf("accessors on an unpublished host = (%q, %q, %q), want empty",
				host.HostID(), host.DataCenter(), host.Rack())
		}
		if host.identity().missingRack {
			t.Fatal("an unpublished host must report missingRack false, as the plain field did")
		}
	})

	t.Run("NewHostInfoFromAddrPort publishes no identity", func(t *testing.T) {
		host, err := NewHostInfoFromAddrPort(net.IPv4(127, 0, 0, 1), 9042)
		if err != nil {
			t.Fatalf("NewHostInfoFromAddrPort: %v", err)
		}
		if host.identity() != &emptyHostIdentity {
			t.Fatalf("identity = %+v, want the shared emptyHostIdentity", *host.identity())
		}
		if host.identity().missingRack {
			t.Fatal("NewHostInfoFromAddrPort must leave missingRack false")
		}
	})

	t.Run("IsUp is nil safe", func(t *testing.T) {
		if (*HostInfo)(nil).IsUp() {
			t.Fatal("IsUp on a nil host must be false")
		}
	})

	t.Run("the row's rack column decides missingRack", func(t *testing.T) {
		var nilRack *string
		emptyRack := ""

		tests := []struct {
			name            string
			rack            interface{}
			present         bool
			wantRack        string
			wantMissingRack bool
		}{
			{name: "no rack column", present: false, wantMissingRack: true},
			{name: "null rack", rack: nilRack, present: true, wantMissingRack: true},
			{name: "empty rack pointer", rack: &emptyRack, present: true, wantMissingRack: false},
			{name: "empty rack value", rack: "", present: true, wantMissingRack: false},
			{name: "named rack", rack: "rack1", present: true, wantRack: "rack1", wantMissingRack: false},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				row := map[string]interface{}{
					"peer":            "127.0.0.2",
					"rpc_address":     "127.0.0.2",
					"data_center":     "dc1",
					"host_id":         "8d4e8b0a-1f6a-4c2e-9e4a-2b7c1d3f5a60",
					"release_version": "4.0.0",
				}
				if test.present {
					row["rack"] = test.rack
				}

				host, err := NewTestHostInfoFromRow(row)
				if err != nil {
					t.Fatalf("NewTestHostInfoFromRow: %v", err)
				}
				id := host.identity()
				if id.missingRack != test.wantMissingRack {
					t.Errorf("missingRack = %v, want %v", id.missingRack, test.wantMissingRack)
				}
				if id.rack != test.wantRack {
					t.Errorf("rack = %q, want %q", id.rack, test.wantRack)
				}
			})
		}
	})

	t.Run("a zero uuid host_id publishes no host id", func(t *testing.T) {
		host, err := NewTestHostInfoFromRow(map[string]interface{}{
			"peer":        "127.0.0.2",
			"rpc_address": "127.0.0.2",
			"data_center": "dc1",
			"rack":        "rack1",
			"host_id":     UUID{},
		})
		if err != nil {
			t.Fatalf("NewTestHostInfoFromRow: %v", err)
		}
		if got := host.HostID(); got != "" {
			t.Fatalf("host id from a zero uuid = %q, want empty", got)
		}
	})

	t.Run("update fills only what is empty", func(t *testing.T) {
		from := hostIdentity{hostId: "fromID", dataCenter: "fromDC", rack: "fromRack"}

		tests := []struct {
			name string
			dest hostIdentity
			want hostIdentity
		}{
			{
				name: "everything empty and the rack missing",
				dest: hostIdentity{missingRack: true},
				want: hostIdentity{hostId: "fromID", dataCenter: "fromDC", rack: "fromRack"},
			},
			{
				name: "a present empty rack is not missing",
				dest: hostIdentity{},
				want: hostIdentity{hostId: "fromID", dataCenter: "fromDC"},
			},
			{
				name: "a named rack is kept",
				dest: hostIdentity{rack: "own"},
				want: hostIdentity{hostId: "fromID", dataCenter: "fromDC", rack: "own"},
			},
			{
				name: "a named host id is kept",
				dest: hostIdentity{hostId: "own", missingRack: true},
				want: hostIdentity{hostId: "own", dataCenter: "fromDC", rack: "fromRack"},
			},
			{
				name: "a named data center is kept",
				dest: hostIdentity{dataCenter: "own", missingRack: true},
				want: hostIdentity{hostId: "fromID", dataCenter: "own", rack: "fromRack"},
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				dest := identityHost(1, test.dest)
				dest.update(identityHost(2, from))
				requireIdentity(t, dest, test.want, "after update")
			})
		}
	})

	t.Run("a self update is a no-op", func(t *testing.T) {
		host := identityHost(1, hostIdentity{hostId: "h", dataCenter: "dc", rack: "r"})
		before := host.ident.Load()
		host.update(host)
		if host.ident.Load() != before {
			t.Fatal("a self update must not publish")
		}
		requireIdentity(t, host, hostIdentity{hostId: "h", dataCenter: "dc", rack: "r"},
			"after a self update")
	})

	t.Run("an update that changes nothing publishes nothing", func(t *testing.T) {
		dest := identityHost(1, hostIdentity{hostId: "h", dataCenter: "dc", rack: "r"})
		from := identityHost(2, hostIdentity{hostId: "h", dataCenter: "dc", rack: "r"})

		before := dest.ident.Load()
		dest.update(from)
		if got := dest.ident.Load(); got != before {
			t.Fatalf("a no-op update republished the identity: %p -> %p", before, got)
		}

		// The same for a host that never published: it must stay unpublished
		// rather than acquire an allocated default.
		unpublished := &HostInfo{connectAddress: net.IPv4(10, 0, 0, 3)}
		unpublished.update(&HostInfo{connectAddress: net.IPv4(10, 0, 0, 4)})
		if unpublished.ident.Load() != nil {
			t.Fatal("an update between two unpublished hosts must not publish an identity")
		}
	})

	t.Run("a no-op update allocates nothing", func(t *testing.T) {
		// Pointer equality above only proves nothing was published; this is what
		// proves nothing was allocated.
		// The fixture is built outside the measured call,
		// and the two hosts are distinct so update does not take its self update
		// short cut.
		dest := identityHost(1, hostIdentity{hostId: "h", dataCenter: "dc", rack: "r"})
		dest.peer = net.IPv4(10, 0, 0, 1)
		dest.rpcAddress = net.IPv4(10, 0, 0, 1)
		dest.tokens = []string{"1"}
		from := identityHost(2, hostIdentity{hostId: "h", dataCenter: "dc", rack: "r"})

		if allocs := testing.AllocsPerRun(100, func() { dest.update(from) }); allocs != 0 {
			t.Errorf("a no-op update allocated %v objects per run, want 0", allocs)
		}
	})
}

// ---------------------------------------------------------------------------
// T6 - I1, I6: String and isValidPeer read one snapshot
// ---------------------------------------------------------------------------

// TestHostInfo_StringAndIsValidPeerUnderConcurrentWrites asserts the two
// in-package consumers that need more than one identity field decide from a
// single snapshot.
//
// String renders the data center, the rack and the host id together, so its
// output must always be one of the two snapshots the host actually held and never
// a mixture.
// isValidPeer answers one question from three fields, so under a legal fill its
// answer must only ever go from false to true.
//
// The writer performs one legal fill on a fresh host per round, which is the
// shape production writers have.
// It cannot tell one identity load from several: String holds h.mu's read lock
// while it renders, and update needs the write lock, so no legal publication can
// land between two reads inside String, and isValidPeer's monotonic answer would
// look the same either way.
// T1 and T7b are the measured single-load detectors.
// What this test contributes is that the real writer path never produces a mixed
// rendering or a regressing peer verdict.
func TestHostInfo_StringAndIsValidPeerUnderConcurrentWrites(t *testing.T) {
	t.Run("String never renders a mixed identity", func(t *testing.T) {
		// A fresh host starts on the incomplete identity, which renders as the
		// empty triple, and one update fills it to the complete one; those two
		// are the only snapshots a round ever holds.
		const fillRounds = 200

		empty := hostIdentity{missingRack: true}
		complete := hostIdentity{hostId: "h1", dataCenter: "dc1", rack: "r1"}
		rendered := func(id hostIdentity) string {
			return fmt.Sprintf("data_center=%q rack=%q host_id=%q", id.dataCenter, id.rack, id.hostId)
		}
		wantEmpty, wantComplete := rendered(empty), rendered(complete)

		var mixed, badField, badState atomic.Int64

		for round := 0; round < fillRounds; round++ {
			host := &HostInfo{connectAddress: net.IPv4(10, 0, 0, 1), port: 9042}
			publishIdentity(host, &hostIdentity{missingRack: true})

			s := newIdentityStress(t)
			s.readers(runtime.GOMAXPROCS(0), func(last bool) {
				out := host.String()
				if !strings.Contains(out, wantEmpty) && !strings.Contains(out, wantComplete) {
					mixed.Add(1)
				}
				// Each accessor call is its own observation, so only the
				// individual values are constrained here, never their
				// combination.
				if dc := host.DataCenter(); dc != "" && dc != complete.dataCenter {
					badField.Add(1)
				}
				if rack := host.Rack(); rack != "" && rack != complete.rack {
					badField.Add(1)
				}
				if id := host.HostID(); id != "" && id != complete.hostId {
					badField.Add(1)
				}
				if host.State() != NodeUp {
					badState.Add(1)
				}
			})
			s.write(func() { host.update(identityHost(2, complete)) })
			s.wait()

			requireIdentity(t, host, complete, fmt.Sprintf("T6 round %d after the fill", round))
			if t.Failed() {
				break
			}
		}

		if n := mixed.Load(); n != 0 {
			t.Errorf("String() rendered an identity that was never published %d times", n)
		}
		if n := badField.Load(); n != 0 {
			t.Errorf("an accessor returned a value from neither snapshot %d times", n)
		}
		if n := badState.Load(); n != 0 {
			t.Errorf("State() left NodeUp %d times while only identities were published", n)
		}
	})

	t.Run("isValidPeer decides from one snapshot", func(t *testing.T) {
		// The host already carries the fields isValidPeer reads outside the
		// identity, so the update below publishes an identity and nothing else.
		host := identityHost(1, hostIdentity{missingRack: true})
		host.rpcAddress = net.IPv4(10, 0, 0, 1)
		host.tokens = []string{"1"}

		if isValidPeer(host) {
			t.Fatal("a host with no identity must not be a valid peer")
		}

		from := identityHost(2, hostIdentity{hostId: "h", dataCenter: "dc", rack: "r"})

		var regressions atomic.Int64

		s := newIdentityStress(t)
		s.readersEach(runtime.GOMAXPROCS(0), func() func(bool) {
			// Per reader, so one reader's own sequence of answers is what is
			// checked: a fill only ever adds fields, so true must never go back
			// to false.
			sawValid := false
			return func(last bool) {
				valid := isValidPeer(host)
				if sawValid && !valid {
					regressions.Add(1)
				}
				if valid {
					sawValid = true
				}
			}
		})
		s.write(func() { host.update(from) })
		s.wait()

		if n := regressions.Load(); n != 0 {
			t.Errorf("isValidPeer went back to false %d times after reporting true", n)
		}
		if !isValidPeer(host) {
			t.Fatal("after the fill the host must be a valid peer")
		}
	})

	t.Run("isValidPeer rejects an incomplete identity", func(t *testing.T) {
		complete := func() *HostInfo {
			h := identityHost(1, hostIdentity{hostId: "h", dataCenter: "dc", rack: "r"})
			h.rpcAddress = net.IPv4(10, 0, 0, 1)
			h.tokens = []string{"1"}
			return h
		}

		if !isValidPeer(complete()) {
			t.Fatal("a complete host must be a valid peer")
		}

		tests := []struct {
			name   string
			damage func(*HostInfo)
		}{
			{"no host id", func(h *HostInfo) {
				publishIdentity(h, &hostIdentity{dataCenter: "dc", rack: "r"})
			}},
			{"no data center", func(h *HostInfo) {
				publishIdentity(h, &hostIdentity{hostId: "h", rack: "r"})
			}},
			{"a missing rack", func(h *HostInfo) {
				publishIdentity(h, &hostIdentity{hostId: "h", dataCenter: "dc", missingRack: true})
			}},
			{"no rpc address", func(h *HostInfo) { h.rpcAddress = nil }},
			{"no tokens", func(h *HostInfo) { h.tokens = nil }},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				host := complete()
				test.damage(host)
				if isValidPeer(host) {
					t.Fatalf("a host with %s must not be a valid peer", test.name)
				}
			})
		}
	})
}

// ---------------------------------------------------------------------------
// T7 - I1 through Pick: the token-aware classify loop
// ---------------------------------------------------------------------------

// tierRecorder is a test-only fallback that records every tier the token-aware
// classify loop asks for.
//
// It delegates every decision to a production RackAwareRoundRobinPolicy, so the
// tiers it reports are the ones the driver would use; the only thing it adds is
// a tally per host, which is how a test can assert on classifications that
// happen inside Pick and are otherwise invisible.
type tierRecorder struct {
	inner  HostSelectionPolicy
	tierer HostTierer

	mu    sync.Mutex
	tiers map[*HostInfo]map[uint]int
}

// newTierRecorder builds a recorder around a rack-aware policy local to
// localDC and localRack.
//
// Parameters:
//   - localDC: the data center the wrapped policy calls local
//   - localRack: the rack the wrapped policy calls local
//
// Returns:
//   - *tierRecorder: a recorder with an empty tally
func newTierRecorder(localDC, localRack string) *tierRecorder {
	inner := RackAwareRoundRobinPolicy(localDC, localRack)
	return &tierRecorder{
		inner:  inner,
		tierer: inner.(HostTierer),
		tiers:  make(map[*HostInfo]map[uint]int),
	}
}

// HostTier records and returns the wrapped policy's tier for host.
//
// Parameters:
//   - host: the host being classified
//
// Returns:
//   - uint: the tier the wrapped rack-aware policy assigned
func (r *tierRecorder) HostTier(host *HostInfo) uint {
	tier := r.tierer.HostTier(host)
	r.mu.Lock()
	seen := r.tiers[host]
	if seen == nil {
		seen = make(map[uint]int, 3)
		r.tiers[host] = seen
	}
	seen[tier]++
	r.mu.Unlock()
	return tier
}

// MaxHostTier returns the wrapped policy's maximum tier.
//
// Returns:
//   - uint: the highest tier HostTier can return
func (r *tierRecorder) MaxHostTier() uint { return r.tierer.MaxHostTier() }

// Init forwards session attachment to the wrapped policy.
func (r *tierRecorder) Init(s *Session) { r.inner.Init(s) }

// KeyspaceChanged forwards a keyspace event to the wrapped policy.
func (r *tierRecorder) KeyspaceChanged(e KeyspaceUpdateEvent) { r.inner.KeyspaceChanged(e) }

// SetPartitioner forwards the partitioner name to the wrapped policy.
func (r *tierRecorder) SetPartitioner(p string) { r.inner.SetPartitioner(p) }

// IsLocal forwards the locality question to the wrapped policy.
//
// Returns:
//   - bool: whether the wrapped policy considers host local
func (r *tierRecorder) IsLocal(host *HostInfo) bool { return r.inner.IsLocal(host) }

// Pick forwards host selection to the wrapped policy.
//
// Returns:
//   - NextHost: the wrapped policy's iterator
func (r *tierRecorder) Pick(s ExecutableStatement) NextHost { return r.inner.Pick(s) }

// AddHost forwards host registration to the wrapped policy.
func (r *tierRecorder) AddHost(host *HostInfo) { r.inner.AddHost(host) }

// RemoveHost forwards host removal to the wrapped policy.
func (r *tierRecorder) RemoveHost(host *HostInfo) { r.inner.RemoveHost(host) }

// HostUp forwards an up event to the wrapped policy.
func (r *tierRecorder) HostUp(host *HostInfo) { r.inner.HostUp(host) }

// HostDown forwards a down event to the wrapped policy.
func (r *tierRecorder) HostDown(host *HostInfo) { r.inner.HostDown(host) }

// Reset drops every recorded observation, so a test measures only what happened
// after its fixture was built.
func (r *tierRecorder) Reset() {
	r.mu.Lock()
	r.tiers = make(map[*HostInfo]map[uint]int)
	r.mu.Unlock()
}

// observedHosts returns the set of hosts the recorder was asked to classify.
//
// Returns:
//   - map[*HostInfo]struct{}: a copy, safe to inspect while readers run
func (r *tierRecorder) observedHosts() map[*HostInfo]struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[*HostInfo]struct{}, len(r.tiers))
	for host := range r.tiers {
		out[host] = struct{}{}
	}
	return out
}

// observedTiers returns how often each tier was reported for host.
//
// Parameters:
//   - host: the host to report on
//
// Returns:
//   - map[uint]int: tier to observation count, a copy
func (r *tierRecorder) observedTiers(host *HostInfo) map[uint]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[uint]int, 3)
	for tier, n := range r.tiers[host] {
		out[tier] = n
	}
	return out
}

// tierFixtureReplicas is the replication factor, and so the number of replicas
// the fixture's routing key resolves to.
const tierFixtureReplicas = 3

// tierSnapshotA is the changing host's starting identity: nothing published, so
// the rack-aware fallback puts it in tier 2.
//
// Returns:
//   - *hostIdentity: a fresh snapshot, ready to publish
func tierSnapshotA() *hostIdentity { return &hostIdentity{missingRack: true} }

// tierSnapshotB is the changing host's filled identity: the local data center
// and the local rack, so the rack-aware fallback puts it in tier 0.
//
// Returns:
//   - *hostIdentity: a fresh snapshot, ready to publish
func tierSnapshotB() *hostIdentity {
	return &hostIdentity{hostId: "h0", dataCenter: "dc1", rack: "r1"}
}

// newTierFixture builds a token-aware policy over six ordered-token hosts whose
// first three are the replicas for the query's routing key.
//
// hosts[0] is the host the test changes: it starts on snapshot A, tier 2, and
// the fill or the flip moves it to snapshot B, tier 0. The other five are
// already local, so any tier the recorder reports for hosts[0] is unambiguous.
//
// Parameters:
//   - t: the test, for the fixture's own assertions
//
// Returns:
//   - HostSelectionPolicy: the token-aware policy, hosts added and partitioner set
//   - *Query: a query whose routing key resolves to hosts[0..2]
//   - *tierRecorder: the fallback, tally reset
//   - []*HostInfo: the six hosts, in ring order
func newTierFixture(t *testing.T) (HostSelectionPolicy, *Query, *tierRecorder, []*HostInfo) {
	t.Helper()

	recorder := newTierRecorder("dc1", "r1")
	policy, query := setupShuffleTestPolicy(tierFixtureReplicas, recorder, ShuffleReplicas())

	tokens := []string{"10", "20", "30", "40", "50", "60"}
	hosts := make([]*HostInfo, len(tokens))
	for i, tok := range tokens {
		host := &HostInfo{connectAddress: net.IPv4(10, 0, 0, byte(i+1)), tokens: []string{tok}}
		if i == 0 {
			publishIdentity(host, tierSnapshotA())
		} else {
			publishIdentity(host, &hostIdentity{
				hostId:     fmt.Sprintf("h%d", i),
				dataCenter: "dc1",
				rack:       "r1",
			})
		}
		hosts[i] = host
		policy.AddHost(host)
	}
	policy.SetPartitioner("OrderedPartitioner")

	// "05" sorts before every token, so the ordered ring walks from token "10"
	// and SimpleStrategy RF=3 makes hosts[0..2] the replicas.
	query.RoutingKey([]byte("05"))

	recorder.Reset()
	return policy, query, recorder, hosts
}

// pickObserver accumulates every rule the readers saw Pick break.
//
// Counters rather than flags: the mutation runs need how often a rule broke, and
// a single latched boolean would throw that away.
type pickObserver struct {
	picks      atomic.Int64
	duplicates atomic.Int64
	unknown    atomic.Int64
	longPrefix atomic.Int64
	shortDrain atomic.Int64
	overruns   atomic.Int64
	panics     atomic.Int64
}

// drainPick runs one Pick to exhaustion and records every rule it breaks.
//
// The prefix - everything returned before the first non-replica - holds only
// replicas by the loop's own construction; what is checked is that it is no
// longer than the replica set.
// The full drain may continue into registered non-replicas,
// because the token-aware policy hands over to its fallback once the replicas
// run out,
// but it must never repeat a host, never produce a host nobody registered,
// and must end at nil.
//
// The drain is bounded at one yield per registered host plus the terminating nil
// call, and it returns the moment a yield is invalid.
// A correct iterator needs exactly that many calls, so the bound never trips on
// a healthy run;
// an iterator that kept yielding would otherwise spin here forever, holding up
// the reader's ready signal and never seeing done.
// Tripping the bound is itself a recorded rule break - see overruns - so a
// spinning iterator fails the test instead of merely no longer hanging it.
//
// Parameters:
//   - policy: the token-aware policy under test
//   - query: the query whose routing key names the replicas
//   - replicas: the replica set for that routing key
//   - registered: every host added to the policy
//   - obs: where the broken rules are recorded
func drainPick(policy HostSelectionPolicy, query *Query, replicas, registered map[*HostInfo]struct{},
	obs *pickObserver) {
	defer func() {
		if r := recover(); r != nil {
			obs.panics.Add(1)
		}
	}()

	iter := policy.Pick(newInternalQuery(query, nil))
	obs.picks.Add(1)

	seen := make(map[*HostInfo]struct{}, len(registered))
	prefix := 0
	inPrefix := true
	for i := 0; i <= len(registered); i++ {
		selected := iter()
		if selected == nil {
			if prefix > len(replicas) {
				obs.longPrefix.Add(1)
			}
			if len(seen) != len(registered) {
				obs.shortDrain.Add(1)
			}
			return
		}
		host := selected.Info()
		if _, dup := seen[host]; dup {
			obs.duplicates.Add(1)
			return
		}
		seen[host] = struct{}{}
		if _, ok := registered[host]; !ok {
			obs.unknown.Add(1)
			return
		}
		if inPrefix {
			if _, ok := replicas[host]; ok {
				prefix++
			} else {
				inPrefix = false
			}
		}
	}
	obs.overruns.Add(1)
}

// reportPickObservations turns the accumulated rule breaks into failures.
//
// Parameters:
//   - t: the test
//   - obs: the accumulated observations
//   - context: which phase produced them
func reportPickObservations(t *testing.T, obs *pickObserver, context string) {
	t.Helper()
	if n := obs.picks.Load(); n == 0 {
		t.Fatalf("%s: no Pick ran", context)
	}
	if n := obs.panics.Load(); n != 0 {
		t.Errorf("%s: Pick panicked %d times", context, n)
	}
	if n := obs.duplicates.Load(); n != 0 {
		t.Errorf("%s: Pick returned a host twice %d times", context, n)
	}
	if n := obs.unknown.Load(); n != 0 {
		t.Errorf("%s: Pick returned an unregistered host %d times", context, n)
	}
	if n := obs.longPrefix.Load(); n != 0 {
		t.Errorf("%s: the replica prefix was longer than the replica set %d times", context, n)
	}
	if n := obs.shortDrain.Load(); n != 0 {
		t.Errorf("%s: a full drain did not yield every registered host %d times", context, n)
	}
	if n := obs.overruns.Load(); n != 0 {
		t.Errorf("%s: Pick kept yielding past the registered host count %d times", context, n)
	}
}

// reportChangingHostTiers fails unless every tier observed for the changing host
// is one it was actually in.
//
// Parameters:
//   - t: the test
//   - recorder: the fallback that classified it
//   - host: the changing host
//   - context: which phase produced the observations
func reportChangingHostTiers(t *testing.T, recorder *tierRecorder, host *HostInfo, context string) {
	t.Helper()
	tiers := recorder.observedTiers(host)
	if len(tiers) == 0 {
		t.Fatalf("%s: the classify loop never reached the changing host", context)
	}
	for tier, n := range tiers {
		if tier != 0 && tier != 2 {
			t.Errorf("%s: the classify loop saw the impossible tier %d %d times "+
				"(the new data center paired with the old rack)", context, tier, n)
		}
	}
}

// TestTokenAwarePolicy_PickDuringIdentityFill runs the token-aware classify loop
// against a host whose identity changes underneath it.
//
// The fallback is rack-aware and local to dc1/r1. The changing host moves between
// snapshot A ("", "", missing rack), tier 2, and snapshot B (dc1, r1), tier 0.
// Tier 1 is the impossible one: it needs B's data center with A's rack, which no
// publication ever held.
// The mirror tear, A's data center with B's rack,
// still reads as tier 2 and cannot be told apart from A, so it is not asserted on.
//
// The first subtest proves the classify loop is actually reached, so a later
// green result cannot come from Pick returning early.
func TestTokenAwarePolicy_PickDuringIdentityFill(t *testing.T) {
	replicaAndRegisteredSets := func(hosts []*HostInfo) (replicas, registered map[*HostInfo]struct{}) {
		replicas = make(map[*HostInfo]struct{}, tierFixtureReplicas)
		for _, host := range hosts[:tierFixtureReplicas] {
			replicas[host] = struct{}{}
		}
		registered = make(map[*HostInfo]struct{}, len(hosts))
		for _, host := range hosts {
			registered[host] = struct{}{}
		}
		return replicas, registered
	}

	t.Run("the classify loop reaches the replicas", func(t *testing.T) {
		policy, query, recorder, hosts := newTierFixture(t)

		policy.Pick(newInternalQuery(query, nil))

		got := recorder.observedHosts()
		want, _ := replicaAndRegisteredSets(hosts)
		if len(got) != len(want) {
			t.Fatalf("the classify loop asked about %d hosts, want the %d replicas", len(got), len(want))
		}
		for host := range want {
			if _, ok := got[host]; !ok {
				t.Fatalf("the classify loop never asked about replica %v", host.ConnectAddress())
			}
		}
	})

	t.Run("a legal fill during Pick", func(t *testing.T) {
		const trials = 200

		var obs pickObserver
		for trial := 0; trial < trials; trial++ {
			policy, query, recorder, hosts := newTierFixture(t)
			replicas, registered := replicaAndRegisteredSets(hosts)

			s := newIdentityStress(t)
			s.readers(runtime.GOMAXPROCS(0), func(last bool) {
				drainPick(policy, query, replicas, registered, &obs)
			})
			s.write(func() {
				from := &HostInfo{connectAddress: net.IPv4(10, 0, 0, 100)}
				publishIdentity(from, tierSnapshotB())
				hosts[0].update(from)
			})
			s.wait()

			context := fmt.Sprintf("T7a trial %d", trial)
			reportChangingHostTiers(t, recorder, hosts[0], context)
			requireIdentity(t, hosts[0], *tierSnapshotB(), context+" after the fill")
			if t.Failed() {
				break
			}
		}
		reportPickObservations(t, &obs, "T7a")
		t.Logf("T7a: %d trials, %d Picks drained", trials, obs.picks.Load())
	})

	t.Run("flipping identities during Pick", func(t *testing.T) {
		policy, query, recorder, hosts := newTierFixture(t)
		replicas, registered := replicaAndRegisteredSets(hosts)
		snapA, snapB := tierSnapshotA(), tierSnapshotB()

		var obs pickObserver
		s := newIdentityStress(t)
		s.readers(runtime.GOMAXPROCS(0), func(last bool) {
			drainPick(policy, query, replicas, registered, &obs)
		})
		s.write(func() {
			for i := 0; i < identityFlips; i++ {
				publishIdentity(hosts[0], snapA)
				publishIdentity(hosts[0], snapB)
			}
		})
		s.wait()

		t.Logf("T7b: tiers observed for the changing host: %v", recorder.observedTiers(hosts[0]))
		reportChangingHostTiers(t, recorder, hosts[0], "T7b")
		reportPickObservations(t, &obs, "T7b")
		t.Logf("T7b: %d flips, %d Picks drained", identityFlips, obs.picks.Load())
	})
}

// ---------------------------------------------------------------------------
// T8 - I1: replicaMap captures each host's identity once
// ---------------------------------------------------------------------------

// replicaMapBuilds is how many builds each replica-map stress phase runs.
const replicaMapBuilds = 1000

// noReplicasPanicPrefix is what replicaMap panics with when a token ends up with
// no replica at all.
// Matched as a prefix: the message carries the token.
const noReplicasPanicPrefix = "no replicas for token"

// singleHostRing builds a one-host, one-token ring for the replica map.
//
// Parameters:
//   - host: the only host in the ring
//
// Returns:
//   - *tokenRing: a ring whose single token names host
func singleHostRing(host *HostInfo) *tokenRing {
	return &tokenRing{
		hosts:  []*HostInfo{host},
		tokens: []hostToken{{token: orderedToken("10"), host: host}},
	}
}

// safeReplicaMap builds the replica map and turns the "no replicas" panic into a
// value, so a stress loop can count it instead of aborting the test binary.
//
// Parameters:
//   - strat: the network topology strategy
//   - ring: the token ring to build against
//
// Returns:
//   - tokenRingReplicas: the map, or nil when the build panicked
//   - bool: true when the build panicked with "no replicas for token"
//   - interface{}: any other panic value, so the caller can report it
func safeReplicaMap(strat *networkTopology, ring *tokenRing) (replicas tokenRingReplicas,
	noReplicas bool, other interface{}) {
	defer func() {
		if r := recover(); r != nil {
			if strings.HasPrefix(fmt.Sprint(r), noReplicasPanicPrefix) {
				noReplicas = true
				return
			}
			other = r
		}
	}()

	replicas = strat.replicaMap(ring)
	return replicas, false, nil
}

// replicaShape names which of the accepted results a build produced, without
// reading anything back off the host.
//
// The returned hosts are mutable and go on changing after the build, so what
// they report is no evidence of which snapshot the build used; only the shape of
// the result is.
//
// Parameters:
//   - replicas: the built map
//   - host: the ring's only host
//
// Returns:
//   - string: "empty", "one entry", or a description of what was unexpected
func replicaShape(replicas tokenRingReplicas, host *HostInfo) string {
	switch {
	case len(replicas) == 0:
		return "empty"
	case len(replicas) == 1 && len(replicas[0].hosts) == 1 && replicas[0].hosts[0] == host:
		return "one entry"
	default:
		return fmt.Sprintf("unexpected (%d entries)", len(replicas))
	}
}

// TestNetworkTopology_ReplicaMapCapturesIdentityOnce asserts building the replica
// map is one logical decision that reads each host once.
//
// The build reads every host three times - the rack inventory, the primary data
// center check, and the selection loop - and an update landing between two of
// them would let the inventory file a host under one rack and the selection loop
// look it up under another, which reads as an unknown rack.
//
// The static controls fix what each snapshot must produce on its own; the stress
// then accepts only those two shapes.
//
// The ready handshake is stress scheduling, not a cross-pass barrier: it only
// establishes that the writer was running before the build started,
// never that a publication landed between two particular passes.
// So a green run is no proof the tear is impossible, and the detector this test
// is credited with is the A'/B' variant,
// where both snapshots sit in the strategy's data center and a torn build has
// nowhere to file the host but panic.
// How often that variant actually fires against the M-f mutation is measured and
// recorded, not assumed.
func TestNetworkTopology_ReplicaMapCapturesIdentityOnce(t *testing.T) {
	strat := newNetworkTopology(map[string]int{"remote": 1})

	// A publishes no data center, which is not in the strategy,
	// so the token is skipped and the map comes out empty.
	// B is in the strategy and produces exactly one entry naming the host.
	snapA := &hostIdentity{missingRack: true}
	snapB := &hostIdentity{dataCenter: "remote", rack: "r"}

	t.Run("frozen A yields an empty map", func(t *testing.T) {
		host := identityHost(1, *snapA)
		replicas, noReplicas, other := safeReplicaMap(strat, singleHostRing(host))
		if other != nil {
			t.Fatalf("replicaMap panicked: %v", other)
		}
		if noReplicas {
			t.Fatal("a host outside the strategy's data centers must be skipped, not panic")
		}
		if got := replicaShape(replicas, host); got != "empty" {
			t.Fatalf("shape = %s, want empty", got)
		}
	})

	t.Run("frozen B yields one entry", func(t *testing.T) {
		host := identityHost(1, *snapB)
		replicas, noReplicas, other := safeReplicaMap(strat, singleHostRing(host))
		if other != nil {
			t.Fatalf("replicaMap panicked: %v", other)
		}
		if noReplicas {
			t.Fatal("a host in the strategy's data center must have a replica")
		}
		if got := replicaShape(replicas, host); got != "one entry" {
			t.Fatalf("shape = %s, want one entry", got)
		}
	})

	t.Run("flipping between A and B", func(t *testing.T) {
		shapes, panics := stressReplicaMap(t, strat, snapA, snapB)

		t.Logf("A/B: %d builds, shapes %v, %q panics %d",
			replicaMapBuilds, shapes, noReplicasPanicPrefix, panics)
		// An empty map is a legitimate result for A here, so it is not a hit;
		// any other shape, and any panic, is.
		if panics != 0 {
			t.Errorf("replicaMap panicked with %q %d times", noReplicasPanicPrefix, panics)
		}
		for name, n := range shapes {
			if name != "empty" && name != "one entry" {
				t.Errorf("replicaMap produced %s %d times", name, n)
			}
		}
	})

	t.Run("flipping between two racks in the same data center", func(t *testing.T) {
		// Both snapshots are in the strategy's data center,
		// so every build must produce one entry
		// and an empty map is no longer a legitimate result.
		// A torn build files the host under one rack and looks it up under the
		// other, finds a rack it does not know, skips the host and panics.
		rackA := &hostIdentity{dataCenter: "remote", rack: "r1"}
		rackB := &hostIdentity{dataCenter: "remote", rack: "r2"}

		shapes, panics := stressReplicaMap(t, strat, rackA, rackB)

		t.Logf("A'/B': %d builds, shapes %v, %q panics %d",
			replicaMapBuilds, shapes, noReplicasPanicPrefix, panics)
		if panics != 0 {
			t.Errorf("replicaMap panicked with %q %d times out of %d builds: "+
				"the rack inventory and the selection loop read different publications",
				noReplicasPanicPrefix, panics, replicaMapBuilds)
		}
		for name, n := range shapes {
			if name != "one entry" {
				t.Errorf("replicaMap produced %s %d times, want one entry every time", name, n)
			}
		}
	})
}

// stressReplicaMap builds the replica map replicaMapBuilds times, each time with
// a writer publishing the two snapshots alternately underneath it.
//
// Parameters:
//   - t: the test, for an unexpected panic
//   - strat: the network topology strategy
//   - first: the snapshot each build starts on
//   - second: the snapshot the writer alternates with
//
// Returns:
//   - map[string]int: how many builds produced each shape
//   - int: how many builds panicked with "no replicas for token"
func stressReplicaMap(t *testing.T, strat *networkTopology, first, second *hostIdentity) (map[string]int, int) {
	t.Helper()

	shapes := make(map[string]int, 2)
	panics := 0

	for build := 0; build < replicaMapBuilds; build++ {
		host := identityHost(1, hostIdentity{})
		publishIdentity(host, first)

		// The build waits for the flipper's first publication, so it cannot run
		// to completion before the writer was ever scheduled.
		stop, ready := startFlipper(host, first, second)
		<-ready
		replicas, noReplicas, other := safeReplicaMap(strat, singleHostRing(host))
		stop()

		if other != nil {
			t.Fatalf("build %d: replicaMap panicked: %v", build, other)
		}
		if noReplicas {
			panics++
			continue
		}
		shapes[replicaShape(replicas, host)]++
	}

	return shapes, panics
}

// startFlipper publishes the two snapshots on host alternately until the
// returned function is called, which also joins the goroutine.
//
// The returned channel is closed after the flipper's first publication, so a
// caller can start its own work knowing the flipper is already running rather
// than still waiting to be scheduled.
// Round 0 publishes first, which is what the host already carries, so waiting on
// it changes nothing the build can observe.
//
// Parameters:
//   - host: the host to publish on
//   - first: the snapshot published on even rounds
//   - second: the snapshot published on odd rounds
//
// Returns:
//   - func(): stops the flipper and waits for it; safe to call more than once
//   - <-chan struct{}: closed once the flipper has published at least once
func startFlipper(host *HostInfo, first, second *hostIdentity) (func(), <-chan struct{}) {
	done := make(chan struct{})
	ready := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var started sync.Once
		for round := 0; ; round++ {
			select {
			case <-done:
				return
			default:
			}
			if round%2 == 0 {
				publishIdentity(host, first)
			} else {
				publishIdentity(host, second)
			}
			started.Do(func() { close(ready) })
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() {
			close(done)
			wg.Wait()
		})
	}, ready
}

// ---------------------------------------------------------------------------
// T9 - I5: published snapshots are immutable
// ---------------------------------------------------------------------------

// TestHostInfo_PublishedSnapshotsAreImmutable asserts nothing ever writes through
// a snapshot a reader can already be holding.
//
// The shared emptyHostIdentity is the sharpest case: every host that published
// nothing hands out the same pointer, so a single writer mutating it in place
// would change what every one of them reports.
//
// Each writer is then taken on its own, against both a published and a
// never-published destination.
// The retained pointer is taken immediately before exactly one writer call, so
// what the assertions constrain is that call and no other: a writer that mutated
// the snapshot it found would otherwise be covered by whichever writer ran
// before it.
func TestHostInfo_PublishedSnapshotsAreImmutable(t *testing.T) {
	emptyBefore := emptyHostIdentity
	unpublished := &HostInfo{connectAddress: net.IPv4(10, 0, 0, 9)}
	if unpublished.identity() != &emptyHostIdentity {
		t.Fatal("an unpublished host must hand out the shared emptyHostIdentity")
	}

	host := identityHost(1, hostIdentity{missingRack: true})
	published := host.identity()
	publishedBefore := *published

	host.setHostID("h")
	host.update(identityHost(2, hostIdentity{hostId: "other", dataCenter: "dc", rack: "r"}))

	if emptyHostIdentity != emptyBefore {
		t.Errorf("emptyHostIdentity changed to %+v, want %+v", emptyHostIdentity, emptyBefore)
	}
	if unpublished.identity() != &emptyHostIdentity {
		t.Error("the unpublished host stopped reporting the shared emptyHostIdentity")
	}
	if got := *unpublished.identity(); got != (hostIdentity{}) {
		t.Errorf("the unpublished host now reports %+v, want the zero value", got)
	}
	if *published != publishedBefore {
		t.Errorf("a snapshot a reader was holding changed to %+v, want %+v", *published, publishedBefore)
	}
	if host.ident.Load() == published {
		t.Error("the writers must have published new snapshots, not mutated the old one")
	}

	// One more publication, over a snapshot captured after the first two.
	held := host.identity()
	heldBefore := *held
	host.setHostID("final")
	if *held != heldBefore {
		t.Errorf("the held snapshot changed to %+v, want %+v", *held, heldBefore)
	}
	if got := host.HostID(); got != "final" {
		t.Errorf("HostID() = %q, want %q", got, "final")
	}

	// One writer per case, with the pointer retained immediately before it, so
	// each case constrains that writer alone.
	// The subtests share emptyHostIdentity, so none of them runs in parallel.

	t.Run("setHostID on a published host", func(t *testing.T) {
		dest := identityHost(1, hostIdentity{dataCenter: "dc", rack: "r"})
		retained := dest.identity()
		before := *retained

		dest.setHostID("h")

		requireRetainedSnapshot(t, retained, before, "setHostID on a published host")
		requireIdentity(t, dest, hostIdentity{hostId: "h", dataCenter: "dc", rack: "r"},
			"after setHostID on a published host")
		if dest.ident.Load() == retained {
			t.Error("setHostID must publish a new snapshot, not mutate the retained one")
		}
	})

	t.Run("update on a published host", func(t *testing.T) {
		// The destination has a gap in every field the source fills, so update
		// has something to do and must publish.
		dest := identityHost(1, hostIdentity{missingRack: true})
		retained := dest.identity()
		before := *retained

		dest.update(identityHost(2, hostIdentity{hostId: "h", dataCenter: "dc", rack: "r"}))

		requireRetainedSnapshot(t, retained, before, "update on a published host")
		requireIdentity(t, dest, hostIdentity{hostId: "h", dataCenter: "dc", rack: "r"},
			"after update on a published host")
		if dest.ident.Load() == retained {
			t.Error("update must publish a new snapshot, not mutate the retained one")
		}
	})

	t.Run("setHostID on a never-published host", func(t *testing.T) {
		emptyWas := emptyHostIdentity
		dest := &HostInfo{connectAddress: net.IPv4(10, 0, 0, 1)}
		retained := dest.identity()
		if retained != &emptyHostIdentity {
			t.Fatal("a never-published host must hand out the shared emptyHostIdentity")
		}

		dest.setHostID("h")

		requireRetainedSnapshot(t, retained, emptyWas, "setHostID on a never-published host")
		if emptyHostIdentity != emptyWas {
			t.Errorf("emptyHostIdentity changed to %+v, want %+v", emptyHostIdentity, emptyWas)
		}
		requireIdentity(t, dest, hostIdentity{hostId: "h"}, "after setHostID on a never-published host")
		if dest.ident.Load() == &emptyHostIdentity {
			t.Error("setHostID must publish its own snapshot, not the shared empty one")
		}
	})

	t.Run("update on a never-published host", func(t *testing.T) {
		emptyWas := emptyHostIdentity
		dest := &HostInfo{connectAddress: net.IPv4(10, 0, 0, 1)}
		retained := dest.identity()
		if retained != &emptyHostIdentity {
			t.Fatal("a never-published host must hand out the shared emptyHostIdentity")
		}

		dest.update(identityHost(2, hostIdentity{hostId: "h", dataCenter: "dc", rack: "r"}))

		requireRetainedSnapshot(t, retained, emptyWas, "update on a never-published host")
		if emptyHostIdentity != emptyWas {
			t.Errorf("emptyHostIdentity changed to %+v, want %+v", emptyHostIdentity, emptyWas)
		}
		// The zero identity has missingRack false, so its empty rack is an
		// existing empty rack and the source's rack is not a legal fill; the host
		// id and the data center are.
		requireIdentity(t, dest, hostIdentity{hostId: "h", dataCenter: "dc"},
			"after update on a never-published host")
		if dest.ident.Load() == &emptyHostIdentity {
			t.Error("update must publish its own snapshot, not the shared empty one")
		}
	})
}

// requireRetainedSnapshot fails the test unless a snapshot retained across a
// write still holds what it held before it.
//
// Parameters:
//   - t: the test
//   - retained: the pointer taken before the write
//   - want: what it held then
//   - context: which write it was retained across, for the failure message
func requireRetainedSnapshot(t *testing.T, retained *hostIdentity, want hostIdentity, context string) {
	t.Helper()
	if *retained != want {
		t.Errorf("%s: the retained snapshot changed to %+v, want %+v", context, *retained, want)
	}
}
