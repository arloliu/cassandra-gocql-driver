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

// ownershipBudget bounds every positive wait in these tests.
const ownershipBudget = 10 * time.Second

// absenceWindow is how long a "must not happen" assertion watches.
const absenceWindow = 300 * time.Millisecond

// policyCall is one membership call recorded by recordingPolicy.
type policyCall struct {
	op    string
	host  *HostInfo
	owned bool
	seq   int64
}

// recordingPolicy wraps the pause-recovery fixture's collector and records every
// membership call, with whether the ring owned the host at that moment, so a
// test can assert the order in which publication and removal happened.
//
// A gate can be armed per operation to block inside the call; the call is then
// blocked while holding hostPublishMu, which is exactly the state the D4c
// serialisation tests need to observe.
type recordingPolicy struct {
	*hostStateCollector

	session atomic.Pointer[Session]
	seq     *atomic.Int64

	mu      sync.Mutex
	calls   []policyCall
	gates   map[string]chan struct{}
	entered chan policyCall
}

var _ HostSelectionPolicy = (*recordingPolicy)(nil)

func newRecordingPolicy(inner *hostStateCollector, seq *atomic.Int64) *recordingPolicy {
	return &recordingPolicy{
		hostStateCollector: inner,
		seq:                seq,
		gates:              map[string]chan struct{}{},
		entered:            make(chan policyCall, 64),
	}
}

// Init captures the session so calls can record ring ownership.
func (r *recordingPolicy) Init(s *Session) {
	r.session.Store(s)
	r.hostStateCollector.Init(s)
}

// armGate makes the next calls of op block until releaseGate.
func (r *recordingPolicy) armGate(op string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gates[op] = make(chan struct{})
}

// releaseGate unblocks op; safe to call twice.
func (r *recordingPolicy) releaseGate(op string) {
	r.mu.Lock()
	gate := r.gates[op]
	delete(r.gates, op)
	r.mu.Unlock()
	if gate != nil {
		close(gate)
	}
}

func (r *recordingPolicy) record(op string, host *HostInfo) {
	owned := false
	if s := r.session.Load(); s != nil {
		owned = s.ring.owns(host)
	}
	call := policyCall{op: op, host: host, owned: owned, seq: r.seq.Add(1)}

	r.mu.Lock()
	r.calls = append(r.calls, call)
	gate := r.gates[op]
	r.mu.Unlock()

	select {
	case r.entered <- call:
	default:
	}
	if gate != nil {
		<-gate
	}
}

// AddHost records the call, then forwards it to the fixture collector.
func (r *recordingPolicy) AddHost(host *HostInfo) {
	r.record("AddHost", host)
	r.hostStateCollector.AddHost(host)
}

// RemoveHost records the call, then forwards it to the fixture collector.
func (r *recordingPolicy) RemoveHost(host *HostInfo) {
	r.record("RemoveHost", host)
	r.hostStateCollector.RemoveHost(host)
}

// HostUp records the call, then forwards it to the fixture collector.
func (r *recordingPolicy) HostUp(host *HostInfo) {
	r.record("HostUp", host)
	r.hostStateCollector.HostUp(host)
}

// HostDown records the call, then forwards it to the fixture collector.
func (r *recordingPolicy) HostDown(host *HostInfo) {
	r.record("HostDown", host)
	r.hostStateCollector.HostDown(host)
}

// drainEntered discards queued entry events, so a later awaitEntered cannot
// match a call made during session initialisation.
func (r *recordingPolicy) drainEntered() {
	for {
		select {
		case <-r.entered:
		default:
			return
		}
	}
}

// mark returns the current history length, for callsSince.
func (r *recordingPolicy) mark() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

// callsSince returns the ops recorded for host after mark, in order.
func (r *recordingPolicy) callsSince(mark int, host *HostInfo) []string {
	var ops []string
	for _, c := range r.history()[mark:] {
		if c.host == host {
			ops = append(ops, c.op)
		}
	}
	return ops
}

// history returns a copy of every recorded call.
func (r *recordingPolicy) history() []policyCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]policyCall(nil), r.calls...)
}

// callsFor returns the recorded ops for host, in order.
func (r *recordingPolicy) callsFor(host *HostInfo) []string {
	var ops []string
	for _, c := range r.history() {
		if c.host == host {
			ops = append(ops, c.op)
		}
	}
	return ops
}

// awaitEntered waits until op is called for host.
//
// Returns:
//   - policyCall: the matching call
func (r *recordingPolicy) awaitEntered(t *testing.T, op string, host *HostInfo) policyCall {
	t.Helper()

	deadline := time.After(ownershipBudget)
	for {
		select {
		case call := <-r.entered:
			if call.op == op && (host == nil || call.host == host) {
				return call
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s(%v)", op, host)
			return policyCall{}
		}
	}
}

// poolEventRecord is one testPoolHook checkpoint.
type poolEventRecord struct {
	ev   poolEvent
	host *HostInfo
}

// poolHooks routes ClusterConfig.testPoolHook into a channel and an optional
// per-host block on the connect attempt.
type poolHooks struct {
	events chan poolEventRecord

	mu          sync.Mutex
	blockDialOf *HostInfo
	dialGate    chan struct{}
	dialBlocked chan struct{}
}

func newPoolHooks() *poolHooks {
	return &poolHooks{events: make(chan poolEventRecord, 256)}
}

func (h *poolHooks) hook(ev poolEvent, host *HostInfo) {
	select {
	case h.events <- poolEventRecord{ev: ev, host: host}:
	default:
	}
	if ev != poolConnectAttempt {
		return
	}
	h.mu.Lock()
	target, gate, blocked := h.blockDialOf, h.dialGate, h.dialBlocked
	h.mu.Unlock()
	if target != nil && host == target {
		select {
		case blocked <- struct{}{}:
		default:
		}
		<-gate
	}
}

// blockDial makes the next connect attempts for host block until releaseDial.
func (h *poolHooks) blockDial(host *HostInfo) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.blockDialOf = host
	h.dialGate = make(chan struct{})
	h.dialBlocked = make(chan struct{}, 8)
}

// awaitDialBlocked waits until a connect attempt for the blocked host is parked.
func (h *poolHooks) awaitDialBlocked(t *testing.T) {
	t.Helper()

	h.mu.Lock()
	blocked := h.dialBlocked
	h.mu.Unlock()
	select {
	case <-blocked:
	case <-time.After(ownershipBudget):
		t.Fatal("timed out waiting for the dial to block")
	}
}

// releaseDial unblocks the parked dial; safe to call twice.
func (h *poolHooks) releaseDial() {
	h.mu.Lock()
	gate := h.dialGate
	h.blockDialOf, h.dialGate = nil, nil
	h.mu.Unlock()
	if gate != nil {
		close(gate)
	}
}

// drain returns every event queued so far without waiting for more.
func (h *poolHooks) drain() []poolEventRecord {
	var out []poolEventRecord
	for {
		select {
		case rec := <-h.events:
			out = append(out, rec)
		default:
			return out
		}
	}
}

// await waits for ev to fire for host, discarding other events.
func (h *poolHooks) await(t *testing.T, ev poolEvent, host *HostInfo, what string) {
	t.Helper()

	deadline := time.After(ownershipBudget)
	for {
		select {
		case rec := <-h.events:
			if rec.ev == ev && rec.host == host {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// ownershipFixture is a pause-recovery session whose policy records membership
// calls and whose pool checkpoints are observable.
type ownershipFixture struct {
	session   *Session
	host      *HostInfo
	collector *hostStateCollector
	recorder  *recordingPolicy
	hooks     *poolHooks
	gate      *gatedDialer

	// connected receives every host whose handleNodeConnected callback returned,
	// so a test can join that asynchronous callback.
	connected chan *HostInfo
}

// awaitConnectedCallback waits until handleNodeConnected returned for host.
func (f *ownershipFixture) awaitConnectedCallback(t *testing.T, host *HostInfo, what string) {
	t.Helper()

	deadline := time.After(ownershipBudget)
	for {
		select {
		case h := <-f.connected:
			if h == host {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// newOwnershipFixture builds the fixture and waits for the host's initial fill
// to complete, not only for its UP, so no fill is in flight when the test starts.
func newOwnershipFixture(t *testing.T) *ownershipFixture {
	t.Helper()

	hooks := newPoolHooks()
	gate := &gatedDialer{}
	var seq atomic.Int64
	var recorder *recordingPolicy
	session, host, collector := newPauseRecoverySession(t, func(cluster *ClusterConfig) {
		inner, ok := cluster.PoolConfig.HostSelectionPolicy.(*hostStateCollector)
		require.True(t, ok, "the pause-recovery fixture installs a hostStateCollector")
		recorder = newRecordingPolicy(inner, &seq)
		cluster.PoolConfig.HostSelectionPolicy = recorder
		cluster.Dialer = gate
		cluster.ReconnectionPolicy = &ConstantReconnectionPolicy{MaxRetries: 1, Interval: time.Millisecond}
		cluster.ReconnectInterval = 0
		cluster.testPoolHook = hooks.hook
	})
	t.Cleanup(hooks.releaseDial)

	hooks.await(t, poolFillDone, host, "the fixture host's initial fill to complete")
	// newPauseRecoverySession already observed the initial UP, which the recorder
	// entered before the collector published it, so every initialisation event
	// is queued by now; drop them so the test cannot mistake one for its own.
	recorder.drainEntered()

	connected := make(chan *HostInfo, 64)
	session.testAfterNodeConnected = func(h *HostInfo) {
		select {
		case connected <- h:
		default:
		}
	}

	return &ownershipFixture{session: session, host: host, collector: collector, recorder: recorder, hooks: hooks, gate: gate, connected: connected}
}

// replacement returns a ring-owned replacement object for the fixture host:
// same host ID, a second loopback address, backed by its own server.
func (f *ownershipFixture) replacement(t *testing.T) *HostInfo {
	t.Helper()

	b := newReplacementHost(t, f.host)
	f.session.ring.removeHost(f.host.HostID())
	_, existed := f.session.ring.addHostIfMissing(b)
	require.False(t, existed, "the replacement must take the freed host ID")
	return b
}

// isPoolClosed reads pool.closed under its mutex.
func isPoolClosed(pool *hostConnPool) bool {
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	return pool.closed
}

// requireEventuallyClosed waits for the pool to be closed.
func requireEventuallyClosed(t *testing.T, pool *hostConnPool, what string) {
	t.Helper()
	require.Eventually(t, func() bool { return isPoolClosed(pool) }, ownershipBudget, 10*time.Millisecond, what)
}

// requireNotReturned asserts done is still open after absenceWindow.
func requireNotReturned(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
		t.Fatalf("%s returned but must still be blocked", what)
	case <-time.After(absenceWindow):
	}
}

// awaitDone waits for done or fails.
func awaitDone(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(ownershipBudget):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// TestPoolAddHost_ReplacesPoolWhenRingOwnsNewObject: a pool registered for a
// superseded object is replaced when the ring's current object is admitted.
func TestPoolAddHost_ReplacesPoolWhenRingOwnsNewObject(t *testing.T) {
	f := newOwnershipFixture(t)
	oldPool, ok := f.session.pool.getPoolFor(f.host)
	require.True(t, ok, "the fixture host must have a pool")

	b := f.replacement(t)
	f.session.pool.addHost(b)

	_, ok = f.session.pool.getPoolFor(b)
	require.True(t, ok, "the replacement's pool must be registered")
	_, ok = f.session.pool.getPoolFor(f.host)
	require.False(t, ok, "the superseded object's pool must be gone")
	requireEventuallyClosed(t, oldPool, "the superseded pool must be closed")

	f.hooks.await(t, poolFillDone, b, "the replacement's fill to complete")
	newPool, _ := f.session.pool.getPoolFor(b)
	require.NotNil(t, newPool.Pick(), "the replacement's pool must serve")
}

// TestPoolAddHost_RejectsStaleObjectAfterReplacement: admitting the superseded
// object again leaves the replacement's pool untouched.
func TestPoolAddHost_RejectsStaleObjectAfterReplacement(t *testing.T) {
	f := newOwnershipFixture(t)
	b := f.replacement(t)
	f.session.pool.addHost(b)
	f.hooks.await(t, poolFillDone, b, "the replacement's fill to complete")
	bPool, ok := f.session.pool.getPoolFor(b)
	require.True(t, ok)

	f.session.pool.addHost(f.host)

	again, ok := f.session.pool.getPoolFor(b)
	require.True(t, ok, "the replacement's pool must still be registered")
	require.Same(t, bPool, again, "the replacement's pool must be the same object")
	_, ok = f.session.pool.getPoolFor(f.host)
	require.False(t, ok, "no pool may be registered for the superseded object")
	require.NotNil(t, bPool.Pick(), "the replacement must still serve")
}

// TestPoolAddHost_RejectsUnownedObjectWithNoPool: with no pool registered and
// the ring not owning the object, nothing is registered.
func TestPoolAddHost_RejectsUnownedObjectWithNoPool(t *testing.T) {
	f := newOwnershipFixture(t)
	f.session.pool.removeHost(f.host)
	_, ok := f.session.pool.getPool(f.host)
	require.False(t, ok, "the pool must be unregistered before the test acts")
	f.session.ring.removeHost(f.host.HostID())

	f.session.pool.addHost(f.host)

	_, ok = f.session.pool.getPool(f.host)
	require.False(t, ok, "an object the ring does not own must not get a pool")
}

// TestPoolAddHost_SkipsFillOfUnownedObjectsOwnPool:
// an object the ring no longer owns is ignored even while its own pool is still registered,
// so no fill cycle runs on a pool removeHost is about to close.
//
// A fill on the fixture's full pool would run synchronously inside addHost:
// it would claim, find nothing to do and fire poolFillDone before addHost returned,
// so the event history is checked without waiting.
func TestPoolAddHost_SkipsFillOfUnownedObjectsOwnPool(t *testing.T) {
	f := newOwnershipFixture(t)
	a := f.host
	before, ok := f.session.pool.getPool(a)
	require.True(t, ok)
	f.session.ring.removeHost(a.HostID())
	f.hooks.drain()

	f.session.pool.addHost(a)

	require.Empty(t, f.hooks.drain(), "no fill checkpoint may fire for an unowned object")
	after, ok := f.session.pool.getPool(a)
	require.True(t, ok)
	require.Same(t, before, after, "the registered pool must be untouched")
}

// TestPoolAddHost_ThenRemoveHostLeavesNoOrphan: an admission that passed while the
// ring still owned the object is taken out again by removeHost.
func TestPoolAddHost_ThenRemoveHostLeavesNoOrphan(t *testing.T) {
	f := newOwnershipFixture(t)
	f.session.pool.removeHost(f.host)

	f.session.pool.addHost(f.host)
	f.hooks.await(t, poolFillDone, f.host, "the re-admitted pool's fill to complete")
	pool, ok := f.session.pool.getPoolFor(f.host)
	require.True(t, ok, "the ring still owns the host, so the pool must be registered")

	f.session.removeHost(f.host)

	_, ok = f.session.pool.getPool(f.host)
	require.False(t, ok, "removeHost must unregister the pool")
	requireEventuallyClosed(t, pool, "removeHost must close the pool")
}

// TestRemoveHost_RingFirst: by the time the policy and the pool see the removal,
// the ring no longer owns the object, and the policy sees it before the pool.
func TestRemoveHost_RingFirst(t *testing.T) {
	f := newOwnershipFixture(t)

	var notifySeq atomic.Int64
	var notifyOwned atomic.Bool
	f.session.pool.testAfterParentNotify = func() {
		notifyOwned.Store(f.session.ring.owns(f.host))
		notifySeq.Store(f.recorder.seq.Add(1))
	}

	f.session.removeHost(f.host)

	var removeCall *policyCall
	for _, c := range f.recorder.history() {
		if c.op == "RemoveHost" && c.host == f.host {
			c := c
			removeCall = &c
		}
	}
	require.NotNil(t, removeCall, "the policy must see RemoveHost")
	require.False(t, removeCall.owned, "the ring must have dropped the host before the policy is told")
	require.False(t, notifyOwned.Load(), "the ring must have dropped the host before the pool is unregistered")
	require.Less(t, removeCall.seq, notifySeq.Load(), "the policy must be told before the pool is unregistered")
}

// TestStartPoolFill_PublicationSerialisedWithRemoval: a publication holding
// hostPublishMu blocks removeHost, and removal follows it.
//
// removeHost takes the host out of the ring inside that mutex, so the ring entry
// survives for as long as the publication holds it: the removal is one
// transaction that waits, not a ring change that lands early and un-publishes
// later.
func TestStartPoolFill_PublicationSerialisedWithRemoval(t *testing.T) {
	f := newOwnershipFixture(t)
	f.session.pool.removeHost(f.host)
	mark := f.recorder.mark()

	f.recorder.armGate("AddHost")
	t.Cleanup(func() { f.recorder.releaseGate("AddHost") })

	filled := make(chan struct{})
	go func() {
		f.session.startPoolFill(f.host)
		close(filled)
	}()
	f.recorder.awaitEntered(t, "AddHost", f.host)

	removed := make(chan struct{})
	go func() {
		f.session.removeHost(f.host)
		close(removed)
	}()
	requireNotReturned(t, removed, "removeHost while a publication holds hostPublishMu")
	require.True(t, f.session.ring.owns(f.host),
		"the ring removal is inside the transaction, so it must not land while the publication holds the mutex")

	f.recorder.releaseGate("AddHost")
	awaitDone(t, filled, "startPoolFill")
	awaitDone(t, removed, "removeHost")
	require.False(t, f.session.ring.owns(f.host),
		"the ring removal must complete once the transaction gets the mutex")

	// The fill's own connected callback may publish HostUp before startPoolFill
	// reaches AddHost; that is ordinary. What the mutex guarantees is that the
	// removal ran after the gated publication and was the last transition.
	ops := f.recorder.callsSince(mark, f.host)
	require.Contains(t, ops, "AddHost", "history: %v", ops)
	require.Equal(t, "RemoveHost", ops[len(ops)-1], "the removal that waited must be the last transition; history: %v", ops)
	require.Equal(t, 1, countOf(ops, "RemoveHost"), "exactly one removal; history: %v", ops)
	require.NotContains(t, f.collector.hosts(t), f.host, "the host must not remain in the policy")
	_, ok := f.session.pool.getPool(f.host)
	require.False(t, ok, "no pool may remain")
}

// countOf returns how many times op appears in ops.
func countOf(ops []string, op string) int {
	n := 0
	for _, o := range ops {
		if o == op {
			n++
		}
	}
	return n
}

// TestStartPoolFill_StaleFillDoesNotPublish: a fill that passed the entry guard and
// then blocked in its first dial must not publish once the host was replaced.
func TestStartPoolFill_StaleFillDoesNotPublish(t *testing.T) {
	f := newOwnershipFixture(t)
	a := f.host
	f.session.pool.removeHost(a)
	mark := f.recorder.mark()

	f.hooks.blockDial(a)
	filled := make(chan struct{})
	go func() {
		f.session.startPoolFill(a)
		close(filled)
	}()
	f.hooks.awaitDialBlocked(t)
	aPool, ok := f.session.pool.getPoolFor(a)
	require.True(t, ok, "the stale fill registered a pool before blocking in its dial")

	f.session.removeHost(a)
	b := newReplacementHost(t, a)
	_, existed := f.session.ring.addHostIfMissing(b)
	require.False(t, existed, "the replacement must take the freed host ID")
	f.session.startPoolFill(b)
	f.hooks.await(t, poolFillDone, b, "the replacement's fill to complete")

	f.hooks.releaseDial()
	awaitDone(t, filled, "the stale startPoolFill")
	// The dial succeeded, so the fill scheduled handleNodeConnected(A) on its own
	// goroutine; join it before reading the history.
	f.awaitConnectedCallback(t, a, "the stale fill's connected callback")
	ops := f.recorder.callsSince(mark, a)
	require.NotContains(t, ops, "AddHost", "a replaced object must not be published")
	require.NotContains(t, ops, "HostUp", "a replaced object must not come UP")
	require.Equal(t, []*HostInfo{b}, f.collector.hosts(t), "the policy must hold exactly the replacement")
	bPool, ok := f.session.pool.getPoolFor(b)
	require.True(t, ok, "the replacement's pool must be registered")
	require.NotNil(t, bPool.Pick(), "the replacement must serve")
	requireEventuallyClosed(t, aPool, "the stale pool must be closed")
}

// TestDownRace_UpHoldsMutex: a late fill success publishing UP holds hostPublishMu;
// the DOWN that follows is applied after it, and state, policy and pool agree.
func TestDownRace_UpHoldsMutex(t *testing.T) {
	f := newOwnershipFixture(t)
	a := f.host
	f.session.pool.removeHost(a)
	mark := f.recorder.mark()

	f.recorder.armGate("HostUp")
	t.Cleanup(func() { f.recorder.releaseGate("HostUp") })
	go f.session.pool.addHost(a)
	f.recorder.awaitEntered(t, "HostUp", a)

	// The DOWN passes its unlocked ownership pre-check and then must queue on
	// the mutex; the hook after that pre-check is the arrival barrier.
	arrived := make(chan struct{}, 1)
	f.session.testAfterOwnsDown = func() {
		select {
		case arrived <- struct{}{}:
		default:
		}
	}
	downed := make(chan struct{})
	go func() {
		f.session.handleHostDown(a)
		close(downed)
	}()
	awaitDone(t, arrived, "handleHostDown to reach the mutex")
	requireNotReturned(t, downed, "handleHostDown while HostUp holds hostPublishMu")

	f.recorder.releaseGate("HostUp")
	awaitDone(t, downed, "handleHostDown")
	f.awaitConnectedCallback(t, a, "the UP callback to return")
	f.hooks.await(t, poolFillDone, a, "the fill to complete")

	require.Equal(t, []string{"HostUp", "HostDown"}, f.recorder.callsSince(mark, a), "UP first, then the DOWN that waited")
	require.Equal(t, NodeDown, a.State())
	_, ok := f.session.pool.getPoolFor(a)
	require.False(t, ok, "DOWN must have removed the pool")
	require.NotContains(t, f.collector.hosts(t), a, "DOWN must have removed policy membership")

	// A fresh fill recovers the host, as the reconnect tick would. The UP that
	// was gated above also reached the collector; drop it so the wait below can
	// only be satisfied by the recovery's own UP.
	drainHosts(f.collector.up)
	f.session.pool.addHost(a)
	f.hooks.await(t, poolFillDone, a, "the recovery fill to complete")
	awaitHost(t, f.collector.up, a, "the host to come back UP")
	require.Equal(t, NodeUp, a.State())
	_, ok = f.session.pool.getPoolFor(a)
	require.True(t, ok, "recovery must register a pool")
	require.Contains(t, f.collector.hosts(t), a, "recovery must restore membership")
}

// TestDownRace_DownHoldsMutex: DOWN holds hostPublishMu while a late UP callback
// waits; the callback then finds no pool and does nothing.
func TestDownRace_DownHoldsMutex(t *testing.T) {
	f := newOwnershipFixture(t)
	a := f.host
	mark := f.recorder.mark()

	f.recorder.armGate("HostDown")
	t.Cleanup(func() { f.recorder.releaseGate("HostDown") })
	downed := make(chan struct{})
	go func() {
		f.session.handleHostDown(a)
		close(downed)
	}()
	f.recorder.awaitEntered(t, "HostDown", a)

	// The late UP passes its unlocked ownership pre-check and then must queue on
	// the mutex; the hook after that pre-check is the arrival barrier.
	arrived := make(chan struct{}, 1)
	f.session.testAfterOwnsConnected = func() {
		select {
		case arrived <- struct{}{}:
		default:
		}
	}
	upped := make(chan struct{})
	go func() {
		f.session.handleNodeConnected(a)
		close(upped)
	}()
	awaitDone(t, arrived, "handleNodeConnected to reach the mutex")
	requireNotReturned(t, upped, "handleNodeConnected while HostDown holds hostPublishMu")

	f.recorder.releaseGate("HostDown")
	awaitDone(t, downed, "handleHostDown")
	awaitDone(t, upped, "handleNodeConnected")

	require.Equal(t, []string{"HostDown"}, f.recorder.callsSince(mark, a), "the late UP must not publish after the DOWN")
	require.Equal(t, NodeDown, a.State())
	_, ok := f.session.pool.getPoolFor(a)
	require.False(t, ok, "no pool after DOWN")
	require.NotContains(t, f.collector.hosts(t), a, "no membership after DOWN")

	drainHosts(f.collector.up)
	f.session.pool.addHost(a)
	f.hooks.await(t, poolFillDone, a, "the recovery fill to complete")
	awaitHost(t, f.collector.up, a, "the host to come back UP")
	require.Equal(t, NodeUp, a.State())
	require.Contains(t, f.collector.hosts(t), a, "recovery must restore membership")
}

// TestMarkHostDown_SkippedWhenUnowned: an object the ring no longer owns is left
// untouched, whatever its state.
func TestMarkHostDown_SkippedWhenUnowned(t *testing.T) {
	for _, initial := range []nodeState{NodeUp, NodeDown} {
		t.Run(initial.String(), func(t *testing.T) {
			f := newOwnershipFixture(t)
			a := f.host
			a.setState(initial)
			f.session.ring.removeHost(a.HostID())
			drainHosts(f.collector.down)
			drainHosts(f.collector.listenerDown)
			mark := f.recorder.mark()

			f.session.markHostDown(a)

			require.Empty(t, f.recorder.callsSince(mark, a), "an unowned object must not be reported DOWN")
			require.Empty(t, drainHosts(f.collector.listenerDown), "no listener event for an unowned object")
			require.Equal(t, initial, a.State(), "an unowned object keeps its state")
		})
	}
}

// TestStartPoolFill_SkippedWhenUnowned: startPoolFill on an object the ring does
// not own neither publishes nor touches the registered pool.
func TestStartPoolFill_SkippedWhenUnowned(t *testing.T) {
	f := newOwnershipFixture(t)
	a := f.host
	before, ok := f.session.pool.getPool(a)
	require.True(t, ok)
	f.session.ring.removeHost(a.HostID())
	mark := f.recorder.mark()

	f.session.startPoolFill(a)

	require.Empty(t, f.recorder.callsSince(mark, a), "an unowned object must not be published")
	after, ok := f.session.pool.getPool(a)
	require.True(t, ok)
	require.Same(t, before, after, "the registered pool must be untouched")
}

// panickingRemovePolicy panics in RemoveHost once, standing in for application
// policy code that fails inside the driver's publication critical section.
type panickingRemovePolicy struct {
	*hostStateCollector
	armed atomic.Bool
}

// RemoveHost panics while armed, otherwise forwards.
func (p *panickingRemovePolicy) RemoveHost(host *HostInfo) {
	if p.armed.CompareAndSwap(true, false) {
		panic("gocql: test policy panic in RemoveHost")
	}
	p.hostStateCollector.RemoveHost(host)
}

// TestRemoveHost_PolicyPanicReleasesPublishMutex: a policy that panics inside
// removeHost's critical section must not leave hostPublishMu locked, or every
// later host transition would block forever.
func TestRemoveHost_PolicyPanicReleasesPublishMutex(t *testing.T) {
	var policy *panickingRemovePolicy
	session, host, _ := newPauseRecoverySession(t, func(cluster *ClusterConfig) {
		inner, ok := cluster.PoolConfig.HostSelectionPolicy.(*hostStateCollector)
		require.True(t, ok)
		policy = &panickingRemovePolicy{hostStateCollector: inner}
		cluster.PoolConfig.HostSelectionPolicy = policy
	})

	policy.armed.Store(true)
	require.Panics(t, func() { session.removeHost(host) }, "the policy panic must propagate to the caller")

	// A later transition on any object must still get the mutex.
	ran := make(chan bool, 1)
	go func() { ran <- session.withOwnedHost(host, func() bool { return true }) }()
	select {
	case owned := <-ran:
		require.False(t, owned, "the ring removal preceded the panic, so the object is no longer owned")
	case <-time.After(ownershipBudget):
		t.Fatal("hostPublishMu was left locked by the panicking policy")
	}
}

// bulkRecordingPolicy adds AddHosts so Session.init takes its bulk branch.
type bulkRecordingPolicy struct {
	*recordingPolicy
}

// AddHosts records and forwards each host, as the plain policy would.
func (b *bulkRecordingPolicy) AddHosts(hosts []*HostInfo) {
	for _, host := range hosts {
		b.AddHost(host)
	}
}

// TestInit_PublicationWinsBeforeRemoval: init's publication holds hostPublishMu, so
// a removal issued meanwhile waits for it and then takes the object out of the ring
// and un-publishes it, both inside the same transaction.
func TestInit_PublicationWinsBeforeRemoval(t *testing.T) {
	_, srv, _ := startLocalHostServer(t)

	var seq atomic.Int64
	inner := newHostStateCollector()
	recorder := newRecordingPolicy(inner, &seq)
	recorder.armGate("AddHost")
	t.Cleanup(func() { recorder.releaseGate("AddHost") })

	cluster := newLocalHostCluster(t, "", srv.Address, func(cluster *ClusterConfig, _ string) {
		cluster.PoolConfig.HostSelectionPolicy = recorder
	})

	type created struct {
		session *Session
		err     error
	}
	createdCh := make(chan created, 1)
	go func() {
		s, err := cluster.CreateSession()
		createdCh <- created{s, err}
	}()

	// Init is inside AddHost, holding hostPublishMu.
	call := recorder.awaitEntered(t, "AddHost", nil)
	original := call.host
	require.NotNil(t, original)
	session := recorder.session.Load()
	require.NotNil(t, session, "the policy must have captured the session in Init")

	// Replace the host while init holds the mutex: the removal must wait.
	replacement := newReplacementHost(t, original)
	removed := make(chan struct{})
	go func() {
		session.removeHost(original)
		close(removed)
	}()
	requireNotReturned(t, removed, "removeHost while init's publication holds hostPublishMu")
	require.True(t, session.ring.owns(original),
		"the ring removal is inside the transaction, so it must not land while init's publication holds the mutex")

	recorder.releaseGate("AddHost")
	awaitDone(t, removed, "removeHost")
	require.False(t, session.ring.owns(original),
		"the ring removal must complete once the transaction gets the mutex")
	_, existed := session.ring.addHostIfMissing(replacement)
	require.False(t, existed)
	session.startPoolFill(replacement)

	var result created
	select {
	case result = <-createdCh:
	case <-time.After(ownershipBudget):
		t.Fatal("CreateSession did not return")
	}
	require.NoError(t, result.err, "CreateSession")
	t.Cleanup(result.session.Close)

	ops := recorder.callsFor(original)
	require.Equal(t, "RemoveHost", ops[len(ops)-1], "the removal must follow init's publication; history: %v", ops)
	require.NotContains(t, inner.hosts(t), original, "the replaced object must not remain in the policy")
	require.Contains(t, inner.hosts(t), replacement, "the replacement must be in the policy")
}

// TestInit_PublishesOnlyOwnedHosts: Session.init publishes only the objects the
// ring still owns when it gets to the policy, so a refresh-driven replacement
// that raced ahead of it is not undone.
func TestInit_PublishesOnlyOwnedHosts(t *testing.T) {
	for _, bulk := range []bool{false, true} {
		name := "AddHost"
		if bulk {
			name = "AddHosts"
		}
		t.Run(name, func(t *testing.T) {
			script, srv, _, _ := func() (*localHostServer, *TestServer, int, *Session) {
				s, srv, port := startLocalHostServer(t)
				return s, srv, port, nil
			}()

			var seq atomic.Int64
			inner := newHostStateCollector()
			recorder := newRecordingPolicy(inner, &seq)
			var policy HostSelectionPolicy = recorder
			if bulk {
				policy = &bulkRecordingPolicy{recordingPolicy: recorder}
			}

			publishing := make(chan struct{}, 1)
			gate := make(chan struct{})
			var release sync.Once
			t.Cleanup(func() { release.Do(func() { close(gate) }) })
			done := make(chan error, 8)

			cluster := newLocalHostCluster(t, "", srv.Address, func(cluster *ClusterConfig, _ string) {
				cluster.PoolConfig.HostSelectionPolicy = policy
				cluster.testInitPublishHook = func() {
					publishing <- struct{}{}
					<-gate
				}
				cluster.testRingRefreshDone = func(err error) { done <- err }
			})

			type created struct {
				session *Session
				err     error
			}
			createdCh := make(chan created, 1)
			go func() {
				s, err := cluster.CreateSession()
				createdCh <- created{s, err}
			}()

			select {
			case <-publishing:
			case <-time.After(ownershipBudget):
				t.Fatal("init did not reach its policy publication")
			}
			session := recorder.session.Load()
			require.NotNil(t, session, "the policy must have captured the session in Init")
			original := session.ring.allHosts()
			require.Len(t, original, 1)

			// A refresh that replaces the host while init is paused before publishing.
			script.setBroadcastAddress("10.9.9.9")
			session.control.reconnect()
			require.NoError(t, awaitRefreshDone(t, done, "the reconnect-driven refresh"))
			replaced := session.ring.allHosts()
			require.Len(t, replaced, 1)
			require.NotSame(t, original[0], replaced[0], "the refresh must have replaced the ring object")

			release.Do(func() { close(gate) })
			var result created
			select {
			case result = <-createdCh:
			case <-time.After(ownershipBudget):
				t.Fatal("CreateSession did not return")
			}
			require.NoError(t, result.err, "CreateSession")
			t.Cleanup(result.session.Close)

			require.NotContains(t, inner.hosts(t), original[0], "init must not publish the object the refresh replaced")
			require.Contains(t, inner.hosts(t), replaced[0], "the replacement must be in the policy")
			require.NotContains(t, recorder.callsFor(original[0]), "AddHost", "init must skip the replaced object")
		})
	}
}
