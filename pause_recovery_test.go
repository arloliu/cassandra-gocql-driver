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
	"context"
	"errors"
	"net"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// errGatedDialerClosed is the dial error a closed gatedDialer reports.
var errGatedDialerClosed = errors.New("gocql: gated dialer is closed")

// hostStateEventBudget bounds how long a test waits for a host state transition it expects to happen.
// It only has to cover fillingStopped's 31-131 ms back-off plus one reconnection-policy attempt,
// but is generous so a loaded CI machine does not turn a passing test into a flake.
const hostStateEventBudget = 10 * time.Second

// hostStateCollector observes every host UP/DOWN transition a session makes.
//
// It wraps the selection policy rather than only registering as a
// HostStateChangeListener because the listener is gated on
// Session.initialized():
// the pool's very first fill can publish its UP before init flips that flag,
// so listener UP notifications for the initial fill are delivered
// nondeterministically and cannot be used as a barrier.
// handleNodeConnected and markHostDown call policy.HostUp / policy.HostDown
// under exactly the same conditions as the listener and without that gate,
// so the policy sees the complete, ungated history.
//
// Sends are non-blocking into a generously sized buffer:
// these tests produce a handful of events,
// and a full channel must never wedge a driver goroutine.
type hostStateCollector struct {
	HostSelectionPolicy

	// up and down carry the ungated policy-level transitions.
	up   chan *HostInfo
	down chan *HostInfo

	// listenerDown carries HostDownEvent notifications from the public
	// HostStateChangeListener API.
	// Only DOWN is collected: nothing in session init marks a host down,
	// so these are free of initialization noise.
	listenerDown chan *HostInfo
}

var (
	_ HostSelectionPolicy      = (*hostStateCollector)(nil)
	_ HostStatusChangeListener = (*hostStateCollector)(nil)
)

// newHostStateCollector returns a collector wrapping the round robin policy.
//
// Returns:
//   - *hostStateCollector: collector ready to be installed as both the
//     cluster's HostSelectionPolicy and its HostStateChangeListener
func newHostStateCollector() *hostStateCollector {
	return &hostStateCollector{
		HostSelectionPolicy: RoundRobinHostPolicy(),
		up:                  make(chan *HostInfo, 64),
		down:                make(chan *HostInfo, 64),
		listenerDown:        make(chan *HostInfo, 64),
	}
}

// HostUp forwards to the wrapped policy and records the transition.
func (c *hostStateCollector) HostUp(host *HostInfo) {
	c.HostSelectionPolicy.HostUp(host)
	publishHost(c.up, host)
}

// HostDown forwards to the wrapped policy and records the transition.
func (c *hostStateCollector) HostDown(host *HostInfo) {
	c.HostSelectionPolicy.HostDown(host)
	publishHost(c.down, host)
}

// OnHostUp satisfies HostStatusChangeListener; UP notifications are observed
// through the policy instead.
func (c *hostStateCollector) OnHostUp(_ HostUpEvent) {}

// OnHostDown records a DOWN notification from the public listener API.
func (c *hostStateCollector) OnHostDown(event HostDownEvent) {
	publishHost(c.listenerDown, event.Host)
}

// hosts returns the hosts the wrapped policy currently holds, regardless of
// their UP/DOWN state.
//
// Returns:
//   - []*HostInfo: the policy's host list
func (c *hostStateCollector) hosts(t *testing.T) []*HostInfo {
	t.Helper()

	policy, ok := c.HostSelectionPolicy.(*roundRobinHostPolicy)
	require.True(t, ok, "collector expects the round robin host policy, got %T", c.HostSelectionPolicy)
	return policy.hosts.get()
}

// publishHost records host on ch without ever blocking the caller.
func publishHost(ch chan<- *HostInfo, host *HostInfo) {
	select {
	case ch <- host:
	default:
	}
}

// gatedDialer is a Dialer whose dials can be failed on demand,
// so a test can make a host unreachable without stopping the TestServer that backs it
// and without racing the listener teardown.
type gatedDialer struct {
	dialer net.Dialer
	closed atomic.Bool
}

var _ Dialer = (*gatedDialer)(nil)

// DialContext dials addr unless the gate has been closed.
//
// Returns:
//   - net.Conn: the dialed connection while the gate is open
//   - error: errGatedDialerClosed once close has been called
func (d *gatedDialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if d.closed.Load() {
		return nil, errGatedDialerClosed
	}
	return d.dialer.DialContext(ctx, network, addr)
}

// close makes every subsequent dial fail.
func (d *gatedDialer) close() {
	d.closed.Store(true)
}

// open lets dials through again, so a test can drive a real recovery rather
// than simulate one by writing host state directly.
func (d *gatedDialer) open() {
	d.closed.Store(false)
}

// recordingConvictionPolicy records every AddFailure call and lets a test decide the verdict,
// optionally mutating driver state inside the call to exercise the check-then-convict window.
type recordingConvictionPolicy struct {
	// onFailure decides the verdict; nil means "convict".
	onFailure func(host *HostInfo) bool

	mu    sync.Mutex
	hosts []*HostInfo
}

var _ ConvictionPolicy = (*recordingConvictionPolicy)(nil)

// AddFailure records the host and returns the verdict of onFailure.
//
// Returns:
//   - bool: true when the host should be marked DOWN
func (p *recordingConvictionPolicy) AddFailure(_ error, host *HostInfo) bool {
	p.mu.Lock()
	p.hosts = append(p.hosts, host)
	p.mu.Unlock()

	if p.onFailure == nil {
		return true
	}
	return p.onFailure(host)
}

// Reset is a no-op; conviction is decided entirely by onFailure.
func (p *recordingConvictionPolicy) Reset(_ *HostInfo) {}

// recorded returns the hosts AddFailure was invoked with, in call order.
//
// Returns:
//   - []*HostInfo: one entry per AddFailure call
func (p *recordingConvictionPolicy) recorded() []*HostInfo {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.hosts)
}

// newPauseRecoverySession starts a TestServer
// and returns a single-host session wired to a hostStateCollector,
// together with the ring object for the served host.
//
// The control connection is disabled (testCluster),
// so the ring holds exactly the contact point, whose broadcast address is unset —
// the same shape as a port-mapped or NAT'd deployment,
// where DOWN bookkeeping keyed by the broadcast address cannot find the host the driver actually dialled.
//
// It returns only after the initial fill has published its UP through the policy,
// so no session-init goroutine is still mutating the host when the test body starts.
//
// Parameters:
//   - t: the test; the server and session are registered for cleanup
//   - tune: optional last-minute cluster tweaks applied before CreateSession
//
// Returns:
//   - *Session: connected session with one host and one connection
//   - *HostInfo: the ring object for the served host
//   - *hostStateCollector: the session's transition collector
func newPauseRecoverySession(t *testing.T, tune func(*ClusterConfig)) (*Session, *HostInfo, *hostStateCollector) {
	t.Helper()

	srv := NewTestServer(t, defaultProto, testServerContext(t))
	t.Cleanup(srv.Stop)

	collector := newHostStateCollector()
	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = 1
	cluster.PoolConfig.HostSelectionPolicy = collector
	cluster.Metadata.HostListener.HostStateChangeListener = collector
	if tune != nil {
		tune(cluster)
	}

	session, err := cluster.CreateSession()
	require.NoError(t, err, "CreateSession")
	t.Cleanup(session.Close)

	hosts := session.ring.allHosts()
	require.Len(t, hosts, 1, "the test fixture must produce exactly one ring host")

	host := hosts[0]
	awaitHost(t, collector.up, host, "the fixture host to finish its initial fill")
	require.Equal(t, NodeUp, host.State(), "the fixture host must start UP")

	return session, host, collector
}

// testServerContext returns a context cancelled when the test finishes,
// for TestServer instances that must outlive individual assertions.
//
// Returns:
//   - context.Context: cancelled during test cleanup
func testServerContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	return ctx
}

// newReplacementHost starts a second TestServer on the 127.0.0.2 loopback alias
// and returns a HostInfo carrying old's host ID at that new address —
// what refreshRing produces when a node comes back at a different address.
//
// A distinct IP, not merely a distinct port, matters:
// the selection policies key on ConnectAddress,
// so a replacement sharing the old IP could not distinguish
// "the stale object was evicted" from "the replacement was evicted".
// The test is skipped where the alias cannot be bound.
//
// Parameters:
//   - t: the test; the second server is registered for cleanup
//   - old: the ring object being replaced; its host ID is reused
//
// Returns:
//   - *HostInfo: the replacement object, not yet in the ring
func newReplacementHost(t *testing.T, old *HostInfo) *HostInfo {
	t.Helper()

	probe, err := net.Listen("tcp", "127.0.0.2:0")
	if err != nil {
		t.Skipf("cannot bind the second loopback address 127.0.0.2: %v", err)
	}
	require.NoError(t, probe.Close())

	srv := NewTestServerWithAddress("127.0.0.2:0", t, defaultProto, testServerContext(t))
	t.Cleanup(srv.Stop)

	ip, portText, err := net.SplitHostPort(srv.Address)
	require.NoError(t, err, "SplitHostPort(%q)", srv.Address)
	port, err := strconv.Atoi(portText)
	require.NoError(t, err, "parse port from %q", srv.Address)

	host, err := NewHostInfoFromAddrPort(net.ParseIP(ip), port)
	require.NoError(t, err, "NewHostInfoFromAddrPort")
	host.setHostID(old.HostID())

	return host
}

// awaitHost blocks until want is published on ch, failing the test if the budget expires first.
//
// Parameters:
//   - t: the test
//   - ch: the collector channel to read
//   - want: the host whose event is expected
//   - what: description used in the failure message
//
// Returns:
//   - []*HostInfo: the hosts published before want, so a caller can assert on them
func awaitHost(t *testing.T, ch <-chan *HostInfo, want *HostInfo, what string) []*HostInfo {
	t.Helper()

	var others []*HostInfo
	deadline := time.After(hostStateEventBudget)
	for {
		select {
		case got := <-ch:
			if got == want {
				return others
			}
			others = append(others, got)
		case <-deadline:
			t.Fatalf("timed out after %v waiting for %s", hostStateEventBudget, what)
		}
	}
}

// drainHosts returns every event currently buffered on ch without blocking.
//
// Returns:
//   - []*HostInfo: the buffered hosts, in publication order
func drainHosts(ch <-chan *HostInfo) []*HostInfo {
	var hosts []*HostInfo
	for {
		select {
		case host := <-ch:
			hosts = append(hosts, host)
		default:
			return hosts
		}
	}
}

// TestHandleHostDown_MarksHostDownByIdentity documents the address-keyed defect
// and proves the identity-keyed path fixes it.
//
// ring.hostIPToUUID is keyed by nodeToNodeAddress (the broadcast address),
// while pool and control-connection failures only ever hold the connect address.
// Where the two differ — port mapping, NAT, an AddressTranslator,
// and any contact point that never went through a ring refresh —
// handleNodeDown's lookup misses and the host silently stays UP,
// so reconnectDownedHosts never takes ownership of its recovery.
// handleHostDown takes the *HostInfo the caller failed on instead, so the lookup cannot miss.
func TestHandleHostDown_MarksHostDownByIdentity(t *testing.T) {
	session, host, collector := newPauseRecoverySession(t, nil)

	// The defect: the by-address path cannot find a host whose broadcast
	// address is unset, so it leaves the host UP.
	session.handleNodeDown(host.ConnectAddress(), host.Port())
	require.Equal(t, NodeUp, host.State(), "handleNodeDown must miss here; that miss is the bug under test")
	require.Empty(t, drainHosts(collector.down), "the missed lookup must not report a transition")

	session.handleHostDown(host)

	require.Equal(t, NodeDown, host.State(), "handleHostDown must mark the host it was handed DOWN")

	_, ok := session.pool.getPoolFor(host)
	require.False(t, ok, "the host's pool must be unregistered so traffic cannot refill it")

	require.Equal(t, []*HostInfo{host}, drainHosts(collector.down), "exactly one DOWN transition for this host")
	require.Equal(t, []*HostInfo{host}, drainHosts(collector.listenerDown), "exactly one HostDownEvent for this host")
	require.NotContains(t, collector.hosts(t), host, "the host must be removed from the selection policy")

	next := session.policy.Pick(nil)
	require.Nil(t, next(), "round robin must no longer yield the downed host")
}

// TestHandleHostDown_IgnoresNilAndEmptyHostID asserts the identity guard rejects objects the ring cannot own:
// a nil host, and a contact-point object that never received a host ID.
func TestHandleHostDown_IgnoresNilAndEmptyHostID(t *testing.T) {
	session, host, collector := newPauseRecoverySession(t, nil)

	session.handleHostDown(nil)

	orphan, err := NewHostInfoFromAddrPort(net.ParseIP("127.0.0.1"), host.Port())
	require.NoError(t, err, "NewHostInfoFromAddrPort")
	require.Empty(t, orphan.HostID(), "the orphan must have no host ID")

	session.handleHostDown(orphan)

	require.Equal(t, NodeUp, host.State(), "the ring host must be untouched")
	require.Empty(t, drainHosts(collector.down), "an unowned host must not report a transition")
	require.Empty(t, drainHosts(collector.listenerDown), "an unowned host must not notify listeners")

	_, ok := session.pool.getPoolFor(host)
	require.True(t, ok, "the ring host's pool must survive")
}

// TestHandleHostDown_RespectsHostFilter asserts a filtered host still records the DOWN state
// but is not pushed through the policy, the pool or the listeners —
// the early return handleNodeDown has always had.
func TestHandleHostDown_RespectsHostFilter(t *testing.T) {
	session, host, collector := newPauseRecoverySession(t, nil)

	// Applied after the fixture barrier: Session.cfg is a copy and no init
	// goroutine is still reading it, so this changes only the path under test.
	session.cfg.HostFilter = DenyAllFilter()

	session.handleHostDown(host)

	require.Equal(t, NodeDown, host.State(), "state is recorded even for a filtered host")
	require.Empty(t, drainHosts(collector.down), "a filtered host must not reach the policy")
	require.Empty(t, drainHosts(collector.listenerDown), "a filtered host must not notify listeners")
	require.Contains(t, collector.hosts(t), host, "a filtered host must not be removed from the policy")

	_, ok := session.pool.getPoolFor(host)
	require.True(t, ok, "a filtered host's pool must not be removed")
}

// TestHandleHostDown_ReplacementAfterOwnershipCheck closes the window
// between ring.owns returning true and markHostDown mutating state:
// refreshRing swaps the ring entry for a new object under the same host ID,
// and the in-flight DOWN must not tear down the replacement's freshly filled pool.
//
// The replacement's own UP is the barrier — handleNodeConnected only runs once
// its first connection is registered, so observing it proves the pool exists and is non-empty.
// Nothing may be reported DOWN: not the replacement, and not the stale object either.
// markHostDown re-checks ownership inside its critical section and leaves an object
// the ring no longer owns untouched, because reporting it DOWN to a policy that
// keys by address could evict a replacement that took the same address.
func TestHandleHostDown_ReplacementAfterOwnershipCheck(t *testing.T) {
	session, stale, collector := newPauseRecoverySession(t, nil)
	replacement := newReplacementHost(t, stale)

	var once sync.Once
	session.testAfterOwnsDown = func() {
		once.Do(func() {
			// Disarm before re-entering the driver: the replacement's own
			// failure paths would otherwise run this hook again.
			session.testAfterOwnsDown = nil

			session.removeHost(stale)
			_, existed := session.ring.addHostIfMissing(replacement)
			require.False(t, existed, "the replacement must take the freed host ID")
			session.startPoolFill(replacement)
		})
	}

	session.handleHostDown(stale)

	awaitHost(t, collector.up, replacement, "the replacement host to come UP")

	require.Equal(t, NodeUp, replacement.State(), "the replacement must be UP")
	require.Contains(t, collector.hosts(t), replacement, "the replacement must be in the selection policy")

	pool, ok := session.pool.getPoolFor(replacement)
	require.True(t, ok, "the replacement's pool must still be registered")
	require.NotZero(t, pool.Size(), "the replacement's pool must still hold connections")

	// handleHostDown returned before these assertions, so every effect of the
	// stale DOWN has already been applied - and there must be none.
	require.Empty(t, drainHosts(collector.down), "a replaced object must not be reported DOWN to the policy")
	require.Empty(t, drainHosts(collector.listenerDown), "a replaced object must not reach the listeners")
	require.Equal(t, NodeUp, stale.State(), "a replaced object is left untouched")
}

// TestHandleNodeConnected_ReplacementAfterOwnershipCheck closes the mirror window on the UP path:
// a late fill success for an object refreshRing has already replaced must not resurrect it.
// ring.owns cannot catch this alone — it is checked before the swap —
// so handleNodeConnected re-validates the pool by identity,
// and the pointer mismatch against the replacement's pool stops the stale UP.
func TestHandleNodeConnected_ReplacementAfterOwnershipCheck(t *testing.T) {
	session, stale, collector := newPauseRecoverySession(t, nil)
	replacement := newReplacementHost(t, stale)

	// The stale object is on its way down; a late fill success is what would
	// wrongly bring it back.
	stale.setState(NodeDown)

	var once sync.Once
	session.testAfterOwnsConnected = func() {
		once.Do(func() {
			session.testAfterOwnsConnected = nil

			session.removeHost(stale)
			_, existed := session.ring.addHostIfMissing(replacement)
			require.False(t, existed, "the replacement must take the freed host ID")
			session.startPoolFill(replacement)
		})
	}

	session.handleNodeConnected(stale)

	before := awaitHost(t, collector.up, replacement, "the replacement host to come UP")
	require.NotContains(t, before, stale, "a replaced object must not be brought back UP")
	require.NotContains(t, drainHosts(collector.up), stale, "a replaced object must not be brought back UP")
	require.Equal(t, NodeDown, stale.State(), "a replaced object must keep its DOWN state")

	pool, ok := session.pool.getPoolFor(replacement)
	require.True(t, ok, "the replacement's pool must be registered")
	require.NotZero(t, pool.Size(), "the replacement's pool must hold connections")
}

// TestFillingStopped_ConvictsByIdentity drives the production failure path:
// the pool loses its last connection, the refill cycle cannot dial,
// and fillingStopped convicts the host it holds rather than an address it has to look up.
// Before the fix this cycle left the host UP behind port mapping or NAT,
// so application traffic kept refilling it and recovery never became ReconnectInterval's job.
func TestFillingStopped_ConvictsByIdentity(t *testing.T) {
	gate := &gatedDialer{}
	conviction := &recordingConvictionPolicy{}
	session, host, collector := newPauseRecoverySession(t, func(cluster *ClusterConfig) {
		cluster.Dialer = gate
		cluster.ConvictionPolicy = conviction
		// One attempt, so the failing cycle reaches fillingStopped promptly.
		cluster.ReconnectionPolicy = &ConstantReconnectionPolicy{MaxRetries: 1, Interval: time.Millisecond}
	})

	pool, ok := session.pool.getPoolFor(host)
	require.True(t, ok, "the fixture host must have a pool")

	conn := pool.Pick()
	require.NotNil(t, conn, "the fixture host must have a connection to lose")

	// From here the host is unreachable, so the refill this close triggers
	// cannot succeed.
	gate.close()
	conn.closeWithError(errors.New("gocql: test induced connection failure"))

	awaitHost(t, collector.down, host, "the host to be convicted after the failed fill cycle")

	require.Equal(t, []*HostInfo{host}, conviction.recorded(), "the failed cycle must convict the host object it filled")
	require.Equal(t, NodeDown, host.State(), "a fill cycle that ends with an empty pool must mark the host DOWN")

	_, ok = session.pool.getPoolFor(host)
	require.False(t, ok, "the convicted host's pool must be unregistered")
}

// TestConvictOnDialFailure_NoPoolDuringInit asserts the control connection does not convict a host
// that has no pool at all —
// the shape Session.init has before the pools are built, and the shape a removed host leaves behind.
// Consuming a stateful ConvictionPolicy for a host this path cannot act on
// would silently spend one of its failures.
func TestConvictOnDialFailure_NoPoolDuringInit(t *testing.T) {
	session, host, collector := newPauseRecoverySession(t, nil)

	policy := &recordingConvictionPolicy{}
	session.cfg.ConvictionPolicy = policy

	session.pool.removeHost(host)
	_, ok := session.pool.getPoolFor(host)
	require.False(t, ok, "the fixture must leave the host without a pool")

	control := &controlConn{session: session}
	control.convictOnDialFailure(host, errors.New("gocql: test induced dial failure"))

	require.Empty(t, policy.recorded(), "a host without a pool must not reach the conviction policy")
	require.Equal(t, NodeUp, host.State(), "a host without a pool must not be marked DOWN")
	require.Empty(t, drainHosts(collector.down), "no DOWN transition may be reported")
}

// TestConvictOnDialFailure_HealthyPoolNotConvicted asserts a host whose pool still holds connections
// survives a failed control dial.
// Tearing such a host down on a single control-connection failure
// is the all-or-nothing behaviour that takes a working node out of rotation.
func TestConvictOnDialFailure_HealthyPoolNotConvicted(t *testing.T) {
	session, host, collector := newPauseRecoverySession(t, nil)

	policy := &recordingConvictionPolicy{}
	session.cfg.ConvictionPolicy = policy

	pool, ok := session.pool.getPoolFor(host)
	require.True(t, ok, "the fixture host must have a pool")
	require.NotZero(t, pool.Size(), "the fixture host's pool must hold connections")

	control := &controlConn{session: session}
	control.convictOnDialFailure(host, errors.New("gocql: test induced dial failure"))

	require.Empty(t, policy.recorded(), "a host with a healthy pool must not reach the conviction policy")
	require.Equal(t, NodeUp, host.State(), "a host with a healthy pool must stay UP")
	require.Empty(t, drainHosts(collector.down), "no DOWN transition may be reported")

	_, ok = session.pool.getPoolFor(host)
	require.True(t, ok, "the healthy pool must survive")
}

// TestConvictOnDialFailure_EmptyPoolGainsConnDuringDecision covers the check-then-convict window:
// emptiness is read before AddFailure,
// so a connection that lands while the policy is deciding is convicted with the host.
// That is deliberate — the pool is removed and the late connection is closed with it,
// the host stays DOWN,
// and a later fill success cannot undo that
// because handleNodeConnected re-validates the pool by identity and misses.
// reconnectDownedHosts owns the recovery from there.
func TestConvictOnDialFailure_EmptyPoolGainsConnDuringDecision(t *testing.T) {
	session, host, collector := newPauseRecoverySession(t, nil)

	pool, ok := session.pool.getPoolFor(host)
	require.True(t, ok, "the fixture host must have a pool")

	// Empty the pool while keeping the connection alive, so it can be handed
	// back inside the conviction decision.
	pool.mu.Lock()
	conns := pool.conns
	pool.conns = nil
	pool.mu.Unlock()
	require.Len(t, conns, 1, "the fixture must hold exactly one connection")
	late := conns[0]

	policy := &recordingConvictionPolicy{
		onFailure: func(_ *HostInfo) bool {
			pool.mu.Lock()
			pool.conns = append(pool.conns, late)
			pool.mu.Unlock()
			return true
		},
	}
	session.cfg.ConvictionPolicy = policy

	control := &controlConn{session: session}
	control.convictOnDialFailure(host, errors.New("gocql: test induced dial failure"))

	require.Equal(t, []*HostInfo{host}, policy.recorded(), "an empty pool must reach the conviction policy exactly once")
	require.Equal(t, NodeDown, host.State(), "a convicted host must be marked DOWN")

	_, ok = session.pool.getPoolFor(host)
	require.False(t, ok, "the convicted host's pool must be unregistered")

	awaitHost(t, collector.down, host, "the DOWN transition for the convicted host")

	// Closing the pool closes the connection that landed inside the window.
	select {
	case <-late.ctx.Done():
	case <-time.After(hostStateEventBudget):
		t.Fatalf("the connection added during the conviction decision was not closed within %v", hostStateEventBudget)
	}

	// A fill success arriving after the conviction must not resurrect the host:
	// the pool it belonged to is gone, so the identity check misses.
	session.handleNodeConnected(host)
	require.Equal(t, NodeDown, host.State(), "a late fill success must not undo the conviction")
	require.Empty(t, drainHosts(collector.up), "a late fill success must not report an UP transition")
}
