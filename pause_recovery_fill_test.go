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
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fillEventBudget bounds how long a fill test waits for an event it expects to happen.
// It is generous so a loaded CI machine does not turn a passing test into a flake.
const fillEventBudget = 10 * time.Second

// fillAbsenceWindow bounds how long a fill test watches for an event that must not happen.
// Proving an absence has no barrier to wait on, so it needs a window instead; this one is
// short because it only has to outlast the few instructions a fixture would still execute
// if it were wrongly allowed to return.
const fillAbsenceWindow = 250 * time.Millisecond

// poolEventChanBuffer is the depth of every poolEventRecorder arrival channel.
//
// Publishing is non-blocking, so a burst deeper than this loses arrivals; the recorder
// counts what it dropped and the barriers refuse to trust the channel afterwards.
const poolEventChanBuffer = 64

// errFillTestConnClosed is the error the tests close a pooled connection with.
var errFillTestConnClosed = errors.New("gocql: connection closed by the fill test")

// errFillTestDialRefused is the error a gated dial reports when the test wants the fill to fail.
var errFillTestDialRefused = errors.New("gocql: dial refused by the fill test")

// errFillTestEventDropped marks a barrier that refused to wait because the recorder had
// dropped publications of the event it was keyed on.
var errFillTestEventDropped = errors.New("gocql: pool event publication dropped by the fill test recorder")

// fillLoopbackAddrs are the loopback aliases the multi-host fill tests bind.
//
// Distinct IPs, not merely distinct ports, are required:
// NewTestServer always binds 127.0.0.1 and the selection policies deduplicate hosts by connect address.
var fillLoopbackAddrs = []string{"127.0.0.2:0", "127.0.0.3:0", "127.0.0.4:0"}

// gatedHostDialer is a HostDialer whose dials can be parked and failed on demand.
//
// While the gate is disarmed every dial passes straight through, so a session can be
// created normally; once armed, each dial publishes on started and then waits for one
// release token, which lets a test drive fill cycles one attempt at a time.
type gatedHostDialer struct {
	inner HostDialer

	// started receives one token per gated dial, before that dial parks.
	started chan struct{}
	// release hands out one token per gated dial; closing it releases every dial.
	release chan struct{}

	mu        sync.Mutex
	armed     bool
	errFn     func(addr string) error
	closeOnce sync.Once
}

var _ HostDialer = (*gatedHostDialer)(nil)

// newGatedHostDialer returns a disarmed gate wrapping the driver's default dialer.
//
// Returns:
//   - *gatedHostDialer: gate ready to be installed as ClusterConfig.HostDialer
func newGatedHostDialer() *gatedHostDialer {
	return &gatedHostDialer{
		inner:   &defaultHostDialer{dialer: &net.Dialer{}},
		started: make(chan struct{}, 64),
		release: make(chan struct{}, 64),
	}
}

// DialHost parks the dial while the gate is armed, then fails or dials it.
//
// Returns:
//   - *DialedHost: the dialed connection
//   - error: the verdict of the error function, or the underlying dial error
func (d *gatedHostDialer) DialHost(ctx context.Context, host *HostInfo) (*DialedHost, error) {
	d.mu.Lock()
	armed := d.armed
	d.mu.Unlock()

	if !armed {
		return d.inner.DialHost(ctx, host)
	}

	select {
	case d.started <- struct{}{}:
	default:
	}

	select {
	case <-d.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// The verdict is read only once the dial is released, so a test can decide the
	// outcome of a parked dial after it has already started.
	d.mu.Lock()
	errFn := d.errFn
	d.mu.Unlock()

	if errFn != nil {
		if err := errFn(host.ConnectAddressAndPort()); err != nil {
			return nil, err
		}
	}

	return d.inner.DialHost(ctx, host)
}

// arm makes every later dial park until it is released.
//
// Parameters:
//   - errFn: decides the verdict of a released dial; nil dials for real
func (d *gatedHostDialer) arm(errFn func(addr string) error) {
	d.mu.Lock()
	d.armed = true
	d.errFn = errFn
	d.mu.Unlock()
}

// setErr replaces the verdict applied to later released dials.
func (d *gatedHostDialer) setErr(errFn func(addr string) error) {
	d.mu.Lock()
	d.errFn = errFn
	d.mu.Unlock()
}

// awaitStarted blocks until one gated dial has parked.
func (d *gatedHostDialer) awaitStarted(t *testing.T) {
	t.Helper()

	select {
	case <-d.started:
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for a gated dial to start", fillEventBudget)
	}
}

// releaseOne lets exactly one parked dial continue.
func (d *gatedHostDialer) releaseOne() {
	d.release <- struct{}{}
}

// releaseAll lets every parked and future dial continue.
func (d *gatedHostDialer) releaseAll() {
	d.closeOnce.Do(func() { close(d.release) })
}

// poolEventRecorder observes ClusterConfig.testPoolHook checkpoints.
//
// Every event is published on a per-event channel so a test can use it as a barrier,
// and a per-event handler may run extra work inside the driver goroutine that reached
// the checkpoint.
//
// Counts are authoritative: every checkpoint is counted, and a count is never lost.
// The arrival channel is best-effort:
// it holds poolEventChanBuffer publications,
// and the hook drops rather than blocks on an arrival that does not fit,
// because the hook runs inside the driver goroutine that reached the checkpoint.
// A barrier that would need more than poolEventChanBuffer unconsumed publications of one event is therefore not supported.
// The recorder counts every drop,
// and await and awaitEachHost then fail the test with that diagnostic
// instead of hanging on a channel they can no longer trust.
type poolEventRecorder struct {
	mu       sync.Mutex
	chans    map[poolEvent]chan *HostInfo
	handlers map[poolEvent]func(host *HostInfo)
	counts   map[poolEvent]int
	// drops counts the publications of an event that did not fit the arrival channel.
	drops map[poolEvent]int
	// afterReceive, when set, runs inside a barrier immediately after it takes an arrival
	// off the channel and before it re-checks the drop count.
	// It is the seam the drop-during-receive test needs:
	// that interleaving - a publication dropped after the pre-check and before the post-check -
	// cannot be produced deterministically from outside the barrier.
	afterReceive func()
}

// newPoolEventRecorder returns an empty recorder.
//
// Returns:
//   - *poolEventRecorder: recorder ready to be installed as ClusterConfig.testPoolHook
func newPoolEventRecorder() *poolEventRecorder {
	return &poolEventRecorder{
		chans:    map[poolEvent]chan *HostInfo{},
		handlers: map[poolEvent]func(host *HostInfo){},
		counts:   map[poolEvent]int{},
		drops:    map[poolEvent]int{},
	}
}

// TestPoolEventRecorder_OverflowFailsBarriersLoudly proves an arrival channel that
// overflowed fails its barriers with a drop diagnostic instead of hanging on them.
//
// The recorder is driven directly, with nothing consuming the channel, so publishing
// poolEventChanBuffer+3 checkpoints leaves the counts complete and three arrivals dropped.
// Both barriers then refuse to wait while the channel still holds poolEventChanBuffer
// arrivals they could have read, because those arrivals no longer account for every
// checkpoint, and they keep refusing once the channel has been drained, which is the state
// a barrier would otherwise have blocked in until its budget expired.
func TestPoolEventRecorder_OverflowFailsBarriersLoudly(t *testing.T) {
	const overflow = 3
	const published = poolEventChanBuffer + overflow

	events := newPoolEventRecorder()
	host := &HostInfo{}

	for range published {
		events.hook(poolFillDone, host)
	}

	require.Equal(t, published, events.count(poolFillDone), "every checkpoint must be counted")
	require.Equal(t, overflow, events.dropped(poolFillDone),
		"every publication past the buffer must be counted as a drop")
	require.Zero(t, events.dropped(poolConnectAttempt), "an event that never fired must not report a drop")
	require.NoError(t, events.dropErr(poolConnectAttempt, "an event that never fired"),
		"a drop on one event must not disturb a barrier keyed on another")

	ch := events.channel(poolFillDone)
	require.Len(t, ch, poolEventChanBuffer, "the buffer must be full")

	gotHost, err := events.awaitErr(poolFillDone, "the fill to finish")
	require.Nil(t, gotHost, "a barrier that cannot trust the channel must not report a host")
	requireDropDiagnostic(t, err, overflow, "the fill to finish")

	err = events.awaitEachHostErr(poolFillDone, []*HostInfo{host}, "every pool to finish")
	requireDropDiagnostic(t, err, overflow, "every pool to finish")

	require.Len(t, ch, poolEventChanBuffer, "the barriers must fail on the drop, not drain the channel")

	for range poolEventChanBuffer {
		<-ch
	}
	require.Empty(t, ch, "the drain must leave nothing an arrival could be mistaken for")

	// The dropped checkpoints are gone for good, so this is the wait that used to hang.
	gotHost, err = events.awaitErr(poolFillDone, "a dropped fill to finish")
	require.Nil(t, gotHost, "a barrier that cannot trust the channel must not report a host")
	requireDropDiagnostic(t, err, overflow, "a dropped fill to finish")

	err = events.awaitEachHostErr(poolFillDone, []*HostInfo{host}, "a dropped pool to finish")
	requireDropDiagnostic(t, err, overflow, "a dropped pool to finish")
}

// TestPoolEventRecorder_DropDuringReceiveFailsBarriers proves a publication dropped
// after a barrier's pre-check still fails that barrier.
//
// The pre-check cannot see a drop that has not happened yet,
// so a barrier that only checked before reading the channel would return the arrival it had just taken
// and report success for a channel that no longer accounts for every checkpoint.
// The recorder's afterReceive seam reproduces exactly that interleaving:
// it overflows the channel between the receive and the post-check.
//
// awaitEachHostErr is driven with a single host so the receive under test is the final one -
// the arrival that empties the pending set and ends the loop,
// which is the case a check placed only at the top of the loop would never reach.
func TestPoolEventRecorder_DropDuringReceiveFailsBarriers(t *testing.T) {
	const what = "a fill that was dropped mid-receive"

	host := &HostInfo{}

	t.Run("awaitErr", func(t *testing.T) {
		events := newPoolEventRecorder()
		events.hook(poolFillDone, host)
		events.onReceive(func() { overflowPoolEventChannel(events, poolFillDone, host) })

		gotHost, err := events.awaitErr(poolFillDone, what)

		require.Nil(t, gotHost, "a barrier that cannot trust the channel must not report a host")
		requireDropDiagnostic(t, err, 1, what)
	})

	t.Run("awaitEachHostErr", func(t *testing.T) {
		events := newPoolEventRecorder()
		events.hook(poolFillDone, host)
		events.onReceive(func() { overflowPoolEventChannel(events, poolFillDone, host) })

		err := events.awaitEachHostErr(poolFillDone, []*HostInfo{host}, what)

		requireDropDiagnostic(t, err, 1, what)
	})
}

// poolEventName returns the name of ev for use in a diagnostic.
//
// Returns:
//   - string: the constant's name, or a numeric form for an event added since
func poolEventName(ev poolEvent) string {
	switch ev {
	case poolConnectAttempt:
		return "poolConnectAttempt"
	case poolConnAppended:
		return "poolConnAppended"
	case poolFillAsyncStart:
		return "poolFillAsyncStart"
	case poolFillAdmission:
		return "poolFillAdmission"
	case poolFillDone:
		return "poolFillDone"
	default:
		return fmt.Sprintf("poolEvent(%d)", uint8(ev))
	}
}

// requireDropDiagnostic asserts err is the drop diagnostic and names everything a reader
// needs in order to see why the barrier gave up.
//
// Parameters:
//   - err: the error a barrier returned
//   - drops: how many publications the recorder dropped
//   - what: the description the barrier was called with
func requireDropDiagnostic(t *testing.T, err error, drops int, what string) {
	t.Helper()

	require.ErrorIs(t, err, errFillTestEventDropped, "the barrier must fail on the drop, not on the deadline")
	require.Contains(t, err.Error(), "poolFillDone", "the diagnostic must name the event")
	require.Contains(t, err.Error(), fmt.Sprintf("%d time(s)", drops), "the diagnostic must report the drop count")
	require.Contains(t, err.Error(), fmt.Sprintf("%d-slot buffer overflowed", poolEventChanBuffer),
		"the diagnostic must say the buffer overflowed")
	require.Contains(t, err.Error(), "the barrier is unreliable", "the diagnostic must say the barrier cannot be trusted")
	require.Contains(t, err.Error(), what, "the diagnostic must repeat what the barrier was waiting for")
}

// overflowPoolEventChannel publishes ev until exactly one arrival has been dropped.
//
// The arrival channel must be empty, which is the state a barrier leaves it in when it
// has just taken the only arrival off it.
//
// Parameters:
//   - r: the recorder to publish into
//   - ev: the checkpoint to publish
//   - host: the host reported with every publication
func overflowPoolEventChannel(r *poolEventRecorder, ev poolEvent, host *HostInfo) {
	for range poolEventChanBuffer + 1 {
		r.hook(ev, host)
	}
}

// hook records the event, publishes it and runs the registered handler, if any.
//
// The publication stays non-blocking so a full channel cannot stall the driver goroutine
// that reached the checkpoint; an arrival that does not fit is counted as a drop instead.
// The handler runs outside the mutex, so it may call back into the recorder.
func (r *poolEventRecorder) hook(ev poolEvent, host *HostInfo) {
	r.mu.Lock()
	r.counts[ev]++
	ch := r.channelLocked(ev)
	handler := r.handlers[ev]
	select {
	case ch <- host:
	default:
		r.drops[ev]++
	}
	r.mu.Unlock()

	if handler != nil {
		handler(host)
	}
}

// channelLocked returns the channel for ev, creating it on first use.
//
// The caller must hold r.mu.
func (r *poolEventRecorder) channelLocked(ev poolEvent) chan *HostInfo {
	ch, ok := r.chans[ev]
	if !ok {
		ch = make(chan *HostInfo, poolEventChanBuffer)
		r.chans[ev] = ch
	}
	return ch
}

// channel returns the arrival channel of ev, creating it on first use.
//
// Returns:
//   - chan *HostInfo: the channel a test can wake up on
func (r *poolEventRecorder) channel(ev poolEvent) chan *HostInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.channelLocked(ev)
}

// on installs the handler that runs inside the driver goroutine reaching ev.
func (r *poolEventRecorder) on(ev poolEvent, handler func(host *HostInfo)) {
	r.mu.Lock()
	r.handlers[ev] = handler
	r.mu.Unlock()
}

// onReceive installs the seam that runs inside a barrier after every successful receive.
//
// Parameters:
//   - hook: run after an arrival is taken off the channel, before the drop count is re-checked
func (r *poolEventRecorder) onReceive(hook func()) {
	r.mu.Lock()
	r.afterReceive = hook
	r.mu.Unlock()
}

// notifyReceived runs the afterReceive seam, if one is installed.
//
// The seam runs outside the mutex, so it may publish further events.
func (r *poolEventRecorder) notifyReceived() {
	r.mu.Lock()
	hook := r.afterReceive
	r.mu.Unlock()

	if hook != nil {
		hook()
	}
}

// dropped returns how many publications of ev did not fit the arrival channel.
//
// Returns:
//   - int: the number of dropped publications; zero while the channel is trustworthy
func (r *poolEventRecorder) dropped(ev poolEvent) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.drops[ev]
}

// dropErr reports whether a barrier keyed on ev may still trust the arrival channel.
//
// Parameters:
//   - ev: the checkpoint the barrier is keyed on
//   - what: what the barrier is waiting for, used in the message
//
// Returns:
//   - error: nil while nothing was dropped,
//     otherwise an errFillTestEventDropped naming the event and the drop count
func (r *poolEventRecorder) dropErr(ev poolEvent, what string) error {
	drops := r.dropped(ev)
	if drops == 0 {
		return nil
	}

	return fmt.Errorf(
		"%w: %s was dropped %d time(s) while waiting for %s: "+
			"the %d-slot buffer overflowed, so the barrier is unreliable",
		errFillTestEventDropped, poolEventName(ev), drops, what, poolEventChanBuffer)
}

// await blocks until ev is reached once more.
//
// Parameters:
//   - t: the test; a missing checkpoint or a dropped publication fails it
//   - ev: the checkpoint to wait for
//   - what: what the caller is waiting for, used in the failure message
//
// Returns:
//   - *HostInfo: the host the checkpoint was reached for
func (r *poolEventRecorder) await(t *testing.T, ev poolEvent, what string) *HostInfo {
	t.Helper()

	host, err := r.awaitErr(ev, what)
	if err != nil {
		t.Fatalf("%s", err)
		return nil
	}
	return host
}

// awaitErr blocks until ev is reached once more, or reports why it refuses to.
//
// It is the barrier await fails on, split out so a test can assert on the diagnostic.
// A drop is checked before the channel is read,
// because a channel that overflowed no longer says which checkpoints were reached;
// again after the arrival is taken off it,
// because a publication can be dropped between those two moments;
// and again on the deadline,
// so a barrier that overflowed while it waited reports the overflow rather than a bare timeout.
//
// Returns:
//   - *HostInfo: the host the checkpoint was reached for
//   - error: errFillTestEventDropped when a publication of ev was dropped,
//     otherwise a timeout after fillEventBudget
func (r *poolEventRecorder) awaitErr(ev poolEvent, what string) (*HostInfo, error) {
	if err := r.dropErr(ev, what); err != nil {
		return nil, err
	}

	ch := r.channel(ev)

	select {
	case host := <-ch:
		r.notifyReceived()
		if err := r.dropErr(ev, what); err != nil {
			return nil, err
		}
		return host, nil
	case <-time.After(fillEventBudget):
		if err := r.dropErr(ev, what); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("timed out after %v waiting for %s", fillEventBudget, what)
	}
}

// awaitEachHost blocks until ev has been reached once for every host in hosts.
//
// The checkpoints of several hosts interleave, so arrivals are matched against the
// outstanding set by pointer identity instead of being awaited one host at a time.
// A repeated arrival for a host already crossed off is discarded.
//
// Parameters:
//   - t: the test; a missing checkpoint or a dropped publication fails it
//   - ev: the checkpoint to collect
//   - hosts: the hosts whose checkpoint must be observed, matched by pointer
//   - what: what the caller is waiting for, used in the failure message
func (r *poolEventRecorder) awaitEachHost(t testing.TB, ev poolEvent, hosts []*HostInfo, what string) {
	t.Helper()

	if err := r.awaitEachHostErr(ev, hosts, what); err != nil {
		t.Fatalf("%s", err)
	}
}

// awaitEachHostErr collects ev for every host in hosts, or reports why it refuses to.
//
// It is the barrier awaitEachHost fails on, split out so a test can assert on the diagnostic.
// Drops are checked before the channel is read, after every arrival, and on the deadline,
// because an overflowed channel no longer says which hosts reached the checkpoint.
//
// Returns:
//   - error: errFillTestEventDropped when a publication of ev was dropped,
//     otherwise a timeout naming the hosts still outstanding
func (r *poolEventRecorder) awaitEachHostErr(ev poolEvent, hosts []*HostInfo, what string) error {
	pending := make(map[*HostInfo]struct{}, len(hosts))
	for _, host := range hosts {
		pending[host] = struct{}{}
	}

	ch := r.channel(ev)
	deadline := time.After(fillEventBudget)
	for len(pending) > 0 {
		if err := r.dropErr(ev, what); err != nil {
			return err
		}

		select {
		case host := <-ch:
			delete(pending, host)
			r.notifyReceived()
			if err := r.dropErr(ev, what); err != nil {
				return err
			}
		case <-deadline:
			if err := r.dropErr(ev, what); err != nil {
				return err
			}
			return fmt.Errorf("timed out after %v waiting for %s: %d host(s) never reached the checkpoint",
				fillEventBudget, what, len(pending))
		}
	}

	return nil
}

// count returns how often ev has been reached.
//
// Returns:
//   - int: the number of times the checkpoint fired
func (r *poolEventRecorder) count(ev poolEvent) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[ev]
}

// fillHarness is a session whose pools can be emptied and refilled under test control.
type fillHarness struct {
	session   *Session
	hosts     []*HostInfo
	dialer    *gatedHostDialer
	events    *poolEventRecorder
	collector *hostStateCollector
}

// fillHarnessOpts tunes newFillHarnessOpts.
type fillHarnessOpts struct {
	// tune applies cluster tweaks before CreateSession.
	tune func(*ClusterConfig)
	// recvHook runs in every test server's receive loop, synchronously, before the
	// request is processed; ip is the server's loopback address without the port.
	// Blocking in it holds that connection's request in flight.
	recvHook func(ip string, f *framer)
	// respHook, when set, is offered every request of every test server before
	// the server's default handling; ip is the server's loopback address without
	// the port.
	// Returning true means it wrote the response into resp and the default
	// handling is skipped; returning false falls through, so a fixture only has
	// to answer the requests it cares about.
	respHook func(ip string, srv *TestServer, req, resp *framer) bool

	// proto selects the protocol version for both the servers and the cluster.
	// Zero means defaultProto. Setting it on the cluster alone would not do:
	// the fixture servers speak whatever they were started with.
	proto protoVersion

	// hooks pins interleavings inside the driver; see connTestHooks.
	hooks *connTestHooks
}

// newFillHarness starts servers hosts and returns a connected session wired to a
// gated dialer and a pool event recorder.
//
// Parameters:
//   - t: the test or benchmark; servers and the session are registered for cleanup
//   - hosts: how many test servers to start (1 to len(fillLoopbackAddrs))
//   - tune: optional cluster tweaks applied before CreateSession
//
// Returns:
//   - *fillHarness: the connected harness, with every pool filled
func newFillHarness(t testing.TB, hosts int, tune func(*ClusterConfig)) *fillHarness {
	t.Helper()
	return newFillHarnessOpts(t, hosts, fillHarnessOpts{tune: tune})
}

// newFillHarnessOpts is newFillHarness with every option.
//
// A single-host harness uses the plain 127.0.0.1 test server; a multi-host harness
// binds distinct loopback aliases and skips the test where they are unavailable.
//
// Parameters:
//   - t: the test or benchmark; servers and the session are registered for cleanup
//   - hosts: how many test servers to start
//   - opts: the options
//
// Returns:
//   - *fillHarness: the connected harness, with every pool filled
func newFillHarnessOpts(t testing.TB, hosts int, opts fillHarnessOpts) *fillHarness {
	t.Helper()

	proto := opts.proto
	if proto == 0 {
		proto = defaultProto
	}

	startServer := func(addr string) *TestServer {
		ip, _, err := net.SplitHostPort(addr)
		require.NoError(t, err, "split %q", addr)
		var recvHook func(*framer)
		if opts.recvHook != nil {
			recvHook = func(f *framer) { opts.recvHook(ip, f) }
		}
		var respFn func(srv *TestServer, req, resp *framer) bool
		if opts.respHook != nil {
			respFn = func(srv *TestServer, req, resp *framer) bool {
				return opts.respHook(ip, srv, req, resp)
			}
		}
		srv := newTestServerOpts{
			addr:     addr,
			protocol: uint8(proto),
			recvHook: recvHook,
			respFn:   respFn,
		}.newServer(t, testServerContext(t))
		t.Cleanup(srv.Stop)
		return srv
	}

	addresses := make([]string, 0, hosts)
	if hosts == 1 {
		addresses = append(addresses, startServer("127.0.0.1:0").Address)
	} else {
		require.LessOrEqual(t, hosts, len(fillLoopbackAddrs), "no loopback alias reserved for host %d", hosts)
		for i := range hosts {
			addr := fillLoopbackAddrs[i]
			mustBindLoopbackAddr(t, addr)
			addresses = append(addresses, startServer(addr).Address)
		}
	}
	tune := opts.tune

	dialer := newGatedHostDialer()
	events := newPoolEventRecorder()
	collector := newHostStateCollector()

	cluster := testCluster(proto, addresses...)
	cluster.NumConns = 1
	cluster.testHooks = opts.hooks
	cluster.HostDialer = dialer
	cluster.PoolConfig.HostSelectionPolicy = collector
	cluster.ReconnectionPolicy = &ConstantReconnectionPolicy{MaxRetries: 1, Interval: time.Millisecond}
	cluster.testPoolHook = events.hook
	if tune != nil {
		tune(cluster)
	}

	session, err := cluster.CreateSession()
	require.NoError(t, err, "CreateSession")
	t.Cleanup(session.Close)

	ring := session.ring.allHosts()
	require.Len(t, ring, hosts, "the fixture must produce exactly %d ring hosts", hosts)

	harness := &fillHarness{session: session, hosts: ring, dialer: dialer, events: events, collector: collector}

	// The UP events of several hosts interleave, so collect them all in one pass
	// instead of waiting for one host at a time.
	pending := make(map[*HostInfo]struct{}, len(ring))
	for _, host := range ring {
		pending[host] = struct{}{}
	}
	deadline := time.After(fillEventBudget)
	for len(pending) > 0 {
		select {
		case host := <-collector.up:
			delete(pending, host)
		case <-deadline:
			t.Fatalf("timed out after %v waiting for %d fixture pools to finish their initial fill",
				fillEventBudget, len(pending))
		}
	}

	// The initial fill publishes its UP event from the synchronous branch and releases its
	// claim from the asynchronous one, so the UP barrier above says nothing about that
	// claim.
	// Waiting for the release to be observed, rather than merely for the claim count to
	// read zero, is what makes the fixture's own releases attributable: fillDone decrements
	// under pool.mu and only then reaches poolFillDone, so a count-only barrier can return
	// between the two and let a later phase mistake this event for one of its own.
	// It also puts the initial asynchronous branch past its checkpoints, so a test that
	// parks that branch can no longer park the fixture's.
	harness.events.awaitEachHost(t, poolFillDone, ring, "the fixture pools to release their initial fill claim")

	for _, host := range ring {
		pool := harness.pool(t, host)
		require.Equal(t, 1, pool.Size(), "every fixture pool must start with one connection")
		awaitNoPendingFills(t, session.pool, pool)
	}

	return harness
}

// mustBindLoopbackAddr skips the test when addr cannot be bound.
//
// Parameters:
//   - t: the test or benchmark to skip
//   - addr: the loopback address the fixture is about to listen on
func mustBindLoopbackAddr(t testing.TB, addr string) {
	t.Helper()

	probe, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("cannot bind the loopback address %s: %v", addr, err)
	}
	require.NoError(t, probe.Close())
}

// pool returns the registered pool for host.
//
// Returns:
//   - *hostConnPool: the host's pool
func (h *fillHarness) pool(t testing.TB, host *HostInfo) *hostConnPool {
	t.Helper()

	pool, ok := h.session.pool.getPool(host)
	require.True(t, ok, "no pool registered for host %s", host.ConnectAddressAndPort())
	return pool
}

// query runs a trivial query in the background and reports its error.
//
// Returns:
//   - <-chan error: receives the query's error exactly once
func (h *fillHarness) query(ctx context.Context, tune func(*Query)) <-chan error {
	result := make(chan error, 1)
	qry := h.session.Query("void").WithContext(ctx)
	if tune != nil {
		tune(qry)
	}
	go func() { result <- qry.Exec() }()
	return result
}

// awaitQuery blocks until the query reports its outcome.
//
// Returns:
//   - error: the query's error
func awaitQuery(t *testing.T, result <-chan error) error {
	t.Helper()

	select {
	case err := <-result:
		return err
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for the query to finish", fillEventBudget)
		return nil
	}
}

// awaitSignal blocks until ch fires, failing the test if the budget expires first.
func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for %s", fillEventBudget, what)
	}
}

// detachPoolConn removes the pool's last connection without closing it,
// leaving the pool empty and with no fill claim of its own.
//
// Returns:
//   - *Conn: the detached connection
func detachPoolConn(t *testing.T, pool *hostConnPool) *Conn {
	t.Helper()

	pool.mu.Lock()
	defer pool.mu.Unlock()

	require.NotEmpty(t, pool.conns, "the pool must hold a connection to detach")
	conn := pool.conns[len(pool.conns)-1]
	pool.conns = pool.conns[:len(pool.conns)-1]
	return conn
}

// attachPoolConn puts a detached connection back into the pool.
func attachPoolConn(pool *hostConnPool, conn *Conn) {
	pool.mu.Lock()
	pool.conns = append(pool.conns, conn)
	pool.mu.Unlock()
}

// pendingFills reads the pool's published fill claims.
//
// Returns:
//   - int: the number of claims not yet released
func pendingFills(pool *hostConnPool) int {
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	return pool.fillsPending
}

// saturatePoolConns exhausts the stream generator of every connection in the pool,
// so Pick finds connections but none of them usable.
func saturatePoolConns(pool *hostConnPool) {
	pool.mu.RLock()
	conns := append([]*Conn(nil), pool.conns...)
	pool.mu.RUnlock()

	for _, conn := range conns {
		for {
			if _, ok := conn.streams.GetStream(); !ok {
				break
			}
		}
	}
}

// TestQueryWaitsForInFlightFill is the baseline of the whole item:
// an empty pool with a fill in flight must not produce ErrNoConnections.
//
// The pool's only connection is closed through the production error path,
// which removes it and claims a fill,
// and that fill is parked in the dialer.
// A query issued in that window used to fail immediately;
// it must now wait and succeed once the fill lands.
func TestQueryWaitsForInFlightFill(t *testing.T) {
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.Timeout = 5 * time.Second
	})
	pool := harness.pool(t, harness.hosts[0])

	harness.dialer.arm(nil)
	conn := pool.Pick()
	require.NotNil(t, conn, "the fixture pool must serve a connection")
	conn.closeWithError(errFillTestConnClosed)
	harness.dialer.awaitStarted(t)

	waiting := make(chan struct{}, 1)
	harness.session.executor.testBeforeWait = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}

	result := harness.query(t.Context(), nil)
	select {
	case <-waiting:
	case err := <-result:
		t.Fatalf("the query finished before waiting for the in-flight fill: %v", err)
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for the query to await the fill", fillEventBudget)
	}

	harness.dialer.releaseAll()
	require.NoError(t, awaitQuery(t, result), "the query must succeed once the fill lands")
}

// TestAwaitFill_NoMissedWakeBeforeSubscription proves a connection appearing between
// the nil Pick and the recheck is used instead of waited for.
//
// The recheck happens under one read lock, so the connection the hook publishes is
// visible to the same read that would otherwise have recorded a fill candidate.
func TestAwaitFill_NoMissedWakeBeforeSubscription(t *testing.T) {
	harness := newFillHarness(t, 1, nil)
	pool := harness.pool(t, harness.hosts[0])

	harness.dialer.arm(nil)
	conn := detachPoolConn(t, pool)

	var once sync.Once
	harness.session.executor.testBeforeSnapshot = func() {
		once.Do(func() {
			attachPoolConn(pool, conn)
			pool.notify()
		})
	}
	var waited atomic.Bool
	harness.session.executor.testBeforeWait = func() { waited.Store(true) }

	require.NoError(t, awaitQuery(t, harness.query(t.Context(), nil)))
	require.False(t, waited.Load(), "the query must not have waited for a fill")
}

// TestAwaitFill_WakesOnCapturedGeneration proves a connection appearing after the
// waiter subscribed still wakes it.
//
// The generation is captured before the pool is read, so the notify that follows the
// append cannot be lost.
func TestAwaitFill_WakesOnCapturedGeneration(t *testing.T) {
	harness := newFillHarness(t, 1, nil)
	pool := harness.pool(t, harness.hosts[0])

	harness.dialer.arm(nil)
	conn := detachPoolConn(t, pool)

	var once sync.Once
	var waits atomic.Int32
	harness.session.executor.testBeforeWait = func() {
		waits.Add(1)
		once.Do(func() {
			attachPoolConn(pool, conn)
			pool.notify()
		})
	}

	require.NoError(t, awaitQuery(t, harness.query(t.Context(), nil)))
	require.GreaterOrEqual(t, waits.Load(), int32(1), "the query must have waited for a fill")
}

// TestAwaitFill_ScheduledBeforeFillClaimDoesNotFailFast proves a claim published by a
// concurrent Pick after the waiter's snapshot is observed on the next snapshot.
//
// The claim is published before its goroutine is spawned, so no window exists in which
// a scheduled fill looks like no fill at all.
func TestAwaitFill_ScheduledBeforeFillClaimDoesNotFailFast(t *testing.T) {
	harness := newFillHarness(t, 1, nil)
	pool := harness.pool(t, harness.hosts[0])

	harness.dialer.arm(nil)
	detachPoolConn(t, pool)

	var once sync.Once
	picked := make(chan struct{})
	harness.session.executor.testBeforeWait = func() {
		once.Do(func() {
			require.Nil(t, pool.Pick(), "the emptied pool must not serve a connection")
			close(picked)
		})
	}

	result := harness.query(t.Context(), func(qry *Query) { qry.RetryPolicy(&SimpleRetryPolicy{NumRetries: 1}) })
	awaitSignal(t, picked, "the waiting query to publish a second fill claim")

	harness.dialer.releaseAll()
	require.NoError(t, awaitQuery(t, result), "the query must succeed once a fill lands")
}

// TestAwaitFill_UnconvictedFailedCycleReturns proves a waiter gives up once the fill it
// waited for ended without a connection.
//
// An always-false ConvictionPolicy leaves the empty pool registered, so the candidate is
// dropped on the state read, not on a removal.
func TestAwaitFill_UnconvictedFailedCycleReturns(t *testing.T) {
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.Timeout = 0
		cluster.ConvictionPolicy = &recordingConvictionPolicy{onFailure: func(*HostInfo) bool { return false }}
	})
	pool := harness.pool(t, harness.hosts[0])

	harness.dialer.arm(func(string) error { return errFillTestDialRefused })
	detachPoolConn(t, pool)

	waiting := make(chan struct{}, 1)
	harness.session.executor.testBeforeWait = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}

	result := harness.query(t.Context(), nil)
	select {
	case <-waiting:
	case err := <-result:
		t.Fatalf("the query finished before waiting for the in-flight fill: %v", err)
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for the query to await the fill", fillEventBudget)
	}

	harness.dialer.releaseAll()
	require.ErrorIs(t, awaitQuery(t, result), ErrNoConnections,
		"a failed, unconvicted fill cycle must end the wait")
}

// TestAwaitFill_FollowsNewerInFlightCycle proves a waiter woken by a failed cycle follows
// a newer cycle instead of giving up.
//
// Both subtests publish the newer fill through a production path — never by touching
// fillsPending — so the claim ordering under test is the real one.
func TestAwaitFill_FollowsNewerInFlightCycle(t *testing.T) {
	cases := []struct {
		name    string
		publish func(t *testing.T, harness *fillHarness, pool *hostConnPool, conn *Conn)
	}{
		{
			name: "scheduled",
			publish: func(t *testing.T, _ *fillHarness, pool *hostConnPool, _ *Conn) {
				require.Nil(t, pool.Pick(), "the emptied pool must not serve a connection")
			},
		},
		{
			name: "claimed",
			publish: func(t *testing.T, harness *fillHarness, pool *hostConnPool, conn *Conn) {
				attachPoolConn(pool, conn)
				pool.HandleError(conn, errFillTestConnClosed, true)
				harness.events.await(t, poolConnectAttempt, "the newer fill to start dialling")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
				cluster.Timeout = 0
				cluster.ConvictionPolicy = &recordingConvictionPolicy{onFailure: func(*HostInfo) bool { return false }}
			})
			pool := harness.pool(t, harness.hosts[0])

			harness.dialer.arm(func(string) error { return errFillTestDialRefused })
			conn := detachPoolConn(t, pool)

			var wakes atomic.Int32
			published := make(chan struct{})
			harness.session.executor.testAfterWake = func() {
				if wakes.Add(1) != 2 {
					return
				}
				tc.publish(t, harness, pool, conn)
				close(published)
			}

			waiting := make(chan struct{}, 1)
			harness.session.executor.testBeforeWait = func() {
				select {
				case waiting <- struct{}{}:
				default:
				}
			}

			result := harness.query(t.Context(), nil)
			awaitSignal(t, waiting, "the query to await the first fill")

			// Let the first cycle fail; the wake it produces must not end the wait.
			harness.dialer.releaseOne()
			awaitSignal(t, published, "the newer fill cycle to be published")

			harness.dialer.setErr(nil)
			harness.dialer.releaseAll()
			require.NoError(t, awaitQuery(t, result), "the query must follow the newer fill cycle")
		})
	}
}

// TestAwaitFill_NewerClaimRacesIdleSnapshot proves a claim published while the waiter is
// between its snapshot and its state read is still observed.
//
// The waiter is parked after the snapshot, outside every lock, so the competing Pick wins
// the race to pool.mu; the waiter's single read then has to see the claim.
func TestAwaitFill_NewerClaimRacesIdleSnapshot(t *testing.T) {
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.Timeout = 0
		cluster.ConvictionPolicy = &recordingConvictionPolicy{onFailure: func(*HostInfo) bool { return false }}
	})
	pool := harness.pool(t, harness.hosts[0])

	harness.dialer.arm(func(string) error { return errFillTestDialRefused })
	detachPoolConn(t, pool)

	var reads atomic.Int32
	parked := make(chan struct{})
	resume := make(chan struct{})
	harness.session.executor.testBeforePickOrState = func() {
		if reads.Add(1) != 2 {
			return
		}
		close(parked)
		<-resume
	}

	waiting := make(chan struct{}, 1)
	harness.session.executor.testBeforeWait = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}

	result := harness.query(t.Context(), nil)
	awaitSignal(t, waiting, "the query to await the first fill")

	// The first cycle fails and wakes the waiter, which parks before its state read.
	harness.dialer.releaseOne()
	awaitSignal(t, parked, "the woken query to park before its state read")

	require.Nil(t, pool.Pick(), "the emptied pool must not serve a connection")
	harness.events.await(t, poolConnectAttempt, "the newer fill to start dialling")
	harness.dialer.setErr(nil)
	close(resume)

	harness.dialer.releaseAll()
	require.NoError(t, awaitQuery(t, result), "the waiter must keep the candidate and follow the newer claim")
}

// TestDo_PoolHasConnsRecheckFollowsClaimedFill proves the initial recheck cannot observe a
// pool as empty and idle while a removal has already claimed a fill.
//
// The removal and the claim are one critical section, so the read that follows them sees
// both or neither.
func TestDo_PoolHasConnsRecheckFollowsClaimedFill(t *testing.T) {
	harness := newFillHarness(t, 1, nil)
	pool := harness.pool(t, harness.hosts[0])

	harness.dialer.arm(nil)
	saturatePoolConns(pool)
	conn := harness.pickAnyConn(t, pool)

	var once sync.Once
	parked := make(chan struct{})
	resume := make(chan struct{})
	harness.session.executor.testBeforeSnapshot = func() {
		once.Do(func() {
			close(parked)
			<-resume
		})
	}

	result := harness.query(t.Context(), nil)
	awaitSignal(t, parked, "the query to park before its state read")

	conn.closeWithError(errFillTestConnClosed)
	harness.dialer.awaitStarted(t)
	close(resume)

	harness.dialer.releaseAll()
	require.NoError(t, awaitQuery(t, result), "the query must follow the claimed fill")
}

// TestAwaitFill_PoolHasConnsRecheckFollowsClaimedFill proves the same for a waiter's
// recheck after it was woken by a successful fill.
func TestAwaitFill_PoolHasConnsRecheckFollowsClaimedFill(t *testing.T) {
	harness := newFillHarness(t, 1, nil)
	pool := harness.pool(t, harness.hosts[0])

	harness.dialer.arm(nil)
	detachPoolConn(t, pool)

	var reads atomic.Int32
	parked := make(chan struct{})
	resume := make(chan struct{})
	harness.session.executor.testBeforePickOrState = func() {
		if reads.Add(1) != 2 {
			return
		}
		close(parked)
		<-resume
	}

	waiting := make(chan struct{}, 1)
	harness.session.executor.testBeforeWait = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}

	result := harness.query(t.Context(), nil)
	awaitSignal(t, waiting, "the query to await the first fill")

	// The first cycle succeeds and appends a connection, waking the waiter.
	harness.dialer.releaseOne()
	awaitSignal(t, parked, "the woken query to park before its state read")

	appended := harness.pickAnyConn(t, pool)
	appended.closeWithError(errFillTestConnClosed)
	harness.dialer.awaitStarted(t)
	close(resume)

	harness.dialer.releaseAll()
	require.NoError(t, awaitQuery(t, result), "the waiter must follow the newly claimed fill")
}

// pickAnyConn returns the pool's current connection, waiting for one to appear.
//
// Returns:
//   - *Conn: the connection the pool holds
func (h *fillHarness) pickAnyConn(t *testing.T, pool *hostConnPool) *Conn {
	t.Helper()

	pool.mu.RLock()
	defer pool.mu.RUnlock()
	require.NotEmpty(t, pool.conns, "the pool must hold a connection")
	return pool.conns[0]
}

// infoPanickingLogger is a StructuredLogger whose Info panics,
// standing in for an application logger that misbehaves inside HandleError.
type infoPanickingLogger struct {
	StructuredLogger
}

var _ StructuredLogger = (*infoPanickingLogger)(nil)

// Info always panics.
func (l *infoPanickingLogger) Info(_ string, _ ...LogField) {
	panic("gocql: logger panic injected by the fill test")
}

// awaitPoolLock proves pool.mu can still be taken, i.e. it was not stranded.
func awaitPoolLock(t *testing.T, pool *hostConnPool) {
	t.Helper()

	locked := make(chan struct{})
	go func() {
		pool.mu.Lock()
		pool.mu.Unlock()
		close(locked)
	}()
	awaitSignal(t, locked, "pool.mu to become available again")
}

// TestHandleError_UnlocksClosedBranch proves the closed branch releases pool.mu and
// claims nothing.
func TestHandleError_UnlocksClosedBranch(t *testing.T) {
	pool := &hostConnPool{closed: true, size: 1, logger: newTestLogger(LogLevelDebug)}

	pool.HandleError(&Conn{}, errFillTestConnClosed, true)

	awaitPoolLock(t, pool)
	require.Zero(t, pendingFills(pool), "the closed branch must not publish a fill claim")
	require.False(t, pool.filling, "the closed branch must not start a fill")
}

// TestHandleError_PanickingLoggerDoesNotStrandMutex proves an application logger that
// panics leaves neither pool.mu held nor the pool half-mutated.
//
// The claim is published only after the removal, which the panic never reaches.
func TestHandleError_PanickingLoggerDoesNotStrandMutex(t *testing.T) {
	conn := &Conn{}
	pool := &hostConnPool{size: 1, conns: []*Conn{conn}, logger: &infoPanickingLogger{}}

	require.Panics(t, func() { pool.HandleError(conn, errFillTestConnClosed, true) },
		"the injected logger panic must surface")

	awaitPoolLock(t, pool)
	require.Zero(t, pendingFills(pool), "no fill claim may be published before the removal")
	pool.mu.RLock()
	require.Equal(t, []*Conn{conn}, pool.conns, "the connection must still be in the pool")
	pool.mu.RUnlock()
}

// TestAwaitFill_RemoveHostWakeCannotBeMissed proves a host removed while a query waits
// for its fill ends the wait.
//
// The unregistration and the wake share one p.mu section, so the waiter either sees the
// pool gone or holds the generation the removal ends.
func TestAwaitFill_RemoveHostWakeCannotBeMissed(t *testing.T) {
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) { cluster.Timeout = 0 })
	host := harness.hosts[0]
	pool := harness.pool(t, host)

	harness.dialer.arm(nil)
	detachPoolConn(t, pool)

	waiting := make(chan struct{}, 1)
	harness.session.executor.testBeforeWait = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}

	result := harness.query(t.Context(), nil)
	awaitSignal(t, waiting, "the query to await the fill")

	// The child pool is closed only after the waiter has given up, so the wake must
	// come from the unregistration alone.
	queryDone := make(chan error, 1)
	var once sync.Once
	harness.session.pool.testAfterParentNotify = func() {
		once.Do(func() { queryDone <- awaitQuery(t, result) })
	}

	harness.session.handleHostDown(host)
	select {
	case err := <-queryDone:
		require.ErrorIs(t, err, ErrNoConnections, "a removed host must end the wait")
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for the removal to end the wait", fillEventBudget)
	}
}

// TestPolicyConnPoolClose_IsTerminal proves closing the session's pool is terminal:
// every waiter gives up with ErrSessionClosed, a concurrent addHost registers nothing,
// and Close itself returns.
func TestPolicyConnPoolClose_IsTerminal(t *testing.T) {
	const waiters = 4

	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) { cluster.Timeout = 0 })
	host := harness.hosts[0]
	pool := harness.pool(t, host)

	harness.dialer.arm(nil)
	detachPoolConn(t, pool)

	waiting := make(chan struct{}, waiters)
	harness.session.executor.testBeforeWait = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}

	results := make([]<-chan error, waiters)
	for i := range waiters {
		results[i] = harness.query(t.Context(), nil)
	}
	for range waiters {
		awaitSignal(t, waiting, "every query to await the fill")
	}

	// Every waiter must re-snapshot and give up before the children are closed.
	var once sync.Once
	errs := make(chan error, waiters)
	harness.session.pool.testAfterParentNotify = func() {
		once.Do(func() {
			for _, result := range results {
				errs <- awaitQuery(t, result)
			}
		})
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		harness.session.pool.addHost(host)
	}()

	closed := make(chan struct{})
	go func() {
		harness.session.Close()
		close(closed)
	}()

	for range waiters {
		select {
		case err := <-errs:
			require.ErrorIs(t, err, ErrSessionClosed, "a closed session pool must end every wait")
		case <-time.After(fillEventBudget):
			t.Fatalf("timed out after %v waiting for a waiter to give up", fillEventBudget)
		}
	}

	awaitSignal(t, closed, "Close to return")
	wg.Wait()

	harness.session.pool.mu.RLock()
	defer harness.session.pool.mu.RUnlock()
	require.Nil(t, harness.session.pool.wake, "no successor generation may exist after Close")
	require.Empty(t, harness.session.pool.hostConnPools, "no pool may be registered after Close")
}

// TestAwaitFill_DoesNotWaitForSaturatedPool proves a saturated pool keeps its
// next-host behaviour instead of becoming something to wait for.
func TestAwaitFill_DoesNotWaitForSaturatedPool(t *testing.T) {
	harness := newFillHarness(t, 2, nil)

	saturatePoolConns(harness.pool(t, harness.hosts[0]))

	var waited atomic.Bool
	harness.session.executor.testBeforeWait = func() { waited.Store(true) }

	require.NoError(t, awaitQuery(t, harness.query(t.Context(), nil)),
		"the query must be served by the second host")
	require.False(t, waited.Load(), "a saturated pool must not be waited for")
}

// TestAwaitFill_AllSaturatedFailsImmediately proves an entirely saturated cluster still
// fails fast.
func TestAwaitFill_AllSaturatedFailsImmediately(t *testing.T) {
	harness := newFillHarness(t, 2, nil)

	for _, host := range harness.hosts {
		saturatePoolConns(harness.pool(t, host))
	}

	var waited atomic.Bool
	harness.session.executor.testBeforeWait = func() { waited.Store(true) }

	require.ErrorIs(t, awaitQuery(t, harness.query(t.Context(), nil)), ErrNoConnections,
		"a fully saturated cluster must fail fast")
	require.False(t, waited.Load(), "a saturated pool must not be waited for")
}

// TestAwaitFill_UsesSecondSuccessfulPool proves a waiter follows whichever candidate
// pool produces a connection first.
func TestAwaitFill_UsesSecondSuccessfulPool(t *testing.T) {
	harness := newFillHarness(t, 2, func(cluster *ClusterConfig) {
		cluster.Timeout = 0
		cluster.ConvictionPolicy = &recordingConvictionPolicy{onFailure: func(*HostInfo) bool { return false }}
	})

	failing := harness.hosts[0].ConnectAddressAndPort()
	harness.dialer.arm(func(addr string) error {
		if addr == failing {
			return errFillTestDialRefused
		}
		return nil
	})
	for _, host := range harness.hosts {
		detachPoolConn(t, harness.pool(t, host))
	}

	waiting := make(chan struct{}, 1)
	harness.session.executor.testBeforeWait = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}

	result := harness.query(t.Context(), nil)
	awaitSignal(t, waiting, "the query to await both fills")

	harness.dialer.releaseAll()
	require.NoError(t, awaitQuery(t, result), "the reachable host must serve the query")
}

// runStageRecorder observes queryExecutor.testRunHook checkpoints and can park the
// goroutine that reached one, so a test can order the runners of a speculative query.
type runStageRecorder struct {
	mu     sync.Mutex
	counts map[runStage]int
	chans  map[runStage]chan struct{}
	gates  map[runStage]chan struct{}
}

// newRunStageRecorder returns an empty recorder.
//
// Returns:
//   - *runStageRecorder: recorder ready to be installed as queryExecutor.testRunHook
func newRunStageRecorder() *runStageRecorder {
	return &runStageRecorder{
		counts: map[runStage]int{},
		chans:  map[runStage]chan struct{}{},
		gates:  map[runStage]chan struct{}{},
	}
}

// hook records the stage, publishes it and parks on the stage's gate, if one is installed.
func (r *runStageRecorder) hook(stage runStage) {
	r.mu.Lock()
	r.counts[stage]++
	ch := r.channelLocked(stage)
	gate := r.gates[stage]
	r.mu.Unlock()

	select {
	case ch <- struct{}{}:
	default:
	}

	if gate != nil {
		<-gate
	}
}

// channelLocked returns the arrival channel for stage, creating it on first use.
//
// The caller must hold r.mu.
func (r *runStageRecorder) channelLocked(stage runStage) chan struct{} {
	ch, ok := r.chans[stage]
	if !ok {
		ch = make(chan struct{}, 64)
		r.chans[stage] = ch
	}
	return ch
}

// gate parks every later arrival at stage until it is handed a token.
//
// Returns:
//   - chan struct{}: send one token to release one parked arrival
func (r *runStageRecorder) gate(stage runStage) chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()

	gate := make(chan struct{}, 64)
	r.gates[stage] = gate
	return gate
}

// arrivals returns the arrival channel of stage, creating it on first use.
//
// Returns:
//   - chan struct{}: receives one token per arrival at stage
func (r *runStageRecorder) arrivals(stage runStage) chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.channelLocked(stage)
}

// await blocks until stage is reached once more.
func (r *runStageRecorder) await(t *testing.T, stage runStage, what string) {
	t.Helper()

	r.mu.Lock()
	ch := r.channelLocked(stage)
	r.mu.Unlock()

	select {
	case <-ch:
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for %s", fillEventBudget, what)
	}
}

// count returns how often stage has been reached.
//
// Returns:
//   - int: the number of arrivals
func (r *runStageRecorder) count(stage runStage) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[stage]
}

// speculative configures a query for speculative execution.
//
// Returns:
//   - func(*Query): the query tweak
func speculative(attempts int, delay time.Duration) func(*Query) {
	return func(qry *Query) {
		qry.Idempotent(true).SetSpeculativeExecutionPolicy(&SimpleSpeculativeExecution{
			NumAttempts:  attempts,
			TimeoutDelay: delay,
		})
	}
}

// TestAwaitFill_SpeculativeNoHostCannotWin proves a speculative runner that found no host
// cannot beat a sibling that is waiting for a fill.
func TestAwaitFill_SpeculativeNoHostCannotWin(t *testing.T) {
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) { cluster.Timeout = 0 })
	pool := harness.pool(t, harness.hosts[0])

	stages := newRunStageRecorder()
	harness.session.executor.testRunHook = stages.hook

	harness.dialer.arm(nil)
	detachPoolConn(t, pool)

	waiting := make(chan struct{}, 1)
	harness.session.executor.testBeforeWait = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}

	result := harness.query(t.Context(), speculative(1, time.Millisecond))
	awaitSignal(t, waiting, "the first runner to await the fill")
	stages.await(t, runRetired, "the speculative runner to report no host")

	select {
	case err := <-result:
		t.Fatalf("the no-host report ended the query: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	harness.dialer.releaseAll()
	require.NoError(t, awaitQuery(t, result), "the waiting runner must still win")
}

// TestCoordinate_NoHostBetweenTicks proves no-host reports arriving between speculative
// launches never terminate the query while a launch is still scheduled.
func TestCoordinate_NoHostBetweenTicks(t *testing.T) {
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) { cluster.Timeout = 0 })
	pool := harness.pool(t, harness.hosts[0])

	stages := newRunStageRecorder()
	entered := stages.gate(runEntered)
	reported := stages.gate(runRetired)
	harness.session.executor.testRunHook = stages.hook

	harness.dialer.arm(nil)
	detachPoolConn(t, pool)

	waiting := make(chan struct{}, 1)
	harness.session.executor.testBeforeWait = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}

	result := harness.query(t.Context(), speculative(2, 50*time.Millisecond))

	// The main runner claims the only host and waits for its fill.
	stages.await(t, runEntered, "the main runner to start")
	entered <- struct{}{}
	awaitSignal(t, waiting, "the main runner to await the fill")

	// Each sibling finds no host; its report is released before the next one starts.
	for sibling := 1; sibling <= 2; sibling++ {
		stages.await(t, runEntered, "a speculative runner to start")
		entered <- struct{}{}
		stages.await(t, runRetired, "a speculative runner to report no host")
		reported <- struct{}{}
	}

	harness.dialer.releaseAll()
	require.NoError(t, awaitQuery(t, result), "the waiting runner must still win")
	require.Equal(t, 3, stages.count(runEntered), "one main runner and two speculative runners must have started")
	require.Equal(t, 2, stages.count(runRetired), "both speculative runners must have reported no host")
}

// TestCoordinate_NoHostEndsWithLaunchScheduled proves a query ends as soon as every
// launched runner reported no host, even while a speculative launch is still scheduled.
//
// The speculative delay is an hour, so a coordinator that waited for the next tick
// would outlive both Session.Timeout and Session.Close.
func TestCoordinate_NoHostEndsWithLaunchScheduled(t *testing.T) {
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) { cluster.Timeout = 0 })

	stages := newRunStageRecorder()
	reported := stages.gate(runRetired)
	harness.session.executor.testRunHook = stages.hook

	// Dropping the pool leaves the host selectable but unusable, so the runner
	// reaches no host at all instead of waiting for a fill.
	harness.session.pool.removeHost(harness.hosts[0])

	result := harness.query(t.Context(), speculative(1, time.Hour))
	stages.await(t, runRetired, "the main runner to report no host")

	start := time.Now()
	reported <- struct{}{}
	require.ErrorIs(t, awaitQuery(t, result), ErrNoConnections, "the only launched runner found no host")
	require.Less(t, time.Since(start), time.Second, "the query must not wait for the scheduled launch")
	require.Equal(t, 1, stages.count(runEntered), "the scheduled speculative runner must never start")
}

// TestCoordinate_NoHostReleasesQueryOnSessionClose proves the query goroutine of a
// runner that reached no host exits while the session is closing.
//
// The query deliberately runs on context.Background(), the default of Query and Batch:
// the coordinator selects only on the query context, so binding this query to
// t.Context() would let the test's own cancellation end it and would destroy what this
// test proves.
// A regression therefore surfaces as the awaitQuery budget expiring.
func TestCoordinate_NoHostReleasesQueryOnSessionClose(t *testing.T) {
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) { cluster.Timeout = 0 })

	stages := newRunStageRecorder()
	reported := stages.gate(runRetired)
	harness.session.executor.testRunHook = stages.hook

	harness.session.pool.removeHost(harness.hosts[0])

	result := harness.query(context.Background(), speculative(1, time.Hour))
	stages.await(t, runRetired, "the main runner to report no host")

	// Close cancels the session context, which the coordinator does not select on,
	// so only the no-host report itself can end this query.
	harness.session.Close()

	start := time.Now()
	reported <- struct{}{}
	require.ErrorIs(t, awaitQuery(t, result), ErrNoConnections, "the query goroutine must exit")
	require.Less(t, time.Since(start), time.Second, "Session.Close must not leave the query running")
}

// TestAwaitFill_TimeoutZeroUsesContext proves a zero Session.Timeout leaves the wait
// bounded by the caller's context alone.
func TestAwaitFill_TimeoutZeroUsesContext(t *testing.T) {
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) { cluster.Timeout = 0 })
	pool := harness.pool(t, harness.hosts[0])

	harness.dialer.arm(nil)
	detachPoolConn(t, pool)

	var waited atomic.Bool
	harness.session.executor.testBeforeWait = func() { waited.Store(true) }

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	require.ErrorIs(t, awaitQuery(t, harness.query(ctx, nil)), context.DeadlineExceeded,
		"the caller's deadline must end the wait")
	require.True(t, waited.Load(), "the query must have waited for the fill")
}

// TestAwaitFill_CallerDeadlineOverridesSessionTimeout proves a caller deadline wins over a
// shorter Session.Timeout, exactly as it does for a request on a connection.
func TestAwaitFill_CallerDeadlineOverridesSessionTimeout(t *testing.T) {
	harness := newFillHarness(t, 1, nil)
	pool := harness.pool(t, harness.hosts[0])

	// A Timeout this small cannot complete a startup handshake, so it is applied once
	// the session is up; only awaitFill reads it at runtime.
	harness.session.cfg.Timeout = time.Nanosecond

	harness.dialer.arm(nil)
	detachPoolConn(t, pool)

	waiting := make(chan struct{}, 1)
	harness.session.executor.testBeforeWait = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	result := harness.query(ctx, nil)
	awaitSignal(t, waiting, "the query to await the fill")

	harness.dialer.releaseAll()
	require.NoError(t, awaitQuery(t, result), "the caller's deadline must keep the wait alive")
}

// TestAwaitFill_ReturnsContextCanceled proves a cancelled query context surfaces as
// context.Canceled rather than ErrNoConnections.
func TestAwaitFill_ReturnsContextCanceled(t *testing.T) {
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) { cluster.Timeout = 0 })
	pool := harness.pool(t, harness.hosts[0])

	harness.dialer.arm(nil)
	detachPoolConn(t, pool)

	waiting := make(chan struct{}, 1)
	harness.session.executor.testBeforeWait = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	result := harness.query(ctx, nil)
	awaitSignal(t, waiting, "the query to await the fill")

	cancel()
	require.ErrorIs(t, awaitQuery(t, result), context.Canceled, "cancellation must surface as such")
}

// TestAwaitFill_TimeoutExpiresWithoutContext proves Session.Timeout bounds the wait when
// the caller supplied no deadline.
func TestAwaitFill_TimeoutExpiresWithoutContext(t *testing.T) {
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) { cluster.Timeout = 20 * time.Millisecond })
	pool := harness.pool(t, harness.hosts[0])

	harness.dialer.arm(nil)
	detachPoolConn(t, pool)

	require.ErrorIs(t, awaitQuery(t, harness.query(t.Context(), nil)), ErrNoConnections,
		"Session.Timeout must bound a wait with no caller deadline")
}

// awaitNoPendingFills blocks until the pool holds no fill claim any more.
//
// The wake generation is captured before the claim count is read, so a fillDone that
// lands between the two cannot be missed; a claim released twice leaves a negative
// count and is reported as a timeout.
// It answers "is this pool quiescent yet", not "was this claim released exactly once":
// a phase that must prove the latter counts poolFillDone events instead.
func awaitNoPendingFills(t testing.TB, parent *policyConnPool, pool *hostConnPool) {
	t.Helper()

	deadline := time.After(fillEventBudget)
	for {
		gen, parentClosed, _ := parent.snapshot(nil)
		require.False(t, parentClosed, "the session pool must still be open")

		if pending := pendingFills(pool); pending == 0 {
			return
		} else if pending < 0 {
			t.Fatalf("fillsPending went negative (%d): a claim was released more than once", pending)
		}

		select {
		case <-gen:
		case <-deadline:
			t.Fatalf("timed out after %v waiting for %d fill claims to be released",
				fillEventBudget, pendingFills(pool))
		}
	}
}

// awaitFillDone blocks until the pool published want claim releases past base,
// then proves those claims were released exactly once.
//
// Counting poolFillDone is what makes a delayed second release visible: a counter
// that merely passes through zero would satisfy a poll, while the event is emitted
// after every decrement and is the last thing an asynchronous fill branch does.
// The count, not the arrival channel, is the authority, because the fixture's own
// initial fills already left releases of their own behind; base is read once the
// fixture is quiescent, which newFillHarness guarantees by waiting for every pool
// to finish its first fill.
//
// Parameters:
//   - events: the recorder installed as the cluster's pool hook
//   - pool: the pool whose claims are counted
//   - base: events.count(poolFillDone) read before the observed phase started
//   - want: how many claims that phase must release
func awaitFillDone(t *testing.T, events *poolEventRecorder, pool *hostConnPool, base, want int) {
	t.Helper()

	arrived := events.channel(poolFillDone)
	deadline := time.After(fillEventBudget)
	for events.count(poolFillDone) < base+want {
		select {
		case <-arrived:
		case <-deadline:
			t.Fatalf("timed out after %v waiting for %d of %d fill claims to be released",
				fillEventBudget, base+want-events.count(poolFillDone), want)
		}
	}

	require.Equal(t, base+want, events.count(poolFillDone), "every claim must be released exactly once")
	require.Zero(t, pendingFills(pool), "the releases must leave the claim count at zero")
}

// TestPoolGeneration_CloseDuringFillOrders proves the wake generation survives a removal
// and a fill ending in either order, and is ended exactly once.
func TestPoolGeneration_CloseDuringFillOrders(t *testing.T) {
	require.NotPanics(t, func() { (&hostConnPool{}).notify() },
		"a zero-value pool must tolerate a notify")

	cases := []struct {
		name string
		run  func(parent *policyConnPool, pool *hostConnPool, host *HostInfo)
	}{
		{
			name: "append wake before removal",
			run: func(parent *policyConnPool, pool *hostConnPool, host *HostInfo) {
				pool.notify()
				parent.removeHost(host)
				pool.fillDone()
			},
		},
		{
			name: "removal before the fill ends",
			run: func(parent *policyConnPool, pool *hostConnPool, host *HostInfo) {
				parent.removeHost(host)
				pool.notify()
				pool.fillDone()
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			harness := newFillHarness(t, 1, nil)
			host := harness.hosts[0]
			pool := harness.pool(t, host)
			parent := harness.session.pool

			require.True(t, pool.claimFill(), "the pool must accept a fill claim")
			gen, parentClosed, _ := parent.snapshot(nil)
			require.False(t, parentClosed)

			require.NotPanics(t, func() { tc.run(parent, pool, host) },
				"the captured generation must be ended exactly once")

			awaitSignal(t, gen, "the captured generation to be ended")
			require.Zero(t, pendingFills(pool), "the claim must be released exactly once")
		})
	}
}

// parkAsyncFill parks fill's asynchronous branch at its first checkpoint.
//
// The branch publishes its arrival, waits to be released and then runs then, so a
// test can inspect the claim of a handed-off fill while neither owner can touch it.
//
// Parameters:
//   - events: the recorder installed as the cluster's pool hook
//   - then: extra work run inside the released branch; nil to let it continue
//
// Returns:
//   - <-chan struct{}: closed once the branch parked
//   - chan struct{}: close it to release the branch
func parkAsyncFill(events *poolEventRecorder, then func()) (<-chan struct{}, chan struct{}) {
	started := make(chan struct{})
	resume := make(chan struct{})

	var once sync.Once
	events.on(poolFillAsyncStart, func(*HostInfo) {
		once.Do(func() { close(started) })
		<-resume
		if then != nil {
			then()
		}
	})

	return started, resume
}

// TestFillHarness_BaselineWaitsForInitialFillDone proves the fixture cannot hand a test a
// poolFillDone baseline that is still missing one of the fixture's own initial claims.
//
// fillDone decrements fillsPending, reaches the poolFillDone checkpoint and only then
// notifies the parent, so a fixture that calls a pool quiescent on a zero claim count alone
// can return while its own release is still on its way to the recorder.
// A test sampling the baseline there would later credit that stray release to the claim it
// is measuring, and stop waiting one event too early.
// Parking the checkpoint between the decrement and the recorder pins exactly that window
// open, which is what makes this test deterministic.
func TestFillHarness_BaselineWaitsForInitialFillDone(t *testing.T) {
	parked := make(chan struct{})
	release := make(chan struct{})
	var parkOnce, releaseOnce sync.Once
	releaseParked := func() { releaseOnce.Do(func() { close(release) }) }

	// The fixture registers session.Close with t.Cleanup, and cleanups run after this
	// defer, so a failure below still frees the parked driver goroutine before teardown.
	defer releaseParked()

	// baseline carries events.count(poolFillDone) as read the instant the fixture returned.
	baseline := make(chan int, 1)
	go func() {
		// Sent from a defer: an assertion failing inside the fixture ends this goroutine
		// through runtime.Goexit, and the test must report that instead of blocking.
		count := -1
		defer func() { baseline <- count }()

		harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
			forward := cluster.testPoolHook
			cluster.testPoolHook = func(ev poolEvent, host *HostInfo) {
				first := false
				if ev == poolFillDone {
					parkOnce.Do(func() { first = true })
				}
				if first {
					close(parked)
					<-release
				}
				forward(ev, host)
			}
		})
		count = harness.events.count(poolFillDone)
	}()

	awaitSignal(t, parked, "the initial fill claim to be released")

	// The claim is gone by now, which is the entire state a claim-count-only fixture
	// waited for.
	// An absence has no barrier to wait on, hence the bounded window.
	select {
	case count := <-baseline:
		t.Fatalf("the fixture exposed a baseline of %d while its initial poolFillDone event was still parked", count)
	case <-time.After(fillAbsenceWindow):
	}

	releaseParked()

	select {
	case count := <-baseline:
		require.Equal(t, 1, count, "the fixture's baseline must already count the initial claim release")
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for the fixture to become ready", fillEventBudget)
	}
}

// TestRunFill_EveryExitPathDecrementsOnce proves every way out of a fill cycle releases
// its claim exactly once, so a waiter never observes a pool as pending forever.
//
// Each case starts from exactly one claim, so a missing release leaves the count at one
// and a double release leaves it negative; only an exact release reaches zero.
// The cases that hand the claim to the asynchronous branch count poolFillDone events
// instead of reading the count alone: a count that passed through zero and went
// negative later would satisfy a plain read taken too early.
func TestRunFill_EveryExitPathDecrementsOnce(t *testing.T) {
	t.Run("another fill already admitted", func(t *testing.T) {
		pool := &hostConnPool{size: 1, logger: newTestLogger(LogLevelDebug)}
		pool.mu.Lock()
		pool.filling = true
		pool.mu.Unlock()

		require.True(t, pool.claimFill())
		pool.runFill()
		require.Zero(t, pendingFills(pool), "the admission check must release its claim")
	})

	t.Run("pool already full", func(t *testing.T) {
		pool := &hostConnPool{size: 1, conns: []*Conn{{}}, logger: newTestLogger(LogLevelDebug)}

		require.True(t, pool.claimFill())
		pool.runFill()
		require.Zero(t, pendingFills(pool), "a full pool must release its claim")
	})

	t.Run("synchronous failure", func(t *testing.T) {
		harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
			cluster.ConvictionPolicy = &recordingConvictionPolicy{onFailure: func(*HostInfo) bool { return false }}
		})
		pool := harness.pool(t, harness.hosts[0])

		harness.dialer.arm(func(string) error { return errFillTestDialRefused })
		harness.dialer.releaseAll()
		detachPoolConn(t, pool)

		require.True(t, pool.claimFill())
		pool.runFill()
		require.Zero(t, pendingFills(pool), "a failed cycle must release its claim")
	})

	t.Run("synchronous panic", func(t *testing.T) {
		harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
			cluster.ConvictionPolicy = &recordingConvictionPolicy{onFailure: func(*HostInfo) bool { return false }}
		})
		pool := harness.pool(t, harness.hosts[0])

		detachPoolConn(t, pool)
		harness.events.on(poolConnectAttempt, func(*HostInfo) {
			panic("gocql: connect panic injected by the fill test")
		})

		require.True(t, pool.claimFill())
		require.NotPanics(t, pool.runFill, "the fill guard must swallow the panic")
		require.Zero(t, pendingFills(pool), "a panicking cycle must release its claim")
	})

	t.Run("handoff with success", func(t *testing.T) {
		harness := newFillHarness(t, 1, nil)
		pool := harness.pool(t, harness.hosts[0])

		// Parking the asynchronous branch at its first checkpoint pins the moment
		// after runFill returned and before the new owner can release anything,
		// which is where a runFill that released a handed-off claim shows up.
		asyncStarted, resumeAsync := parkAsyncFill(harness.events, nil)

		detachPoolConn(t, pool)
		base := harness.events.count(poolFillDone)

		require.True(t, pool.claimFill())
		pool.runFill()

		awaitSignal(t, asyncStarted, "the asynchronous fill branch to start")
		require.Equal(t, 1, pendingFills(pool), "runFill must leave the handed-off claim to the asynchronous branch")
		require.Equal(t, base, harness.events.count(poolFillDone), "runFill must not release a handed-off claim")

		close(resumeAsync)
		awaitFillDone(t, harness.events, pool, base, 1)
	})

	t.Run("asynchronous panic", func(t *testing.T) {
		harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
			cluster.ConvictionPolicy = &recordingConvictionPolicy{onFailure: func(*HostInfo) bool { return false }}
		})
		pool := harness.pool(t, harness.hosts[0])

		asyncStarted, resumeAsync := parkAsyncFill(harness.events, func() {
			panic("gocql: async fill panic injected by the fill test")
		})

		detachPoolConn(t, pool)
		base := harness.events.count(poolFillDone)

		require.True(t, pool.claimFill())
		pool.runFill()

		awaitSignal(t, asyncStarted, "the asynchronous fill branch to start")
		require.Equal(t, 1, pendingFills(pool), "runFill must leave the handed-off claim to the asynchronous branch")
		require.Equal(t, base, harness.events.count(poolFillDone), "runFill must not release a handed-off claim")

		close(resumeAsync)
		awaitFillDone(t, harness.events, pool, base, 1)
	})

	t.Run("double check exit", func(t *testing.T) {
		harness := newFillHarness(t, 1, nil)
		pool := harness.pool(t, harness.hosts[0])

		parked := make(chan struct{})
		resume := make(chan struct{})
		var once sync.Once
		harness.events.on(poolFillAdmission, func(*HostInfo) {
			first := false
			once.Do(func() { first = true })
			if !first {
				return
			}
			close(parked)
			<-resume
		})

		harness.dialer.arm(nil)
		detachPoolConn(t, pool)
		base := harness.events.count(poolFillDone)

		// The first runner parks in the read-to-write transition; the second wins
		// admission and blocks in the dialer with the fill claim it published.
		require.Nil(t, pool.Pick(), "the emptied pool must not serve a connection")
		awaitSignal(t, parked, "the first runner to park before the double check")
		require.Nil(t, pool.Pick(), "the emptied pool must not serve a connection")
		harness.dialer.awaitStarted(t)

		close(resume)
		harness.dialer.releaseAll()
		awaitFillDone(t, harness.events, pool, base, 2)
	})
}
