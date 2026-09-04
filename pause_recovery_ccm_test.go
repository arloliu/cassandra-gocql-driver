//go:build ccm
// +build ccm

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
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/internal/ccm"
	"github.com/stretchr/testify/require"
)

const (
	// pauseNumConns is the size of every pool of the pause/resume session.
	pauseNumConns = 2

	// pauseConnTimeout bounds one connection setup and one request.
	pauseConnTimeout = 2 * time.Second

	// pauseHeartbeatInterval is the start-to-start heartbeat spacing of the pause/resume session.
	// Together with the heartbeat timeout floor (5s), the six-failure threshold and a zero phase,
	// a connection to a paused node is closed after 0 + 6*5s + 5*max(0, 1s-5s) = 30s,
	// and both connections of a pool drain within one interval of each other.
	pauseHeartbeatInterval = time.Second

	// pauseDrainBudget bounds the wait for that 30s drain.
	pauseDrainBudget = 75 * time.Second

	// pauseEventBudget bounds every gate that does not wait for a drain.
	pauseEventBudget = 30 * time.Second
)

// pauseHostStateListener publishes host state changes on buffered channels so a
// test gates on them instead of polling.
//
// The driver calls the listener inline, so the sends never block.
type pauseHostStateListener struct {
	up   chan *HostInfo
	down chan *HostInfo
}

// newPauseHostStateListener returns a listener ready to be installed as
// MetadataConfig.HostListener.HostStateChangeListener.
//
// Returns:
//   - *pauseHostStateListener: listener with empty event channels
func newPauseHostStateListener() *pauseHostStateListener {
	return &pauseHostStateListener{
		up:   make(chan *HostInfo, 64),
		down: make(chan *HostInfo, 64),
	}
}

func (l *pauseHostStateListener) OnHostUp(event HostUpEvent) {
	select {
	case l.up <- event.Host:
	default:
	}
}

func (l *pauseHostStateListener) OnHostDown(event HostDownEvent) {
	select {
	case l.down <- event.Host:
	default:
	}
}

// awaitHostEvent blocks until ch reports hostID, discarding other hosts.
//
// Parameters:
//   - ch: one of the listener's event channels
//   - hostID: the host the test waits for
//   - budget: how long to wait before failing the test
//   - what: what the wait is for, used in the failure message
func awaitHostEvent(t *testing.T, ch <-chan *HostInfo, hostID string, budget time.Duration, what string) {
	t.Helper()

	deadline := time.After(budget)
	for {
		select {
		case host := <-ch:
			if host.HostID() == hostID {
				return
			}
		case <-deadline:
			t.Fatalf("timed out after %v waiting for %s", budget, what)
			return
		}
	}
}

// hostEventSeen reports whether ch already holds an event for hostID.
//
// It never blocks and consumes everything the channel holds.
//
// Returns:
//   - bool: true when an event for hostID was buffered
func hostEventSeen(ch <-chan *HostInfo, hostID string) bool {
	for {
		select {
		case host := <-ch:
			if host.HostID() == hostID {
				return true
			}
		default:
			return false
		}
	}
}

// poolErrorLogger forwards every line to an inner logger and publishes the
// address of each pool connection error, so a test can gate on a pool draining.
type poolErrorLogger struct {
	StructuredLogger
	errs chan string
}

// newPoolErrorLogger wraps inner with a pool connection error gate.
//
// Parameters:
//   - inner: the logger every line is forwarded to
//
// Returns:
//   - *poolErrorLogger: logger ready to be installed as ClusterConfig.Logger
func newPoolErrorLogger(inner StructuredLogger) *poolErrorLogger {
	return &poolErrorLogger{StructuredLogger: inner, errs: make(chan string, 64)}
}

func (l *poolErrorLogger) Info(msg string, fields ...LogField) {
	l.StructuredLogger.Info(msg, fields...)

	if !strings.Contains(msg, "Pool connection error") {
		return
	}
	for _, field := range fields {
		if field.Name != "addr" {
			continue
		}
		select {
		case l.errs <- field.Value.String():
		default:
		}
	}
}

// awaitPoolDrain blocks until count pool connections of ip reported an error.
//
// Parameters:
//   - ip: the connect address of the host whose pool is draining
//   - count: how many connections the pool held
//   - budget: how long to wait before failing the test
func (l *poolErrorLogger) awaitPoolDrain(t *testing.T, ip string, count int, budget time.Duration) {
	t.Helper()

	deadline := time.After(budget)
	seen := 0
	for seen < count {
		select {
		case addr := <-l.errs:
			if host, _, err := net.SplitHostPort(addr); err == nil && host == ip {
				seen++
			}
		case <-deadline:
			t.Fatalf("timed out after %v waiting for %d pool connection errors on %s, saw %d",
				budget, count, ip, seen)
			return
		}
	}
}

// pausePoolHook observes the fill checkpoints of ClusterConfig.testPoolHook.
//
// It counts appended connections per host, so a test waits for a pool to be full
// before pausing its node, and it parks the first connect attempt of one armed
// host, so the test decides when a fill cycle reaches the network.
type pausePoolHook struct {
	mu       sync.Mutex
	appended map[string]chan struct{}
	armed    string
	released bool
	reached  chan struct{}
	release  chan struct{}
}

// newPausePoolHook returns a hook with nothing armed.
//
// Returns:
//   - *pausePoolHook: hook whose hook method is installed as testPoolHook
func newPausePoolHook() *pausePoolHook {
	return &pausePoolHook{
		appended: map[string]chan struct{}{},
		reached:  make(chan struct{}),
		release:  make(chan struct{}),
	}
}

// hook records an append and parks the armed host's next connect attempt.
func (h *pausePoolHook) hook(ev poolEvent, host *HostInfo) {
	switch ev {
	case poolConnAppended:
		h.mu.Lock()
		ch := h.appendedLocked(host.HostID())
		h.mu.Unlock()

		select {
		case ch <- struct{}{}:
		default:
		}
	case poolConnectAttempt:
		h.mu.Lock()
		park := h.armed != "" && h.armed == host.HostID()
		if park {
			// Only the first attempt of the armed host is parked; a later one
			// belongs to the fill cycle the test released.
			h.armed = ""
			close(h.reached)
		}
		h.mu.Unlock()

		if park {
			<-h.release
		}
	default:
	}
}

// appendedLocked returns the append channel of hostID, creating it on first use.
//
// The caller must hold h.mu.
func (h *pausePoolHook) appendedLocked(hostID string) chan struct{} {
	ch, ok := h.appended[hostID]
	if !ok {
		ch = make(chan struct{}, 64)
		h.appended[hostID] = ch
	}
	return ch
}

// arm parks the next connect attempt of hostID until releaseAll.
func (h *pausePoolHook) arm(hostID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.armed = hostID
}

// awaitAppended blocks until the pool of hostID appended count connections.
func (h *pausePoolHook) awaitAppended(t *testing.T, hostID string, count int, budget time.Duration) {
	t.Helper()

	h.mu.Lock()
	ch := h.appendedLocked(hostID)
	h.mu.Unlock()

	deadline := time.After(budget)
	for i := 0; i < count; i++ {
		select {
		case <-ch:
		case <-deadline:
			t.Fatalf("timed out after %v waiting for %d connections of host %s, saw %d",
				budget, count, hostID, i)
			return
		}
	}
}

// awaitParked blocks until the armed host's connect attempt is parked.
func (h *pausePoolHook) awaitParked(t *testing.T, budget time.Duration, what string) {
	t.Helper()

	select {
	case <-h.reached:
	case <-time.After(budget):
		t.Fatalf("timed out after %v waiting for %s", budget, what)
	}
}

// releaseAll lets every parked connect attempt continue.
// It is idempotent, so a cleanup can release a test that already did.
func (h *pausePoolHook) releaseAll() {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.released {
		return
	}
	h.released = true
	close(h.release)
}

// pauseRecoveryFixture is one session against the ccm cluster, wired to the gates
// both pause phases need.
type pauseRecoveryFixture struct {
	session  *Session
	hook     *pausePoolHook
	logs     *poolErrorLogger
	listener *pauseHostStateListener
	// nodes maps a host's connect address to its ccm node name.
	nodes map[string]string
	// waiting fires whenever a query is about to block on a fill.
	waiting chan struct{}
}

// newPauseRecoveryFixture connects a session whose pools drain within 30s of a
// node being paused.
//
// Returns:
//   - *pauseRecoveryFixture: fixture whose session is closed by t.Cleanup
func newPauseRecoveryFixture(t *testing.T, clusterInfo *ccm.ClusterInfo) *pauseRecoveryFixture {
	t.Helper()

	nodes := make(map[string]string, len(clusterInfo.Hosts))
	for _, node := range clusterInfo.Hosts {
		nodes[node.Addr] = node.Name
	}

	listener := newPauseHostStateListener()
	logs := newPoolErrorLogger(NewLogger(LogLevelInfo))
	hook := newPausePoolHook()

	cluster := createCluster(func(cfg *ClusterConfig) {
		cfg.Hosts = clusterInfo.HostAddrs()
		cfg.NumConns = pauseNumConns
		cfg.Timeout = pauseConnTimeout
		cfg.ConnectTimeout = pauseConnTimeout
		cfg.ReconnectInterval = 500 * time.Millisecond
		cfg.ReconnectionPolicy = &ConstantReconnectionPolicy{MaxRetries: 1, Interval: 100 * time.Millisecond}
		cfg.heartbeatInterval = pauseHeartbeatInterval
		cfg.heartbeatPhase = func(time.Duration) time.Duration { return 0 }
		// Client-side detection owns both phases.
		// A peer convicts a paused node well inside the drain,
		// and its STATUS_CHANGE DOWN would remove the pool before the pre-conviction window opens,
		// while a STATUS_CHANGE UP would hide the recovery reconnectDownedHosts is asserted to drive.
		cfg.Events.DisableNodeStatusEvents = true
		cfg.Metadata.HostListener.HostStateChangeListener = listener
		cfg.Logger = logs
		cfg.testPoolHook = hook.hook
	})

	session, err := cluster.CreateSession()
	require.NoError(t, err, "CreateSession")
	t.Cleanup(session.Close)
	// Registered after session.Close so it runs before it: a parked connect
	// attempt has to be released before the session tears its pools down.
	t.Cleanup(hook.releaseAll)

	fixture := &pauseRecoveryFixture{
		session:  session,
		hook:     hook,
		logs:     logs,
		listener: listener,
		nodes:    nodes,
		waiting:  make(chan struct{}, 8),
	}
	session.executor.testBeforeWait = func() {
		select {
		case fixture.waiting <- struct{}{}:
		default:
		}
	}

	return fixture
}

// node returns the ccm node name backing host.
//
// Returns:
//   - string: the node name, e.g. "node2"
func (f *pauseRecoveryFixture) node(t *testing.T, host *HostInfo) string {
	t.Helper()

	node := f.nodes[host.ConnectAddress().String()]
	require.NotEmpty(t, node, "no ccm node for host %s", host.ConnectAddress())
	return node
}

// pause suspends the node backing host and registers its resume, so a failing
// test cannot leave a stopped node behind.
func (f *pauseRecoveryFixture) pause(t *testing.T, host *HostInfo) {
	t.Helper()

	node := f.node(t, host)
	t.Cleanup(func() { _ = ccm.Resume(node) })
	t.Logf("pausing %s (%s)", node, host.ConnectAddress())
	require.NoError(t, ccm.Pause(node), "pause %s", node)
}

// resume continues the node backing host.
func (f *pauseRecoveryFixture) resume(t *testing.T, host *HostInfo) {
	t.Helper()

	node := f.node(t, host)
	t.Logf("resuming %s (%s)", node, host.ConnectAddress())
	require.NoError(t, ccm.Resume(node), "resume %s", node)
}

// query runs a read served by the host itself.
//
// Returns:
//   - error: whatever the driver reports, ErrNoConnections included
func (f *pauseRecoveryFixture) query(hostID string) error {
	return f.session.Query("SELECT key FROM system.local").
		Consistency(One).
		SetHostID(hostID).
		Exec()
}

// TestPauseResumeRecovery covers a paused (SIGSTOPped) node on both sides of the
// conviction boundary, on one session.
//
// Before conviction: a query for a host whose pool has just drained waits for the
// fill cycle already in flight instead of failing with ErrNoConnections without
// touching the network.
//
// After conviction: a fill cycle that fails on an empty pool marks the host DOWN,
// and reconnectDownedHosts brings it back once the node resumes.
// That phase uses the control connection's own host,
// so it also covers a control connection that has to move while its host is being convicted.
func TestPauseResumeRecovery(t *testing.T) {
	require.NoError(t, ccm.AllUp())

	clusterInfo, err := ccm.CurrentClusterInfo()
	require.NoError(t, err)
	if len(clusterInfo.Hosts) < 2 {
		t.Skip("this test requires at least 2 nodes")
	}

	fixture := newPauseRecoveryFixture(t, clusterInfo)

	controlHost := fixture.session.control.getConn().host
	require.NotNil(t, controlHost, "the session must hold a control connection")

	var target *HostInfo
	for _, host := range fixture.session.ring.allHosts() {
		if host.HostID() != controlHost.HostID() {
			target = host
			break
		}
	}
	require.NotNil(t, target, "expected a ring host that does not carry the control connection")

	if !t.Run("QueryWaitsForTheFillOfAPausedHost", func(t *testing.T) {
		pauseRecoveryBeforeConviction(t, fixture, target)
	}) {
		return
	}

	t.Run("ConvictedControlHostRecovers", func(t *testing.T) {
		// The control connection may have moved while the first phase ran.
		pauseRecoveryAfterConviction(t, fixture, fixture.session.control.getConn().host)
	})
}

// pauseRecoveryBeforeConviction pauses a host, lets its pool drain, and parks the
// fill cycle the drain triggers before it reaches the network.
//
// The host is still UP with an empty pool and a claimed fill: a query for it must wait for that cycle.
// The node is resumed before the query starts,
// so only the parked attempt keeps the pool empty and the release decides when the query succeeds.
func pauseRecoveryBeforeConviction(t *testing.T, f *pauseRecoveryFixture, target *HostInfo) {
	// A pool that is not full yet would drain in two waves, so wait for both
	// connections before pausing.
	f.hook.awaitAppended(t, target.HostID(), pauseNumConns, pauseEventBudget)

	f.hook.arm(target.HostID())
	f.pause(t, target)

	f.logs.awaitPoolDrain(t, target.ConnectAddress().String(), pauseNumConns, pauseDrainBudget)
	f.hook.awaitParked(t, pauseDrainBudget, "the fill cycle of the paused host to reach its connect attempt")

	f.resume(t, target)

	result := make(chan error, 1)
	go func() { result <- f.query(target.HostID()) }()

	select {
	case <-f.waiting:
	case err := <-result:
		t.Fatalf("the query finished before waiting for the in-flight fill: %v", err)
	case <-time.After(pauseEventBudget):
		t.Fatalf("timed out after %v waiting for the query to await the fill", pauseEventBudget)
	}

	f.hook.releaseAll()

	select {
	case err := <-result:
		require.NoError(t, err, "the query must succeed once the parked fill lands")
	case <-time.After(pauseEventBudget):
		t.Fatalf("timed out after %v waiting for the query to finish", pauseEventBudget)
	}

	require.False(t, hostEventSeen(f.listener.down, target.HostID()),
		"the host must not be convicted while its fill cycle is still in flight")
}

// pauseRecoveryAfterConviction pauses a host and lets the fill cycle its drain
// triggers run into the paused node.
//
// The cycle fails on an empty pool, so the host is marked DOWN, and once the node
// resumes, reconnectDownedHosts is the only thing that can bring it back: server
// status events are disabled for this session.
func pauseRecoveryAfterConviction(t *testing.T, f *pauseRecoveryFixture, host *HostInfo) {
	f.hook.awaitAppended(t, host.HostID(), pauseNumConns, pauseEventBudget)

	f.pause(t, host)

	f.logs.awaitPoolDrain(t, host.ConnectAddress().String(), pauseNumConns, pauseDrainBudget)
	awaitHostEvent(t, f.listener.down, host.HostID(), pauseEventBudget,
		"the paused host to be marked DOWN by its failing fill cycle")

	f.resume(t, host)

	awaitHostEvent(t, f.listener.up, host.HostID(), pauseEventBudget,
		"the resumed host to be marked UP by reconnectDownedHosts")
	require.NoError(t, f.query(host.HostID()), "the recovered host must serve queries again")
}
