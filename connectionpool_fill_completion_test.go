//go:build all || unit

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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The three messages a fill cycle's completion path hands to the application logger.
// Each is a seam a misbehaving logger can panic at,
// and each panic used to cost something different.
const (
	fillWarnMsg  = "Connection pool filling failed."
	fillDebugMsg = "Logging number of connections of pool after filling stopped."
	hostDownMsg  = "Node is DOWN."
)

// msgPanicLogger panics the first budget times it is handed a chosen message,
// the way an application logger with a bug of its own would.
//
// Only the chosen message panics: the completion path logs several times and a
// logger that panicked at all of them would not say which seam a test was
// exercising.
type msgPanicLogger struct {
	StructuredLogger

	target string
	mu     sync.Mutex
	armed  bool
	budget int
	fired  int
}

var _ StructuredLogger = (*msgPanicLogger)(nil)

// newMsgPanicLogger returns a disarmed logger that will panic on the first budget
// calls carrying target once arm is called.
//
// It starts disarmed because the fixture's own initial fill runs the same
// completion path the tests are aiming at,
// and a budget spent there would leave the cycle under test with a logger that
// no longer panics - a test that passes for the wrong reason.
//
// Returns:
//   - *msgPanicLogger: logger ready to install as ClusterConfig.Logger
func newMsgPanicLogger(target string, budget int) *msgPanicLogger {
	return &msgPanicLogger{StructuredLogger: newTestLogger(LogLevelDebug), target: target, budget: budget}
}

// arm lets the logger start panicking. Call it once the fixture is built.
func (l *msgPanicLogger) arm() {
	l.mu.Lock()
	l.armed = true
	l.mu.Unlock()
}

// maybePanic panics when msg is the chosen one and the budget is not spent.
func (l *msgPanicLogger) maybePanic(msg string) {
	l.mu.Lock()
	spend := l.armed && msg == l.target && l.fired < l.budget
	if spend {
		l.fired++
	}
	l.mu.Unlock()

	if spend {
		panic("gocql: test induced logger panic on " + msg)
	}
}

// fires reports how many panics the logger has raised.
//
// Returns:
//   - int: the number of panics raised so far
func (l *msgPanicLogger) fires() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.fired
}

func (l *msgPanicLogger) Debug(msg string, fields ...LogField) {
	l.maybePanic(msg)
	l.StructuredLogger.Debug(msg, fields...)
}

func (l *msgPanicLogger) Info(msg string, fields ...LogField) {
	l.maybePanic(msg)
	l.StructuredLogger.Info(msg, fields...)
}

func (l *msgPanicLogger) Warning(msg string, fields ...LogField) {
	l.maybePanic(msg)
	l.StructuredLogger.Warning(msg, fields...)
}

// awaitClaimReleases blocks until want further claim releases have been recorded
// past base.
//
// awaitFillDone is the usual barrier, but it also requires the pool to end with no
// claim outstanding; these tests deliberately leave a successor parked in the
// dialer holding one.
func awaitClaimReleases(t *testing.T, events *poolEventRecorder, base, want int) {
	t.Helper()

	arrived := events.channel(poolFillDone)
	deadline := time.After(fillEventBudget)
	for events.count(poolFillDone) < base+want {
		select {
		case <-arrived:
		case <-deadline:
			t.Fatalf("timed out after %v waiting for %d claim releases, saw %d",
				fillEventBudget, want, events.count(poolFillDone)-base)
		}
	}
}

// poolGate reads the pool's gate and the generation holding it.
//
// Returns:
//   - bool: whether a cycle holds the gate
//   - uint64: the generation of the cycle that took it last
func poolGate(pool *hostConnPool) (bool, uint64) {
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	return pool.filling, pool.fillGen
}

// awaitGate blocks until the pool's gate reaches want, failing the test otherwise.
//
// Returns:
//   - uint64: the generation observed once the gate reached want
func awaitGate(t *testing.T, pool *hostConnPool, want bool, what string) uint64 {
	t.Helper()

	deadline := time.After(fillEventBudget)
	for {
		if held, gen := poolGate(pool); held == want {
			return gen
		}
		select {
		case <-time.After(time.Millisecond):
		case <-deadline:
			held, gen := poolGate(pool)
			t.Fatalf("timed out after %v waiting for %s (filling=%v gen=%d)",
				fillEventBudget, what, held, gen)
			return 0
		}
	}
}

// startGatedCycle empties the pool and starts one fill cycle that parks in the
// gated dialer, so the caller owns a cycle it can finish on demand.
//
// Returns:
//   - uint64: the generation that cycle was admitted with
func startGatedCycle(t *testing.T, harness *fillHarness, pool *hostConnPool) uint64 {
	t.Helper()

	harness.dialer.arm(nil)
	detached := detachPoolConn(t, pool)
	t.Cleanup(func() { detached.Close() })

	require.True(t, pool.claimFill(), "the fixture must be able to claim a fill")
	go pool.runFill()
	harness.dialer.awaitStarted(t)

	return awaitGate(t, pool, true, "the cycle to take the gate")
}

// TestFillingStopped_ReEntryCannotCompleteAnotherCycle proves a completion that
// arrives after its own cycle finished leaves the gate alone.
//
// The tail of fillingStopped runs application code - the diagnostic, the
// conviction policy, the DOWN notification - and a panic there sends fill's
// recover back into fillingStopped.
// By then the gate may already belong to a newer cycle.
// A completion that only asked "is anyone filling?" would clear that cycle's gate
// and let a third runner in beside it;
// the generation the cycle was admitted with is what makes the second call inert.
func TestFillingStopped_ReEntryCannotCompleteAnotherCycle(t *testing.T) {
	conviction := &recordingConvictionPolicy{}
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.ConvictionPolicy = conviction
	})
	pool := harness.pool(t, harness.hosts[0])

	firstGen := startGatedCycle(t, harness, pool)

	// The successor is started from inside the first cycle's conviction callback:
	// at that point the first cycle has already released the gate, so the successor
	// can take it, and the panic that follows is the late completion under test.
	var secondGen uint64
	conviction.onFailure = func(*HostInfo) bool {
		held, _ := poolGate(pool)
		require.False(t, held, "the failing cycle must release the gate before its tail runs")

		pool.scheduleFill()
		secondGen = awaitGate(t, pool, true, "the successor to take the gate")
		require.Greater(t, secondGen, firstGen, "the successor must be a later generation")

		panic("gocql: test induced conviction panic")
	}

	// The first cycle's claim release is the barrier:
	// it is the last thing that cycle does,
	// so the gate read below is taken after the late completion has had its chance.
	// A poll for "the successor still holds the gate" without it would be satisfied
	// the moment the successor took it, before the panic had even unwound.
	base := harness.events.count(poolFillDone)
	harness.dialer.setErr(func(string) error { return errFillTestDialRefused })
	harness.dialer.releaseOne()
	awaitClaimReleases(t, harness.events, base, 1)

	// The successor is parked in the dialer, so its gate must still be the one held.
	held, gen := poolGate(pool)
	require.True(t, held, "the successor's gate must survive the first cycle's panic")
	require.Equal(t, secondGen, gen, "no later cycle may have been admitted beside it")

	require.Len(t, conviction.recorded(), 1, "the panicking policy must not be consulted twice")
}

// TestFillingStopped_WarningPanicStillReleasesTheGate proves a logger that panics
// on every filling-failed warning cannot strand the pool.
//
// That warning is handed to the application before the gate is released.
// A logger that panics there sends the recovery handler back in,
// and one that panics again leaves filling set for ever
// while the claim is released anyway:
// every waiter then reads the pool as empty and idle,
// and every later runner is refused at a gate nobody owns.
func TestFillingStopped_WarningPanicStillReleasesTheGate(t *testing.T) {
	logger := newMsgPanicLogger(fillWarnMsg, 8)
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.Logger = logger
	})
	pool := harness.pool(t, harness.hosts[0])
	logger.arm()

	startGatedCycle(t, harness, pool)

	harness.dialer.setErr(func(string) error { return errFillTestDialRefused })
	harness.dialer.releaseOne()

	awaitGate(t, pool, false, "the gate to be released despite the panicking logger")
	require.NotZero(t, logger.fires(), "the fixture must have made the logger panic")
	awaitNoPendingFills(t, harness.session.pool, pool)
}

// TestFillingStopped_DiagnosticPanicStillConvicts proves the tail diagnostic
// cannot keep a failed cycle from reaching the conviction policy.
//
// The diagnostic sits between the gate release and the policy call, so a panic
// there used to end the cycle with the pool empty, the host still UP
// and the policy never consulted.
// The policy must also see the cycle's own error,
// not the panic the driver recovered on the way out.
func TestFillingStopped_DiagnosticPanicStillConvicts(t *testing.T) {
	conviction := &recordingConvictionPolicy{}
	logger := newMsgPanicLogger(fillDebugMsg, 1)
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.ConvictionPolicy = conviction
		cluster.Logger = logger
	})
	host := harness.hosts[0]
	pool := harness.pool(t, host)
	logger.arm()

	startGatedCycle(t, harness, pool)

	harness.dialer.setErr(func(string) error { return errFillTestDialRefused })
	harness.dialer.releaseOne()

	awaitHost(t, harness.collector.down, host, "the host to be convicted after the failed cycle")
	require.Equal(t, 1, logger.fires(), "the fixture must have made the diagnostic panic once")
	require.Equal(t, []*HostInfo{host}, conviction.recorded(), "the failed cycle must consult the policy")
	require.Equal(t, []error{errFillTestDialRefused}, conviction.recordedErrs(),
		"the policy must see the cycle's own failure, not a recovered panic")
}

// TestFillingStopped_DownstreamPanicDoesNotReconsultThePolicy proves a panic
// after the policy approved is not answered by consulting it again.
//
// handleHostDown logs before it changes any state,
// so a logger that panics there leaves the host UP.
// The completion has already made its attempt,
// and one cycle must spend exactly one policy decision:
// a stateful policy that would have approved on a second call must not get one.
//
// This pins the attempt-once property, not the generation token -
// no successor holds the gate in this schedule,
// so a completion that only asked "is anyone filling?" would be inert here too.
// TestFillingStopped_ReEntryCannotCompleteAnotherCycle is what separates the two.
func TestFillingStopped_DownstreamPanicDoesNotReconsultThePolicy(t *testing.T) {
	conviction := &recordingConvictionPolicy{}
	logger := newMsgPanicLogger(hostDownMsg, 1)
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.ConvictionPolicy = conviction
		cluster.Logger = logger
	})
	host := harness.hosts[0]
	pool := harness.pool(t, host)
	logger.arm()

	startGatedCycle(t, harness, pool)

	harness.dialer.setErr(func(string) error { return errFillTestDialRefused })
	harness.dialer.releaseOne()

	awaitGate(t, pool, false, "the failed cycle to release the gate")
	awaitNoPendingFills(t, harness.session.pool, pool)

	require.Equal(t, 1, logger.fires(), "the fixture must have made the DOWN notification panic once")
	require.Len(t, conviction.recorded(), 1, "one cycle must spend exactly one policy decision")
	require.Equal(t, NodeUp, host.State(),
		"a DOWN notification that panicked before it changed state leaves the host as it was")
}

// poolRefillPending reads the obligation a removal left behind.
//
// Returns:
//   - bool: whether a removal is waiting for the cycle holding the gate to end
func poolRefillPending(pool *hostConnPool) bool {
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	return pool.refillPending
}

// awaitDeferredRemoval blocks until the connection killed inside a fill cycle has
// been taken out of the pool and its refill has been accounted for.
//
// The accounting is an either/or on purpose.
// A pool that defers the removal records it and publishes no claim,
// so there is no release to wait for;
// one that spawns a runner the gate then refuses publishes a claim and releases it.
// Waiting for whichever arrives lets the same schedule drive a build that keeps the
// obligation and one that drops it,
// instead of the second timing out in setup and failing for the wrong reason.
//
// Returns:
//   - bool: true when the removal was deferred, false when a claim was refused instead
func awaitDeferredRemoval(t *testing.T, events *poolEventRecorder, pool *hostConnPool, base int) bool {
	t.Helper()

	deadline := time.After(fillEventBudget)
	for {
		if pool.Size() == 0 {
			if poolRefillPending(pool) {
				return true
			}
			if events.count(poolFillDone) > base {
				return false
			}
		}
		select {
		case <-time.After(time.Millisecond):
		case <-deadline:
			t.Fatalf("timed out after %v waiting for the removal to be accounted for (size=%d)",
				fillEventBudget, pool.Size())
			return false
		}
	}
}

// TestFill_RemovalDuringACycleIsNotLost proves a connection that dies while one of
// the pool's own fill cycles is in flight still gets a fill of its own.
//
// HandleError is the only place in normal operation that takes a connection out of
// a pool, so its request is the pool's only record that the connection is gone.
// A fill spawned for it while a cycle holds the gate is refused and forgotten, and
// the cycle that refused it then judges conviction by ITS OWN error - nil, because
// it succeeded - over a pool that is empty by the time it reads the count.
// The pool was left empty, idle and UP, with nothing scheduled and the reconnect
// sweep skipping it because the host is not down.
//
// The kill and its refill both happen inside the poolConnAppended hook, so the gate
// is provably still held when the refill is accounted for, rather than racing it.
func TestFill_RemovalDuringACycleIsNotLost(t *testing.T) {
	harness := newFillHarness(t, 1, nil)
	host := harness.hosts[0]
	pool := harness.pool(t, host)

	startGatedCycle(t, harness, pool)

	// The successor is held at admission, so "a successor ran" is observed rather
	// than inferred from a pool that something else might have refilled.
	admitted := make(chan struct{}, 1)
	release := make(chan struct{})
	releaseSuccessor := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseSuccessor)

	var deferred bool
	var killOnce sync.Once
	killed := make(chan struct{})
	harness.events.on(poolConnAppended, func(*HostInfo) {
		killOnce.Do(func() {
			defer close(killed)

			base := harness.events.count(poolFillDone)
			pool.mu.RLock()
			conn := pool.conns[len(pool.conns)-1]
			pool.mu.RUnlock()
			conn.closeWithError(errors.New("gocql: test induced connection failure"))

			deferred = awaitDeferredRemoval(t, harness.events, pool, base)
			held, _ := poolGate(pool)
			require.True(t, held, "the cycle must still hold the gate when the removal is accounted for")
		})
	})
	harness.events.on(poolFillAdmission, func(*HostInfo) {
		select {
		case admitted <- struct{}{}:
			<-release
		default:
		}
	})

	harness.dialer.releaseAll()
	awaitSignal(t, killed, "the appended connection to be removed")
	require.True(t, deferred, "the removal must be recorded against the cycle that holds the gate")

	awaitSignal(t, admitted, "a successor to be admitted for the deferred removal")
	releaseSuccessor()

	require.Eventually(t, func() bool { return pool.Size() == 1 }, fillEventBudget, time.Millisecond,
		"the deferred removal must end with the pool refilled")
	require.Equal(t, NodeUp, host.State(), "a pool that refilled must leave its host up")
	awaitNoPendingFills(t, harness.session.pool, pool)
}

// startPartialCycle leaves the pool one connection short and starts a cycle that
// parks in the gated dialer filling the gap.
//
// The pool keeps a connection throughout, so fill takes its asynchronous branch and
// the test still has something to kill while the gate is held.
//
// Returns:
//   - *Conn: the connection still in the pool
func startPartialCycle(t *testing.T, harness *fillHarness, pool *hostConnPool) *Conn {
	t.Helper()

	require.Equal(t, 2, pool.size, "this fixture needs a pool that holds two connections")

	harness.dialer.arm(nil)
	detached := detachPoolConn(t, pool)
	t.Cleanup(func() { detached.Close() })

	require.True(t, pool.claimFill(), "the fixture must be able to claim a fill")
	go pool.runFill()
	harness.dialer.awaitStarted(t)
	awaitGate(t, pool, true, "the partial cycle to take the gate")

	pool.mu.RLock()
	defer pool.mu.RUnlock()
	require.Len(t, pool.conns, 1, "the partial cycle must leave one connection in the pool")
	return pool.conns[0]
}

// killDeferred removes conn and waits until the removal has been recorded against
// the cycle that holds the gate.
func killDeferred(t *testing.T, pool *hostConnPool, conn *Conn) {
	t.Helper()

	conn.closeWithError(errors.New("gocql: test induced connection failure"))
	deadline := time.After(fillEventBudget)
	for !(pool.Size() == 0 && poolRefillPending(pool)) {
		select {
		case <-time.After(time.Millisecond):
		case <-deadline:
			t.Fatalf("timed out after %v waiting for the removal to be deferred (size=%d pending=%v)",
				fillEventBudget, pool.Size(), poolRefillPending(pool))
		}
	}
}

// TestFill_ConvictingCycleDischargesTheRemoval proves a cycle that is about to
// consult the conviction policy keeps the deferred removal to itself.
//
// The policy decides what happens to the host, so a successor dialling it at the
// same moment would race the removal the policy is about to cause.
// The obligation ends there whatever the policy answers:
// this fixture declines conviction, which leaves the pool registered, empty and
// idle - exactly what an unconvicted failed cycle leaves behind today.
func TestFill_ConvictingCycleDischargesTheRemoval(t *testing.T) {
	conviction := &recordingConvictionPolicy{onFailure: func(*HostInfo) bool { return false }}
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.NumConns = 2
		cluster.ConvictionPolicy = conviction
	})
	host := harness.hosts[0]
	pool := harness.pool(t, host)

	survivor := startPartialCycle(t, harness, pool)
	killDeferred(t, pool, survivor)

	attempts := harness.events.count(poolConnectAttempt)
	base := harness.events.count(poolFillDone)
	harness.dialer.setErr(func(string) error { return errFillTestDialRefused })
	harness.dialer.releaseAll()
	awaitClaimReleases(t, harness.events, base, 1)

	require.Len(t, conviction.recorded(), 1, "the failed cycle must consult the policy")
	require.False(t, poolRefillPending(pool), "the obligation must not outlive the cycle that held it")
	require.Equal(t, attempts, harness.events.count(poolConnectAttempt),
		"a cycle that consulted the policy must not also start a successor")
	require.Zero(t, pendingFills(pool), "no claim may be left outstanding")
	require.Equal(t, NodeUp, host.State(), "a declined conviction leaves the host as it was")
}

// TestFill_ClosedPoolDischargesTheRemoval proves a removal deferred behind a cycle
// dies with the pool.
//
// The cycle here succeeds, so nothing else would stop a successor being handed the
// obligation; closure is the only reason there is no fill left to give.
func TestFill_ClosedPoolDischargesTheRemoval(t *testing.T) {
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) { cluster.NumConns = 2 })
	pool := harness.pool(t, harness.hosts[0])

	survivor := startPartialCycle(t, harness, pool)
	killDeferred(t, pool, survivor)

	pool.Close()

	base := harness.events.count(poolFillDone)
	harness.dialer.releaseAll()
	awaitClaimReleases(t, harness.events, base, 1)

	require.False(t, poolRefillPending(pool), "the obligation must not outlive the pool")
	require.Equal(t, base+1, harness.events.count(poolFillDone),
		"a closed pool must reserve no successor, so only the owning cycle releases a claim")
	require.Zero(t, pendingFills(pool), "no claim may be left outstanding")
}

// TestFill_ShutdownDischargesTheRemoval proves a cycle cut short by session shutdown
// does not convict and does not hand its obligation on.
//
// The cycle predicts conviction from its own empty-pool failure and keeps the
// obligation on that basis, then finds the session cancelled and returns before the
// policy is consulted at all. Nothing is owed after that: recovery during shutdown
// is not this pool's job.
func TestFill_ShutdownDischargesTheRemoval(t *testing.T) {
	conviction := &recordingConvictionPolicy{}
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
		cluster.NumConns = 2
		cluster.ConvictionPolicy = conviction
	})
	pool := harness.pool(t, harness.hosts[0])

	survivor := startPartialCycle(t, harness, pool)
	killDeferred(t, pool, survivor)

	// Cancelling alone, rather than closing the session: the child pool stays open,
	// which is the window the completion has to get right.
	harness.session.cancel()

	attempts := harness.events.count(poolConnectAttempt)
	base := harness.events.count(poolFillDone)
	awaitClaimReleases(t, harness.events, base, 1)

	require.Empty(t, conviction.recorded(), "a fill cancelled by shutdown must not convict")
	require.False(t, poolRefillPending(pool), "the obligation must not outlive the cycle that held it")
	require.Equal(t, attempts, harness.events.count(poolConnectAttempt),
		"nothing may be dialled after the session was cancelled")
	require.Zero(t, pendingFills(pool), "no claim may be left outstanding")
}

// TestFill_HandoverNeverLeavesThePoolIdle proves the successor's claim is already
// outstanding when the cycle that handed it over releases its own.
//
// A query reads the pool as worth waiting for only while a claim is outstanding
// (pickOrState), so a handover that released before it reserved would show the pool
// as empty and idle for an instant and send every waiter away with
// ErrNoConnections.
// The claim is therefore reserved in the same critical section that clears the
// gate, and the sample below is taken on the outgoing goroutine immediately after
// its own decrement.
func TestFill_HandoverNeverLeavesThePoolIdle(t *testing.T) {
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) { cluster.NumConns = 2 })
	pool := harness.pool(t, harness.hosts[0])

	survivor := startPartialCycle(t, harness, pool)
	killDeferred(t, pool, survivor)

	var sampleOnce sync.Once
	sampled := make(chan int, 1)
	harness.events.on(poolFillDone, func(*HostInfo) {
		sampleOnce.Do(func() { sampled <- pendingFills(pool) })
	})

	harness.dialer.releaseAll()

	select {
	case pending := <-sampled:
		require.GreaterOrEqual(t, pending, 1,
			"the successor's claim must already be published when its predecessor releases")
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for the handover", fillEventBudget)
	}

	require.Eventually(t, func() bool { return pool.Size() == 2 }, fillEventBudget, time.Millisecond,
		"the successor must fill the gap the removal left")
	awaitNoPendingFills(t, harness.session.pool, pool)
}
