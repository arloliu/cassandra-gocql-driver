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
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// bigRowsStatement is the statement the oversized fixture response answers.
//
// It is not one of the server's built-in statements, so the servers that are not asked
// for the big response fall through to their plain void result.
const bigRowsStatement = "bigrows"

// oversizedBodySize is the size of that response's blob column.
//
// The frame body it produces is larger than maxPooledBufSize, which is what makes the
// response observable: readFrame sizes the read buffer to the body, and only
// framer.release trims a buffer that big back to defaultBufSize.
const oversizedBodySize = maxPooledBufSize + 4096

// drainRunnersFrame is the stack frame a goroutine inside the drain shows.
const drainRunnersFrame = "(*queryExecutor).drainRunners"

// hostSet is the set of server addresses answering with the oversized response.
//
// The servers are started before the ring is known, so the fixture picks its losers
// afterwards and the receive path reads the choice under a mutex.
type hostSet struct {
	mu  sync.Mutex
	ips map[string]bool
}

// newHostSet returns an empty set.
//
// Returns:
//   - *hostSet: the set
func newHostSet() *hostSet {
	return &hostSet{ips: map[string]bool{}}
}

// set replaces the membership with ips.
func (s *hostSet) set(ips ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ips = map[string]bool{}
	for _, ip := range ips {
		s.ips[ip] = true
	}
}

// has reports whether ip is in the set.
//
// Returns:
//   - bool: true when ip was chosen
func (s *hostSet) has(ip string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ips[ip]
}

// bigRowsRespHook returns a fillHarnessOpts.respHook answering bigRowsStatement with one
// row holding one oversized blob, from the servers in big.
//
// Every other server, and every other request, falls through to the server's own
// handling.
//
// Parameters:
//   - big: the servers that must answer with the oversized response
//
// Returns:
//   - func(string, *TestServer, *framer, *framer) bool: the response hook
func bigRowsRespHook(big *hostSet) func(string, *TestServer, *framer, *framer) bool {
	payload := make([]byte, oversizedBodySize)
	return func(ip string, _ *TestServer, req, resp *framer) bool {
		if req.header == nil || req.header.op != opQuery || !big.has(ip) {
			return false
		}

		// Peek at the statement without consuming it: the server's own handler
		// re-reads the same body when this hook falls through.
		buf := req.buf
		query, err := req.readLongString()
		req.buf = buf
		if err != nil || query != bigRowsStatement {
			return false
		}

		resp.writeHeader(0, opResult, req.header.stream)
		resp.writeInt(resultKindRows)
		// <metadata>
		resp.writeInt(int32(flagGlobalTableSpec)) // <flags>
		resp.writeInt(1)                          // <columns_count>
		resp.writeString("ks")                    // <global_table_spec>
		resp.writeString("t")
		resp.writeString("payload") // <col_spec_0>
		resp.writeShort(uint16(TypeBlob))
		// <rows_count>
		resp.writeInt(1)
		resp.writeBytes(payload)
		return true
	}
}

// drainSeam observes queryExecutor.testBeforeDrainClose.
//
// It carries no cleanup of its own: it publishes the iterator the drain is about to
// close and parks the drain there until the test releases it.
// Because every arrival is on the drain's own goroutine, an arrival received by the test
// happens after the previous iterator was closed, which is the only ordering these tests
// have for reading a drained iterator's state.
type drainSeam struct {
	arrivals chan *Iter
	release  chan struct{}
	once     sync.Once
	seen     atomic.Int32
	panics   atomic.Bool
}

// newDrainSeam returns a seam that parks every arrival until it is released.
//
// Returns:
//   - *drainSeam: the seam, ready to be installed as testBeforeDrainClose
func newDrainSeam() *drainSeam {
	return &drainSeam{arrivals: make(chan *Iter, 8), release: make(chan struct{})}
}

// hook is the queryExecutor.testBeforeDrainClose.
func (s *drainSeam) hook(iter *Iter) {
	s.seen.Add(1)
	select {
	case s.arrivals <- iter:
	default:
	}

	<-s.release
	if s.panics.Load() {
		panic("drain seam panic")
	}
}

// panicOnRelease makes every released arrival panic, so a test can inject a panic into
// the drain loop itself rather than into one iterator's cleanup.
func (s *drainSeam) panicOnRelease() {
	s.panics.Store(true)
}

// await blocks until the drain reaches one more iterator, and returns it.
//
// Returns:
//   - *Iter: the iterator the drain is about to close
func (s *drainSeam) await(t *testing.T, what string) *Iter {
	t.Helper()

	select {
	case iter := <-s.arrivals:
		return iter
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for %s", fillEventBudget, what)
		return nil
	}
}

// releaseAll lets every parked and future arrival through.
func (s *drainSeam) releaseAll() {
	s.once.Do(func() { close(s.release) })
}

// count returns how many iterators the drain reached.
//
// Returns:
//   - int: the number of arrivals
func (s *drainSeam) count() int {
	return int(s.seen.Load())
}

// consumeRecorder observes queryExecutor.testAfterConsume.
//
// Every arrival is a message coordinate itself took off one of the two channels, on the
// coordinate goroutine, so awaiting one is a deterministic acknowledgement that the
// accounting happened - not that the message was merely published.
// The drain does not report here, which is what keeps the two consumers apart.
type consumeRecorder struct {
	arrivals chan int
	seen     atomic.Int32
}

// newConsumeRecorder returns a recorder ready to be installed as testAfterConsume.
//
// Returns:
//   - *consumeRecorder: the recorder
func newConsumeRecorder() *consumeRecorder {
	return &consumeRecorder{arrivals: make(chan int, 16)}
}

// hook is the queryExecutor.testAfterConsume. It never blocks coordinate.
func (r *consumeRecorder) hook(consumed int) {
	r.seen.Store(int32(consumed))
	select {
	case r.arrivals <- consumed:
	default:
	}
}

// await blocks until coordinate has counted its want-th message.
func (r *consumeRecorder) await(t *testing.T, want int, what string) {
	t.Helper()

	for {
		select {
		case got := <-r.arrivals:
			if got == want {
				return
			}
			require.Less(t, got, want, "coordinate consumed past %d while waiting for %s", want, what)
		case <-time.After(fillEventBudget):
			t.Fatalf("timed out after %v waiting for %s", fillEventBudget, what)
		}
	}
}

// count returns how many messages coordinate counted.
//
// Returns:
//   - int: the last consumed value reported
func (r *consumeRecorder) count() int {
	return int(r.seen.Load())
}

// publicationHolds parks chosen publications of a speculative query.
//
// The Nth arrival at the chosen stage parks on the Nth hold, when the test asked for one,
// so a test decides which runner publishes - and therefore wins - and which runners are
// left for coordinate's drain.
// Arrival order is the test's own release order, because the runners are let out of
// runEntered one at a time.
type publicationHolds struct {
	// stage is the run checkpoint the holds count and park at.
	stage runStage

	mu    sync.Mutex
	holds map[int]chan struct{}
	once  map[int]*sync.Once
	seen  int
}

// newPublicationHolds returns holds that park the given result publications.
//
// Parameters:
//   - park: the 1-based publication numbers to hold
//
// Returns:
//   - *publicationHolds: the holds
func newPublicationHolds(park ...int) *publicationHolds {
	return newPublicationHoldsAt(runResult, park...)
}

// newPublicationHoldsAt returns holds that park the given arrivals at stage.
//
// A retirement publishes at runRetired instead of runResult, and the seam sits before
// that send just as it does for a result, so the same ordering discipline works for
// either: the Nth arrival at the stage parks on the Nth hold.
//
// Parameters:
//   - stage: the run checkpoint to count and park at
//   - park: the 1-based arrival numbers to hold
//
// Returns:
//   - *publicationHolds: the holds
func newPublicationHoldsAt(stage runStage, park ...int) *publicationHolds {
	h := &publicationHolds{stage: stage, holds: map[int]chan struct{}{}, once: map[int]*sync.Once{}}
	for _, n := range park {
		h.holds[n] = make(chan struct{})
		h.once[n] = &sync.Once{}
	}
	return h
}

// wrap returns a testRunHook that runs inner and then parks the held publications.
//
// Returns:
//   - func(runStage): the hook to install
func (h *publicationHolds) wrap(inner func(runStage)) func(runStage) {
	return func(stage runStage) {
		if stage != h.stage {
			inner(stage)
			return
		}

		// The ordinal is claimed before the arrival is announced, because the
		// announcement is what lets the test release the next runner: a runner
		// descheduled between announcing and claiming would let the runner released
		// on the strength of that announcement take the hold meant for it.
		h.mu.Lock()
		h.seen++
		hold := h.holds[h.seen]
		h.mu.Unlock()

		inner(stage)

		if hold != nil {
			<-hold
		}
	}
}

// release lets the nth publication through.
func (h *publicationHolds) release(n int) {
	h.mu.Lock()
	hold, once := h.holds[n], h.once[n]
	h.mu.Unlock()

	if once != nil {
		once.Do(func() { close(hold) })
	}
}

// releaseAll lets every held publication through.
func (h *publicationHolds) releaseAll() {
	h.mu.Lock()
	held := make([]int, 0, len(h.holds))
	for n := range h.holds {
		held = append(held, n)
	}
	h.mu.Unlock()

	for _, n := range held {
		h.release(n)
	}
}

// drainGoroutines counts the goroutines currently inside queryExecutor.drainRunners.
//
// Returns:
//   - int: the number of goroutines in the drain
func drainGoroutines() int {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Count(string(buf[:n]), drainRunnersFrame)
		}
		buf = make([]byte, 2*len(buf))
	}
}

// awaitDrainGoroutines blocks until exactly want goroutines are inside the drain.
//
// A goroutine's own termination is not a checkpoint a test can subscribe to, so this is
// the one place these tests poll; the deadline is what turns a drain that never ends
// into a failure instead of a hang.
// The caller passes a count relative to a baseline taken at its own start, so a drain
// leaked by an earlier test cannot decide this one.
func awaitDrainGoroutines(t *testing.T, want int, what string) {
	t.Helper()

	deadline := time.After(fillEventBudget)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		got := drainGoroutines()
		if got == want {
			return
		}

		select {
		case <-ticker.C:
		case <-deadline:
			t.Fatalf("timed out after %v waiting for %s: %d goroutine(s) in the drain, want %d",
				fillEventBudget, what, got, want)
		}
	}
}

// iterAsync runs qry in the background and reports its iterator once.
//
// It is execAsync with the iterator kept, for the tests that assert on the winning
// runner's host rather than only on the query's error.
//
// Returns:
//   - <-chan *Iter: receives the query's iterator exactly once
func iterAsync(qry *Query) <-chan *Iter {
	result := make(chan *Iter, 1)
	go func() { result <- qry.Iter() }()
	return result
}

// awaitIter blocks until the query produced its iterator.
//
// Returns:
//   - *Iter: the query's iterator
func awaitIter(t *testing.T, result <-chan *Iter, what string) *Iter {
	t.Helper()

	select {
	case iter := <-result:
		return iter
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for %s", fillEventBudget, what)
		return nil
	}
}

// oversizedFramer returns a framer whose read buffer is larger than any pooled buffer.
//
// Returns:
//   - *framer: the framer, in the state release trims and reset leaves alone
func oversizedFramer() *framer {
	buf := make([]byte, oversizedBodySize)
	return &framer{buf: buf[:0], readBuffer: buf}
}

// drainOnlyExecutor returns an executor wired with nothing but the logger the drain
// needs, for the sub-cases that call drainRunners directly.
//
// Returns:
//   - *queryExecutor: the executor
func drainOnlyExecutor() *queryExecutor {
	return &queryExecutor{pool: &policyConnPool{session: &Session{logger: nopLoggerSingleton}}}
}

// panickingDelayPolicy is a SpeculativeExecutionPolicy whose Delay panics: user code that
// fails after coordinate has already launched a runner.
type panickingDelayPolicy struct{}

var _ SpeculativeExecutionPolicy = panickingDelayPolicy{}

// Attempts asks for one speculative launch, so coordinate reads Delay.
func (panickingDelayPolicy) Attempts() int { return 1 }

// Delay panics instead of returning a tick.
func (panickingDelayPolicy) Delay() time.Duration { panic("speculative delay") }

// fixedAttemptsPolicy is a SpeculativeExecutionPolicy with a caller-chosen attempt count,
// including the negative counts SimpleSpeculativeExecution does not reject.
type fixedAttemptsPolicy struct {
	attempts int
}

var _ SpeculativeExecutionPolicy = fixedAttemptsPolicy{}

// Attempts returns the configured count.
func (p fixedAttemptsPolicy) Attempts() int { return p.attempts }

// Delay is an hour: a negative attempt count must never reach a ticker anyway.
func (fixedAttemptsPolicy) Delay() time.Duration { return time.Hour }

// TestSpeculative_LosingItersAreClosed proves the iterators no caller receives are closed:
// the two losing runners publish after coordinate returned, and the drain returns their
// response framers to the pool.
//
// The assertion is the one effect release has and reset does not: an oversized read
// buffer is trimmed back to defaultBufSize (frame.go:648-650).
// The observation window is bounded and still by construction, so no other user of the
// framer pool can be holding the framer under test: the control connection is disabled
// (conn_test.go:99-103), the initial fill is complete before newFillHarnessOpts returns,
// both halves of the heartbeat are turned off, the fixture issues no further request,
// and no fixture runs in parallel with it.
func TestSpeculative_LosingItersAreClosed(t *testing.T) {
	baseline := drainGoroutines()

	big := newHostSet()
	harness := newFillHarnessOpts(t, 3, fillHarnessOpts{
		// Pinned to v4: the v5 segment encoder rejects a payload of this size
		// (frame.go:2784-2792), and the size is the whole point of the response.
		proto:    protoVersion4,
		respHook: bigRowsRespHook(big),
		tune: func(cluster *ClusterConfig) {
			// Both halves: noHeartbeat only pushes out the interval, the first
			// heartbeat of a connection is scheduled by the phase.
			noHeartbeat(cluster)
			cluster.heartbeatPhase = func(time.Duration) time.Duration { return time.Hour }
		},
	})

	losers, winner := harness.hosts[:2], harness.hosts[2]
	big.set(hostIP(losers[0]), hostIP(losers[1]))
	harness.session.executor.policy = &scriptedIterPolicy{
		HostSelectionPolicy: harness.session.executor.policy,
		script:              [][]*HostInfo{{losers[0], losers[1], winner}},
	}

	seam := newDrainSeam()
	t.Cleanup(seam.releaseAll)
	harness.session.executor.testBeforeDrainClose = seam.hook

	stages := newRunStageRecorder()
	entered := stages.gate(runEntered)
	t.Cleanup(releaseStageGate(entered))
	holds := newPublicationHolds(1, 2)
	t.Cleanup(holds.releaseAll)
	harness.session.executor.testRunHook = holds.wrap(stages.hook)

	qry := harness.session.Query(bigRowsStatement).WithContext(t.Context()).RetryPolicy(nil)
	speculative(2, speculativeTick)(qry)
	result := iterAsync(qry)

	// One runner at a time: each draws its scripted host and reaches publication before
	// the next is let go, so the two losers are the two oversized responses.
	// Only the released runner is running, and a publication claims its hold before it
	// is announced, so the hold each runner parks on is the one this loop intends.
	for i := range 3 {
		stages.await(t, runEntered, fmt.Sprintf("runner %d to start", i+1))
		entered <- struct{}{}
		if i < 2 {
			stages.await(t, runResult, fmt.Sprintf("runner %d to reach publication", i+1))
		}
	}

	// The host on the winning iterator is the direct check that the schedule assigned
	// the runners the way the oversized precondition below assumes.
	won := awaitIter(t, result, "the third runner's result to win")
	// Pointer identity: a HostInfo carries a mutex and mutable fields, so comparing the
	// structs would neither mean identity nor be safe to read.
	require.Same(t, winner, won.Host(), "the runner that was let publish is the void one")
	require.NoError(t, won.Close(), "the winning iterator is the caller's")

	// coordinate returned with two runners still holding their iterator.
	awaitDrainGoroutines(t, baseline+1, "the drain to wait for the held publications")
	require.Equal(t, 0, seam.count(), "nothing can have been drained yet")

	holds.releaseAll()

	first := seam.await(t, "the drain to reach the first losing iterator")
	require.True(t, first.Host() == losers[0] || first.Host() == losers[1],
		"the drain must reach a losing runner's iterator")
	framer := first.framer
	require.NotNil(t, framer, "the losing iterator must still hold its response framer")
	require.Greater(t, cap(framer.readBuffer), maxPooledBufSize,
		"the fixture must produce a response larger than any pooled buffer")

	seam.releaseAll()

	// The second arrival is on the drain's goroutine, after the first close returned.
	second := seam.await(t, "the drain to reach the second losing iterator")
	require.NotSame(t, first, second, "the drain must reach both losing iterators")
	require.True(t, second.Host() == losers[0] || second.Host() == losers[1],
		"the drain must reach a losing runner's iterator")
	require.NotSame(t, first.Host(), second.Host(), "the two drained iterators are the two losers")
	require.Nil(t, first.framer, "the drained iterator must have let go of its framer")
	require.LessOrEqual(t, cap(framer.readBuffer), defaultBufSize,
		"release must trim the oversized read buffer")

	awaitDrainGoroutines(t, baseline, "the drain to finish")
	require.Equal(t, 2, seam.count(), "both outstanding iterators were processed")
}

// TestSpeculative_DrainReleasesOversizedFramer is the supplementary coverage for the gate
// above: a synthetic iterator holding an oversized framer goes through the production
// drain and the real Iter.Close.
//
// It does not prove the ownership hand-over of a real response, so it does not replace
// TestSpeculative_LosingItersAreClosed.
func TestSpeculative_DrainReleasesOversizedFramer(t *testing.T) {
	framer := oversizedFramer()
	iter := &Iter{framer: framer}

	results := make(chan *Iter, 1)
	results <- iter
	drainOnlyExecutor().drainRunners(results, make(chan retirement), 1)

	require.Nil(t, iter.framer, "the drain must close the iterator")
	require.LessOrEqual(t, cap(framer.readBuffer), defaultBufSize,
		"release must trim the oversized read buffer")
}

// TestSpeculative_DrainAccountingTerminates covers every way coordinate can consume the
// runners' terminal messages.
//
// Each sub-case asserts the two halves separately: the drain ended, and every outstanding
// iterator was processed.
// A sub-case with nothing outstanding asserts no drain was ever started, which is not the
// same as one that started and finished.
//
// The sub-cases that mix a no-host report with a result wait for coordinate itself to
// count that report - testAfterConsume runs on the coordinate goroutine, and the drain
// never reports there - before the winner is released or the context is cancelled.
// The history is therefore established rather than probable, and each of those sub-cases
// asserts it as an equality: how many messages coordinate consumed, how many runners were
// launched, and that the drain processed exactly the difference.
func TestSpeculative_DrainAccountingTerminates(t *testing.T) {
	t.Run("main wins before the first tick", func(t *testing.T) {
		baseline := drainGoroutines()
		harness := newFillHarness(t, 2, nil)

		seam := newDrainSeam()
		seam.releaseAll()
		harness.session.executor.testBeforeDrainClose = seam.hook
		stages := newRunStageRecorder()
		harness.session.executor.testRunHook = stages.hook

		qry := harness.session.Query("void").WithContext(t.Context())
		speculative(1, time.Hour)(qry)
		require.NoError(t, awaitQuery(t, execAsync(qry)))
		awaitRunnersExited(t, stages, 1)

		require.Equal(t, 1, stages.count(runEntered), "the scheduled launch never happened")
		require.Equal(t, 0, seam.count(), "nothing was left for a drain")
		awaitDrainGoroutines(t, baseline, "no drain to have been started at all")
	})

	t.Run("both runners succeed", func(t *testing.T) {
		baseline := drainGoroutines()
		harness := newFillHarness(t, 2, nil)

		seam := newDrainSeam()
		t.Cleanup(seam.releaseAll)
		harness.session.executor.testBeforeDrainClose = seam.hook

		stages := newRunStageRecorder()
		entered := stages.gate(runEntered)
		t.Cleanup(releaseStageGate(entered))
		holds := newPublicationHolds(1)
		t.Cleanup(holds.releaseAll)
		harness.session.executor.testRunHook = holds.wrap(stages.hook)

		qry := harness.session.Query("void").WithContext(t.Context())
		speculative(1, speculativeTick)(qry)
		result := execAsync(qry)

		stages.await(t, runEntered, "the main runner to start")
		entered <- struct{}{}
		stages.await(t, runResult, "the main runner to reach publication")
		stages.await(t, runEntered, "the speculative runner to start")
		entered <- struct{}{}
		require.NoError(t, awaitQuery(t, result), "the speculative runner's result wins")

		awaitDrainGoroutines(t, baseline+1, "the drain to wait for the held publication")
		require.Equal(t, 0, seam.count(), "nothing can have been drained yet")

		holds.releaseAll()
		seam.await(t, "the drain to reach the losing iterator")
		seam.releaseAll()

		awaitDrainGoroutines(t, baseline, "the drain to finish")
		require.Equal(t, 1, seam.count(), "the one outstanding iterator was processed")
	})

	t.Run("no host then success", func(t *testing.T) {
		baseline := drainGoroutines()
		harness := newFillHarness(t, 2, nil)
		harness.session.executor.policy = &scriptedIterPolicy{
			HostSelectionPolicy: harness.session.executor.policy,
			script:              [][]*HostInfo{{harness.hosts[0], harness.hosts[1]}},
		}

		seam := newDrainSeam()
		t.Cleanup(seam.releaseAll)
		harness.session.executor.testBeforeDrainClose = seam.hook
		consumes := newConsumeRecorder()
		harness.session.executor.testAfterConsume = consumes.hook

		stages := newRunStageRecorder()
		entered := stages.gate(runEntered)
		t.Cleanup(releaseStageGate(entered))
		reported := stages.gate(runRetired)
		t.Cleanup(releaseStageGate(reported))
		holds := newPublicationHolds(1, 2)
		t.Cleanup(holds.releaseAll)
		harness.session.executor.testRunHook = holds.wrap(stages.hook)

		qry := harness.session.Query("void").WithContext(t.Context())
		speculative(2, speculativeTick)(qry)
		result := execAsync(qry)

		// Two runners take the two hosts and hold their result; the third finds the
		// shared enumeration spent and holds its no-host report.
		for i := range 2 {
			stages.await(t, runEntered, fmt.Sprintf("runner %d to start", i+1))
			entered <- struct{}{}
			stages.await(t, runResult, fmt.Sprintf("runner %d to reach publication", i+1))
		}
		stages.await(t, runEntered, "the third runner to start")
		entered <- struct{}{}
		stages.await(t, runRetired, "the third runner to reach its no-host report")

		// The winner is released only once coordinate has counted the no-host report,
		// so the history is a consumed report followed by a consumed result.
		reported <- struct{}{}
		consumes.await(t, 1, "coordinate to consume the no-host report")

		holds.release(2)
		require.NoError(t, awaitQuery(t, result), "the second runner's result wins")

		awaitDrainGoroutines(t, baseline+1, "the drain to wait for the held runner")
		require.Equal(t, 3, stages.count(runEntered), "three runners were launched")
		require.Equal(t, 2, consumes.count(), "coordinate consumed the report and the winner")

		holds.release(1)
		seam.await(t, "the drain to reach the losing iterator")
		seam.releaseAll()

		// launched 3 - consumed 2 = 1 outstanding, and the drain processed exactly that.
		awaitDrainGoroutines(t, baseline, "the drain to finish")
		require.Equal(t, 1, seam.count(), "the one outstanding iterator was processed")
	})

	t.Run("all runners report no host", func(t *testing.T) {
		baseline := drainGoroutines()
		harness := newFillHarness(t, 2, nil)
		for _, host := range harness.hosts {
			harness.session.markHostDown(host)
		}
		installOneShotPolicy(harness)

		seam := newDrainSeam()
		seam.releaseAll()
		harness.session.executor.testBeforeDrainClose = seam.hook

		stages := newRunStageRecorder()
		reported := stages.gate(runRetired)
		t.Cleanup(releaseStageGate(reported))
		harness.session.executor.testRunHook = stages.hook

		qry := harness.session.Query("void").WithContext(t.Context())
		speculative(1, speculativeTick)(qry)
		result := execAsync(qry)

		// Both runners hold their report, so both are launched before either is
		// consumed.
		stages.await(t, runRetired, "the main runner to reach its no-host report")
		stages.await(t, runRetired, "the speculative runner to reach its no-host report")
		reported <- struct{}{}
		reported <- struct{}{}

		require.ErrorIs(t, awaitQuery(t, result), ErrNoConnections)
		awaitRunnersExited(t, stages, 2)

		require.Equal(t, 0, seam.count(), "a no-host report carries no iterator")
		awaitDrainGoroutines(t, baseline, "no drain to have been started at all")
	})

	t.Run("ctx cancelled with partial messages", func(t *testing.T) {
		baseline := drainGoroutines()
		harness := newFillHarness(t, 2, nil)
		harness.session.executor.policy = &scriptedIterPolicy{
			HostSelectionPolicy: harness.session.executor.policy,
			script:              [][]*HostInfo{{harness.hosts[0], harness.hosts[1]}},
		}

		seam := newDrainSeam()
		t.Cleanup(seam.releaseAll)
		harness.session.executor.testBeforeDrainClose = seam.hook
		consumes := newConsumeRecorder()
		harness.session.executor.testAfterConsume = consumes.hook

		stages := newRunStageRecorder()
		entered := stages.gate(runEntered)
		t.Cleanup(releaseStageGate(entered))
		reported := stages.gate(runRetired)
		t.Cleanup(releaseStageGate(reported))
		holds := newPublicationHolds(1, 2)
		t.Cleanup(holds.releaseAll)
		harness.session.executor.testRunHook = holds.wrap(stages.hook)

		ctx, cancel := context.WithCancel(t.Context())
		qry := harness.session.Query("void").WithContext(ctx)
		speculative(2, speculativeTick)(qry)
		result := execAsync(qry)

		for i := range 2 {
			stages.await(t, runEntered, fmt.Sprintf("runner %d to start", i+1))
			entered <- struct{}{}
			stages.await(t, runResult, fmt.Sprintf("runner %d to reach publication", i+1))
		}

		// The context is cancelled only once coordinate has counted the third runner's
		// no-host report, so the query ends with one message consumed and two held.
		stages.await(t, runEntered, "the third runner to start")
		entered <- struct{}{}
		stages.await(t, runRetired, "the third runner to reach its no-host report")
		reported <- struct{}{}
		consumes.await(t, 1, "coordinate to consume the no-host report")

		cancel()
		require.ErrorIs(t, awaitQuery(t, result), context.Canceled)
		require.Equal(t, 3, stages.count(runEntered), "three runners were launched")
		require.Equal(t, 1, consumes.count(), "coordinate consumed the report and nothing else")

		awaitDrainGoroutines(t, baseline+1, "the drain to wait for both held publications")
		holds.releaseAll()

		seam.await(t, "the drain to reach the first iterator")
		seam.releaseAll()
		seam.await(t, "the drain to reach the second iterator")

		// launched 3 - consumed 1 = 2 outstanding, and the drain processed exactly those.
		awaitDrainGoroutines(t, baseline, "the drain to finish")
		require.Equal(t, 2, seam.count(), "both outstanding iterators were processed")
	})

	t.Run("a panicking delay still drains the launched runner", func(t *testing.T) {
		baseline := drainGoroutines()
		harness := newFillHarness(t, 2, nil)

		seam := newDrainSeam()
		t.Cleanup(seam.releaseAll)
		harness.session.executor.testBeforeDrainClose = seam.hook

		stages := newRunStageRecorder()
		holds := newPublicationHolds(1)
		t.Cleanup(holds.releaseAll)
		harness.session.executor.testRunHook = holds.wrap(stages.hook)

		qry := harness.session.Query("void").WithContext(t.Context())
		qry.Idempotent(true).SetSpeculativeExecutionPolicy(panickingDelayPolicy{})

		// The main runner is already launched when Delay panics, so the cleanup that
		// drains it must be installed before that setup runs.
		require.Panics(t, func() { qry.Iter() }, "the policy's Delay panics")

		awaitDrainGoroutines(t, baseline+1, "the drain to wait for the launched runner")
		holds.releaseAll()

		seam.await(t, "the drain to reach the launched runner's iterator")
		seam.releaseAll()

		awaitDrainGoroutines(t, baseline, "the drain to finish")
		require.Equal(t, 1, seam.count(), "the one outstanding iterator was processed")
	})

	t.Run("a panic in the drain loop is contained", func(t *testing.T) {
		baseline := drainGoroutines()
		harness := newFillHarness(t, 2, nil)

		seam := newDrainSeam()
		t.Cleanup(seam.releaseAll)
		seam.panicOnRelease()
		harness.session.executor.testBeforeDrainClose = seam.hook

		stages := newRunStageRecorder()
		entered := stages.gate(runEntered)
		t.Cleanup(releaseStageGate(entered))
		holds := newPublicationHolds(1)
		t.Cleanup(holds.releaseAll)
		harness.session.executor.testRunHook = holds.wrap(stages.hook)

		qry := harness.session.Query("void").WithContext(t.Context())
		speculative(1, speculativeTick)(qry)
		result := execAsync(qry)

		stages.await(t, runEntered, "the main runner to start")
		entered <- struct{}{}
		stages.await(t, runResult, "the main runner to reach publication")
		stages.await(t, runEntered, "the speculative runner to start")
		entered <- struct{}{}
		require.NoError(t, awaitQuery(t, result), "the speculative runner's result wins")

		holds.releaseAll()
		seam.await(t, "the drain to reach the losing iterator")

		// The panic is raised on the drain's goroutine, where only the drain's own
		// recover can see it: without it the process would not survive this line.
		seam.releaseAll()
		awaitDrainGoroutines(t, baseline, "the drain to end on its own recover")
	})

	t.Run("a panicking cleanup does not stop the drain", func(t *testing.T) {
		framer := oversizedFramer()
		iter := &Iter{framer: framer}

		// Close panics on a nil iterator, which is the cleanup panic the per-item
		// isolation has to absorb; the real iterator behind it must still be consumed.
		results := make(chan *Iter, 2)
		results <- nil
		results <- iter
		drainOnlyExecutor().drainRunners(results, make(chan retirement), 2)

		require.Nil(t, iter.framer, "the drain must consume the messages behind the panic")
		require.LessOrEqual(t, cap(framer.readBuffer), defaultBufSize,
			"release must trim the oversized read buffer")
	})
}

// TestSpeculative_LatePublisherIsDrained proves a runner that publishes after coordinate
// returned is still drained: it is holding a response framer, and nothing else would
// ever reclaim it.
func TestSpeculative_LatePublisherIsDrained(t *testing.T) {
	baseline := drainGoroutines()
	harness := newFillHarness(t, 2, nil)

	seam := newDrainSeam()
	t.Cleanup(seam.releaseAll)
	harness.session.executor.testBeforeDrainClose = seam.hook

	stages := newRunStageRecorder()
	entered := stages.gate(runEntered)
	t.Cleanup(releaseStageGate(entered))
	holds := newPublicationHolds(1)
	t.Cleanup(holds.releaseAll)
	harness.session.executor.testRunHook = holds.wrap(stages.hook)

	qry := harness.session.Query("void").WithContext(t.Context())
	speculative(1, speculativeTick)(qry)
	result := execAsync(qry)

	stages.await(t, runEntered, "the main runner to start")
	entered <- struct{}{}
	stages.await(t, runResult, "the main runner to reach publication")
	stages.await(t, runEntered, "the speculative runner to start")
	entered <- struct{}{}
	require.NoError(t, awaitQuery(t, result), "the speculative runner's result wins")

	// Only now does the loser publish: the query is over, its context is cancelled, and
	// no caller is left to read the channel.
	holds.releaseAll()

	late := seam.await(t, "the drain to reach the late publication")
	require.NotNil(t, late.framer, "the late iterator must still hold its response framer")
	seam.releaseAll()

	awaitDrainGoroutines(t, baseline, "the drain to finish")
	require.Equal(t, 1, seam.count(), "the late publication was processed")
}

// TestSpeculative_NegativeAttemptsIsTreatedAsZero proves a policy asking for a negative
// number of speculative runs behaves as one asking for none.
//
// The channel capacities are 1+Attempts: a negative count would size them below the
// number of runners, and under -1 make is not even able to build them.
func TestSpeculative_NegativeAttemptsIsTreatedAsZero(t *testing.T) {
	for _, attempts := range []int{-1, -2} {
		t.Run(fmt.Sprintf("attempts %d", attempts), func(t *testing.T) {
			baseline := drainGoroutines()
			harness := newFillHarness(t, 2, nil)

			seam := newDrainSeam()
			seam.releaseAll()
			harness.session.executor.testBeforeDrainClose = seam.hook
			stages := newRunStageRecorder()
			harness.session.executor.testRunHook = stages.hook

			qry := harness.session.Query("void").WithContext(t.Context())
			qry.Idempotent(true).SetSpeculativeExecutionPolicy(fixedAttemptsPolicy{attempts: attempts})
			require.NoError(t, awaitQuery(t, execAsync(qry)))
			awaitRunnersExited(t, stages, 1)

			require.Equal(t, 1, stages.count(runEntered), "only the main runner is launched")
			require.Equal(t, 0, seam.count(), "nothing was left for a drain")
			awaitDrainGoroutines(t, baseline, "no drain to have been started at all")
		})
	}
}

// hostPanicObserver is a QueryObserver that panics for the attempts made against one host.
//
// It is the cheapest way to make q.do panic inside one speculative runner and only that
// one: the observation runs on the runner's own goroutine, inside attemptQuery, and the
// host it reports is the host that runner drew.
// A panicking RetryPolicy would not do - it is consulted only for a failed attempt, and
// the fixture servers answer successfully.
type hostPanicObserver struct {
	// ip is the connect address of the host whose attempts panic.
	ip string
	// value is the panic value raised.
	value any
}

var _ QueryObserver = hostPanicObserver{}

// ObserveQuery panics for the attempts made against the chosen host.
func (o hostPanicObserver) ObserveQuery(_ context.Context, q ObservedQuery) {
	if q.Host != nil && hostIP(q.Host) == o.ip {
		panic(o.value)
	}
}

// TestSpeculative_PanickingRunnerStillPublishes proves a runner whose q.do panics still
// publishes exactly one terminal message, and that coordinate's accounting is unharmed by
// it.
//
// The publication is the plain send inside run's recoverGoroutine teardown. Without it the
// panicking runner would go silent: coordinate has no ticker left after its single launch,
// so it would sit on the query context - which defaults to context.Background() in
// production - and the drain accounting would wait for a message that never comes.
//
// The panicking runner is the second one to draw, so the first runner is holding a
// publication when the panic decides the query. That leaves the two halves observable
// separately: the caller receives the panic as an error, and the runner nobody read is
// still drained.
func TestSpeculative_PanickingRunnerStillPublishes(t *testing.T) {
	baseline := drainGoroutines()
	harness := newFillHarness(t, 2, nil)
	harness.session.executor.policy = &scriptedIterPolicy{
		HostSelectionPolicy: harness.session.executor.policy,
		script:              [][]*HostInfo{{harness.hosts[0], harness.hosts[1]}},
	}

	seam := newDrainSeam()
	t.Cleanup(seam.releaseAll)
	harness.session.executor.testBeforeDrainClose = seam.hook
	consumes := newConsumeRecorder()
	harness.session.executor.testAfterConsume = consumes.hook

	stages := newRunStageRecorder()
	entered := stages.gate(runEntered)
	t.Cleanup(releaseStageGate(entered))
	holds := newPublicationHolds(1)
	t.Cleanup(holds.releaseAll)
	harness.session.executor.testRunHook = holds.wrap(stages.hook)

	value := callbackPanic{where: "speculative runner"}
	qry := harness.session.Query("void").WithContext(t.Context()).
		Observer(hostPanicObserver{ip: hostIP(harness.hosts[1]), value: value})
	speculative(1, speculativeTick)(qry)
	result := execAsync(qry)

	// The first runner draws the host whose attempts are observed normally and holds its
	// result; the second draws the host the observer panics on, so the only message
	// coordinate can consume is the one run's teardown publishes.
	stages.await(t, runEntered, "the main runner to start")
	entered <- struct{}{}
	stages.await(t, runResult, "the main runner to reach publication")
	stages.await(t, runEntered, "the speculative runner to start")
	entered <- struct{}{}

	err := awaitQuery(t, result)
	require.ErrorContains(t, err, "gocql: panic in goroutine queryExecutor.run",
		"the caller receives the panic recoverGoroutine converted")
	require.ErrorContains(t, err, "speculative runner", "the error carries the original panic value")

	require.Equal(t, 2, stages.count(runEntered), "two runners were launched")
	require.Equal(t, 1, consumes.count(), "coordinate consumed the panicking runner's message and no other")

	// launched 2 - consumed 1 = 1 outstanding: the runner still holding its publication.
	awaitDrainGoroutines(t, baseline+1, "the drain to wait for the held publication")
	require.Equal(t, 0, seam.count(), "nothing can have been drained yet")

	holds.releaseAll()
	late := seam.await(t, "the drain to reach the held publication")
	require.NotNil(t, late.framer, "the held iterator must still carry its response framer")
	seam.releaseAll()

	awaitDrainGoroutines(t, baseline, "the drain to finish")
	awaitRunnersExited(t, stages, 2)
	require.Equal(t, 1, seam.count(), "the one outstanding iterator was processed")
}

// TestSpeculative_LateUnattemptedRetirementIsDrained proves the drain consumes a
// retirement that arrives after coordinate returned with the winner, and closes the
// iterator it carries.
//
// It is the retirement arm of the drain's select, and the shape is what makes the arm
// attributable: every message left outstanding is a retirement, so the drain only ends if
// it took that message off the second channel.
// The counts say who took it. testAfterConsume runs on the coordinate goroutine and the
// drain deliberately does not report there, so a consumed count that stays where it stood
// when the query returned means the drain, and not coordinate, consumed the late
// retirement.
//
// The iterator a retirement with no attempt behind it carries has no framer, so the only
// evidence Close ran on it is its closed flag; the drain seam supplies the pointer to read
// it on.
//
// One host and three runners is the only shape that leaves a pure unattempted retirement
// outstanding: the shared selector hands its single host to the first runner and reports
// exhaustion to the other two, and coordinate consumes one of those retirements before the
// winner.
func TestSpeculative_LateUnattemptedRetirementIsDrained(t *testing.T) {
	baseline := drainGoroutines()
	harness := newFillHarness(t, 1, nil)

	seam := newDrainSeam()
	seam.releaseAll()
	harness.session.executor.testBeforeDrainClose = seam.hook
	consumes := newConsumeRecorder()
	harness.session.executor.testAfterConsume = consumes.hook

	stages := newRunStageRecorder()
	entered := stages.gate(runEntered)
	t.Cleanup(releaseStageGate(entered))
	// The retirement gate is the hold for a report: the checkpoint sits immediately
	// before the send, so a runner parked there has not published.
	reported := stages.gate(runRetired)
	t.Cleanup(releaseStageGate(reported))
	holds := newPublicationHolds(1)
	t.Cleanup(holds.releaseAll)
	harness.session.executor.testRunHook = holds.wrap(stages.hook)

	qry := harness.session.Query("void").WithContext(t.Context())
	speculative(2, speculativeTick)(qry)
	result := execAsync(qry)

	// The first runner takes the only host and holds its result; the other two find the
	// shared enumeration spent and hold their retirements.
	stages.await(t, runEntered, "the main runner to start")
	entered <- struct{}{}
	stages.await(t, runResult, "the main runner to reach publication")
	for i := range 2 {
		stages.await(t, runEntered, fmt.Sprintf("speculative runner %d to start", i+1))
		entered <- struct{}{}
		stages.await(t, runRetired, fmt.Sprintf("speculative runner %d to reach its retirement", i+1))
	}

	// One retirement is released and counted before the winner, so the history coordinate
	// leaves behind is a consumed retirement followed by a consumed result - and one
	// retirement still unsent.
	// The consumed one does not end the query: two runners are still launched and
	// unpublished, which is the sibling protection this shape also pins.
	reported <- struct{}{}
	consumes.await(t, 1, "coordinate to consume the first retirement")

	holds.releaseAll()
	require.NoError(t, awaitQuery(t, result), "the main runner's result wins")

	require.Equal(t, 3, stages.count(runEntered), "three runners were launched")
	require.Equal(t, 2, consumes.count(), "coordinate consumed the first retirement and the winner")

	// launched 3 - consumed 2 = 1 outstanding, and it is a retirement.
	awaitDrainGoroutines(t, baseline+1, "the drain to wait for the held retirement")

	reported <- struct{}{}
	awaitDrainGoroutines(t, baseline, "the drain to consume the late retirement and finish")

	awaitRunnersExited(t, stages, 3)
	require.Equal(t, 1, seam.count(), "the drain reached the late retirement's iterator")
	require.Equal(t, 2, consumes.count(), "the drain, not coordinate, took the late retirement")

	drained := seam.await(t, "the drained retirement iterator")
	requireSameError(t, ErrNoConnections, drained.err,
		"a retirement with no attempt behind it carries ErrNoConnections")
	require.Nil(t, drained.framer, "an unattempted retirement never held a response framer")
	require.Equal(t, int32(1), atomic.LoadInt32(&drained.closed), "the drain must have closed it")
}

// retiredSeam observes queryExecutor.testBeforeRetiredClose.
//
// It records what a coordinator-side close is about to reclaim - the iterator, its host,
// its framer and that framer's read buffer capacity - because every one of those is gone
// once Close has run.
// Unlike drainSeam it does not park: the closes it watches happen on coordinate's own
// goroutine, which the caller of the query is waiting on, so parking there would deadlock
// the test rather than order it.
type retiredSeam struct {
	mu sync.Mutex
	// seen is one record per close, in the order coordinate made them.
	seen []retiredRecord
}

// retiredRecord is what the seam captured for one iterator.
type retiredRecord struct {
	iter   *Iter
	host   *HostInfo
	framer *framer
	// bufCap is cap(framer.readBuffer) before the close, or 0 without a framer.
	bufCap int
}

// newRetiredSeam returns a seam ready to be installed as testBeforeRetiredClose.
//
// Returns:
//   - *retiredSeam: the seam
func newRetiredSeam() *retiredSeam {
	return &retiredSeam{}
}

// hook is the queryExecutor.testBeforeRetiredClose.
func (s *retiredSeam) hook(iter *Iter) {
	record := retiredRecord{iter: iter, host: iter.Host(), framer: iter.framer}
	if record.framer != nil {
		record.bufCap = cap(record.framer.readBuffer)
	}

	s.mu.Lock()
	s.seen = append(s.seen, record)
	s.mu.Unlock()
}

// records returns what the seam captured, in order.
//
// Returns:
//   - []retiredRecord: the records
func (s *retiredSeam) records() []retiredRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]retiredRecord(nil), s.seen...)
}

// count returns how many iterators coordinate closed.
//
// Returns:
//   - int: the number of closes
func (s *retiredSeam) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.seen)
}

// iters returns the iterators the seam saw, and fails when one appears twice.
//
// A repeat would mean an iterator was closed by two different paths, which is exactly what
// the ownership rule forbids.
//
// Returns:
//   - []*Iter: the iterators, in order
func (s *retiredSeam) iters(t *testing.T) []*Iter {
	t.Helper()

	records := s.records()
	iters := make([]*Iter, 0, len(records))
	seen := make(map[*Iter]bool, len(records))
	for _, record := range records {
		require.False(t, seen[record.iter], "an iterator reached the coordinator's close twice")
		seen[record.iter] = true
		iters = append(iters, record.iter)
	}
	return iters
}

// requireClosed asserts every iterator the seam saw went through Iter.Close and let go of
// its framer.
func (s *retiredSeam) requireClosed(t *testing.T) {
	t.Helper()

	for i, record := range s.records() {
		require.Equal(t, int32(1), atomic.LoadInt32(&record.iter.closed),
			"the coordinator's close must have run on retired iterator %d", i)
		require.Nil(t, record.iter.framer, "retired iterator %d must have let go of its framer", i)
	}
}
