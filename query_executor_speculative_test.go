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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// speculativeTick is the speculative delay of the two-runner schedules: long enough for
// the main runner to draw first in practice, short enough not to matter, and every
// schedule below is correct in either draw order anyway.
const speculativeTick = 10 * time.Millisecond

// awaitRunnersExited blocks until n runners have left run, so every attempt they made is
// recorded and every host they marked is marked.
func awaitRunnersExited(t *testing.T, stages *runStageRecorder, n int) {
	t.Helper()
	for i := range n {
		stages.await(t, runExited, "runner "+string(rune('1'+i))+" to exit")
	}
}

// TestSpeculative_ConfiguredOneShotRetriesAcrossHosts proves that configuring speculative
// execution no longer disables the retry across hosts a one-shot policy gets: with the
// speculative launch an hour away, the main runner alone reaches both hosts.
func TestSpeculative_ConfiguredOneShotRetriesAcrossHosts(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	policy := installOneShotPolicy(harness)
	stages := newRunStageRecorder()
	harness.session.executor.testRunHook = stages.hook

	recorder := newAttemptRecorder()
	qry := harness.session.Query("kill").RetryPolicy(&SimpleRetryPolicy{NumRetries: 5}).Observer(recorder)
	speculative(1, time.Hour)(qry)

	iter := qry.Iter()
	require.Error(t, iter.Close(), "the test server answers kill with an error")
	awaitRunnersExited(t, stages, 1)

	require.Equal(t, 2, recorder.count(), "one attempt per up host")
	require.Len(t, recorder.hosts(), 2, "both hosts were attempted")
	require.Equal(t, int32(2), policy.picks.Load())
}

// TestSpeculative_RunnersShareOneBudget proves the selection budget is query-wide: the
// main runner holds one host with its request in flight, the speculative runner replaces
// the exhausted iterator and takes the other, and nothing is drawn beyond the two.
func TestSpeculative_RunnersShareOneBudget(t *testing.T) {
	gate := newRequestGate()
	harness := newFillHarnessOpts(t, 2, fillHarnessOpts{recvHook: gate.hook})
	t.Cleanup(gate.releaseAll)
	held, other := harness.hosts[0], harness.hosts[1]
	gate.arm(hostIP(held))

	policy := &scriptedOneShotPolicy{HostSelectionPolicy: harness.session.executor.policy, script: []*HostInfo{held, other, held, other}}
	harness.session.executor.policy = policy

	stages := newRunStageRecorder()
	entered := stages.gate(runEntered)
	t.Cleanup(releaseStageGate(entered))
	harness.session.executor.testRunHook = stages.hook

	recorder := newAttemptRecorder()
	qry := harness.session.Query("kill").WithContext(t.Context()).
		RetryPolicy(&SimpleRetryPolicy{NumRetries: 5}).Observer(recorder)
	speculative(1, speculativeTick)(qry)
	result := execAsync(qry)

	// The first runner draws the held host and parks at the server; only then may the
	// sibling draw.
	stages.await(t, runEntered, "the main runner to start")
	entered <- struct{}{}
	gate.awaitStarted(t, "the held host's request to park")
	stages.await(t, runEntered, "the speculative runner to start")
	entered <- struct{}{}

	// The sibling either attempts the other host (a replacement was drawn) or reports
	// no host (no replacement); release the held request once it has done either.
	otherAttempted := func() bool { return recorder.hosts()[other] > 0 }
	deadline := time.After(fillEventBudget)
	for !otherAttempted() {
		select {
		case <-recorder.updated:
		case <-stages.arrivals(runRetired):
			otherAttempted = func() bool { return true }
		case <-deadline:
			t.Fatalf("timed out after %v waiting for the speculative runner to attempt or give up", fillEventBudget)
		}
	}
	gate.releaseAll()

	require.Error(t, awaitQuery(t, result), "the test server answers kill with an error")
	awaitRunnersExited(t, stages, 2)

	require.Equal(t, 2, recorder.count(), "one attempt per up host across both runners")
	require.Equal(t, map[*HostInfo]int{held: 1, other: 1}, recorder.hosts())
	require.Equal(t, int32(2), policy.picks.Load(), "one replacement, drawn by the sibling")
}

// TestSpeculative_TerminalPassDoesNotReopenEnumeration proves the host a terminal pass
// consumes is charged: a sibling entering after the main runner's terminal pass finds the
// enumeration complete and draws nothing, as it does today.
func TestSpeculative_TerminalPassDoesNotReopenEnumeration(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	policy := &countingPickPolicy{HostSelectionPolicy: harness.session.executor.policy}
	harness.session.executor.policy = policy

	stages := newRunStageRecorder()
	entered := stages.gate(runEntered)
	t.Cleanup(releaseStageGate(entered))
	// Both runners retire: the main one because NumRetries: 0 refuses it a further
	// attempt, the sibling because the enumeration is already complete.
	// Holding both keeps the sibling's draw inside the window this test measures.
	holds := newPublicationHoldsAt(runRetired, 1, 2)
	t.Cleanup(holds.releaseAll)
	harness.session.executor.testRunHook = holds.wrap(stages.hook)

	recorder := newAttemptRecorder()
	qry := harness.session.Query("kill").WithContext(t.Context()).
		RetryPolicy(&SimpleRetryPolicy{NumRetries: 0}).Observer(recorder)
	speculative(1, speculativeTick)(qry)
	result := execAsync(qry)

	// The main runner attempts one host, spends its attempt budget, advances past the
	// other, and parks before publishing its retirement.
	stages.await(t, runEntered, "the main runner to start")
	entered <- struct{}{}
	stages.await(t, runRetired, "the main runner to reach publication")

	// The sibling now finds nothing to draw.
	stages.await(t, runEntered, "the speculative runner to start")
	entered <- struct{}{}
	stages.await(t, runRetired, "the speculative runner to report no host")

	// The query ends only once every launched runner has retired, so both holds go;
	// the order between them does not affect the selection accounting asserted below.
	holds.releaseAll()
	require.Error(t, awaitQuery(t, result), "the test server answers kill with an error")
	awaitRunnersExited(t, stages, 2)

	require.Equal(t, 1, recorder.count(), "the terminal pass must not buy the sibling a replacement")
	require.Equal(t, int32(1), policy.picks.Load())
}

// TestSpeculative_RetainedFillTerminalPassAdvancesOnce proves the terminal pass still
// makes its one raw iterator call after the selection was exhausted and the attempt came
// from a retained fill candidate.
func TestSpeculative_RetainedFillTerminalPassAdvancesOnce(t *testing.T) {
	harness := newFillHarness(t, 1, nil)
	pool := harness.pool(t, harness.hosts[0])
	policy := installOneShotPolicy(harness)

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

	recorder := newAttemptRecorder()
	qry := harness.session.Query("kill").WithContext(t.Context()).
		RetryPolicy(&SimpleRetryPolicy{NumRetries: 0}).Observer(recorder)
	speculative(1, time.Hour)(qry)
	result := execAsync(qry)

	awaitSignal(t, waiting, "the runner to wait for the fill")
	harness.dialer.releaseAll()

	require.Error(t, awaitQuery(t, result), "the test server answers kill with an error")
	awaitRunnersExited(t, stages, 1)

	require.Equal(t, 1, recorder.count())
	require.Equal(t, int32(1), policy.picks.Load())
	require.Equal(t, int32(3), policy.calls.Load(), "the host, the exhausting nil, and the terminal pass")
}

// TestSpeculative_EnumeratingRunnersUnchanged is the control: under an enumerating
// policy two runners take the two hosts from one iterator and no replacement is drawn.
func TestSpeculative_EnumeratingRunnersUnchanged(t *testing.T) {
	gate := newRequestGate()
	harness := newFillHarnessOpts(t, 2, fillHarnessOpts{recvHook: gate.hook})
	t.Cleanup(gate.releaseAll)
	for _, host := range harness.hosts {
		gate.arm(hostIP(host))
	}

	policy := &countingPickPolicy{HostSelectionPolicy: harness.session.executor.policy}
	harness.session.executor.policy = policy
	stages := newRunStageRecorder()
	harness.session.executor.testRunHook = stages.hook

	recorder := newAttemptRecorder()
	qry := harness.session.Query("kill").WithContext(t.Context()).
		RetryPolicy(&SimpleRetryPolicy{NumRetries: 5}).Observer(recorder)
	speculative(1, speculativeTick)(qry)
	result := execAsync(qry)

	gate.awaitStarted(t, "the first runner's request to park")
	gate.awaitStarted(t, "the second runner's request to park")
	gate.releaseAll()

	require.Error(t, awaitQuery(t, result), "the test server answers kill with an error")
	awaitRunnersExited(t, stages, 2)

	require.Equal(t, 2, recorder.count())
	require.Len(t, recorder.hosts(), 2, "each runner took its own host")
	require.Equal(t, int32(1), policy.picks.Load(), "an enumerating policy is never re-picked")
}

// TestSpeculative_NoHostExitStillPrompt proves the query still ends on the main runner's
// no-host report rather than waiting for the scheduled launch.
func TestSpeculative_NoHostExitStillPrompt(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	for _, host := range harness.hosts {
		harness.session.markHostDown(host)
	}
	policy := installOneShotPolicy(harness)
	stages := newRunStageRecorder()
	harness.session.executor.testRunHook = stages.hook

	qry := harness.session.Query("void")
	speculative(1, time.Hour)(qry)

	start := time.Now()
	iter := qry.Iter()
	require.ErrorIs(t, iter.Close(), ErrNoConnections)
	require.Less(t, time.Since(start), fillEventBudget, "the query must not wait for the scheduled launch")
	awaitRunnersExited(t, stages, 1)

	require.Equal(t, 0, iter.Attempts())
	require.Equal(t, int32(1), policy.picks.Load())
}

// TestSpeculative_WaitingRunnerSurvivesSiblingAfterReplacement proves a runner waiting
// for a fill is not ended by a sibling's no-host report, with a replacement in play: the
// main runner spent the budget on a replacement before waiting, so the sibling finds the
// selector exhausted.
func TestSpeculative_WaitingRunnerSurvivesSiblingAfterReplacement(t *testing.T) {
	harness := newFillHarness(t, 2, func(cluster *ClusterConfig) {
		cluster.Timeout = 0
		noHeartbeat(cluster)
	})
	empty, saturated := harness.hosts[0], harness.hosts[1]

	harness.dialer.arm(nil)
	detachPoolConn(t, harness.pool(t, empty))
	saturatePool(t, harness, harness.pool(t, saturated))

	policy := &scriptedOneShotPolicy{HostSelectionPolicy: harness.session.executor.policy, script: []*HostInfo{empty, saturated}}
	harness.session.executor.policy = policy
	stages := newRunStageRecorder()
	harness.session.executor.testRunHook = stages.hook

	waiting := make(chan struct{}, 1)
	harness.session.executor.testBeforeWait = func() {
		select {
		case waiting <- struct{}{}:
		default:
		}
	}

	recorder := newAttemptRecorder()
	result := harness.query(t.Context(), func(qry *Query) {
		speculative(1, speculativeTick)(qry)
		qry.RetryPolicy(nil).Observer(recorder)
	})

	awaitSignal(t, waiting, "the main runner to wait for the fill")
	stages.await(t, runRetired, "the speculative runner to report no host")
	harness.dialer.releaseAll()

	require.NoError(t, awaitQuery(t, result), "the waiting runner must still win")
	awaitRunnersExited(t, stages, 2)

	require.Equal(t, 1, recorder.count())
	require.Equal(t, map[*HostInfo]int{empty: 1}, recorder.hosts())
	require.Equal(t, int32(2), policy.picks.Load(), "the main runner drew the replacement before waiting")
}

// TestSpeculative_PinnedNeverRePicks proves a pinned query with speculation configured
// stays on its host and never asks the policy.
func TestSpeculative_PinnedNeverRePicks(t *testing.T) {
	harness := newFillHarness(t, 2, nil)
	pinned := harness.hosts[0]
	policy := installOneShotPolicy(harness)
	stages := newRunStageRecorder()
	harness.session.executor.testRunHook = stages.hook

	qry := harness.session.Query("kill").SetHostID(pinned.HostID()).RetryPolicy(&SimpleRetryPolicy{NumRetries: 5})
	speculative(1, time.Hour)(qry)

	iter := qry.Iter()
	require.Error(t, iter.Close(), "the test server answers kill with an error")
	require.Equal(t, 1, iter.Attempts())
	require.Equal(t, int32(0), policy.picks.Load())
	require.Equal(t, 0, stages.count(runEntered), "a pinned query bypasses the runners")
}
