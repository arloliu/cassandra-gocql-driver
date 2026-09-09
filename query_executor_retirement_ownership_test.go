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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// siblingOutcome describes how the second runner ends, and who owns its iterator.
type siblingOutcome struct {
	name string
	// answers is the retry script; the first round is the main runner's retirement.
	answers []retryAnswer
	// succeeds leaves the sibling's host answering normally, so its attempt succeeds.
	succeeds bool
	// panicMark makes the sibling's host Mark panic instead of a policy callback.
	panicMark bool
	// panicObserve makes the sibling's attempt observation panic.
	panicObserve bool
	// deliversAttemptIter says the caller receives the sibling's own attempt iterator.
	deliversAttemptIter bool
	// wantErr, when set, is the error the caller's iterator must report, by identity.
	wantErr error
	// wantErrMsg, when set, is the message the caller's iterator's error must carry.
	//
	// It is for the errors no package-level value names:
	// the server error the sibling's host answers with,
	// and the error run builds out of a recovered callback panic.
	wantErrMsg string
}

// panicIterErr is the message run's error iterator carries for a callback panic.
//
// Asserting on it is what tells "the caller was handed the panic" apart from
// "the caller was handed some fresh error iterator":
// the recovery formats the panic value into the message,
// and run builds its error iterator out of exactly that.
//
// Parameters:
//   - value: the value the callback panicked with
//
// Returns:
//   - string: the message the caller's iterator must report
func panicIterErr(value any) string {
	return fmt.Sprintf("gocql: panic in goroutine queryExecutor.run: %v", value)
}

// TestSpeculative_HeldRetirementSurvivesSiblingOutcomes proves the retirement coordinate
// is holding is reclaimed exactly once however the sibling that ends the query ends.
//
// Each case puts the sibling's own attempt iterator in a different pair of hands - the
// caller's, do's unknown-retry-type branch, do's cleanup defer, attemptQuery's cleanup
// defer - and none of them may touch the held retirement, which stays coordinate's.
func TestSpeculative_HeldRetirementSurvivesSiblingOutcomes(t *testing.T) {
	retire := retryAnswer{attempt: true, retryType: RetryNextHost}
	panicValue := callbackPanic{where: "sibling outcome"}

	cases := []siblingOutcome{
		{
			name:                "success",
			answers:             []retryAnswer{retire},
			succeeds:            true,
			deliversAttemptIter: true,
		},
		{
			// Ignore clears the attempt's error on the attempt's own iterator, so
			// the caller receives that iterator reporting nothing at all.
			name:                "ignore",
			answers:             []retryAnswer{retire, {attempt: true, retryType: Ignore}},
			deliversAttemptIter: true,
		},
		{
			// Rethrow hands the same iterator over with the server's error intact,
			// which is the one thing that tells it apart from Ignore.
			name:                "rethrow",
			answers:             []retryAnswer{retire, {attempt: true, retryType: Rethrow}},
			deliversAttemptIter: true,
			wantErrMsg:          retirementErrMsg(1),
		},
		{
			name:    "unknown retry type",
			answers: []retryAnswer{retire, {attempt: true, retryType: RetryType(255)}},
			wantErr: ErrUnknownRetryType,
		},
		{
			name:       "attempt panics",
			answers:    []retryAnswer{retire, {panicInAttempt: panicValue}},
			wantErrMsg: panicIterErr(panicValue),
		},
		{
			name:       "get retry type panics",
			answers:    []retryAnswer{retire, {attempt: true, panicInGetRetryType: panicValue}},
			wantErrMsg: panicIterErr(panicValue),
		},
		{
			name:       "mark panics",
			answers:    []retryAnswer{retire},
			panicMark:  true,
			wantErrMsg: panicIterErr(panicValue),
		},
		{
			name:         "observer panics",
			answers:      []retryAnswer{retire},
			panicObserve: true,
			wantErrMsg:   panicIterErr(panicValue),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runSiblingOutcome(t, tc)
		})
	}
}

// runSiblingOutcome drives one ownership case.
//
// The schedule is fixed:
// the main runner retires on the first host and is held,
// the second runner ends the query on the second host,
// and the third is still in flight,
// so exactly one message is left for the drain.
//
// Parameters:
//   - t: the test
//   - tc: the sibling outcome under test and the ownership it must produce
func runSiblingOutcome(t *testing.T, tc siblingOutcome) {
	t.Helper()

	failures := []retirementFailure{{host: 0, warnings: []string{"w-first"}, oversized: true}}
	if !tc.succeeds {
		failures = append(failures, retirementFailure{host: 1, warnings: []string{"w-second"}})
	}
	fixture := newRetirementFixture(t, retirementOpts{
		hosts:     3,
		holdsAt:   runRetired,
		park:      []int{1},
		failures:  failures,
		parkDrain: true,
	})
	first, second, third := fixture.harness.hosts[0], fixture.harness.hosts[1], fixture.harness.hosts[2]
	fixture.armAll()

	policy := &scriptedIterPolicy{
		HostSelectionPolicy: fixture.harness.session.executor.policy,
		script:              [][]*HostInfo{{first, second, third}},
	}
	if tc.panicMark {
		policy.wrap = panicOnMarkFor(second, callbackPanic{where: "sibling outcome"})
	}
	fixture.harness.session.executor.policy = policy

	var observer QueryObserver = fixture.attempts
	if tc.panicObserve {
		observer = hostPanicObserver{ip: hostIP(second), value: callbackPanic{where: "sibling outcome"}}
	}

	// The request is wrapped so the iterator each attempt produced can be named; the
	// wrapper survives the per-runner snapshot, so every runner's attempts are recorded.
	capture := &iterCapture{}
	pub := fixture.harness.session.Query(retirementStmt).WithContext(t.Context()).
		RetryPolicy(&scriptedRetryPolicy{answers: tc.answers}).Observer(observer)
	speculative(2, speculativeTick)(pub)
	request := &iterCapturingRequest{
		internalRequest: newInternalQuery(pub, t.Context()),
		capture:         capture,
	}

	result := make(chan *Iter, 1)
	go func() {
		iter, err := fixture.harness.session.executor.executeQuery(request)
		require.NoError(t, err, "the executor must not refuse this query")
		result <- iter
	}()

	fixture.startAt(t, first, "the main runner")
	fixture.startAt(t, second, "the second runner")
	fixture.startAt(t, third, "the third runner")

	fixture.gate.release(hostIP(first))
	fixture.retireAt(t, 1, 1, "the main runner's retirement")
	requireStillRunning(t, result)

	fixture.gate.release(hostIP(second))
	got := awaitIter(t, result, "the second runner's outcome to end the query")

	// The held retirement is the coordinator's, and only the coordinator closed it.
	require.Equal(t, 1, fixture.retired.count(), "the held retirement is the only coordinator-side close")
	records := fixture.retired.records()
	require.Same(t, first, records[0].host, "the retirement closed is the main runner's attempt")
	fixture.retired.requireClosed(t)
	// The third runner's response is still gated, so nothing has taken the released
	// framer back out of the pool yet.
	requireReleased(t, records[0])

	// The second runner's own attempt iterator is in exactly one pair of hands.
	attemptIter := capture.forHost(t, second)
	require.NotSame(t, attemptIter, records[0].iter, "the two iterators are distinct")
	if tc.deliversAttemptIter {
		require.Same(t, attemptIter, got, "the caller receives the attempt's own iterator")
		require.Zero(t, atomic.LoadInt32(&attemptIter.closed), "the caller's iterator must not have been closed")
	} else {
		require.NotSame(t, attemptIter, got, "the caller receives a fresh iterator")
		require.Equal(t, int32(1), atomic.LoadInt32(&attemptIter.closed),
			"the attempt's own iterator must have been closed by whoever held it")
	}
	// What the caller receives is asserted for every case, not only the named ones:
	// treating Ignore as Rethrow, or handing back a fresh success iterator after a
	// recovered panic, is invisible to the ownership assertions above.
	requireCallerError(t, tc, got.err, "the caller's iterator reports the expected error")
	requireCallerError(t, tc, got.Close(), "Close reports the same error")

	// The third runner is the one outstanding message, and it is the drain's.
	drained := fixture.drain.await(t, "the drain to reach the third runner's iterator")
	require.Same(t, third, drained.Host(), "the drained iterator is the third runner's")
	require.NotSame(t, attemptIter, drained, "the second runner's iterator is not the drain's")
	require.NotSame(t, records[0].iter, drained, "the held retirement is not the drain's")
	fixture.drain.releaseAll()
	fixture.gate.release(hostIP(third))
	awaitDrainGoroutines(t, fixture.baseline, "the drain to finish")
	require.Equal(t, 1, fixture.drain.count(), "exactly one message was left outstanding")
	awaitRunnersExited(t, fixture.stages, 3)
}

// requireCallerError asserts the error the caller's iterator reports for one case.
//
// The three shapes are the three kinds of error these outcomes produce:
// a package-level value compared by identity,
// a message-only error the server or the panic recovery built,
// and no error at all.
//
// Parameters:
//   - t: the test
//   - tc: the case being driven
//   - got: the error the caller's iterator reported
//   - msg: what the assertion is about
func requireCallerError(t *testing.T, tc siblingOutcome, got error, msg string) {
	t.Helper()

	switch {
	case tc.wantErrMsg != "":
		require.EqualError(t, got, tc.wantErrMsg, msg)
	case tc.wantErr != nil:
		requireSameError(t, tc.wantErr, got, msg)
	default:
		require.NoError(t, got, msg)
	}
}
