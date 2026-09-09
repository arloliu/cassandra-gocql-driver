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

// TestSpeculative_NonPositiveDelayLaunchesImmediately proves a zero speculative delay
// does not panic the caller's goroutine and still launches the speculative runner.
func TestSpeculative_NonPositiveDelayLaunchesImmediately(t *testing.T) {
	requireImmediateSpeculativeLaunch(t, 0)
}

// TestSpeculative_NegativeDelayLaunchesImmediately proves a negative speculative delay is
// treated the same as zero: the caller does not panic and the speculative runner launches.
func TestSpeculative_NegativeDelayLaunchesImmediately(t *testing.T) {
	requireImmediateSpeculativeLaunch(t, -time.Second)
}

// requireImmediateSpeculativeLaunch runs one speculative query with the given delay and
// proves both runners started.
//
// Every runner is parked at runEntered until both have arrived, so the main runner cannot
// answer before the tick and end the coordination without a sibling:
// the assertion is about the launch, not about which runner is faster.
//
// Parameters:
//   - t: the test
//   - delay: the speculative delay under test, zero or negative
func requireImmediateSpeculativeLaunch(t *testing.T, delay time.Duration) {
	t.Helper()

	harness := newFillHarness(t, 2, nil)
	policy := installOneShotPolicy(harness)
	stages := newRunStageRecorder()
	entered := stages.gate(runEntered)
	// A failing await must not leave a runner parked: the session closes at cleanup and a
	// runner still waiting at the gate would outlive it.
	t.Cleanup(func() { close(entered) })
	harness.session.executor.testRunHook = stages.hook

	result := harness.query(t.Context(), func(qry *Query) {
		qry.RetryPolicy(&SimpleRetryPolicy{NumRetries: 5})
		speculative(1, delay)(qry)
	})

	stages.await(t, runEntered, "the main runner to start")
	stages.await(t, runEntered, "the speculative runner to start")
	entered <- struct{}{}
	entered <- struct{}{}

	require.NoError(t, awaitQuery(t, result), "the winning runner answers void")
	awaitRunnersExited(t, stages, 2)

	require.Equal(t, 2, stages.count(runEntered), "the speculative runner must launch")
	require.Equal(t, int32(2), policy.picks.Load(), "each runner drew its own host")
}
