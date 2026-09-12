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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The result set the mid-paging outage is driven through: three pages of two
// rows, so there is a page before the outage, a page that has to survive it, and
// a page after it that proves the iterator kept going.
const (
	pagingRecoveryPages = 3
	pagingRecoveryRows  = 2
)

// errPagingRecoveryUnreachable is the dial verdict that makes the fixture host
// unreachable, so a refill cycle fails and the host is convicted.
var errPagingRecoveryUnreachable = errors.New("gocql: paging recovery fixture host is unreachable")

// pagingScan is the outcome of scanning the whole paged result set.
type pagingScan struct {
	// rows is every value scanned, in the order the iterator produced it.
	rows []int
	// err is the iterator's terminal error.
	err error
}

// pagingRecoveryFixture is a paged read parked between its first and second
// page, with the coordinator's connection under test control.
//
// The parking point is the production connTestHooks.beforeNextPage seam, which
// runs once the first page's response has been parsed and before the request for
// the next page is built.
// Everything a test does from there lands strictly between two pages of the same
// logical read, which is the window this fixture exists to open.
type pagingRecoveryFixture struct {
	harness *fillHarness
	script  *pagingScript
	gate    *nextPageGate
	// host is the single fixture host, the coordinator of every page.
	host *HostInfo
	pool *hostConnPool
	// scanned receives the whole scan once the iterator is exhausted.
	scanned <-chan pagingScan
}

// newPagingRecoveryFixture starts a paged read and parks it between pages.
//
// The read is idempotent and carries a retry policy, the shape a downstream
// caller that pages every read has: without those, a page whose coordinator
// disappeared is not retried at all.
//
// Parameters:
//   - t: the test; the harness, servers and session are registered for cleanup
//   - tune: optional cluster tweaks applied on top of the fixture's own
//
// Returns:
//   - *pagingRecoveryFixture: the fixture, parked between page 1 and page 2
func newPagingRecoveryFixture(t *testing.T, tune func(*ClusterConfig)) *pagingRecoveryFixture {
	t.Helper()

	script := newPagingScript(pagingRecoveryPages, pagingRecoveryRows)
	t.Cleanup(script.releaseAll)
	gate := newNextPageGate()
	t.Cleanup(gate.releaseAll)

	harness := newFillHarnessOpts(t, 1, fillHarnessOpts{
		respHook: script.hook,
		hooks:    gate.hooks(),
		tune: func(cluster *ClusterConfig) {
			noHeartbeat(cluster)
			// The page that survives the outage waits for the pool to refill,
			// so the request timeout has to outlast a reconnect.
			cluster.Timeout = 5 * time.Second
			if tune != nil {
				tune(cluster)
			}
		},
	})
	host := harness.hosts[0]

	scanned := make(chan pagingScan, 1)
	query := harness.session.Query(pagingStmt).WithContext(t.Context()).
		PageSize(pagingRecoveryRows).Idempotent(true).
		RetryPolicy(&SimpleRetryPolicy{NumRetries: 5})
	go func() {
		iter := query.Iter()
		var (
			rows  []int
			value int
		)
		for iter.Scan(&value) {
			rows = append(rows, value)
		}
		scanned <- pagingScan{rows: rows, err: iter.Close()}
	}()

	fixture := &pagingRecoveryFixture{
		harness: harness,
		script:  script,
		gate:    gate,
		host:    host,
		pool:    harness.pool(t, host),
		scanned: scanned,
	}

	// From here the first page is parsed and the next page's request has not
	// been built, so the outage each schedule drives lands between them.
	gate.awaitReached(t)
	return fixture
}

// forgetAndResume makes the server reject the prepared id the driver still holds
// and then lets the parked read continue.
//
// Forgetting before the release is what puts the re-prepare inside the same
// window as the reconnect: the driver's next request for page two is the first
// one the restarted server sees.
func (f *pagingRecoveryFixture) forgetAndResume() {
	f.script.forgetPrepared()
	f.gate.releaseAll()
}

// awaitScan returns the whole scan, failing the test if the read never finishes.
//
// Returns:
//   - pagingScan: every row the iterator produced, and its terminal error
func (f *pagingRecoveryFixture) awaitScan(t *testing.T) pagingScan {
	t.Helper()

	select {
	case scan := <-f.scanned:
		return scan
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for the paged read to finish", fillEventBudget)
		return pagingScan{}
	}
}

// requireResumedAcrossReprepare asserts the whole composition: the read survived
// the outage, re-prepared, and resumed on the page it was on.
//
// The three assertions are deliberately separate.
// The row sequence proves the result set is intact from the caller's side;
// the paging-state comparison proves why, by pinning the state the re-executed
// request carried;
// and the sequencing proves a fresh PREPARE is what stood between the rejection
// and the success, rather than the server having changed its mind.
func requireResumedAcrossReprepare(t *testing.T, f *pagingRecoveryFixture) {
	t.Helper()

	scan := f.awaitScan(t)
	require.NoError(t, scan.err, "a paged read whose coordinator reconnected must still complete")

	want := make([]int, 0, pagingRecoveryPages*pagingRecoveryRows)
	for row := range pagingRecoveryPages * pagingRecoveryRows {
		want = append(want, row)
	}
	require.Equal(t, want, scan.rows,
		"every row must arrive exactly once and in order: a lost paging state repeats a page, a skipped one drops it")

	// The second page is the one the outage interrupted, so it is the only page
	// the fixture answered twice.
	var second []pagingRequest
	for _, req := range f.script.seen() {
		if req.page == 2 {
			second = append(second, req)
		}
	}
	require.Len(t, second, 2,
		"page two must be asked for exactly twice: once into the rejection, once after the re-prepare")
	require.Equal(t, second[0].pagingState, second[1].pagingState,
		"the request sent after the re-prepare must resume from the state the rejected one carried")
	require.Equal(t, pagingStateFor(2), second[1].pagingState,
		"and that state must still be page two's, not the start of the result set")

	prepares := f.script.seenPrepares()
	require.Len(t, prepares, 2,
		"exactly two PREPAREs: the fixture's own, and the one the rejected id forced")
	require.Greater(t, prepares[1], second[0].seq,
		"the re-prepare must follow the rejected request, not precede it")
	require.Less(t, prepares[1], second[1].seq,
		"and it must precede the request that finally served page two")
}

// TestPaging_SurvivesReconnectAndReprepareBetweenPages proves a paged read whose
// coordinator's connection is replaced between two pages resumes on the page it
// was on, even when the node it comes back as has forgotten the prepared
// statement.
//
// This composes three things the suite otherwise only covers separately: an
// automatic paging sequence in progress, a coordinator connection lost and
// re-established, and a prepared id the server rejects.
// It is the shape a caller that pages every read and reconnects quickly actually
// meets, and the failure it guards is silent - a lost paging state does not
// error, it repeats or drops rows.
//
// The two schedules are the two ways a pool loses and regains a connection.
// In the first the refill succeeds, so the host is never convicted and its pool
// keeps its registration.
// In the second the refill fails first, so the host is convicted DOWN and its
// pool unregistered before a later cycle brings both back - a strictly longer
// path to the same resumed read.
func TestPaging_SurvivesReconnectAndReprepareBetweenPages(t *testing.T) {
	t.Run("the connection is replaced without convicting the host", func(t *testing.T) {
		f := newPagingRecoveryFixture(t, nil)

		// Park the refill in the dialer first, so the close below cannot be
		// answered before the fixture has observed that it started.
		f.harness.dialer.arm(nil)
		conn := f.pool.Pick()
		require.NotNil(t, conn, "the fixture pool must serve a connection to lose")
		conn.closeWithError(errFillTestConnClosed)
		f.harness.dialer.awaitStarted(t)

		f.harness.dialer.releaseAll()
		f.harness.events.awaitEachHost(t, poolFillDone, []*HostInfo{f.host},
			"the pool to release the claim of the refill that replaced the connection")
		require.Equal(t, NodeUp, f.host.State(),
			"a refill that succeeded must leave the host UP, so this schedule never reaches conviction")

		f.forgetAndResume()
		requireResumedAcrossReprepare(t, f)
	})

	t.Run("the host is convicted and recovers", func(t *testing.T) {
		f := newPagingRecoveryFixture(t, func(cluster *ClusterConfig) {
			// A convicted host is only reconsidered by the reconnect path, which
			// does not exist at all while the interval is zero.
			cluster.ReconnectInterval = time.Millisecond
		})

		// Every dial fails from here, so the refill this close triggers cannot
		// succeed and the cycle ends with an empty pool.
		f.harness.dialer.arm(func(string) error { return errPagingRecoveryUnreachable })
		f.harness.dialer.releaseAll()
		conn := f.pool.Pick()
		require.NotNil(t, conn, "the fixture pool must serve a connection to lose")
		conn.closeWithError(errFillTestConnClosed)

		awaitHost(t, f.harness.collector.down, f.host, "the host to be convicted by the failed refill")
		// The DOWN transition is the only conviction fact this schedule can
		// assert here. The policy event is published before markHostDown
		// unregisters the pool, and the millisecond reconnect above is already
		// registering pools of its own, so the registration is not a stable
		// observation at this point - pause_recovery_test.go and
		// tickFixture.driveDown pin the unregistration where it is.
		require.Equal(t, NodeDown, f.host.State(), "a fill cycle that ends empty must mark the host DOWN")

		// Reachable again, so the next reconnect cycle refills it.
		f.harness.dialer.setErr(nil)
		awaitHost(t, f.harness.collector.up, f.host, "the host to come back UP through the reconnect path")

		f.forgetAndResume()
		requireResumedAcrossReprepare(t, f)
	})
}
