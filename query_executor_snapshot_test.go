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
	"encoding/binary"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// pagingStmt is the statement the paging fixture answers.
//
// It starts with SELECT, so the session prepares it and the routing metadata a
// prepared response carries is set on the request; Conn.query forces skipPrepare
// and reaches the same fixture through a QUERY frame instead.
const pagingStmt = "SELECT pages FROM snapshot_t"

// pagingPreparedID is the prepared-statement id the paging fixture hands out.
// It is deliberately none of the ids the default test server knows.
const pagingPreparedID uint64 = 0x5041

// pagingKeyspace and pagingTable are the routing metadata the prepared response
// carries, so a test can see whether the routing info survived a copy.
const (
	pagingKeyspace = "snapshot_ks"
	pagingTable    = "snapshot_tbl"
)

// pagingStateFor returns the paging state that resumes at page.
//
// Returns:
//   - []byte: the opaque state, empty for the first page
func pagingStateFor(page int) []byte {
	if page <= 1 {
		return nil
	}
	return []byte(fmt.Sprintf("page-%d", page))
}

// pageForState returns the 1-based page a paging state resumes at.
//
// Returns:
//   - int: the page number
//   - bool: false when state is not one this fixture issued
func pageForState(state []byte) (int, bool) {
	if len(state) == 0 {
		return 1, true
	}
	var page int
	if _, err := fmt.Sscanf(string(state), "page-%d", &page); err != nil {
		return 0, false
	}
	return page, true
}

// requestHolds parks selected requests at the test servers, one hold per server.
//
// Each server has its own release channel, so a schedule can keep one runner in
// flight while it lets another one finish. A parked request is a request in
// flight from the driver's point of view: the connection answers nothing else
// until the hold is released.
type requestHolds struct {
	mu    sync.Mutex
	holds map[string]*requestHold
}

// requestHold is one server's hold.
type requestHold struct {
	// from is the request ordinal from which requests park; 0 parks nothing.
	from int
	// seen counts the requests this server answered.
	seen int
	// parked receives one token per parked request, before it parks.
	parked chan struct{}
	// released is closed to let every parked and future request through.
	released chan struct{}
	once     sync.Once
}

// newRequestHolds returns holds with no server held.
//
// Returns:
//   - *requestHolds: the holds
func newRequestHolds() *requestHolds {
	return &requestHolds{holds: map[string]*requestHold{}}
}

// holdHandle returns the hold for ip, creating it on first use.
//
// The caller must hold h.mu.
//
// Returns:
//   - *requestHold: the server's hold
func (h *requestHolds) holdLocked(ip string) *requestHold {
	hold, ok := h.holds[ip]
	if !ok {
		hold = &requestHold{parked: make(chan struct{}, 64), released: make(chan struct{})}
		h.holds[ip] = hold
	}
	return hold
}

// holdFrom parks the nth request the server at ip answers, and every later one,
// until release or releaseAll.
//
// Parameters:
//   - ip: the server's connect address without the port
//   - n: the 1-based request ordinal from which to park; 0 parks nothing
func (h *requestHolds) holdFrom(ip string, n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.holdLocked(ip).from = n
}

// enter counts one request at ip and parks it when the hold says so.
//
// It runs on the server's own goroutine, so it must be called after the request
// was recorded: a test waiting for the request has to see it before it parks.
func (h *requestHolds) enter(ip string) {
	h.mu.Lock()
	hold := h.holdLocked(ip)
	hold.seen++
	park := hold.from > 0 && hold.seen >= hold.from
	h.mu.Unlock()

	if !park {
		return
	}

	select {
	case hold.parked <- struct{}{}:
	default:
	}
	<-hold.released
}

// awaitParked blocks until one request parked at ip.
func (h *requestHolds) awaitParked(t *testing.T, ip, what string) {
	t.Helper()

	h.mu.Lock()
	hold := h.holdLocked(ip)
	h.mu.Unlock()

	awaitSignal(t, hold.parked, what)
}

// release lets every parked and future request at ip through.
func (h *requestHolds) release(ip string) {
	h.mu.Lock()
	hold := h.holdLocked(ip)
	h.mu.Unlock()

	hold.once.Do(func() { close(hold.released) })
}

// releaseAll lets every parked and future request through, at every server.
func (h *requestHolds) releaseAll() {
	h.mu.Lock()
	holds := make([]*requestHold, 0, len(h.holds))
	for _, hold := range h.holds {
		holds = append(holds, hold)
	}
	h.mu.Unlock()

	for _, hold := range holds {
		hold.once.Do(func() { close(hold.released) })
	}
}

// pagingRequest is one request the paging fixture answered.
type pagingRequest struct {
	// ip is the connect address of the server that answered, without the port.
	ip string
	// op is the request's opcode.
	op frameOp
	// consistency is the consistency the request carried.
	consistency Consistency
	// pagingState is the state the request resumed from, empty on the first page.
	pagingState []byte
	// page is the 1-based page the request asked for.
	page int
}

// pagingScript is a test server response hook that serves one statement as a
// multi-page result set and records every request it answered.
//
// The default test server has no paged response, and the next page is built
// inside the connection execution, so every test that needs to watch that copy
// needs a server that reports more pages.
type pagingScript struct {
	// pages is how many pages the result set has.
	pages int
	// rows is how many rows every page carries.
	rows int
	// errorsBefore is how many requests are answered with a server error before
	// the first page is served, so a test can drive the retry path.
	// It is atomic: the servers read it on their own goroutines.
	errorsBefore atomic.Int32

	// errors counts the errors already answered.
	errors atomic.Int32

	// holds parks selected requests, per server.
	*requestHolds

	mu       sync.Mutex
	requests []pagingRequest
	// failIPs answers every request reaching one of these servers with a server
	// error, so a test can decide which runner loses.
	failIPs map[string]bool

	// updated is signalled after every recorded request.
	updated chan struct{}
}

// newPagingScript returns a script serving pages pages of rows rows each.
//
// Returns:
//   - *pagingScript: the script, ready to be installed as fillHarnessOpts.respHook
func newPagingScript(pages, rows int) *pagingScript {
	return &pagingScript{
		pages:        pages,
		rows:         rows,
		requestHolds: newRequestHolds(),
		failIPs:      map[string]bool{},
		updated:      make(chan struct{}, 64),
	}
}

// failHost answers every request reaching the server at ip with a server error.
func (s *pagingScript) failHost(ip string) {
	s.mu.Lock()
	s.failIPs[ip] = true
	s.mu.Unlock()
}

// hook is the fillHarnessOpts.respHook.
//
// It claims only the fixture's own statement and leaves every other request,
// connection startup included, to the server's default handling.
//
// Returns:
//   - bool: true when it wrote the response itself
func (s *pagingScript) hook(ip string, srv *TestServer, req, resp *framer) bool {
	head := req.header
	if head == nil {
		return false
	}

	saved := req.buf
	switch head.op {
	case opPrepare:
		stmt, err := req.readLongString()
		req.buf = saved
		if err != nil || stmt != pagingStmt {
			return false
		}
		s.writePrepared(resp, head)
		return true
	case opQuery:
		stmt, err := req.readLongString()
		if err != nil || stmt != pagingStmt {
			req.buf = saved
			return false
		}
	case opExecute:
		id, err := req.readShortBytes()
		if err != nil || len(id) != 8 || binary.BigEndian.Uint64(id) != pagingPreparedID {
			req.buf = saved
			return false
		}
	default:
		return false
	}

	cons, state, ok := s.readParams(req, srv)
	if !ok {
		req.buf = saved
		return false
	}
	page, ok := pageForState(state)
	if !ok {
		req.buf = saved
		return false
	}

	s.mu.Lock()
	fail := s.failIPs[ip]
	s.mu.Unlock()

	s.record(pagingRequest{ip: ip, op: head.op, consistency: cons, pagingState: state, page: page})
	s.enter(ip)

	if failBefore := s.errorsBefore.Load(); fail || (s.errors.Load() < failBefore && s.errors.Add(1) <= failBefore) {
		writeReadTimeoutError(resp, head)
		return true
	}

	s.writeRows(resp, head, page)
	return true
}

// readParams walks the query parameters far enough to reach the paging state.
//
// Returns:
//   - Consistency: the consistency the request carried
//   - []byte: the paging state, nil when the request carried none
//   - bool: false when the frame did not parse
func (s *pagingScript) readParams(f *framer, srv *TestServer) (Consistency, []byte, bool) {
	cons, err := f.readConsistency()
	if err != nil {
		return 0, nil, false
	}

	var flags uint32
	if srv.protocol > protoVersion4 {
		i, err := f.readInt()
		if err != nil {
			return 0, nil, false
		}
		flags = uint32(i)
	} else {
		b, err := f.readByte()
		if err != nil {
			return 0, nil, false
		}
		flags = uint32(b)
	}

	if flags&flagValues != 0 {
		n, err := f.readShort()
		if err != nil {
			return 0, nil, false
		}
		for range int(n) {
			if flags&flagWithNameValues != 0 {
				if _, err := f.readString(); err != nil {
					return 0, nil, false
				}
			}
			size, err := f.readInt()
			if err != nil {
				return 0, nil, false
			}
			if size > 0 {
				if len(f.buf) < size {
					return 0, nil, false
				}
				f.buf = f.buf[size:]
			}
		}
	}
	if flags&flagPageSize != 0 {
		if _, err := f.readInt(); err != nil {
			return 0, nil, false
		}
	}
	var state []byte
	if flags&flagWithPagingState != 0 {
		b, err := f.readBytes()
		if err != nil {
			return 0, nil, false
		}
		state = copyBytes(b)
	}

	return cons, state, true
}

// writePrepared answers a PREPARE with the fixture's id and routing metadata.
func (s *pagingScript) writePrepared(resp *framer, head *frameHeader) {
	writeTracedResultHeader(resp, head)
	resp.writeInt(resultKindPrepared)
	resp.writeShortBytes(binary.BigEndian.AppendUint64(nil, pagingPreparedID))
	// <metadata> of the bind markers: none, but with the global table spec the
	// driver copies into the request's routing info.
	resp.writeInt(int32(flagGlobalTableSpec))
	resp.writeInt(0)
	if head.version.version() >= protoVersion4 {
		resp.writeInt(0)
	}
	resp.writeString(pagingKeyspace)
	resp.writeString(pagingTable)
	// <result_metadata>: one column, with the global table spec the driver
	// copies into the request's routing info.
	resp.writeInt(int32(flagGlobalTableSpec))
	resp.writeInt(1)
	resp.writeString(pagingKeyspace)
	resp.writeString(pagingTable)
	resp.writeString("col0")
	resp.writeShort(uint16(TypeInt))
}

// writeRows answers with one page of the result set.
func (s *pagingScript) writeRows(resp *framer, head *frameHeader, page int) {
	resp.writeHeader(0, opResult, head.stream)
	resp.writeInt(resultKindRows)

	flags := flagGlobalTableSpec
	if page < s.pages {
		flags |= flagHasMorePages
	}
	resp.writeInt(int32(flags))
	resp.writeInt(1)
	if page < s.pages {
		resp.writeBytes(pagingStateFor(page + 1))
	}
	resp.writeString(pagingKeyspace)
	resp.writeString(pagingTable)
	resp.writeString("col0")
	resp.writeShort(uint16(TypeInt))

	resp.writeInt(int32(s.rows))
	for row := range s.rows {
		resp.writeBytes(binary.BigEndian.AppendUint32(nil, uint32((page-1)*s.rows+row)))
	}
}

// writeReadTimeoutError answers with a read timeout.
//
// DowngradingConsistencyRetryPolicy classifies it as Retry, so the runner that
// receives it downgrades its own request and tries the same host again instead of
// consuming another host from the shared selector.
func writeReadTimeoutError(resp *framer, head *frameHeader) {
	resp.writeHeader(0, opError, head.stream)
	resp.writeInt(ErrCodeReadTimeout)
	resp.writeString("snapshot fixture read timeout")
	resp.writeConsistency(Quorum)
	resp.writeInt(0)
	resp.writeInt(2)
	resp.writeByte(0)
}

// record appends req and signals the waiters.
func (s *pagingScript) record(req pagingRequest) {
	s.mu.Lock()
	s.requests = append(s.requests, req)
	s.mu.Unlock()

	select {
	case s.updated <- struct{}{}:
	default:
	}
}

// seen returns every request answered so far.
//
// Returns:
//   - []pagingRequest: a copy of the recorded requests, in arrival order
func (s *pagingScript) seen() []pagingRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]pagingRequest(nil), s.requests...)
}

// pages returns the page number of every request answered so far.
//
// Returns:
//   - []int: the pages, in arrival order
func (s *pagingScript) seenPages() []int {
	seen := s.seen()
	pages := make([]int, len(seen))
	for i, req := range seen {
		pages[i] = req.page
	}
	return pages
}

// await blocks until at least n requests were answered.
func (s *pagingScript) await(t *testing.T, n int) {
	t.Helper()

	deadline := time.After(fillEventBudget)
	for len(s.seen()) < n {
		select {
		case <-s.updated:
		case <-deadline:
			t.Fatalf("timed out after %v waiting for %d fixture requests, saw %d",
				fillEventBudget, n, len(s.seen()))
		}
	}
}

// scanAll drains iter and returns the values it yielded.
//
// The row count is capped: a request that lost its paging state resumes at the
// first page forever, so an uncapped drain would hang instead of failing.
//
// Returns:
//   - []int: every value scanned, in order
func scanAll(t *testing.T, iter *Iter, want int) []int {
	t.Helper()

	var (
		got []int
		v   int
	)
	for iter.Scan(&v) {
		got = append(got, v)
		if len(got) > want {
			require.NoError(t, iter.Close())
			t.Fatalf("the fixture yielded more than %d rows: a page must be resuming from the wrong state", want)
		}
	}
	require.NoError(t, iter.Close(), "the fixture's pages must all be readable")
	return got
}

// pagingQuery returns a query against the paging fixture.
//
// Returns:
//   - *Query: the query, with the fixture's page size and no retry policy
func pagingQuery(t *testing.T, h *fillHarness) *Query {
	t.Helper()

	return h.session.Query(pagingStmt).WithContext(boundedContext(t)).
		Consistency(Quorum).PageSize(4).Idempotent(true)
}

// boundedContext returns a context that expires well before the suite's timeout.
//
// Several of the field-drop mutations leave a runner unable to publish anything
// at all, so without a deadline they hang instead of failing.
//
// Returns:
//   - context.Context: cancelled after fillEventBudget or when the test ends
func boundedContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), fillEventBudget)
	t.Cleanup(cancel)
	return ctx
}

// retainingRetryPolicy keeps the request it was handed.
//
// RetryableQuery's godoc places no lifetime restriction on the argument, so a
// RetryPolicy retaining it and writing to it later is legal, and the driver's
// own copies of that request must tolerate it.
type retainingRetryPolicy struct {
	// retryType is what GetRetryType answers.
	retryType RetryType
	// maxAttempts caps the completed attempts, so a failing fixture terminates.
	maxAttempts int

	// retained receives the first request the policy was handed.
	retained chan RetryableQuery
	once     sync.Once
}

var _ RetryPolicy = (*retainingRetryPolicy)(nil)

// newRetainingRetryPolicy returns a policy that retries on the same host.
//
// Returns:
//   - *retainingRetryPolicy: the policy
func newRetainingRetryPolicy(maxAttempts int) *retainingRetryPolicy {
	return &retainingRetryPolicy{
		retryType:   Retry,
		maxAttempts: maxAttempts,
		retained:    make(chan RetryableQuery, 8),
	}
}

// Attempt publishes the request the first time and permits another attempt while
// the budget lasts.
func (p *retainingRetryPolicy) Attempt(q RetryableQuery) bool {
	p.once.Do(func() { p.retained <- q })
	return q.Attempts() < p.maxAttempts
}

// GetRetryType always answers the configured type.
func (p *retainingRetryPolicy) GetRetryType(error) RetryType { return p.retryType }

// awaitRetained blocks until the policy has been handed a request.
//
// Returns:
//   - RetryableQuery: the retained request
func (p *retainingRetryPolicy) awaitRetained(t *testing.T) RetryableQuery {
	t.Helper()

	select {
	case q := <-p.retained:
		return q
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for the retry policy to be called", fillEventBudget)
		return nil
	}
}

// consistencyWriter writes SetConsistency on a retained request from its own
// goroutine until it is stopped.
//
// Nothing orders those writes against the driver's own reads of the same field.
// That is deliberate: a synchronisation point between the two would hide exactly
// the race the next-page copy must not have.
type consistencyWriter struct {
	stop    atomic.Bool
	started chan struct{}
	done    chan struct{}
}

// startConsistencyWriter starts writing to q and returns once one write landed.
//
// The first write is ordered before the caller resumes, so the caller knows the
// writer is running; every later write is unordered, which is what makes a plain
// read of the same field a race.
//
// Returns:
//   - *consistencyWriter: the running writer
func startConsistencyWriter(q RetryableQuery) *consistencyWriter {
	w := &consistencyWriter{started: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		q.SetConsistency(LocalQuorum)
		close(w.started)
		for !w.stop.Load() {
			q.SetConsistency(LocalQuorum)
		}
	}()
	<-w.started
	return w
}

// stopAndWait ends the writer and waits for its goroutine.
func (w *consistencyWriter) stopAndWait() {
	w.stop.Store(true)
	<-w.done
}

// nextPageWriterHooks returns connection hooks that start a consistency writer on
// the retained request the first time a next page is about to be built.
//
// The writer is started inside the seam and released to the caller through a
// channel: the seam itself cleans up nothing, so removing the production copy's
// atomic read is the only thing that can make the race appear or disappear.
//
// Returns:
//   - *connTestHooks: the hooks to install on the cluster
//   - <-chan *consistencyWriter: receives the writer once it is running
func nextPageWriterHooks(t *testing.T, policy *retainingRetryPolicy) (*connTestHooks, <-chan *consistencyWriter) {
	writers := make(chan *consistencyWriter, 1)
	var once sync.Once
	hooks := &connTestHooks{
		beforeNextPage: func(*internalQuery) {
			once.Do(func() { writers <- startConsistencyWriter(policy.awaitRetained(t)) })
		},
	}
	return hooks, writers
}

// awaitWriter blocks until the seam published its writer and registers its stop.
//
// Returns:
//   - *consistencyWriter: the running writer
func awaitWriter(t *testing.T, writers <-chan *consistencyWriter) *consistencyWriter {
	t.Helper()

	select {
	case w := <-writers:
		t.Cleanup(w.stopAndWait)
		return w
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for the next-page seam", fillEventBudget)
		return nil
	}
}

// TestRetryCallbackMayRetainItsArgument proves the production next-page copy
// tolerates a RetryPolicy that kept the request it was handed and writes to it
// from another goroutine.
//
// This is the regression test for the copy contract on internalQuery: the copy
// must read consistency atomically, because nothing in RetryableQuery's contract
// stops a policy from writing it at that moment.
// Reverting conn.go to a whole-struct copy makes the race detector report this
// test.
func TestRetryCallbackMayRetainItsArgument(t *testing.T) {
	script := newPagingScript(2, 4)
	script.errorsBefore.Store(1)
	policy := newRetainingRetryPolicy(6)
	hooks, writers := nextPageWriterHooks(t, policy)

	harness := newFillHarnessOpts(t, 1, fillHarnessOpts{
		respHook: script.hook,
		hooks:    hooks,
		tune:     noHeartbeat,
	})

	iter := pagingQuery(t, harness).Prefetch(0).RetryPolicy(policy).Iter()
	got := scanAll(t, iter, script.pages*script.rows)
	awaitWriter(t, writers)

	require.Len(t, got, script.pages*script.rows, "every page must be delivered")
	require.Equal(t, []int{1, 1, 2}, script.seenPages(),
		"the first attempt fails, the retry serves page 1, and the next page resumes at page 2")
}

// TestSpeculative_PageCopyDoesNotRaceRetainedCallback is
// TestRetryCallbackMayRetainItsArgument through a speculative execution: the
// winning runner's own retained request is written while its next page is built.
//
// A runner snapshot does not remove this race.
// The sibling writes another object now,
// but the runner's own retry callback still holds the very request the next-page
// copy reads, so the copy has to stay atomic.
func TestSpeculative_PageCopyDoesNotRaceRetainedCallback(t *testing.T) {
	script := newPagingScript(2, 4)
	script.errorsBefore.Store(1)
	policy := newRetainingRetryPolicy(8)
	hooks, writers := nextPageWriterHooks(t, policy)

	harness := newFillHarnessOpts(t, 2, fillHarnessOpts{
		respHook: script.hook,
		hooks:    hooks,
		tune:     noHeartbeat,
	})
	stages := newRunStageRecorder()
	harness.session.executor.testRunHook = stages.hook

	qry := pagingQuery(t, harness).Prefetch(0).RetryPolicy(policy)
	speculative(1, speculativeTick)(qry)

	got := scanAll(t, qry.Iter(), script.pages*script.rows)
	awaitWriter(t, writers)

	require.Len(t, got, script.pages*script.rows, "every page must be delivered")
	require.GreaterOrEqual(t, stages.count(runEntered), 1, "the query ran through the speculative runners")
}

// downgradeSignalPolicy wraps a RetryPolicy and reports every consistency its
// Attempt lowered, so a test can order a downgrade against another runner's work
// without inventing its own downgrade rule.
type downgradeSignalPolicy struct {
	inner RetryPolicy
	// downgraded receives the new consistency after every Attempt that changed it.
	downgraded chan Consistency
}

var _ RetryPolicy = (*downgradeSignalPolicy)(nil)

// newDowngradeSignalPolicy wraps DowngradingConsistencyRetryPolicy over levels.
//
// Returns:
//   - *downgradeSignalPolicy: the policy
func newDowngradeSignalPolicy(levels ...Consistency) *downgradeSignalPolicy {
	return &downgradeSignalPolicy{
		inner:      &DowngradingConsistencyRetryPolicy{ConsistencyLevelsToTry: levels},
		downgraded: make(chan Consistency, 8),
	}
}

// Attempt delegates and reports a lowered consistency.
func (p *downgradeSignalPolicy) Attempt(q RetryableQuery) bool {
	before := q.GetConsistency()
	ok := p.inner.Attempt(q)
	if after := q.GetConsistency(); after != before {
		select {
		case p.downgraded <- after:
		default:
		}
	}
	return ok
}

// GetRetryType delegates.
func (p *downgradeSignalPolicy) GetRetryType(err error) RetryType { return p.inner.GetRetryType(err) }

// awaitDowngrade blocks until a runner's consistency was lowered.
//
// Returns:
//   - Consistency: the consistency that runner now holds
func (p *downgradeSignalPolicy) awaitDowngrade(t *testing.T) Consistency {
	t.Helper()

	select {
	case cons := <-p.downgraded:
		return cons
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for a runner to downgrade", fillEventBudget)
		return 0
	}
}

// nextPageGate parks the first next-page construction until it is released.
//
// The next page is built inside the connection execution, before the observer
// accounting and before run publishes anything, so this is the only place a test
// can hold a winning response outside its own copy.
type nextPageGate struct {
	// reached receives one token when a next page is about to be built.
	reached chan struct{}
	// release is closed to let the held construction proceed.
	release chan struct{}
	once    sync.Once
	held    sync.Once
}

// newNextPageGate returns a gate that holds the first next-page construction.
//
// Returns:
//   - *nextPageGate: the gate
func newNextPageGate() *nextPageGate {
	return &nextPageGate{reached: make(chan struct{}, 8), release: make(chan struct{})}
}

// hooks returns the connection hooks that install the gate.
//
// Returns:
//   - *connTestHooks: the hooks
func (g *nextPageGate) hooks() *connTestHooks {
	return &connTestHooks{
		beforeNextPage: func(*internalQuery) {
			g.held.Do(func() {
				g.reached <- struct{}{}
				<-g.release
			})
		},
	}
}

// awaitReached blocks until a next page is about to be built.
func (g *nextPageGate) awaitReached(t *testing.T) {
	t.Helper()
	awaitSignal(t, g.reached, "the winning response to reach the next-page copy")
}

// releaseAll lets the held construction proceed.
func (g *nextPageGate) releaseAll() {
	g.once.Do(func() { close(g.release) })
}

// downgradeFixture is the two-runner setup both schedules of
// TestSpeculative_DowngradeStaysInsideItsRunner share.
type downgradeFixture struct {
	harness *fillHarness
	script  *pagingScript
	policy  *downgradeSignalPolicy
	gate    *nextPageGate
	stages  *runStageRecorder
	// loser is the host whose server answers with errors.
	loser *HostInfo
	// published releases one runner from the checkpoint before it publishes.
	published chan struct{}
	// result receives the query's iterator.
	result chan *Iter
}

// newDowngradeFixture starts two runners: the main one wins on a paged host, the
// speculative one loses on a host that only answers errors.
//
// The main runner is released into the selector first, so it holds the paged
// host and the sibling holds the failing one, whatever order the goroutines
// would otherwise have taken.
//
// Returns:
//   - *downgradeFixture: the running fixture
func newDowngradeFixture(t *testing.T, holdLoserFrom int) *downgradeFixture {
	t.Helper()

	script := newPagingScript(2, 4)
	gate := newNextPageGate()
	t.Cleanup(gate.releaseAll)
	t.Cleanup(script.releaseAll)

	harness := newFillHarnessOpts(t, 2, fillHarnessOpts{
		respHook: script.hook,
		hooks:    gate.hooks(),
		tune:     noHeartbeat,
	})
	winner, loser := harness.hosts[0], harness.hosts[1]
	script.failHost(hostIP(loser))
	script.holdFrom(hostIP(loser), holdLoserFrom)

	policy := &scriptedOneShotPolicy{
		HostSelectionPolicy: harness.session.executor.policy,
		script:              []*HostInfo{winner, loser, loser},
	}
	harness.session.executor.policy = policy

	stages := newRunStageRecorder()
	entered := stages.gate(runEntered)
	published := stages.gate(runResult)
	t.Cleanup(releaseStageGate(entered))
	t.Cleanup(releaseStageGate(published))
	harness.session.executor.testRunHook = stages.hook

	retry := newDowngradeSignalPolicy(One, One, One)
	qry := pagingQuery(t, harness).Prefetch(0).RetryPolicy(retry)
	speculative(1, speculativeTick)(qry)

	f := &downgradeFixture{
		harness:   harness,
		script:    script,
		policy:    retry,
		gate:      gate,
		stages:    stages,
		loser:     loser,
		published: published,
		result:    make(chan *Iter, 1),
	}
	go func() { f.result <- qry.Iter() }()

	// The main runner draws first and reaches the paged host; only then may the
	// sibling draw, so it can only get the failing one.
	stages.await(t, runEntered, "the main runner to start")
	entered <- struct{}{}
	script.await(t, 1)
	stages.await(t, runEntered, "the speculative runner to start")
	entered <- struct{}{}

	return f
}

// releaseWinner waits for the winning runner to reach publication and lets it go.
//
// The sibling is held at its server for as long as a schedule needs it, so the
// runner waiting here is always the winner, and the token reaches it first.
func (f *downgradeFixture) releaseWinner(t *testing.T) {
	t.Helper()

	f.stages.await(t, runResult, "the winning runner to reach publication")
	f.published <- struct{}{}
}

// awaitIter blocks until the query returned its iterator.
//
// Returns:
//   - *Iter: the winning iterator
func (f *downgradeFixture) awaitIter(t *testing.T) *Iter {
	t.Helper()

	select {
	case iter := <-f.result:
		return iter
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for the query to return", fillEventBudget)
		return nil
	}
}

// TestSpeculative_DowngradeStaysInsideItsRunner proves a sibling runner's
// downgrade cannot decide what the winner's next page asks for.
//
// The schedule is pinned rather than left to the goroutines: the winning response
// is held before the next page is built, the sibling downgrades and is kept in
// flight so it can never publish, and only then is the copy allowed to run.
// The reverse order is the control group - it is a legal schedule and not a
// mutation, so it must stay green either way, while the ordered schedule is the
// one a shared request breaks.
func TestSpeculative_DowngradeStaysInsideItsRunner(t *testing.T) {
	t.Run("downgrade before the copy", func(t *testing.T) {
		// The sibling's retry is held at its server, so its terminal result can
		// never beat the winner's.
		f := newDowngradeFixture(t, 2)

		// 2: the winning response is held outside the copy.
		f.gate.awaitReached(t)
		// 3: the sibling downgrades its own snapshot and stays in flight.
		require.Equal(t, One, f.policy.awaitDowngrade(t), "the sibling must have been downgraded")
		f.script.awaitParked(t, hostIP(f.loser), "the sibling's retry to park at its server")
		// 4: the copy may now run, and the winner publishes it.
		f.gate.releaseAll()
		f.releaseWinner(t)

		iter := f.awaitIter(t)
		require.NoError(t, iter.Close())
		require.NotNil(t, iter.next, "the winning page must report a next page")
		require.Equal(t, Quorum, iter.next.q.GetConsistency(),
			"the next page must ask for the consistency the application set")
	})

	t.Run("copy before the downgrade", func(t *testing.T) {
		// The sibling's first request is held, so nothing can downgrade until the
		// copy has run.
		f := newDowngradeFixture(t, 1)

		f.gate.awaitReached(t)
		f.script.awaitParked(t, hostIP(f.loser), "the sibling's first request to park at its server")
		// The copy runs first, and the winner is held before publishing, so the
		// query is still live when the sibling finally downgrades.
		f.gate.releaseAll()
		f.stages.await(t, runResult, "the winning runner to reach publication")

		f.script.release(hostIP(f.loser))
		require.Equal(t, One, f.policy.awaitDowngrade(t), "the sibling must have been downgraded")

		f.published <- struct{}{}
		iter := f.awaitIter(t)
		require.NoError(t, iter.Close())
		require.NotNil(t, iter.next, "the winning page must report a next page")
		require.Equal(t, Quorum, iter.next.q.GetConsistency(),
			"a downgrade after the copy cannot reach the page that was already built")
	})
}

// batchStmt is the statement the batch fixture puts in every entry.
//
// It is a single word, so the driver does not prepare it and every entry travels
// as raw CQL, which keeps the request frame simple enough to read back.
const batchStmt = "noop"

// batchScript is a test server response hook that answers every BATCH with a read
// timeout and records the consistency each one carried.
//
// A batch has no next page - nextIter is built only for a rows result - so the
// only place a sibling's downgrade could surface is the frame the other runner
// sends next.
type batchScript struct {
	// holds parks selected requests, per server.
	*requestHolds

	mu sync.Mutex
	// consistencies is the consistency of every BATCH each server answered.
	consistencies map[string][]Consistency

	// updated is signalled after every recorded request.
	updated chan struct{}
}

// newBatchScript returns a script that fails every batch.
//
// Returns:
//   - *batchScript: the script, ready to be installed as fillHarnessOpts.respHook
func newBatchScript() *batchScript {
	return &batchScript{
		requestHolds:  newRequestHolds(),
		consistencies: map[string][]Consistency{},
		updated:       make(chan struct{}, 64),
	}
}

// hook is the fillHarnessOpts.respHook.
//
// Returns:
//   - bool: true when it wrote the response itself
func (s *batchScript) hook(ip string, srv *TestServer, req, resp *framer) bool {
	head := req.header
	if head == nil || head.op != opBatch {
		return false
	}

	saved := req.buf
	cons, ok := readBatchConsistency(req)
	req.buf = saved
	if !ok {
		return false
	}

	s.mu.Lock()
	s.consistencies[ip] = append(s.consistencies[ip], cons)
	s.mu.Unlock()
	select {
	case s.updated <- struct{}{}:
	default:
	}

	s.enter(ip)
	writeReadTimeoutError(resp, head)
	return true
}

// readBatchConsistency walks a BATCH request as far as its consistency.
//
// The consistency sits after the statements, which is exactly where the default
// test server's own batch walk stops.
//
// Returns:
//   - Consistency: the consistency the batch carried
//   - bool: false when the frame did not parse
func readBatchConsistency(f *framer) (Consistency, bool) {
	if _, err := f.readByte(); err != nil {
		return 0, false
	}
	n, err := f.readShort()
	if err != nil {
		return 0, false
	}
	for range int(n) {
		kind, err := f.readByte()
		if err != nil {
			return 0, false
		}
		if kind == 1 {
			if _, err := f.readShortBytes(); err != nil {
				return 0, false
			}
		} else if _, err := f.readLongString(); err != nil {
			return 0, false
		}
		values, err := f.readShort()
		if err != nil {
			return 0, false
		}
		for range int(values) {
			size, err := f.readInt()
			if err != nil {
				return 0, false
			}
			if size > 0 {
				if len(f.buf) < size {
					return 0, false
				}
				f.buf = f.buf[size:]
			}
		}
	}

	cons, err := f.readConsistency()
	if err != nil {
		return 0, false
	}
	return cons, true
}

// seenAt returns the consistency of every BATCH the server at ip answered.
//
// Returns:
//   - []Consistency: the consistencies, in arrival order
func (s *batchScript) seenAt(ip string) []Consistency {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Consistency(nil), s.consistencies[ip]...)
}

// awaitAt blocks until the server at ip has answered at least n batches.
func (s *batchScript) awaitAt(t *testing.T, ip string, n int) {
	t.Helper()

	deadline := time.After(fillEventBudget)
	for len(s.seenAt(ip)) < n {
		select {
		case <-s.updated:
		case <-deadline:
			t.Fatalf("timed out after %v waiting for %d batches at %s, saw %d",
				fillEventBudget, n, ip, len(s.seenAt(ip)))
		}
	}
}

// downgradeOncePolicy lowers the consistency of the first request it is handed
// and leaves every later one alone.
//
// The decision is keyed on a call counter, never on the request's identity: a
// snapshot that wrongly returns the shared request must change what the test
// observes, not how the policy behaves.
type downgradeOncePolicy struct {
	// to is the consistency the first Attempt writes.
	to Consistency
	// maxAttempts caps the completed attempts of the whole page execution.
	maxAttempts int

	calls atomic.Int32
	// downgraded is closed once the first Attempt wrote.
	downgraded chan struct{}
	once       sync.Once
}

var _ RetryPolicy = (*downgradeOncePolicy)(nil)

// newDowngradeOncePolicy returns a policy that downgrades exactly once.
//
// Returns:
//   - *downgradeOncePolicy: the policy
func newDowngradeOncePolicy(to Consistency, maxAttempts int) *downgradeOncePolicy {
	return &downgradeOncePolicy{to: to, maxAttempts: maxAttempts, downgraded: make(chan struct{})}
}

// Attempt downgrades the first request and permits another attempt while the
// shared budget lasts.
func (p *downgradeOncePolicy) Attempt(q RetryableQuery) bool {
	if p.calls.Add(1) == 1 {
		q.SetConsistency(p.to)
		p.once.Do(func() { close(p.downgraded) })
	}
	return q.Attempts() < p.maxAttempts
}

// GetRetryType keeps the runner on its own host, so a retry never spends another
// host from the shared selector.
func (p *downgradeOncePolicy) GetRetryType(error) RetryType { return Retry }

// awaitDowngrade blocks until the first request was downgraded.
func (p *downgradeOncePolicy) awaitDowngrade(t *testing.T) {
	t.Helper()

	select {
	case <-p.downgraded:
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for the sibling to downgrade", fillEventBudget)
	}
}

// TestSpeculative_BatchRunnersGetOwnSnapshot proves a sibling runner's downgrade
// cannot decide what another runner's next BATCH frame asks for.
//
// A batch has no next page, so the acceptance point is the request frame itself:
// the main runner's first batch is held at its server until the sibling has
// downgraded, and the frame it sends afterwards must still carry the consistency
// the application set.
func TestSpeculative_BatchRunnersGetOwnSnapshot(t *testing.T) {
	script := newBatchScript()
	t.Cleanup(script.releaseAll)

	harness := newFillHarnessOpts(t, 2, fillHarnessOpts{respHook: script.hook, tune: noHeartbeat})
	winner, loser := harness.hosts[0], harness.hosts[1]
	// The main runner's first batch is held, so it cannot reach the retry policy
	// before the sibling does; the sibling's retry is held, so it can never
	// publish a terminal result and end the query.
	script.holdFrom(hostIP(winner), 1)
	script.holdFrom(hostIP(loser), 2)

	policy := &scriptedOneShotPolicy{
		HostSelectionPolicy: harness.session.executor.policy,
		script:              []*HostInfo{winner, loser, loser},
	}
	harness.session.executor.policy = policy

	stages := newRunStageRecorder()
	entered := stages.gate(runEntered)
	t.Cleanup(releaseStageGate(entered))
	harness.session.executor.testRunHook = stages.hook

	retry := newDowngradeOncePolicy(One, 3)
	batch := harness.session.Batch(LoggedBatch).WithContext(boundedContext(t)).
		Consistency(Quorum).RetryPolicy(retry).
		SpeculativeExecutionPolicy(&SimpleSpeculativeExecution{NumAttempts: 1, TimeoutDelay: speculativeTick})
	batch.Entries = append(batch.Entries, BatchEntry{Stmt: batchStmt, Idempotent: true})

	result := make(chan *Iter, 1)
	go func() { result <- batch.Iter() }()

	// The main runner draws first and parks its first batch at its own server.
	stages.await(t, runEntered, "the main runner to start")
	entered <- struct{}{}
	script.awaitAt(t, hostIP(winner), 1)
	script.awaitParked(t, hostIP(winner), "the main runner's first batch to park")

	// The sibling now fails, downgrades its own snapshot, and parks its retry.
	stages.await(t, runEntered, "the speculative runner to start")
	entered <- struct{}{}
	retry.awaitDowngrade(t)
	script.awaitParked(t, hostIP(loser), "the sibling's retry to park at its server")

	// Only now may the main runner see its error and send its next batch.
	script.release(hostIP(winner))
	script.awaitAt(t, hostIP(winner), 2)

	seen := script.seenAt(hostIP(winner))
	require.Equal(t, []Consistency{Quorum, Quorum}, seen[:2],
		"the sibling's downgrade must not reach the frames the other runner sends")

	select {
	case iter := <-result:
		require.Error(t, iter.Close(), "the fixture answers every batch with a read timeout")
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for the batch to finish", fillEventBudget)
	}
}

// requestField is one field of an internal request, as the copy contract sees it.
type requestField struct {
	name string
	typ  string
}

// internalQueryFields is the field list both copy sites of internalQuery must
// carry: snapshotForRunner and the next-page construction in conn.go.
var internalQueryFields = []requestField{
	{"originalQuery", "*gocql.Query"},
	{"qryOpts", "*gocql.queryOptions"},
	{"pageState", "[]uint8"},
	{"conn", "*gocql.Conn"},
	{"consistency", "uint32"},
	{"session", "*gocql.Session"},
	{"routingInfo", "*gocql.queryRoutingInfo"},
	{"metrics", "*gocql.queryMetrics"},
	{"hostMetricsManager", "gocql.hostMetricsManager"},
	{"runnerCtx", "context.Context"},
}

// internalBatchFields is the field list internalBatch.snapshotForRunner must carry.
var internalBatchFields = []requestField{
	{"originalBatch", "*gocql.Batch"},
	{"batchOpts", "*gocql.batchOptions"},
	{"consistency", "uint32"},
	{"routingInfo", "*gocql.queryRoutingInfo"},
	{"session", "*gocql.Session"},
	{"metrics", "*gocql.queryMetrics"},
	{"hostMetricsManager", "gocql.hostMetricsManager"},
	{"runnerCtx", "context.Context"},
}

// requireFields compares a struct's fields against the copy contract's list.
func requireFields(t *testing.T, value any, want []requestField) {
	t.Helper()

	typ := reflect.TypeOf(value)
	got := make([]requestField, 0, typ.NumField())
	for i := range typ.NumField() {
		field := typ.Field(i)
		got = append(got, requestField{name: field.Name, typ: field.Type.String()})
	}
	require.Equal(t, want, got,
		"%s changed: every copy site must carry the new field, see the copy contract on the struct",
		typ.Name())
}

// pagingHarness starts a fixture whose servers serve the paging statement.
//
// Returns:
//   - *fillHarness: the harness
//   - *pagingScript: the script the servers answer from
//   - *runStageRecorder: the runner checkpoints, already installed
func pagingHarness(t *testing.T, hosts, pages int) (*fillHarness, *pagingScript, *runStageRecorder) {
	t.Helper()

	script := newPagingScript(pages, 4)
	t.Cleanup(script.releaseAll)

	harness := newFillHarnessOpts(t, hosts, fillHarnessOpts{respHook: script.hook, tune: noHeartbeat})
	stages := newRunStageRecorder()
	harness.session.executor.testRunHook = stages.hook
	return harness, script, stages
}

// requireConsistency asserts every request the fixture answered carried cons.
func requireConsistency(t *testing.T, script *pagingScript, cons Consistency) {
	t.Helper()

	for i, req := range script.seen() {
		require.Equal(t, cons, req.consistency, "request %d carried the wrong consistency", i)
	}
}

// wantRows returns the values the fixture's pages carry, in order.
//
// Returns:
//   - []int: the expected values
func wantRows(script *pagingScript) []int {
	rows := make([]int, 0, script.pages*script.rows)
	for i := range script.pages * script.rows {
		rows = append(rows, i)
	}
	return rows
}

// sameBacking reports whether two byte slices share one backing array.
//
// Returns:
//   - bool: true when both are empty, or both start at the same address
func sameBacking(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	return &a[0] == &b[0]
}

// batchAttemptRecorder is a BatchObserver that keeps every observation.
type batchAttemptRecorder struct {
	mu       sync.Mutex
	attempts []ObservedBatch
	// updated is signalled after every observation.
	updated chan struct{}
}

var _ BatchObserver = (*batchAttemptRecorder)(nil)

// newBatchAttemptRecorder returns an empty recorder.
//
// Returns:
//   - *batchAttemptRecorder: the recorder
func newBatchAttemptRecorder() *batchAttemptRecorder {
	return &batchAttemptRecorder{updated: make(chan struct{}, 8)}
}

// ObserveBatch records the attempt.
func (r *batchAttemptRecorder) ObserveBatch(_ context.Context, b ObservedBatch) {
	r.mu.Lock()
	r.attempts = append(r.attempts, b)
	r.mu.Unlock()

	select {
	case r.updated <- struct{}{}:
	default:
	}
}

// observed returns every observation so far.
//
// Returns:
//   - []ObservedBatch: the observations, in arrival order
func (r *batchAttemptRecorder) observed() []ObservedBatch {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]ObservedBatch(nil), r.attempts...)
}

// await blocks until at least n attempts were observed.
func (r *batchAttemptRecorder) await(t *testing.T, n int) {
	t.Helper()

	deadline := time.After(fillEventBudget)
	for len(r.observed()) < n {
		select {
		case <-r.updated:
		case <-deadline:
			t.Fatalf("timed out after %v waiting for %d batch attempts, saw %d",
				fillEventBudget, n, len(r.observed()))
		}
	}
}

// nextPageCapture records the request every next-page construction was built
// from, so a test can compare the copy against its source at the conn.go site.
//
// It only observes; production builds and owns the copy.
type nextPageCapture struct {
	mu      sync.Mutex
	sources []*internalQuery
}

// hooks returns the connection hooks that install the capture.
//
// Returns:
//   - *connTestHooks: the hooks
func (c *nextPageCapture) hooks() *connTestHooks {
	return &connTestHooks{
		beforeNextPage: func(q *internalQuery) {
			c.mu.Lock()
			c.sources = append(c.sources, q)
			c.mu.Unlock()
		},
	}
}

// source returns the nth request a next page was built from, counting from zero.
//
// Returns:
//   - *internalQuery: the source request
func (c *nextPageCapture) source(t *testing.T, n int) *internalQuery {
	t.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()
	require.Greater(t, len(c.sources), n, "only %d next pages were built", len(c.sources))
	return c.sources[n]
}

// populatedInternalQuery returns a request whose every shared field holds a
// distinct, non-zero object.
//
// An identity assertion over a half-empty request proves nothing: a copy site
// that rebuilt a field would produce an equal empty object. Every pointer here
// is therefore one a fresh equivalent could not impersonate.
//
// Returns:
//   - *Query: the public query behind it
//   - *internalQuery: the populated request
func populatedInternalQuery(t *testing.T, h *fillHarness) (*Query, *internalQuery) {
	t.Helper()

	pub := pagingQuery(t, h).Observer(newAttemptRecorder())
	internal := newInternalQuery(pub, t.Context())
	internal.conn = h.pickAnyConn(t, h.pool(t, h.hosts[0]))
	internal.pageState = []byte("resume-here")
	internal.routingInfo.set(pagingKeyspace, pagingTable)
	internal.SetConsistency(Quorum)

	require.NotSame(t, emptyHostMetricsManager, internal.hostMetricsManager,
		"the fixture must carry a real manager, or the identity assertion proves nothing")
	return pub, internal
}

// populatedInternalBatch is populatedInternalQuery for a batch.
//
// Returns:
//   - *Batch: the public batch behind it
//   - *internalBatch: the populated request
func populatedInternalBatch(t *testing.T, h *fillHarness) (*Batch, *internalBatch) {
	t.Helper()

	pub := h.session.Batch(LoggedBatch).WithContext(boundedContext(t)).
		Consistency(Quorum).Observer(newBatchAttemptRecorder())
	pub.Entries = append(pub.Entries, BatchEntry{Stmt: batchStmt, Idempotent: true})

	internal := newInternalBatch(pub, t.Context())
	internal.routingInfo.set(pagingKeyspace, pagingTable)
	internal.SetConsistency(Quorum)

	require.NotSame(t, emptyHostMetricsManager, internal.hostMetricsManager,
		"the fixture must carry a real manager, or the identity assertion proves nothing")
	return pub, internal
}

// TestInternalRequestFieldsAreSnapshotted guards the copy contract on the two
// internal request types.
//
// The reflection subtests are only a maintenance alarm: they prove somebody
// changed the struct, never that the copies are right. Dropping an existing
// field leaves them green, so every field the plan calls out has its own
// behaviour subtest below, and those are what the mutation gates run against.
func TestInternalRequestFieldsAreSnapshotted(t *testing.T) {
	t.Run("query fields", func(t *testing.T) {
		requireFields(t, internalQuery{}, internalQueryFields)
	})

	t.Run("batch fields", func(t *testing.T) {
		requireFields(t, internalBatch{}, internalBatchFields)
	})

	// Identity, not equality: the plan's copy table says which pointers the copy
	// shares, and a copy site that rebuilt one of them into an equivalent object
	// would satisfy every wire test below while breaking the contract.
	t.Run("query snapshot shares every pointer field", func(t *testing.T) {
		harness, _, _ := pagingHarness(t, 1, 1)
		pub, orig := populatedInternalQuery(t, harness)

		runnerCtx, cancel := context.WithCancel(t.Context())
		defer cancel()

		snap, ok := orig.snapshotForRunner(runnerCtx).(*internalQuery)
		require.True(t, ok, "a query snapshot must still be an *internalQuery")
		require.NotSame(t, orig, snap, "the runner must own a different request object")

		// The runner context is the one field the snapshot adds rather than shares:
		// a RetryPolicy consulted inside the runner must see the runner's own
		// cancellation, and the request the caller holds must keep its own context.
		require.Same(t, runnerCtx, snap.Context(), "the snapshot reports the runner context")
		require.Same(t, orig.qryOpts.context, orig.Context(),
			"the original request must still report the caller's context")
		require.Nil(t, orig.runnerCtx, "the original request must not have acquired a runner context")

		require.Same(t, pub, snap.originalQuery, "originalQuery must stay the public query the caller holds")
		require.Same(t, orig.qryOpts, snap.qryOpts, "qryOpts")
		require.Same(t, orig.conn, snap.conn, "conn")
		require.Same(t, orig.session, snap.session, "session")
		require.Same(t, orig.routingInfo, snap.routingInfo, "routingInfo")
		require.Same(t, orig.metrics, snap.metrics, "metrics")
		require.Same(t, orig.hostMetricsManager, snap.hostMetricsManager,
			"hostMetricsManager must be the same manager, not a new one behind the same interface")
		require.True(t, sameBacking(orig.pageState, snap.pageState),
			"pageState must be carried over, not rebuilt")

		// consistency is the one field a runner owns.
		require.Equal(t, Quorum, snap.GetConsistency())
		snap.SetConsistency(One)
		require.Equal(t, Quorum, orig.GetConsistency(),
			"a runner's downgrade must stay inside its own request")
	})

	t.Run("batch snapshot shares every pointer field", func(t *testing.T) {
		harness, _, _ := pagingHarness(t, 1, 1)
		pub, orig := populatedInternalBatch(t, harness)

		runnerCtx, cancel := context.WithCancel(t.Context())
		defer cancel()

		snap, ok := orig.snapshotForRunner(runnerCtx).(*internalBatch)
		require.True(t, ok, "a batch snapshot must still be an *internalBatch")
		require.NotSame(t, orig, snap, "the runner must own a different request object")

		require.Same(t, runnerCtx, snap.Context(), "the snapshot reports the runner context")
		require.Same(t, orig.batchOpts.context, orig.Context(),
			"the original request must still report the caller's context")
		require.Nil(t, orig.runnerCtx, "the original request must not have acquired a runner context")

		require.Same(t, pub, snap.originalBatch, "originalBatch must stay the public batch the caller holds")
		require.Same(t, orig.batchOpts, snap.batchOpts, "batchOpts")
		require.Same(t, orig.session, snap.session, "session")
		require.Same(t, orig.routingInfo, snap.routingInfo, "routingInfo")
		require.Same(t, orig.metrics, snap.metrics, "metrics")
		require.Same(t, orig.hostMetricsManager, snap.hostMetricsManager,
			"hostMetricsManager must be the same manager, not a new one behind the same interface")

		require.Equal(t, Quorum, snap.GetConsistency())
		snap.SetConsistency(One)
		require.Equal(t, Quorum, orig.GetConsistency(),
			"a runner's downgrade must stay inside its own request")
	})

	// The same identities at the other copy site, plus its three exceptions.
	t.Run("next page shares the request's pointers", func(t *testing.T) {
		capture := &nextPageCapture{}
		script := newPagingScript(3, 4)
		t.Cleanup(script.releaseAll)
		harness := newFillHarnessOpts(t, 1, fillHarnessOpts{
			respHook: script.hook,
			hooks:    capture.hooks(),
			tune:     noHeartbeat,
		})

		qry := pagingQuery(t, harness).Prefetch(0).Observer(newAttemptRecorder())
		iter := qry.Iter()
		require.NotNil(t, iter.next, "the first page must report a next page")

		src, next := capture.source(t, 0), iter.next.q
		require.NotSame(t, src, next, "the next page must be a different request object")
		require.Same(t, qry, next.originalQuery, "originalQuery must stay the public query the caller holds")
		require.Same(t, src.qryOpts, next.qryOpts, "qryOpts")
		require.Same(t, src.session, next.session, "session")
		require.Same(t, src.routingInfo, next.routingInfo, "routingInfo")
		require.Equal(t, src.GetConsistency(), next.GetConsistency(),
			"the next page must ask for the consistency the page it follows used")

		// The exceptions: a fresh attempt budget and a fresh manager per page.
		// NotSame alone is satisfied by a nil copy, so require the object first.
		require.NotNil(t, next.metrics, "the next page must carry a queryMetrics")
		require.NotSame(t, src.metrics, next.metrics, "the next page must start from a fresh queryMetrics")
		require.Equal(t, 0, next.metrics.attempts())
		require.NotSame(t, src.hostMetricsManager, next.hostMetricsManager,
			"an observed query gets its own host metrics per page")
		require.NotSame(t, emptyHostMetricsManager, next.hostMetricsManager,
			"an observed query must not fall back to the empty manager")
		require.Equal(t, pagingStateFor(2), next.pageState)

		// Page two onwards has a paging state of its own, so the third exception
		// is visible: the state is the response's, copied, not the source's.
		var v int
		for range script.rows + 1 {
			require.True(t, iter.Scan(&v), "the fixture's first two pages must be readable")
		}
		src, next = capture.source(t, 1), iter.next.q
		require.NotEmpty(t, src.pageState, "the second page must have resumed from a state")
		require.Equal(t, pagingStateFor(3), next.pageState)
		require.False(t, sameBacking(src.pageState, next.pageState),
			"the next page's state must be the response's, copied, not the source's")
		require.NoError(t, iter.Close())
	})

	// runnerCtx is the one field the next page must NOT inherit: it belongs to the
	// runner that fetched the page it follows, and executeQuery cancels that context
	// as soon as the page is returned.
	t.Run("next page does not inherit the runner context", func(t *testing.T) {
		capture := &nextPageCapture{}
		script := newPagingScript(2, 4)
		t.Cleanup(script.releaseAll)
		harness := newFillHarnessOpts(t, 1, fillHarnessOpts{
			respHook: script.hook,
			hooks:    capture.hooks(),
			tune:     noHeartbeat,
		})

		callerCtx := boundedContext(t)
		qry := harness.session.Query(pagingStmt).WithContext(callerCtx).
			Consistency(Quorum).PageSize(4).Idempotent(true).Prefetch(0)
		// One speculative attempt an hour away: only the main runner ever starts, so
		// the request the page is built from is deterministically its snapshot.
		speculative(1, time.Hour)(qry)

		iter := qry.Iter()
		require.NotNil(t, iter.next, "the first page must report a next page")

		src, next := capture.source(t, 0), iter.next.q
		require.NotNil(t, src.runnerCtx, "the page was fetched by a runner, so its request carries one")
		require.Same(t, src.runnerCtx, src.Context(), "the runner's request reports the runner context")
		// The query has returned, so executeQuery's deferred cancel has run.
		requireSameError(t, context.Canceled, src.runnerCtx.Err(),
			"the runner context is cancelled once the page is returned")

		require.Nil(t, next.runnerCtx, "the next page must not inherit the runner context")
		require.Same(t, callerCtx, next.Context(), "the next page reports the caller's own context")

		// The proof that matters: a next page built on a cancelled context could not
		// be fetched at all.
		got := scanAll(t, iter, script.pages*script.rows)
		require.Equal(t, wantRows(script), got, "both pages must be delivered in order")
	})

	t.Run("pinned next page keeps its connection and the empty manager", func(t *testing.T) {
		capture := &nextPageCapture{}
		script := newPagingScript(2, 4)
		t.Cleanup(script.releaseAll)
		harness := newFillHarnessOpts(t, 1, fillHarnessOpts{
			respHook: script.hook,
			hooks:    capture.hooks(),
			tune:     noHeartbeat,
		})

		conn := harness.pickAnyConn(t, harness.pool(t, harness.hosts[0]))
		iter := conn.query(t.Context(), pagingStmt)
		require.NotNil(t, iter.next, "the first page must report a next page")

		src, next := capture.source(t, 0), iter.next.q
		require.NotNil(t, src.conn, "the fixture must pin the query, or the identity assertion proves nothing")
		require.Same(t, conn, next.conn, "the next page must stay on the pinned connection")
		require.Same(t, src.conn, next.conn, "conn")
		require.Same(t, emptyHostMetricsManager, next.hostMetricsManager,
			"an unobserved query gets the empty manager")
		require.NoError(t, iter.Close())
	})

	// pageState: a runner that fetches page two resumes from the state page one
	// returned. Every page runs through the executor again, so every page after
	// the first is fetched by a runner holding a snapshot.
	t.Run("synchronous paging reaches the third page", func(t *testing.T) {
		harness, script, stages := pagingHarness(t, 1, 3)

		qry := pagingQuery(t, harness).Prefetch(0)
		speculative(1, time.Hour)(qry)

		got := scanAll(t, qry.Iter(), script.pages*script.rows)
		require.Equal(t, wantRows(script), got, "every page must be delivered in order")
		require.Equal(t, []int{1, 2, 3}, script.seenPages(),
			"each request must resume where the previous page stopped")
		requireConsistency(t, script, Quorum)
		require.Equal(t, 3, stages.count(runEntered),
			"every page is fetched by a runner, so every page uses a snapshot")
	})

	t.Run("prefetched paging reaches the third page", func(t *testing.T) {
		harness, script, stages := pagingHarness(t, 1, 3)

		qry := pagingQuery(t, harness).Prefetch(1)
		speculative(1, time.Hour)(qry)

		got := scanAll(t, qry.Iter(), script.pages*script.rows)
		require.Equal(t, wantRows(script), got, "every page must be delivered in order")
		require.Equal(t, []int{1, 2, 3}, script.seenPages(),
			"a prefetched page must resume where the previous page stopped")
		requireConsistency(t, script, Quorum)
		require.Equal(t, 3, stages.count(runEntered),
			"every prefetched page is fetched by a runner, so every page uses a snapshot")
	})

	// conn: a query pinned to one connection keeps paging on it. The selection
	// policy is pointed at the other host, so losing the pinned connection is
	// visible as a page served by the wrong server.
	t.Run("pinned paging stays on its connection", func(t *testing.T) {
		harness, script, _ := pagingHarness(t, 2, 3)
		pinned, other := harness.hosts[0], harness.hosts[1]
		harness.session.executor.policy = &scriptedOneShotPolicy{
			HostSelectionPolicy: harness.session.executor.policy,
			script:              []*HostInfo{other, other, other},
		}

		conn := harness.pickAnyConn(t, harness.pool(t, pinned))
		got := scanAll(t, conn.query(t.Context(), pagingStmt), script.pages*script.rows)

		require.Equal(t, wantRows(script), got, "every page must be delivered in order")
		require.Equal(t, []int{1, 2, 3}, script.seenPages())
		for i, req := range script.seen() {
			require.Equal(t, hostIP(pinned), req.ip,
				"request %d left the pinned connection", i)
		}
	})

	// routingInfo: the prepared response's keyspace and table are recorded on the
	// request the runner holds, and the iterator reports them.
	t.Run("routing metadata survives the snapshot", func(t *testing.T) {
		harness, _, _ := pagingHarness(t, 1, 1)

		qry := pagingQuery(t, harness)
		speculative(1, time.Hour)(qry)

		iter := qry.Iter()
		require.Equal(t, pagingTable, iter.Table(), "the prepared response's table must reach the iterator")
		require.NoError(t, iter.Close())
	})

	// hostMetricsManager: the per-host metrics an observer receives are the ones
	// the request carries, and they are shared, not rebuilt per runner.
	t.Run("observer metrics survive the snapshot", func(t *testing.T) {
		harness, script, _ := pagingHarness(t, 1, 1)
		// One failure, so the runner attempts the same host twice: two attempts
		// through one shared manager return the very same hostMetrics.
		script.errorsBefore.Store(1)

		recorder := newAttemptRecorder()
		qry := pagingQuery(t, harness).Observer(recorder).RetryPolicy(newRetainingRetryPolicy(2))
		speculative(1, time.Hour)(qry)

		require.NoError(t, qry.Iter().Close())
		recorder.await(t, 2)

		seen := recorder.attempts
		require.Len(t, seen, 2)
		for i, attempt := range seen {
			require.NotNil(t, attempt.Metrics, "attempt %d reached the observer without host metrics", i)
			require.Same(t, qry, attempt.Query,
				"attempt %d must reach the observer with the public query, whose identity does not change", i)
		}
		require.Same(t, seen[0].Metrics, seen[1].Metrics,
			"both attempts on one host must come from the one shared manager")
		require.Equal(t, 2, seen[1].Metrics.Attempts)
	})

	t.Run("batch observer metrics survive the snapshot", func(t *testing.T) {
		script := newBatchScript()
		t.Cleanup(script.releaseAll)
		harness := newFillHarnessOpts(t, 1, fillHarnessOpts{respHook: script.hook, tune: noHeartbeat})

		recorder := newBatchAttemptRecorder()
		batch := harness.session.Batch(LoggedBatch).WithContext(boundedContext(t)).
			Consistency(Quorum).Observer(recorder).RetryPolicy(newRetainingRetryPolicy(2)).
			SpeculativeExecutionPolicy(&SimpleSpeculativeExecution{NumAttempts: 1, TimeoutDelay: time.Hour})
		batch.Entries = append(batch.Entries, BatchEntry{Stmt: batchStmt, Idempotent: true})

		require.Error(t, batch.Iter().Close(), "the fixture answers every batch with a read timeout")
		recorder.await(t, 2)

		seen := recorder.observed()
		require.Len(t, seen, 2)
		for i, attempt := range seen {
			require.NotNil(t, attempt.Metrics, "attempt %d reached the observer without host metrics", i)
			require.Same(t, batch, attempt.Batch,
				"attempt %d must reach the observer with the public batch, whose identity does not change", i)
		}
		require.Same(t, seen[0].Metrics, seen[1].Metrics,
			"both attempts on one host must come from the one shared manager")
		require.Equal(t, 2, seen[1].Metrics.Attempts)
	})
}

// attemptsRecorder is a RetryPolicy that records the completed-attempt count it
// was shown and then stops the runner.
type attemptsRecorder struct {
	mu sync.Mutex
	// seen is the count every Attempt call observed, in order.
	seen []int
	// calls counts the Attempt calls.
	calls atomic.Int32
	// updated is signalled after every call.
	updated chan struct{}
}

var _ RetryPolicy = (*attemptsRecorder)(nil)

// newAttemptsRecorder returns a policy that lets each runner make one attempt.
//
// Returns:
//   - *attemptsRecorder: the policy
func newAttemptsRecorder() *attemptsRecorder {
	return &attemptsRecorder{updated: make(chan struct{}, 8)}
}

// Attempt records the count and permits nothing further.
func (p *attemptsRecorder) Attempt(q RetryableQuery) bool {
	p.mu.Lock()
	p.seen = append(p.seen, q.Attempts())
	p.mu.Unlock()
	p.calls.Add(1)

	select {
	case p.updated <- struct{}{}:
	default:
	}
	return true
}

// GetRetryType ends the runner, so each runner makes exactly one attempt.
func (p *attemptsRecorder) GetRetryType(error) RetryType { return Rethrow }

// counts returns every completed-attempt count observed so far.
//
// Returns:
//   - []int: the counts, in call order
func (p *attemptsRecorder) counts() []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]int(nil), p.seen...)
}

// awaitCalls blocks until the policy has been called n times.
func (p *attemptsRecorder) awaitCalls(t *testing.T, n int) {
	t.Helper()

	deadline := time.After(fillEventBudget)
	for int(p.calls.Load()) < n {
		select {
		case <-p.updated:
		case <-deadline:
			t.Fatalf("timed out after %v waiting for %d retry decisions, saw %d",
				fillEventBudget, n, p.calls.Load())
		}
	}
}

// TestAttemptAccountingIsPerPage records the attempt budget this stage keeps
// unchanged.
//
// The count a RetryPolicy reads is shared by every runner of one page execution,
// it is not an admission cap on requests, and it starts over on the next page.
// Changing any of those would change what a RetryPolicy observes, which this
// fork treats as a minor release.
func TestAttemptAccountingIsPerPage(t *testing.T) {
	t.Run("the runners of one page share the count", func(t *testing.T) {
		harness, script, stages := pagingHarness(t, 2, 1)
		first, second := harness.hosts[0], harness.hosts[1]
		for _, host := range harness.hosts {
			script.failHost(hostIP(host))
			script.holdFrom(hostIP(host), 1)
		}
		harness.session.executor.policy = &scriptedOneShotPolicy{
			HostSelectionPolicy: harness.session.executor.policy,
			script:              []*HostInfo{first, second, second},
		}

		entered := stages.gate(runEntered)
		published := stages.gate(runResult)
		t.Cleanup(releaseStageGate(entered))
		t.Cleanup(releaseStageGate(published))

		policy := newAttemptsRecorder()
		qry := pagingQuery(t, harness).RetryPolicy(policy)
		speculative(1, speculativeTick)(qry)
		result := make(chan *Iter, 1)
		go func() { result <- qry.Iter() }()

		stages.await(t, runEntered, "the main runner to start")
		entered <- struct{}{}
		script.awaitParked(t, hostIP(first), "the main runner's request to park")
		stages.await(t, runEntered, "the speculative runner to start")
		entered <- struct{}{}
		script.awaitParked(t, hostIP(second), "the speculative runner's request to park")

		// Two requests are in flight and the retry policy has not been consulted
		// once: the budget is not an admission cap.
		require.Equal(t, int32(0), policy.calls.Load(),
			"a speculative launch never asks the retry policy")

		// The first runner completes its attempt and is held before publishing,
		// so the second runner's decision is taken with the first one counted.
		script.release(hostIP(first))
		policy.awaitCalls(t, 1)
		stages.await(t, runResult, "the first runner to reach publication")

		script.release(hostIP(second))
		policy.awaitCalls(t, 2)

		require.Equal(t, []int{1, 2}, policy.counts(),
			"the second runner must see the first runner's attempt counted")

		published <- struct{}{}
		select {
		case iter := <-result:
			require.Error(t, iter.Close(), "the fixture answers every request with a read timeout")
		case <-time.After(fillEventBudget):
			t.Fatalf("timed out after %v waiting for the query to finish", fillEventBudget)
		}
	})

	t.Run("the count starts over on the next page", func(t *testing.T) {
		harness, script, _ := pagingHarness(t, 1, 2)
		// The first page needs two attempts, the second page one.
		script.errorsBefore.Store(1)

		policy := newRetainingRetryPolicy(6)
		iter := pagingQuery(t, harness).Prefetch(0).RetryPolicy(policy).Iter()

		var v int
		for range script.rows {
			require.True(t, iter.Scan(&v), "the first page must be readable")
		}
		require.Equal(t, 2, iter.Attempts(), "the first page took a failed attempt and a retry")

		require.True(t, iter.Scan(&v), "the second page must be readable")
		require.Equal(t, 1, iter.Attempts(), "the next page starts from a fresh count")
		require.NoError(t, iter.Close())
	})
}
