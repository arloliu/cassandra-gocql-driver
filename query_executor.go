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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Deprecated: Will be removed in a future major release. Also Query and Batch no longer implement this interface.
//
// Please use Statement (for Query / Batch objects) or ExecutableStatement (in HostSelectionPolicy implementations) instead.
type ExecutableQuery = ExecutableStatement

// ExecutableStatement is an interface that represents a query or batch statement that
// exposes the correct functions for the HostSelectionPolicy to operate correctly.
type ExecutableStatement interface {
	GetRoutingKey() ([]byte, error)
	Keyspace() string
	Table() string
	IsIdempotent() bool
	GetHostID() string
	Statement() Statement
}

// Statement is an interface that represents a CQL statement that the driver can execute
// (currently Query and Batch via Session.Query and Session.Batch)
type Statement interface {
	Iter() *Iter
	IterContext(ctx context.Context) *Iter
	Exec() error
	ExecContext(ctx context.Context) error
}

type internalRequest interface {
	execute(ctx context.Context, conn *Conn) *Iter
	attempt(keyspace string, end, start time.Time, iter *Iter, host *HostInfo)
	retryPolicy() RetryPolicy
	speculativeExecutionPolicy() SpeculativeExecutionPolicy
	getQueryMetrics() *queryMetrics
	getRoutingInfo() *queryRoutingInfo
	getKeyspaceFunc() func() string
	RetryableQuery
	ExecutableStatement
}

// runStage identifies a checkpoint in run that a test can observe through
// queryExecutor.testRunHook.
type runStage uint8

const (
	// runEntered fires as run starts, before any host is selected.
	runEntered runStage = iota
	// runNoHost fires just before run reports that it found no host to try.
	runNoHost
	// runResult fires after do produced a result, before run publishes it.
	// A runner that found no host reports and returns before this stage.
	runResult
	// runExited fires as run returns, after its report or result was sent.
	runExited
)

// hostSelector hands out the hosts one execution may try,
// replacing a policy iterator that ran dry while the selection budget allows it.
//
// It is shared by every runner of a speculative execution, so the budget is query-wide:
// a draw, the decision to replace the iterator, and the cap are one critical section.
//
// The budget is maxHosts, the number of up, pooled hosts snapshotted once per query.
// The first iterator is the policy's own answer and is never capped;
// a replacement is drawn only while fewer than maxHosts such hosts were consumed
// and fewer than maxHosts replacements were drawn,
// and it hands out hosts only up to that count.
// A one-shot policy (hostpool.HostPoolHostPolicy, see #812 and #1259) is therefore
// tried on up to one host per up, pooled host and asked for at most 1+maxHosts iterators;
// a policy whose iterator already enumerates the up hosts consumes the budget in its first round
// and is never asked for a replacement, as long as the hosts it holds are the hosts the pool holds.
// When the two differ (a host joining or leaving mid-query, two host IDs behind one connect address)
// an enumerating policy may be asked for a bounded number of further selections.
//
// With maxHosts == 0 the selector is the raw iterator behind a mutex:
// no filtering, no accounting, no replacement.
// That is the shape of a query pinned with SetHostID and of a non-idempotent query.
type hostSelector struct {
	mu sync.Mutex
	// iter is the current iterator.
	iter NextHost
	// pick draws a replacement iterator from the policy; nil for a pinned query.
	pick func() NextHost
	// eligible reports whether a host is up and pooled - the population maxHosts counts.
	// Hosts outside it are skipped:
	// pickForHost would reject them without a fill candidate, and they must not spend budget.
	eligible func(*HostInfo) bool
	// maxHosts is the budget; 0 turns replacement off.
	maxHosts int

	// used counts population hosts consumed from iterators: handed out by draw,
	// or consumed and discarded by advance.
	used int
	// rePicks counts replacement iterators drawn; once it is positive,
	// iter is a replacement, whose draws are capped.
	rePicks int
	// exhausted is set when draw returned nil: every later draw returns nil.
	exhausted bool
}

// charge records a host handed out by the current iterator against the budget.
//
// With maxHosts == 0 nothing is counted and every host is accepted.
// Otherwise only a population host is counted and accepted;
// an ineligible one is rejected and costs nothing.
//
// Parameters:
//   - host: the iterator's selection, non-nil
//
// Returns:
//   - bool: true when the host was accepted
func (s *hostSelector) charge(host SelectedHost) bool {
	if s.maxHosts == 0 {
		return true
	}
	info := host.Info()
	if info == nil || !s.eligible(info) {
		return false
	}
	s.used++
	return true
}

// draw returns the next selection, or nil once the budget is spent.
//
// Ineligible hosts are skipped.
// A dry iterator is replaced while the budget allows;
// a replacement that yields nothing spends a re-pick and is replaced in turn,
// so the re-pick cap ends a policy that keeps sampling hosts the ring has already marked down.
// Once nil is returned it stays nil:
// both counters only grow, and the flag stops re-reading an iterator that came back to life.
//
// Returns:
//   - SelectedHost: the host to try, or nil
func (s *hostSelector) draw() SelectedHost {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		if s.exhausted {
			return nil
		}
		if s.rePicks > 0 && s.used >= s.maxHosts {
			s.exhausted = true
			return nil
		}

		host := s.iter()
		if host != nil {
			if s.charge(host) {
				return host
			}
			continue
		}

		if s.used >= s.maxHosts || s.rePicks >= s.maxHosts {
			s.exhausted = true
			return nil
		}
		s.rePicks++
		s.iter = s.pick()
	}
}

// advance calls the current iterator once and discards the result.
//
// It is the retry policy's terminal pass:
// the selection made there is thrown away, so no replacement may be spent on it,
// but the raw call is kept so policies with lazy internal state (TokenAware's fallback Pick)
// see exactly the calls they always saw.
// A population host consumed this way is charged:
// it was part of the enumeration,
// and a speculative sibling must not read the shortened round as grounds for a replacement.
func (s *hostSelector) advance() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if host := s.iter(); host != nil {
		s.charge(host)
	}
}

// fillCandidate is a host whose pool was empty with a fill in flight,
// so a query may wait for that fill instead of failing fast.
type fillCandidate struct {
	host SelectedHost
	pool *hostConnPool
}

type queryExecutor struct {
	pool   *policyConnPool
	policy HostSelectionPolicy

	// testBeforeSnapshot runs in do between a nil Pick and pickOrState.
	// Nil in production.
	testBeforeSnapshot func()
	// testAfterWake runs in awaitFill before every snapshot,
	// including the first one.
	// Nil in production.
	testAfterWake func()
	// testBeforePickOrState runs in awaitFill after the snapshot and before each
	// candidate is inspected, outside every lock.
	// Nil in production.
	testBeforePickOrState func()
	// testBeforeWait runs in awaitFill immediately before it blocks on a
	// generation.
	// Nil in production.
	testBeforeWait func()
	// testRunHook runs at run's checkpoints.
	// Nil in production.
	testRunHook func(stage runStage)
	// testAfterSnapshot runs in executeQuery after the selector took its
	// budget snapshot and before the first draw.
	// Nil in production.
	testAfterSnapshot func()
}

func (q *queryExecutor) attemptQuery(ctx context.Context, qry internalRequest, conn *Conn) *Iter {
	start := time.Now()
	iter := qry.execute(ctx, conn)
	end := time.Now()

	qry.attempt(q.pool.keyspace, end, start, iter, conn.host)

	return iter
}

// coordinate runs the main execution plus the speculative ones and returns the
// first outcome that decides the query.
//
// A runner that never reached a host reports on its own channel instead of
// producing an iterator, so a no-host report can never beat a sibling that is
// waiting for a fill.
// The query fails with ErrNoConnections as soon as every launched runner reported
// no host, whether or not a further launch is still scheduled.
//
// Parameters:
//   - ctx: cancelled by executeQuery once a result is returned
//   - sp: the speculative execution policy; sp.Attempts() extra runners
//   - sel: the selector shared by every runner
//
// Returns:
//   - *Iter: the winning iterator, or an error iterator
func (q *queryExecutor) coordinate(ctx context.Context, qry internalRequest, sp SpeculativeExecutionPolicy,
	sel *hostSelector) *Iter {
	// remaining counts scheduled launches that have not started yet.
	remaining := sp.Attempts()
	results := make(chan *Iter, 1+remaining)
	// Buffered so a runner reporting no host never blocks.
	noHostCh := make(chan struct{}, 1+remaining)
	launched, noHost := 1, 0

	go q.run(ctx, qry, sel, results, noHostCh)

	var tick <-chan time.Time
	if remaining > 0 {
		ticker := time.NewTicker(sp.Delay())
		defer ticker.Stop()
		tick = ticker.C
	}

	for {
		select {
		case iter := <-results:
			// Any real result wins.
			return iter
		case <-noHostCh:
			// A no-host report never consumes a launch.
			noHost++
			// noHost == launched means no launched runner is still viable, so the
			// query must not stay alive until the next speculative tick: with a
			// long delay that wait outlives Session.Timeout, and the query context
			// defaults to context.Background(), so Session.Close cannot release it.
			// A runner waiting for a fill has not reported, so it keeps
			// noHost < launched and the sibling protection intact, and a launch
			// that has not started yet cannot help either: a no-host report means
			// the shared selector is exhausted, and it stays exhausted.
			if noHost == launched {
				return newErrIter(ErrNoConnections, qry.getQueryMetrics(), qry.Keyspace(),
					qry.getRoutingInfo(), qry.getKeyspaceFunc())
			}
		case <-tick:
			// Only the ticker launches.
			remaining--
			launched++
			go q.run(ctx, qry, sel, results, noHostCh)
			if remaining == 0 {
				tick = nil
			}
		case <-ctx.Done():
			return newErrIter(ctx.Err(), qry.getQueryMetrics(), qry.Keyspace(), qry.getRoutingInfo(), qry.getKeyspaceFunc())
		}
	}
}

func (q *queryExecutor) executeQuery(qry internalRequest) (*Iter, error) {
	var hostIter NextHost

	// check if the host id is specified for the query,
	// if it is, the query should be executed at the corresponding host.
	if hostID := qry.GetHostID(); hostID != "" {
		host, ok := q.pool.session.ring.getHost(hostID)
		if !ok {
			return nil, ErrNoConnections
		}
		var returnedHostOnce int32 = 0
		hostIter = func() SelectedHost {
			if atomic.CompareAndSwapInt32(&returnedHostOnce, 0, 1) {
				return (*selectedHost)(host)
			}
			return nil
		}
	}

	// if host is not specified for the query,
	// then a host will be picked by HostSelectionPolicy.
	sel := &hostSelector{iter: hostIter}
	if hostIter == nil {
		sel.iter = q.policy.Pick(qry)
		// Only an idempotent query may be moved to a replacement host: a pinned
		// query's iterator is deliberately bound to one host, and a non-idempotent
		// query keeps the host sequence it always had.
		if qry.IsIdempotent() {
			sel.pick = func() NextHost { return q.policy.Pick(qry) }
			sel.eligible = q.pooledUp
			// The budget is snapshotted once so it is query-local and fixed:
			// read live it could grow under a stream of host additions,
			// which would leave a custom RetryPolicy whose Attempt never returns false
			// without a termination proof.
			// Missing a host that comes up mid-query is the conservative direction.
			sel.maxHosts = q.pool.upHostCount()
		}
	}
	if q.testAfterSnapshot != nil {
		q.testAfterSnapshot()
	}

	// check if the query is not marked as idempotent, if
	// it is, we force the policy to NonSpeculative
	sp := qry.speculativeExecutionPolicy()
	if qry.GetHostID() != "" || !qry.IsIdempotent() || sp.Attempts() == 0 {
		return q.do(qry.Context(), qry, sel), nil
	}

	ctx, cancel := context.WithCancel(qry.Context())
	defer cancel()

	// The speculative executions are launched _in addition_ to the main execution, on a timer.
	// So Speculation{2} would make 3 executions running in total.
	// They share sel, whose mutex serializes their draws.
	return q.coordinate(ctx, qry, sp, sel), nil
}

// upPool returns host's registered pool if host is up.
//
// It is the admission predicate shared by the selector's budget
// (the population upHostCount measures) and by pickForHost.
//
// Parameters:
//   - host: the host to judge
//
// Returns:
//   - *hostConnPool: the pool, when the host is up and pooled
//   - bool: true when the host is up and pooled
func (q *queryExecutor) upPool(host *HostInfo) (*hostConnPool, bool) {
	if !host.IsUp() {
		return nil, false
	}
	return q.pool.getPool(host)
}

// pooledUp reports whether host is up and holds a registered pool.
//
// Parameters:
//   - host: the host to judge
//
// Returns:
//   - bool: true when the host is up and pooled
func (q *queryExecutor) pooledUp(host *HostInfo) bool {
	_, ok := q.upPool(host)
	return ok
}

// do runs qry against the hosts sel hands out, applying the retry policy between
// attempts.
//
// Parameters:
//   - ctx: cancels the execution
//   - qry: the statement to run
//   - sel: hands out hosts until it reports exhaustion by returning nil; see
//     hostSelector for the budget that governs replacement iterators
//
// Returns:
//   - *Iter: the query's iterator, or an error iterator
func (q *queryExecutor) do(ctx context.Context, qry internalRequest, sel *hostSelector) *Iter {
	selectedHost := sel.draw()
	rt := qry.retryPolicy()

	var lastErr error
	var iter *Iter
	// cands stays nil until a host's pool is found empty with a fill in flight.
	var cands []fillCandidate
	for {
		if selectedHost == nil {
			// The hosts are exhausted; wait for a fill before giving up.
			if lastErr != nil || len(cands) == 0 {
				break
			}

			conn, host, err := q.awaitFill(ctx, cands)
			cands = nil
			if err != nil {
				return newErrIter(err, qry.getQueryMetrics(), qry.Keyspace(), qry.getRoutingInfo(), qry.getKeyspaceFunc())
			}
			if conn == nil {
				break
			}

			selectedHost = host
			iter = q.attemptQuery(ctx, qry, conn)
		} else {
			var conn *Conn
			conn, cands = q.pickForHost(selectedHost, cands)
			if conn == nil {
				selectedHost = sel.draw()
				continue
			}

			iter = q.attemptQuery(ctx, qry, conn)
		}

		iter.host = selectedHost.Info()
		// Update host
		switch iter.err {
		case context.Canceled, context.DeadlineExceeded, ErrNotFound:
			// those errors represents logical errors, they should not count
			// toward removing a node from the pool
			selectedHost.Mark(nil)
			return iter
		default:
			selectedHost.Mark(iter.err)
		}

		// Exit if the query was successful
		// or query is not idempotent or no retry policy defined
		if iter.err == nil || !qry.IsIdempotent() || rt == nil {
			return iter
		}

		attemptsReached := !rt.Attempt(qry)
		retryType := rt.GetRetryType(iter.err)

		var stopRetries bool

		// If query is unsuccessful, check the error with RetryPolicy to retry
		switch retryType {
		case Retry:
			// retry on the same host
		case RetryNextHost:
			// retry on the next host
			if attemptsReached {
				// The terminal pass discards its selection below; advance the iterator
				// once, as always, without spending a replacement on it.
				sel.advance()
			} else {
				selectedHost = sel.draw()
			}
		case Ignore:
			iter.err = nil
			stopRetries = true
		case Rethrow:
			stopRetries = true
		default:
			// Undefined? Return nil and error, this will panic in the requester
			return newErrIter(ErrUnknownRetryType, qry.getQueryMetrics(), qry.Keyspace(), qry.getRoutingInfo(), qry.getKeyspaceFunc())
		}

		if stopRetries || attemptsReached {
			return iter
		}

		lastErr = iter.err
		continue
	}

	if lastErr != nil {
		return newErrIter(lastErr, qry.getQueryMetrics(), qry.Keyspace(), qry.getRoutingInfo(), qry.getKeyspaceFunc())
	}

	return newErrIter(ErrNoConnections, qry.getQueryMetrics(), qry.Keyspace(), qry.getRoutingInfo(), qry.getKeyspaceFunc())
}

// pickForHost selects a connection for selectedHost.
//
// When the host's pool turns out to be empty with a fill in flight,
// the host is recorded as a fill candidate,
// so the caller can wait for that fill once the hosts are exhausted.
// The recheck after a nil Pick is a single read of the pool,
// so a connection cannot disappear between deciding "no connection" and deciding "no fill".
//
// Parameters:
//   - selectedHost: the host to serve the query from
//   - cands: the fill candidates collected so far
//
// Returns:
//   - *Conn: a usable connection, or nil when the caller should move to the next host
//   - []fillCandidate: cands, extended when this host is worth waiting for
func (q *queryExecutor) pickForHost(selectedHost SelectedHost, cands []fillCandidate) (*Conn, []fillCandidate) {
	host := selectedHost.Info()
	if host == nil {
		return nil, cands
	}
	pool, ok := q.upPool(host)
	if !ok {
		return nil, cands
	}

	conn := pool.Pick()
	if conn != nil {
		return conn, cands
	}

	if q.testBeforeSnapshot != nil {
		q.testBeforeSnapshot()
	}

	// Pick already published a fill claim if one was due, so this recheck never
	// schedules one of its own.
	conn, state := pool.pickOrState()
	if state == poolEmptyPending {
		cands = append(cands, fillCandidate{host: selectedHost, pool: pool})
	}

	return conn, cands
}

// awaitFill waits until one of the candidate pools produces a connection or until
// every candidate has stopped being worth waiting for.
//
// All candidates share one absolute deadline.
// The timer rules mirror those of a request on a connection:
// a caller deadline wins over Session.Timeout,
// and a non-positive Session.Timeout waits on the context alone.
//
// Parameters:
//   - ctx: the query context
//   - cands: pools that were empty with a fill in flight; filtered in place
//
// Returns:
//   - *Conn: the connection to attempt the query on, nil when none appeared
//   - SelectedHost: the host that connection belongs to
//   - error: ErrNoConnections when no candidate is left or the timeout expired,
//     ctx.Err() on cancellation, ErrSessionClosed once the session's pool is closed
func (q *queryExecutor) awaitFill(ctx context.Context, cands []fillCandidate) (*Conn, SelectedHost, error) {
	var timeoutCh <-chan time.Time
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		if timeout := q.pool.session.cfg.Timeout; timeout > 0 {
			timer := time.NewTimer(timeout)
			defer timer.Stop()
			timeoutCh = timer.C
		}
	}

	for {
		if q.testAfterWake != nil {
			q.testAfterWake()
		}

		// The generation is captured before any pool is read, so a wake that
		// happens during the reads below cannot be lost.
		gen, parentClosed, kept := q.pool.snapshot(cands)
		if parentClosed {
			return nil, nil, ErrSessionClosed
		}

		cands = kept[:0]
		for _, cand := range kept {
			if q.testBeforePickOrState != nil {
				q.testBeforePickOrState()
			}

			conn, state := cand.pool.pickOrState()
			switch state {
			case poolPicked:
				return conn, cand.host, nil
			case poolEmptyPending:
				// A fill is scheduled or running: still worth waiting for.
				cands = append(cands, cand)
			default:
				// Closed, saturated, or empty and idle: nothing to wait for.
			}
		}

		if len(cands) == 0 {
			return nil, nil, ErrNoConnections
		}

		if q.testBeforeWait != nil {
			q.testBeforeWait()
		}

		select {
		case <-gen:
		case <-timeoutCh:
			return nil, nil, ErrNoConnections
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-q.pool.session.ctx.Done():
			return nil, nil, ErrSessionClosed
		}
	}
}

func (q *queryExecutor) run(ctx context.Context, qry internalRequest, sel *hostSelector, results chan<- *Iter,
	noHost chan<- struct{}) {
	if q.testRunHook != nil {
		q.testRunHook(runEntered)
		defer q.testRunHook(runExited)
	}

	// Coordination teardown: parent at coordinate selects on <-results, <-noHost or <-ctx.Done().
	// If q.do panics, no result is sent and the parent waits until ctx cancel.
	// Push a panic-error iter so the parent unblocks immediately.
	defer recoverGoroutine(q.pool.session.logger, "queryExecutor.run", func(err error) {
		errIter := newErrIter(err, qry.getQueryMetrics(), qry.Keyspace(),
			qry.getRoutingInfo(), qry.getKeyspaceFunc())
		select {
		case results <- errIter:
		case <-ctx.Done():
		}
	})

	// Speculative runners share one selector, so its budget is query-wide
	// and coordinate's no-host accounting can rely on it:
	// a runner that found no host has exhausted the selection for every runner, launched or not.
	// See #812.
	iter := q.do(ctx, qry, sel)
	if iter.err == ErrNoConnections {
		// do returns ErrNoConnections only when it never reached a host, so a
		// sibling still waiting for a fill must not lose to this report.
		if q.testRunHook != nil {
			q.testRunHook(runNoHost)
		}
		select {
		case noHost <- struct{}{}:
		case <-ctx.Done():
		}
		return
	}

	if q.testRunHook != nil {
		q.testRunHook(runResult)
	}
	select {
	case results <- iter:
	case <-ctx.Done():
	}
}

type queryOptions struct {
	stmt string

	// Paging
	pageSize        int
	disableAutoPage bool

	// Monitoring
	trace    Tracer
	observer QueryObserver

	// Parameters
	values  []interface{}
	binding func(q *QueryInfo) ([]interface{}, error)

	// Timestamp
	defaultTimestamp      bool
	defaultTimestampValue int64

	// Consistency
	serialCons SerialConsistency

	// Protocol flag
	disableSkipMetadata bool

	customPayload     map[string][]byte
	prefetch          float64
	rt                RetryPolicy
	spec              SpeculativeExecutionPolicy
	context           context.Context
	idempotent        bool
	keyspace          string
	skipPrepare       bool
	routingKey        []byte
	nowInSecondsValue *int
	hostID            string

	// getKeyspace is field so that it can be overriden in tests
	getKeyspace func() string
}

func newQueryOptions(q *Query, ctx context.Context) *queryOptions {
	var newRoutingKey []byte
	if q.routingKey != nil {
		routingKey := q.routingKey
		newRoutingKey = make([]byte, len(routingKey))
		copy(newRoutingKey, routingKey)
	}
	if ctx == nil {
		ctx = q.Context()
	}
	return &queryOptions{
		stmt:                  q.stmt,
		values:                q.values,
		pageSize:              q.pageSize,
		prefetch:              q.prefetch,
		trace:                 q.trace,
		observer:              q.observer,
		rt:                    q.rt,
		spec:                  q.spec,
		binding:               q.binding,
		serialCons:            q.serialCons,
		defaultTimestamp:      q.defaultTimestamp,
		defaultTimestampValue: q.defaultTimestampValue,
		disableSkipMetadata:   q.disableSkipMetadata,
		context:               ctx,
		idempotent:            q.idempotent,
		customPayload:         q.customPayload,
		disableAutoPage:       q.disableAutoPage,
		skipPrepare:           q.skipPrepare,
		routingKey:            newRoutingKey,
		getKeyspace:           q.getKeyspace,
		nowInSecondsValue:     q.nowInSecondsValue,
		keyspace:              q.keyspace,
		hostID:                q.hostID,
	}
}

type internalQuery struct {
	originalQuery      *Query
	qryOpts            *queryOptions
	pageState          []byte
	conn               *Conn
	consistency        uint32
	session            *Session
	routingInfo        *queryRoutingInfo
	metrics            *queryMetrics
	hostMetricsManager hostMetricsManager
}

func newInternalQuery(q *Query, ctx context.Context) *internalQuery {
	var newPageState []byte
	if q.initialPageState != nil {
		pageState := q.initialPageState
		newPageState = make([]byte, len(pageState))
		copy(newPageState, pageState)
	}
	var hostMetricsMgr hostMetricsManager
	if q.observer != nil {
		hostMetricsMgr = newHostMetricsManager()
	} else {
		hostMetricsMgr = emptyHostMetricsManager
	}
	return &internalQuery{
		originalQuery:      q,
		qryOpts:            newQueryOptions(q, ctx),
		metrics:            &queryMetrics{},
		hostMetricsManager: hostMetricsMgr,
		consistency:        uint32(q.initialConsistency),
		pageState:          newPageState,
		conn:               nil,
		session:            q.session,
		routingInfo:        &queryRoutingInfo{},
	}
}

// Attempts returns the number of times the query was executed.
func (q *internalQuery) Attempts() int {
	return q.metrics.attempts()
}

func (q *internalQuery) attempt(keyspace string, end, start time.Time, iter *Iter, host *HostInfo) {
	latency := end.Sub(start)
	attempt := q.metrics.attempt(latency)

	if q.qryOpts.observer != nil {
		metricsForHost := q.hostMetricsManager.attempt(latency, host)
		q.qryOpts.observer.ObserveQuery(q.qryOpts.context, ObservedQuery{
			Keyspace:  keyspace,
			Statement: q.qryOpts.stmt,
			Values:    q.qryOpts.values,
			Start:     start,
			End:       end,
			Rows:      iter.numRows,
			Host:      host,
			Metrics:   metricsForHost,
			Err:       iter.err,
			Attempt:   attempt,
			Query:     q.originalQuery,
		})
	}
}

func (q *internalQuery) execute(ctx context.Context, conn *Conn) *Iter {
	return conn.executeQuery(ctx, q)
}

func (q *internalQuery) retryPolicy() RetryPolicy {
	return q.qryOpts.rt
}

func (q *internalQuery) speculativeExecutionPolicy() SpeculativeExecutionPolicy {
	return q.qryOpts.spec
}

func (q *internalQuery) GetRoutingKey() ([]byte, error) {
	if q.qryOpts.routingKey != nil {
		return q.qryOpts.routingKey, nil
	}

	if q.qryOpts.binding != nil && len(q.qryOpts.values) == 0 {
		// If this query was created using session.Bind we wont have the query
		// values yet, so we have to pass down to the next policy.
		// TODO: Remove this and handle this case
		return nil, nil
	}

	// try to determine the routing key
	meta, err := q.session.routingStatementMetadata(q.Context(), q.qryOpts.stmt, q.qryOpts.keyspace)
	if err != nil {
		return nil, err
	}

	if meta != nil {
		q.routingInfo.set(meta.Keyspace, meta.Table)
	}
	return createRoutingKey(meta, q.qryOpts.values)
}

func (q *internalQuery) Keyspace() string {
	if q.qryOpts.getKeyspace != nil {
		return q.qryOpts.getKeyspace()
	}

	qrKs := q.routingInfo.getKeyspace()
	if qrKs != "" {
		return qrKs
	}
	if q.qryOpts.keyspace != "" {
		return q.qryOpts.keyspace
	}

	if q.session == nil {
		return ""
	}
	// TODO(chbannis): this should be parsed from the query or we should let
	// this be set by users.
	return q.session.cfg.Keyspace
}

func (q *internalQuery) Table() string {
	return q.routingInfo.getTable()
}

func (q *internalQuery) IsIdempotent() bool {
	return q.qryOpts.idempotent
}

func (q *internalQuery) getQueryMetrics() *queryMetrics {
	return q.metrics
}

func (q *internalQuery) SetConsistency(c Consistency) {
	atomic.StoreUint32(&q.consistency, uint32(c))
}

func (q *internalQuery) GetConsistency() Consistency {
	return Consistency(atomic.LoadUint32(&q.consistency))
}

func (q *internalQuery) Context() context.Context {
	return q.qryOpts.context
}

func (q *internalQuery) Statement() Statement {
	return q.originalQuery
}

func (q *internalQuery) GetHostID() string {
	return q.qryOpts.hostID
}

func (q *internalQuery) getRoutingInfo() *queryRoutingInfo {
	return q.routingInfo
}

func (q *internalQuery) getKeyspaceFunc() func() string {
	return q.qryOpts.getKeyspace
}

type batchOptions struct {
	trace    Tracer
	observer BatchObserver

	bType   BatchType
	entries []BatchEntry

	defaultTimestamp      bool
	defaultTimestampValue int64

	serialCons SerialConsistency

	customPayload map[string][]byte
	rt            RetryPolicy
	spec          SpeculativeExecutionPolicy
	context       context.Context
	keyspace      string
	idempotent    bool
	routingKey    []byte
	nowInSeconds  *int
}

func newBatchOptions(b *Batch, ctx context.Context) *batchOptions {
	// make a new array so if user keeps appending entries on the Batch object it doesn't affect this execution
	newEntries := make([]BatchEntry, len(b.Entries))
	for i, e := range b.Entries {
		newEntries[i] = e
	}
	var newRoutingKey []byte
	if b.routingKey != nil {
		routingKey := b.routingKey
		newRoutingKey = make([]byte, len(routingKey))
		copy(newRoutingKey, routingKey)
	}
	if ctx == nil {
		ctx = b.Context()
	}
	return &batchOptions{
		bType:                 b.Type,
		entries:               newEntries,
		customPayload:         b.CustomPayload,
		rt:                    b.rt,
		spec:                  b.spec,
		trace:                 b.trace,
		observer:              b.observer,
		serialCons:            b.serialCons,
		defaultTimestamp:      b.defaultTimestamp,
		defaultTimestampValue: b.defaultTimestampValue,
		context:               ctx,
		keyspace:              b.Keyspace(),
		idempotent:            b.IsIdempotent(),
		routingKey:            newRoutingKey,
		nowInSeconds:          b.nowInSeconds,
	}
}

type internalBatch struct {
	originalBatch      *Batch
	batchOpts          *batchOptions
	consistency        uint32
	routingInfo        *queryRoutingInfo
	session            *Session
	metrics            *queryMetrics
	hostMetricsManager hostMetricsManager
}

func newInternalBatch(batch *Batch, ctx context.Context) *internalBatch {
	var hostMetricsMgr hostMetricsManager
	if batch.observer != nil {
		hostMetricsMgr = newHostMetricsManager()
	} else {
		hostMetricsMgr = emptyHostMetricsManager
	}
	return &internalBatch{
		originalBatch:      batch,
		batchOpts:          newBatchOptions(batch, ctx),
		routingInfo:        &queryRoutingInfo{},
		session:            batch.session,
		consistency:        uint32(batch.GetConsistency()),
		metrics:            &queryMetrics{},
		hostMetricsManager: hostMetricsMgr,
	}
}

// Attempts returns the number of attempts made to execute the batch.
func (b *internalBatch) Attempts() int {
	return b.metrics.attempts()
}

func (b *internalBatch) attempt(keyspace string, end, start time.Time, iter *Iter, host *HostInfo) {
	latency := end.Sub(start)
	attempt := b.metrics.attempt(latency)

	if b.batchOpts.observer == nil {
		return
	}

	metricsForHost := b.hostMetricsManager.attempt(latency, host)

	statements := make([]string, len(b.batchOpts.entries))
	values := make([][]interface{}, len(b.batchOpts.entries))

	for i, entry := range b.batchOpts.entries {
		statements[i] = entry.Stmt
		values[i] = entry.Args
	}

	b.batchOpts.observer.ObserveBatch(b.batchOpts.context, ObservedBatch{
		Keyspace:   keyspace,
		Statements: statements,
		Values:     values,
		Start:      start,
		End:        end,
		// Rows not used in batch observations // TODO - might be able to support it when using BatchCAS
		Host:    host,
		Metrics: metricsForHost,
		Err:     iter.err,
		Attempt: attempt,
		Batch:   b.originalBatch,
	})
}

func (b *internalBatch) retryPolicy() RetryPolicy {
	return b.batchOpts.rt
}

func (b *internalBatch) speculativeExecutionPolicy() SpeculativeExecutionPolicy {
	return b.batchOpts.spec
}

func (b *internalBatch) GetRoutingKey() ([]byte, error) {
	if b.batchOpts.routingKey != nil {
		return b.batchOpts.routingKey, nil
	}

	if len(b.batchOpts.entries) == 0 {
		return nil, nil
	}

	entry := b.batchOpts.entries[0]
	if entry.binding != nil {
		// bindings do not have the values let's skip it like Query does.
		return nil, nil
	}
	// try to determine the routing key
	meta, err := b.session.routingStatementMetadata(b.Context(), entry.Stmt, b.batchOpts.keyspace)
	if err != nil {
		return nil, err
	}

	if meta != nil {
		b.routingInfo.set(meta.Keyspace, meta.Table)
	}

	return createRoutingKey(meta, entry.Args)
}

func (b *internalBatch) Keyspace() string {
	return b.batchOpts.keyspace
}

func (b *internalBatch) Table() string {
	return b.routingInfo.getTable()
}

func (b *internalBatch) IsIdempotent() bool {
	return b.batchOpts.idempotent
}

func (b *internalBatch) getQueryMetrics() *queryMetrics {
	return b.metrics
}

func (b *internalBatch) SetConsistency(c Consistency) {
	atomic.StoreUint32(&b.consistency, uint32(c))
}

func (b *internalBatch) GetConsistency() Consistency {
	return Consistency(atomic.LoadUint32(&b.consistency))
}

func (b *internalBatch) Context() context.Context {
	return b.batchOpts.context
}

func (b *internalBatch) Statement() Statement {
	return b.originalBatch
}

func (b *internalBatch) GetHostID() string {
	return ""
}

func (b *internalBatch) getRoutingInfo() *queryRoutingInfo {
	return b.routingInfo
}

func (b *internalBatch) getKeyspaceFunc() func() string {
	return nil
}

func (b *internalBatch) execute(ctx context.Context, conn *Conn) *Iter {
	return conn.executeBatch(ctx, b)
}
