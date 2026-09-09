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
	// snapshotForRunner returns a copy one speculative runner owns.
	//
	// Pointer fields stay shared - metrics, routing info and host metrics are
	// per-page, not per-runner.
	// Only the value fields a RetryPolicy may write become per-runner.
	//
	// ctx is the runner context, which the copy's Context reports in place of
	// the caller's: a RetryPolicy consulted inside a runner then sees the
	// cancellation a winning sibling causes, so a backoff nap ends with it.
	snapshotForRunner(ctx context.Context) internalRequest
	// getQueryMetrics returns the counters one page execution shares.
	//
	// Every runner of a speculative execution, and every retry inside a runner,
	// accounts against the same completed-attempt count, and a per-runner
	// snapshot keeps sharing it: the count a RetryPolicy reads through Attempts
	// is per page execution, not per runner.
	//
	// It is not an admission cap on requests. attemptQuery sends the request and
	// records the attempt only afterwards, and a speculative launch never
	// consults the retry policy at all, so several requests can already be in
	// flight before the first Attempt decision is taken; NumRetries+1 is
	// therefore not a hard ceiling on the number of requests sent.
	//
	// It does not carry across pages: the request that fetches the next page
	// starts from a fresh queryMetrics, so the count resets per page.
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
	// runRetired fires just before run publishes that its execution retired.
	runRetired
	// runResult fires after do produced a decisive result, before run publishes it.
	// A runner whose execution retired publishes and returns before this stage.
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
	// testBeforeDrainClose runs in drainRunners on each drained iterator, before it
	// is closed, and carries no cleanup of its own.
	// Nil in production.
	testBeforeDrainClose func(iter *Iter)
	// testAfterConsume runs in coordinate on the coordinate goroutine, immediately
	// after each terminal message is counted and before it is acted on; consumed is
	// the running count.
	// drainRunners deliberately does not call it: what it reports is the accounting
	// coordinate did, not the messages the drain took afterwards.
	// Nil in production.
	testAfterConsume func(consumed int)
	// testBeforeRetiredClose runs in coordinate on each retirement iterator it
	// closes, before it is closed, and carries no cleanup of its own.
	// Every coordinator-side close goes through closeRetired, the deferred one
	// included, so this seam sees all of them.
	// Nil in production.
	testBeforeRetiredClose func(iter *Iter)
}

// doOutcome is how do classifies the way one execution ended.
//
// run and coordinate act on this value alone.
// The iterator's error never classifies an execution: the same error value is a
// decisive result under one retry policy answer and a retirement under another.
type doOutcome uint8

const (
	// outcomeResult is a decisive result: it ends the query on its own.
	//
	// A success, an Ignore or a Rethrow, an error no retry can follow, a
	// cancellation, or an M5 checkpoint.
	outcomeResult doOutcome = iota
	// outcomeRetiredAttempted is a retirement by an execution that reached a host:
	// it wanted a further attempt and could not have one.
	//
	// It carries the last attempt's own iterator, host and framer included.
	outcomeRetiredAttempted
	// outcomeRetiredUnattempted is a retirement by an execution that never reached
	// a host.
	//
	// It carries an ErrNoConnections error iterator, which has no framer.
	outcomeRetiredUnattempted
)

// retirement is the message a runner publishes when its execution retired.
//
// attempted is what orders two retirements against each other: a retirement that
// reached a host is preferred over one that did not.
type retirement struct {
	// iter is the retiring execution's iterator; never nil.
	iter *Iter
	// attempted reports whether the execution reached a host.
	attempted bool
}

// closeIfHeld closes iter when the frame that produced it is still holding it.
//
// It is the cleanup half of the ownership discipline in attemptQuery and do:
// the holder clears its owned reference at the hand-over, so a non-nil iter here means
// the frame is unwinding — a user callback panicked — with an iterator nobody receives.
// Closing it returns the response framer to the pool;
// these iterators never carry a leak detector, so nothing else would ever reclaim them.
//
// Parameters:
//   - iter: the held iterator, or nil once it was handed over
func closeIfHeld(iter *Iter) {
	if iter != nil {
		iter.Close()
	}
}

// attemptQuery runs one attempt of qry on conn and reports it to the request's observer.
//
// The observer is user code that runs while this frame owns the iterator,
// so the iterator is closed if it panics; on a normal return it is the caller's.
// This is a pure cleanup defer: it does not recover, so a panic reaches do
// and the synchronous caller of do unchanged.
//
// Parameters:
//   - ctx: cancels the attempt
//   - qry: the statement to run
//   - conn: the connection to run it on
//
// Returns:
//   - *Iter: the attempt's iterator, owned by the caller
func (q *queryExecutor) attemptQuery(ctx context.Context, qry internalRequest, conn *Conn) *Iter {
	start := time.Now()
	iter := qry.execute(ctx, conn)
	end := time.Now()

	owned := iter
	defer func() { closeIfHeld(owned) }()

	qry.attempt(q.pool.keyspace, end, start, iter, conn.host)

	owned = nil
	return iter
}

// coordinate runs the main execution plus the speculative ones and returns the first outcome that decides the query.
//
// A runner whose execution retired - it wanted a further attempt and could not have one -
// publishes on its own channel instead of deciding the query,
// so it can never beat a sibling that is still running.
// Retirements are held rather than returned while any launched runner has not published:
// only when every launched runner has retired is one of their iterators returned,
// and a launch that has not started yet is not waited for.
// That stop rule is the same one the no-host accounting used before it:
// waiting for the next speculative tick would let the query outlive Session.Timeout,
// and the query context defaults to context.Background(),
// so Session.Close could not release it.
// Which retirement is returned follows one rule:
// a retirement that reached a host beats one that did not,
// and between two of the same kind the one consumed last wins.
// "Consumed" is the order this goroutine takes messages off the channels,
// unrelated to the order the runners produced them.
//
// Every launched runner publishes exactly one terminal message, on results or on retiredCh;
// consumed counts the ones this frame took.
// The runners whose message nobody read are handed to drainRunners,
// which closes their iterators.
// The winner is not among them: it was counted as consumed.
//
// Returning does not wait for that cleanup.
// While any launched runner has not published, a drain goroutine may exist indefinitely:
// Session.Close cancels the session context and closes the session's resources,
// but it does not join the runners,
// and some of the waits a runner can sit in are not bounded by any context -
// an accepted socket write, or a custom retry or selection callback.
// An ExponentialBackoffRetryPolicy nap is no longer one of them:
// it ends with the runner context, which executeQuery cancels once this frame returns.
// That lifetime is unchanged by this cleanup; see drainRunners for what it guarantees.
//
// Parameters:
//   - ctx: cancelled by executeQuery once a result is returned
//   - qry: the request every runner takes its own snapshot of
//   - sp: the speculative execution policy; sp.Attempts() extra runners
//   - sel: the selector shared by every runner
//
// Returns:
//   - *Iter: the winning iterator, or an error iterator
func (q *queryExecutor) coordinate(ctx context.Context, qry internalRequest, sp SpeculativeExecutionPolicy,
	sel *hostSelector) *Iter {
	// remaining counts scheduled launches that have not started yet.
	remaining := sp.Attempts()
	if remaining < 0 {
		// A negative count would make the capacities below smaller than the number of
		// runners, and a capacity under -1 is not even representable.
		remaining = 0
	}
	results := make(chan *Iter, 1+remaining)
	// Buffered so a retiring runner never blocks.
	retiredCh := make(chan retirement, 1+remaining)
	launched, retired := 1, 0
	// consumed counts the terminal messages taken off the two channels.
	consumed := 0
	// held is the retirement candidate this frame owns, heldAttempted its kind.
	var held *Iter
	heldAttempted := false
	// incoming is a retirement iterator taken off the channel but not yet placed,
	// so an unwinding frame still has a reference to close.
	var incoming *Iter

	// The one cleanup point, installed before the first launch: sp.Delay() below runs
	// when a runner already exists and is user code that may panic.
	// Both counts are final when this runs: only the ticker raises launched, and the
	// ticker is gone once the loop has returned.
	defer func() {
		if outstanding := launched - consumed; outstanding > 0 {
			go q.drainRunners(results, retiredCh, outstanding)
		}
	}()

	// Installed after the drain defer, so it runs before it.
	// The two are independent: consumed is raised as a retirement is taken off the
	// channel, before any of the handling below, so the drain's outstanding count
	// already excludes incoming and only this cleanup can reclaim it.
	defer func() {
		q.closeRetired(incoming)
		q.closeRetired(held)
	}()

	// Every runner, the main one included, gets its own snapshot: sharing one
	// request would let a RetryPolicy's SetConsistency on one runner decide what
	// a sibling sends next, and would leave the next-page copy reading a field a
	// sibling writes.
	// Giving the main runner the original instead would only be safe under the
	// extra premise that nobody writes the original.
	go q.run(ctx, qry.snapshotForRunner(ctx), sel, results, retiredCh)

	var tick <-chan time.Time
	if remaining > 0 {
		// A non-positive delay means "launch immediately"; NewTicker requires a
		// positive interval, so clamp to the smallest one.
		delay := sp.Delay()
		if delay <= 0 {
			delay = time.Nanosecond
		}
		ticker := time.NewTicker(delay)
		defer ticker.Stop()
		tick = ticker.C
	}

	for {
		select {
		case iter := <-results:
			// Any decisive result wins.
			consumed++
			q.afterConsume(consumed)
			return iter
		case r := <-retiredCh:
			// Taken over first: any panic from here on has this frame's cleanup
			// defer to fall back on.
			incoming = r.iter
			consumed++
			retired++
			q.afterConsume(consumed)

			if held != nil && heldAttempted && !r.attempted {
				// A retirement that reached no host never displaces one that did.
				loser := incoming
				incoming = nil
				q.closeRetired(loser)
			} else {
				loser := held
				held, heldAttempted = incoming, r.attempted
				incoming = nil
				q.closeRetired(loser)
			}

			if retired == launched {
				// No launched runner is still viable; a launch that has not
				// started yet is not waited for.
				if err := ctx.Err(); err != nil {
					// Cancellation is its own terminal state and outranks the
					// last retirement, so the answer does not depend on which
					// case a ready select happened to pick.
					loser := held
					held = nil
					q.closeRetired(loser)
					return newErrIter(err, qry.getQueryMetrics(), qry.Keyspace(),
						qry.getRoutingInfo(), qry.getKeyspaceFunc())
				}
				winner := held
				held = nil
				return winner
			}
		case <-tick:
			// Only the ticker launches.
			remaining--
			launched++
			go q.run(ctx, qry.snapshotForRunner(ctx), sel, results, retiredCh)
			if remaining == 0 {
				tick = nil
			}
		case <-ctx.Done():
			return newErrIter(ctx.Err(), qry.getQueryMetrics(), qry.Keyspace(), qry.getRoutingInfo(), qry.getKeyspaceFunc())
		}
	}
}

// afterConsume runs the testAfterConsume seam for the message just counted.
//
// The seam is test code that runs on coordinate's own goroutine, and coordinate is a
// synchronous call from executeQuery with no recover of its own, so a panic out of the
// seam must not unwind it.
//
// Parameters:
//   - consumed: the running count of terminal messages this frame took
func (q *queryExecutor) afterConsume(consumed int) {
	if q.testAfterConsume == nil {
		return
	}
	safely(q.pool.session.logger, "queryExecutor.coordinate.testAfterConsume",
		func() { q.testAfterConsume(consumed) })
}

// closeRetired closes a retirement iterator coordinate owns and reclaims its framer.
//
// It is the one coordinator-side close, the deferred cleanup included, so every
// retirement iterator that reaches no caller goes through exactly one of these calls.
// The seam and the close are isolated separately: a panicking seam must not cost the
// iterator its Close.
// A panicking Close is not retried, and promises nothing beyond the remaining
// retirements still being handled: Iter.Close marks itself closed before it releases.
//
// Parameters:
//   - iter: the iterator to close, or nil when there is nothing held
func (q *queryExecutor) closeRetired(iter *Iter) {
	if iter == nil {
		return
	}

	logger := q.pool.session.logger
	if q.testBeforeRetiredClose != nil {
		safely(logger, "queryExecutor.coordinate.testBeforeRetiredClose",
			func() { q.testBeforeRetiredClose(iter) })
	}
	safely(logger, "queryExecutor.coordinate.closeRetired", func() { iter.Close() })
}

// drainRunners consumes the terminal messages of the runners coordinate left behind and
// closes the iterators among them.
//
// Those iterators reach no caller, so nothing else would return their response framer to
// the pool: they never carry a leak detector either.
//
// The guarantee is conditional:
//
//	coordinate does not wait for this cleanup.
//	While any launched runner has not published, this goroutine may exist indefinitely.
//	If every launched runner publishes exactly once, and every close completes normally,
//	the drain ends once it has consumed all the remaining messages.
//
// Two shapes are excluded from that termination, both because the runner never publishes:
// a callback that ends its runner with runtime.Goexit, which raises no recoverable panic
// for run's teardown to answer, and a panic inside that teardown itself, which builds its
// error iterator out of the request and so can fail before it ever sends.
//
// A retirement carries an iterator too, so both channels are reclaimed the same way.
// A retirement that never reached a host carries an error iterator with no framer;
// closing it still costs nothing and keeps the two branches one shape.
//
// The seam and each close are isolated separately, and the loop goes on to the next
// message after either panics, so one panicking cleanup cannot strand the messages
// behind it; the recoverGoroutine at the top is the backstop for the loop itself, as it
// is for every driver-spawned goroutine.
//
// Parameters:
//   - results: the channel the runners publish a decisive iterator on
//   - retired: the channel the runners publish a retirement on
//   - outstanding: how many messages are still owed, launched minus consumed
func (q *queryExecutor) drainRunners(results <-chan *Iter, retired <-chan retirement, outstanding int) {
	logger := q.pool.session.logger
	defer recoverGoroutine(logger, "queryExecutor.drainRunners", nil)

	for ; outstanding > 0; outstanding-- {
		select {
		case iter := <-results:
			q.drainClose(logger, iter)
		case r := <-retired:
			q.drainClose(logger, r.iter)
		}
	}
}

// drainClose runs the drain's seam on iter and closes it, each isolated on its own.
//
// Parameters:
//   - logger: the session logger the isolation reports to
//   - iter: the iterator the drain reached
func (q *queryExecutor) drainClose(logger StructuredLogger, iter *Iter) {
	if q.testBeforeDrainClose != nil {
		safely(logger, "queryExecutor.drainRunners.testBeforeDrainClose",
			func() { q.testBeforeDrainClose(iter) })
	}
	safely(logger, "queryExecutor.drainRunners.Close", func() { iter.Close() })
}

// executeQuery runs one page of a request and returns the iterator that answers it.
//
// It is the one place the shape of the execution is chosen.
// A request that is pinned to a host, is not idempotent,
// or whose speculative policy schedules no extra attempt gets a single execution,
// and is answered by it directly:
// with no sibling to wait for,
// a retirement is that query's answer just as a decisive outcome is.
// Everything else is handed to coordinate under a context of this frame's own,
// which is cancelled on the way out
// so the runners that lost stop working towards an answer nobody will read.
//
// The selector is built here rather than inside do because every runner shares it:
// the replacement picks, the up-host budget and the enumeration itself are query-wide.
//
// Parameters:
//   - qry: the request to run
//
// Returns:
//   - *Iter: the iterator answering this page; never nil when the error is nil
//   - error: ErrNoConnections when the request names a host the ring does not hold
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
		// A single execution has no sibling to wait for, so a retirement is this
		// query's answer just as a decisive outcome is: the outcome is ignored.
		iter, _ := q.do(qry.Context(), qry, sel)
		return iter, nil
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
// Ownership of every iterator an attempt produces, one row per way out of the loop:
//
//   - an attempt returns: do takes ownership from attemptQuery
//   - an attempt returns: do holds it in held, and keeps holding it while the selection
//     moves across hosts that yield no connection
//   - the next attempt really starts: the iterator it replaces is closed here
//   - ctx cancelled / ErrNotFound: handed to the caller
//   - success, or not idempotent, or no retry policy: handed to the caller
//   - a checkpoint sees ctx cancelled: this attempt's iterator is handed to the caller,
//     carrying the context's error in place of the attempt's own
//   - Rethrow or Ignore: handed to the caller
//   - unknown retry type: closed here, the caller gets a fresh ErrUnknownRetryType iter
//   - the selection is exhausted or the attempt budget is reached, after at least one
//     attempt: the last attempt's own iterator is handed to the caller as the retirement
//   - a retirement with no attempt behind it: no iterator is held, the caller gets a
//     fresh ErrNoConnections iter
//   - awaitFill fails: no iterator is held
//   - a callback panics: the deferred cleanup closes held
//
// The hosts' Mark, the retry policy's Attempt and GetRetryType, the host selection
// policy's NextHost/Pick (reached through the selector's draw and advance), and
// SelectedHost.Info are user code that runs while do owns an iterator, so a panic out
// of them closes it.
// This is a pure cleanup defer: it does not recover, so the synchronous caller in
// executeQuery still sees the original panic value.
//
// Once ctx is cancelled, do stops consulting the retry policy.
// Three checkpoints straddle the two policy callbacks - before Attempt, between Attempt
// and GetRetryType, and after GetRetryType - and the first one to see a cancelled ctx
// ends the execution: no further callback runs, no further host is drawn, and no further
// attempt is sent.
// The guarantee reaches exactly that far.
// A callback already running is not interrupted,
// and a cancellation that lands after the last checkpoint still finds the loop going
// round.
// The checkpoints sit after the success, non-idempotent and no-policy exits, so an
// attempt that succeeded still returns its result even under a cancelled ctx.
//
// An execution that wanted a further attempt and could not have one retires instead of
// deciding the query: the retry policy asked for Retry or RetryNextHost, and either the
// selection was exhausted or the policy's own Attempt refused the budget.
// Retirement is reported through the returned doOutcome, never through the iterator's
// error: the same error value is decisive under one policy answer and a retirement under
// another.
// A retirement that ran at least one attempt carries that attempt's own iterator, so its
// host, warnings and custom payload stay observable; one that never reached a host
// carries a fresh ErrNoConnections iterator.
//
// Parameters:
//   - ctx: cancels the execution
//   - qry: the statement to run
//   - sel: hands out hosts until it reports exhaustion by returning nil; see
//     hostSelector for the budget that governs replacement iterators
//
// Returns:
//   - *Iter: the query's iterator, or an error iterator
//   - doOutcome: whether that iterator decides the query or retires the execution
func (q *queryExecutor) do(ctx context.Context, qry internalRequest, sel *hostSelector) (*Iter, doOutcome) {
	selectedHost := sel.draw()
	rt := qry.retryPolicy()

	// held is the last attempt's iterator, this frame's to close until it is handed
	// over or the attempt that replaces it really starts.
	// It stays held across hosts that yield no connection, so a retirement several
	// fruitless draws later still carries the attempt that actually ran.
	var held *Iter
	defer func() { closeIfHeld(held) }()
	// retiredUnattempted is the retirement of an execution that never reached a host.
	// It holds no iterator, so there is nothing to hand over.
	retiredUnattempted := func() (*Iter, doOutcome) {
		return newErrIter(ErrNoConnections, qry.getQueryMetrics(), qry.Keyspace(),
			qry.getRoutingInfo(), qry.getKeyspaceFunc()), outcomeRetiredUnattempted
	}
	// cancelled ends the execution at a checkpoint: the attempt's own iterator goes to
	// the caller with the context's error in place of the attempt's, so the host, the
	// framer and the metrics the attempt gathered stay observable and the framer is
	// still returned by the caller's Close.
	// The error is stored bare, so callers can compare it against context.Canceled and
	// context.DeadlineExceeded by identity the way every other exit from do allows.
	// Mark is deliberately not called again: the attempt's own error was marked when it
	// came back, and the cancellation says nothing about the host.
	cancelled := func(iter *Iter, err error) (*Iter, doOutcome) {
		iter.err = err
		held = nil
		return iter, outcomeResult
	}
	// cands stays nil until a host's pool is found empty with a fill in flight.
	var cands []fillCandidate
	for {
		if selectedHost == nil {
			if held != nil {
				// An attempt already ran, so there is nothing left to wait for:
				// retire with that attempt's own iterator.
				retiring := held
				held = nil
				return retiring, outcomeRetiredAttempted
			}
			// The hosts are exhausted; wait for a fill before giving up.
			if len(cands) == 0 {
				break
			}

			conn, host, err := q.awaitFill(ctx, cands)
			cands = nil
			if err == ErrNoConnections {
				// Nothing left to wait for, and no attempt behind it.
				return retiredUnattempted()
			}
			if err != nil {
				// A cancellation or a closed session decides the query.
				return newErrIter(err, qry.getQueryMetrics(), qry.Keyspace(),
					qry.getRoutingInfo(), qry.getKeyspaceFunc()), outcomeResult
			}
			if conn == nil {
				break
			}

			selectedHost = host
			held = q.attemptQuery(ctx, qry, conn)
		} else {
			var conn *Conn
			conn, cands = q.pickForHost(selectedHost, cands)
			if conn == nil {
				selectedHost = sel.draw()
				continue
			}

			// The next attempt really starts here, and only here is the iterator
			// it supersedes closed.
			closeIfHeld(held)
			// Cleared before the attempt runs: if it panics, the deferred cleanup
			// must not close that iterator a second time.
			held = nil
			held = q.attemptQuery(ctx, qry, conn)
		}

		iter := held

		iter.host = selectedHost.Info()
		// Update host
		switch iter.err {
		case context.Canceled, context.DeadlineExceeded, ErrNotFound:
			// those errors represents logical errors, they should not count
			// toward removing a node from the pool
			selectedHost.Mark(nil)
			held = nil
			return iter, outcomeResult
		default:
			selectedHost.Mark(iter.err)
		}

		// Exit if the query was successful
		// or query is not idempotent or no retry policy defined
		if iter.err == nil || !qry.IsIdempotent() || rt == nil {
			held = nil
			return iter, outcomeResult
		}

		// If query is unsuccessful, check the error with RetryPolicy to retry
		if err := ctx.Err(); err != nil {
			return cancelled(iter, err)
		}
		attemptsReached := !rt.Attempt(qry)
		if err := ctx.Err(); err != nil {
			return cancelled(iter, err)
		}
		retryType := rt.GetRetryType(iter.err)
		if err := ctx.Err(); err != nil {
			return cancelled(iter, err)
		}
		next, step := planRetry(retryType, sel, selectedHost, attemptsReached)

		switch {
		case step == retryStepUnknown:
			// Undefined? Return nil and error, this will panic in the requester
			iter.Close()
			held = nil
			return newErrIter(ErrUnknownRetryType, qry.getQueryMetrics(), qry.Keyspace(),
				qry.getRoutingInfo(), qry.getKeyspaceFunc()), outcomeResult
		case step == retryStepIgnore:
			iter.err = nil
			held = nil
			return iter, outcomeResult
		case step == retryStepStop:
			held = nil
			return iter, outcomeResult
		case attemptsReached:
			// The policy refused a further attempt for a retryable error: this
			// execution can do no more, but it does not decide the query.
			held = nil
			return iter, outcomeRetiredAttempted
		}

		// held keeps this attempt's iterator: next may hand out hosts that yield no
		// connection before one of them does, and the retirement must still carry
		// the attempt that ran.
		selectedHost = next
	}

	return retiredUnattempted()
}

// retryStep is what a retry policy's answer means to do's loop.
type retryStep uint8

const (
	// retryStepAgain runs a further attempt, subject to the attempt budget.
	retryStepAgain retryStep = iota
	// retryStepIgnore hands the iterator to the caller with its error cleared.
	retryStepIgnore
	// retryStepStop hands the iterator to the caller as it stands.
	retryStepStop
	// retryStepUnknown reports an answer outside the defined set.
	retryStepUnknown
)

// planRetry turns a retry policy's answer into do's next move,
// moving the selection on when the policy asked for another host.
//
// It holds no iterator and closes none: do keeps ownership across the call and acts on
// the step returned, so every hand-over and every Close stays in do's own frame.
//
// Parameters:
//   - retryType: the policy's answer for this attempt's error
//   - sel: the selector to advance or draw the next host from
//   - current: the host this attempt ran on
//   - attemptsReached: true when the policy refused a further attempt
//
// Returns:
//   - SelectedHost: the host a further attempt would run on; current when the policy
//     asked to stay put or when no further attempt follows
//   - retryStep: what do does next
func planRetry(retryType RetryType, sel *hostSelector, current SelectedHost, attemptsReached bool) (SelectedHost, retryStep) {
	switch retryType {
	case Retry:
		// retry on the same host
		return current, retryStepAgain
	case RetryNextHost:
		// retry on the next host
		if attemptsReached {
			// The terminal pass discards its selection; advance the iterator once,
			// as always, without spending a replacement on it.
			sel.advance()
			return current, retryStepAgain
		}
		return sel.draw(), retryStepAgain
	case Ignore:
		return current, retryStepIgnore
	case Rethrow:
		return current, retryStepStop
	default:
		return current, retryStepUnknown
	}
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

// run executes one runner of a speculative execution and publishes its single terminal
// message.
//
// The message goes on results when do reports a decisive outcome and on retired when the
// execution retired; the outcome do returned is the only thing that decides which, so an
// error value never has to be sniffed for.
//
// Parameters:
//   - ctx: the runner context
//   - qry: this runner's own snapshot of the request
//   - sel: the selector shared by every runner
//   - results: where a decisive iterator is published
//   - retired: where a retirement is published
func (q *queryExecutor) run(ctx context.Context, qry internalRequest, sel *hostSelector, results chan<- *Iter,
	retired chan<- retirement) {
	if q.testRunHook != nil {
		q.testRunHook(runEntered)
		defer q.testRunHook(runExited)
	}

	// Coordination teardown: parent at coordinate selects on <-results, <-retired or <-ctx.Done().
	// If q.do panics, no result is sent and the parent waits until ctx cancel.
	// Push a panic-error iter so the parent unblocks immediately.
	//
	// Every send below is a plain send, never a select on ctx.Done().
	// Both channels hold 1+S messages, S being the sp.Attempts() coordinate sampled;
	// only coordinate launches runners, at most 1+S of them, and each one publishes
	// exactly one message - so no send here can block.
	// A ctx.Done() case would be picked at random against a send that is ready anyway,
	// dropping an iterator with its framer and leaving coordinate's drain accounting
	// waiting for a message that was never sent.
	defer recoverGoroutine(q.pool.session.logger, "queryExecutor.run", func(err error) {
		errIter := newErrIter(err, qry.getQueryMetrics(), qry.Keyspace(),
			qry.getRoutingInfo(), qry.getKeyspaceFunc())
		results <- errIter
	})

	// Speculative runners share one selector, so its budget is query-wide
	// and coordinate's retirement accounting can rely on it:
	// a runner that found no host has exhausted the selection for every runner, launched or not.
	// See #812.
	iter, outcome := q.do(ctx, qry, sel)
	if outcome != outcomeResult {
		// This execution wanted a further attempt and could not have one, so it
		// must not end the query while a sibling is still running.
		if q.testRunHook != nil {
			q.testRunHook(runRetired)
		}
		retired <- retirement{iter: iter, attempted: outcome == outcomeRetiredAttempted}
		return
	}

	if q.testRunHook != nil {
		q.testRunHook(runResult)
	}
	results <- iter
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

// internalQuery is the per-execution state of one Query.
//
// Two sites copy an existing internalQuery, and both are bound by one contract:
// build the copy field by field, and read consistency through GetConsistency.
//
//   - (*internalQuery).snapshotForRunner, below: the request one speculative
//     runner owns.
//   - Conn.executeQueryWithUnprepRetries (conn.go): the request that fetches the
//     next page.
//
// newInternalQuery below builds one from the public Query instead of copying
// one, so it is a constructor and not a copy site.
//
// No copy site may read the struct as a whole.
// A RetryPolicy writes consistency through SetConsistency while a sibling runner
// or the page that just arrived is being copied,
// so a whole-struct read of it is a data race that an atomic reload afterwards
// cannot undo.
//
// Adding a field means adding it to both copy sites; forgetting one is a silent
// bug, which TestInternalRequestFieldsAreSnapshotted turns into a failing test.
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
	// runnerCtx is the context of the speculative runner this request belongs to.
	//
	// It is non-nil only on a snapshotForRunner copy, and Context reports it in
	// place of the caller's context so a RetryPolicy consulted inside the runner
	// observes the cancellation a winning sibling causes.
	// The next-page copy in conn.go deliberately leaves it nil: that request
	// outlives the runner whose context this is.
	runnerCtx context.Context
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

// snapshotForRunner returns the copy of the query one speculative runner owns.
//
// consistency is read through GetConsistency, so the copy never races the
// SetConsistency a sibling's RetryPolicy may be making.
// Every pointer field keeps its identity: metrics, routing info and host metrics
// belong to the page execution rather than to one runner,
// and pageState is read-only for the whole execution.
//
// Parameters:
//   - ctx: the runner's context, which the copy's Context reports
//
// Returns:
//   - internalRequest: the runner's own request
func (q *internalQuery) snapshotForRunner(ctx context.Context) internalRequest {
	return &internalQuery{
		originalQuery:      q.originalQuery,
		qryOpts:            q.qryOpts,
		pageState:          q.pageState,
		conn:               q.conn,
		consistency:        uint32(q.GetConsistency()),
		session:            q.session,
		routingInfo:        q.routingInfo,
		metrics:            q.metrics,
		hostMetricsManager: q.hostMetricsManager,
		runnerCtx:          ctx,
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

// Context returns the context this execution runs under.
//
// A speculative runner's snapshot reports the runner context, so a RetryPolicy
// reading it through RetryableQuery observes the cancellation a winning sibling
// causes; every other request reports the caller's own context.
//
// Returns:
//   - context.Context: the runner context on a snapshot, the caller's otherwise
func (q *internalQuery) Context() context.Context {
	if q.runnerCtx != nil {
		return q.runnerCtx
	}
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

// internalBatch is the per-execution state of one Batch.
//
// One site copies an existing internalBatch, and it is bound by the same
// contract internalQuery states: (*internalBatch).snapshotForRunner below builds
// the copy field by field and reads consistency through GetConsistency.
// A batch has no next page - nextIter is built only for a rows result, in
// conn.go - so there is no second copy site, and there is no pageState or conn
// field to carry.
//
// newInternalBatch below builds one from the public Batch instead of copying
// one, so it is a constructor and not a copy site.
//
// Adding a field means adding it to snapshotForRunner; forgetting one is a
// silent bug, which TestInternalRequestFieldsAreSnapshotted turns into a failing
// test.
type internalBatch struct {
	originalBatch      *Batch
	batchOpts          *batchOptions
	consistency        uint32
	routingInfo        *queryRoutingInfo
	session            *Session
	metrics            *queryMetrics
	hostMetricsManager hostMetricsManager
	// runnerCtx is the context of the speculative runner this request belongs to.
	//
	// It is non-nil only on a snapshotForRunner copy, and Context reports it in
	// place of the caller's context; see the field of the same name on
	// internalQuery.
	// A batch has no next page, so it has no second copy site to exclude.
	runnerCtx context.Context
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

// snapshotForRunner returns the copy of the batch one speculative runner owns.
//
// consistency is read through GetConsistency, so the copy never races the
// SetConsistency a sibling's RetryPolicy may be making.
// Every pointer field keeps its identity, hostMetricsManager included: it is an
// interface value that is carried over, not a manager that is rebuilt.
//
// Parameters:
//   - ctx: the runner's context, which the copy's Context reports
//
// Returns:
//   - internalRequest: the runner's own request
func (b *internalBatch) snapshotForRunner(ctx context.Context) internalRequest {
	return &internalBatch{
		originalBatch:      b.originalBatch,
		batchOpts:          b.batchOpts,
		consistency:        uint32(b.GetConsistency()),
		routingInfo:        b.routingInfo,
		session:            b.session,
		metrics:            b.metrics,
		hostMetricsManager: b.hostMetricsManager,
		runnerCtx:          ctx,
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

// Context returns the context this execution runs under.
//
// A speculative runner's snapshot reports the runner context; see
// (*internalQuery).Context.
//
// Returns:
//   - context.Context: the runner context on a snapshot, the caller's otherwise
func (b *internalBatch) Context() context.Context {
	if b.runnerCtx != nil {
		return b.runnerCtx
	}
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
