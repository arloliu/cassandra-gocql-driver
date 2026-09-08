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
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/internal/streams"
)

// errScriptedAttempt is the error every scripted attempt carries.
//
// It is deliberately none of the errors do treats as logical (context.Canceled,
// context.DeadlineExceeded, ErrNotFound), so an attempt always reaches the retry policy.
var errScriptedAttempt = errors.New("gocql_test: scripted attempt failed")

// callbackPanic is the value the panicking callbacks below raise.
//
// Its own named type is the point: asserting on it proves do propagated the original
// panic value rather than recovering and republishing it as some error type.
type callbackPanic struct {
	where string
}

// heldIter pairs a scripted iterator with the framer it was built on, so a test can tell
// whether that framer went through release() after the Iter dropped its reference.
//
// The framer's readBuffer starts larger than the pool retains, which is the one state
// release() acts on and reset() does not: release() replaces an oversized readBuffer with
// a fresh defaultBufSize slice, while reset() never touches readBuffer at all.
// The shared "f.buf = f.readBuffer[:0]" assignment proves nothing, and neither does
// iter.framer being nil, which Iter.Close does on its own.
type heldIter struct {
	iter   *Iter
	framer *framer
	buf    []byte
}

// newHeldIter returns an iterator carrying an oversized framer and failing with err.
//
// Parameters:
//   - err: the error the iterator reports
//
// Returns:
//   - *heldIter: the iterator and the framer state to assert against
func newHeldIter(err error) *heldIter {
	buf := make([]byte, maxPooledBufSize+1)
	f := &framer{buf: buf[:0], readBuffer: buf}
	return &heldIter{iter: &Iter{framer: f, err: err}, framer: f, buf: buf}
}

// released reports whether the framer went through framer.release().
//
// The gate is the identity of the readBuffer's backing array, not its capacity:
// release() swapped the oversized slice for a fresh one, and nothing else in the driver
// replaces readBuffer. Comparing identity rather than capacity also survives the framer
// being taken back out of the global pool and regrown by another user, while staying red
// for a framer that was never released — one that never reached the pool cannot change.
//
// Returns:
//   - bool: true once release() ran on the framer
func (h *heldIter) released() bool {
	return &h.framer.readBuffer[0] != &h.buf[0]
}

// scriptedQuery is an internalRequest whose attempts return prepared iterators instead of
// talking to a connection.
//
// do then owns real *Iter values carrying real framers without the fixture having to push
// a multi-megabyte response through a test server, and every scripted response carries an
// error rather than a successful void, so the executor genuinely holds a framer for the
// whole attempt.
// Everything else — the retry policy, idempotency, the observer callout —
// is the production internalQuery.
type scriptedQuery struct {
	*internalQuery

	iters  []*heldIter
	served int
}

var _ internalRequest = (*scriptedQuery)(nil)

// newScriptedQuery builds an idempotent query wired to rt and observer whose attempts
// yield iters in order.
//
// Parameters:
//   - rt: the retry policy do consults between attempts
//   - observer: the query observer attemptQuery calls, or nil
//   - iters: one entry per attempt, in order
//
// Returns:
//   - *scriptedQuery: the request
func newScriptedQuery(rt RetryPolicy, observer QueryObserver, iters []*heldIter) *scriptedQuery {
	qry := &Query{stmt: "SELECT ownership FROM t", rt: rt, observer: observer, idempotent: true}
	return &scriptedQuery{internalQuery: newInternalQuery(qry, context.Background()), iters: iters}
}

// execute returns the next scripted iterator.
//
// Returns:
//   - *Iter: the iterator for this attempt
func (q *scriptedQuery) execute(context.Context, *Conn) *Iter {
	iter := scriptedIter(q.iters, q.served)
	q.served++
	return iter
}

// scriptedBatch is scriptedQuery for batches.
//
// The batch cases must not use a successful void response: conn.executeBatch releases the
// framer inside connection execution for that shape, so the executor never holds one and
// the test would prove nothing.
// Every scripted iterator here carries an error and a framer,
// which is what a batch reply with an error or rows leaves the executor holding.
type scriptedBatch struct {
	*internalBatch

	iters  []*heldIter
	served int
}

var _ internalRequest = (*scriptedBatch)(nil)

// newScriptedBatch builds a batch wired to rt and observer whose attempts yield iters in
// order.
//
// Parameters:
//   - rt: the retry policy do consults between attempts
//   - observer: the batch observer attemptQuery calls, or nil
//   - iters: one entry per attempt, in order
//
// Returns:
//   - *scriptedBatch: the request
func newScriptedBatch(rt RetryPolicy, observer BatchObserver, iters []*heldIter) *scriptedBatch {
	batch := &Batch{rt: rt, observer: observer}
	return &scriptedBatch{internalBatch: newInternalBatch(batch, context.Background()), iters: iters}
}

// execute returns the next scripted iterator.
//
// Returns:
//   - *Iter: the iterator for this attempt
func (b *scriptedBatch) execute(context.Context, *Conn) *Iter {
	iter := scriptedIter(b.iters, b.served)
	b.served++
	return iter
}

// scriptedIter returns the nth scripted iterator.
//
// Parameters:
//   - iters: the script
//   - n: the attempt index
//
// Returns:
//   - *Iter: the iterator for that attempt
func scriptedIter(iters []*heldIter, n int) *Iter {
	if n >= len(iters) {
		panic("gocql_test: do made more attempts than the script provides")
	}
	return iters[n].iter
}

// doFixture is the minimum world do needs: one up, pooled host holding one connection.
//
// Nothing in it reads or writes a socket and no session runs alongside it, so the global
// framer pool stays quiet for the whole test and the released() gate is stable.
type doFixture struct {
	executor *queryExecutor
	host     *HostInfo
}

// newDoFixture returns a fixture whose single host always yields a connection.
//
// Returns:
//   - *doFixture: the fixture
func newDoFixture() *doFixture {
	host := (&HostInfo{
		hostId:         "ownership-1",
		connectAddress: net.IPv4(127, 0, 0, 1),
		port:           9042,
	}).setState(NodeUp)
	conn := &Conn{host: host, streams: streams.New(int(protoVersion4), 64)}
	pool := &policyConnPool{
		hostConnPools: map[string]*hostConnPool{
			host.HostID(): {host: host, size: 1, conns: []*Conn{conn}},
		},
	}
	return &doFixture{executor: &queryExecutor{pool: pool}, host: host}
}

// selector hands out hosts in order with replacement turned off, so do stays on the
// selections the test named.
//
// Parameters:
//   - hosts: the selections, in order
//
// Returns:
//   - *hostSelector: the selector
func (f *doFixture) selector(hosts ...SelectedHost) *hostSelector {
	i := 0
	return &hostSelector{iter: func() SelectedHost {
		if i >= len(hosts) {
			return nil
		}
		host := hosts[i]
		i++
		return host
	}}
}

// pinned returns the fixture's host as a plain SelectedHost.
//
// Returns:
//   - SelectedHost: the selection
func (f *doFixture) pinned() SelectedHost {
	return (*selectedHost)(f.host)
}

// retrySameHost permits budget further attempts and always retries on the same host, so a
// fixture drives exactly budget+1 attempts against one host.
type retrySameHost struct {
	left int
}

var _ RetryPolicy = (*retrySameHost)(nil)

// Attempt permits another attempt while the budget lasts.
//
// Returns:
//   - bool: true while attempts remain
func (p *retrySameHost) Attempt(RetryableQuery) bool {
	p.left--
	return p.left >= 0
}

// GetRetryType always asks for a retry on the same host.
//
// Returns:
//   - RetryType: Retry
func (p *retrySameHost) GetRetryType(error) RetryType { return Retry }

// unknownRetryTypePolicy answers with a retry type do does not know, driving the
// ErrUnknownRetryType branch.
type unknownRetryTypePolicy struct{}

var _ RetryPolicy = unknownRetryTypePolicy{}

// Attempt always permits another attempt.
//
// Returns:
//   - bool: true
func (unknownRetryTypePolicy) Attempt(RetryableQuery) bool { return true }

// GetRetryType returns a retry type outside the defined set.
//
// Returns:
//   - RetryType: an undefined value
func (unknownRetryTypePolicy) GetRetryType(error) RetryType { return RetryType(255) }

// panicOnAttempt is a retry policy whose Attempt panics.
type panicOnAttempt struct {
	value any
}

var _ RetryPolicy = panicOnAttempt{}

// Attempt panics.
func (p panicOnAttempt) Attempt(RetryableQuery) bool { panic(p.value) }

// GetRetryType is never reached.
//
// Returns:
//   - RetryType: Retry
func (p panicOnAttempt) GetRetryType(error) RetryType { return Retry }

// panicOnGetRetryType is a retry policy whose GetRetryType panics.
type panicOnGetRetryType struct {
	value any
}

var _ RetryPolicy = panicOnGetRetryType{}

// Attempt always permits another attempt.
//
// Returns:
//   - bool: true
func (p panicOnGetRetryType) Attempt(RetryableQuery) bool { return true }

// GetRetryType panics.
func (p panicOnGetRetryType) GetRetryType(error) RetryType { panic(p.value) }

// panicObserver is a query and batch observer that panics out of the observation.
type panicObserver struct {
	value any
}

var (
	_ QueryObserver = panicObserver{}
	_ BatchObserver = panicObserver{}
)

// ObserveQuery panics.
func (o panicObserver) ObserveQuery(context.Context, ObservedQuery) { panic(o.value) }

// ObserveBatch panics.
func (o panicObserver) ObserveBatch(context.Context, ObservedBatch) { panic(o.value) }

// markPanicHost is a SelectedHost whose Mark panics, the way a HostSelectionPolicy's own
// selection could.
type markPanicHost struct {
	info  *HostInfo
	value any
}

var _ SelectedHost = markPanicHost{}

// Info returns the host.
//
// Returns:
//   - *HostInfo: the host
func (h markPanicHost) Info() *HostInfo { return h.info }

// Mark panics.
func (h markPanicHost) Mark(error) { panic(h.value) }

// TestDo_SupersededItersAreClosed proves do reclaims every iterator it replaces: the ones
// the retry loop supersedes and the one the unknown-retry-type branch throws away.
// The iterator the caller receives must survive untouched.
func TestDo_SupersededItersAreClosed(t *testing.T) {
	t.Run("retry loop", func(t *testing.T) {
		fixture := newDoFixture()
		iters := []*heldIter{
			newHeldIter(errScriptedAttempt),
			newHeldIter(errScriptedAttempt),
			newHeldIter(errScriptedAttempt),
			newHeldIter(errScriptedAttempt),
		}
		qry := newScriptedQuery(&retrySameHost{left: 3}, nil, iters)

		result := fixture.executor.do(context.Background(), qry, fixture.selector(fixture.pinned()))

		require.Equal(t, len(iters), qry.served, "three retries must have driven four attempts")
		require.Same(t, iters[len(iters)-1].iter, result, "the last attempt is the one the caller receives")
		for i, held := range iters[:len(iters)-1] {
			require.True(t, held.released(), "the superseded iter %d must have released its framer", i)
		}

		last := iters[len(iters)-1]
		require.False(t, last.released(), "the returned iter must still hold its framer")
		require.NotNil(t, result.framer, "the returned iter must still hold its framer")

		require.ErrorIs(t, result.Close(), errScriptedAttempt)
	})

	t.Run("unknown retry type", func(t *testing.T) {
		fixture := newDoFixture()
		held := newHeldIter(errScriptedAttempt)
		qry := newScriptedQuery(unknownRetryTypePolicy{}, nil, []*heldIter{held})

		result := fixture.executor.do(context.Background(), qry, fixture.selector(fixture.pinned()))

		require.ErrorIs(t, result.err, ErrUnknownRetryType, "the caller gets a fresh error iter")
		require.NotSame(t, held.iter, result, "the replaced iter is not the one returned")
		require.True(t, held.released(), "the replaced iter must have released its framer")
	})
}

// TestDo_PanicInCallbackReleasesIter proves that a panic out of any callback that runs
// while an attempt's iterator is held still reclaims that iterator, and that the panic
// reaches the synchronous caller of do with its original value and type — do installs a
// cleanup defer, not a recover.
func TestDo_PanicInCallbackReleasesIter(t *testing.T) {
	// wire installs one panicking callback and returns the request and the selection do
	// should run against.
	type wire func(fixture *doFixture, held *heldIter, value any) (internalRequest, SelectedHost)

	cases := []struct {
		name string
		wire wire
	}{
		{
			name: "query observer",
			wire: func(f *doFixture, held *heldIter, value any) (internalRequest, SelectedHost) {
				return newScriptedQuery(&retrySameHost{left: 3}, panicObserver{value: value},
					[]*heldIter{held}), f.pinned()
			},
		},
		{
			name: "query mark",
			wire: func(f *doFixture, held *heldIter, value any) (internalRequest, SelectedHost) {
				return newScriptedQuery(&retrySameHost{left: 3}, nil, []*heldIter{held}),
					markPanicHost{info: f.host, value: value}
			},
		},
		{
			name: "query attempt",
			wire: func(f *doFixture, held *heldIter, value any) (internalRequest, SelectedHost) {
				return newScriptedQuery(panicOnAttempt{value: value}, nil, []*heldIter{held}), f.pinned()
			},
		},
		{
			name: "query get retry type",
			wire: func(f *doFixture, held *heldIter, value any) (internalRequest, SelectedHost) {
				return newScriptedQuery(panicOnGetRetryType{value: value}, nil, []*heldIter{held}), f.pinned()
			},
		},
		{
			name: "batch observer",
			wire: func(f *doFixture, held *heldIter, value any) (internalRequest, SelectedHost) {
				return newScriptedBatch(&retrySameHost{left: 3}, panicObserver{value: value},
					[]*heldIter{held}), f.pinned()
			},
		},
		{
			name: "batch mark",
			wire: func(f *doFixture, held *heldIter, value any) (internalRequest, SelectedHost) {
				return newScriptedBatch(&retrySameHost{left: 3}, nil, []*heldIter{held}),
					markPanicHost{info: f.host, value: value}
			},
		},
		{
			name: "batch attempt",
			wire: func(f *doFixture, held *heldIter, value any) (internalRequest, SelectedHost) {
				return newScriptedBatch(panicOnAttempt{value: value}, nil, []*heldIter{held}), f.pinned()
			},
		},
		{
			name: "batch get retry type",
			wire: func(f *doFixture, held *heldIter, value any) (internalRequest, SelectedHost) {
				return newScriptedBatch(panicOnGetRetryType{value: value}, nil, []*heldIter{held}), f.pinned()
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newDoFixture()
			held := newHeldIter(errScriptedAttempt)
			value := callbackPanic{where: tc.name}
			req, host := tc.wire(fixture, held, value)

			require.PanicsWithValue(t, value, func() {
				fixture.executor.do(context.Background(), req, fixture.selector(host))
			}, "do must let the original panic value through untouched")

			require.True(t, held.released(), "the held iter must have released its framer")
		})
	}
}
