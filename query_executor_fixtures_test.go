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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// requestGate parks QUERY requests at the test server while their host is armed.
//
// It is installed as fillHarnessOpts.recvHook,
// so it runs in the server's receive loop before the request is answered:
// a parked request is a request in flight from the driver's point of view,
// and it occupies its stream until the gate is released.
type requestGate struct {
	mu    sync.Mutex
	armed map[string]bool
	// startedAt receives one token per parked request at that host, before it parks.
	//
	// Per host rather than aggregate because it is the evidence that a particular
	// runner reached a particular host: the attemptRecorder only records once execute
	// has returned, which is after the response, so it cannot witness a request in
	// flight.
	startedAt map[string]chan struct{}
	// releases is closed per host by release, letting that host's requests through.
	releases map[string]chan struct{}
	onces    map[string]*sync.Once

	// started receives one token per parked request at any host, before it parks.
	started chan struct{}
	// allRelease is closed to let every parked and future request through.
	allRelease chan struct{}
	once       sync.Once
}

// newRequestGate returns a gate with no host armed.
//
// Returns:
//   - *requestGate: the gate
func newRequestGate() *requestGate {
	return &requestGate{
		armed:      map[string]bool{},
		startedAt:  map[string]chan struct{}{},
		releases:   map[string]chan struct{}{},
		onces:      map[string]*sync.Once{},
		started:    make(chan struct{}, 64),
		allRelease: make(chan struct{}),
	}
}

// arm parks every later QUERY or BATCH request that reaches the server at ip.
func (g *requestGate) arm(ip string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.armed[ip] = true
	if _, ok := g.releases[ip]; !ok {
		g.releases[ip] = make(chan struct{})
		g.onces[ip] = &sync.Once{}
	}
	if _, ok := g.startedAt[ip]; !ok {
		g.startedAt[ip] = make(chan struct{}, 64)
	}
}

// hook is the fillHarnessOpts.recvHook.
func (g *requestGate) hook(ip string, f *framer) {
	if f.header == nil || (f.header.op != opQuery && f.header.op != opBatch) {
		return
	}

	g.mu.Lock()
	armed := g.armed[ip]
	perHost, started := g.releases[ip], g.startedAt[ip]
	g.mu.Unlock()
	if !armed {
		return
	}

	select {
	case g.started <- struct{}{}:
	default:
	}
	select {
	case started <- struct{}{}:
	default:
	}

	select {
	case <-g.allRelease:
	case <-perHost:
	}
}

// awaitStarted blocks until one gated request has parked.
func (g *requestGate) awaitStarted(t *testing.T, what string) {
	t.Helper()
	awaitSignal(t, g.started, what)
}

// awaitStartedAt blocks until one gated request has parked at the server at ip.
func (g *requestGate) awaitStartedAt(t *testing.T, ip string, what string) {
	t.Helper()

	g.mu.Lock()
	started := g.startedAt[ip]
	g.mu.Unlock()
	require.NotNil(t, started, "the gate must be armed for %s before a request there can be awaited", ip)
	awaitSignal(t, started, what)
}

// release lets every parked and future request at the server at ip through.
func (g *requestGate) release(ip string) {
	g.mu.Lock()
	perHost, once := g.releases[ip], g.onces[ip]
	g.mu.Unlock()

	if once != nil {
		once.Do(func() { close(perHost) })
	}
}

// releaseAll lets every parked and future request through.
func (g *requestGate) releaseAll() {
	g.once.Do(func() { close(g.allRelease) })
}

// errorRespHook returns a fillHarnessOpts.respHook answering every QUERY and BATCH
// reaching a server in hosts with a server error carrying the given metadata.
//
// The warnings and the custom payload are what make the error iterator distinguishable
// from a synthetic one, and the payload is also where an oversized response comes from:
// writeString is length-prefixed with 16 bits, so the message cannot carry enough bytes
// to outgrow a pooled buffer, while a custom payload value can.
// Its protocol must therefore be pinned to v4, as the oversized fixtures are: the v5
// segment encoder rejects a payload of that size.
//
// Parameters:
//   - hosts: the servers that must answer with the error
//   - code: the CQL error code
//   - msg: the error message
//   - warnings: the warnings the response carries, or nil
//   - payload: the custom payload the response carries, or nil
//
// Returns:
//   - func(string, *TestServer, *framer, *framer) bool: the response hook
func errorRespHook(hosts *hostSet, code int32, msg string, warnings []string,
	payload map[string][]byte,
) func(string, *TestServer, *framer, *framer) bool {
	return func(ip string, _ *TestServer, req, resp *framer) bool {
		if req.header == nil || !hosts.has(ip) {
			return false
		}
		if req.header.op != opQuery && req.header.op != opBatch {
			return false
		}

		var flags byte
		if len(warnings) > 0 {
			flags |= flagWarning
		}
		if len(payload) > 0 {
			flags |= flagCustomPayload
		}
		resp.writeHeader(flags, opError, req.header.stream)
		// The order the response body is parsed in: warnings, then payload, then the
		// error itself.
		if len(warnings) > 0 {
			resp.writeStringList(warnings)
		}
		if len(payload) > 0 {
			resp.writeBytesMap(payload)
		}
		resp.writeInt(code)
		resp.writeString(msg)
		return true
	}
}

// oversizedPayload returns a custom payload whose one value outgrows any pooled buffer.
//
// Returns:
//   - map[string][]byte: the payload
func oversizedPayload() map[string][]byte {
	return map[string][]byte{"gocql_test": make([]byte, maxPooledBufSize+1)}
}

// ctxRecordingPolicy records the context a RetryPolicy sees through RetryableQuery and
// then delegates.
//
// The context is read inside Attempt, which is where a backoff policy would read it, and
// entered is signalled from there too: a test that waits on it knows the delegate is
// already inside its nap.
type ctxRecordingPolicy struct {
	inner RetryPolicy
	// entered receives one token as each Attempt records, before it delegates.
	entered chan struct{}

	mu   sync.Mutex
	ctxs []context.Context
}

var _ RetryPolicy = (*ctxRecordingPolicy)(nil)

// newCtxRecordingPolicy returns a recorder wrapping inner.
//
// Returns:
//   - *ctxRecordingPolicy: the policy
func newCtxRecordingPolicy(inner RetryPolicy) *ctxRecordingPolicy {
	return &ctxRecordingPolicy{inner: inner, entered: make(chan struct{}, 8)}
}

// Attempt records the query's context, announces the entry and delegates.
//
// Returns:
//   - bool: the delegate's answer
func (p *ctxRecordingPolicy) Attempt(q RetryableQuery) bool {
	p.mu.Lock()
	p.ctxs = append(p.ctxs, q.Context())
	p.mu.Unlock()

	select {
	case p.entered <- struct{}{}:
	default:
	}

	return p.inner.Attempt(q)
}

// GetRetryType delegates.
//
// Returns:
//   - RetryType: the delegate's answer
func (p *ctxRecordingPolicy) GetRetryType(err error) RetryType {
	return p.inner.GetRetryType(err)
}

// recorded returns the contexts Attempt has seen, in order.
//
// Returns:
//   - []context.Context: the contexts
func (p *ctxRecordingPolicy) recorded() []context.Context {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]context.Context(nil), p.ctxs...)
}

// hostIP returns the address a requestGate keys on.
//
// Returns:
//   - string: the host's connect address without the port
func hostIP(host *HostInfo) string {
	return host.ConnectAddress().String()
}

// attemptRecorder is a QueryObserver that keeps every attempt, so a test can assert on
// the complete history once every runner has exited.
type attemptRecorder struct {
	mu       sync.Mutex
	attempts []ObservedQuery
	// updated is signalled after every observation.
	updated chan struct{}
}

var _ QueryObserver = (*attemptRecorder)(nil)

// newAttemptRecorder returns an empty recorder.
//
// Returns:
//   - *attemptRecorder: the recorder
func newAttemptRecorder() *attemptRecorder {
	return &attemptRecorder{updated: make(chan struct{}, 1)}
}

// ObserveQuery records the attempt.
func (r *attemptRecorder) ObserveQuery(_ context.Context, q ObservedQuery) {
	r.mu.Lock()
	r.attempts = append(r.attempts, q)
	r.mu.Unlock()

	select {
	case r.updated <- struct{}{}:
	default:
	}
}

// count returns how many attempts were observed.
//
// Returns:
//   - int: the number of attempts
func (r *attemptRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.attempts)
}

// hosts returns how many attempts each host received.
//
// Returns:
//   - map[*HostInfo]int: attempts per host
func (r *attemptRecorder) hosts() map[*HostInfo]int {
	r.mu.Lock()
	defer r.mu.Unlock()

	hosts := make(map[*HostInfo]int, len(r.attempts))
	for _, attempt := range r.attempts {
		hosts[attempt.Host]++
	}
	return hosts
}

// await blocks until at least n attempts were observed.
func (r *attemptRecorder) await(t *testing.T, n int) {
	t.Helper()

	deadline := time.After(fillEventBudget)
	for r.count() < n {
		select {
		case <-r.updated:
		case <-deadline:
			t.Fatalf("timed out after %v waiting for %d attempts, saw %d", fillEventBudget, n, r.count())
		}
	}
}

// saturatePool takes every stream of the pool's connection, so pickOrState reports
// poolSaturated: the host is up and pooled but cannot serve a request.
//
// Tests that use it set cluster.heartbeatInterval to an hour, so no heartbeat contends
// for a stream within the test.
func saturatePool(t *testing.T, h *fillHarness, pool *hostConnPool) {
	t.Helper()

	conn := h.pickAnyConn(t, pool)
	for conn.AvailableStreams() > 0 {
		_, ok := conn.streams.GetStream()
		require.True(t, ok, "the connection must hand out a stream while it reports one available")
	}
	_, state := pool.pickOrState()
	require.Equal(t, poolSaturated, state, "the pool must report saturation")
}

// unpoolHost drops the host's pool while leaving the host UP.
func unpoolHost(h *fillHarness, host *HostInfo) {
	h.session.pool.removeHost(host)
}

// noHeartbeat is a cluster tweak that pushes the first heartbeat past the test.
func noHeartbeat(cluster *ClusterConfig) {
	cluster.heartbeatInterval = time.Hour
}

// alwaysNextHostPolicy is a RetryPolicy whose Attempt never returns false and that
// always asks for the next host: the uncooperative policy the selection budget must
// bound on its own.
type alwaysNextHostPolicy struct{}

var _ RetryPolicy = alwaysNextHostPolicy{}

// Attempt always permits another attempt.
func (alwaysNextHostPolicy) Attempt(RetryableQuery) bool { return true }

// GetRetryType always asks for the next host.
func (alwaysNextHostPolicy) GetRetryType(error) RetryType { return RetryNextHost }

// scriptedIterPolicy is a policy whose Nth Pick yields the Nth scripted host list, in
// order, standing in for an enumerating policy with a deterministic visit order.
//
// Past the end of the script every Pick yields an empty iterator.
type scriptedIterPolicy struct {
	HostSelectionPolicy

	// script holds the host list each successive Pick enumerates.
	script [][]*HostInfo

	// wrap, when set, decorates every selection the iterator hands out, so a test can
	// make one host's Mark misbehave without touching the enumeration order.
	wrap func(SelectedHost) SelectedHost

	// picks counts how many iterators were handed out.
	picks atomic.Int32
}

var _ HostSelectionPolicy = (*scriptedIterPolicy)(nil)

// Pick returns an iterator over the next scripted host list.
func (p *scriptedIterPolicy) Pick(_ ExecutableStatement) NextHost {
	n := int(p.picks.Add(1)) - 1
	var hosts []*HostInfo
	if n < len(p.script) {
		hosts = p.script[n]
	}

	i := 0
	return func() SelectedHost {
		if i >= len(hosts) {
			return nil
		}
		host := hosts[i]
		i++
		var selected SelectedHost = (*selectedHost)(host)
		if p.wrap != nil {
			selected = p.wrap(selected)
		}
		return selected
	}
}

// panicOnMarkFor returns a selection decorator whose Mark panics for one host only.
//
// Mark runs after attemptQuery has returned, so do owns the attempt's iterator when it
// panics: it is the narrowest way to test that ownership window without disturbing any
// other host's selection.
//
// Parameters:
//   - host: the host whose Mark panics
//   - value: the panic value raised
//
// Returns:
//   - func(SelectedHost) SelectedHost: the decorator
func panicOnMarkFor(host *HostInfo, value any) func(SelectedHost) SelectedHost {
	return func(selected SelectedHost) SelectedHost {
		if selected.Info() != host {
			return selected
		}
		return markPanicHost{info: host, value: value}
	}
}

// capturedAttempt is one attempt an iterCapture recorded.
type capturedAttempt struct {
	host *HostInfo
	iter *Iter
}

// iterCapture collects the iterators the attempts of one query produced.
//
// It is the only way a test can name the iterator an attempt returned: ObservedQuery
// carries no *Iter, and adding one to the public observer would be a production change
// for a test's benefit.
type iterCapture struct {
	mu   sync.Mutex
	seen []capturedAttempt
}

// record keeps one attempt's host and iterator.
func (c *iterCapture) record(host *HostInfo, iter *Iter) {
	c.mu.Lock()
	c.seen = append(c.seen, capturedAttempt{host: host, iter: iter})
	c.mu.Unlock()
}

// forHost returns the iterator the attempt on host produced.
//
// Returns:
//   - *Iter: the attempt's iterator
func (c *iterCapture) forHost(t *testing.T, host *HostInfo) *Iter {
	t.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, attempt := range c.seen {
		if attempt.host == host {
			return attempt.iter
		}
	}
	t.Fatalf("no attempt was recorded on %v", host)
	return nil
}

// count returns how many attempts were recorded.
//
// Returns:
//   - int: the number of attempts
func (c *iterCapture) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.seen)
}

// iterCapturingRequest wraps a request so every attempt's iterator is recorded.
//
// snapshotForRunner is overridden to keep the wrapper around the runner's copy: the
// embedded method would return the bare request and the recording would be lost for every
// runner but the first.
// The recorder is shared by every copy, so the capture is per query, not per runner.
type iterCapturingRequest struct {
	internalRequest

	capture *iterCapture
}

var _ internalRequest = (*iterCapturingRequest)(nil)

// execute records the iterator the attempt produced before handing it back.
//
// Returns:
//   - *Iter: the attempt's iterator, unchanged
func (r *iterCapturingRequest) execute(ctx context.Context, conn *Conn) *Iter {
	iter := r.internalRequest.execute(ctx, conn)
	r.capture.record(conn.host, iter)
	return iter
}

// snapshotForRunner keeps the wrapper on the runner's own copy.
//
// Parameters:
//   - ctx: the runner context the snapshot answers Context() with
//
// Returns:
//   - internalRequest: the wrapped snapshot, sharing this request's recorder
func (r *iterCapturingRequest) snapshotForRunner(ctx context.Context) internalRequest {
	return &iterCapturingRequest{
		internalRequest: r.internalRequest.snapshotForRunner(ctx),
		capture:         r.capture,
	}
}

// scriptedIterator returns an iterator over hosts, counting raw calls in calls.
//
// Returns:
//   - NextHost: the iterator
func scriptedIterator(hosts []*HostInfo, calls *atomic.Int32) NextHost {
	i := 0
	return func() SelectedHost {
		if calls != nil {
			calls.Add(1)
		}
		if i >= len(hosts) {
			return nil
		}
		host := hosts[i]
		i++
		return (*selectedHost)(host)
	}
}

// tokenAwareKeyspace is the keyspace tokenAwareOver wires up.
const tokenAwareKeyspace = "ks"

// tokenAwareRoutingKey routes to the token "25" under OrderedPartitioner.
var tokenAwareRoutingKey = []byte("20")

// tokenAwareOver installs a token-aware policy over fallback as the harness executor's
// policy, wired without a server the way the policy tests do, and returns the wrapper
// that counts its outer Picks.
//
// The routing key tokenAwareRoutingKey resolves to replicas[0], then replicas[1];
// the remaining harness hosts are not replicas.
// Queries must call RoutingKey with it and set getKeyspace to tokenAwareKeyspace.
//
// Parameters:
//   - t: the test
//   - h: the harness whose ring hosts become the token ring
//   - fallback: the fallback policy, usually wrapped in countingPickPolicy
//   - rf: the replication factor
//   - replicas: the hosts the routing key must resolve to, in order
//
// Returns:
//   - *countingPickPolicy: the installed policy, for its outer Pick count
func tokenAwareOver(t *testing.T, h *fillHarness, fallback HostSelectionPolicy, rf int,
	replicas []*HostInfo) *countingPickPolicy {
	t.Helper()

	policy := TokenAwareHostPolicy(fallback, DoNotShuffleReplicas())
	internal, ok := policy.(*tokenAwareHostPolicy)
	require.True(t, ok, "TokenAwareHostPolicy must return a *tokenAwareHostPolicy, got %T", policy)
	internal.getKeyspaceName = func() string { return tokenAwareKeyspace }

	ksMeta := &KeyspaceMetadata{
		Name:          tokenAwareKeyspace,
		StrategyClass: "SimpleStrategy",
		StrategyOptions: map[string]any{
			"class":              "SimpleStrategy",
			"replication_factor": rf,
		},
	}
	ksMeta.placementStrategy = getStrategy(ksMeta, nopLoggerSingleton)
	internal.getSchemaMeta = func() *schemaMeta {
		return &schemaMeta{keyspaceMeta: map[string]*KeyspaceMetadata{tokenAwareKeyspace: ksMeta}}
	}

	// Replicas own "25" and "50", so the routing key "20" resolves to them in order;
	// the other hosts own tokens the key never reaches first.
	replicaTokens := []string{"25", "50"}
	otherTokens := []string{"00", "75", "90"}
	isReplica := map[*HostInfo]int{}
	for i, host := range replicas {
		isReplica[host] = i
	}
	other := 0
	for _, host := range h.hosts {
		var token string
		if i, ok := isReplica[host]; ok {
			token = replicaTokens[i]
		} else {
			token = otherTokens[other]
			other++
		}
		host.mu.Lock()
		host.tokens = []string{token}
		host.mu.Unlock()
		policy.AddHost(host)
	}
	policy.SetPartitioner("OrderedPartitioner")

	counting := &countingPickPolicy{HostSelectionPolicy: policy}
	h.session.executor.policy = counting
	return counting
}

// routed makes qry route through tokenAwareOver's keyspace and key.
//
// Returns:
//   - *Query: qry
func routed(qry *Query) *Query {
	qry.getKeyspace = func() string { return tokenAwareKeyspace }
	return qry.RoutingKey(tokenAwareRoutingKey)
}

// onFreshConnAppended installs a handler that signals once a connection is appended to
// host's pool from now on.
// Buffered arrivals from the fixture's own fills never satisfy it.
//
// Returns:
//   - <-chan struct{}: fires on the first fresh append for host
func onFreshConnAppended(h *fillHarness, host *HostInfo) <-chan struct{} {
	appended := make(chan struct{}, 1)
	h.events.on(poolConnAppended, func(got *HostInfo) {
		if got != host {
			return
		}
		select {
		case appended <- struct{}{}:
		default:
		}
	})
	return appended
}

// releaseStageGate returns a cleanup that lets every runner parked at gate through.
//
// Returns:
//   - func(): the cleanup
func releaseStageGate(gate chan struct{}) func() {
	return sync.OnceFunc(func() { close(gate) })
}

// execAsync runs qry in the background and reports its error once.
//
// Returns:
//   - <-chan error: receives the query's error exactly once
func execAsync(qry *Query) <-chan error {
	result := make(chan error, 1)
	go func() { result <- qry.Exec() }()
	return result
}

// prepareGate parks PREPARE requests at the test server and counts every PREPARE
// the server sees, per statement.
//
// It is installed as fillHarnessOpts.recvHook, so it runs in the server's receive
// loop before the request is answered: a parked PREPARE is a request in flight from
// the driver's point of view, and the connection answers nothing else until the gate
// is released. Once released, the request falls through to the server's normal
// PREPARE handling, so a caller that shares the load still gets a usable statement.
type prepareGate struct {
	// park, when non-nil, is the statement substring whose PREPARE is held.
	park atomic.Pointer[string]

	mu     sync.Mutex
	counts map[string]int

	// started receives the server address of each parked PREPARE, before it parks.
	started chan string
	// release is closed to let every parked and future PREPARE through.
	release chan struct{}
	once    sync.Once
}

// newPrepareGate returns a gate that parks nothing and counts every PREPARE.
//
// Returns:
//   - *prepareGate: the gate
func newPrepareGate() *prepareGate {
	return &prepareGate{
		counts:  map[string]int{},
		started: make(chan string, 64),
		release: make(chan struct{}),
	}
}

// parkStatements holds every later PREPARE whose statement contains substr.
//
// Parameters:
//   - substr: the statement substring to park on
func (g *prepareGate) parkStatements(substr string) {
	g.park.Store(&substr)
}

// hook is the fillHarnessOpts.recvHook.
//
// It peeks at the statement without consuming it: the server's own handler re-reads
// the same body afterwards.
func (g *prepareGate) hook(ip string, f *framer) {
	if f.header == nil || f.header.op != opPrepare {
		return
	}

	buf := f.buf
	stmt, err := f.readLongString()
	f.buf = buf
	if err != nil {
		return
	}

	g.mu.Lock()
	g.counts[stmt]++
	g.mu.Unlock()

	park := g.park.Load()
	if park == nil || !strings.Contains(stmt, *park) {
		return
	}

	select {
	case g.started <- ip:
	default:
	}
	<-g.release
}

// awaitParked blocks until one PREPARE has parked, and reports where.
//
// Returns:
//   - string: the connect address of the server that parked it
func (g *prepareGate) awaitParked(t *testing.T, what string) string {
	t.Helper()

	select {
	case ip := <-g.started:
		return ip
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for %s", fillEventBudget, what)
		return ""
	}
}

// releaseAll lets every parked and future PREPARE through.
func (g *prepareGate) releaseAll() {
	g.once.Do(func() { close(g.release) })
}

// count returns how many PREPARE requests the server saw for stmt.
//
// Returns:
//   - int: the number of PREPARE requests
func (g *prepareGate) count(stmt string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.counts[stmt]
}

// respHookFunc is the shape of a fillHarnessOpts.respHook.
type respHookFunc func(ip string, srv *TestServer, req, resp *framer) bool

// chainRespHooks offers each request to every hook in order and stops at the first one
// that answers it.
//
// A harness takes one response hook, but a fixture often needs several - one per server
// whose response carries its own metadata - and each of them already declines what is not
// its own.
//
// Parameters:
//   - hooks: the hooks, in the order they are offered the request
//
// Returns:
//   - respHookFunc: the combined hook
func chainRespHooks(hooks ...respHookFunc) respHookFunc {
	return func(ip string, srv *TestServer, req, resp *framer) bool {
		for _, hook := range hooks {
			if hook(ip, srv, req, resp) {
				return true
			}
		}
		return false
	}
}
