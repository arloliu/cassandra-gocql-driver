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
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// interface to implement to receive the host information
type SetHosts interface {
	SetHosts(hosts []*HostInfo)
}

// interface to implement to receive the partitioner value
type SetPartitioner interface {
	SetPartitioner(partitioner string)
}

func setupTLSConfig(sslOpts *SslOptions) (*tls.Config, error) {
	//  Config.InsecureSkipVerify | EnableHostVerification | Result
	//  Config is nil             | true                   | verify host
	//  Config is nil             | false                  | do not verify host
	//  false                     | false                  | verify host
	//  true                      | false                  | do not verify host
	//  false                     | true                   | verify host
	//  true                      | true                   | verify host
	var tlsConfig *tls.Config
	if sslOpts.Config == nil {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: !sslOpts.EnableHostVerification,
		}
	} else {
		// use clone to avoid race.
		tlsConfig = sslOpts.Config.Clone()
	}

	if tlsConfig.InsecureSkipVerify && sslOpts.EnableHostVerification {
		tlsConfig.InsecureSkipVerify = false
	}

	// ca cert is optional
	if sslOpts.CaPath != "" {
		if tlsConfig.RootCAs == nil {
			tlsConfig.RootCAs = x509.NewCertPool()
		}

		pem, err := ioutil.ReadFile(sslOpts.CaPath)
		if err != nil {
			return nil, fmt.Errorf("connectionpool: unable to open CA certs: %w", err)
		}

		if !tlsConfig.RootCAs.AppendCertsFromPEM(pem) {
			return nil, errors.New("connectionpool: failed parsing or CA certs")
		}
	}

	if sslOpts.CertPath != "" || sslOpts.KeyPath != "" {
		mycert, err := tls.LoadX509KeyPair(sslOpts.CertPath, sslOpts.KeyPath)
		if err != nil {
			return nil, fmt.Errorf("connectionpool: unable to load X509 key pair: %w", err)
		}
		tlsConfig.Certificates = append(tlsConfig.Certificates, mycert)
	}

	return tlsConfig, nil
}

// poolState is what hostConnPool.pickOrState observed in one read of a pool.
type poolState uint8

const (
	// poolClosed means the pool is closed and will never serve a connection again.
	poolClosed poolState = iota
	// poolPicked means a usable connection was returned alongside this state.
	poolPicked
	// poolSaturated means the pool holds connections but none of them has an available stream.
	poolSaturated
	// poolEmptyPending means the pool holds no connection and at least one fill is in flight.
	poolEmptyPending
	// poolEmptyIdle means the pool holds no connection and no fill is in flight,
	// so waiting for this pool would wait forever.
	poolEmptyIdle
)

// poolEvent identifies a checkpoint in the fill machinery that a test can observe
// through ClusterConfig.testPoolHook.
type poolEvent uint8

const (
	// poolConnectAttempt fires in connect, before each session.connect attempt.
	poolConnectAttempt poolEvent = iota
	// poolConnAppended fires in connect, after a connection was appended to the pool
	// and before the parent generation is notified.
	poolConnAppended
	// poolFillAsyncStart fires in fill's asynchronous branch, before connectMany.
	poolFillAsyncStart
	// poolFillAdmission fires in fill, in the read-to-write lock transition that
	// precedes the double check.
	poolFillAdmission
	// poolFillDone fires in fillDone, after a fill claim was released and before
	// the parent generation is notified.
	// It is the last thing an asynchronous fill branch does, so a test can use it
	// as a completion barrier instead of polling the claim count.
	poolFillDone
)

type policyConnPool struct {
	session *Session

	port     int
	numConns int
	keyspace string

	mu            sync.RWMutex
	hostConnPools map[string]*hostConnPool
	// closed is terminal: once set, no pool is registered again and no successor
	// wake generation is ever created, so a waiter can safely give up.
	closed bool
	// wake is the current wake generation, created lazily by generation and
	// closed exactly once by notifyLocked.
	wake chan struct{}

	// testAfterParentNotify, when set, runs after removeHost or Close published the
	// terminal state and released p.mu, before the child pools are closed.
	// Nil in production.
	testAfterParentNotify func()
}

func connConfig(cfg *ClusterConfig) (*ConnConfig, error) {
	var (
		err        error
		hostDialer HostDialer
	)

	hostDialer = cfg.HostDialer
	if hostDialer == nil {
		var tlsConfig *tls.Config

		// TODO(zariel): move tls config setup into session init.
		if cfg.SslOpts != nil {
			tlsConfig, err = setupTLSConfig(cfg.SslOpts)
			if err != nil {
				return nil, err
			}
		}

		dialer := cfg.Dialer
		if dialer == nil {
			d := &net.Dialer{
				Timeout: cfg.ConnectTimeout,
			}
			if cfg.SocketKeepalive > 0 {
				d.KeepAlive = cfg.SocketKeepalive
			}
			dialer = d
		}

		hostDialer = &defaultHostDialer{
			dialer:         dialer,
			tlsConfig:      tlsConfig,
			connectTimeout: cfg.ConnectTimeout,
		}
	}

	interval := cfg.heartbeatInterval
	if interval <= 0 {
		interval = heartbeatInterval
	}
	phase := cfg.heartbeatPhase
	if phase == nil {
		phase = defaultHeartbeatPhase
	}
	hbTimeout := resolveHeartbeatTimeout(cfg.HeartbeatTimeout)

	return &ConnConfig{
		ProtoVersion:      cfg.ProtoVersion,
		MaxStreams:        cfg.MaxStreams,
		CQLVersion:        cfg.CQLVersion,
		Timeout:           cfg.Timeout,
		WriteTimeout:      cfg.WriteTimeout,
		ConnectTimeout:    cfg.ConnectTimeout,
		Dialer:            cfg.Dialer,
		HostDialer:        hostDialer,
		Compressor:        cfg.Compressor,
		Authenticator:     cfg.Authenticator,
		AuthProvider:      cfg.AuthProvider,
		Keepalive:         cfg.SocketKeepalive,
		Logger:            cfg.Logger,
		heartbeatInterval: interval,
		heartbeatPhase:    phase,
		heartbeatTimeout:  hbTimeout,
	}, nil
}

func newPolicyConnPool(session *Session) *policyConnPool {
	// create the pool
	pool := &policyConnPool{
		session:       session,
		port:          session.cfg.Port,
		numConns:      session.cfg.NumConns,
		keyspace:      session.cfg.Keyspace,
		hostConnPools: map[string]*hostConnPool{},
	}

	return pool
}

func (p *policyConnPool) SetHosts(hosts []*HostInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		// Close is terminal: registering a pool again would strand it.
		return
	}

	toRemove := make(map[string]struct{})
	for hostID := range p.hostConnPools {
		toRemove[hostID] = struct{}{}
	}

	pools := make(chan *hostConnPool)
	createCount := 0
	for _, host := range hosts {
		if !host.IsUp() {
			// don't create a connection pool for a down host
			continue
		}
		hostID := host.HostID()
		if _, exists := p.hostConnPools[hostID]; exists {
			// still have this host, so don't remove it
			delete(toRemove, hostID)
			continue
		}

		createCount++
		go func(host *HostInfo) {
			// Coordination teardown: parent below waits for exactly
			// createCount receives with no ctx escape. Send nil
			// unconditionally so a panic does not hang SetHosts forever.
			// Receiver is nil-tolerant.
			defer recoverGoroutine(p.session.logger, "policyConnPool.create.hostPool", func(err error) {
				pools <- nil
			})
			// create a connection pool for the host
			pools <- newHostConnPool(
				p.session,
				host,
				p.port,
				p.numConns,
				p.keyspace,
			)
		}(host)
	}

	// add created pools
	for createCount > 0 {
		pool := <-pools
		createCount--
		// pool may be nil if the create goroutine panicked (see the
		// recovery teardown above). Skip nil safely.
		if pool != nil && pool.Size() > 0 {
			// add pool only if there a connections available
			p.hostConnPools[pool.host.HostID()] = pool
		}
	}

	for addr := range toRemove {
		pool := p.hostConnPools[addr]
		delete(p.hostConnPools, addr)
		p.closeAsync(pool, "hostConnPool.Close.async")
	}
}

// closeAsync closes pool on its own goroutine, so the caller never blocks on it.
//
// Parameters:
//   - pool: the pool to close
//   - site: the recoverGoroutine label naming the caller
func (p *policyConnPool) closeAsync(pool *hostConnPool, site string) {
	go func() {
		defer recoverGoroutine(p.session.logger, site, nil)
		pool.Close()
	}()
}

// upHostCount reports how many hosts hold a pool and are up.
//
// It is the budget hostSelector snapshots:
// the hosts a selection policy can hand out and the pool can serve.
// Registration alone would not do:
// startPoolFill registers a pool before adding the host to the policy,
// and reconnectDownedHosts drives that path for hosts that are still down,
// so a registered pool can belong to a host no iterator will produce for as long as its fill takes.
// Up excludes those, and markHostDown sets NodeDown before it touches the policy or the pool.
// A host rejected by HostFilter never enters either.
//
// The count and the policy's host set can still differ for a moment -
// a host that reached UP but is not yet published to the policy,
// a host joining or leaving between a Pick and this snapshot -
// and two host IDs behind one connect address are one host to the policies but two pools here;
// hostSelector bounds what such a mismatch can cost.
//
// Returns:
//   - int: the number of up hosts with a registered pool
func (p *policyConnPool) upHostCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	count := 0
	for _, pool := range p.hostConnPools {
		if pool.host.IsUp() {
			count++
		}
	}
	return count
}

func (p *policyConnPool) Size() int {
	p.mu.RLock()
	count := 0
	for _, pool := range p.hostConnPools {
		count += pool.Size()
	}
	p.mu.RUnlock()

	return count
}

func (p *policyConnPool) getPool(host *HostInfo) (pool *hostConnPool, ok bool) {
	hostID := host.HostID()
	p.mu.RLock()
	pool, ok = p.hostConnPools[hostID]
	p.mu.RUnlock()
	return
}

// getPoolFor returns the pool registered for host only if it was built for
// this exact *HostInfo object.
//
// The pointer check is evaluated under p.mu, so a pool that refreshRing
// replaced under the same host ID is reported as missing.
//
// Parameters:
//   - host: the ring object whose pool is wanted
//
// Returns:
//   - *hostConnPool: the registered pool, or nil
//   - bool: true when a pool is registered for host.HostID() and pool.host == host
func (p *policyConnPool) getPoolFor(host *HostInfo) (pool *hostConnPool, ok bool) {
	hostID := host.HostID()
	p.mu.RLock()
	pool, ok = p.hostConnPools[hostID]
	if ok && pool.host != host {
		pool, ok = nil, false
	}
	p.mu.RUnlock()
	return
}

func (p *policyConnPool) getPoolByHostID(hostID string) (pool *hostConnPool, ok bool) {
	p.mu.RLock()
	pool, ok = p.hostConnPools[hostID]
	p.mu.RUnlock()
	return
}

// generation returns the wake channel a waiter should block on, creating it when
// this is the first waiter of the current generation.
//
// The caller must hold p.mu.
// Capturing the channel and reading pool state must happen in this order,
// otherwise a notify between the two would be lost.
//
// Returns:
//   - <-chan struct{}: closed by the next pool event
func (p *policyConnPool) generation() <-chan struct{} {
	if p.wake == nil {
		p.wake = make(chan struct{})
	}
	return p.wake
}

// notifyLocked ends the current wake generation, waking every waiter holding it.
//
// The caller must hold p.mu.
// Dropping the channel makes the close happen at most once per generation;
// the next waiter creates a successor.
func (p *policyConnPool) notifyLocked() {
	if p.wake != nil {
		close(p.wake)
		p.wake = nil
	}
}

// notify ends the current wake generation.
//
// It must never be called while a hostConnPool.mu is held: the lock order is
// p.mu -> pool.mu and never the reverse.
func (p *policyConnPool) notify() {
	p.mu.Lock()
	p.notifyLocked()
	p.mu.Unlock()
}

// snapshot captures the wake generation and revalidates cands in one critical section.
//
// Doing both under p.mu is what makes a removal or a close unmissable: a waiter
// either observes the pool as unregistered (or the parent as closed), or it holds a
// generation that the later removal or close is guaranteed to end.
//
// Parameters:
//   - cands: the candidates a waiter still wants to wait for; filtered in place
//
// Returns:
//   - <-chan struct{}: the generation to block on, nil when the parent is closed
//   - bool: true when the parent pool is closed, which is terminal
//   - []fillCandidate: the candidates whose exact pool pointer is still registered
func (p *policyConnPool) snapshot(cands []fillCandidate) (<-chan struct{}, bool, []fillCandidate) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		// Terminal: no successor generation is ever created.
		return nil, true, nil
	}

	gen := p.generation()
	kept := cands[:0]
	for _, cand := range cands {
		if p.hostConnPools[cand.host.Info().HostID()] == cand.pool {
			kept = append(kept, cand)
		}
	}

	return gen, false, kept
}

func (p *policyConnPool) Close() {
	p.mu.Lock()
	p.closed = true
	pools := p.hostConnPools
	p.hostConnPools = map[string]*hostConnPool{}
	p.notifyLocked()
	p.mu.Unlock()

	if p.testAfterParentNotify != nil {
		p.testAfterParentNotify()
	}

	// The children are closed outside p.mu; a child never notifies the parent from
	// its own Close, so no waiter can be woken by a half-torn-down pool.
	for _, pool := range pools {
		pool.Close()
	}
}

// registerPool registers a pool for host under p.mu, without filling it.
//
// Pools are keyed by host ID, and refreshRing replaces a host that changed
// address with a new *HostInfo under the same ID, so the object a caller
// holds may be superseded by the time it gets here.
// Under p.mu, an object the ring no longer owns is ignored; for an owned one:
//
//   - no pool for the ID: one is registered;
//   - a pool built for this exact object: it is returned as it is;
//   - a pool built for another object under this ID: that pool belongs to
//     a superseded object and is replaced and closed.
//
// Together with removeHost, which takes the host out of the ring before the
// pool, every interleaving of a stale caller with a replacement ends with one
// pool for the ID, owned by the ring's current object.
//
// The fill is left to the caller because it must run with p.mu released:
// connect notifies the parent generation, which takes p.mu, so filling under
// it would self-deadlock. Splitting the two lets a caller that must decide
// something about host under another mutex - whether the ring still owns it,
// what its state is - hold that mutex across the registration without holding
// it across a dial.
//
// Parameters:
//   - host: the object to admit
//
// Returns:
//   - *hostConnPool: the registered pool, or nil when host was not admitted
//   - bool: whether the caller must fill the returned pool. It is true for
//     every non-nil pool, newly built or already registered: a pool that lost
//     its connections needs the refill just as much as a new one does, and
//     hostConnPool.fill rejects a concurrent fill on its own.
func (p *policyConnPool) registerPool(host *HostInfo) (*hostConnPool, bool) {
	hostID := host.HostID()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || !p.session.ring.owns(host) {
		return nil, false
	}
	pool, ok := p.hostConnPools[hostID]
	if ok && pool.host != host {
		// The registered pool was built for an object refreshRing has replaced.
		delete(p.hostConnPools, hostID)
		p.notifyLocked()
		p.closeAsync(pool, "hostConnPool.Close.addHost")
		ok = false
	}
	if !ok {
		pool = newHostConnPool(
			p.session,
			host,
			host.Port(), // TODO: if port == 0 use pool.port?
			p.numConns,
			p.keyspace,
		)

		p.hostConnPools[hostID] = pool
	}
	return pool, true
}

// addHost registers a pool for host, if the ring still owns host, and fills it.
//
// See registerPool for the admission rules; this is that registration followed
// by the fill, with p.mu released in between.
//
// Parameters:
//   - host: the object to admit
func (p *policyConnPool) addHost(host *HostInfo) {
	pool, needFill := p.registerPool(host)
	if !needFill {
		return
	}
	if pool.claimFill() {
		pool.runFill()
	}
}

// removeHost unregisters and closes the pool built for this exact *HostInfo.
//
// The pointer check runs under p.mu, so a caller holding an object that
// refreshRing has since replaced under the same host ID leaves the
// replacement's pool untouched.
//
// The unregistration and the wake share one p.mu section,
// so a query waiting for a fill on this pool cannot miss the removal.
//
// Parameters:
//   - host: the ring object whose pool should be removed
func (p *policyConnPool) removeHost(host *HostInfo) {
	hostID := host.HostID()
	p.mu.Lock()
	pool, ok := p.hostConnPools[hostID]
	if !ok || pool.host != host {
		p.mu.Unlock()
		return
	}

	delete(p.hostConnPools, hostID)
	p.notifyLocked()
	p.mu.Unlock()

	if p.testAfterParentNotify != nil {
		p.testAfterParentNotify()
	}

	p.closeAsync(pool, "hostConnPool.Close.removeHost")
}

// hostConnPool is a connection pool for a single host.
// Connection selection is based on a provided ConnSelectionPolicy
type hostConnPool struct {
	session  *Session
	host     *HostInfo
	port     int
	size     int
	keyspace string
	// protection for conns, closed, filling, fillsPending
	mu      sync.RWMutex
	conns   []*Conn
	closed  bool
	filling bool
	// fillsPending counts the fill claims that have been published and not yet
	// released by fillDone.
	// It is a plain int, not an atomic, on purpose:
	// pool.mu is the single linearization point,
	// so a claim and a pickOrState decision are totally ordered.
	fillsPending int

	pos    uint32
	logger StructuredLogger
}

func (h *hostConnPool) String() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return fmt.Sprintf("[filling=%v closed=%v conns=%v size=%v host=%v]",
		h.filling, h.closed, len(h.conns), h.size, h.host)
}

func newHostConnPool(session *Session, host *HostInfo, port, size int,
	keyspace string) *hostConnPool {

	pool := &hostConnPool{
		session:  session,
		host:     host,
		port:     port,
		size:     size,
		keyspace: keyspace,
		conns:    make([]*Conn, 0, size),
		filling:  false,
		closed:   false,
		logger:   session.logger,
	}

	// the pool is not filled or connected
	return pool
}

// claimFill publishes a pending-fill claim so a waiting query observes the pool as
// poolEmptyPending instead of giving up.
//
// The claim and the pool state it describes are published under the same pool.mu,
// which is what makes the claim unmissable.
// Exactly one fillDone must release each claim.
//
// Returns:
//   - bool: true when the claim was published; false when the pool is closed
func (pool *hostConnPool) claimFill() bool {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	if pool.closed {
		return false
	}
	pool.fillsPending++

	return true
}

// scheduleFill claims a fill and runs it on a new goroutine.
func (pool *hostConnPool) scheduleFill() {
	if pool.claimFill() {
		// The goroutine is spawned outside pool.mu.
		go pool.runFill()
	}
}

// runFill runs one claimed fill cycle and releases its claim exactly once.
//
// The caller must already hold a claim published by claimFill or by
// HandleError's critical section.
func (pool *hostConnPool) runFill() {
	var handedOff bool
	// Registered first, so it runs last and can still recover a panic escaping
	// fillDone itself.
	defer recoverGoroutine(pool.logger, "hostConnPool.runFill", nil)
	defer func() {
		if !handedOff {
			pool.fillDone()
		}
	}()

	handedOff = pool.fill()
}

// fillDone releases one fill claim and ends the current wake generation.
//
// The decrement is published under pool.mu and the parent is notified only after
// that lock was released, keeping the p.mu -> pool.mu lock order intact.
// The poolFillDone checkpoint is reached between the two, so observing it proves
// the decrement already happened.
func (pool *hostConnPool) fillDone() {
	pool.mu.Lock()
	pool.fillsPending--
	pool.mu.Unlock()

	pool.testHook(poolFillDone)

	pool.notify()
}

// notify ends the session-wide wake generation, if this pool belongs to a session.
//
// It tolerates a zero-value pool so hand-built pools in tests never panic.
func (pool *hostConnPool) notify() {
	if pool.session == nil || pool.session.pool == nil {
		return
	}
	pool.session.pool.notify()
}

// testHook invokes the cluster's pool test hook, if one is configured.
//
// Parameters:
//   - ev: the checkpoint being reached
func (pool *hostConnPool) testHook(ev poolEvent) {
	if pool.session == nil || pool.session.cfg.testPoolHook == nil {
		return
	}
	pool.session.cfg.testPoolHook(ev, pool.host)
}

// Pick a connection from this connection pool for the given query.
func (pool *hostConnPool) Pick() *Conn {
	pool.mu.RLock()

	if pool.closed {
		pool.mu.RUnlock()
		return nil
	}

	size := len(pool.conns)
	needFill := size < pool.size
	if size == 0 {
		pool.mu.RUnlock()
		if needFill {
			// The claim is published outside the read lock, so it is ordered
			// against every pickOrState decision by pool.mu alone.
			pool.scheduleFill()
		}
		return nil
	}

	pos := int(atomic.AddUint32(&pool.pos, 1) - 1)

	var (
		leastBusyConn    *Conn
		streamsAvailable int
	)

	// find the conn which has the most available streams, this is racy
	for i := 0; i < size; i++ {
		conn := pool.conns[(pos+i)%size]
		if streams := conn.AvailableStreams(); streams > streamsAvailable {
			leastBusyConn = conn
			streamsAvailable = streams
		}
	}

	pool.mu.RUnlock()

	if needFill {
		pool.scheduleFill()
	}

	return leastBusyConn
}

// pickOrState is the slow path taken only after Pick returned nil.
//
// One read lock decides both "is there a usable connection" and "is a fill in
// flight" from the same read of conns and fillsPending, so a connection cannot
// disappear between the two questions.
// It never schedules a fill: Pick already published a claim if one was due.
//
// Returns:
//   - *Conn: the least busy connection, non-nil only with poolPicked
//   - poolState: what this single read observed
func (pool *hostConnPool) pickOrState() (*Conn, poolState) {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	if pool.closed {
		return nil, poolClosed
	}

	size := len(pool.conns)
	if size == 0 {
		if pool.fillsPending > 0 {
			return nil, poolEmptyPending
		}
		return nil, poolEmptyIdle
	}

	pos := int(atomic.AddUint32(&pool.pos, 1) - 1)

	var (
		leastBusyConn    *Conn
		streamsAvailable int
	)

	// same selection loop as Pick
	for i := 0; i < size; i++ {
		conn := pool.conns[(pos+i)%size]
		if streams := conn.AvailableStreams(); streams > streamsAvailable {
			leastBusyConn = conn
			streamsAvailable = streams
		}
	}

	if leastBusyConn == nil {
		return nil, poolSaturated
	}

	return leastBusyConn, poolPicked
}

// Size returns the number of connections currently active in the pool
func (pool *hostConnPool) Size() int {
	pool.mu.RLock()
	defer pool.mu.RUnlock()

	return len(pool.conns)
}

// Close the connection pool
func (pool *hostConnPool) Close() {
	pool.mu.Lock()

	if pool.closed {
		pool.mu.Unlock()
		return
	}
	pool.closed = true

	// ensure we dont try to reacquire the lock in handleError
	// TODO: improve this as the following can happen
	// 1) we have locked pool.mu write lock
	// 2) conn.Close calls conn.closeWithError(nil)
	// 3) conn.closeWithError calls conn.Close() which returns an error
	// 4) conn.closeWithError calls pool.HandleError with the error from conn.Close
	// 5) pool.HandleError tries to lock pool.mu
	// deadlock

	// empty the pool
	conns := pool.conns
	pool.conns = nil

	pool.mu.Unlock()

	// close the connections
	for _, conn := range conns {
		conn.Close()
	}
}

// fill runs one fill cycle for the connection pool.
//
// Every caller must already hold a pending-fill claim (see claimFill) and runFill
// owns its release: the claim is what a waiting query observes as poolEmptyPending.
// filling and fillsPending are deliberately different things.
// filling is fill's own admission gate, so a claim can be published and then exit
// at the double check below because another runner won admission;
// that runner holds a claim of its own, so no waiter is left without a pending fill.
//
// Returns:
//   - handedOff: true once the asynchronous branch was spawned,
//     after which that goroutine owns the single fillDone for this claim
func (pool *hostConnPool) fill() (handedOff bool) {
	pool.mu.RLock()
	// avoid filling a closed pool, or concurrent filling
	if pool.closed || pool.filling {
		pool.mu.RUnlock()
		return false
	}

	// determine the filling work to be done
	startCount := len(pool.conns)
	fillCount := pool.size - startCount

	// avoid filling a full (or overfull) pool
	if fillCount <= 0 {
		pool.mu.RUnlock()
		return false
	}

	// switch from read to write lock
	pool.mu.RUnlock()
	pool.testHook(poolFillAdmission)
	pool.mu.Lock()

	// double check everything since the lock was released
	startCount = len(pool.conns)
	fillCount = pool.size - startCount
	if pool.closed || pool.filling || fillCount <= 0 {
		// looks like another goroutine already beat this
		// goroutine to the filling
		pool.mu.Unlock()
		return false
	}

	// ok fill the pool
	pool.filling = true

	// allow others to access the pool while filling
	pool.mu.Unlock()
	// only this goroutine should make calls to fill/empty the pool at this
	// point until after this routine or its subordinates calls
	// fillingStopped

	// Synchronous-body panic guard: pool.filling is true; if the
	// synchronous part below panics before the async branch takes over
	// the fillingStopped responsibility, the pool is permanently stuck
	// (later Pick / HandleError see filling=true and skip refilling).
	// `handedOff` flips once the async goroutine has been spawned, after
	// which IT owns calling fillingStopped and the single fillDone.
	defer func() {
		if r := recover(); r != nil {
			if !handedOff {
				// Clear the filling claim; swallow fillingStopped panic so
				// it can't escape this recover.
				func() {
					defer func() { _ = recover() }()
					pool.fillingStopped(fmt.Errorf("gocql: fill panicked: %v", r))
				}()
			}
			// Surface the panic through the standard handler for uniform
			// logging (stack trace, etc.).
			handleRecoveredPanic(pool.logger, "hostConnPool.fill", r, nil)
		}
	}()

	// fill only the first connection synchronously
	if startCount == 0 {
		err := pool.connect()
		pool.logConnectErr(err)

		if err != nil {
			// probably unreachable host
			pool.fillingStopped(err)
			return false
		}
		// notify the session that this node is connected
		go func() {
			defer recoverGoroutine(pool.logger, "Session.handleNodeConnected", nil)
			pool.session.handleNodeConnected(pool.host)
		}()

		// filled one
		fillCount--
	}

	// fill the rest of the pool asynchronously
	handedOff = true
	go func() {
		// Registered first, so it runs last: the claim is released after
		// fillingStopped, and the parent generation is notified once.
		defer pool.fillDone()

		var stopped bool
		// Recovery teardown: if connectMany panics,
		// pool.filling stays true forever and no future fill() can run.
		// Ensure fillingStopped runs.
		// `stopped` flag prevents double-call when the body completed normally.
		defer recoverGoroutine(pool.logger, "hostConnPool.fill.async", func(err error) {
			if !stopped {
				pool.fillingStopped(err)
			}
		})

		pool.testHook(poolFillAsyncStart)

		err := pool.connectMany(fillCount)

		// mark the end of filling
		pool.fillingStopped(err)
		stopped = true

		if err == nil && startCount > 0 {
			// notify the session that this node is connected again
			// handleNodeConnected runs application HostUp callbacks,
			// so it is recovered like the initial-fill notification above.
			go func() {
				defer recoverGoroutine(pool.logger, "Session.handleNodeConnected", nil)
				pool.session.handleNodeConnected(pool.host)
			}()
		}
	}()

	return handedOff
}

func (pool *hostConnPool) logConnectErr(err error) {
	if opErr, ok := err.(*net.OpError); ok && (opErr.Op == "dial" || opErr.Op == "read") {
		// connection refused
		// these are typical during a node outage so avoid log spam.
		pool.logger.Debug("Pool unable to establish a connection to host.",
			NewLogFieldIP("host_addr", pool.host.ConnectAddress()), NewLogFieldString("host_id", pool.host.HostID()), NewLogFieldError("err", err))
	} else if err != nil {
		// unexpected error
		pool.logger.Debug("Pool failed to connect to host due to error.",
			NewLogFieldIP("host_addr", pool.host.ConnectAddress()), NewLogFieldString("host_id", pool.host.HostID()), NewLogFieldError("err", err))
	}
}

// transition back to a not-filling state.
func (pool *hostConnPool) fillingStopped(err error) {
	if err != nil {
		pool.logger.Warning("Connection pool filling failed.",
			NewLogFieldIP("host_addr", pool.host.ConnectAddress()), NewLogFieldString("host_id", pool.host.HostID()), NewLogFieldError("err", err))
		// wait for some time to avoid back-to-back filling
		// this provides some time between failed attempts
		// to fill the pool for the host to recover
		time.Sleep(time.Duration(rand.Int31n(100)+31) * time.Millisecond)
	}

	pool.mu.Lock()
	pool.filling = false
	count := len(pool.conns)
	host := pool.host
	pool.mu.Unlock()

	// A fill cancelled by session shutdown must not convict its host: the session
	// context is cancelled before the refreshers are joined in Close, so a fill in
	// flight then fails, and marking the host DOWN here would fire the user's
	// HostDown callback during shutdown. Only ctx cancellation is skipped; every
	// other failure still convicts as before. fillingStopped already did not check
	// pool.closed, so a post-shutdown conviction was possible before this; the
	// cancel is one more trigger, not the first.
	if pool.session.ctx.Err() != nil {
		return
	}

	// if we errored and the size is now zero, make sure the host is marked as down
	// see https://github.com/apache/cassandra-gocql-driver/issues/1614
	pool.logger.Debug("Logging number of connections of pool after filling stopped.",
		NewLogFieldIP("host_addr", host.ConnectAddress()), NewLogFieldString("host_id", host.HostID()), NewLogFieldInt("count", count))
	if err != nil && count == 0 {
		if pool.session.cfg.ConvictionPolicy.AddFailure(err, host) {
			pool.session.handleHostDown(host)
		}
	}
}

// connectMany creates new connections concurrent.
func (pool *hostConnPool) connectMany(count int) error {
	if count == 0 {
		return nil
	}
	var (
		wg         sync.WaitGroup
		mu         sync.Mutex
		connectErr error
	)
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			// wg.Done() registered first → runs last (LIFO). On panic,
			// recoverGoroutine fires first and records the error so the
			// parent observes the failure instead of false success.
			defer wg.Done()
			defer recoverGoroutine(pool.logger, "connectMany.worker", func(err error) {
				mu.Lock()
				if connectErr == nil {
					connectErr = err
				}
				mu.Unlock()
			})

			err := pool.connect()
			pool.logConnectErr(err)
			if err != nil {
				mu.Lock()
				connectErr = err
				mu.Unlock()
			}
		}()
	}
	// wait for all connections are done
	wg.Wait()

	return connectErr
}

// create a new connection to the host and add it to the pool
func (pool *hostConnPool) connect() (err error) {
	// TODO: provide a more robust connection retry mechanism, we should also
	// be able to detect hosts that come up by trying to connect to downed ones.
	// try to connect
	var conn *Conn
	reconnectionPolicy := pool.session.cfg.ReconnectionPolicy
	maxRetries := reconnectionPolicy.GetMaxRetries()
	for i := 0; i < maxRetries; i++ {
		pool.testHook(poolConnectAttempt)
		conn, err = pool.session.connect(pool.session.ctx, pool.host, pool)
		if err == nil {
			break
		}
		pool.logger.Warning("Pool failed to connect to host. Reconnecting according to the reconnection policy.",
			NewLogFieldIP("host", pool.host.ConnectAddress()),
			NewLogFieldString("host_id", pool.host.HostID()),
			NewLogFieldError("err", err),
			NewLogFieldString("reconnectionPolicy", fmt.Sprintf("%T", reconnectionPolicy)))
		// Skip the inter-attempt sleep after the final attempt — no
		// further retry will run, so waiting only adds latency before
		// the error surfaces.
		if i+1 >= maxRetries {
			break
		}
		select {
		case <-time.After(reconnectionPolicy.GetInterval(i)):
		case <-pool.session.ctx.Done():
			return pool.session.ctx.Err()
		}
	}

	if err != nil {
		return err
	}

	if pool.keyspace != "" {
		// set the keyspace
		if err = conn.UseKeyspace(pool.keyspace); err != nil {
			conn.Close()
			return err
		}
	}

	// add the Conn to the pool
	pool.mu.Lock()

	if pool.closed {
		pool.mu.Unlock()
		conn.Close()
		return nil
	}

	pool.conns = append(pool.conns, conn)
	pool.mu.Unlock()

	pool.testHook(poolConnAppended)

	// Wake waiters on the first connection of a multi-connection cycle rather
	// than only when the whole cycle ends.
	// The notify takes p.mu, so pool.mu must already be released here.
	pool.notify()

	return nil
}

// handle any error from a Conn
func (pool *hostConnPool) HandleError(conn *Conn, err error, closed bool) {
	if !closed {
		// still an open connection, so continue using it
		return
	}

	// The mutation keeps a deferred unlock so a panicking application logger
	// cannot strand pool.mu; only the spawn moves outside the lock.
	spawn := func() bool {
		// TODO: track the number of errors per host and detect when a host is dead,
		// then also have something which can detect when a host comes back.
		pool.mu.Lock()
		defer pool.mu.Unlock()

		if pool.closed {
			// pool closed: nothing was removed, so nothing is claimed
			return false
		}

		pool.logger.Info("Pool connection error.",
			NewLogFieldString("addr", conn.addr), NewLogFieldError("err", err))

		// find the connection index
		for i, candidate := range pool.conns {
			if candidate != conn {
				continue
			}
			// remove the connection, not preserving order
			pool.conns[i], pool.conns = pool.conns[len(pool.conns)-1], pool.conns[:len(pool.conns)-1]

			// The claim is published in the same critical section as the
			// removal, so a query cannot observe the pool empty and idle in
			// between.
			pool.fillsPending++
			return true
		}

		// the connection was not ours: nothing removed, nothing claimed
		return false
	}()

	if spawn {
		go pool.runFill()
	}
}
