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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/maypok86/otter/v2"
)

// Session is the interface used by users to interact with the database.
//
// It's safe for concurrent use by multiple goroutines and a typical usage
// scenario is to have one global session object to interact with the
// whole Cassandra cluster.
//
// This type extends the Node interface by adding a convenient query builder
// and automatically sets a default consistency level on all operations
// that do not have a consistency level set.
type Session struct {
	cons                 Consistency
	pageSize             int
	prefetch             float64
	routingMetadataCache *routingKeyInfoLRU
	schemaDescriber      *schemaDescriber
	trace                Tracer
	queryObserver        QueryObserver
	batchObserver        BatchObserver
	connectObserver      ConnectObserver
	frameObserver        FrameHeaderObserver
	streamObserver       StreamObserver
	hostSource           *ringDescriber
	ringRefresher        *refreshDebouncer
	stmtsLRU             *preparedLRU
	types                *RegisteredTypes

	connCfg *ConnConfig

	executor *queryExecutor
	pool     *policyConnPool
	policy   HostSelectionPolicy

	ring     ring
	metadata clusterMetadata

	control *controlConn

	// event handlers
	nodeEvents *eventDebouncer

	// host state and topology change listeners
	hostListeners internalHostListeners

	// schema change listeners
	schemaListeners internalSchemaListeners

	// session ready listeners
	sessionReadyListeners internalSessionReadyListener

	// ring metadata
	useSystemSchema           bool
	hasAggregatesAndFunctions bool

	cfg ClusterConfig

	ctx    context.Context
	cancel context.CancelFunc

	// sessionStateMu protects isClosed and isInitialized.
	sessionStateMu sync.RWMutex
	// isClosed is true once Session.Close is finished.
	isClosed bool
	// isClosing bool is true once Session.Close is started.
	isClosing bool
	// isInitialized is true once Session.init succeeds.
	// you can use initialized() to read the value.
	isInitialized bool

	logger StructuredLogger

	// hostPublishMu serialises every membership and state transition of a ring
	// host in the selection policy (AddHost, RemoveHost, HostUp, HostDown)
	// with removeHost's un-publication of that host.
	// withOwnedHost re-checks ring ownership inside the critical section,
	// so a transition that lost the race to a removal does nothing.
	hostPublishMu sync.Mutex

	// hostPublishClosed gates host publication on shutdown. Close stores true
	// before it cancels the session context, and withOwnedHost reads it inside
	// hostPublishMu, so a policy publication scheduled by a late pool fill is
	// rejected. The narrow property this buys: Close itself takes no
	// hostPublishMu, so it cannot block on, or self-deadlock against, an
	// application policy callback that this path would run under that mutex. It
	// does not make Close independent of every callback: an already-admitted
	// callback still runs to completion, and an existing flusher join elsewhere
	// in Close can still wait on a callback or its mutex dependencies.
	hostPublishClosed atomic.Bool

	// publishedHosts records the ring objects this session has already announced
	// to the selection policy with AddHost, keyed by host ID. It is read and
	// written under hostPublishMu.
	//
	// It answers a question a registered pool cannot: "has this exact object had
	// its first policy publication?". The two are not the same. The scheduled
	// reconnect path admits a pool without publishing, and the fill's success
	// reports HostUp, which tokenAwareHostPolicy forwards to its fallback without
	// touching the token ring - only AddHost rebuilds that. A host recovered that
	// way therefore has a pool, is UP, and is still missing from token-aware
	// routing, and an admission repair keyed on pool existence would never notice.
	//
	// The value is the object, not a flag, so the record is pointer-sensitive the
	// same way ring.owns is: a replacement that takes the same host ID does not
	// inherit its predecessor's publication.
	//
	// It is allocated in NewSession, before the control connection can produce a
	// host event, and never replaced afterwards.
	publishedHosts map[string]*HostInfo

	// outage is the session's outage ledger. It is allocated in NewSession,
	// before the control connection can produce a host event, and never
	// replaced afterwards: a later allocation would drop generations, nudges
	// and membership that the control connection's own UP and DOWN events have
	// already recorded.
	outage outageState

	// Per-instance test hooks, invoked immediately after ring.owns returned true
	// in handleHostDown / handleNodeConnected so tests can replace the ring entry
	// inside the check-to-mutation window.
	// Default nil (no-op).
	testAfterOwnsDown      func()
	testAfterOwnsConnected func()
	// testAfterNodeConnected runs when handleNodeConnected returns, on every path,
	// so a test can join that asynchronous callback. Default nil (no-op).
	testAfterNodeConnected func(host *HostInfo)
}

func addrsToHosts(addrs []string, defaultPort int, logger StructuredLogger) ([]*HostInfo, error) {
	var hosts []*HostInfo
	for _, hostaddr := range addrs {
		resolvedHosts, err := hostInfo(hostaddr, defaultPort)
		if err != nil {
			// Try other hosts if unable to resolve DNS name
			if _, ok := err.(*net.DNSError); ok {
				logger.Error("DNS error.", NewLogFieldError("err", err))
				continue
			}
			return nil, err
		}

		hosts = append(hosts, resolvedHosts...)
	}
	if len(hosts) == 0 {
		return nil, errors.New("failed to resolve any of the provided hostnames")
	}
	return hosts, nil
}

// NewSession wraps an existing Node.
func NewSession(cfg ClusterConfig) (*Session, error) {
	// Check that hosts in the ClusterConfig is not empty
	if len(cfg.Hosts) < 1 {
		return nil, ErrNoHosts
	}

	// Check that either Authenticator is set or AuthProvider, not both
	if cfg.Authenticator != nil && cfg.AuthProvider != nil {
		return nil, errors.New("Can't use both Authenticator and AuthProvider in cluster config.")
	}

	if cfg.SerialConsistency > 0 && !cfg.SerialConsistency.isSerial() {
		return nil, fmt.Errorf("the default SerialConsistency level is not allowed to be anything else but SERIAL or LOCAL_SERIAL. Recived value: %v", cfg.SerialConsistency)
	}

	// TODO: we should take a context in here at some point
	ctx, cancel := context.WithCancel(context.TODO())

	s := &Session{
		cons:            cfg.Consistency,
		prefetch:        cfg.NextPagePrefetch,
		cfg:             cfg,
		pageSize:        cfg.PageSize,
		stmtsLRU:        newPreparedLRU(cfg.MaxPreparedStmts),
		connectObserver: cfg.ConnectObserver,
		ctx:             ctx,
		cancel:          cancel,
		logger:          cfg.newLogger(),
		trace:           cfg.Tracer,
		publishedHosts:  make(map[string]*HostInfo),
		outage: outageState{
			downSet: make(map[string]struct{}),
			nudge:   make(chan struct{}, 1),
		},
	}
	if cfg.RegisteredTypes == nil {
		s.types = GlobalTypes.Copy()
	} else {
		s.types = cfg.RegisteredTypes.Copy()
	}

	schemaDebounce := schemaRefreshDebounceTime
	if cfg.schemaRefreshDebounce > 0 {
		schemaDebounce = cfg.schemaRefreshDebounce
	}
	s.schemaDescriber = newSchemaDescriber(s, newRefreshDebouncer(schemaDebounce, s.runSchemaRefresh, s.logger))

	// The overflow callback is a method value, not s.ringRefresher.trigger: the
	// refresher is not built until a few lines below this, so binding its method here
	// would capture a nil receiver and panic on the first overflow.
	s.nodeEvents = newEventDebouncer("NodeEvents", s.handleNodeEvent, s.triggerRingRefresh, s.logger)

	s.routingMetadataCache = newRoutingKeyInfoLRU(cfg.MaxRoutingKeyInfo)

	s.hostSource = &ringDescriber{session: s}
	s.ringRefresher = newRefreshDebouncer(ringRefreshDebounceTime, s.runRingRefresh, s.logger)

	s.queryObserver = cfg.QueryObserver
	s.batchObserver = cfg.BatchObserver
	s.connectObserver = cfg.ConnectObserver
	s.frameObserver = cfg.FrameHeaderObserver
	s.streamObserver = cfg.StreamObserver

	// Propagate node status, topology and schema change listeners
	s.hostListeners = newInternalHostStateListeners(
		s,
		cfg.Metadata.HostListener.HostStateChangeListener,
		cfg.Metadata.HostListener.TopologyChangeListener,
	)

	// Propagate schema change listeners
	s.schemaListeners = newInternalSchemaChangeListeners(
		cfg.Metadata.SchemaListener.KeyspaceChangeListener,
		cfg.Metadata.SchemaListener.TableChangeListener,
		cfg.Metadata.SchemaListener.UserTypeChangeListener,
		cfg.Metadata.SchemaListener.FunctionChangeListener,
		cfg.Metadata.SchemaListener.AggregateChangeListener,
	)

	if cfg.Metadata.CacheMode == Disabled && s.schemaListeners.hasSchemaChangeListeners() {
		return nil, errors.New("Schema change listeners are not supported in Disabled metadata cache mode")
	}

	if cfg.Metadata.CacheMode == KeyspaceOnly && s.schemaListeners.hasNonKeyspaceSchemaChangeListeners() {
		return nil, errors.New("Schema change listeners are not supported in KeyspaceOnly metadata cache mode")
	}

	// Propagate session ready listener
	s.sessionReadyListeners = newInternalSessionReadyListener(cfg.Metadata.SessionReadyListener)

	// Check the TLS Config before trying to connect to anything external
	connCfg, err := connConfig(&s.cfg)
	if err != nil {
		// TODO: Return a typed error
		return nil, fmt.Errorf("gocql: unable to create session: %w", err)
	}
	s.connCfg = connCfg

	if cfg.PoolConfig.HostSelectionPolicy == nil {
		cfg.PoolConfig.HostSelectionPolicy = RoundRobinHostPolicy()
	}
	s.pool = cfg.PoolConfig.buildPool(s)
	s.policy = cfg.PoolConfig.HostSelectionPolicy

	// set the executor here in case the policy needs to execute queries in Init
	s.executor = &queryExecutor{
		pool:   s.pool,
		policy: cfg.PoolConfig.HostSelectionPolicy,
	}

	s.policy.Init(s)

	if err := s.init(); err != nil {
		s.Close()
		if err == ErrNoConnectionsStarted {
			// This error used to be generated inside NewSession & returned directly
			// Forward it on up to be backwards compatible
			return nil, ErrNoConnectionsStarted
		} else {
			// TODO(zariel): dont wrap this error in fmt.Errorf, return a typed error
			return nil, fmt.Errorf("gocql: unable to create session: %w", err)
		}
	}

	return s, nil
}

func (s *Session) init() error {
	hosts, err := addrsToHosts(s.cfg.Hosts, s.cfg.Port, s.logger)
	if err != nil {
		return err
	}

	if !s.cfg.disableControlConn {
		s.control = createControlConn(s)
		if s.cfg.ProtoVersion == 0 {
			proto, err := s.control.discoverProtocol(hosts)
			if err != nil {
				return fmt.Errorf("unable to discover protocol version: %w", err)
			} else if proto == 0 {
				return errors.New("unable to discovery protocol version")
			}

			// TODO(zariel): we really only need this in 1 place
			s.cfg.ProtoVersion = proto
			s.connCfg.ProtoVersion = proto
			s.logger.Info("Discovered protocol version.", NewLogFieldInt("protocol_version", proto))
		}

		if err := s.control.connect(hosts, true); err != nil {
			return err
		}

		if !s.cfg.DisableInitialHostLookup {
			var partitioner string
			newHosts, partitioner, err := s.hostSource.GetHosts()
			if err != nil {
				return err
			}
			s.policy.SetPartitioner(partitioner)
			filteredHosts := make([]*HostInfo, 0, len(newHosts))
			for _, host := range newHosts {
				if !s.cfg.filterHost(host) {
					filteredHosts = append(filteredHosts, host)
				}
			}
			if len(filteredHosts) < 1 {
				return errors.New("gocql: HostFilter rejected every host during initial host lookup")
			}

			hosts = filteredHosts
			s.logger.Info("Refreshed ring.", NewLogFieldString("ring", ringString(hosts)))
		} else {
			s.logger.Info("Not performing a ring refresh because DisableInitialHostLookup is true.")
		}
	}

	for _, host := range hosts {
		// In case when host lookup is disabled and when we are in unit tests,
		// host are not discovered, and we are missing host ID information used
		// by internal logic.
		// Associate random UUIDs here with all hosts missing this information.
		if len(host.HostID()) == 0 {
			host.setHostID(MustRandomUUID().String())
		}
	}

	hostMap := make(map[string]*HostInfo, len(hosts))
	for _, host := range hosts {
		hostMap[host.HostID()] = host
	}

	hosts = hosts[:0]
	// each host will increment left and decrement it after connecting and once
	// there's none left, we'll close hostCh
	var left int64
	// we will receive up to len(hostMap) of messages so create a buffer so we
	// don't end up stuck in a goroutine if we stopped listening
	connectedCh := make(chan struct{}, len(hostMap))
	// we add one here because we don't want to end up closing hostCh until we're
	// done looping and the decerement code might be reached before we've looped
	// again
	atomic.AddInt64(&left, 1)
	for _, host := range hostMap {
		host, exists := s.ring.addOrUpdate(host)
		if s.cfg.filterHost(host) {
			continue
		}
		if !exists {
			s.logger.Info("Adding host (session initialization).",
				NewLogFieldIP("host_addr", host.ConnectAddress()), NewLogFieldString("host_id", host.HostID()))
		}

		atomic.AddInt64(&left, 1)
		go func() {
			completed := false
			// Coordination teardown: parent at session.go:374 reads
			// `for range connectedCh`. The chan closes when `left` hits 0.
			// If addHost panics, the body's dec is skipped and the close
			// never fires — parent hangs. Teardown completes the dec and
			// either closes (if it drove left to 0) or sends a single
			// struct so the iteration progresses.
			defer recoverGoroutine(s.logger, "Session.init.addHost", func(err error) {
				if completed {
					return
				}
				if atomic.AddInt64(&left, -1) == 0 {
					// safe-close: another goroutine may have raced us
					defer func() { _ = recover() }()
					close(connectedCh)
				} else {
					select {
					case connectedCh <- struct{}{}:
					case <-s.ctx.Done():
					}
				}
			})

			s.pool.addHost(host)
			connectedCh <- struct{}{}

			// if there are no hosts left, then close the hostCh to unblock the loop
			// below if its still waiting
			shouldClose := atomic.AddInt64(&left, -1) == 0
			completed = true // mark BEFORE close — a close panic must not double-dec
			if shouldClose {
				close(connectedCh)
			}
		}()

		hosts = append(hosts, host)
	}
	// once we're done looping we subtract the one we initially added and check
	// to see if we should close
	if atomic.AddInt64(&left, -1) == 0 {
		close(connectedCh)
	}

	// If we disable the initial host lookup, we need to still check if the
	// cluster is using the newer system schema or not... however, if control
	// connection is disable, we really have no choice, so we just make our
	// best guess...
	if !s.cfg.disableControlConn && s.cfg.DisableInitialHostLookup {
		newer, _ := checkSystemSchema(s.control)
		s.useSystemSchema = newer
	} else {
		version := s.ring.rrHost().Version()
		s.useSystemSchema = version.AtLeast(3, 0, 0)
		s.hasAggregatesAndFunctions = version.AtLeast(2, 2, 0)
	}

	// before waiting for them to connect, add them all to the policy so we can
	// utilize efficiencies by calling AddHosts if the policy supports it
	type bulkAddHosts interface {
		AddHosts([]*HostInfo)
	}
	// The control connection's heartbeat is already running,
	// so a reconnect-driven ring refresh can replace one of these objects before this point.
	// Publish only what the ring still owns, and do it under hostPublishMu,
	// so that such a removal either precedes this (and the object is skipped)
	// or follows it (and un-publishes the object again).
	if s.cfg.testInitPublishHook != nil {
		s.cfg.testInitPublishHook()
	}
	func() {
		// Scoped so the deferred unlock covers a panicking policy without holding
		// the mutex across the connection wait that follows.
		s.hostPublishMu.Lock()
		defer s.hostPublishMu.Unlock()
		owned := make([]*HostInfo, 0, len(hosts))
		for _, host := range hosts {
			if s.ring.owns(host) {
				owned = append(owned, host)
			}
		}
		if v, ok := s.policy.(bulkAddHosts); ok {
			v.AddHosts(owned)
		} else {
			for _, host := range owned {
				s.policy.AddHost(host)
			}
		}
		// Record the publication so a later admission repair does not announce
		// these hosts a second time. Both branches above are an AddHost.
		for _, host := range owned {
			s.publishedHosts[host.HostID()] = host
		}
	}()

	readyPolicy, _ := s.policy.(ReadyPolicy)
	// now loop over connectedCh until it's closed (meaning we've connected to all)
	// or until the policy says we're ready
	for range connectedCh {
		if readyPolicy != nil && readyPolicy.Ready() {
			break
		}
	}

	// The scheduler always runs: the periodic ring refresh is its own safety net
	// and is not something ReconnectInterval turns off. With ReconnectInterval at
	// zero the worker serves that refresh and nothing else.
	go s.reconnectDownedHosts(s.cfg.ReconnectInterval)

	if s.pool.Size() == 0 {
		return ErrNoConnectionsStarted
	}

	// Invoke KeyspaceChanged to let the policy cache the session keyspace
	// parameters. This is used by tokenAwareHostPolicy to discover replicas.
	if !s.cfg.disableControlConn && s.schemaDescriber != nil {
		err := s.schemaDescriber.refreshSchemaMetadata()
		if err != nil {
			s.logger.Warning("Failed to initialize schema metadata. "+
				"Token-aware routing will fall back to the configured fallback policy. "+
				"Attempts to retrieve keyspace metadata will fail with ErrKeyspaceDoesNotExist until schema refresh succeeds.",
				NewLogFieldError("err", err))
		}
	}

	s.sessionStateMu.Lock()
	s.isInitialized = true
	s.sessionStateMu.Unlock()

	s.sessionReadyListeners.OnSessionReady(s)

	s.logger.Info("Session initialized successfully.")
	return nil
}

// AwaitSchemaAgreement will wait until schema versions across all nodes in the
// cluster are the same (as seen from the point of view of the control connection).
// The maximum amount of time this takes is governed
// by the MaxWaitSchemaAgreement setting in the configuration (default: 60s).
// AwaitSchemaAgreement returns an error in case schema versions are not the same
// after the timeout specified in MaxWaitSchemaAgreement elapses.
func (s *Session) AwaitSchemaAgreement(ctx context.Context) error {
	if s.cfg.disableControlConn {
		return errNoControl
	}
	return s.control.withConn(func(conn *Conn) *Iter {
		return newErrIter(conn.awaitSchemaAgreement(ctx), &queryMetrics{}, "", nil, nil)
	}).err
}

// ringFullRefreshInterval is how often the scheduler asks for a full ring
// refresh while nothing else prompts one.
//
// It is deliberately independent of ReconnectInterval: the periodic refresh is
// a safety net against lost topology events, not a reconnection setting, so a
// long reconnect interval must not stretch it.
const ringFullRefreshInterval = 5 * time.Minute

// ringRefreshRetryDelay is how long the ring-refresh phase waits before
// retrying after it failed to deliver its request. It is the delay behind
// hostScheduler.refreshRetryFloor.
//
// The floor is phase scoped: it suppresses only the refresh phase's own
// eligibility to wake the scheduler, so a reconnect that comes due inside the
// floor is still served on time.
const ringRefreshRetryDelay = time.Second

// schedulerClock supplies the host scheduler with its notion of time.
//
// Tests replace it to drive deadlines without waiting on a wall clock, and to
// observe the delay the scheduler picks for each round - which is the
// scheduling decision under test, not the moment a dial is observed.
type schedulerClock interface {
	// now returns the current time.
	now() time.Time
	// newTimer returns a channel that receives once after d elapses, and a
	// function that releases the timer's resources.
	newTimer(d time.Duration) (<-chan time.Time, func())
}

// realSchedulerClock is the wall clock the session uses in production.
type realSchedulerClock struct{}

var _ schedulerClock = realSchedulerClock{}

// now returns the current wall-clock time.
//
// Returns:
//   - time.Time: time.Now()
func (realSchedulerClock) now() time.Time { return time.Now() }

// newTimer arms a one-shot timer.
//
// Parameters:
//   - d: the delay before the channel receives
//
// Returns:
//   - <-chan time.Time: fires once after d
//   - func(): stops the timer
func (realSchedulerClock) newTimer(d time.Duration) (<-chan time.Time, func()) {
	timer := time.NewTimer(d)
	return timer.C, func() { timer.Stop() }
}

// hostScheduler services the session's periodic host work from a single
// deadline-driven loop.
//
// A ticker cannot express what this loop needs: both the reconnect sweep and
// the periodic ring refresh have to decide for themselves when the next wake-up
// is due, and a ticker's next firing is fixed the moment it is created. The
// loop instead sleeps until the earliest armed deadline and serves whichever
// phases that wake-up made due.
//
// Every field is worker local. Nothing else reads or writes them, so the loop
// needs no lock of its own.
//
// The two phases are independent by construction (I3): each has its own
// deadline, its own recovery boundary, and its own rule for advancing after a
// failure. A panic out of one phase must never be able to stop or delay the
// other, because the ring refresh is the session's safety net and the reconnect
// sweep is the phase that calls application code.
type hostScheduler struct {
	// session owns the ring, the pool and the refresh debouncer this loop drives.
	session *Session
	// clock is the scheduler's source of time and timers.
	clock schedulerClock

	// reconnectInterval is ClusterConfig.ReconnectInterval, the rhythm of the
	// reconnect phase.
	reconnectInterval time.Duration

	// reconnectDeadline is when the reconnect phase is next due. The zero value
	// means the phase has no deadline and takes no part in choosing the next
	// wake-up.
	reconnectDeadline time.Time

	// fullRefreshDeadline is when the periodic ring refresh is next due. The
	// zero value means the phase has no deadline, which the session never
	// produces: the refresh is armed at construction and re-armed every time it
	// is served.
	fullRefreshDeadline time.Time

	// refreshRetryFloor holds back the refresh phase after a delivery failure.
	// It is the zero value whenever the last attempt was delivered.
	refreshRetryFloor time.Time
}

// newHostScheduler builds the scheduler the reconnect goroutine runs.
//
// Parameters:
//   - intv: the reconnect interval; must be positive
//
// Returns:
//   - *hostScheduler: a scheduler with the periodic ring refresh armed one
//     period out, and the reconnect phase armed only when intv is positive
func (s *Session) newHostScheduler(intv time.Duration) *hostScheduler {
	w := &hostScheduler{
		session:           s,
		clock:             realSchedulerClock{},
		reconnectInterval: intv,
	}
	w.fullRefreshDeadline = w.clock.now().Add(ringFullRefreshInterval)
	return w
}

// reconnectEnabled reports whether this session schedules retries for DOWN hosts
// it already knows about.
//
// A zero ReconnectInterval does not mean "retry immediately": it means the phase
// does not exist. It arms no deadline, is never advanced, and takes no part in
// choosing the next wake-up. Producing a deadline and then declining to act on
// it would leave a deadline that is due and never served, which is a wake-up
// with no delay - the scheduler would spin, calling the application's HostFilter
// as fast as it could.
//
// Returns:
//   - bool: true when the reconnect phase exists
func (w *hostScheduler) reconnectEnabled() bool {
	return w.reconnectInterval > 0
}

// reconnectDownedHosts runs the session's host scheduler until the session's
// context is cancelled.
//
// Parameters:
//   - intv: the reconnect interval
func (s *Session) reconnectDownedHosts(intv time.Duration) {
	defer recoverGoroutine(s.logger, "Session.reconnectDownedHosts", nil)

	s.newHostScheduler(intv).run()
}

// run arms the reconnect phase and serves rounds until the session's context is
// cancelled.
func (w *hostScheduler) run() {
	if w.session.cfg.testSchedulerStarted != nil {
		w.session.cfg.testSchedulerStarted(w.reconnectInterval)
	}
	if w.reconnectEnabled() {
		w.reconnectDeadline = w.clock.now().Add(w.reconnectInterval)
	}

	for {
		wait, armed := w.nextWait(w.clock.now())

		// A nil channel blocks forever, which is what an unarmed scheduler
		// should do: wait for cancellation and nothing else.
		var fired <-chan time.Time
		stop := func() {}
		if armed {
			fired, stop = w.clock.newTimer(wait)
		}

		select {
		case <-w.session.ctx.Done():
			stop()
			return
		case <-fired:
			stop()
		}

		w.serve()
	}
}

// nextWait reports how long to sleep before the next round.
//
// A phase whose deadline is the zero value takes no part in the choice. An
// already-passed deadline yields zero, not a negative delay.
//
// Parameters:
//   - now: the round's reference time
//
// Returns:
//   - time.Duration: the delay until the earliest armed deadline
//   - bool: false when no phase is armed, in which case the delay is meaningless
func (w *hostScheduler) nextWait(now time.Time) (time.Duration, bool) {
	var earliest time.Time
	for _, deadline := range []time.Time{w.reconnectDeadline, w.refreshWakeAt()} {
		if deadline.IsZero() {
			continue
		}
		if earliest.IsZero() || deadline.Before(earliest) {
			earliest = deadline
		}
	}
	if earliest.IsZero() {
		return 0, false
	}
	return max(earliest.Sub(now), 0), true
}

// refreshWakeAt returns the time the ring-refresh phase may next wake the
// scheduler, or the zero value when the phase is not armed.
//
// A pending retry floor holds the phase back past its own deadline; the floor
// is deliberately not applied to the reconnect phase.
//
// Returns:
//   - time.Time: the phase's effective wake-up time
func (w *hostScheduler) refreshWakeAt() time.Time {
	if w.fullRefreshDeadline.IsZero() {
		return time.Time{}
	}
	if w.refreshRetryFloor.After(w.fullRefreshDeadline) {
		return w.refreshRetryFloor
	}
	return w.fullRefreshDeadline
}

// serve runs one round: every phase this wake-up made due, each inside its own
// recovery boundary.
//
// The ring refresh goes first because delivering its request neither dials nor
// calls application code, while the reconnect sweep does both. Ordering it
// after the sweep would make the safety net's liveness depend on the phase most
// likely to fail.
func (w *hostScheduler) serve() {
	now := w.clock.now()
	w.serveRingRefresh(now)
	w.serveReconnect(now)
}

// serveRingRefresh requests a full ring refresh when the periodic deadline is
// due.
//
// A request that is not delivered leaves the deadline where it is - the phase
// has not been served - and takes a retry floor instead, so repeated failures
// retry on a rhythm rather than spinning the loop.
//
// Parameters:
//   - now: the round's reference time
func (w *hostScheduler) serveRingRefresh(now time.Time) {
	wakeAt := w.refreshWakeAt()
	if wakeAt.IsZero() || now.Before(wakeAt) {
		return
	}

	delivered := false
	defer func() {
		if delivered {
			w.fullRefreshDeadline = w.clock.now().Add(ringFullRefreshInterval)
			w.refreshRetryFloor = time.Time{}
			return
		}
		w.refreshRetryFloor = w.clock.now().Add(ringRefreshRetryDelay)
	}()
	defer recoverGoroutine(w.session.logger, "Session.hostScheduler.ringRefresh", nil)

	w.session.ringRefresher.trigger()
	delivered = true
}

// serveReconnect runs one reconnect sweep when its deadline is due.
//
// The round consumes an interval whether the sweep returned or panicked, so a
// callback that panics on every sweep is retried on the configured rhythm
// instead of immediately.
//
// Parameters:
//   - now: the round's reference time
func (w *hostScheduler) serveReconnect(now time.Time) {
	if !w.reconnectEnabled() || w.reconnectDeadline.IsZero() || now.Before(w.reconnectDeadline) {
		return
	}

	defer func() {
		w.reconnectDeadline = w.clock.now().Add(w.reconnectInterval)
	}()
	defer recoverGoroutine(w.session.logger, "Session.hostScheduler.reconnect", nil)

	w.session.reconnectDownedHostsOnce()
}

// reconnectDownedHostsOnce is one sweep of the host scheduler's reconnect phase.
//
// While at least one unfiltered ring host is DOWN it first requests a ring
// refresh, then starts a pool fill for every host that is not UP.
//
// The refresh is what finds a host that came back at a different address
// when the server events that would announce it are lost (#1884):
// the ring is otherwise refreshed only from a topology event,
// from a status event naming an unknown address,
// and from a control-connection reconnect,
// so with events off and the control host unaffected
// the fill below would dial the stale address forever.
// refreshRing sees the same host_id at the new address and rebuilds the entry.
// The request is issued before the fills because a fill dials its first
// connection synchronously, with the reconnection policy's retries and waits,
// and must not delay the discovery behind every unreachable host.
// It goes through the debouncer's immediate trigger,
// so it is bounded to one request per tick, coalesces with a running refresh,
// and never waits; on a healthy ring nothing is requested at all.
func (s *Session) reconnectDownedHostsOnce() {
	s.logger.Debug("Connecting to downed hosts if there is any.")
	hosts := s.ring.allHosts()

	// Print session.ring for debug.
	s.logger.Debug("Logging current ring state.", NewLogFieldString("ring", ringString(hosts)))

	if s.hasUnfilteredDownHost(hosts) {
		s.ringRefresher.trigger()
	}

	for _, h := range hosts {
		if h.IsUp() {
			continue
		}
		if s.cfg.filterHost(h) {
			// A filtered host can sit DOWN in the ring - the control connection adds
			// its host before filtering, and a DOWN sets the state before the filter is
			// consulted - but the pool never admits it, so every dial here is wasted
			// work against a node the application excluded on purpose.
			continue
		}
		s.logger.Debug("Reconnecting to downed host.",
			NewLogFieldIP("host_addr", h.ConnectAddress()),
			NewLogFieldInt("host_port", h.Port()),
			NewLogFieldString("host_id", h.HostID()))
		// we let the pool call handleNodeConnected to change the host state
		s.pool.addHost(h)
	}
}

// hasUnfilteredDownHost reports whether any host the HostFilter accepts is not UP.
//
// A filtered host can sit DOWN in the ring - the control connection adds its
// host before filtering, and a DOWN sets state before checking the filter -
// but is never pooled, so it must not keep a ring refresh going on an
// otherwise healthy ring.
//
// Parameters:
//   - hosts: a ring snapshot
//
// Returns:
//   - bool: true when an accepted host is DOWN
func (s *Session) hasUnfilteredDownHost(hosts []*HostInfo) bool {
	for _, h := range hosts {
		if !h.IsUp() && !s.cfg.filterHost(h) {
			return true
		}
	}
	return false
}

// Query generates a new query object for interacting with the database.
// Further details of the query may be tweaked using the resulting query
// value before the query is executed. Query is automatically prepared
// if it has not previously been executed.
//
// Supported Go to CQL type conversions for query parameters are as follows:
//
//	Go type (value)             | CQL type                    | Note
//	string, []byte              | varchar, ascii, blob, text  |
//	bool                        | boolean                     |
//	integer types               | tinyint, smallint, int      |
//	string                      | tinyint, smallint, int      | formatted as base 10 number
//	integer types               | bigint, counter             |
//	big.Int                     | bigint, counter             | according to cassandra bigint specification the big.Int value limited to int64 size(an eight-byte two's complement integer.)
//	string                      | bigint, counter             | formatted as base 10 number
//	float32                     | float                       |
//	float64                     | double                      |
//	inf.Dec                     | decimal                     |
//	int64                       | time                        | nanoseconds since start of day
//	time.Duration               | time                        | duration since start of day
//	int64                       | timestamp                   | milliseconds since Unix epoch
//	time.Time                   | timestamp                   |
//	slice, array                | list, set                   |
//	map[X]struct{}              | list, set                   |
//	map[X]Y                     | map                         |
//	gocql.UUID                  | uuid, timeuuid              |
//	[16]byte                    | uuid, timeuuid              | raw UUID bytes
//	[]byte                      | uuid, timeuuid              | raw UUID bytes, length must be 16 bytes
//	string                      | uuid, timeuuid              | hex representation, see ParseUUID
//	integer types               | varint                      |
//	big.Int                     | varint                      |
//	string                      | varint                      | value of number in decimal notation
//	net.IP                      | inet                        |
//	string                      | inet                        | IPv4 or IPv6 address string
//	slice, array                | tuple                       |
//	struct                      | tuple                       | fields are marshaled in order of declaration
//	gocql.UDTMarshaler          | user-defined type           | MarshalUDT is called
//	map[string]interface{}      | user-defined type           |
//	struct                      | user-defined type           | struct fields' cql tags are used for column names
//	int64                       | date                        | milliseconds since Unix epoch to start of day (in UTC)
//	time.Time                   | date                        | start of day (in UTC)
//	string                      | date                        | parsed using "2006-01-02" format
//	int64                       | duration                    | duration in nanoseconds
//	time.Duration               | duration                    |
//	gocql.Duration              | duration                    |
//	string                      | duration                    | parsed with time.ParseDuration
func (s *Session) Query(stmt string, values ...interface{}) *Query {
	qry := &Query{}
	qry.session = s
	qry.stmt = stmt
	qry.values = values
	qry.hostID = ""
	qry.defaultsFromSession()
	return qry
}

// QueryInfo represents metadata information about a prepared query.
// It contains the query ID, argument information, result information, and primary key columns.
type QueryInfo struct {
	Id          []byte
	Args        []ColumnInfo
	Rval        []ColumnInfo
	PKeyColumns []int
}

// Bind generates a new query object based on the query statement passed in.
// The query is automatically prepared if it has not previously been executed.
// The binding callback allows the application to define which query argument
// values will be marshalled as part of the query execution.
// During execution, the meta data of the prepared query will be routed to the
// binding callback, which is responsible for producing the query argument values.
//
// For supported Go to CQL type conversions for query parameters, see Session.Query documentation.
func (s *Session) Bind(stmt string, b func(q *QueryInfo) ([]interface{}, error)) *Query {
	qry := &Query{}
	qry.session = s
	qry.stmt = stmt
	qry.binding = b
	qry.defaultsFromSession()
	return qry
}

// Close closes all connections. The session is unusable after this
// operation.
func (s *Session) Close() {
	s.sessionStateMu.Lock()
	if s.isClosing {
		s.sessionStateMu.Unlock()
		return
	}
	s.isClosing = true
	s.sessionStateMu.Unlock()

	// Reject host publications not yet admitted, before anything else in Close
	// so a late pool fill cannot re-publish a host as it tears down.
	s.hostPublishClosed.Store(true)

	// Latch the control connection closing and snapshot what it must close, then
	// cancel the session context, then do the physical closes. The order matters:
	// latchAndSnapshot only latches and reads memory, so it cannot block; the
	// cancel unwinds any cooperative in-flight work (dials, TLS, startup, schema
	// agreement waits) before the refreshers are joined below, so a reconnect
	// running on a flusher's goroutine cannot hold the join forever; and the
	// physical closes run after the cancel because Conn.Close can reach the
	// application logger synchronously.
	var controlSnap controlSnapshot
	if s.control != nil {
		controlSnap = s.control.latchAndSnapshot()
	}

	if s.cancel != nil {
		s.cancel()
	}

	if s.control != nil {
		s.control.closeSnapshot(controlSnap)
	}

	if s.pool != nil {
		s.pool.Close()
	}

	if s.schemaDescriber != nil {
		s.schemaDescriber.schemaRefresher.stop()
	}

	if s.nodeEvents != nil {
		s.nodeEvents.stop()
	}

	if s.ringRefresher != nil {
		s.ringRefresher.stop()
	}

	s.sessionStateMu.Lock()
	s.isClosed = true
	s.sessionStateMu.Unlock()
}

func (s *Session) Closed() bool {
	s.sessionStateMu.RLock()
	closed := s.isClosed
	s.sessionStateMu.RUnlock()
	return closed
}

func (s *Session) initialized() bool {
	s.sessionStateMu.RLock()
	initialized := s.isInitialized
	s.sessionStateMu.RUnlock()
	return initialized
}

func (s *Session) executeQuery(qry *internalQuery) (it *Iter) {
	// fail fast
	if s.Closed() {
		return newErrIter(ErrSessionClosed, qry.metrics, qry.Keyspace(), qry.getRoutingInfo(), qry.getKeyspaceFunc())
	}

	iter, err := s.executor.executeQuery(qry)
	if err != nil {
		return newErrIter(err, qry.metrics, qry.Keyspace(), qry.getRoutingInfo(), qry.getKeyspaceFunc())
	}
	if iter == nil {
		panic("nil iter")
	}

	return iter
}

// outageState is the session's outage ledger.
//
// It answers exactly one question - has a new outage begun - and deliberately
// answers neither "which hosts should be reconnected" nor "how long to wait
// before trying". Both of those belong to the scheduler, which recomputes
// membership every round and keeps its backoff as goroutine-local state.
//
// Every field is written only by the three producers that run inside a
// hostPublishMu transaction: markHostDown, handleNodeConnected and removeHost.
// Those are the whole set. A host can only enter DOWN through markHostDown, and
// can only leave it by coming up or by leaving the ring, because HostInfo.state
// is written nowhere else. The scheduler reads gen and startedAt and receives
// from nudge; it never writes here. A ledger the scheduler could correct would
// lose whatever a producer recorded while it was sampling.
//
// Membership is a set of host IDs rather than a count because a repeated DOWN
// report for a host already down must not open a second outage, and a set is
// idempotent where a count is not. The empty state is len(downSet) == 0.
//
// Lock order is hostPublishMu then mu, never the reverse. Nothing but a map
// operation, a comparison and one non-blocking send may run under mu: no
// application callback, no dial, no logging, no ring lock.
type outageState struct {
	mu sync.Mutex

	// downSet holds the host IDs that are relevant - not excluded by the
	// HostFilter - and not UP.
	downSet map[string]struct{}

	// gen counts outages. It advances only when downSet goes from empty to
	// non-empty, so a host that fails while another is already down joins the
	// outage under way instead of starting a new one.
	gen uint64

	// startedAt is when the current outage began. It is the base the scheduler
	// arms the outage's first retry from, so that deadline is absolute and does
	// not slide with when the scheduler happens to wake. It is meaningless
	// while downSet is empty.
	startedAt time.Time

	// nudge carries at most one pending "an outage has begun" hint.
	//
	// It is a hint and nothing more. The channel has room for one item and
	// keeps no history, so a receive is not evidence of an unseen generation:
	// the notification can still be sitting in the channel after its generation
	// has already been read. A reader must re-read gen every time it wakes.
	nudge chan struct{}
}

// outageAdd records that the host with the given ID is relevant and not UP.
//
// The caller must hold hostPublishMu.
//
// The first member of an empty ledger opens a new outage: the generation
// advances, the start time is stamped and the scheduler is nudged. A member
// joining an outage already under way changes nothing else, so one host failing
// while another is already down can neither reset a backoff nor push a pending
// retry out. A repeated report for a host already in the set does nothing at
// all.
//
// Parameters:
//   - id: the host ID to record
func (s *Session) outageAdd(id string) {
	s.outage.mu.Lock()
	defer s.outage.mu.Unlock()

	// Idempotence comes from the set, not from a guard: adding an ID that is
	// already there leaves the set non-empty, so opened is false and a repeated
	// DOWN report changes nothing.
	opened := len(s.outage.downSet) == 0
	s.outage.downSet[id] = struct{}{}
	if !opened {
		return
	}

	s.outage.gen++
	s.outage.startedAt = time.Now()
	// Sent here, under outage.mu, so opening an outage and announcing it are
	// indivisible: a producer whose policy callback later blocks or panics can
	// neither delay the wake-up nor lose it. A non-blocking send on a buffered
	// channel cannot block, which is what makes it safe to do under the lock,
	// and it is the only thing besides map work allowed here.
	select {
	case s.outage.nudge <- struct{}{}:
	default:
	}
}

// outageRemove drops the host with the given ID from the ledger.
//
// The caller must hold hostPublishMu.
//
// Emptying the ledger does not advance the generation. An empty ledger means
// there is no outage and so nothing to schedule; the reset that matters happens
// at the next empty-to-non-empty transition instead, which is the same
// observable behaviour and one fewer rule.
//
// Parameters:
//   - id: the host ID to drop
func (s *Session) outageRemove(id string) {
	s.outage.mu.Lock()
	defer s.outage.mu.Unlock()

	delete(s.outage.downSet, id)
}

// removeHost takes h out of the ring, the outage ledger, the selection policy
// and the pool.
//
// The ring removal, the ledger update and the policy un-publication are one
// transaction under hostPublishMu. That is what keeps the ledger honest: were
// the ring removal outside, another host could transition DOWN in the window
// between it and this call's bookkeeping, find h still in the ledger, and so
// join an outage that the real membership had already left. The backoff of a
// finished outage would then be inherited by a new one.
//
// Every path that checks ring ownership under hostPublishMu - withOwnedHost and
// completeAdmission - therefore cannot interleave with a removal: whichever
// runs second sees the other's result. The one admission path that checks
// ownership outside this mutex, policyConnPool.addHost under the pool's own
// lock, stays safe for a different reason that has not changed: pool.removeHost
// compares pointers, so an admission that raced ahead registered h's own pool
// and this call removes exactly that.
//
// What the transaction does not do is stop setupConn from inserting a fresh
// object for the same host ID while it runs - that path takes only the ring's
// lock. What it does stop is that object's DOWN transition being recorded
// before this removal's bookkeeping, because every DOWN must take
// hostPublishMu and so queues behind it.
//
// pool.removeHost does not touch the ledger: taking a pool away is not a
// membership transition, and recording it as one would open outages for hosts
// that never went down.
//
// Parameters:
//   - h: the ring object to remove
func (s *Session) removeHost(h *HostInfo) {
	s.logger.Warning("Removing host.", NewLogFieldIP("host_addr", h.ConnectAddress()), NewLogFieldString("host_id", h.HostID()))
	func() {
		// The unlock is deferred: the policy is application code, and a panic
		// in it is recovered by the calling goroutine's recoverGoroutine, which
		// must not leave every later host transition blocked on this mutex.
		s.hostPublishMu.Lock()
		defer s.hostPublishMu.Unlock()
		if s.ring.removeHost(h.HostID()) {
			s.outageRemove(h.HostID())
		}
		s.unpublishHostLocked(h)
	}()
	s.pool.removeHost(h)
}

// unpublishHostLocked retires h's publication record and removes it from the
// selection policy.
//
// The caller must hold hostPublishMu.
//
// Parameters:
//   - h: the object to remove from the policy
func (s *Session) unpublishHostLocked(h *HostInfo) {
	// Retire the publication record, but only while it still names h. A
	// replacement that took this host ID has its own record under the same key,
	// and dropping that would make a later repair publish it a second time.
	if s.publishedHosts[h.HostID()] == h {
		delete(s.publishedHosts, h.HostID())
	}
	s.policy.RemoveHost(h)
}

// withOwnedHost runs fn under hostPublishMu, but only if the session is not
// shutting down and host is the ring's current object for its host ID at that
// moment.
//
// Every membership or state transition of a host in the selection policy goes
// through here, and removeHost takes the host out of the ring and un-publishes it
// as one transaction under the same mutex, so a transition and a removal of the
// same host cannot interleave: whichever runs second sees the other's result.
//
// The shutdown gate rejects any transition not yet admitted once Close has set
// hostPublishClosed: a pool fill that finishes after Close and reaches here does
// not re-publish its host. An already-admitted callback (past this check, still
// running) is not interrupted. Close does not take hostPublishMu, so it does not
// wait on this path's callback; it makes no claim about callbacks awaited by an
// existing flusher join elsewhere in Close.
//
// Parameters:
//   - host: the object the caller holds
//   - fn: the transition to apply to an owned host; its result is passed through
//
// Returns:
//   - bool: fn's result, or false when host is not owned and fn did not run
func (s *Session) withOwnedHost(host *HostInfo, fn func() bool) bool {
	s.hostPublishMu.Lock()
	defer s.hostPublishMu.Unlock()
	if s.hostPublishClosed.Load() {
		return false
	}
	if !s.ring.owns(host) {
		return false
	}
	return fn()
}

// hostPoolEmpty reports whether host is the current ring object and its
// registered pool holds no connections.
//
// A host without a pool (during Session.init, or after removal) and an object
// the ring no longer owns both return false, so callers never convict a host
// they cannot act on.
//
// Parameters:
//   - host: the ring object to inspect
//
// Returns:
//   - bool: true when ring.owns(host), a pool is registered for this exact
//     object, and that pool's Size() is zero
func (s *Session) hostPoolEmpty(host *HostInfo) bool {
	if !s.ring.owns(host) {
		return false
	}
	pool, ok := s.pool.getPoolFor(host)
	return ok && pool.Size() == 0
}

// KeyspaceMetadata returns the schema metadata for the keyspace specified. Returns an error if the keyspace does not exist.
// If MetadataConfig.CacheMode is Disabled this method will query the system tables,
// otherwise it will retrieve the metadata from the driver's cache.
//
// Check AllKeyspaceMetadata if you're interested in retrieving the metadata for all keyspaces instead.
func (s *Session) KeyspaceMetadata(keyspace string) (*KeyspaceMetadata, error) {
	// fail fast
	if s.Closed() {
		return nil, ErrSessionClosed
	} else if keyspace == "" {
		return nil, ErrNoKeyspace
	}

	return s.schemaDescriber.getSchema(keyspace)
}

// AllKeyspaceMetadata returns the schema metadata for all keyspaces.
// If MetadataConfig.CacheMode is Disabled this method will query the system tables,
// otherwise it will retrieve the metadata from the driver's cache.
//
// Check KeyspaceMetadata if you're interested in retrieving the metadata for a single keyspace by name instead.
func (s *Session) AllKeyspaceMetadata() (map[string]*KeyspaceMetadata, error) {
	// fail fast
	if s.Closed() {
		return nil, ErrSessionClosed
	}

	return s.schemaDescriber.getAllSchema()
}

func (s *Session) getConn() *Conn {
	hosts := s.ring.allHosts()

	for _, host := range hosts {
		if !host.IsUp() {
			continue
		}

		pool, ok := s.pool.getPool(host)
		if !ok {
			continue
		} else if conn := pool.Pick(); conn != nil {
			return conn
		}
	}

	return nil
}

// Returns statement metadata for the purposes of generating a routing key.
// If keyspace == "" it uses the keyspace which is specified in Cluster.Keyspace
func (s *Session) routingStatementMetadata(ctx context.Context, stmt string, keyspace string) (*StatementMetadata, error) {
	if keyspace == "" {
		keyspace = s.cfg.Keyspace
	}

	cacheKey := s.routingMetadataCache.keyFor(keyspace, stmt)

	loader := otter.LoaderFunc[routingCacheKey, *StatementMetadata](
		func(loadCtx context.Context, key routingCacheKey) (*StatementMetadata, error) {
			meta, err := s.StatementMetadata(loadCtx, key.statement, key.keyspace)
			if err != nil {
				return nil, err
			}
			return &meta, nil
		},
	)

	return s.routingMetadataCache.getOrLoad(ctx, cacheKey, loader)
}

// StatementMetadata represents various metadata about a statement.
type StatementMetadata struct {
	// Keyspace is the keyspace of the table for the statement.
	Keyspace string

	// Table is the table of the statement.
	Table string

	// BindColumns are columns bound to the statement.
	BindColumns []ColumnInfo

	// PKBindColumnIndexes are the indexes of the BindColumns that correspond to
	// partition key columns. If this is empty then one or more columns in the
	// partition key were not bound to the statement.
	PKBindColumnIndexes []int

	// ResultColumns are the columns that are returned by the statement.
	ResultColumns []ColumnInfo
}

// StatementMetadata returns metadata for a statement. If keyspace is empty,
// the session's keyspace is used.
func (s *Session) StatementMetadata(ctx context.Context, stmt, keyspace string) (StatementMetadata, error) {
	if keyspace == "" {
		keyspace = s.cfg.Keyspace
	}

	conn := s.getConn()
	if conn == nil {
		return StatementMetadata{}, ErrNoConnections
	}

	// get the query info for the statement
	info, err := conn.prepareStatement(ctx, stmt, nil, keyspace)
	if err != nil {
		// TODO: it would be nice to mark hosts here but as we are not using the policies
		// to fetch hosts we cant and we can't use the policies because they might
		// require token awareness which requires this method
		return StatementMetadata{}, err
	}

	if info.request.keyspace != "" {
		keyspace = info.request.keyspace
	}

	meta := StatementMetadata{
		Keyspace:            keyspace,
		Table:               info.request.table,
		BindColumns:         info.request.columns,
		PKBindColumnIndexes: info.request.pkeyColumns,
		ResultColumns:       info.response.columns,
	}

	// if it is protocol < v4 then we need to calculate the routing key info
	if !info.request.supportsPKeyColumns && len(info.request.columns) > 0 {
		keyspaceMetadata, err := s.KeyspaceMetadata(meta.Keyspace)
		if err != nil {
			// don't cache this error
			return StatementMetadata{}, err
		}

		tableMetadata, found := keyspaceMetadata.Tables[meta.Table]
		if !found {
			// unlikely that the statement could be prepared and the metadata for
			// the table couldn't be found, but this may indicate either a bug
			// in the metadata code, or that the table was just dropped.
			return StatementMetadata{}, ErrNoMetadata
		}

		meta.PKBindColumnIndexes = make([]int, len(tableMetadata.PartitionKey))
		for keyIndex, keyColumn := range tableMetadata.PartitionKey {
			// set an indicator for checking if the mapping is missing
			meta.PKBindColumnIndexes[keyIndex] = -1

			// find the column in the query info
			for colIndex, boundColumn := range info.request.columns {
				if keyColumn.Name == boundColumn.Name {
					// there may be many such bound columns, pick the first
					meta.PKBindColumnIndexes[keyIndex] = colIndex
					break
				}
			}

			if meta.PKBindColumnIndexes[keyIndex] == -1 {
				// the partition key column is not bound to the statement
				meta.PKBindColumnIndexes = nil
				break
			}
		}
	}
	return meta, nil
}

// Exec executes a batch operation and returns nil if successful
// otherwise an error is returned describing the failure.
func (b *Batch) Exec() error {
	iter := b.session.executeBatch(b, b.context)
	return iter.Close()
}

// ExecContext executes a batch operation with the provided context and returns nil if successful
// otherwise an error is returned describing the failure.
func (b *Batch) ExecContext(ctx context.Context) error {
	iter := b.session.executeBatch(b, ctx)
	return iter.Close()
}

// Iter executes a batch operation and returns an Iter object
// that can be used to access properties related to the execution like Iter.Attempts and Iter.Latency
func (b *Batch) Iter() *Iter { return b.IterContext(b.context) }

// IterContext executes a batch operation with the provided context and returns an Iter object
// that can be used to access properties related to the execution like Iter.Attempts and Iter.Latency
func (b *Batch) IterContext(ctx context.Context) *Iter {
	iter := b.session.executeBatch(b, ctx)
	iter.attachLeakDetector(b.session.logger)
	return iter
}

func (s *Session) executeBatch(batch *Batch, ctx context.Context) *Iter {
	b := newInternalBatch(batch, ctx)
	// fail fast
	if s.Closed() {
		return newErrIter(ErrSessionClosed, b.metrics, b.Keyspace(), b.getRoutingInfo(), b.getKeyspaceFunc())
	}

	// Prevent the execution of the batch if greater than the limit
	// Currently batches have a limit of 65536 queries.
	// https://datastax-oss.atlassian.net/browse/JAVA-229
	if batch.Size() > BatchSizeMaximum {
		return newErrIter(ErrTooManyStmts, b.metrics, b.Keyspace(), b.getRoutingInfo(), b.getKeyspaceFunc())
	}

	iter, err := s.executor.executeQuery(b)
	if err != nil {
		return newErrIter(err, b.metrics, b.Keyspace(), b.getRoutingInfo(), b.getKeyspaceFunc())
	}

	return iter
}

// Deprecated: use Batch.Exec instead.
// ExecuteBatch executes a batch operation and returns nil if successful
// otherwise an error is returned describing the failure.
func (s *Session) ExecuteBatch(batch *Batch) error {
	iter := s.executeBatch(batch, batch.context)
	return iter.Close()
}

// Deprecated: use Batch.ExecCAS instead
// ExecuteBatchCAS executes a batch operation and returns true if successful and
// an iterator (to scan additional rows if more than one conditional statement)
// was sent.
// Further scans on the interator must also remember to include
// the applied boolean as the first argument to *Iter.Scan
func (s *Session) ExecuteBatchCAS(batch *Batch, dest ...interface{}) (applied bool, iter *Iter, err error) {
	return batch.ExecCAS(dest...)
}

// ExecCAS executes a batch operation and returns true if successful and
// an iterator (to scan additional rows if more than one conditional statement)
// was sent.
// Further scans on the interator must also remember to include
// the applied boolean as the first argument to *Iter.Scan
func (b *Batch) ExecCAS(dest ...interface{}) (applied bool, iter *Iter, err error) {
	return b.ExecCASContext(b.context, dest...)
}

// ExecCASContext executes a batch operation with the provided context and returns true if successful and
// an iterator (to scan additional rows if more than one conditional statement)
// was sent.
// Further scans on the interator must also remember to include
// the applied boolean as the first argument to *Iter.Scan
func (b *Batch) ExecCASContext(ctx context.Context, dest ...interface{}) (applied bool, iter *Iter, err error) {
	iter = b.session.executeBatch(b, ctx)
	if err := iter.checkErrAndNotFound(); err != nil {
		iter.Close()
		return false, nil, err
	}

	if len(iter.Columns()) > 1 {
		dest = append([]interface{}{&applied}, dest...)
		iter.Scan(dest...)
	} else {
		iter.Scan(&applied)
	}

	return applied, iter, iter.err
}

// Deprecated: use Batch.MapExecCAS instead
// MapExecuteBatchCAS executes a batch operation much like ExecuteBatchCAS,
// however it accepts a map rather than a list of arguments for the initial
// scan.
func (s *Session) MapExecuteBatchCAS(batch *Batch, dest map[string]interface{}) (applied bool, iter *Iter, err error) {
	return batch.MapExecCAS(dest)
}

// MapExecCAS executes a batch operation much like ExecuteBatchCAS,
// however it accepts a map rather than a list of arguments for the initial
// scan.
func (b *Batch) MapExecCAS(dest map[string]interface{}) (applied bool, iter *Iter, err error) {
	return b.MapExecCASContext(b.context, dest)
}

// MapExecCASContext executes a batch operation with the provided context much like ExecuteBatchCAS,
// however it accepts a map rather than a list of arguments for the initial
// scan.
func (b *Batch) MapExecCASContext(ctx context.Context, dest map[string]interface{}) (applied bool, iter *Iter, err error) {
	iter = b.session.executeBatch(b, ctx)
	if err := iter.checkErrAndNotFound(); err != nil {
		iter.Close()
		return false, nil, err
	}
	iter.MapScan(dest)
	if iter.err != nil {
		return false, iter, iter.err
	}
	// check if [applied] was returned, otherwise it might not be CAS
	if _, ok := dest["[applied]"]; ok {
		applied = dest["[applied]"].(bool)
		delete(dest, "[applied]")
	}

	// we usually close here, but instead of closing, just returin an error
	// if MapScan failed. Although Close just returns err, using Close
	// here might be confusing as we are not actually closing the iter
	return applied, iter, iter.err
}

type hostMetrics struct {
	// Attempts is count of how many times this query has been attempted for this host.
	// An attempt is either a retry or fetching next page of results.
	Attempts int

	// TotalLatency is the sum of attempt latencies for this host in nanoseconds.
	TotalLatency int64
}

type queryMetrics struct {
	totalAttempts int64
	totalLatency  int64
}

func (qm *queryMetrics) attempt(addLatency time.Duration) int {
	atomic.AddInt64(&qm.totalLatency, addLatency.Nanoseconds())
	return int(atomic.AddInt64(&qm.totalAttempts, 1) - 1)
}

func (qm *queryMetrics) attempts() int {
	return int(atomic.LoadInt64(&qm.totalAttempts))
}

func (qm *queryMetrics) latency() int64 {
	attempts := atomic.LoadInt64(&qm.totalAttempts)
	if attempts == 0 {
		return atomic.LoadInt64(&qm.totalLatency)
	}
	return atomic.LoadInt64(&qm.totalLatency) / attempts
}

type hostMetricsManager interface {
	attempt(addLatency time.Duration, host *HostInfo) *hostMetrics
}

type hostMetricsManagerImpl struct {
	l sync.RWMutex
	m map[string]*hostMetrics
}

func newHostMetricsManager() *hostMetricsManagerImpl {
	return &hostMetricsManagerImpl{m: make(map[string]*hostMetrics)}
}

// preFilledHostMetricsMetricsManager initializes new hostMetrics based on per-host supplied data.
func preFilledHostMetricsMetricsManager(m map[string]*hostMetrics) *hostMetricsManagerImpl {
	return &hostMetricsManagerImpl{m: m}
}

// hostMetricsLocked gets or creates host metrics for given host.
// It must be called only while holding qm.l lock.
func (qm *hostMetricsManagerImpl) hostMetricsLocked(host *HostInfo) *hostMetrics {
	metrics, exists := qm.m[host.ConnectAddress().String()]
	if !exists {
		// if the host is not in the map, it means it's been accessed for the first time
		metrics = &hostMetrics{}
		qm.m[host.ConnectAddress().String()] = metrics
	}

	return metrics
}

func (qm *hostMetricsManagerImpl) attempt(addLatency time.Duration, host *HostInfo) *hostMetrics {
	qm.l.Lock()
	updateHostMetrics := qm.hostMetricsLocked(host)
	updateHostMetrics.Attempts += 1
	updateHostMetrics.TotalLatency += addLatency.Nanoseconds()
	qm.l.Unlock()
	return updateHostMetrics
}

var emptyHostMetricsManager = &emptyHostMetricsManagerImpl{}

type emptyHostMetricsManagerImpl struct{}

func (qm *emptyHostMetricsManagerImpl) attempt(_ time.Duration, _ *HostInfo) *hostMetrics {
	return nil
}

// Query represents a CQL statement that can be executed.
type Query struct {
	stmt                  string
	values                []interface{}
	initialConsistency    Consistency
	pageSize              int
	routingKey            []byte
	initialPageState      []byte
	prefetch              float64
	trace                 Tracer
	observer              QueryObserver
	session               *Session
	rt                    RetryPolicy
	spec                  SpeculativeExecutionPolicy
	binding               func(q *QueryInfo) ([]interface{}, error)
	serialCons            Consistency
	defaultTimestamp      bool
	defaultTimestampValue int64
	disableSkipMetadata   bool
	context               context.Context
	idempotent            bool
	customPayload         map[string][]byte

	disableAutoPage bool

	// getKeyspace is field so that it can be overriden in tests
	getKeyspace func() string

	// used by control conn queries to prevent triggering a write to systems
	// tables in AWS MCS see
	skipPrepare bool

	// hostID specifies the host on which the query should be executed.
	// If it is empty, then the host is picked by HostSelectionPolicy
	hostID string

	keyspace          string
	nowInSecondsValue *int
}

// queryRoutingInfo holds the keyspace and table associated with a query
// execution. Both fields are populated as the executor learns them — from
// the routing-metadata cache during host selection and again from the
// prepared-statement metadata after prepare. Speculative execution can
// drive these writes concurrently, but every writer writes the same
// canonical values, so the race is benign.
//
// The fields are exposed through an atomic.Pointer so reads are
// lock-free; this matters because Iter.Keyspace / Iter.Table and the
// observer paths read from here on every query attempt and (in some
// integrations) every row.
type queryRoutingInfo struct {
	v atomic.Pointer[routingKsTable]
}

type routingKsTable struct {
	keyspace string
	table    string
}

func (qr *queryRoutingInfo) getKeyspace() string {
	if p := qr.v.Load(); p != nil {
		return p.keyspace
	}
	return ""
}

func (qr *queryRoutingInfo) getTable() string {
	if p := qr.v.Load(); p != nil {
		return p.table
	}
	return ""
}

func (qr *queryRoutingInfo) set(keyspace, table string) {
	qr.v.Store(&routingKsTable{keyspace: keyspace, table: table})
}

func (q *Query) defaultsFromSession() {
	s := q.session

	q.initialConsistency = s.cons
	q.pageSize = s.pageSize
	q.trace = s.trace
	q.observer = s.queryObserver
	q.prefetch = s.prefetch
	q.rt = s.cfg.RetryPolicy
	q.serialCons = s.cfg.SerialConsistency
	q.defaultTimestamp = s.cfg.DefaultTimestamp
	q.idempotent = s.cfg.DefaultIdempotence

	q.spec = &NonSpeculativeExecution{}
}

// Statement returns the statement that was used to generate this query.
func (q Query) Statement() string {
	return q.stmt
}

// Values returns the values passed in via Bind.
// This can be used by a wrapper type that needs to access the bound values.
func (q Query) Values() []interface{} {
	return q.values
}

// String implements the stringer interface.
func (q Query) String() string {
	return fmt.Sprintf("[query statement=%q values=%+v consistency=%s]", q.stmt, q.values, q.initialConsistency)
}

// Consistency sets the consistency level for this query. If no consistency
// level have been set, the default consistency level of the cluster
// is used.
func (q *Query) Consistency(c Consistency) *Query {
	q.initialConsistency = c
	return q
}

// GetConsistency returns the currently configured consistency level for
// the query.
func (q *Query) GetConsistency() Consistency {
	return q.initialConsistency
}

// Deprecated: use Query.Consistency instead
func (q *Query) SetConsistency(c Consistency) {
	q.initialConsistency = c
}

// CustomPayload sets the custom payload level for this query. The map is not copied internally
// so it shouldn't be modified after the query is scheduled for execution.
func (q *Query) CustomPayload(customPayload map[string][]byte) *Query {
	q.customPayload = customPayload
	return q
}

// Deprecated: Context retrieval is deprecated. Pass context directly to execution methods
// like ExecContext or IterContext instead.
func (q *Query) Context() context.Context {
	if q.context == nil {
		return context.Background()
	}
	return q.context
}

// Trace enables tracing of this query. Look at the documentation of the
// Tracer interface to learn more about tracing.
func (q *Query) Trace(trace Tracer) *Query {
	q.trace = trace
	return q
}

// Observer enables query-level observer on this query.
// The provided observer will be called every time this query is executed.
func (q *Query) Observer(observer QueryObserver) *Query {
	q.observer = observer
	return q
}

// PageSize will tell the iterator to fetch the result in pages of size n.
// This is useful for iterating over large result sets, but setting the
// page size too low might decrease the performance. This feature is only
// available in Cassandra 2 and onwards.
func (q *Query) PageSize(n int) *Query {
	q.pageSize = n
	return q
}

// DefaultTimestamp will enable the with default timestamp flag on the query.
// If enable, this will replace the server side assigned
// timestamp as default timestamp. Note that a timestamp in the query itself
// will still override this timestamp. This is entirely optional.
//
// Only available on protocol >= 3
func (q *Query) DefaultTimestamp(enable bool) *Query {
	q.defaultTimestamp = enable
	return q
}

// WithTimestamp will enable the with default timestamp flag on the query
// like DefaultTimestamp does. But also allows to define value for timestamp.
// It works the same way as USING TIMESTAMP in the query itself, but
// should not break prepared query optimization.
//
// Only available on protocol >= 3
func (q *Query) WithTimestamp(timestamp int64) *Query {
	q.DefaultTimestamp(true)
	q.defaultTimestampValue = timestamp
	return q
}

// RoutingKey sets the routing key to use when a token aware connection
// pool is used to optimize the routing of this query.
func (q *Query) RoutingKey(routingKey []byte) *Query {
	q.routingKey = routingKey
	return q
}

// Deprecated: Use Query.ExecContext or Query.IterContext instead. This will be removed in a future major version.
//
// WithContext returns a shallow copy of q with its context
// set to ctx.
//
// The provided context controls the entire lifetime of executing a
// query, queries will be canceled and return once the context is
// canceled.
//
// If ctx carries a [context.Context.Deadline], that deadline overrides the
// connection-level [ClusterConfig.Timeout] for this query — the query is
// not capped at the connection timeout. Use this to grant individual
// queries (TRUNCATE, schema operations, large batch reads) more time
// than the default while keeping the cluster timeout low for normal
// reads:
//
//	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//	err := session.Query("TRUNCATE TABLE big_table").WithContext(ctx).Exec()
func (q *Query) WithContext(ctx context.Context) *Query {
	q2 := *q
	q2.context = ctx
	return &q2
}

// Keyspace returns the keyspace the query will be executed against.
func (q *Query) Keyspace() string {
	if q.getKeyspace != nil {
		return q.getKeyspace()
	}
	if q.keyspace != "" {
		return q.keyspace
	}

	if q.session == nil {
		return ""
	}
	// TODO(chbannis): this should be parsed from the query or we should let
	// this be set by users.
	return q.session.cfg.Keyspace
}

func (q *Query) shouldPrepare() bool {
	return shouldPrepare(q.stmt)
}

func shouldPrepare(s string) bool {
	stmt := strings.TrimLeftFunc(strings.TrimRightFunc(s, func(r rune) bool {
		return unicode.IsSpace(r) || r == ';'
	}), unicode.IsSpace)

	var stmtType string
	if n := strings.IndexFunc(stmt, unicode.IsSpace); n >= 0 {
		stmtType = strings.ToLower(stmt[:n])
	}
	if stmtType == "begin" {
		if n := strings.LastIndexFunc(stmt, unicode.IsSpace); n >= 0 {
			stmtType = strings.ToLower(stmt[n+1:])
		}
	}
	switch stmtType {
	case "select", "insert", "update", "delete", "batch":
		return true
	}
	return false
}

// SetPrefetch sets the default threshold for pre-fetching new pages. If
// there are only p*pageSize rows remaining, the next page will be requested
// automatically.
func (q *Query) Prefetch(p float64) *Query {
	q.prefetch = p
	return q
}

// RetryPolicy sets the policy to use when retrying the query.
func (q *Query) RetryPolicy(r RetryPolicy) *Query {
	q.rt = r
	return q
}

// SetSpeculativeExecutionPolicy sets the execution policy
func (q *Query) SetSpeculativeExecutionPolicy(sp SpeculativeExecutionPolicy) *Query {
	q.spec = sp
	return q
}

// IsIdempotent returns whether the query is marked as idempotent.
// Non-idempotent query won't be retried.
// See "Retries and speculative execution" in package docs for more details.
func (q *Query) IsIdempotent() bool {
	return q.idempotent
}

// Idempotent marks the query as being idempotent or not depending on
// the value.
// Non-idempotent query won't be retried.
// See "Retries and speculative execution" in package docs for more details.
func (q *Query) Idempotent(value bool) *Query {
	q.idempotent = value
	return q
}

// Bind sets query arguments of query.
// This can also be used to rebind new query arguments to an existing query instance.
//
// Binding a value set clears any callback previously set by [Session.Bind] or
// [Query.Binding]: a query draws its arguments from exactly one of the two.
//
// For supported Go to CQL type conversions for query parameters, see Session.Query documentation.
//
// Parameters:
//   - v: query argument values, positional, one per bind marker in the statement
//
// Returns:
//   - *Query: the same query, for chaining
func (q *Query) Bind(v ...interface{}) *Query {
	q.values = v
	q.binding = nil
	return q
}

// Binding sets a callback that generates this query's arguments at execution time.
//
// The callback receives the prepared statement's metadata and returns the argument
// values to marshal.
// It replaces any callback set earlier by [Session.Bind] or a previous Binding call,
// and clears values previously set by [Query.Bind]:
// a query draws its arguments from exactly one of the two.
//
// The callback is invoked only for statements the driver prepares; see [Session.Bind].
//
// Parameters:
//   - binding: callback returning the argument values, or an error to fail the query
//
// Returns:
//   - *Query: the same query, for chaining
func (q *Query) Binding(binding func(q *QueryInfo) ([]any, error)) *Query {
	q.values = nil
	q.binding = binding
	return q
}

// SerialConsistency sets the consistency level for the
// serial phase of conditional updates. That consistency can only be
// either SERIAL or LOCAL_SERIAL and if not present, it defaults to
// SERIAL. This option will be ignored for anything else that a
// conditional update/insert.
func (q *Query) SerialConsistency(cons Consistency) *Query {
	if !cons.isSerial() {
		panic("serial consistency can only be SERIAL or LOCAL_SERIAL got " + cons.String())
	}
	q.serialCons = cons
	return q
}

// GetSerialConsistency returns the currently configured serial consistency level
// for the query. The boolean return value indicates whether a serial consistency
// level has been set.
func (q *Query) GetSerialConsistency() (Consistency, bool) {
	return q.serialCons, q.serialCons.isSerial()
}

// PageState sets the paging state for the query to resume paging from a specific
// point in time. Setting this will disable to query paging for this query, and
// must be used for all subsequent pages.
func (q *Query) PageState(state []byte) *Query {
	q.initialPageState = state
	q.disableAutoPage = true
	return q
}

// NoSkipMetadata will override the internal result metadata cache so that the driver does not
// send skip_metadata for queries, this means that the result will always contain
// the metadata to parse the rows and will not reuse the metadata from the prepared
// statement. This should only be used to work around cassandra bugs, such as when using
// CAS operations which do not end in Cas.
//
// See https://issues.apache.org/jira/browse/CASSANDRA-11099
// https://github.com/apache/cassandra-gocql-driver/issues/612
func (q *Query) NoSkipMetadata() *Query {
	q.disableSkipMetadata = true
	return q
}

// Exec executes the query without returning any rows.
func (q *Query) Exec() error {
	return q.Iter().Close()
}

// ExecContext executes the query with the provided context without returning any rows.
func (q *Query) ExecContext(ctx context.Context) error {
	return q.IterContext(ctx).Close()
}

func isUseStatement(stmt string) bool {
	if len(stmt) < 3 {
		return false
	}

	return strings.EqualFold(stmt[0:3], "use")
}

// Iter executes the query and returns an iterator capable of iterating
// over all results.
func (q *Query) Iter() *Iter {
	return q.IterContext(q.context)
}

// IterContext executes the query with the provided context and returns an iterator capable of iterating
// over all results.
func (q *Query) IterContext(ctx context.Context) *Iter {
	if isUseStatement(q.stmt) {
		return newErrIter(ErrUseStmt, &queryMetrics{}, q.Keyspace(), nil, q.getKeyspace)
	}

	internalQry := newInternalQuery(q, ctx)
	iter := q.session.executeQuery(internalQry)
	iter.attachLeakDetector(q.session.logger)
	return iter
}

func (q *Query) iterInternal(c *Conn, ctx context.Context) *Iter {
	internalQry := newInternalQuery(q, ctx)
	internalQry.conn = c

	iter := c.executeQuery(internalQry.Context(), internalQry)
	if iter != nil {
		// set iter.host so that the caller can retrieve the connect address which should be preferable (if valid) for the local host
		iter.host = c.host
	}
	return iter
}

// MapScan executes the query, copies the columns of the first selected
// row into the map pointed at by m and discards the rest. If no rows
// were selected, ErrNotFound is returned.
//
// Columns are automatically converted to Go types based on their CQL type.
// See Iter.SliceMap for the complete CQL to Go type mapping table and examples.
func (q *Query) MapScan(m map[string]interface{}) error {
	return q.MapScanContext(q.context, m)
}

// MapScanContext executes the query with the provided context, copies the columns of the first selected
// row into the map pointed at by m and discards the rest. If no rows
// were selected, ErrNotFound is returned.
func (q *Query) MapScanContext(ctx context.Context, m map[string]interface{}) error {
	iter := q.IterContext(ctx)
	if err := iter.checkErrAndNotFound(); err != nil {
		iter.Close()
		return err
	}
	iter.MapScan(m)
	return iter.Close()
}

// Scan executes the query, copies the columns of the first selected
// row into the values pointed at by dest and discards the rest. If no rows
// were selected, ErrNotFound is returned.
//
// For supported CQL to Go type conversions, see Iter.Scan documentation.
func (q *Query) Scan(dest ...interface{}) error {
	return q.ScanContext(q.context, dest...)
}

// ScanContext executes the query with the provided context, copies the columns of the first selected
// row into the values pointed at by dest and discards the rest. If no rows
// were selected, ErrNotFound is returned.
//
// For supported CQL to Go type conversions, see Iter.Scan documentation.
func (q *Query) ScanContext(ctx context.Context, dest ...interface{}) error {
	iter := q.IterContext(ctx)
	if err := iter.checkErrAndNotFound(); err != nil {
		iter.Close()
		return err
	}
	iter.Scan(dest...)
	return iter.Close()
}

// ScanCAS executes a lightweight transaction (i.e. an UPDATE or INSERT
// statement containing an IF clause). If the transaction fails because
// the existing values did not match, the previous values will be stored
// in dest.
//
// As for INSERT .. IF NOT EXISTS, previous values will be returned as if
// SELECT * FROM. So using ScanCAS with INSERT is inherently prone to
// column mismatching. Use MapScanCAS to capture them safely.
//
// For supported CQL to Go type conversions, see Iter.Scan documentation.
func (q *Query) ScanCAS(dest ...interface{}) (applied bool, err error) {
	return q.ScanCASContext(q.context, dest...)
}

// ScanCASContext executes a lightweight transaction (i.e. an UPDATE or INSERT
// statement containing an IF clause) with the provided context. If the transaction fails because
// the existing values did not match, the previous values will be stored
// in dest.
//
// As for INSERT .. IF NOT EXISTS, previous values will be returned as if
// SELECT * FROM. So using ScanCAS with INSERT is inherently prone to
// column mismatching. Use MapScanCAS to capture them safely.
//
// For supported CQL to Go type conversions, see Iter.Scan documentation.
func (q *Query) ScanCASContext(ctx context.Context, dest ...interface{}) (applied bool, err error) {
	q.disableSkipMetadata = true
	iter := q.IterContext(ctx)
	if err := iter.checkErrAndNotFound(); err != nil {
		iter.Close()
		return false, err
	}
	if len(iter.Columns()) > 1 {
		dest = append([]interface{}{&applied}, dest...)
		iter.Scan(dest...)
	} else {
		iter.Scan(&applied)
	}
	return applied, iter.Close()
}

// MapScanCAS executes a lightweight transaction (i.e. an UPDATE or INSERT
// statement containing an IF clause). If the transaction fails because
// the existing values did not match, the previous values will be stored
// in dest map.
//
// As for INSERT .. IF NOT EXISTS, previous values will be returned as if
// SELECT * FROM. So using ScanCAS with INSERT is inherently prone to
// column mismatching. MapScanCAS is added to capture them safely.
func (q *Query) MapScanCAS(dest map[string]interface{}) (applied bool, err error) {
	return q.MapScanCASContext(q.context, dest)
}

// MapScanCASContext executes a lightweight transaction (i.e. an UPDATE or INSERT
// statement containing an IF clause) with the provided context. If the transaction fails because
// the existing values did not match, the previous values will be stored
// in dest map.
//
// As for INSERT .. IF NOT EXISTS, previous values will be returned as if
// SELECT * FROM. So using ScanCAS with INSERT is inherently prone to
// column mismatching. MapScanCAS is added to capture them safely.
func (q *Query) MapScanCASContext(ctx context.Context, dest map[string]interface{}) (applied bool, err error) {
	q.disableSkipMetadata = true
	iter := q.IterContext(ctx)
	if err := iter.checkErrAndNotFound(); err != nil {
		iter.Close()
		return false, err
	}
	iter.MapScan(dest)
	if iter.err != nil {
		iter.Close()
		return false, iter.err
	}
	// check if [applied] was returned, otherwise it might not be CAS
	if _, ok := dest["[applied]"]; ok {
		applied = dest["[applied]"].(bool)
		delete(dest, "[applied]")
	}

	return applied, iter.Close()
}

// SetHostID allows to define the host the query should be executed against. If the
// host was filtered or otherwise unavailable, then the query will error. If an empty
// string is sent, the default behavior, using the configured HostSelectionPolicy will
// be used. A hostID can be obtained from HostInfo.HostID() after calling GetHosts().
func (q *Query) SetHostID(hostID string) *Query {
	q.hostID = hostID
	return q
}

// GetHostID returns id of the host on which query should be executed.
func (q *Query) GetHostID() string {
	return q.hostID
}

// SetKeyspace will enable keyspace flag on the query.
// It allows to specify the keyspace that the query should be executed in
//
// Only available on protocol >= 5.
func (q *Query) SetKeyspace(keyspace string) *Query {
	q.keyspace = keyspace
	return q
}

// WithNowInSeconds will enable the with now_in_seconds flag on the query.
// Also, it allows to define now_in_seconds value.
//
// Only available on protocol >= 5.
func (q *Query) WithNowInSeconds(now int) *Query {
	q.nowInSecondsValue = &now
	return q
}

// Iter represents the result that was returned by the execution of a statement.
//
// If the statement is a query then this can be seen as an iterator that can be used to iterate over all rows that
// were returned by the query. The iterator might send additional queries to the
// database during the iteration if paging was enabled.
//
// It also contains metadata about the request that can be accessed by Iter.Keyspace(), Iter.Table(), Iter.Attempts(), Iter.Latency().
type Iter struct {
	err     error
	pos     int
	meta    resultMetadata
	numRows int
	next    *nextIter
	host    *HostInfo
	metrics *queryMetrics

	getKeyspace func() string
	keyspace    string
	routingInfo *queryRoutingInfo

	framer *framer
	closed int32

	// mapScanCache is lazily built on the first MapScan call and reused
	// for every subsequent row in this page. Replaced by paging swaps.
	mapScanCache *iterMapScanCache
}

// iterMapScanCache holds the per-Iter buffers that MapScan reuses
// across rows: column names, default-value slots whose addresses are
// handed to Scan when the user has not pre-populated the map, and a
// reusable values slice for the Scan call.
type iterMapScanCache struct {
	names     []string   // tuple-expanded scan-order column names
	zeros     []any      // per-entry default storage; refreshed per row
	zeroTypes []TypeInfo // TypeInfo per entry, used to refresh zeros[i] each row
	values    []any      // reused buffer passed to iter.Scan(values...)
}

func newErrIter(err error, metrics *queryMetrics, keyspace string, routingInfo *queryRoutingInfo, getKeyspace func() string) *Iter {
	iter := newIter(metrics, keyspace, routingInfo, getKeyspace)
	iter.err = err
	return iter
}

func newIter(metrics *queryMetrics, keyspace string, routingInfo *queryRoutingInfo, getKeyspace func() string) *Iter {
	return &Iter{metrics: metrics, keyspace: keyspace, routingInfo: routingInfo, getKeyspace: getKeyspace}
}

// attachLeakDetector arms a finalizer that warns and releases the framer
// if the Iter is garbage-collected without Close() being called. Close()
// clears the finalizer, so the warning fires only for genuine misuse.
//
// The finalizer is set ONLY on the user-facing iter pointer (returned
// from Query.IterContext / Batch.IterContext). It must NOT be set on
// the inner iter produced by nextIter.fetch() during paging, because
// Scan() does *iter = *iter.next.fetch() — a struct copy that shares
// the framer pointer. A finalizer on the inner iter would fire after
// the swap and double-release a framer still in use by the outer iter.
func (iter *Iter) attachLeakDetector(logger StructuredLogger) {
	if iter == nil || logger == nil || iter.framer == nil {
		return
	}
	runtime.SetFinalizer(iter, func(i *Iter) {
		if atomic.LoadInt32(&i.closed) != 0 {
			return
		}
		logger.Warning("gocql: Iter was garbage-collected without Close() — possible resource leak; always defer iter.Close() after Iter()/IterContext()")
		_ = i.Close()
	})
}

// Host returns the host which the statement was sent to.
func (iter *Iter) Host() *HostInfo {
	return iter.host
}

// Columns returns the name and type of the selected columns.
func (iter *Iter) Columns() []ColumnInfo {
	return iter.meta.columns
}

// Attempts returns the number of times the statement was executed.
func (iter *Iter) Attempts() int {
	return iter.metrics.attempts()
}

// Latency returns the average amount of nanoseconds per attempt of the statement.
func (iter *Iter) Latency() int64 {
	return iter.metrics.latency()
}

// Keyspace returns the keyspace the statement was executed against if the driver could determine it.
func (iter *Iter) Keyspace() string {
	if iter.getKeyspace != nil {
		return iter.getKeyspace()
	}

	if iter.routingInfo != nil {
		if ks := iter.routingInfo.getKeyspace(); ks != "" {
			return ks
		}
	}

	return iter.keyspace
}

// Table returns name of the table the statement was executed against if the driver could determine it.
func (iter *Iter) Table() string {
	if iter.routingInfo != nil {
		return iter.routingInfo.getTable()
	}
	return ""
}

type Scanner interface {
	// Next advances the row pointer to point at the next row, the row is valid until
	// the next call of Next. It returns true if there is a row which is available to be
	// scanned into with Scan.
	// Next must be called before every call to Scan.
	Next() bool

	// Scan copies the current row's columns into dest. If the length of dest does not equal
	// the number of columns returned in the row an error is returned. If an error is encountered
	// when unmarshalling a column into the value in dest an error is returned and the row is invalidated
	// until the next call to Next.
	// Next must be called before calling Scan, if it is not an error is returned.
	//
	// For supported CQL to Go type conversions, see Iter.Scan documentation.
	Scan(...interface{}) error

	// Err returns the if there was one during iteration that resulted in iteration being unable to complete.
	// Err will also release resources held by the iterator, the Scanner should not used after being called.
	Err() error
}

type iterScanner struct {
	iter  *Iter
	cols  [][]byte
	valid bool
}

func (is *iterScanner) Next() bool {
	iter := is.iter
	if iter.err != nil {
		return false
	}

	if iter.pos >= iter.numRows {
		if iter.next != nil {
			// Close the outgoing iter before reassigning is.iter to the
			// next page. This (a) returns the previous page's framer to
			// the pool, and (b) clears any leak-detector finalizer on
			// the user-facing iter so it does not warn after a normal
			// scanner advance across page boundaries.
			iter.Close()
			is.iter = iter.next.fetch()
			return is.Next()
		}
		return false
	}

	for i := 0; i < len(is.cols); i++ {
		col, err := iter.readColumn()
		if err != nil {
			iter.err = err
			return false
		}
		is.cols[i] = col
	}
	iter.pos++
	is.valid = true

	return true
}

func scanColumn(p []byte, col ColumnInfo, dest []interface{}) (int, error) {
	if dest[0] == nil {
		return 1, nil
	}

	if col.TypeInfo.Type() == TypeTuple {
		// this will panic, actually a bug, please report
		tuple := col.TypeInfo.(TupleTypeInfo)

		count := len(tuple.Elems)
		// here we pass in a slice of the struct which has the number number of
		// values as elements in the tuple
		if err := Unmarshal(col.TypeInfo, p, dest[:count]); err != nil {
			return 0, err
		}
		return count, nil
	} else {
		if err := Unmarshal(col.TypeInfo, p, dest[0]); err != nil {
			return 0, err
		}
		return 1, nil
	}
}

func (is *iterScanner) Scan(dest ...interface{}) error {
	if !is.valid {
		return errors.New("gocql: Scan called without calling Next")
	}

	iter := is.iter
	// currently only support scanning into an expand tuple, such that its the same
	// as scanning in more values from a single column
	if len(dest) != iter.meta.actualColCount {
		return fmt.Errorf("gocql: not enough columns to scan into: have %d want %d", len(dest), iter.meta.actualColCount)
	}

	// i is the current position in dest, could posible replace it and just use
	// slices of dest
	i := 0
	var err error
	for _, col := range iter.meta.columns {
		var n int
		n, err = scanColumn(is.cols[i], col, dest[i:])
		if err != nil {
			break
		}
		i += n
	}

	is.valid = false
	return err
}

func (is *iterScanner) Err() error {
	iter := is.iter
	is.iter = nil
	is.cols = nil
	is.valid = false
	return iter.Close()
}

// Scanner returns a row Scanner which provides an interface to scan rows in a manner which is
// similar to database/sql. The iter should NOT be used again after calling this method.
func (iter *Iter) Scanner() Scanner {
	if iter == nil {
		return nil
	}

	return &iterScanner{iter: iter, cols: make([][]byte, len(iter.meta.columns))}
}

func (iter *Iter) readColumn() ([]byte, error) {
	return iter.framer.readBytes()
}

// Scan consumes the next row of the iterator and copies the columns of the
// current row into the values pointed at by dest. Use nil as a dest value
// to skip the corresponding column. Scan might send additional queries
// to the database to retrieve the next set of rows if paging was enabled.
//
// Scan returns true if the row was successfully unmarshaled or false if the
// end of the result set was reached or if an error occurred. Close should
// be called afterwards to retrieve any potential errors.
//
// Supported CQL to Go type conversions are as follows, other type combinations may be added in the future:
//
//	CQL Type                     | Go Type (dest)              | Note
//	ascii, text, varchar         | *string                     |
//	ascii, text, varchar         | *[]byte                     | non-nil buffer is reused
//	bigint, counter              | *int64                      |
//	bigint, counter              | *int, *int32, *int16, *int8 | with range checking
//	bigint, counter              | *uint64, *uint32, *uint16   | with range checking
//	bigint, counter              | *big.Int                    |
//	bigint, counter              | *string                     | formatted as base 10 number
//	blob                         | *[]byte                     | non-nil buffer is reused
//	boolean                      | *bool                       |
//	date                         | *time.Time                  | start of day in UTC
//	date                         | *string                     | formatted as "2006-01-02"
//	decimal                      | *inf.Dec                    |
//	double                       | *float64                    |
//	duration                     | *gocql.Duration             |
//	duration                     | *time.Duration              | with range checking
//	float                        | *float32                    |
//	inet                         | *net.IP                     |
//	inet                         | *string                     | IPv4 or IPv6 address string
//	int                          | *int                        |
//	int                          | *int32, *int16, *int8       | with range checking
//	int                          | *uint32, *uint16, *uint8    | with range checking
//	list<T>, set<T>              | *[]T                        |
//	list<T>, set<T>              | *[N]T                       | array with compatible size
//	map<K,V>                     | *map[K]V                    |
//	smallint                     | *int16                      |
//	smallint                     | *int, *int32, *int8         | with range checking
//	smallint                     | *uint16, *uint8             | with range checking
//	time                         | *time.Duration              | nanoseconds since start of day
//	time                         | *int64                      | nanoseconds since start of day
//	timestamp                    | *time.Time                  |
//	timestamp                    | *int64                      | milliseconds since Unix epoch
//	timeuuid                     | *gocql.UUID                 |
//	timeuuid                     | *time.Time                  | timestamp of the UUID
//	timeuuid                     | *string                     | hex representation
//	timeuuid                     | *[]byte                     | 16-byte raw UUID
//	tinyint                      | *int8                       |
//	tinyint                      | *int, *int32, *int16        | with range checking
//	tinyint                      | *uint8                      | with range checking
//	tuple<T1,T2,...>             | *[]interface{}              |
//	tuple<T1,T2,...>             | *[N]interface{}             | array with compatible size
//	tuple<T1,T2,...>             | *struct                     | fields unmarshaled in declaration order
//	user-defined types           | gocql.UDTUnmarshaler        | UnmarshalUDT is called
//	user-defined types           | *map[string]interface{}     |
//	user-defined types           | *struct                     | cql tag or field name matching
//	uuid                         | *gocql.UUID                 |
//	uuid                         | *string                     | hex representation
//	uuid                         | *[]byte                     | 16-byte raw UUID
//	varint                       | *big.Int                    |
//	varint                       | *int64, *int32, *int16, *int8 | with range checking
//	varint                       | *string                     | formatted as base 10 number
//	vector<T,N>                  | *[]T                        |
//	vector<T,N>                  | *[N]T                       | array with exact size match
//
// Important Notes:
//   - NULL values are unmarshaled as zero values of the destination type
//   - Use **Type (pointer to pointer) to distinguish NULL from zero values
//   - Range checking prevents overflow when converting between numeric types
//   - For SliceMap/MapScan type mappings, see Iter.SliceMap documentation
func (iter *Iter) Scan(dest ...interface{}) bool {
	if iter.err != nil {
		return false
	}

	if iter.pos >= iter.numRows {
		if iter.next != nil {
			*iter = *iter.next.fetch()
			return iter.Scan(dest...)
		}
		return false
	}

	if iter.next != nil && iter.pos >= iter.next.pos {
		iter.next.fetchAsync()
	}

	// currently only support scanning into an expand tuple, such that its the same
	// as scanning in more values from a single column
	if len(dest) != iter.meta.actualColCount {
		iter.err = fmt.Errorf("gocql: not enough columns to scan into: have %d want %d", len(dest), iter.meta.actualColCount)
		return false
	}

	// i is the current position in dest, could posible replace it and just use
	// slices of dest
	i := 0
	for _, col := range iter.meta.columns {
		colBytes, err := iter.readColumn()
		if err != nil {
			iter.err = err
			return false
		}

		n, err := scanColumn(colBytes, col, dest[i:])
		if err != nil {
			iter.err = err
			return false
		}
		i += n
	}

	iter.pos++
	return true
}

// GetCustomPayload returns any parsed custom payload results if given in the
// response from Cassandra. Note that the result is not a copy.
//
// This additional feature of CQL Protocol v4
// allows additional results and query information to be returned by
// custom QueryHandlers running in your C* cluster.
// See https://datastax.github.io/java-driver/manual/custom_payloads/
func (iter *Iter) GetCustomPayload() map[string][]byte {
	if iter.framer != nil {
		return iter.framer.customPayload
	}
	return nil
}

// Warnings returns any warnings generated if given in the response from Cassandra.
//
// This is only available starting with CQL Protocol v4.
func (iter *Iter) Warnings() []string {
	if iter.framer != nil {
		return iter.framer.header.warnings
	}
	return nil
}

// Close closes the iterator and returns any errors that happened during
// the query or the iteration.
func (iter *Iter) Close() error {
	if atomic.CompareAndSwapInt32(&iter.closed, 0, 1) {
		if iter.framer != nil {
			iter.framer.release()
			iter.framer = nil
		}
		// Drop the MapScan cache so its zeros slots — which retain the
		// last row's values, including possibly large strings or blob
		// []byte — are not held alive by a long-lived closed Iter.
		iter.mapScanCache = nil
		// Clear the leak-detector finalizer (if any). Safe to call even
		// when no finalizer was registered (e.g. newErrIter path).
		runtime.SetFinalizer(iter, nil)
	}

	return iter.err
}

// WillSwitchPage detects if iterator reached end of current page
// and the next page is available.
func (iter *Iter) WillSwitchPage() bool {
	return iter.pos >= iter.numRows && iter.next != nil
}

// checkErrAndNotFound handle error and NotFound in one method.
func (iter *Iter) checkErrAndNotFound() error {
	if iter.err != nil {
		return iter.err
	} else if iter.numRows == 0 {
		return ErrNotFound
	}
	return nil
}

// PageState return the current paging state for a query which can be used for
// subsequent queries to resume paging this point.
func (iter *Iter) PageState() []byte {
	return iter.meta.pagingState
}

// NumRows returns the number of rows in this pagination, it will update when new
// pages are fetched, it is not the value of the total number of rows this iter
// will return unless there is only a single page returned.
func (iter *Iter) NumRows() int {
	return iter.numRows
}

// nextIter holds state for fetching a single page in an iterator.
// single page might be attempted multiple times due to retries.
type nextIter struct {
	q     *internalQuery
	pos   int
	oncea sync.Once
	once  sync.Once
	next  *Iter
}

func (n *nextIter) fetchAsync() {
	n.oncea.Do(func() {
		go func() {
			// n.fetch handles its own panic recovery inside the once.Do.
			// This outer wrapper is belt-and-braces in case something else
			// panics in the spawned goroutine.
			defer recoverGoroutine(n.q.session.logger, "nextIter.fetchAsync", nil)
			n.fetch()
		}()
	})
}

func (n *nextIter) fetch() *Iter {
	n.once.Do(func() {
		// Recover INSIDE the once.Do body. If the page fetch panics,
		// sync.Once marks itself done after the panic returns from the
		// once body — any later n.fetch() call would return early
		// without re-running, leaving n.next == nil and the caller at
		// session.go:1852 nil-derefs. Teardown sets n.next to a
		// panic-error iter before the once finalizes.
		defer recoverGoroutine(n.q.session.logger, "nextIter.fetch", func(err error) {
			n.next = newErrIter(err, n.q.metrics, n.q.Keyspace(),
				n.q.routingInfo, n.q.qryOpts.getKeyspace)
		})

		// if the query was specifically run on a connection then re-use that
		// connection when fetching the next results
		if n.q.conn != nil {
			n.next = n.q.conn.executeQuery(n.q.qryOpts.context, n.q)
		} else {
			n.next = n.q.session.executeQuery(n.q)
		}
	})
	return n.next
}

type Batch struct {
	Type                  BatchType
	Entries               []BatchEntry
	Cons                  Consistency
	routingKey            []byte
	CustomPayload         map[string][]byte
	rt                    RetryPolicy
	spec                  SpeculativeExecutionPolicy
	trace                 Tracer
	observer              BatchObserver
	session               *Session
	serialCons            Consistency
	defaultTimestamp      bool
	defaultTimestampValue int64
	context               context.Context
	keyspace              string
	nowInSeconds          *int
}

// Deprecated: use Session.Batch instead
// NewBatch creates a new batch operation using defaults defined in the cluster
//
// Deprecated: use Session.Batch instead
func (s *Session) NewBatch(typ BatchType) *Batch {
	return s.Batch(typ)
}

// Batch creates a new batch operation using defaults defined in the cluster
func (s *Session) Batch(typ BatchType) *Batch {
	batch := &Batch{
		Type:             typ,
		rt:               s.cfg.RetryPolicy,
		serialCons:       s.cfg.SerialConsistency,
		trace:            s.trace,
		observer:         s.batchObserver,
		session:          s,
		Cons:             s.cons,
		defaultTimestamp: s.cfg.DefaultTimestamp,
		keyspace:         s.cfg.Keyspace,
		spec:             &NonSpeculativeExecution{},
	}

	return batch
}

// Trace enables tracing of this batch. Look at the documentation of the
// Tracer interface to learn more about tracing.
func (b *Batch) Trace(trace Tracer) *Batch {
	b.trace = trace
	return b
}

// Observer enables batch-level observer on this batch.
// The provided observer will be called every time this batched query is executed.
func (b *Batch) Observer(observer BatchObserver) *Batch {
	b.observer = observer
	return b
}

func (b *Batch) Keyspace() string {
	return b.keyspace
}

// Consistency sets the consistency level for this batch. If no consistency
// level have been set, the default consistency level of the cluster
// is used.
func (b *Batch) Consistency(cons Consistency) *Batch {
	b.Cons = cons
	return b
}

// GetConsistency returns the currently configured consistency level for the batch
// operation.
func (b *Batch) GetConsistency() Consistency {
	return b.Cons
}

// Deprecated: Use Batch.Consistency
func (b *Batch) SetConsistency(c Consistency) {
	b.Cons = c
}

// Deprecated: Context retrieval is deprecated. Pass context directly to execution methods
// like ExecContext or IterContext instead.
func (b *Batch) Context() context.Context {
	if b.context == nil {
		return context.Background()
	}
	return b.context
}

func (b *Batch) IsIdempotent() bool {
	for _, entry := range b.Entries {
		if !entry.Idempotent {
			return false
		}
	}
	return true
}

func (b *Batch) speculativeExecutionPolicy() SpeculativeExecutionPolicy {
	return b.spec
}

func (b *Batch) SpeculativeExecutionPolicy(sp SpeculativeExecutionPolicy) *Batch {
	b.spec = sp
	return b
}

// Query adds the query to the batch operation.
//
// For supported Go to CQL type conversions for query parameters, see Session.Query documentation.
func (b *Batch) Query(stmt string, args ...interface{}) *Batch {
	b.Entries = append(b.Entries, BatchEntry{Stmt: stmt, Args: args})
	return b
}

// Bind adds the query to the batch operation and correlates it with a binding callback
// that will be invoked when the batch is executed. The binding callback allows the application
// to define which query argument values will be marshalled as part of the batch execution.
//
// For supported Go to CQL type conversions for query parameters, see Session.Query documentation.
func (b *Batch) Bind(stmt string, bind func(q *QueryInfo) ([]interface{}, error)) {
	b.Entries = append(b.Entries, BatchEntry{Stmt: stmt, binding: bind})
}

// RetryPolicy sets the retry policy to use when executing the batch operation
func (b *Batch) RetryPolicy(r RetryPolicy) *Batch {
	b.rt = r
	return b
}

// Deprecated: Use Batch.ExecContext or Batch.IterContext instead. This will be removed in a future major version.
//
// WithContext returns a shallow copy of b with its context
// set to ctx.
//
// The provided context controls the entire lifetime of executing a
// query, queries will be canceled and return once the context is
// canceled.
func (b *Batch) WithContext(ctx context.Context) *Batch {
	b2 := *b
	b2.context = ctx
	return &b2
}

// Size returns the number of batch statements to be executed by the batch operation.
func (b *Batch) Size() int {
	return len(b.Entries)
}

// SerialConsistency sets the consistency level for the
// serial phase of conditional updates. That consistency can only be
// either SERIAL or LOCAL_SERIAL and if not present, it defaults to
// SERIAL. This option will be ignored for anything else that a
// conditional update/insert.
//
// Only available for protocol 3 and above
func (b *Batch) SerialConsistency(cons Consistency) *Batch {
	if !cons.isSerial() {
		panic("serial consistency can only be SERIAL or LOCAL_SERIAL got " + cons.String())
	}
	b.serialCons = cons
	return b
}

// GetSerialConsistency returns the currently configured serial consistency level
// for the batch. The boolean return value indicates whether a serial consistency
// level has been set.
func (b *Batch) GetSerialConsistency() (Consistency, bool) {
	return b.serialCons, b.serialCons.isSerial()
}

// DefaultTimestamp will enable the with default timestamp flag on the query.
// If enable, this will replace the server side assigned
// timestamp as default timestamp. Note that a timestamp in the query itself
// will still override this timestamp. This is entirely optional.
//
// Only available on protocol >= 3
func (b *Batch) DefaultTimestamp(enable bool) *Batch {
	b.defaultTimestamp = enable
	return b
}

// WithTimestamp will enable the with default timestamp flag on the query
// like DefaultTimestamp does. But also allows to define value for timestamp.
// It works the same way as USING TIMESTAMP in the query itself, but
// should not break prepared query optimization.
//
// Only available on protocol >= 3
func (b *Batch) WithTimestamp(timestamp int64) *Batch {
	b.DefaultTimestamp(true)
	b.defaultTimestampValue = timestamp
	return b
}

func createRoutingKey(meta *StatementMetadata, values []interface{}) ([]byte, error) {
	if meta == nil || len(meta.PKBindColumnIndexes) == 0 {
		return nil, nil
	}

	if len(values) != len(meta.BindColumns) {
		return nil, errors.New("gocql: number of values does not match the number of bind columns")
	}

	if len(meta.PKBindColumnIndexes) == 1 {
		// single column routing key
		routingKey, err := Marshal(
			meta.BindColumns[meta.PKBindColumnIndexes[0]].TypeInfo,
			values[meta.PKBindColumnIndexes[0]],
		)
		if err != nil {
			return nil, err
		}
		return routingKey, nil
	}

	// composite routing key
	buf := bytes.NewBuffer(make([]byte, 0, 256))
	lenBuf := make([]byte, 2)
	for i := range meta.PKBindColumnIndexes {
		encoded, err := Marshal(
			meta.BindColumns[meta.PKBindColumnIndexes[i]].TypeInfo,
			values[meta.PKBindColumnIndexes[i]],
		)
		if err != nil {
			return nil, err
		}
		// first write the length of the encoded value as a 16-bit big endian integer
		binary.BigEndian.PutUint16(lenBuf, uint16(len(encoded)))
		buf.Write(lenBuf)
		// then write the encoded value and a null byte to separate the values
		buf.Write(encoded)
		buf.WriteByte(0x00)
	}
	return buf.Bytes(), nil
}

// SetKeyspace will enable keyspace flag on the query.
// It allows to specify the keyspace that the query should be executed in
//
// Only available on protocol >= 5.
func (b *Batch) SetKeyspace(keyspace string) *Batch {
	b.keyspace = keyspace
	return b
}

// WithNowInSeconds will enable the with now_in_seconds flag on the query.
// Also, it allows to define now_in_seconds value.
//
// Only available on protocol >= 5.
func (b *Batch) WithNowInSeconds(now int) *Batch {
	b.nowInSeconds = &now
	return b
}

// BatchType represents the type of batch.
// Available types: LoggedBatch, UnloggedBatch, CounterBatch.
type BatchType byte

const (
	LoggedBatch   BatchType = 0
	UnloggedBatch BatchType = 1
	CounterBatch  BatchType = 2
)

// BatchEntry represents a single statement within a batch operation.
// It contains the statement, arguments, and execution metadata.
type BatchEntry struct {
	Stmt       string
	Args       []interface{}
	Idempotent bool
	binding    func(q *QueryInfo) ([]interface{}, error)
}

// ColumnInfo represents metadata about a column in a query result.
// It contains the keyspace, table, column name, and type information.
type ColumnInfo struct {
	Keyspace string
	Table    string
	Name     string
	TypeInfo TypeInfo
}

func (c ColumnInfo) String() string {
	return fmt.Sprintf("[column keyspace=%s table=%s name=%s type=%v]", c.Keyspace, c.Table, c.Name, c.TypeInfo)
}

// routingCacheKey is the cache key for routing metadata.
// Using a struct avoids string concatenation allocations.
type routingCacheKey struct {
	keyspace  string
	statement string
}

// routingKeyInfoLRU is the routing metadata cache using otter for high-performance
// concurrent access without global mutex contention.
type routingKeyInfoLRU struct {
	cache *otter.Cache[routingCacheKey, *StatementMetadata]
}

// newRoutingKeyInfoLRU creates a new routing metadata cache with the given maximum size.
func newRoutingKeyInfoLRU(maxSize int) *routingKeyInfoLRU {
	if maxSize <= 0 {
		maxSize = 1000 // Fallback to default if invalid
	}
	return &routingKeyInfoLRU{
		cache: otter.Must(&otter.Options[routingCacheKey, *StatementMetadata]{
			MaximumSize: maxSize,
		}),
	}
}

// keyFor constructs a cache key from keyspace and statement.
func (r *routingKeyInfoLRU) keyFor(keyspace, statement string) routingCacheKey {
	return routingCacheKey{
		keyspace:  keyspace,
		statement: statement,
	}
}

// get retrieves routing metadata from the cache.
func (r *routingKeyInfoLRU) get(key routingCacheKey) (*StatementMetadata, bool) {
	return r.cache.GetIfPresent(key)
}

// getOrLoad retrieves routing metadata from the cache, loading it if not present.
// Otter handles deduplication of concurrent loads for the same key.
//
// Waiters are not bound by ctx: otter parks them in a context-free WaitGroup, so a
// caller whose context expires still waits for the load to finish. Conn.prepareStatement
// works around the same limitation for the prepared-statement cache; this cache is
// deliberately left alone, because nothing has yet measured it as a problem here.
func (r *routingKeyInfoLRU) getOrLoad(
	ctx context.Context,
	key routingCacheKey,
	loader otter.Loader[routingCacheKey, *StatementMetadata],
) (*StatementMetadata, error) {
	return r.cache.Get(ctx, key, loader)
}

// set adds or updates routing metadata in the cache.
func (r *routingKeyInfoLRU) set(key routingCacheKey, val *StatementMetadata) {
	r.cache.Set(key, val)
}

// delete removes routing metadata from the cache.
func (r *routingKeyInfoLRU) delete(key routingCacheKey) {
	r.cache.Invalidate(key)
}

// size returns the estimated number of entries in the cache.
func (r *routingKeyInfoLRU) size() int {
	return r.cache.EstimatedSize()
}

// clear removes all entries from the cache.
func (r *routingKeyInfoLRU) clear() {
	r.cache.InvalidateAll()
}

// Tracer is the interface implemented by query tracers. Tracers have the
// ability to obtain a detailed event log of all events that happened during
// the execution of a query from Cassandra. Gathering this information might
// be essential for debugging and optimizing queries, but this feature should
// not be used on production systems with very high load.
//
// Trace is usually called before the query it belongs to returns, but it is not
// guaranteed to be: the PREPARE of a statement is shared with every concurrent
// caller of that statement and runs to completion even after the caller that
// started it has given up on its context, so that caller's Tracer can be called
// after its Query.Exec or Query.Iter has returned. An implementation must
// therefore be safe to call from another goroutine and must not assume the
// destination it writes to is still owned by the caller.
type Tracer interface {
	Trace(traceId []byte)
}

type traceWriter struct {
	session *Session
	w       io.Writer
	mu      sync.Mutex
}

// NewTraceWriter returns a simple Tracer implementation that outputs
// the event log in a textual format.
func NewTraceWriter(session *Session, w io.Writer) Tracer {
	return &traceWriter{session: session, w: w}
}

func (t *traceWriter) Trace(traceId []byte) {
	var (
		coordinator string
		duration    int
	)
	iter := t.session.control.query(`SELECT coordinator, duration
			FROM system_traces.sessions
			WHERE session_id = ?`, traceId)

	iter.Scan(&coordinator, &duration)
	if err := iter.Close(); err != nil {
		t.mu.Lock()
		fmt.Fprintln(t.w, "Error:", err)
		t.mu.Unlock()
		return
	}

	var (
		timestamp time.Time
		activity  string
		source    string
		elapsed   int
		thread    string
	)

	t.mu.Lock()
	defer t.mu.Unlock()

	fmt.Fprintf(t.w, "Tracing session %016x (coordinator: %s, duration: %v):\n",
		traceId, coordinator, time.Duration(duration)*time.Microsecond)

	iter = t.session.control.query(`SELECT event_id, activity, source, source_elapsed, thread
			FROM system_traces.events
			WHERE session_id = ?`, traceId)

	for iter.Scan(&timestamp, &activity, &source, &elapsed, &thread) {
		fmt.Fprintf(t.w, "%s: %s [%s] (source: %s, elapsed: %d)\n",
			timestamp.Format("2006/01/02 15:04:05.999999"), activity, thread, source, elapsed)
	}

	if err := iter.Close(); err != nil {
		fmt.Fprintln(t.w, "Error:", err)
	}
}

// GetHosts return a list of hosts in the ring the driver knows of.
func (s *Session) GetHosts() []*HostInfo {
	return s.ring.allHosts()
}

type ObservedQuery struct {
	Keyspace  string
	Statement string

	// Values holds a slice of bound values for the query.
	// Do not modify the values here, they are shared with multiple goroutines.
	Values []interface{}

	Start time.Time // time immediately before the query was called
	End   time.Time // time immediately after the query returned

	// Rows is the number of rows in the current iter.
	// In paginated queries, rows from previous scans are not counted.
	// Rows is not used in batch queries and remains at the default value
	Rows int

	// Host is the information about the host that performed the query
	Host *HostInfo

	// The metrics per this host
	Metrics *hostMetrics

	// Err is the error in the query.
	// It only tracks network errors or errors of bad cassandra syntax, in particular selects with no match return nil error
	Err error

	// Attempt is the index of attempt at executing this query.
	// The first attempt is number zero and any retries have non-zero attempt number.
	Attempt int

	// Query object associated with this request. Should be used as read only.
	Query *Query
}

// QueryObserver is the interface implemented by query observers / stat collectors.
//
// Experimental, this interface and use may change
type QueryObserver interface {
	// ObserveQuery gets called on every query to cassandra, including all queries in an iterator when paging is enabled.
	// It doesn't get called if there is no query because the session is closed or there are no connections available.
	// The error reported only shows query errors, i.e. if a SELECT is valid but finds no matches it will be nil.
	ObserveQuery(context.Context, ObservedQuery)
}

type ObservedBatch struct {
	Keyspace   string
	Statements []string

	// Values holds a slice of bound values for each statement.
	// Values[i] are bound values passed to Statements[i].
	// Do not modify the values here, they are shared with multiple goroutines.
	Values [][]interface{}

	Start time.Time // time immediately before the batch query was called
	End   time.Time // time immediately after the batch query returned

	// Host is the informations about the host that performed the batch
	Host *HostInfo

	// Err is the error in the batch query.
	// It only tracks network errors or errors of bad cassandra syntax, in particular selects with no match return nil error
	Err error

	// The metrics per this host
	Metrics *hostMetrics

	// Attempt is the index of attempt at executing this query.
	// The first attempt is number zero and any retries have non-zero attempt number.
	Attempt int

	// Batch object associated with this request. Should be used as read only.
	Batch *Batch
}

// BatchObserver is the interface implemented by batch observers / stat collectors.
type BatchObserver interface {
	// ObserveBatch gets called on every batch query to cassandra.
	// It also gets called once for each query in a batch.
	// It doesn't get called if there is no query because the session is closed or there are no connections available.
	// The error reported only shows query errors, i.e. if a SELECT is valid but finds no matches it will be nil.
	// Unlike QueryObserver.ObserveQuery it does no reporting on rows read.
	ObserveBatch(context.Context, ObservedBatch)
}

type ObservedConnect struct {
	// Host is the information about the host about to connect
	Host *HostInfo

	Start time.Time // time immediately before the dial is called
	End   time.Time // time immediately after the dial returned

	// Err is the connection error (if any)
	Err error
}

// ConnectObserver is the interface implemented by connect observers / stat collectors.
type ConnectObserver interface {
	// ObserveConnect gets called when a new connection to cassandra is made.
	ObserveConnect(ObservedConnect)
}

// Deprecated: Unused
type Error struct {
	Code    int
	Message string
}

func (e Error) Error() string {
	return e.Message
}

var (
	ErrNotFound             = errors.New("not found")
	ErrUnavailable          = errors.New("unavailable")
	ErrUnsupported          = errors.New("feature not supported")
	ErrTooManyStmts         = errors.New("too many statements")
	ErrUseStmt              = errors.New("use statements aren't supported. Please see https://github.com/apache/cassandra-gocql-driver for explanation.")
	ErrSessionClosed        = errors.New("session has been closed")
	ErrNoConnections        = errors.New("gocql: no hosts available in the pool")
	ErrNoKeyspace           = errors.New("no keyspace provided")
	ErrKeyspaceDoesNotExist = errors.New("keyspace does not exist")
	ErrNoMetadata           = errors.New("no metadata available")
)

// ErrProtocol represents a protocol-level error.
type ErrProtocol struct{ error }

// Unwrap returns the wrapped underlying error so that errors.As and errors.Is
// can inspect the cause carried by an ErrProtocol.
func (e ErrProtocol) Unwrap() error {
	return e.error
}

// NewErrProtocol creates a new protocol error with the specified format and arguments.
func NewErrProtocol(format string, args ...interface{}) error {
	return ErrProtocol{fmt.Errorf(format, args...)}
}

// BatchSizeMaximum is the maximum number of statements a batch operation can have.
// This limit is set by Cassandra and could change in the future.
const BatchSizeMaximum = 65535
