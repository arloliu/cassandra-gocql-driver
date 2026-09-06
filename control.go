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
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	controlConnStarting = 0
	controlConnStarted  = 1
	controlConnClosing  = -1
)

// Ensure that the atomic variable is aligned to a 64bit boundary
// so that atomic operations can be applied on 32bit architectures.
type controlConn struct {
	state        int32
	reconnecting int32

	session *Session
	conn    atomic.Value

	retry RetryPolicy

	// quit is signaled by close() to stop the heartBeat loop. Buffered 1
	// so close() never blocks even if the heartBeat goroutine has exited
	// early (e.g. via a recovered panic). The receiver only reads it once
	// from the heartBeat select.
	quit chan struct{}

	// lifecycleMu serialises the terminal close latch, the candidate set and
	// the publication of a connection, so that a setup racing Session.Close
	// either publishes before close latched (and close then owns and closes the
	// published connection) or finds the closing state (and does not publish).
	lifecycleMu sync.Mutex
	// candidates holds every connection dialled for the control connection that
	// has neither been published nor closed by its caller yet, so that a
	// concurrent close can close the ones still in flight. Guarded by lifecycleMu.
	candidates map[*Conn]struct{}
}

// errControlConnClosing reports that the control connection is closing, so a
// reconnect must stop rather than dial another host or fall back to contact
// points. A connection returned alongside it is already closed.
var errControlConnClosing = errors.New("gocql: control connection is closing")

// controlSnapshot is what close() latched: the published connection, if any,
// and every candidate still in flight, so both can be closed outside lifecycleMu.
type controlSnapshot struct {
	published  *connHost
	candidates []*Conn
}

func createControlConn(session *Session) *controlConn {
	control := &controlConn{
		session:    session,
		quit:       make(chan struct{}, 1),
		retry:      &SimpleRetryPolicy{NumRetries: 3},
		candidates: map[*Conn]struct{}{},
	}

	control.conn.Store((*connHost)(nil))

	return control
}

func (c *controlConn) heartBeat() {
	// Plain log-and-exit teardown. Do NOT transition state on recovered
	// panic: controlConnClosing is the terminal Session.Close sentinel
	// and reconnect() short-circuits when state == Closing, so reusing
	// that state here would permanently disable reconnection. The
	// buffered c.quit chan independently guarantees close() does not
	// block if the heartbeat goroutine has exited via recovery. The
	// session continues in a degraded state (no heartbeat) until
	// Session.Close, but reconnects still work.
	defer recoverGoroutine(c.session.logger, "controlConn.heartBeat", nil)

	// If close() latched controlConnClosing before this goroutine started, the
	// CAS fails and the heartbeat exits at once; the terminal state stands.
	if !atomic.CompareAndSwapInt32(&c.state, controlConnStarting, controlConnStarted) {
		return
	}

	sleepTime := 1 * time.Second
	timer := time.NewTimer(sleepTime)
	defer timer.Stop()

	for {
		timer.Reset(sleepTime)

		select {
		case <-c.quit:
			return
		case <-timer.C:
		}

		resp, err := c.writeFrame(&writeOptionsFrame{})
		if err != nil {
			c.session.logger.Debug("Control connection failed to send heartbeat.", NewLogFieldError("err", err))
			goto reconn
		}

		switch actualResp := resp.(type) {
		case *supportedFrame:
			// Everything ok
			sleepTime = 5 * time.Second
			continue
		case error:
			c.session.logger.Debug("Control connection heartbeat failed.", NewLogFieldError("err", actualResp))
			goto reconn
		default:
			c.session.logger.Error("Unknown frame in response to options.", NewLogFieldString("frame_type", fmt.Sprintf("%T", resp)))
		}

	reconn:
		// try to connect a bit faster
		sleepTime = 1 * time.Second
		c.reconnect()
		continue
	}
}

var hostLookupPreferV4 = os.Getenv("GOCQL_HOST_LOOKUP_PREFER_V4") == "true"

func hostInfo(addr string, defaultPort int) ([]*HostInfo, error) {
	var port int
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
		port = defaultPort
	} else {
		port, err = strconv.Atoi(portStr)
		if err != nil {
			return nil, err
		}
	}

	var hosts []*HostInfo

	// Check if host is a literal IP address
	if ip := net.ParseIP(host); ip != nil {
		h, err := NewHostInfoFromAddrPort(ip, port)
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, h)
		return hosts, nil
	}

	// Look up host in DNS
	ips, err := LookupIP(host)
	if err != nil {
		return nil, err
	} else if len(ips) == 0 {
		return nil, fmt.Errorf("no IP's returned from DNS lookup for %q", addr)
	}

	// Filter to v4 addresses if any present
	if hostLookupPreferV4 {
		var preferredIPs []net.IP
		for _, v := range ips {
			if v4 := v.To4(); v4 != nil {
				preferredIPs = append(preferredIPs, v4)
			}
		}
		if len(preferredIPs) != 0 {
			ips = preferredIPs
		}
	}

	for _, ip := range ips {
		h, err := NewHostInfoFromAddrPort(ip, port)
		if err != nil {
			return nil, err
		}

		hosts = append(hosts, h)
	}

	return hosts, nil
}

func shuffleHosts(hosts []*HostInfo) []*HostInfo {
	shuffled := make([]*HostInfo, len(hosts))
	copy(shuffled, hosts)

	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	return shuffled
}

func (c *controlConn) discoverProtocol(hosts []*HostInfo) (int, error) {
	hosts = shuffleHosts(hosts)

	handler := connErrorHandlerFn(func(c *Conn, err error, closed bool) {
		// we should never get here, but if we do it means we connected to a
		// host successfully which means our attempted protocol version worked
		if !closed {
			c.Close()
		}
	})

	var err error
	var proto int
	for _, host := range hosts {
		proto, err = c.tryProtocolVersionsForHost(host, handler)
		if err == nil {
			return proto, nil
		}

		c.session.logger.Debug("Failed to discover protocol version for host.",
			NewLogFieldIP("host_addr", host.ConnectAddress()),
			NewLogFieldError("err", err))
	}

	return 0, err
}

func (c *controlConn) tryProtocolVersionsForHost(host *HostInfo, handler ConnErrorHandler) (int, error) {
	connCfg := *c.session.connCfg

	var triedVersions []int

	for proto := highestProtocolVersionSupported; proto >= lowestProtocolVersionSupported; proto-- {
		connCfg.ProtoVersion = proto

		conn, err := c.session.dial(c.session.ctx, host, &connCfg, handler)
		if conn != nil {
			conn.Close()
		}

		if err == nil {
			return proto, nil
		}

		var unsupportedErr *unsupportedProtocolVersionError
		if errors.As(err, &unsupportedErr) {
			// the host does not support this protocol version, try a lower version
			c.session.logger.Debug("Failed to connect to host during protocol negotiation.",
				NewLogFieldIP("host_addr", host.ConnectAddress()),
				NewLogFieldInt("proto_version", proto),
				NewLogFieldError("err", err))
			triedVersions = append(triedVersions, connCfg.ProtoVersion)
			continue
		}

		c.session.logger.Debug("Error connecting to host during protocol negotiation.",
			NewLogFieldIP("host_addr", host.ConnectAddress()),
			NewLogFieldError("err", err))
		return 0, err
	}

	return 0, fmt.Errorf("gocql: failed to discover protocol version for host %s, tried versions: %v", host.ConnectAddress(), triedVersions)
}

func (c *controlConn) connect(hosts []*HostInfo, sessionInit bool) error {
	if len(hosts) == 0 {
		return errors.New("control: no endpoints specified")
	}

	// shuffle endpoints so not all drivers will connect to the same initial
	// node.
	hosts = shuffleHosts(hosts)

	cfg := *c.session.connCfg
	cfg.disableCoalesce = true

	var conn *Conn
	var err error
	for _, host := range hosts {
		conn, err = c.setupCandidate(host, &cfg, sessionInit)
		if err == nil {
			break
		}
		conn = nil
	}
	if conn == nil {
		return fmt.Errorf("unable to connect to initial hosts: %w", err)
	}

	// we could fetch the initial ring here and update initial host data. So that
	// when we return from here we have a ring topology ready to go.

	go c.heartBeat()

	return nil
}

type connHost struct {
	conn *Conn
	host *HostInfo
}

// setupCandidate dials host with cfg for the initial connect and runs setupConn.
//
// It mirrors attemptReconnectToHost's ownership discipline for the init path: on
// any non-success exit, including a panic in setupConn or its logging, the
// candidate is closed and released before this returns, in that fixed order,
// under a deferred cleanup. NewSession calls Session.Close only when init returns
// an error, not when it panics, so without this a panicking init would leak the
// candidate.
//
// Parameters:
//   - host: the host to dial and set up
//   - cfg: the connection config to dial with (init disables coalescing)
//   - sessionInit: whether this is running during Session initialization
//
// Returns:
//   - *Conn: the connection when setup succeeded, already published
//   - error: the dial or setup failure, nil on success
func (c *controlConn) setupCandidate(host *HostInfo, cfg *ConnConfig, sessionInit bool) (*Conn, error) {
	conn, err := c.dialCandidate(host, cfg)
	if err != nil {
		c.session.logger.Info("Control connection failed to establish a connection to host.",
			NewLogFieldIP("host_addr", host.ConnectAddress()),
			NewLogFieldInt("port", host.Port()),
			NewLogFieldString("host_id", host.HostID()),
			NewLogFieldError("err", err))
		return nil, err
	}

	setupOK := false
	defer func() {
		if !setupOK {
			if c.session.cfg.testControlAfterSetupFailure != nil {
				c.session.cfg.testControlAfterSetupFailure()
			}
			conn.Close()
			c.releaseCandidate(conn)
		}
	}()

	if err = c.setupConn(conn, sessionInit); err == nil {
		setupOK = true
		return conn, nil
	}
	c.session.logger.Info("Control connection setup failed after connecting to host.",
		NewLogFieldIP("host_addr", host.ConnectAddress()),
		NewLogFieldInt("port", host.Port()),
		NewLogFieldString("host_id", host.HostID()),
		NewLogFieldError("err", err))
	return nil, err
}

func (c *controlConn) setupConn(conn *Conn, sessionInit bool) error {
	// we need up-to-date host info for the filterHost call below
	iter := conn.querySystemLocal(context.TODO())
	// The address and the port both come from the HostInfo this connection was dialled
	// with, so the pair published to the ring is the logical dial target: what the next
	// dial to this host is made with, and what a custom HostDialer is handed.
	// The socket's remote port is deliberately not read here.
	// It equals this one under the default dialer, which dials ConnectAddressAndPort,
	// but a HostDialer that redirects can make the two differ,
	// and pairing this address with that port yields an endpoint neither side ever named -
	// one that such a dialer is entitled to refuse.
	// ringDescriber.getLocalHostInfo keeps the same pair on every later refresh.
	host, err := c.session.hostInfoFromIter(iter, conn.host.ConnectAddress(), conn.host.Port())
	if err != nil {
		// just cleanup
		iter.Close()
		return fmt.Errorf("could not retrieve control host info: %w", err)
	}
	if host == nil {
		return errors.New("could not retrieve control host info: query returned 0 rows")
	}

	var exists bool
	host, exists = c.session.ring.addOrUpdate(host)

	if c.session.cfg.filterHost(host) {
		return fmt.Errorf("host was filtered: %v (%s)", host.ConnectAddress(), host.HostID())
	}

	if !exists {
		logLevel := LogLevelInfo
		msg := "Added control host."
		if sessionInit {
			logLevel = LogLevelDebug
			msg = "Added control host (session initialization)."
		}
		logHelper(c.session.logger, logLevel, msg,
			NewLogFieldIP("host_addr", host.ConnectAddress()), NewLogFieldString("host_id", host.HostID()))
	}

	if err := c.registerEvents(conn); err != nil {
		return fmt.Errorf("register events: %w", err)
	}

	ch := &connHost{
		conn: conn,
		host: host,
	}

	if c.session.cfg.testControlBeforePublish != nil {
		c.session.cfg.testControlBeforePublish(host)
	}

	if !c.publish(ch) {
		return errControlConnClosing
	}

	c.session.logger.Info("Control connection connected to host.",
		NewLogFieldIP("host_addr", host.ConnectAddress()), NewLogFieldString("host_id", host.HostID()))

	if c.session.initialized() {
		// Request the schema refresh; never wait for it.
		// This setup can be running on the schema flusher's own goroutine:
		// a schema refresh's control query that fails on write reaches HandleError,
		// and so reconnect, synchronously.
		// Waiting on the flusher from there is a deadlock that also leaves the
		// reconnecting claim held forever, and Session.Close then hangs in
		// schemaRefresher.stop.
		// A failed refresh is logged by Session.runSchemaRefresh.
		c.session.schemaDescriber.debounceRefreshSchemaMetadata()
		// We connected to control conn, so add the connect the host in pool as well.
		// Notify session we can start trying to connect to the node.
		// We can't start the fill before the session is initialized, otherwise the fill would interfere
		// with the fill called by Session.init. Session.init needs to wait for its fill to finish and that
		// would return immediately if we started the fill here.
		// TODO(martin-sucha): Trigger pool refill for all hosts, like in reconnectDownedHosts?
		go func() {
			defer recoverGoroutine(c.session.logger, "Session.startPoolFill", nil)
			c.session.startPoolFill(host)
		}()
	}
	return nil
}

func (c *controlConn) registerEvents(conn *Conn) error {
	var events []string

	if !c.session.cfg.Events.DisableTopologyEvents {
		events = append(events, "TOPOLOGY_CHANGE")
	}
	if !c.session.cfg.Events.DisableNodeStatusEvents {
		events = append(events, "STATUS_CHANGE")
	}
	if !c.session.cfg.Events.DisableSchemaEvents {
		events = append(events, "SCHEMA_CHANGE")
	}

	if len(events) == 0 {
		return nil
	}

	framer, err := conn.exec(context.Background(),
		&writeRegisterFrame{
			events: events,
		}, nil)
	if err != nil {
		return err
	}
	defer framer.release()

	frame, err := framer.parseFrame()
	if err != nil {
		return err
	} else if _, ok := frame.(*readyFrame); !ok {
		return fmt.Errorf("unexpected frame in response to register: got %T: %v", frame, frame)
	}

	return nil
}

func (c *controlConn) reconnect() {
	if c.closing() {
		return
	}
	if !atomic.CompareAndSwapInt32(&c.reconnecting, 0, 1) {
		return
	}
	defer func() {
		atomic.StoreInt32(&c.reconnecting, 0)
		if c.session.cfg.testControlReconnectDone != nil {
			c.session.cfg.testControlReconnectDone()
		}
	}()

	_, err := c.attemptReconnect()
	if err != nil {
		if !errors.Is(err, errControlConnClosing) {
			c.session.logger.Error("Unable to reconnect control connection.",
				NewLogFieldError("err", err))
		}
		return
	}

	// Request the ring refresh; never wait for it.
	// This reconnect can be running on the ring flusher's own goroutine:
	// a refresh's control query that fails on write reaches HandleError synchronously.
	// Waiting on the flusher from there is a deadlock,
	// and Session.Close then hangs in ringRefresher.stop.
	// The schema refresh that setupConn requests is debounced for the same reason.
	// The debounce, rather than an immediate trigger,
	// also paces a refresh whose own query keeps failing and reconnecting.
	// A failed refresh is logged by Session.runRingRefresh.
	c.session.debounceRingRefresh()
}

func (c *controlConn) attemptReconnect() (*Conn, error) {
	c.session.logger.Debug("Reconnecting the control connection.")

	hosts := c.session.ring.allHosts()
	hosts = shuffleHosts(hosts)

	// keep the old behavior of connecting to the old host first by moving it to
	// the front of the slice
	ch := c.getConn()
	if ch != nil {
		for i := range hosts {
			if hosts[i].Equal(ch.host) {
				hosts[0], hosts[i] = hosts[i], hosts[0]
				break
			}
		}
		ch.conn.Close()
	}

	conn, err := c.attemptReconnectToAnyOfHosts(hosts)

	if conn != nil {
		return conn, err
	}
	if errors.Is(err, errControlConnClosing) {
		return nil, err
	}

	if c.session.cfg.testControlBeforeFallback != nil {
		c.session.cfg.testControlBeforeFallback()
	}

	// A shutdown may have arrived after the loop returned an ordinary error and
	// before the fallback: do not admit the contact-point walk once closing.
	if c.closing() {
		return nil, errControlConnClosing
	}

	c.session.logger.Error("Unable to connect to any ring node, control connection falling back to initial contact points.", NewLogFieldError("err", err))
	// Fallback to initial contact points, as it may be the case that all known initialHosts
	// changed their IPs while keeping the same hostname(s).
	if c.session.cfg.testControlBeforeResolve != nil {
		c.session.cfg.testControlBeforeResolve()
	}
	initialHosts, resolvErr := addrsToHosts(c.session.cfg.Hosts, c.session.cfg.Port, c.session.logger)
	// A shutdown seen during the admitted resolution is expected: report the
	// sentinel rather than an ordinary resolution failure (which would also be
	// logged as an error by reconnect).
	if c.closing() {
		return nil, errControlConnClosing
	}
	if resolvErr != nil {
		return nil, fmt.Errorf("resolve contact points' hostnames: %w", resolvErr)
	}

	return c.attemptReconnectToAnyOfHosts(initialHosts)
}

func (c *controlConn) attemptReconnectToAnyOfHosts(hosts []*HostInfo) (*Conn, error) {
	var conn *Conn
	var err error
	for _, host := range hosts {
		if c.closing() {
			return nil, errControlConnClosing
		}
		conn, err = c.attemptReconnectToHost(host)
		if err == nil {
			return conn, nil
		}
		if errors.Is(err, errControlConnClosing) {
			return nil, err
		}
	}
	if c.closing() {
		return nil, errControlConnClosing
	}
	return conn, err
}

// attemptReconnectToHost dials host and runs setupConn on the connection.
//
// On any failure the candidate connection is closed and released before this
// returns, under a deferred cleanup so a panic in setupConn tears it down too;
// the caller can then move on to the next host with nothing left in flight.
// A shutdown observed during the attempt is reported as errControlConnClosing
// without convicting the host or logging the failure, since it is expected.
//
// Parameters:
//   - host: the host to dial and set up
//
// Returns:
//   - *Conn: the connection when setup succeeded, already published
//   - error: nil on success, errControlConnClosing on shutdown, else the failure
func (c *controlConn) attemptReconnectToHost(host *HostInfo) (*Conn, error) {
	conn, err := c.dialCandidate(host, c.session.connCfg)
	if err != nil {
		// A shutdown that raced the dial is expected, not a host to convict.
		if errors.Is(err, errControlConnClosing) || c.closing() {
			return nil, errControlConnClosing
		}
		c.convictOnDialFailure(host, err)
		c.session.logger.Info("During reconnection, control connection failed to establish a connection to host.",
			NewLogFieldIP("host_addr", host.ConnectAddress()),
			NewLogFieldInt("port", host.Port()),
			NewLogFieldString("host_id", host.HostID()),
			NewLogFieldError("err", err))
		return nil, err
	}

	// Close and release the candidate on every non-success exit, including a
	// setup panic, in that fixed order. A concurrent close that snapshotted this
	// candidate first calls Close again, which is idempotent.
	setupOK := false
	defer func() {
		if !setupOK {
			if c.session.cfg.testControlAfterSetupFailure != nil {
				c.session.cfg.testControlAfterSetupFailure()
			}
			conn.Close()
			c.releaseCandidate(conn)
		}
	}()

	if err = c.setupConn(conn, false); err == nil {
		setupOK = true
		return conn, nil
	}
	// A shutdown seen after a setup failure is expected: report the sentinel
	// rather than logging the failure.
	if errors.Is(err, errControlConnClosing) || c.closing() {
		return nil, errControlConnClosing
	}
	c.session.logger.Info("During reconnection, control connection setup failed after connecting to host.",
		NewLogFieldIP("host_addr", host.ConnectAddress()),
		NewLogFieldInt("port", host.Port()),
		NewLogFieldString("host_id", host.HostID()),
		NewLogFieldError("err", err))
	return nil, err
}

// convictOnDialFailure routes a control-connection dial failure through the
// ConvictionPolicy, but only for a host whose pool is already empty.
//
// A host that the control connection cannot dial and whose pool holds no
// connections either has nothing left to serve traffic, so it is marked DOWN
// and ReconnectInterval drives its recovery. A host whose pool still holds
// (or has since refilled) connections is left alone: tearing it down on a
// single failed control dial would take a working node out of rotation.
//
// Emptiness and ring identity are evaluated before AddFailure, so a stateful
// ConvictionPolicy is never consumed on a host this path would not act on.
//
// Parameters:
//   - host: the host the control connection failed to dial
//   - err: the dial error handed to the ConvictionPolicy
func (c *controlConn) convictOnDialFailure(host *HostInfo, err error) {
	if c.session.hostPoolEmpty(host) && c.session.cfg.ConvictionPolicy.AddFailure(err, host) {
		c.session.handleHostDown(host)
	}
}

func (c *controlConn) HandleError(conn *Conn, err error, closed bool) {
	if !closed {
		return
	}

	oldConn := c.getConn()

	// If connection has long gone, and not been attempted for awhile,
	// it's possible to have oldConn as nil here (#1297).
	if oldConn != nil && oldConn.conn != conn {
		return
	}

	c.session.logger.Warning("Control connection error.",
		NewLogFieldIP("host_addr", conn.host.ConnectAddress()),
		NewLogFieldString("host_id", conn.host.HostID()),
		NewLogFieldError("err", err))

	c.reconnect()
}

func (c *controlConn) getConn() *connHost {
	return c.conn.Load().(*connHost)
}

func (c *controlConn) writeFrame(w frameBuilder) (frame, error) {
	ch := c.getConn()
	if ch == nil {
		return nil, errNoControl
	}

	framer, err := ch.conn.exec(context.Background(), w, nil)
	if err != nil {
		return nil, err
	}
	defer framer.release()

	return framer.parseFrame()
}

// acquireConn returns the current control connection, reconnecting a bounded
// number of times while there is none.
//
// Returns:
//   - *connHost: the control connection current when it was found
//   - error: errNoControl when no connection could be established
func (c *controlConn) acquireConn() (*connHost, error) {
	const maxConnectAttempts = 5

	for i := 0; i < maxConnectAttempts; i++ {
		if ch := c.getConn(); ch != nil {
			return ch, nil
		}
		c.reconnect()
	}

	return nil, errNoControl
}

func (c *controlConn) withConnHost(fn func(*connHost) *Iter) *Iter {
	ch, err := c.acquireConn()
	if err != nil {
		return newErrIter(err, &queryMetrics{}, "", nil, nil)
	}
	return fn(ch)
}

func (c *controlConn) withConn(fn func(*Conn) *Iter) *Iter {
	return c.withConnHost(func(ch *connHost) *Iter {
		return fn(ch.conn)
	})
}

// query will return nil if the connection is closed or nil
func (c *controlConn) query(statement string, values ...interface{}) (iter *Iter) {
	q := c.session.Query(statement, values...).Consistency(One).RoutingKey([]byte{}).Trace(nil)
	qry := newInternalQuery(q, context.TODO())

	for {
		iter = c.withConn(func(conn *Conn) *Iter {
			qry.conn = conn
			return conn.executeQuery(qry.Context(), qry)
		})

		if iter.err != nil {
			c.session.logger.Warning("Error executing control connection statement.",
				NewLogFieldString("statement", statement), NewLogFieldError("err", iter.err))
		}

		qry.metrics.attempt(0)
		qry.hostMetricsManager.attempt(0, c.getConn().host)
		if iter.err == nil || !c.retry.Attempt(qry) {
			break
		}
	}

	return
}

func (c *controlConn) awaitSchemaAgreement() error {
	return c.withConn(func(conn *Conn) *Iter {
		return newErrIter(conn.awaitSchemaAgreement(c.session.ctx), &queryMetrics{}, "", nil, nil)
	}).err
}

func (c *controlConn) awaitSchemaAgreementWithTimeout(timeout time.Duration) error {
	return c.withConn(func(conn *Conn) *Iter {
		return newErrIter(conn.awaitSchemaAgreementWithTimeout(c.session.ctx, timeout), &queryMetrics{}, "", nil, nil)
	}).err
}

// closing reports whether the control connection is shutting down, either
// because close() has latched the terminal state or the session context is done.
//
// Returns:
//   - bool: true once shutdown has begun
func (c *controlConn) closing() bool {
	return atomic.LoadInt32(&c.state) == controlConnClosing || c.session.ctx.Err() != nil
}

// dialCandidate dials host with cfg and registers the connection as a control
// candidate, so a concurrent close can close it while it is still in flight.
//
// cfg is passed through rather than assumed, because the two dial sites differ:
// connect dials a copy of connCfg with coalescing disabled, and a reconnect
// dials connCfg itself. The connection is registered only if the controller is
// not yet closing; if it is, the connection is closed and errControlConnClosing
// is returned so the caller stops rather than proceeding to set it up.
//
// Parameters:
//   - host: the host to dial
//   - cfg: the connection config to dial with
//
// Returns:
//   - *Conn: the dialled connection, registered as a candidate, on success
//   - error: the dial error, or errControlConnClosing if the controller is closing
func (c *controlConn) dialCandidate(host *HostInfo, cfg *ConnConfig) (*Conn, error) {
	conn, err := c.session.dial(c.session.ctx, host, cfg, c)
	if err != nil {
		return nil, err
	}

	c.lifecycleMu.Lock()
	if atomic.LoadInt32(&c.state) == controlConnClosing {
		c.lifecycleMu.Unlock()
		conn.Close()
		return nil, errControlConnClosing
	}
	c.candidates[conn] = struct{}{}
	c.lifecycleMu.Unlock()
	return conn, nil
}

// releaseCandidate drops conn from the candidate set once its caller has closed it.
//
// Parameters:
//   - conn: the candidate to drop
func (c *controlConn) releaseCandidate(conn *Conn) {
	c.lifecycleMu.Lock()
	delete(c.candidates, conn)
	c.lifecycleMu.Unlock()
}

// publish makes ch the current control connection, moving it from the candidate
// set to published in one step under lifecycleMu, unless the controller is
// closing.
//
// Publishing under the same mutex close() latches on closes the window in which
// a connection is neither a candidate nor published: a close either latches
// before this (and this returns false so the caller closes ch) or after (and
// close() snapshots ch as the published connection and closes it).
//
// Parameters:
//   - ch: the connection and its host to publish
//
// Returns:
//   - bool: true if published; false if the controller is closing
func (c *controlConn) publish(ch *connHost) bool {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if atomic.LoadInt32(&c.state) == controlConnClosing {
		return false
	}
	delete(c.candidates, ch.conn)
	c.conn.Store(ch)
	return true
}

// latchAndSnapshot moves the control connection into the terminal closing state
// and returns what has to be closed: the published connection and every
// candidate still in flight.
//
// It only latches and reads memory, so it never blocks on application code and
// can run before Session.Close cancels the session context. The physical
// Conn.Close calls are left to closeSnapshot, after the cancel.
//
// Returns:
//   - controlSnapshot: the published connection and the in-flight candidates
func (c *controlConn) latchAndSnapshot() controlSnapshot {
	c.lifecycleMu.Lock()
	prev := atomic.SwapInt32(&c.state, controlConnClosing)
	snap := controlSnapshot{published: c.getConn()}
	for conn := range c.candidates {
		snap.candidates = append(snap.candidates, conn)
	}
	c.candidates = map[*Conn]struct{}{}
	c.lifecycleMu.Unlock()

	// Only the first latch signals the heartbeat; quit is buffered 1.
	if prev != controlConnClosing {
		c.quit <- struct{}{}
	}
	return snap
}

// closeSnapshot closes the connections latchAndSnapshot returned.
//
// Conn.Close may synchronously reach the application logger through HandleError,
// so this runs outside lifecycleMu, and Session.Close runs it after cancelling
// the session context.
//
// Parameters:
//   - snap: the published connection and candidates to close
func (c *controlConn) closeSnapshot(snap controlSnapshot) {
	if snap.published != nil {
		snap.published.conn.Close()
	}
	for _, conn := range snap.candidates {
		conn.Close()
	}
}

// close latches the terminal state and closes the control connection and every
// candidate still in flight. Session.Close instead calls latchAndSnapshot and
// closeSnapshot around its context cancel; every other caller uses this.
func (c *controlConn) close() {
	c.closeSnapshot(c.latchAndSnapshot())
}

var errNoControl = errors.New("gocql: no control connection available")
