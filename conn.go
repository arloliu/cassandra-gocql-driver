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
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand/v2"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maypok86/otter/v2"

	"github.com/apache/cassandra-gocql-driver/v2/internal/streams"
)

// approve the authenticator with the list of allowed authenticators. If the provided list is empty,
// the given authenticator is allowed.
func approve(authenticator string, approvedAuthenticators []string) bool {
	if len(approvedAuthenticators) == 0 {
		return true
	}
	for _, s := range approvedAuthenticators {
		if authenticator == s {
			return true
		}
	}
	return false
}

// JoinHostPort is a utility to return an address string that can be used
// by `gocql.Conn` to form a connection with a host.
func JoinHostPort(addr string, port int) string {
	addr = strings.TrimSpace(addr)
	if _, _, err := net.SplitHostPort(addr); err != nil {
		addr = net.JoinHostPort(addr, strconv.Itoa(port))
	}
	return addr
}

// Authenticator handles authentication challenges and responses during connection setup.
type Authenticator interface {
	Challenge(req []byte) (resp []byte, auth Authenticator, err error)
	Success(data []byte) error
}

// PasswordAuthenticator specifies credentials to be used when authenticating.
// It can be configured with an "allow list" of authenticator class names to avoid
// attempting to authenticate with Cassandra if it doesn't provide an expected authenticator.
type PasswordAuthenticator struct {
	Username string
	Password string
	// Setting this to nil or empty will allow authenticating with any authenticator
	// provided by the server.  This is the default behavior of most other driver
	// implementations.
	AllowedAuthenticators []string
}

func (p PasswordAuthenticator) Challenge(req []byte) ([]byte, Authenticator, error) {
	if !approve(string(req), p.AllowedAuthenticators) {
		return nil, nil, fmt.Errorf("unexpected authenticator %q", req)
	}
	resp := make([]byte, 2+len(p.Username)+len(p.Password))
	resp[0] = 0
	copy(resp[1:], p.Username)
	resp[len(p.Username)+1] = 0
	copy(resp[2+len(p.Username):], p.Password)
	return resp, nil, nil
}

func (p PasswordAuthenticator) Success(data []byte) error {
	return nil
}

// SslOptions configures TLS use.
//
// Warning: Due to historical reasons, the SslOptions is insecure by default, so you need to set EnableHostVerification
// to true if no Config is set. Most users should set SslOptions.Config to a *tls.Config.
// SslOptions and Config.InsecureSkipVerify interact as follows:
//
//	Config.InsecureSkipVerify | EnableHostVerification | Result
//	Config is nil             | false                  | do not verify host
//	Config is nil             | true                   | verify host
//	false                     | false                  | verify host
//	true                      | false                  | do not verify host
//	false                     | true                   | verify host
//	true                      | true                   | verify host
type SslOptions struct {
	*tls.Config

	// CertPath and KeyPath are optional depending on server
	// config, but both fields must be omitted to avoid using a
	// client certificate
	CertPath string
	KeyPath  string
	CaPath   string // optional depending on server config
	// If you want to verify the hostname and server cert (like a wildcard for cass cluster) then you should turn this
	// on.
	// This option is basically the inverse of tls.Config.InsecureSkipVerify.
	// See InsecureSkipVerify in http://golang.org/pkg/crypto/tls/ for more info.
	//
	// See SslOptions documentation to see how EnableHostVerification interacts with the provided tls.Config.
	EnableHostVerification bool
}

// ConnConfig contains configuration options for establishing connections to Cassandra nodes.
type ConnConfig struct {
	ProtoVersion   int
	MaxStreams     int
	CQLVersion     string
	Timeout        time.Duration
	WriteTimeout   time.Duration
	ConnectTimeout time.Duration
	Dialer         Dialer
	HostDialer     HostDialer
	Compressor     Compressor
	Authenticator  Authenticator
	AuthProvider   func(h *HostInfo) (Authenticator, error)
	Keepalive      time.Duration
	Logger         StructuredLogger

	tlsConfig       *tls.Config
	disableCoalesce bool

	// heartbeatInterval is the start-to-start spacing of heartbeat OPTIONS
	// frames; connConfig resolves a zero ClusterConfig value to the
	// heartbeatInterval constant.
	heartbeatInterval time.Duration
	// heartbeatPhase returns the wait before a new connection's first
	// heartbeat; connConfig resolves nil to defaultHeartbeatPhase.
	heartbeatPhase func(interval time.Duration) time.Duration
	// heartbeatTimeout bounds each heartbeat OPTIONS round-trip; connConfig
	// resolves a zero or negative ClusterConfig value to
	// heartbeatDefaultTimeout.
	heartbeatTimeout time.Duration
}

// ConnErrorHandler handles connection errors and state changes for connections.
type ConnErrorHandler interface {
	HandleError(conn *Conn, err error, closed bool)
}

type connErrorHandlerFn func(conn *Conn, err error, closed bool)

func (fn connErrorHandlerFn) HandleError(conn *Conn, err error, closed bool) {
	fn(conn, err, closed)
}

// Conn is a single connection to a Cassandra node. It can be used to execute
// queries, but users are usually advised to use a more reliable, higher
// level API.
type Conn struct {
	r ConnReader
	w contextWriter

	writeTimeout time.Duration
	// requestTimeout bounds request round-trips: the execInternal call timer
	// and the startup handshake. It is deliberately decoupled
	// from the connReader read deadline. Idle frame/segment reads disable the
	// read deadline (SetTimeout(0)) so an idle connection does not trip it and
	// reconnect; that transient zero must not disarm request timers (CASSGO-125).
	// Stored as atomic.Int64 (nanoseconds) because it is written at init and by
	// the integration suite on live connections, and read by the request
	// goroutines — the same access pattern as connReader.timeout.
	requestTimeout atomic.Int64
	cfg            *ConnConfig
	frameObserver  FrameHeaderObserver
	streamObserver StreamObserver

	headerBuf [frameHeadSize]byte

	streams *streams.IDGenerator
	mu      sync.Mutex
	// calls stores a mapping from stream ID to callReq.
	//
	// This is on the hot path for every request/response. It uses a sharded map
	// to reduce lock contention without allocating a dense table sized to max streams.
	calls *callMap

	errorHandler ConnErrorHandler
	compressor   Compressor
	auth         Authenticator
	addr         string

	version         uint8
	currentKeyspace string
	host            *HostInfo
	isSchemaV2      bool

	session *Session

	// true if connection close process for the connection started.
	// closed is protected by mu.
	closed bool
	ctx    context.Context
	cancel context.CancelFunc

	logger StructuredLogger

	// Per-instance test hooks for goroutine panic-recovery tests. Set by
	// tests to a panicking func to exercise the corresponding recover
	// wrapper. Default nil (no-op).
	testServePanicAt       func()
	testHeartBeatPanicAt   func()
	testStartupRecvPanicAt func()
	testStartupSendPanicAt func()
}

// connect establishes a connection to a Cassandra node using session's connection config.
func (s *Session) connect(ctx context.Context, host *HostInfo, errorHandler ConnErrorHandler) (*Conn, error) {
	return s.dial(ctx, host, s.connCfg, errorHandler)
}

// dial establishes a connection to a Cassandra node and notifies the session's connectObserver.
func (s *Session) dial(ctx context.Context, host *HostInfo, connConfig *ConnConfig, errorHandler ConnErrorHandler) (*Conn, error) {
	var obs ObservedConnect
	if s.connectObserver != nil {
		obs.Host = host
		obs.Start = time.Now()
	}

	conn, err := s.dialWithoutObserver(ctx, host, connConfig, errorHandler)

	if s.connectObserver != nil {
		obs.End = time.Now()
		obs.Err = err
		s.connectObserver.ObserveConnect(obs)
	}

	return conn, err
}

// dialWithoutObserver establishes connection to a Cassandra node.
//
// dialWithoutObserver does not notify the connection observer, so you most probably want to call dial() instead.
func (s *Session) dialWithoutObserver(ctx context.Context, host *HostInfo, cfg *ConnConfig, errorHandler ConnErrorHandler) (*Conn, error) {
	dialedHost, err := cfg.HostDialer.DialHost(ctx, host)
	if err != nil {
		return nil, err
	}

	writeTimeout := cfg.Timeout
	if cfg.WriteTimeout > 0 {
		writeTimeout = cfg.WriteTimeout
	}

	logger := cfg.Logger
	if logger == nil {
		logger = s.logger
		if logger == nil {
			logger = &defaultLogger{}
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	streamGen := streams.New(cfg.ProtoVersion, cfg.MaxStreams)
	c := &Conn{
		r: &connReader{
			conn: dialedHost.Conn,
			r:    bufio.NewReader(dialedHost.Conn),
		},
		cfg:           cfg,
		calls:         newCallMap(streamGen.NumStreams),
		version:       uint8(cfg.ProtoVersion),
		addr:          dialedHost.Conn.RemoteAddr().String(),
		errorHandler:  errorHandler,
		compressor:    cfg.Compressor,
		session:       s,
		streams:       streamGen,
		host:          host,
		isSchemaV2:    true, // Try using "system.peers_v2" until proven otherwise
		frameObserver: s.frameObserver,
		w: &deadlineContextWriter{
			w:         dialedHost.Conn,
			timeout:   writeTimeout,
			semaphore: make(chan struct{}, 1),
			quit:      make(chan struct{}),
		},
		ctx:            ctx,
		cancel:         cancel,
		logger:         logger,
		streamObserver: s.streamObserver,
		writeTimeout:   writeTimeout,
	}

	if err := c.init(ctx, dialedHost); err != nil {
		cancel()
		c.Close()
		return nil, err
	}

	return c, nil
}

func (c *Conn) init(ctx context.Context, dialedHost *DialedHost) error {
	if c.session.cfg.AuthProvider != nil {
		var err error
		c.auth, err = c.cfg.AuthProvider(c.host)
		if err != nil {
			return err
		}
	} else {
		c.auth = c.cfg.Authenticator
	}

	startup := &startupCoordinator{
		frameTicker: make(chan struct{}),
		conn:        c,
	}

	c.r.SetTimeout(c.cfg.ConnectTimeout)
	c.requestTimeout.Store(int64(c.cfg.ConnectTimeout))
	if err := startup.setupConn(ctx); err != nil {
		return err
	}

	c.r.SetTimeout(c.cfg.Timeout)
	c.requestTimeout.Store(int64(c.cfg.Timeout))

	// dont coalesce startup frames
	if c.session.cfg.WriteCoalesceWaitTime > 0 && !c.cfg.disableCoalesce && !dialedHost.DisableCoalesce {
		c.w = newWriteCoalescer(dialedHost.Conn, c.writeTimeout, c.session.cfg.WriteCoalesceWaitTime,
			ctx.Done(), c.logger,
			func(err error, pending []chan<- writeResult) {
				// Drain pending writeContext callers with the panic error
				// so they unblock immediately. Result chans are buffer-1;
				// flush() may already have sent to some (then resultChans
				// is reset to nil — see writeFlusherImpl). Use non-blocking
				// sends so a full chan does not hang recovery.
				for _, ch := range pending {
					select {
					case ch <- writeResult{err: err}:
					default:
					}
				}
				c.closeWithError(err)
			})
	}

	go c.serve(ctx)
	go c.heartBeat(ctx)

	return nil
}

func (c *Conn) Write(p []byte) (n int, err error) {
	return c.w.writeContext(context.Background(), p)
}

type startupCoordinator struct {
	conn        *Conn
	frameTicker chan struct{}
}

func (s *startupCoordinator) setupConn(ctx context.Context) error {
	var cancel context.CancelFunc
	if requestTimeout := time.Duration(s.conn.requestTimeout.Load()); requestTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, requestTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	// Only for proto v5+.
	// Indicates if STARTUP has been completed.
	// github.com/apache/cassandra/blob/trunk/doc/native_protocol_v5.spec
	// 2.3.1 Initial Handshake
	// 	In order to support both v5 and earlier formats, the v5 framing format is not
	//  applied to message exchanges before an initial handshake is completed.
	startupCompleted := &atomic.Bool{}
	startupCompleted.Store(false)

	startupErr := make(chan error)
	go func() {
		// Push panic-as-error into startupErr so setupConn unblocks
		// immediately instead of waiting until ctx timeout. The select
		// with <-ctx.Done() handles the case where setupConn has already
		// moved on (likely because it observed an error from the other
		// goroutine first).
		defer recoverGoroutine(s.conn.logger, "Conn.startup.recvPump", func(err error) {
			select {
			case startupErr <- err:
			case <-ctx.Done():
			}
		})

		if s.conn.testStartupRecvPanicAt != nil {
			s.conn.testStartupRecvPanicAt()
		}

		for range s.frameTicker {
			err := s.conn.recv(ctx, startupCompleted.Load())
			if err != nil {
				select {
				case startupErr <- err:
				case <-ctx.Done():
				}

				return
			}
		}
	}()

	go func() {
		defer close(s.frameTicker)
		defer recoverGoroutine(s.conn.logger, "Conn.startup.optionsSender", func(err error) {
			select {
			case startupErr <- err:
			case <-ctx.Done():
			}
		})

		if s.conn.testStartupSendPanicAt != nil {
			s.conn.testStartupSendPanicAt()
		}

		err := s.options(ctx, startupCompleted)
		select {
		case startupErr <- err:
		case <-ctx.Done():
		}
	}()

	select {
	case err := <-startupErr:
		if err != nil {
			if s.checkProtocolRelatedError(err) {
				return &unsupportedProtocolVersionError{
					err:      err,
					hostInfo: s.conn.host,
					version:  protoVersion(s.conn.version),
				}
			}
			return err
		}
	case <-ctx.Done():
		return errors.New("gocql: no response to connection startup within timeout")
	}

	return nil
}

// Checks if the error is protocol related and should be retried during startup.
// It returns the frame that caused the error and whether the error should be retried.
func (s *startupCoordinator) checkProtocolRelatedError(err error) bool {
	var unwrappedFrame frame

	var protocolErr *protocolError
	if !errors.As(err, &protocolErr) {
		var errFrame errorFrame
		if !errors.As(err, &errFrame) {
			return false
		} else {
			unwrappedFrame = errFrame
		}
	} else {
		unwrappedFrame = protocolErr.frame
	}

	switch frame := unwrappedFrame.(type) {
	case *supportedFrame:
		// We can receive a supportedFrame wrapped in protocolError from Conn.recv if the host responds to a 0 stream id.
		// If we receive a supportedFrame then we know that the host is not compatible with the protocol version, but it is reachable, so we can retry
		return true
	case errorFrame:
		// If we receive an errorFrame with codes ErrCodeProtocol or ErrCodeServer,
		// then we should try to downgrade a protocol version, so do not skip the host
		return frame.code == ErrCodeProtocol || frame.code == ErrCodeServer
	default:
		// In any other case we should not retry as it means the host is not reachable or some other error happened
		return false
	}
}

func (s *startupCoordinator) write(ctx context.Context, frame frameBuilder, startupCompleted *atomic.Bool) (frame, error) {
	select {
	case s.frameTicker <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	framer, err := s.conn.execInternal(ctx, frame, nil, startupCompleted.Load())
	if err != nil {
		return nil, err
	}
	defer framer.release()

	resp, err := framer.parseFrame()
	if err != nil {
		return nil, err
	}

	// Startup/auth responses must not retain references to pooled framer buffers.
	switch v := resp.(type) {
	case *authChallengeFrame:
		v.data = copyBytes(v.data)
	case *authSuccessFrame:
		v.data = copyBytes(v.data)
	}

	return resp, nil
}

func (s *startupCoordinator) options(ctx context.Context, startupCompleted *atomic.Bool) error {
	frame, err := s.write(ctx, &writeOptionsFrame{}, startupCompleted)
	if err != nil {
		return err
	}

	switch frame := frame.(type) {
	case *supportedFrame:
		return s.startup(ctx, frame.supported, startupCompleted)
	case error:
		return frame
	default:
		return NewErrProtocol("Unknown type of response to startup frame: %T (frame=%s)", frame, frame.String())
	}
}

// chooseCompression selects the STARTUP COMPRESSION option for a compressor
// named name from the server-advertised list, honoring the negotiated protocol
// version. On native protocol v5+, snappy is treated as unavailable regardless
// of what the server advertises: v5 moved compression to the checksummed framing
// layer, which only supports lz4, but Cassandra still lists snappy in SUPPORTED.
// It returns ("", false) when no compatible algorithm is available, signaling
// the caller to disable compression.
func chooseCompression(version byte, name string, supported []string) (string, bool) {
	if version >= protoVersion5 && name == "snappy" {
		return "", false
	}
	for _, c := range supported {
		if c == name {
			return c, true
		}
	}
	return "", false
}

func (s *startupCoordinator) startup(ctx context.Context, supported map[string][]string, startupCompleted *atomic.Bool) error {
	m := map[string]string{
		"CQL_VERSION":    s.conn.cfg.CQLVersion,
		"DRIVER_NAME":    driverName,
		"DRIVER_VERSION": driverVersion,
	}

	if s.conn.compressor != nil {
		name := s.conn.compressor.Name()
		if chosen, ok := chooseCompression(s.conn.version, name, supported["COMPRESSION"]); ok {
			m["COMPRESSION"] = chosen
		} else {
			// Snappy was removed in native protocol v5+ (segment-layer
			// compression is lz4-only), yet Cassandra still advertises it in
			// SUPPORTED. Warn and connect without compression rather than send
			// it and hit a confusing STARTUP rejection; matches the reference
			// drivers. Any other unsupported compressor is disabled silently, as
			// before.
			if s.conn.version >= protoVersion5 && name == "snappy" {
				s.conn.logger.Warning("Snappy compression is not supported on native protocol v5+; connecting without compression (use lz4).",
					NewLogFieldString("address", s.conn.addr),
					NewLogFieldString("compressor", name))
			}
			s.conn.compressor = nil
		}
	}

	frame, err := s.write(ctx, &writeStartupFrame{opts: m}, startupCompleted)
	if err != nil {
		return err
	}

	switch v := frame.(type) {
	case error:
		return v
	case *readyFrame:
		// Startup is successfully completed, so we could use Native Protocol 5
		startupCompleted.Store(true)
		return nil
	case *authenticateFrame:
		// Startup is successfully completed, so we could use Native Protocol 5
		startupCompleted.Store(true)
		return s.authenticateHandshake(ctx, v, startupCompleted)
	default:
		return NewErrProtocol("Unknown type of response to startup frame: %s", v)
	}
}

func (s *startupCoordinator) authenticateHandshake(ctx context.Context, authFrame *authenticateFrame, startupCompleted *atomic.Bool) error {
	if s.conn.auth == nil {
		return fmt.Errorf("authentication required (using %q)", authFrame.class)
	}

	resp, challenger, err := s.conn.auth.Challenge([]byte(authFrame.class))
	if err != nil {
		return err
	}

	req := &writeAuthResponseFrame{data: resp}
	for {
		frame, err := s.write(ctx, req, startupCompleted)
		if err != nil {
			return err
		}

		switch v := frame.(type) {
		case error:
			return v
		case *authSuccessFrame:
			if challenger != nil {
				return challenger.Success(v.data)
			}
			return nil
		case *authChallengeFrame:
			resp, challenger, err = challenger.Challenge(v.data)
			if err != nil {
				return err
			}

			req = &writeAuthResponseFrame{
				data: resp,
			}
		default:
			return fmt.Errorf("unknown frame response during authentication: %v", v)
		}
	}
}

func (c *Conn) closeWithError(err error) {
	if c == nil {
		return
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.mu.Unlock()

	// Snapshot call handlers so we don't hold any shard locks while sending.
	// The snapshot is taken even for a nil error: every outstanding call is owed
	// a terminal observer event, and Conn.Close() closes with a nil error, so
	// gating the snapshot on err != nil left those calls with no event at all.
	var callsToClose []*callReq
	if c.calls != nil {
		callsToClose = c.calls.snapshot()
	}

	for _, req := range callsToClose {
		// Only an actual error is worth delivering. A nil-error notification would
		// reach the caller's success branch with no framer behind it.
		if err != nil {
			// we need to send the error to all waiting queries.
			select {
			case req.resp <- callResp{err: err, closing: true}:
			case <-req.timeout:
			}
		}
		c.notifyStreamEnd(req, streamAbandoned)
	}

	// if error was nil then unblock the quit channel
	c.cancel()
	cerr := c.r.Close()
	if err != nil && c.calls != nil {
		c.calls.clear()
	}

	if err != nil {
		c.errorHandler.HandleError(c, err, true)
	} else if cerr != nil {
		// TODO(zariel): is it a good idea to do this?
		c.errorHandler.HandleError(c, cerr, true)
	}
}

func (c *Conn) Close() {
	c.closeWithError(nil)
}

// Serve starts the stream multiplexer for this connection, which is required
// to execute any queries. This method runs as long as the connection is
// open and is therefore usually called in a separate goroutine.
func (c *Conn) serve(ctx context.Context) {
	defer recoverGoroutine(c.logger, "Conn.serve", func(err error) {
		c.closeWithError(err)
	})

	if c.testServePanicAt != nil {
		c.testServePanicAt()
	}

	var err error
	for err == nil {
		err = c.recv(ctx, true)
	}

	c.closeWithError(err)
}

func (c *Conn) discardFrame(r io.Reader, head frameHeader) error {
	_, err := io.CopyN(ioutil.Discard, r, int64(head.length))
	if err != nil {
		return err
	}
	return nil
}

type protocolError struct {
	frame frame
}

func (p *protocolError) Error() string {
	if err, ok := p.frame.(error); ok {
		return err.Error()
	}
	return fmt.Sprintf("gocql: received unexpected frame on stream %d: %v", p.frame.Header().stream, p.frame)
}

// heartbeatInterval is the steady-state, start-to-start spacing of heartbeat OPTIONS frames on every connection,
// independent of the connection's age and of whether the previous heartbeat succeeded.
// With the failure threshold in Conn.heartBeat, a connection that stops being
// answered is closed within delta + 5*max(interval, T) + T, where T is the
// per-attempt heartbeat timeout and delta is the wait until the connection's
// next heartbeat, in (0, interval]. A connection whose very first heartbeat
// fails starts from a phase in [interval/2, interval) instead.
const heartbeatInterval = 5 * time.Second

// heartbeatDefaultTimeout is the per-OPTIONS heartbeat exec timeout selected by
// a zero (or negative) ClusterConfig.HeartbeatTimeout. It matches the
// steady-state heartbeat interval.
//
// It used to be a floor applied to Session.Timeout: a sub-second Session.Timeout
// (common for low-latency reads) capped every heartbeat at the same value, so a
// GC pause, TCP retransmit or brief coordinator hiccup could trip the failure
// threshold and close otherwise-healthy connections (upstream #1919). The
// heartbeat no longer derives its timeout from Session.Timeout at all, so the
// floor is gone; a configured HeartbeatTimeout is used as given, and its risks
// are documented on the field rather than corrected here.
const heartbeatDefaultTimeout = 5 * time.Second

// resolveHeartbeatTimeout returns the per-attempt heartbeat OPTIONS timeout for
// a configured ClusterConfig.HeartbeatTimeout.
//
// Zero or negative selects heartbeatDefaultTimeout; every positive value is
// returned as given, including one below the heartbeat interval. A negative
// duration would otherwise produce an already-expired context and fail every
// heartbeat at once, so it is treated as unset rather than honoured.
//
// Parameters:
//   - configured: the value from ClusterConfig.HeartbeatTimeout
//
// Returns:
//   - time.Duration: the effective per-attempt timeout, always positive
func resolveHeartbeatTimeout(configured time.Duration) time.Duration {
	if configured <= 0 {
		return heartbeatDefaultTimeout
	}
	return configured
}

// defaultHeartbeatPhase returns a random first heartbeat wait in [interval/2, interval).
//
// Connections created in the same instant (a pool fill) spread their heartbeats over half an interval
// instead of probing, and failing, in lockstep.
//
// Parameters:
//   - interval: The steady-state heartbeat interval; must be positive.
//
// Returns:
//   - time.Duration: The wait before the connection's first heartbeat.
func defaultHeartbeatPhase(interval time.Duration) time.Duration {
	return interval/2 + rand.N(interval/2)
}

// heartBeat sends OPTIONS frames on a fixed start-to-start cadence
// and closes the connection after six consecutive send failures.
//
// The first wait is the per-connection phase from cfg.heartbeatPhase;
// every later wait is cfg.heartbeatInterval,
// re-armed when the previous wait fires and before the OPTIONS round-trip,
// so a slow or failing OPTIONS consumes the interval rather than extending it.
// The cadence never depends on whether a heartbeat succeeded,
// so a dead connection is detected in the same time whether it was created a moment ago or long before.
//
// Parameters:
//   - ctx: Cancelled when the connection closes; ends the loop.
func (c *Conn) heartBeat(ctx context.Context) {
	defer recoverGoroutine(c.logger, "Conn.heartBeat", func(err error) {
		c.closeWithError(err)
	})

	if c.testHeartBeatPanicAt != nil {
		c.testHeartBeatPanicAt()
	}

	interval := c.cfg.heartbeatInterval
	timer := time.NewTimer(c.cfg.heartbeatPhase(interval))
	defer timer.Stop()

	var failures int

	for {
		if failures > 5 {
			c.closeWithError(fmt.Errorf("gocql: heartbeat failed"))
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		// Start-to-start: the timer just fired and was drained,
		// so Reset arms the next wait before the OPTIONS round-trip begins.
		// The phase is used for the first wait only and is never re-applied.
		timer.Reset(interval)

		hbCtx, cancel := context.WithTimeout(ctx, c.cfg.heartbeatTimeout)
		framer, err := c.exec(hbCtx, &writeOptionsFrame{}, nil)
		cancel()
		if err != nil {
			// If the parent ctx was cancelled while c.exec was in flight, the
			// error is shutdown noise rather than evidence of a sick connection;
			// don't count it toward the failure threshold.
			if ctx.Err() != nil {
				return
			}
			// c.exec failures are write/network errors. These DO indicate
			// the connection may be unhealthy; count toward the threshold.
			failures++
			continue
		}

		resp, err := framer.parseFrame()
		if err != nil {
			framer.release()
			// Parse errors here mean we read a complete frame off the wire
			// but failed to decode it (corrupt body, protocol mismatch,
			// etc.). These are far more likely transient than indicative
			// of a dead connection — the wire round-trip itself succeeded.
			// Log at Debug and DO NOT count toward the failure threshold;
			// otherwise 5 transient parse glitches in 25s would
			// unnecessarily flap the connection.
			c.logger.Debug("Heartbeat parse error ignored (transient).",
				NewLogFieldString("host_id", c.host.HostID()),
				NewLogFieldError("err", err))
			continue
		}
		framer.release()

		switch r := resp.(type) {
		case *supportedFrame:
			// Everything ok
			failures = 0
		case error:
			// Server replied with an error frame to OPTIONS. Operationally
			// relevant (e.g. server-side problem, malformed request) but
			// not necessarily a sign that the connection itself is dead.
			// Log and continue without incrementing failures, same as the
			// parse-error path above.
			c.logger.Debug("Heartbeat received error response.",
				NewLogFieldString("host_id", c.host.HostID()),
				NewLogFieldError("err", r))
		default:
			panic(fmt.Sprintf("gocql: unknown frame in response to options: %T", resp))
		}
	}
}

func (c *Conn) recv(ctx context.Context, startupCompleted bool) error {
	// If startup is completed and native proto 5+ is set up then we should
	// unwrap payload from compressed/uncompressed frame
	if startupCompleted && c.version > protoVersion4 {
		return c.recvSegment(ctx)
	}

	return c.processFrame(ctx, c.r)
}

func (c *Conn) processFrame(ctx context.Context, r io.Reader) error {
	// not safe for concurrent reads

	// Read the header without a read deadline: this loops waiting for the next
	// response, so an idle connection must not trip the deadline here (CASSGO-125).
	// Setting the connReader timeout to 0 (rather than clearing the deadline
	// directly) is what actually disarms it — connReader.Read re-arms the deadline
	// on every read while its timeout is non-zero. The timeout is restored for the
	// body read below.
	// TODO: TCP level deadlines? or just query level deadlines?
	readTimeout := c.r.GetTimeout()
	if readTimeout > 0 {
		c.r.SetTimeout(0)
	}

	headStartTime := time.Now()
	// were just reading headers over and over and copy bodies
	head, err := readHeader(r, c.headerBuf[:])
	headEndTime := time.Now()
	if err != nil {
		return err
	}

	// Restore the read timeout for the frame body, which is actively arriving
	// once its header has been read and should be bounded.
	if readTimeout > 0 {
		c.r.SetTimeout(readTimeout)
	}

	if c.frameObserver != nil {
		c.frameObserver.ObserveFrameHeader(context.Background(), ObservedFrameHeader{
			Version: protoVersion(head.version),
			Flags:   head.flags,
			Stream:  int16(head.stream),
			Opcode:  frameOp(head.op),
			Length:  int32(head.length),
			Start:   headStartTime,
			End:     headEndTime,
			Host:    c.host,
		})
	}

	if head.stream >= c.streams.NumStreams {
		return fmt.Errorf("gocql: frame header stream is beyond call expected bounds: %d", head.stream)
	} else if head.stream == -1 {
		// TODO: handle cassandra event frames, we shouldnt get any currently
		framer := getFramer(c.compressor, c.version, c.session.types)
		if err := framer.readFrame(r, &head); err != nil {
			framer.release()
			return err
		}
		go func() {
			defer recoverGoroutine(c.logger, "Session.handleEvent", nil)
			c.session.handleEvent(framer)
		}()
		return nil
	} else if head.stream <= 0 {
		// reserved stream that we dont use, probably due to a protocol error
		// or a bug in Cassandra, this should be an error, parse it and return.
		framer := getFramer(c.compressor, c.version, c.session.types)
		if err := framer.readFrame(r, &head); err != nil {
			framer.release()
			return err
		}

		frame, err := framer.parseFrame()
		framer.release() // Return to pool after parsing
		if err != nil {
			return err
		}

		return &protocolError{
			frame: frame,
		}
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrConnectionClosed
	}
	c.mu.Unlock()
	call, ok := c.calls.loadAndDelete(head.stream)
	if call == nil || !ok {
		c.logger.Warning("Received response for stream which has no handler.", NewLogFieldString("header", head.String()))
		return c.discardFrame(r, head)
	} else if head.stream != call.streamID {
		panic(fmt.Sprintf("call has incorrect streamID: got %d expected %d", call.streamID, head.stream))
	}

	framer := getFramer(c.compressor, c.version, c.session.types)

	err = framer.readFrame(r, &head)
	if err != nil {
		// Close the connection only when the frame boundary is gone. That is a
		// property of how much of the frame was consumed, which readFrame reports
		// with frameReadError -- not something the error's dynamic type can be
		// asked about, since a compressor may return anything it likes for a body
		// that was read in full.
		//
		// This call has already left the call map, so closeWithError's snapshot
		// will not find it and nobody else would report it to the observer.
		var desync *frameReadError
		if errors.As(err, &desync) {
			framer.release()
			c.notifyStreamEnd(call, streamAbandoned)
			return err
		}
	}

	// we either, return a response to the caller, the caller timedout, or the
	// connection has closed. Either way we should never block indefinatly here
	select {
	case call.resp <- callResp{framer: framer, err: err}:
		// The caller may have given up between the check above and this send, in
		// which case nobody is left to read what was just delivered.
		c.drainAbandoned(call)
	case <-call.timeout:
		framer.release()
		c.releaseStream(call)
	case <-ctx.Done():
		framer.release()
		// The call has already left the call map, so closeWithError's snapshot
		// cannot find it to report. Leave the stream id alone: the connection is
		// on its way out and the allocator goes with it.
		c.notifyStreamEnd(call, streamAbandoned)
	}

	return nil
}

// streamOutcome says which terminal event a call's stream observer is owed.
type streamOutcome int

const (
	// streamFinished means a response for the call arrived, whatever it said.
	streamFinished streamOutcome = iota
	// streamAbandoned means no response will be delivered for the call.
	streamAbandoned
)

// releaseStream returns call's stream id to the connection and reports the call
// as finished.
//
// Only the first caller for a given call does anything: reclaiming an id twice
// would free whichever request holds it by then.
func (c *Conn) releaseStream(call *callReq) {
	if !call.released.CompareAndSwap(false, true) {
		return
	}

	c.streams.Clear(call.streamID)
	c.notifyStreamEnd(call, streamFinished)
}

// drainAbandoned discards whatever is left for a call whose caller has gone
// away, and claims the call's termination when a real response is among it.
//
// A caller that gives up closes call.timeout and returns without reading, so a
// response arriving afterwards would otherwise sit in the buffer with nobody to
// return its framer or its stream. Both the receive side and the abandoning
// caller run this, and between them exactly one claims the call: the caller
// closes call.timeout before draining, so a response delivered after that drain
// is guaranteed to be seen by the receive side's own drain.
//
// It loops because closeWithError sends to a snapshot without deleting, so it
// can publish to a call processFrame has already taken out of the map, leaving
// two values behind.
func (c *Conn) drainAbandoned(call *callReq) {
	select {
	case <-call.timeout:
	default:
		// The caller is still waiting for this response and will read it itself.
		return
	}

	for {
		select {
		case resp := <-call.resp:
			if resp.framer != nil {
				resp.framer.release()
			}
			if !resp.closing {
				c.releaseStream(call)
			}
		default:
			return
		}
	}
}

// notifyStreamEnd delivers a call's terminal observer event.
//
// It never touches the stream allocator, which is what makes it safe on a
// connection that is being retired but has not yet started refusing new calls:
// returning the id there would hand it to a caller that would then write into a
// connection already known to be unusable.
func (c *Conn) notifyStreamEnd(call *callReq, outcome streamOutcome) {
	if call.streamObserverContext == nil {
		return
	}

	call.streamObserverEndOnce.Do(func() {
		observed := ObservedStream{Host: c.host}
		if outcome == streamFinished {
			call.streamObserverContext.StreamFinished(observed)
		} else {
			call.streamObserverContext.StreamAbandoned(observed)
		}
	})
}

func (c *Conn) recvSegment(ctx context.Context) error {
	var (
		frame           []byte
		payloadBuf      *[]byte // pooled buffer backing frame; nil on the compressed path
		isSelfContained bool
		err             error
	)

	// Wait for the next segment without a read deadline: an idle proto-v5
	// connection must not trip the deadline between frames (CASSGO-125). The
	// deadline is re-armed by onSegmentHeader as soon as the segment header has
	// been read, so only the idle wait is unbounded — the payload and CRC (and
	// the continuation segments in recvPartialFrames) stay bounded, mirroring the
	// header/body split in processFrame. See processFrame for why the timeout is
	// toggled rather than the deadline cleared directly.
	readTimeout := c.r.GetTimeout()
	var onSegmentHeader func()
	if readTimeout > 0 {
		c.r.SetTimeout(0)
		onSegmentHeader = func() { c.r.SetTimeout(readTimeout) }
	}

	// Read frame based on compression
	if c.compressor != nil {
		frame, isSelfContained, err = readCompressedSegment(c.r, c.compressor, onSegmentHeader)
	} else {
		frame, payloadBuf, isSelfContained, err = readUncompressedSegment(c.r, onSegmentHeader)
	}

	// Safety net: restore the timeout if the reader returned before reading the
	// header (e.g. the idle read hit EOF), so onSegmentHeader never fired.
	if readTimeout > 0 {
		c.r.SetTimeout(readTimeout)
	}
	if err != nil {
		return err
	}
	// Every consumer below copies the payload out (readFrame/readHeader/buf.Write),
	// so the buffer can be returned once this segment is fully processed. A single
	// deferred release covers all exit paths; holding it through recvPartialFrames/
	// processFrame in the non-self-contained case is harmless (frame is already
	// copied into buf). releaseSegmentBuffer is nil-safe for the compressed path.
	defer releaseSegmentBuffer(payloadBuf)

	if isSelfContained {
		return c.processAllFramesInSegment(ctx, bytes.NewReader(frame))
	}

	head, err := readHeader(bytes.NewReader(frame), c.headerBuf[:])
	if err != nil {
		return err
	}

	buf := bytes.NewBuffer(make([]byte, 0, head.length+frameHeadSize))
	buf.Write(frame)

	// Computing how many bytes of message left to read
	bytesToRead := head.length - len(frame) + frameHeadSize

	err = c.recvPartialFrames(buf, bytesToRead)
	if err != nil {
		return err
	}

	return c.processFrame(ctx, buf)
}

// recvPartialFrames reads proto v5 segments from Conn.r and writes decoded partial frames to dst.
// It reads data until the bytesToRead is reached.
// If Conn.compressor is not nil, it processes Compressed Format segments.
func (c *Conn) recvPartialFrames(dst *bytes.Buffer, bytesToRead int) error {
	var (
		read            int
		frame           []byte
		isSelfContained bool
		err             error
	)

	for read != bytesToRead {
		var payloadBuf *[]byte // pooled buffer backing frame; nil on the compressed path

		// Continuation segments are mid-frame: a read deadline is already in force
		// (recvSegment restored it after the first segment header), so pass nil.
		if c.compressor != nil {
			frame, isSelfContained, err = readCompressedSegment(c.r, c.compressor, nil)
		} else {
			frame, payloadBuf, isSelfContained, err = readUncompressedSegment(c.r, nil)
		}
		if err != nil {
			return fmt.Errorf("gocql: failed to read non self-contained frame: %w", err)
		}

		if isSelfContained {
			releaseSegmentBuffer(payloadBuf)
			return fmt.Errorf("gocql: received self-contained segment, but expected not")
		}

		if totalLength := dst.Len() + len(frame); totalLength > dst.Cap() {
			releaseSegmentBuffer(payloadBuf)
			return fmt.Errorf("gocql: expected partial frame of length %d, got %d", dst.Cap(), totalLength)
		}

		// Write (copy) the frame to the destination buffer, then release the
		// pooled payload buffer — it is no longer referenced after the copy.
		n, _ := dst.Write(frame)
		releaseSegmentBuffer(payloadBuf)
		read += n
	}

	return nil
}

func (c *Conn) processAllFramesInSegment(ctx context.Context, r *bytes.Reader) error {
	var err error
	for r.Len() > 0 && err == nil {
		err = c.processFrame(ctx, r)
	}

	return err
}

// ConnReader is like net.Conn but also allows to set timeout duration.
type ConnReader interface {
	net.Conn

	// SetTimeout sets timeout duration for reading data form conn
	SetTimeout(timeout time.Duration)

	// GetTimeout returns timeout duration
	GetTimeout() time.Duration
}

// connReader implements ConnReader.
// It retries to read data up to 5 times or returns error.
//
// timeout is accessed concurrently: Read and GetTimeout run on the receive
// goroutine, and SetTimeout is called both at connection-init time and by the
// integration test suite on live connections. atomic.Int64 keeps the field race-free without taking a lock
// on every read.
type connReader struct {
	conn    net.Conn
	r       *bufio.Reader
	timeout atomic.Int64
}

func (c *connReader) Read(p []byte) (n int, err error) {
	const maxAttempts = 5

	timeout := time.Duration(c.timeout.Load())
	for i := 0; i < maxAttempts; i++ {
		var nn int
		if timeout > 0 {
			c.conn.SetReadDeadline(time.Now().Add(timeout))
		} else if timeout == 0 {
			// A zero timeout means "no read deadline". Clear any deadline a prior
			// read armed; otherwise it persists and fires on an idle read, which
			// is exactly the reconnect regression the callers avoid by toggling
			// the timeout to 0 around idle frame/segment reads (CASSGO-125).
			c.conn.SetReadDeadline(time.Time{})
		}

		nn, err = io.ReadFull(c.r, p[n:])
		n += nn
		if err == nil {
			break
		}

		if verr, ok := err.(net.Error); !ok || !verr.Temporary() {
			break
		}
	}

	return
}

func (c *connReader) Write(b []byte) (n int, err error) {
	return c.conn.Write(b)
}

func (c *connReader) Close() error {
	return c.conn.Close()
}

func (c *connReader) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *connReader) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *connReader) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *connReader) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *connReader) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

func (c *connReader) SetTimeout(timeout time.Duration) {
	c.timeout.Store(int64(timeout))
}

func (c *connReader) GetTimeout() time.Duration {
	return time.Duration(c.timeout.Load())
}

type callReq struct {
	// resp will receive the frame that was sent as a response to this stream.
	resp     chan callResp
	timeout  chan struct{} // indicates to recv() that a call has timed out
	streamID int           // current stream in use

	// released is claimed exactly once, by whichever goroutine reclaims this
	// call's stream id. Without it a second reclamation would clear a *different*
	// request's stream once the id had been handed out again: IDGenerator.Clear
	// inspects only the bit, it has no notion of allocation generation.
	released atomic.Bool

	// The request timer is deliberately NOT held here. It is created by the
	// caller after its frame has been written, so any goroutine that reached
	// this call through the call map could observe it mid-initialisation: a
	// Stop landing between time.NewTimer and the drain of its channel parks the
	// caller for good. The timer is a local in execInternal instead, owned and
	// stopped by the one goroutine that builds it.

	// streamObserverContext is notified about events regarding this stream
	streamObserverContext StreamObserverContext

	// streamObserverEndOnce ensures that either StreamAbandoned or StreamFinished is called,
	// but not both.
	streamObserverEndOnce sync.Once
}

type callResp struct {
	// framer is the response frame.
	// May be nil if err is not nil.
	framer *framer
	// err is error encountered, if any.
	err error
	// closing marks a notification produced by closeWithError rather than a
	// response that came off the wire. The closer owns the call's termination for
	// these, so whoever drains one must discard it without claiming the stream.
	closing bool
}

// contextWriter is like io.Writer, but takes context as well.
type contextWriter interface {
	// writeContext writes p to the connection.
	//
	// If ctx is canceled before we start writing p (e.g. during waiting while another write is currently in progress),
	// p is not written and ctx.Err() is returned. Context is ignored after we start writing p (i.e. we don't interrupt
	// blocked writes that are in progress) so that we always either write the full frame or not write it at all.
	//
	// It returns the number of bytes written from p (0 <= n <= len(p)) and any error that caused the write to stop
	// early. writeContext must return a non-nil error if it returns n < len(p). writeContext must not modify the
	// data in p, even temporarily.
	writeContext(ctx context.Context, p []byte) (n int, err error)
}

type deadlineWriter interface {
	SetWriteDeadline(time.Time) error
	io.Writer
}

type deadlineContextWriter struct {
	w       deadlineWriter
	timeout time.Duration
	// semaphore protects critical section for SetWriteDeadline/Write.
	// It is a channel with capacity 1.
	semaphore chan struct{}

	// quit closed once the connection is closed.
	quit chan struct{}
}

// writeContext implements contextWriter.
func (c *deadlineContextWriter) writeContext(ctx context.Context, p []byte) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-c.quit:
		return 0, ErrConnectionClosed
	case c.semaphore <- struct{}{}:
		// acquired
	}

	defer func() {
		// release
		<-c.semaphore
	}()

	if c.timeout > 0 {
		err := c.w.SetWriteDeadline(time.Now().Add(c.timeout))
		if err != nil {
			return 0, err
		}
	}
	return c.w.Write(p)
}

func newWriteCoalescer(conn deadlineWriter, writeTimeout, coalesceDuration time.Duration,
	quit <-chan struct{}, logger StructuredLogger,
	onPanic func(err error, pending []chan<- writeResult),
) *writeCoalescer {
	wc := &writeCoalescer{
		writeCh: make(chan writeRequest),
		c:       conn,
		quit:    quit,
		timeout: writeTimeout,
		logger:  logger,
		onPanic: onPanic,
	}
	go wc.writeFlusher(coalesceDuration)
	return wc
}

type writeCoalescer struct {
	c deadlineWriter

	mu sync.Mutex

	quit    <-chan struct{}
	writeCh chan writeRequest

	timeout time.Duration

	logger  StructuredLogger
	onPanic func(err error, pending []chan<- writeResult)

	testEnqueuedHook   func()
	testFlushedHook    func()
	testFlusherPanicAt func()
}

type writeRequest struct {
	// resultChan is a channel (with buffer size 1) where to send results of the write.
	resultChan chan<- writeResult
	// data to write.
	data []byte
}

type writeResult struct {
	n   int
	err error
}

// writeContext implements contextWriter.
func (w *writeCoalescer) writeContext(ctx context.Context, p []byte) (int, error) {
	resultChan := make(chan writeResult, 1)
	wr := writeRequest{
		resultChan: resultChan,
		data:       p,
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-w.quit:
		return 0, io.EOF // TODO: better error here?
	case w.writeCh <- wr:
		// enqueued for writing
	}

	if w.testEnqueuedHook != nil {
		w.testEnqueuedHook()
	}

	result := <-resultChan
	return result.n, result.err
}

func (w *writeCoalescer) writeFlusher(interval time.Duration) {
	timer := time.NewTimer(interval)
	defer timer.Stop()

	if !timer.Stop() {
		<-timer.C
	}

	w.writeFlusherImpl(timer.C, func() { timer.Reset(interval) })
}

func (w *writeCoalescer) writeFlusherImpl(timerC <-chan time.Time, resetTimer func()) {
	running := false

	var buffers net.Buffers
	var resultChans []chan<- writeResult

	// Inline recover (not recoverGoroutine) so we can capture the
	// up-to-the-moment value of resultChans for the panic teardown. Go's
	// recover() only works when called directly by a deferred function.
	defer func() {
		if r := recover(); r != nil {
			handleRecoveredPanic(w.logger, "writeCoalescer.flusher", r, func(err error) {
				if w.onPanic != nil {
					w.onPanic(err, resultChans)
				}
			})
		}
	}()

	if w.testFlusherPanicAt != nil {
		w.testFlusherPanicAt()
	}

	for {
		select {
		case req := <-w.writeCh:
			buffers = append(buffers, req.data)
			resultChans = append(resultChans, req.resultChan)
			if !running {
				// Start timer on first write.
				resetTimer()
				running = true
			}
		case <-w.quit:
			result := writeResult{
				n:   0,
				err: io.EOF, // TODO: better error here?
			}
			// Unblock whoever was waiting.
			for _, resultChan := range resultChans {
				// resultChan has capacity 1, so it does not block.
				resultChan <- result
			}
			return
		case <-timerC:
			running = false
			w.flush(resultChans, buffers)
			buffers = nil
			resultChans = nil
			if w.testFlushedHook != nil {
				w.testFlushedHook()
			}
		}
	}
}

func (w *writeCoalescer) flush(resultChans []chan<- writeResult, buffers net.Buffers) {
	// Flush everything we have so far.
	if w.timeout > 0 {
		err := w.c.SetWriteDeadline(time.Now().Add(w.timeout))
		if err != nil {
			for i := range resultChans {
				resultChans[i] <- writeResult{
					n:   0,
					err: err,
				}
			}
			return
		}
	}
	// Copy buffers because WriteTo modifies buffers in-place.
	buffers2 := make(net.Buffers, len(buffers))
	copy(buffers2, buffers)
	n, err := buffers2.WriteTo(w.c)
	// Writes of bytes before n succeeded, writes of bytes starting from n failed with err.
	// Use n as remaining byte counter.
	for i := range buffers {
		if int64(len(buffers[i])) <= n {
			// this buffer was fully written.
			resultChans[i] <- writeResult{
				n:   len(buffers[i]),
				err: nil,
			}
			n -= int64(len(buffers[i]))
		} else {
			// this buffer was not (fully) written.
			resultChans[i] <- writeResult{
				n:   int(n),
				err: err,
			}
			n = 0
		}
	}
}

// addCall attempts to add a call to c.calls.
// It fails with error if the connection already started closing or if a call for the given stream
// already exists.
func (c *Conn) addCall(call *callReq) error {
	if !c.calls.tryStore(call.streamID, call) {
		return fmt.Errorf("attempting to use stream already in use: %d", call.streamID)
	}

	// Reject new calls once closing starts.
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		c.calls.delete(call.streamID)
		return ErrConnectionClosed
	}
	c.mu.Unlock()
	return nil
}

// testBeforeCallerRecv runs just before a caller waits for its response, if a
// test has set it. Whether a caller sees its response or its cancellation first
// is a race the runtime decides, so a test that needs one specific order has to
// arrange the world at this point. Always nil in production.
var testBeforeCallerRecv func()

func (c *Conn) exec(ctx context.Context, req frameBuilder, tracer Tracer) (*framer, error) {
	return c.execInternal(ctx, req, tracer, true)
}

func (c *Conn) execInternal(ctx context.Context, req frameBuilder, tracer Tracer, startupCompleted bool) (*framer, error) {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}

	// TODO: move tracer onto conn
	stream, ok := c.streams.GetStream()
	if !ok {
		return nil, ErrNoStreams
	}

	// resp is basically a waiting semaphore protecting the framer
	framer := getFramer(c.compressor, c.version, c.session.types)
	defer framer.release() // Request framer can be released after write completes

	call := &callReq{
		timeout:  make(chan struct{}),
		streamID: stream,
		// Buffer of 1 allows closeWithError to send without blocking,
		// preventing deadlock when caller is blocked before selecting on resp.
		resp: make(chan callResp, 1),
	}

	if c.streamObserver != nil {
		call.streamObserverContext = c.streamObserver.StreamContext(ctx)
	}

	if err := c.addCall(call); err != nil {
		return nil, err
	}

	// After this point, we need to either read from call.resp or close(call.timeout)
	// since closeWithError can try to write a connection close error to call.resp.
	// If we don't close(call.timeout) or read from call.resp, closeWithError can deadlock.

	if tracer != nil {
		framer.trace()
	}

	if call.streamObserverContext != nil {
		call.streamObserverContext.StreamStarted(ObservedStream{
			Host: c.host,
		})
	}

	err := req.buildFrame(framer, stream)
	if err != nil {
		// closeWithError will block waiting for this stream to either receive a response
		// or for us to timeout.
		close(call.timeout)
		// We failed to serialize the frame into a buffer.
		// This should not affect the connection as we didn't write anything. We just free the current call.
		c.calls.delete(call.streamID)
		// We need to release the stream after we remove the call from c.calls, otherwise the existingCall != nil
		// check above could fail.
		c.releaseStream(call)
		return nil, err
	}

	var n int

	if c.version > protoVersion4 && startupCompleted {
		err = framer.prepareModernLayout()
	}
	if err == nil {
		n, err = c.w.writeContext(ctx, framer.buf)
	}
	if err != nil {
		// closeWithError will block waiting for this stream to either receive a response
		// or for us to timeout, close the timeout chan here. Im not entirely sure
		// but we should not get a response after an error on the write side.
		close(call.timeout)
		if (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) && n == 0 {
			// We have not started to write this frame.
			// Release the stream as no response can come from the server on the stream.
			c.calls.delete(call.streamID)
			// We need to release the stream after we remove the call from c.calls, otherwise the existingCall != nil
			// check above could fail.
			c.releaseStream(call)
		} else {
			// I think this is the correct thing to do, im not entirely sure. It is not
			// ideal as readers might still get some data, but they probably wont.
			// Here we need to be careful as the stream is not available and if all
			// writes just timeout or fail then the pool might use this connection to
			// send a frame on, with all the streams used up and not returned.
			c.closeWithError(err)
		}
		return nil, err
	}

	var timeoutCh <-chan time.Time
	// If the caller's context already has a deadline, defer to it rather than
	// arming the connection-level timeout — otherwise WithContext(ctx-with-deadline)
	// cannot effectively extend a short Session.Timeout.
	_, ctxHasDeadline := ctx.Deadline()
	if requestTimeout := time.Duration(c.requestTimeout.Load()); requestTimeout > 0 && !ctxHasDeadline {
		timer := time.NewTimer(requestTimeout)
		defer timer.Stop()
		timeoutCh = timer.C
	}

	var ctxDone <-chan struct{}
	if ctx != nil {
		ctxDone = ctx.Done()
	}

	if testBeforeCallerRecv != nil {
		testBeforeCallerRecv()
	}

	select {
	case resp := <-call.resp:
		close(call.timeout)
		if resp.err != nil {
			if resp.framer != nil {
				resp.framer.release()
			}
			if !resp.closing {
				// A response that came off the wire ends this call, whatever the
				// connection is doing by now. Only a notification from
				// closeWithError leaves the stream alone: that one belongs to the
				// closer, which reports it and takes the whole allocator with it.
				c.releaseStream(call)
			}
			return nil, resp.err
		}
		// dont release the stream if detect a timeout as another request can reuse
		// that stream and get a response for the old request, which we have no
		// easy way of detecting.
		//
		// Ensure that the stream is not released if there are potentially outstanding
		// requests on the stream to prevent nil pointer dereferences in recv().
		defer c.releaseStream(call)

		if v := resp.framer.header.version.version(); v != c.version {
			errProtocol := NewErrProtocol("unexpected protocol version in response: got %d expected %d", v, c.version)
			responseFrame, err := resp.framer.parseFrame()
			resp.framer.release()
			if err != nil {
				c.logger.Warning("Framer error while attempting to parse potential protocol error.",
					NewLogFieldError("err", err))
				return nil, errProtocol
			}
			//goland:noinspection GoTypeAssertionOnErrors
			errFrame, isErrFrame := responseFrame.(errorFrame)
			if !isErrFrame || errFrame.Code() != ErrCodeProtocol {
				return nil, errProtocol
			}
			return nil, NewErrProtocol("%w", &protocolError{
				errFrame,
			})
		}

		return resp.framer, nil
	case <-timeoutCh:
		close(call.timeout)
		// A response may already be sitting in the buffer: this select and the
		// receive side's can both have been ready at once.
		c.drainAbandoned(call)
		c.logger.Debug("Request timed out on connection.",
			NewLogFieldString("host_id", c.host.HostID()), NewLogFieldIP("addr", c.host.ConnectAddress()))
		return nil, ErrTimeoutNoResponse
	case <-ctxDone:
		c.logger.Debug("Request failed because context elapsed out on connection.",
			NewLogFieldString("host_id", c.host.HostID()), NewLogFieldIP("addr", c.host.ConnectAddress()),
			NewLogFieldError("ctx_err", ctx.Err()))
		close(call.timeout)
		c.drainAbandoned(call)
		// Returned unwrapped: query_executor classifies context errors by equality,
		// so wrapping one turns a caller giving up into a host failure.
		return nil, ctx.Err()
	case <-c.ctx.Done():
		c.logger.Debug("Request failed because connection closed.",
			NewLogFieldString("host_id", c.host.HostID()), NewLogFieldIP("addr", c.host.ConnectAddress()))
		close(call.timeout)
		c.drainAbandoned(call)
		return nil, ErrConnectionClosed
	}
}

// ObservedStream observes a single request/response stream.
type ObservedStream struct {
	// Host of the connection used to send the stream.
	Host *HostInfo
}

// StreamObserver is notified about request/response pairs.
// Streams are created for executing queries/batches or
// internal requests to the database and might live longer than
// execution of the query - the stream is still tracked until
// response arrives so that stream IDs are not reused.
type StreamObserver interface {
	// StreamContext is called before creating a new stream.
	// ctx is context passed to Session.Query / Session.Batch,
	// but might also be an internal context (for example
	// for internal requests that use control connection).
	// StreamContext might return nil if it is not interested
	// in the details of this stream.
	// StreamContext is called before the stream is created
	// and the returned StreamObserverContext might be discarded
	// without any methods called on the StreamObserverContext if
	// creation of the stream fails.
	// Note that if you don't need to track per-stream data,
	// you can always return the same StreamObserverContext.
	StreamContext(ctx context.Context) StreamObserverContext
}

// StreamObserverContext is notified about state of a stream.
// A stream is started every time a request is written to the server
// and is finished when a response is received.
// It is abandoned when the underlying network connection is closed
// before receiving a response.
type StreamObserverContext interface {
	// StreamStarted is called when the stream is started.
	// This happens just before a request is written to the wire.
	StreamStarted(observedStream ObservedStream)

	// StreamAbandoned is called when we stop waiting for response.
	// This happens when the underlying network connection is closed.
	// StreamFinished won't be called if StreamAbandoned is.
	StreamAbandoned(observedStream ObservedStream)

	// StreamFinished is called when we receive a response for the stream.
	StreamFinished(observedStream ObservedStream)
}

type preparedStatment struct {
	id               []byte
	resultMetadataID []byte
	request          preparedMetadata
	response         resultMetadata
}

// prepResult carries a shared prepare load's outcome back to the caller that
// started it, so the caller can stop waiting without stopping the load.
type prepResult struct {
	info *preparedStatment
	err  error
}

// prepareStatement returns the prepared statement for stmt on this connection,
// preparing it once and sharing that result with every concurrent caller.
//
// The caller's context bounds how long this call waits, not how long the prepare
// itself runs: the load is shared, so it completes and populates the cache even
// after the caller that started it has given up.
//
// The returned error must stay unwrapped. queryExecutor classifies a request's
// error by comparing it against context.Canceled and context.DeadlineExceeded by
// equality, so wrapping a caller-context error would make the executor treat a
// caller's cancellation as a host failure and retry the query on another host.
//
// Parameters:
//   - ctx: the caller's context; may be nil, which waits indefinitely
//   - stmt: the CQL statement to prepare
//   - tracer: optional tracer for the PREPARE request
//   - keyspace: the keyspace the statement is prepared against
//
// Returns:
//   - *preparedStatment: the prepared statement, from the cache or a fresh load
//   - error: the caller's ctx.Err() when it gave up waiting, otherwise the load's error
func (c *Conn) prepareStatement(ctx context.Context, stmt string, tracer Tracer, keyspace string) (*preparedStatment, error) {
	cacheKey := c.session.stmtsLRU.keyFor(c.host.HostID(), keyspace, stmt)

	// Serve a warm statement without entering the cancellable slow path below.
	// That path needs a goroutine and a channel per call, and this is the hottest
	// path in the driver: an already-prepared statement must not pay for machinery
	// only a real load needs. Correctness does not depend on this shortcut —
	// GetIfPresent and Get share the same hit semantics, including expiry and
	// recency accounting — but performance does.
	if info, ok := c.session.stmtsLRU.get(cacheKey); ok {
		return info, nil
	}

	var ctxDone <-chan struct{}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			// An already-cancelled caller must not start a load goroutine.
			return nil, err
		}
		ctxDone = ctx.Done()
	}

	loader := otter.LoaderFunc[preparedKey, *preparedStatment](func(_ context.Context, _ preparedKey) (*preparedStatment, error) {
		prep := &writePrepareFrame{
			statement: stmt,
		}
		if c.version > protoVersion4 {
			prep.keyspace = keyspace
		}

		// Use a connection-scoped context for the actual network call to ensure the
		// prepare completes even if the caller's context is canceled (other waiters
		// need the result).
		//
		// The deadline below is load-bearing, not defensive. Waiters that share this
		// load park in otter's context-free WaitGroup, so their goroutines live
		// exactly as long as this call does; removing the deadline removes their only
		// upper bound. It also makes explicit a bound that used to arrive as a side
		// effect of execInternal arming its request timer for a deadline-less context.
		//
		// When Session.Timeout is 0 the user asked for no request timeout, so this
		// load gets none either and lives until the connection closes — the same as
		// every other in-flight request under that configuration. Deriving a zero
		// timeout here would instead produce an already-expired context and fail
		// every prepare outright.
		loadCtx := c.ctx
		if requestTimeout := time.Duration(c.requestTimeout.Load()); requestTimeout > 0 {
			var cancel context.CancelFunc
			loadCtx, cancel = context.WithTimeout(c.ctx, requestTimeout)
			defer cancel()
		}

		framer, err := c.exec(loadCtx, prep, tracer)
		if err != nil {
			// execInternal defers to a context deadline instead of arming its own
			// request timer, so the bound above surfaces as context.DeadlineExceeded
			// rather than ErrTimeoutNoResponse. Otter shares a load's error with every
			// waiter, including ones whose own context is still alive, and the executor
			// reads a bare context error as a caller cancellation that must not be
			// retried elsewhere. Translate it back to the error the request timer would
			// have produced, so a node that really timed out is not mistaken for a
			// caller that gave up. c.ctx carries no deadline of its own, so a deadline
			// here can only be ours.
			if errors.Is(err, context.DeadlineExceeded) && c.ctx.Err() == nil {
				return nil, ErrTimeoutNoResponse
			}
			return nil, err
		}
		defer framer.release()

		frame, err := framer.parseFrame()
		if err != nil {
			return nil, err
		}

		if len(framer.traceID) > 0 && tracer != nil {
			tracer.Trace(framer.traceID)
		}

		switch x := frame.(type) {
		case *resultPreparedFrame:
			return &preparedStatment{
				// defensively copy as we will recycle the underlying buffer after we return.
				id:               copyBytes(x.preparedID),
				resultMetadataID: copyBytes(x.resultMetadataID),
				// the type info's should _not_ have a reference to the framers read buffer,
				// therefore we can just copy them directly.
				request:  x.reqMeta,
				response: x.respMeta,
			}, nil
		case error:
			return nil, x
		default:
			return nil, NewErrProtocol("Unknown type in response to prepare frame: %s", x)
		}
	})

	// Buffered so the load goroutine can always deliver and exit, even when the
	// caller has already stopped waiting.
	done := make(chan prepResult, 1)
	go func() {
		// Deliberately not the caller's context: otter hands this straight to the
		// loader, which ignores it, and the caller's cancellation is handled by the
		// select below. Passing the connection's context keeps "this context has
		// nothing to do with the caller" explicit, so a later change that makes the
		// loader honour it cannot silently cancel a load other waiters depend on.
		info, err := c.session.stmtsLRU.getOrLoad(c.ctx, cacheKey, loader)
		done <- prepResult{info: info, err: err}
	}()

	select {
	case res := <-done:
		// select chooses at random when both cases are ready, so re-check the
		// caller's context: returning a result past its deadline would make
		// "the caller's deadline is honoured" a probabilistic guarantee.
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		return res.info, res.err
	case <-ctxDone:
		return nil, ctx.Err()
	}
}

func marshalQueryValue(typ TypeInfo, value interface{}, dst *queryValues) error {
	if named, ok := value.(*namedValue); ok {
		dst.name = named.name
		value = named.value
	}

	if _, ok := value.(unsetColumn); !ok {
		val, err := Marshal(typ, value)
		if err != nil {
			return err
		}

		dst.value = val
	} else {
		dst.isUnset = true
	}

	return nil
}

// maxUnprepRetries caps the number of times executeQuery and executeBatch
// will respond to a RequestErrUnprepared by evicting the prepared-statement
// cache entry and retrying. Without this cap, a server-side cache thrash
// (Cassandra evicting our prepared statement between every attempt) would
// recurse unboundedly and stack-overflow the goroutine. Five retries is
// generous — a single legitimate eviction is the realistic case — while
// ruling out pathological recursion.
const maxUnprepRetries = 5

func (c *Conn) executeQuery(ctx context.Context, q *internalQuery) *Iter {
	return c.executeQueryWithUnprepRetries(ctx, q, 0)
}

func (c *Conn) executeQueryWithUnprepRetries(ctx context.Context, q *internalQuery, unprepAttempt int) *Iter {
	qryOpts := q.qryOpts
	params := queryParams{
		consistency: q.GetConsistency(),
	}
	iter := newIter(q.metrics, q.Keyspace(), q.routingInfo, q.qryOpts.getKeyspace)

	// frame checks that it is not 0
	params.serialConsistency = qryOpts.serialCons
	params.defaultTimestamp = qryOpts.defaultTimestamp
	params.defaultTimestampValue = qryOpts.defaultTimestampValue

	if len(q.pageState) > 0 {
		params.pagingState = q.pageState
	}
	if qryOpts.pageSize > 0 {
		params.pageSize = qryOpts.pageSize
	}
	if c.version > protoVersion4 {
		params.keyspace = qryOpts.keyspace
		params.nowInSeconds = qryOpts.nowInSecondsValue
	}

	// If a keyspace for the qry is overriden,
	// then we should use it to create stmt cache key
	usedKeyspace := c.currentKeyspace
	if qryOpts.keyspace != "" {
		usedKeyspace = qryOpts.keyspace
	}

	var (
		frame frameBuilder
		info  *preparedStatment
	)

	if !qryOpts.skipPrepare && shouldPrepare(qryOpts.stmt) {
		// Prepare all DML queries. Other queries can not be prepared.
		var err error
		info, err = c.prepareStatement(ctx, qryOpts.stmt, qryOpts.trace, usedKeyspace)
		if err != nil {
			iter.err = err
			return iter
		}

		values := qryOpts.values
		if qryOpts.binding != nil {
			values, err = qryOpts.binding(&QueryInfo{
				Id:          info.id,
				Args:        info.request.columns,
				Rval:        info.response.columns,
				PKeyColumns: info.request.pkeyColumns,
			})
			if err != nil {
				iter.err = err
				return iter
			}
		}

		if len(values) != info.request.actualColCount {
			iter.err = fmt.Errorf("gocql: expected %d values send got %d", info.request.actualColCount, len(values))
			return iter
		}

		params.values = make([]queryValues, len(values))
		for i := 0; i < len(values); i++ {
			v := &params.values[i]
			value := values[i]
			typ := info.request.columns[i].TypeInfo
			if err := marshalQueryValue(typ, value, v); err != nil {
				iter.err = err
				return iter
			}
		}

		// if the metadata was not present in the response then we should not skip it
		params.skipMeta = !(c.session.cfg.DisableSkipMetadata || qryOpts.disableSkipMetadata) && info != nil && info.response.flags&flagNoMetaData == 0

		frame = &writeExecuteFrame{
			preparedID:       info.id,
			params:           params,
			customPayload:    qryOpts.customPayload,
			resultMetadataID: info.resultMetadataID,
		}

		// Set "keyspace" and "table" property in the query if it is present in preparedMetadata
		ks := info.request.keyspace
		if ks == "" {
			ks = usedKeyspace
		}
		q.routingInfo.set(ks, info.request.table)
	} else {
		frame = &writeQueryFrame{
			statement:     qryOpts.stmt,
			params:        params,
			customPayload: qryOpts.customPayload,
		}
	}

	framer, err := c.exec(ctx, frame, qryOpts.trace)
	if err != nil {
		iter.err = err
		return iter
	}

	resp, err := framer.parseFrame()
	if err != nil {
		framer.release()
		iter.err = err
		return iter
	}

	if len(framer.traceID) > 0 && qryOpts.trace != nil {
		qryOpts.trace.Trace(framer.traceID)
	}

	switch x := resp.(type) {
	case *resultVoidFrame:
		iter.framer = framer
		return iter
	case *resultRowsFrame:
		if x.meta.newMetadataID != nil {
			// If a RESULT/Rows message reports
			//      changed resultset metadata with the Metadata_changed flag, the reported new
			//      resultset metadata must be used in subsequent executions
			stmtCacheKey := c.session.stmtsLRU.keyFor(c.host.HostID(), usedKeyspace, qryOpts.stmt)
			oldStmt, ok := c.session.stmtsLRU.get(stmtCacheKey)
			if ok {
				newStmt := &preparedStatment{
					id:               oldStmt.id,
					resultMetadataID: x.meta.newMetadataID,
					request:          oldStmt.request,
					response:         x.meta,
				}
				c.session.stmtsLRU.set(stmtCacheKey, newStmt)
				// Updating info to ensure the code is looking at the updated
				// version of the prepared statement
				info = newStmt
			}
		}
		iter.meta = x.meta
		iter.framer = framer
		iter.numRows = x.numRows

		if x.meta.noMetaData() {
			if info != nil {
				iter.meta = info.response
				iter.meta.pagingState = copyBytes(x.meta.pagingState)
			} else {
				iter = newErrIter(errors.New("gocql: did not receive metadata but prepared info is nil"), q.metrics, q.Keyspace(), q.routingInfo, q.qryOpts.getKeyspace)
				iter.framer = framer
				return iter
			}
		} else {
			iter.meta = x.meta
		}

		if x.meta.morePages() && !qryOpts.disableAutoPage {
			newQry := new(internalQuery)
			*newQry = *q
			newQry.pageState = copyBytes(x.meta.pagingState)
			newQry.metrics = &queryMetrics{}
			if newQry.qryOpts.observer != nil {
				newQry.hostMetricsManager = newHostMetricsManager()
			} else {
				newQry.hostMetricsManager = emptyHostMetricsManager
			}

			iter.next = &nextIter{
				q:   newQry,
				pos: int((1 - qryOpts.prefetch) * float64(x.numRows)),
			}

			if iter.next.pos < 1 {
				iter.next.pos = 1
			}
		}

		return iter
	case *resultKeyspaceFrame:
		iter.framer = framer
		return iter
	case *schemaChangeKeyspace, *schemaChangeTable, *schemaChangeFunction, *schemaChangeAggregate, *schemaChangeType:
		iter.framer = framer
		if err := c.awaitSchemaAgreement(ctx); err != nil {
			// TODO: should have this behind a flag
			c.logger.Warning("Error while awaiting for schema agreement after a schema change event.", NewLogFieldError("err", err))
		}
		// dont return an error from this, might be a good idea to give a warning
		// though. The impact of this returning an error would be that the cluster
		// is not consistent with regards to its schema.
		return iter
	case *RequestErrUnprepared:
		framer.release()
		if unprepAttempt >= maxUnprepRetries {
			// Pathological re-prepare loop (server-side cache evicting
			// our prepared statement between every attempt). Bail with
			// the underlying server error rather than recursing
			// indefinitely.
			iter.err = fmt.Errorf("gocql: failed to execute prepared statement after %d re-prepare attempts: %w",
				unprepAttempt+1, x)
			return iter
		}
		stmtCacheKey := c.session.stmtsLRU.keyFor(c.host.HostID(), usedKeyspace, qryOpts.stmt)
		c.session.stmtsLRU.evictPreparedID(stmtCacheKey, x.StatementId)
		return c.executeQueryWithUnprepRetries(ctx, q, unprepAttempt+1)
	case error:
		iter.err = x
		iter.framer = framer
		return iter
	default:
		iter.err = NewErrProtocol("Unknown type in response to execute query (%T): %s", x, x)
		iter.framer = framer
		return iter
	}
}

func (c *Conn) Pick(qry *Query) *Conn {
	if c.Closed() {
		return nil
	}
	return c
}

func (c *Conn) Closed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *Conn) Address() string {
	return c.addr
}

func (c *Conn) AvailableStreams() int {
	return c.streams.Available()
}

func (c *Conn) UseKeyspace(keyspace string) error {
	q := &writeQueryFrame{statement: `USE "` + keyspace + `"`}
	q.params.consistency = c.session.cons

	framer, err := c.exec(c.ctx, q, nil)
	if err != nil {
		return err
	}
	defer framer.release()

	resp, err := framer.parseFrame()
	if err != nil {
		return err
	}

	switch x := resp.(type) {
	case *resultKeyspaceFrame:
	case error:
		return x
	default:
		return NewErrProtocol("unknown frame in response to USE: %v", x)
	}

	c.currentKeyspace = keyspace

	return nil
}

func (c *Conn) executeBatch(ctx context.Context, b *internalBatch) *Iter {
	return c.executeBatchWithUnprepRetries(ctx, b, 0)
}

// batchFastPathStmt reports whether every entry in the batch is the same prepared
// statement — the write-heavy hot case (e.g. one INSERT over many rows). Such a
// batch needs a single prepare and no per-statement dedup/eviction map, so the
// batch collections can be pooled. It returns the shared statement text. An empty
// batch, any raw (non-prepared) entry, or any differing statement falls back to
// the general path.
func batchFastPathStmt(entries []BatchEntry) (string, bool) {
	if len(entries) == 0 {
		return "", false
	}
	stmt := entries[0].Stmt
	for i := range entries {
		e := &entries[i]
		if e.Stmt != stmt || (len(e.Args) == 0 && e.binding == nil) {
			return "", false
		}
	}
	return stmt, true
}

func (c *Conn) executeBatchWithUnprepRetries(ctx context.Context, b *internalBatch, unprepAttempt int) *Iter {
	iter := newIter(b.metrics, b.Keyspace(), b.routingInfo, nil)
	n := len(b.batchOpts.entries)
	req := &writeBatchFrame{
		typ:                   b.batchOpts.bType,
		consistency:           b.GetConsistency(),
		serialConsistency:     b.batchOpts.serialCons,
		defaultTimestamp:      b.batchOpts.defaultTimestamp,
		defaultTimestampValue: b.batchOpts.defaultTimestampValue,
		customPayload:         b.batchOpts.customPayload,
	}

	if c.version > protoVersion4 {
		req.keyspace = b.batchOpts.keyspace
		req.nowInSeconds = b.batchOpts.nowInSeconds
	}

	usedKeyspace := c.currentKeyspace
	if b.batchOpts.keyspace != "" {
		usedKeyspace = b.batchOpts.keyspace
	}

	// stmts maps preparedID -> statement text for the general path's
	// unprepared-retry eviction. For the single-statement fast path fastID/fastStmt
	// play the same role without a map.
	fastStmt, fastPath := batchFastPathStmt(b.batchOpts.entries)
	var (
		stmts    map[string]string
		fastID   []byte
		stmtsBuf *[]batchStatment
		flat     *[]queryValues
	)

	if fastPath {
		// Return the pooled collections on every build error/panic path. They are
		// niled after c.exec (the normal release point), where these releases
		// become no-ops, so there is no double Put.
		defer func() {
			// Release the statement slice first: clearing it drops the values
			// sub-slice references into flat before flat is pooled.
			releaseBatchStmts(stmtsBuf)
			releaseQueryValues(flat)
		}()

		info, err := c.prepareStatement(ctx, fastStmt, b.batchOpts.trace, usedKeyspace)
		if err != nil {
			iter.err = err
			return iter
		}
		fastID = info.id
		k := info.request.actualColCount

		// One prepare, uniform column count: pool the statement slice and a single
		// flat values buffer sub-sliced per row (no dedup/eviction maps). Pre-sizing
		// flat to n*k keeps each sub-slice stable (no mid-loop regrow).
		stmtsBuf = getBatchStmts(n)
		flat = getQueryValues(n * k)
		req.statements = *stmtsBuf

		for i := 0; i < n; i++ {
			entry := &b.batchOpts.entries[i]
			batchStmt := &req.statements[i]

			var values []interface{}
			if entry.binding == nil {
				values = entry.Args
			} else {
				values, err = entry.binding(&QueryInfo{
					Id:          info.id,
					Args:        info.request.columns,
					Rval:        info.response.columns,
					PKeyColumns: info.request.pkeyColumns,
				})
				if err != nil {
					iter.err = err
					return iter
				}
			}

			if len(values) != k {
				iter.err = fmt.Errorf("gocql: batch statement %d expected %d values send got %d", i, k, len(values))
				return iter
			}

			batchStmt.preparedID = info.id
			batchStmt.values = (*flat)[i*k : i*k+k]

			for j := 0; j < k; j++ {
				v := &batchStmt.values[j]
				typ := info.request.columns[j].TypeInfo
				if err := marshalQueryValue(typ, values[j], v); err != nil {
					iter.err = err
					return iter
				}
			}
		}
	} else {
		req.statements = make([]batchStatment, n)
		stmts = make(map[string]string, len(b.batchOpts.entries))

		// Local cache to deduplicate prepareStatement calls for repeated statements
		// within a single batch execution. Capped at batchDedupThreshold entries to
		// bound memory and map overhead for batches with many distinct statements.
		//
		// Crucially, the cache is always consulted even after it stops growing: a
		// statement already stored continues to get cache hits regardless of how many
		// unique statements appear later. Only new unique statements beyond the cap
		// fall through to the global prepared-statement cache.
		const batchDedupThreshold = 16
		type batchPrepKey struct {
			keyspace  string
			statement string
		}
		localCache := make(map[batchPrepKey]*preparedStatment, min(n, batchDedupThreshold))

		for i := 0; i < n; i++ {
			entry := &b.batchOpts.entries[i]
			batchStmt := &req.statements[i]

			if len(entry.Args) > 0 || entry.binding != nil {
				var info *preparedStatment
				var err error

				key := batchPrepKey{keyspace: usedKeyspace, statement: entry.Stmt}
				var ok bool
				info, ok = localCache[key]
				if !ok {
					info, err = c.prepareStatement(ctx, entry.Stmt, b.batchOpts.trace, usedKeyspace)
					if err != nil {
						iter.err = err
						return iter
					}
					if len(localCache) < batchDedupThreshold {
						localCache[key] = info
					}
				}

				var values []interface{}
				if entry.binding == nil {
					values = entry.Args
				} else {
					values, err = entry.binding(&QueryInfo{
						Id:          info.id,
						Args:        info.request.columns,
						Rval:        info.response.columns,
						PKeyColumns: info.request.pkeyColumns,
					})
					if err != nil {
						iter.err = err
						return iter
					}
				}

				if len(values) != info.request.actualColCount {
					iter.err = fmt.Errorf("gocql: batch statement %d expected %d values send got %d", i, info.request.actualColCount, len(values))
					return iter
				}

				batchStmt.preparedID = info.id
				stmts[string(info.id)] = entry.Stmt

				batchStmt.values = make([]queryValues, info.request.actualColCount)

				for j := 0; j < info.request.actualColCount; j++ {
					v := &batchStmt.values[j]
					value := values[j]
					typ := info.request.columns[j].TypeInfo
					if err := marshalQueryValue(typ, value, v); err != nil {
						iter.err = err
						return iter
					}
				}
			} else {
				batchStmt.statement = entry.Stmt
			}
		}
	}

	framer, err := c.exec(ctx, req, b.batchOpts.trace)
	if fastPath {
		// req was serialized synchronously by buildFrame inside c.exec, so the
		// pooled collections are dead now — return them before response handling
		// and before any unprepared-retry recursion. Niling makes the deferred
		// safety-net release a no-op.
		releaseBatchStmts(stmtsBuf)
		stmtsBuf = nil
		releaseQueryValues(flat)
		flat = nil
		req.statements = nil
	}
	if err != nil {
		iter.err = err
		return iter
	}

	resp, err := framer.parseFrame()
	if err != nil {
		iter.err = err
		iter.framer = framer
		return iter
	}

	if len(framer.traceID) > 0 && b.batchOpts.trace != nil {
		b.batchOpts.trace.Trace(framer.traceID)
	}

	switch x := resp.(type) {
	case *resultVoidFrame:
		framer.release()
		return iter
	case *RequestErrUnprepared:
		framer.release()
		if unprepAttempt >= maxUnprepRetries {
			// Pathological re-prepare loop on the batch path (server-side
			// cache evicting our prepared statement between every attempt).
			// Bail with the underlying server error rather than recursing
			// indefinitely.
			iter.err = fmt.Errorf("gocql: failed to execute batch after %d re-prepare attempts: %w",
				unprepAttempt+1, x)
			return iter
		}
		if fastPath {
			// One statement/preparedID in the batch; evict it if the server
			// rejected that id.
			if bytes.Equal(x.StatementId, fastID) {
				key := c.session.stmtsLRU.keyFor(c.host.HostID(), usedKeyspace, fastStmt)
				c.session.stmtsLRU.evictPreparedID(key, x.StatementId)
			}
		} else if stmt, found := stmts[string(x.StatementId)]; found {
			key := c.session.stmtsLRU.keyFor(c.host.HostID(), usedKeyspace, stmt)
			c.session.stmtsLRU.evictPreparedID(key, x.StatementId)
		}
		return c.executeBatchWithUnprepRetries(ctx, b, unprepAttempt+1)
	case *resultRowsFrame:
		iter.meta = x.meta
		iter.framer = framer
		iter.numRows = x.numRows
		return iter
	case error:
		iter.err = x
		iter.framer = framer
		return iter
	default:
		iter.err = NewErrProtocol("Unknown type in response to batch statement: %s", x)
		iter.framer = framer
		return iter
	}
}

func (c *Conn) query(ctx context.Context, statement string, values ...interface{}) (iter *Iter) {
	q := c.session.Query(statement, values...).Consistency(One).Trace(nil)
	q.skipPrepare = true
	q.disableSkipMetadata = true

	// we want to keep the query on this connection
	return q.iterInternal(c, ctx)
}

func (c *Conn) querySystemPeers(ctx context.Context, version cassVersion) *Iter {
	const (
		peerSchema    = "SELECT * FROM system.peers"
		peerV2Schemas = "SELECT * FROM system.peers_v2"
	)

	c.mu.Lock()
	isSchemaV2 := c.isSchemaV2
	c.mu.Unlock()

	if version.AtLeast(4, 0, 0) && isSchemaV2 {
		// Try "system.peers_v2" and fallback to "system.peers" if it's not found
		iter := c.query(ctx, peerV2Schemas)

		err := iter.checkErrAndNotFound()
		if err != nil {
			var requestErr RequestError
			if errors.As(err, &requestErr) && requestErr.Code() == ErrCodeInvalid { // system.peers_v2 not found, try system.peers
				c.mu.Lock()
				c.isSchemaV2 = false
				c.mu.Unlock()
				return c.query(ctx, peerSchema)
			} else {
				return iter
			}
		}
		return iter
	} else {
		return c.query(ctx, peerSchema)
	}
}

func (c *Conn) querySystemLocal(ctx context.Context) *Iter {
	return c.query(ctx, "SELECT * FROM system.local WHERE key='local'")
}

func (c *Conn) awaitSchemaAgreement(ctx context.Context) (err error) {
	return c.awaitSchemaAgreementWithTimeout(ctx, c.session.cfg.MaxWaitSchemaAgreement)
}

func (c *Conn) awaitSchemaAgreementWithTimeout(ctx context.Context, timeout time.Duration) (err error) {
	const localSchemas = "SELECT schema_version FROM system.local WHERE key='local'"

	var versions map[string]struct{}
	var schemaVersion string
	var rows []map[string]interface{}

	endDeadline := time.Now().Add(timeout)

	for time.Now().Before(endDeadline) {
		iter := c.querySystemPeers(ctx, c.host.version)

		versions = make(map[string]struct{})

		rows, err = iter.SliceMap()
		if err != nil {
			goto cont
		}

		for _, row := range rows {
			var host *HostInfo
			host, err = c.session.newHostInfoFromMap(c.host.ConnectAddress(), c.session.cfg.Port, row)
			if err != nil {
				goto cont
			}
			if !isValidPeer(host) || host.schemaVersion == "" {
				c.logger.Warning("Invalid peer or peer with empty schema_version.", NewLogFieldIP("peer", host.ConnectAddress()))
				continue
			}

			// Skip peers we KNOW to be down. A peer that is not yet tracked in
			// the ring (ok=false) is included as before — its schema_version
			// must still gate convergence so that a CREATE TABLE/DROP TABLE
			// doesn't falsely report "agreed" before gossip has propagated
			// to that peer. Only an explicitly-Down peer (in our ring AND
			// state != NodeUp) is safe to skip: that's the case upstream PR
			// #1738 was fixing — a dead node's stale schema_version would
			// otherwise hold the agreement loop until MaxWaitSchemaAgreement.
			if peerInfo, ok := c.session.ring.getHost(host.HostID()); ok && peerInfo != nil && !peerInfo.IsUp() {
				c.logger.Warning("Skipping known-down peer while waiting for schema agreement.",
					NewLogFieldIP("peer", host.ConnectAddress()),
					NewLogFieldString("host_id", host.HostID()))
				continue
			}

			versions[host.schemaVersion] = struct{}{}
		}

		if err = iter.Close(); err != nil {
			goto cont
		}

		iter = c.query(ctx, localSchemas)
		for iter.Scan(&schemaVersion) {
			versions[schemaVersion] = struct{}{}
			schemaVersion = ""
		}

		if err = iter.Close(); err != nil {
			goto cont
		}

		if len(versions) <= 1 {
			return nil
		}

	cont:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}

	if err != nil {
		return err
	}

	schemas := make([]string, 0, len(versions))
	for schema := range versions {
		schemas = append(schemas, schema)
	}

	// not exported
	return fmt.Errorf("gocql: cluster schema versions not consistent: %+v", schemas)
}

var (
	ErrTimeoutNoResponse = errors.New("gocql: no response received from cassandra within timeout period")
	ErrConnectionClosed  = errors.New("gocql: connection closed waiting for response")
	ErrNoStreams         = errors.New("gocql: no streams available on connection")

	// Deprecated: TimeoutLimit was removed so this is never returned by the driver now
	ErrTooManyTimeouts = errors.New("gocql: too many query timeouts on the connection")

	// Deprecated: Never returned by the driver
	ErrQueryArgLength = errors.New("gocql: query argument length mismatch")
)
