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
	"context"
	"errors"
	"net"
	"time"
)

// PoolConfig configures the connection pool used by the driver, it defaults to
// using a round-robin host selection policy and a round-robin connection selection
// policy for each host.
type PoolConfig struct {
	// HostSelectionPolicy sets the policy for selecting which host to use for a
	// given query (default: RoundRobinHostPolicy())
	// It is not supported to use a single HostSelectionPolicy in multiple sessions
	// (even if you close the old session before using in a new session).
	HostSelectionPolicy HostSelectionPolicy
}

// MetadataCacheMode controls how the driver reads and caches schema metadata from Cassandra system tables.
// This affects the behavior of Session.KeyspaceMetadata and token-aware host selection policies.
//
// See the individual mode constants (Full, KeyspaceOnly, Disabled) for detailed behavior of each mode.
type MetadataCacheMode int

const (
	// Full mode reads and caches all schema metadata including keyspaces, tables, columns,
	// functions, aggregates, user-defined types, and materialized views.
	//
	// Token-aware routing works normally (if TokenAwareHostPolicy is used) with full replica information.
	// Session.KeyspaceMetadata returns cached metadata without querying system tables.
	//
	// It enables SchemaChangeListener to be notified about all schema changes.
	Full MetadataCacheMode = iota

	// KeyspaceOnly mode reads and caches only keyspace metadata (replication strategy and options).
	// This enables token-aware routing (if TokenAwareHostPolicy is used) without the overhead of caching detailed schema information.
	//
	// Token-aware routing works normally (if TokenAwareHostPolicy is used) with full replica information.
	// Session.KeyspaceMetadata returns cached keyspace metadata, but Tables, Functions, Aggregates,
	// MaterializedViews, and UserTypes fields will be nil.
	//
	// If CacheMode is Full, schema change listeners will be notified about all schema changes.
	//
	// Having other then KeyspaceChangeListener of schema events registered will result in an error during session creation.
	KeyspaceOnly

	// Disabled mode completely disables schema metadata caching.
	//
	// Token-aware routing falls back to the configured fallback policy (e.g., RoundRobinHostPolicy,
	// DCAwareRoundRobinPolicy) since replica information is not available.
	// Session.KeyspaceMetadata queries system tables on every call instead of using a cache.
	//
	// Having schema change listeners will result in an error during session creation.
	Disabled
)

func (p PoolConfig) buildPool(session *Session) *policyConnPool {
	return newPolicyConnPool(session)
}

// ClusterConfig is a struct to configure the default cluster implementation
// of gocql. It has a variety of attributes that can be used to modify the
// behavior to fit the most common use cases. Applications that require a
// different setup must implement their own cluster.
type ClusterConfig struct {
	// addresses for the initial connections. It is recommended to use the value set in
	// the Cassandra config for broadcast_address or listen_address, an IP address not
	// a domain name. This is because events from Cassandra will use the configured IP
	// address, which is used to index connected hosts. If the domain name specified
	// resolves to more than 1 IP address then the driver may connect multiple times to
	// the same host, and will not mark the node being down or up from events.
	Hosts []string

	// CQL version (default: 3.0.0)
	CQLVersion string

	// ProtoVersion sets the version of the native protocol to use, this will
	// enable features in the driver for specific protocol versions, generally this
	// should be set to a known version (2,3,4) for the cluster being connected to.
	//
	// If it is 0 or unset (the default) then the driver will attempt to discover the
	// highest supported protocol for the cluster. In clusters with nodes of different
	// versions the protocol selected is not defined (ie, it can be any of the supported in the cluster)
	ProtoVersion int

	// Timeout limits the time spent on the client side while executing a query.
	// Specifically, query or batch execution will return an error if the client does not receive a response
	// from the server within the Timeout period.
	// Timeout is also used to configure the read timeout on the underlying network connection.
	// Client Timeout should always be higher than the request timeouts configured on the server,
	// so that retries don't overload the server.
	// Timeout has a default value of 11 seconds, which is higher than default server timeout for most query types.
	// Timeout is not applied to requests during initial connection setup, see ConnectTimeout.
	Timeout time.Duration

	// ConnectTimeout limits the time spent during connection setup.
	// During initial connection setup, internal queries, AUTH requests will return an error if the client
	// does not receive a response within the ConnectTimeout period.
	// ConnectTimeout is applied to the connection setup queries independently.
	// ConnectTimeout also limits the duration of dialing a new TCP connection
	// in case there is no Dialer nor HostDialer configured,
	// and the duration of the TLS handshake that follows it whenever SslOpts is used.
	// A HostDialer of your own owns the bounding of everything it does.
	// ConnectTimeout has a default value of 11 seconds.
	ConnectTimeout time.Duration

	// HeartbeatTimeout bounds each heartbeat OPTIONS round-trip on a data connection.
	//
	// Zero or negative selects the 5-second default; a positive value is used as
	// given, including one below the 5-second heartbeat interval. This setting is
	// independent of Timeout: a longer query timeout no longer slows failure
	// detection.
	//
	// It bounds the wait for the response and, before that, the wait for the
	// connection's writer to accept the frame. It does not interrupt a socket write
	// already in progress: that is bounded by WriteTimeout, which itself defaults to
	// Timeout. Nor does it interrupt the wait for a frame the coalescer has accepted
	// but not yet flushed, which is WriteCoalesceWaitTime. On a connection whose
	// writes block, or with coalescing configured into the seconds, raising those
	// still delays a heartbeat however short this timeout is.
	//
	// A connection closes after six consecutive heartbeat failures, so with T the
	// effective timeout resolved above, the time to notice a node that stops
	// answering is roughly
	//
	//	delta + 5*max(5s, T) + T
	//
	// where delta is the wait until the connection's next heartbeat, in (0, 5s].
	// A connection whose very first heartbeat fails starts from a phase in
	// [2.5s, 5s) instead. The 5-second heartbeat interval dominates once T drops
	// below it, so values under 5s buy little: 5s gives 30-35s and the achievable
	// floor is about 25s.
	//
	// On a link where a multi-second OPTIONS round-trip is normal, a short value
	// turns transient jitter into the six-strike threshold and closes healthy
	// connections (upstream #1919).
	//
	// HeartbeatTimeout has a default value of 5 seconds.
	HeartbeatTimeout time.Duration

	// WriteTimeout limits the time the driver waits to write a request to a network connection.
	// WriteTimeout should be lower than or equal to Timeout.
	// WriteTimeout defaults to the value of Timeout.
	WriteTimeout time.Duration

	// Port used when dialing.
	// Default: 9042
	Port int

	// Initial keyspace. Optional.
	Keyspace string

	// The size of the connection pool for each host.
	// The pool filling runs in separate gourutine during the session initialization phase.
	// gocql will always try to get 1 connection on each host pool
	// during session initialization AND it will attempt
	// to fill each pool afterward asynchronously if NumConns > 1.
	// Notice: There is no guarantee that pool filling will be finished in the initialization phase.
	// Also, it describes a maximum number of connections at the same time.
	// Default: 2
	NumConns int

	// MaxStreams caps the number of concurrent request streams per connection.
	// It also sizes the per-connection request table (callMap), so lowering it
	// reduces per-connection memory: the table costs 8 bytes per stream, so the
	// default of 2048 uses 16 KB per connection versus 256 KB at the protocol
	// maximum of 32768.
	//
	// Real per-connection concurrency is typically a few hundred streams, so the
	// default is ample. Values are capped to the protocol maximum (128 for
	// protocol v1/v2, 32768 for v3+) and rounded up to a multiple of 64.
	//   0:  use the internal default (2048 for protocol v3+)
	//   <0: use the full protocol maximum
	//   >0: use this value (capped and rounded as above)
	// Default: 0
	MaxStreams int

	// Default consistency level.
	// Default: Quorum
	Consistency Consistency

	// Compression algorithm.
	// Default: nil
	Compressor Compressor

	// Default: nil
	Authenticator Authenticator

	// An Authenticator factory. Can be used to create alternative authenticators.
	// Default: nil
	AuthProvider func(h *HostInfo) (Authenticator, error)

	// Default retry policy to use for queries.
	// Default: no retries.
	RetryPolicy RetryPolicy

	// ConvictionPolicy decides whether to mark host as down based on the error and host info.
	// Default: SimpleConvictionPolicy
	ConvictionPolicy ConvictionPolicy

	// Default reconnection policy to use for reconnecting before trying to mark host as down.
	ReconnectionPolicy ReconnectionPolicy

	// The keepalive period to use, enabled if > 0 (default: 0)
	// SocketKeepalive is used to set up the default dialer and is ignored if Dialer or HostDialer is provided.
	SocketKeepalive time.Duration

	// Maximum cache size for prepared statements globally for gocql.
	// Default: 1000
	MaxPreparedStmts int

	// Maximum cache size for query info about statements for each session.
	// Default: 1000
	MaxRoutingKeyInfo int

	// Default page size to use for created sessions.
	// Default: 5000
	PageSize int

	// Consistency for the serial part of queries, values can be either SERIAL or LOCAL_SERIAL.
	// Default: unset
	SerialConsistency Consistency

	// SslOpts configures TLS use when HostDialer is not set.
	// SslOpts is ignored if HostDialer is set.
	SslOpts *SslOptions

	// Sends a client side timestamp for all requests which overrides the timestamp at which it arrives at the server.
	// Default: true, only enabled for protocol 3 and above.
	DefaultTimestamp bool

	// PoolConfig configures the underlying connection pool, allowing the
	// configuration of host selection and connection selection policies.
	PoolConfig PoolConfig

	// If not zero, gocql attempt to reconnect known DOWN nodes in every ReconnectInterval.
	//
	// While a node is DOWN, every interval also re-reads the peers table over the
	// control connection, so a node that rejoined at a different address is found
	// without relying on server events; nothing is read while every node is UP.
	// With DisableInitialHostLookup that re-read can be the first ring refresh of the
	// session, which is when placeholder host IDs are replaced by the real ones and
	// the affected pools are rebuilt; that used to wait for the first event- or
	// reconnect-driven refresh.
	//
	// Setting it to zero disables that sweep, and no other component takes the job over: a
	// host the driver has convicted then stays down until the server sends an UP event for
	// it. Leave it set unless something outside the driver owns recovery.
	//
	// Default: 60s.
	ReconnectInterval time.Duration

	// The maximum amount of time to wait for schema agreement in a cluster after
	// receiving a schema change frame. (default: 60s)
	MaxWaitSchemaAgreement time.Duration

	// HostFilter will filter all incoming events for host, any which don't pass
	// the filter will be ignored. If set will take precedence over any options set
	// via Discovery
	HostFilter HostFilter

	// AddressTranslator will translate addresses found on peer discovery and/or
	// node change events.
	AddressTranslator AddressTranslator

	// If IgnorePeerAddr is true and the address in system.peers does not match
	// the supplied host by either initial hosts or discovered via events then the
	// host will be replaced with the supplied address.
	//
	// For example if an event comes in with host=10.0.0.1 but when looking up that
	// address in system.local or system.peers returns 127.0.0.1, the peer will be
	// set to 10.0.0.1 which is what will be used to connect to.
	IgnorePeerAddr bool

	// If DisableInitialHostLookup then the driver will not attempt to get host info
	// from the system.peers table, this will mean that the driver will connect to
	// hosts supplied and will not attempt to lookup the hosts information, this will
	// mean that data_center, rack and token information will not be available and as
	// such host filtering and token aware query routing will not be available.
	DisableInitialHostLookup bool

	// Configure events the driver will register for
	Events struct {
		// Disable registering for status events (host up/down)
		DisableNodeStatusEvents bool
		// Disable registering for topology events (node added/removed/moved)
		DisableTopologyEvents bool
		// Disable registering for schema events (keyspace/table/function removed/created/updated)
		DisableSchemaEvents bool
	}

	// DisableSkipMetadata will override the internal result metadata cache so that the driver does not
	// send skip_metadata for queries, this means that the result will always contain
	// the metadata to parse the rows and will not reuse the metadata from the prepared
	// statement.
	//
	// See https://issues.apache.org/jira/browse/CASSANDRA-10786
	DisableSkipMetadata bool

	// QueryObserver will set the provided query observer on all queries created from this session.
	// Use it to collect metrics / stats from queries by providing an implementation of QueryObserver.
	QueryObserver QueryObserver

	// BatchObserver will set the provided batch observer on all queries created from this session.
	// Use it to collect metrics / stats from batch queries by providing an implementation of BatchObserver.
	BatchObserver BatchObserver

	// ConnectObserver will set the provided connect observer on all queries
	// created from this session.
	ConnectObserver ConnectObserver

	// FrameHeaderObserver will set the provided frame header observer on all frames' headers created from this session.
	// Use it to collect metrics / stats from frames by providing an implementation of FrameHeaderObserver.
	FrameHeaderObserver FrameHeaderObserver

	// StreamObserver will be notified of stream state changes.
	// This can be used to track in-flight protocol requests and responses.
	StreamObserver StreamObserver

	// Default idempotence for queries
	DefaultIdempotence bool

	// The time to wait for frames before flushing the frames connection to Cassandra.
	// Can help reduce syscall overhead by making less calls to write. Set to 0 to
	// disable.
	//
	// (default: 200 microseconds)
	WriteCoalesceWaitTime time.Duration

	// Dialer will be used to establish all connections created for this Cluster.
	// If not provided, a default dialer configured with ConnectTimeout will be used.
	// Dialer is ignored if HostDialer is provided.
	Dialer Dialer

	// HostDialer will be used to establish all connections for this Cluster.
	// If not provided, Dialer will be used instead.
	HostDialer HostDialer

	// StructuredLogger for this ClusterConfig.
	//
	// There are 3 built in implementations of StructuredLogger:
	//  - std library "log" package: gocql.NewLogger
	//  - zerolog: gocqlzerolog.NewZerologLogger
	//  - zap: gocqlzap.NewZapLogger
	//
	// You can also provide your own logger implementation of the StructuredLogger interface.
	Logger StructuredLogger

	// Tracer will be used for all queries. Alternatively it can be set of on a
	// per query basis.
	// default: nil
	Tracer Tracer

	// NextPagePrefetch sets the default threshold for pre-fetching new pages. If
	// there are only p*pageSize rows remaining, the next page will be requested
	// automatically. This value can also be changed on a per-query basis.
	// default: 0.25.
	NextPagePrefetch float64

	// RegisteredTypes will be copied for all sessions created from this Cluster.
	// If not provided, a copy of GlobalTypes will be used.
	RegisteredTypes *RegisteredTypes

	// internal config for testing
	disableControlConn bool

	// heartbeatInterval overrides the steady-state heartbeat interval (internal, for testing);
	// zero selects the heartbeatInterval constant.
	heartbeatInterval time.Duration

	// heartbeatPhase overrides the wait before a connection's first heartbeat (internal, for testing);
	// nil selects defaultHeartbeatPhase.
	heartbeatPhase func(interval time.Duration) time.Duration

	// testPoolHook is called at the connection-pool fill checkpoints named by
	// poolEvent (internal, for testing); nil in production.
	testPoolHook func(ev poolEvent, host *HostInfo)

	// testRingRefreshHook is called when a ring refresh starts
	// and testRingRefreshDone when it ends, with its result (internal, for testing);
	// both nil in production.
	testRingRefreshHook func()
	testRingRefreshDone func(err error)

	// testInitPublishHook is called by Session.init immediately before it publishes
	// the initial hosts to the selection policy (internal, for testing); nil in production.
	testInitPublishHook func()

	// testRingSnapshotHook is called by ringDescriber.GetHosts between its
	// system.local and its peers read (internal, for testing); nil in production.
	testRingSnapshotHook func()

	// testSchemaRefreshHook is called when a schema refresh starts
	// and testSchemaRefreshDone when it ends, with its result (internal, for testing);
	// both nil in production.
	testSchemaRefreshHook func()
	testSchemaRefreshDone func(err error)

	// testControlReconnectDone is called when controlConn.reconnect returns,
	// after it has released its reconnecting claim (internal, for testing); nil in production.
	testControlReconnectDone func()

	// testControlBeforePublish is called by controlConn.setupConn after events are
	// registered and before the connection is published, with the host being set up
	// (internal, for testing); nil in production.
	testControlBeforePublish func(host *HostInfo)

	// testControlBeforeFallback is called by controlConn.attemptReconnect after the
	// ring walk failed with an ordinary error and before the contact-point fallback
	// is admitted (internal, for testing); nil in production.
	testControlBeforeFallback func()

	// testControlBeforeResolve is called by controlConn.attemptReconnect just before
	// it resolves the contact points with addrsToHosts, after the pre-fallback
	// shutdown check has admitted the fallback (internal, for testing); nil in
	// production.
	// A test asserts it never fired to prove resolution was skipped.
	testControlBeforeResolve func()

	// testControlAfterSetupFailure is called by the control connection's per-attempt
	// cleanup when a candidate's setup failed, while the candidate is still in the
	// set and before it is closed, so a test can latch a shutdown into that window
	// (internal, for testing); nil in production.
	testControlAfterSetupFailure func()

	// testStartPoolFillStart is called at the start of Session.startPoolFill,
	// before pool admission and the withOwnedHost publication, so a test can gate
	// that goroutine before it reaches the shutdown gate (internal, for testing);
	// nil in production.
	testStartPoolFillStart func(host *HostInfo)

	// testStartPoolFillDone is called at the end of Session.startPoolFill, after the
	// withOwnedHost publication attempt, so a test can join that goroutine
	// (internal, for testing); nil in production.
	testStartPoolFillDone func(host *HostInfo)

	// schemaRefreshDebounce overrides the schema refresh debounce interval (internal, for testing);
	// zero selects the schemaRefreshDebounceTime constant.
	schemaRefreshDebounce time.Duration

	// Metadata configures driver's internal metadata caching and event listening.
	Metadata MetadataConfig
}

// Dialer is the interface that wraps the DialContext method for establishing network connections to Cassandra nodes.
//
// This interface allows customization of how gocql establishes TCP connections, which is useful for:
// connecting through proxies or load balancers, custom TLS configurations, custom timeouts/keep-alive
// settings, service mesh integration, testing with mocked connections, and corporate network routing.
type Dialer interface {
	DialContext(ctx context.Context, network, addr string) (net.Conn, error)
}

// NewCluster generates a new config for the default cluster implementation.
//
// The supplied hosts are used to initially connect to the cluster then the rest of
// the ring will be automatically discovered. It is recommended to use the value set in
// the Cassandra config for broadcast_address or listen_address, an IP address not
// a domain name. This is because events from Cassandra will use the configured IP
// address, which is used to index connected hosts. If the domain name specified
// resolves to more than 1 IP address then the driver may connect multiple times to
// the same host, and will not mark the node being down or up from events.
func NewCluster(hosts ...string) *ClusterConfig {
	cfg := &ClusterConfig{
		Hosts:                  hosts,
		CQLVersion:             "3.0.0",
		Timeout:                11 * time.Second,
		ConnectTimeout:         11 * time.Second,
		HeartbeatTimeout:       heartbeatDefaultTimeout,
		Port:                   9042,
		NumConns:               2,
		Consistency:            Quorum,
		MaxPreparedStmts:       defaultMaxPreparedStmts,
		MaxRoutingKeyInfo:      1000,
		PageSize:               5000,
		DefaultTimestamp:       true,
		MaxWaitSchemaAgreement: 60 * time.Second,
		ReconnectInterval:      60 * time.Second,
		ConvictionPolicy:       &SimpleConvictionPolicy{},
		ReconnectionPolicy:     &ConstantReconnectionPolicy{MaxRetries: 3, Interval: 1 * time.Second},
		WriteCoalesceWaitTime:  200 * time.Microsecond,
		NextPagePrefetch:       0.25,
		Metadata: MetadataConfig{
			CacheMode: Full,
		},
	}
	return cfg
}

func (cfg *ClusterConfig) newLogger() StructuredLogger {
	if cfg.Logger != nil {
		return cfg.Logger
	}
	return NewLogger(LogLevelNone)
}

// CreateSession initializes the cluster based on this config and returns a
// session object that can be used to interact with the database.
func (cfg *ClusterConfig) CreateSession() (*Session, error) {
	return NewSession(*cfg)
}

// translateAddressPort is a helper method that will use the given AddressTranslator
// if defined, to translate the given address and port into a possibly new address
// and port, If no AddressTranslator or if an error occurs, the given address and
// port will be returned.
func (cfg *ClusterConfig) translateAddressPort(addr net.IP, port int, logger StructuredLogger) (net.IP, int) {
	if cfg.AddressTranslator == nil || len(addr) == 0 {
		return addr, port
	}
	newAddr, newPort := cfg.AddressTranslator.Translate(addr, port)
	logger.Debug("Translating address.",
		NewLogFieldIP("old_addr", addr), NewLogFieldInt("old_port", port),
		NewLogFieldIP("new_addr", newAddr), NewLogFieldInt("new_port", newPort))
	return newAddr, newPort
}

func (cfg *ClusterConfig) filterHost(host *HostInfo) bool {
	return !(cfg.HostFilter == nil || cfg.HostFilter.Accept(host))
}

// MetadataConfig configures driver's internal metadata caching and event listening.
type MetadataConfig struct {
	// CacheMode controls how the driver reads and caches schema metadata from Cassandra system tables.
	//
	// Also, it affects the behavior of schema change listeners.
	//
	// If CacheMode is [KeyspaceOnly], only [KeyspaceChangeListener] will be notified,
	// having other listeners registered will result in an error during session creation.
	//
	// If CacheMode is [Disabled], having these listeners will result in an error during session creation.
	//
	// See [MetadataCacheMode] for more details.
	CacheMode MetadataCacheMode

	// HostListener will be notified when host state and topology changes occur.
	//
	// Thread Safety: Topology change callbacks are sequential, but host status callbacks can be concurrent.
	// If your listener implements both TopologyChangeListener and HostStatusChangeListener, it must be
	// thread-safe as these event types can run simultaneously from different sources.
	//
	// Consider using [HostListenersMux] if you need to register multiple listeners for the same type of host state and topology change.
	HostListener HostListenersConfig

	// SchemaListener will be notified when schema changes occur.
	//
	// Consider using [SchemaListenersMux] if you need to register multiple listeners for the same type of schema change.
	SchemaListener SchemaListenersConfig

	// SessionReadyListener will be notified when the session is ready to be used.
	// This is meant to be implemented by Host and Schema listeners but it can also be used as
	// a generic callback for when the session is ready regardless of whether a metadata listener is implemented or not.
	//
	// Consider using [SessionReadyListenersMux] if you need to register multiple listeners for the same session ready event.
	SessionReadyListener SessionReadyListener
}

type HostListenersConfig struct {
	// HostStateChangeListener will be notified about host state events (UP, DOWN).
	HostStateChangeListener HostStatusChangeListener

	// TopologyChangeListener will be notified about topology change events
	// (NEW_NODE, REMOVED_NODE).
	TopologyChangeListener TopologyChangeListener
}

type SchemaListenersConfig struct {
	KeyspaceChangeListener  KeyspaceChangeListener
	TableChangeListener     TableChangeListener
	UserTypeChangeListener  UserTypeChangeListener
	FunctionChangeListener  FunctionChangeListener
	AggregateChangeListener AggregateChangeListener
}

var (
	// ErrNoHosts is returned when no hosts are provided to the cluster configuration.
	ErrNoHosts = errors.New("no hosts provided")
	// ErrNoConnectionsStarted is returned when no connections could be established during session creation.
	ErrNoConnectionsStarted = errors.New("no connections were made when creating the session")
	// Deprecated: Never used or returned by the driver.
	ErrHostQueryFailed = errors.New("unable to populate Hosts")
)
