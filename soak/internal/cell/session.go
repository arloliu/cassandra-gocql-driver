// Package cell assembles one soak cell: driver sessions configured per PLAN §3.4,
// wired to the harness's registry, observers and loggers.
package cell

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
	"github.com/apache/cassandra-gocql-driver/v2/lz4"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/config"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/proxy"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/view"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// Driver settings (PLAN §3.4).
const (
	// NumConns is the connections per host.
	NumConns = 2
	// Timeout is the per-connection response timeout.
	Timeout = 2 * time.Second
	// ConnectTimeout bounds one dial and handshake.
	ConnectTimeout = 5 * time.Second
	// ReconnectInterval is how often a DOWN host is redialled.
	ReconnectInterval = 10 * time.Second
)

// ChurnSpace returns the size of the churn class's statement space: 4 × MaxPreparedStmts (PLAN §4.2).
//
// Returns:
//   - int: the number of distinct churn statements
func ChurnSpace() int {
	return 4 * gocql.NewCluster().MaxPreparedStmts
}

// DriverSettings returns the session configuration as the cell's config records it (PLAN §3.4, §9 step 8).
//
// Returns:
//   - config.Driver: the settings Configure applies, and the per-op deadlines the workload uses
func DriverSettings() config.Driver {
	return config.Driver{
		NumConns: NumConns, Consistency: gocql.Quorum.String(), SerialConsistency: gocql.Serial.String(),
		Timeout: Timeout, ConnectTimeout: ConnectTimeout, ReconnectInterval: ReconnectInterval,
		HostPolicy: "TokenAwareHostPolicy(RoundRobinHostPolicy())", OpDeadline: workload.OpDeadline,
		ShortDeadlineMin: workload.ShortDeadlineMin, ShortDeadlineMax: workload.ShortDeadlineMax, Retries: workload.Retries,
	}
}

// SessionSpec describes one driver session.
type SessionSpec struct {
	// ID is the harness's session id, "primary" or "auxN".
	ID string
	// Generation counts earlier sessions with this id.
	Generation int
	// ContactPoints are the proxies, e.g. 127.0.1.1:19042.
	ContactPoints []string
	// ProtoVersion is the cell's protocol.
	ProtoVersion int
	// Registry records the session's dials (D8).
	Registry *view.Registry
	// HostEvents receives the session's host notifications; may be nil.
	HostEvents func(view.HostEvent)
	// ConnectEvents receives every connection attempt the driver reports (ConnectObserver, PLAN §3.4); may be nil.
	ConnectEvents func(ConnectEvent)
	// Attempts receives every failed query attempt; may be nil.
	Attempts func(workload.AttemptRecord)
	// DriverLog receives the session's driver log lines (Info and above).
	DriverLog io.Writer
	// Settlement receives the session's G15 observations.
	Settlement *workload.Settlement
}

// Session is an open driver session with its instruments.
type Session struct {
	*gocql.Session
	// ID is the harness's session id.
	ID string
	// Streams counts the session's streams (G6).
	Streams *probe.StreamCounters
	// Logger counts the session's G7 lines.
	Logger *probe.CountingLogger
	// Membership records host notifications.
	Membership *view.Membership
	// Observer routes query observations to the settlement table.
	Observer *workload.Observer
	// Frames records the protocol versions seen on received frames (G0).
	Frames *FrameVersions
}

// ConnectEvent is one connection attempt a session's ConnectObserver saw.
type ConnectEvent struct {
	// Session is the harness's session id; Host the host dialled.
	Session string `json:"session"`
	Host    string `json:"host"`
	// Start and End bracket the dial and handshake.
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	// Err is the failure, empty on success.
	Err string `json:"err,omitempty"`
}

// connectObserver forwards ConnectObserver calls to a sink.
type connectObserver struct {
	session string
	sink    func(ConnectEvent)
}

var _ gocql.ConnectObserver = connectObserver{}

// ObserveConnect records one connection attempt.
func (o connectObserver) ObserveConnect(c gocql.ObservedConnect) {
	e := ConnectEvent{Session: o.session, Start: c.Start, End: c.End}
	if c.Host != nil {
		e.Host = c.Host.ConnectAddressAndPort()
	}
	if c.Err != nil {
		e.Err = c.Err.Error()
	}
	o.sink(e)
}

// FrameVersions is a FrameHeaderObserver recording which protocol versions responses carried.
type FrameVersions struct {
	seen atomic.Uint64
}

var _ gocql.FrameHeaderObserver = (*FrameVersions)(nil)

// proxyTranslator maps a discovered peer's native port to its proxy port (PLAN §3.3).
type proxyTranslator struct{}

var _ gocql.AddressTranslator = proxyTranslator{}

// Open builds and opens a session.
// gocql.ClusterConfig.CreateSession takes no context,
// so a caller that needs a bound runs Open in an owner goroutine (PLAN §5.4, churn).
//
// Parameters:
//   - spec: the session
//
// Returns:
//   - *Session: the open session
//   - error: from CreateSession
func Open(spec SessionSpec) (*Session, error) {
	cfg, s := Configure(spec)
	gs, err := cfg.CreateSession()
	if err != nil {
		return nil, err
	}
	s.Session = gs
	return s, nil
}

// Configure returns the session's ClusterConfig and its instruments, without opening it.
//
// Parameters:
//   - spec: the session
//
// Returns:
//   - *gocql.ClusterConfig: the config
//   - *Session: the instruments; its Session field is nil until opened
func Configure(spec SessionSpec) (*gocql.ClusterConfig, *Session) {
	s := &Session{
		ID:         spec.ID,
		Streams:    &probe.StreamCounters{},
		Logger:     probe.NewCountingLogger(spec.ID, spec.DriverLog),
		Membership: view.NewMembership(spec.ID, spec.HostEvents),
		Observer:   workload.NewObserver(spec.Settlement),
		Frames:     &FrameVersions{},
	}
	cfg := gocql.NewCluster(spec.ContactPoints...)
	cfg.ProtoVersion = spec.ProtoVersion
	cfg.Compressor = lz4.LZ4Compressor{}
	cfg.NumConns = NumConns
	cfg.Consistency = gocql.Quorum
	cfg.SerialConsistency = gocql.Serial
	cfg.Timeout = Timeout
	cfg.ConnectTimeout = ConnectTimeout
	cfg.ReconnectInterval = ReconnectInterval
	cfg.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cfg.AddressTranslator = proxyTranslator{}
	cfg.Dialer = spec.Registry.Dialer(spec.ID, spec.Generation, cfg.ConnectTimeout, cfg.SocketKeepalive)
	cfg.StreamObserver = s.Streams
	cfg.QueryObserver = s.Observer
	cfg.BatchObserver = s.Observer
	cfg.FrameHeaderObserver = s.Frames
	if spec.Attempts != nil {
		s.Observer.OnAttemptError(spec.Attempts)
	}
	if spec.ConnectEvents != nil {
		cfg.ConnectObserver = connectObserver{session: spec.ID, sink: spec.ConnectEvents}
	}
	cfg.Metadata.HostListener = s.Membership.Listeners()
	cfg.Logger = s.Logger
	return cfg, s
}

// ObserveFrameHeader records a received frame's protocol version.
func (f *FrameVersions) ObserveFrameHeader(_ context.Context, h gocql.ObservedFrameHeader) {
	v := byte(h.Version) & 0x7f
	if v < 64 {
		f.seen.Or(1 << v)
	}
}

// Versions returns the protocol versions seen so far.
//
// Returns:
//   - []int: the versions, ascending
func (f *FrameVersions) Versions() []int {
	bits := f.seen.Load()
	var out []int
	for v := range 64 {
		if bits&(1<<v) != 0 {
			out = append(out, v)
		}
	}
	return out
}

// Translate maps (ip, 9042) to (ip, 19042) and leaves every other port alone.
func (proxyTranslator) Translate(addr net.IP, port int) (net.IP, int) {
	if port == proxy.NodePort {
		return addr, proxy.ProxyPort
	}
	return addr, port
}
