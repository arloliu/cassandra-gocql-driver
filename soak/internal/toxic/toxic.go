// Package toxic runs the toxic faults (PLAN §59.3): F-lat, F-bw, F-black, F-halfopen and F-reset.
//
// A fault opens harness-owned probe connections to its target's proxy before adding any toxic, runs a control
// round on them with no toxic present, then adds its toxics and repeats effect rounds until one holds.
// F-black and F-halfopen keep their probe connections open through the active period for the removal check.
package toxic

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"slices"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/proxy"
)

// Probe statements and limits (PLAN §59.3).
const (
	// BlobKey is the smallest blob row, which F-bw's probe reads; BlobSize is its payload, 264 134 B.
	BlobKey  = 25
	BlobSize = 264134
	// MinTransfer is the bytes F-bw needs after the transfer's first byte before it judges the rate.
	MinTransfer = 16384
	// MinSpan is the time F-bw needs after the transfer's first byte before it judges the rate.
	MinSpan = time.Second
	// LatencyRise is how much F-lat's round trip must exceed the baseline.
	LatencyRise = 150 * time.Millisecond
	// baselineTrips is how many round trips F-lat's baseline takes.
	baselineTrips = 5
)

const (
	stmtBlob    = "SELECT payload FROM soak.blob WHERE k = 25"
	stmtRelease = "SELECT release_version FROM system.local"
	// frameHeader is a protocol-4 frame header's length.
	frameHeader = 9
)

// ErrNotHeld is an effect round that ended without the effect holding; the round loop retries it.
var ErrNotHeld = errors.New("toxic: effect not observed")

// Ops is toxiproxy's API as a fault uses it; *proxy.Client implements it.
type Ops interface {
	AddToxic(ctx context.Context, proxyName string, t proxy.Toxic) error
	UpdateToxic(ctx context.Context, proxyName, name string, toxicity float64) error
	RemoveToxic(ctx context.Context, proxyName, name string) error
	Toxics(ctx context.Context, proxyName string) ([]string, error)
}

// Conn is a probe connection; *proxy.Probe implements it.
type Conn interface {
	Name() string
	Local() netip.AddrPort
	Request(ctx context.Context, op byte, body []byte) (int16, error)
	Events() <-chan proxy.FrameEvent
	Close()
}

// Opener opens a probe connection to the target's proxy, completing STARTUP with a DRIVER_NAME.
type Opener func(ctx context.Context, driverName string) (Conn, error)

// Sockets reads the kernel's view of a probe connection; production runs nodetool clientstats and ss.
type Sockets interface {
	// Upstream finds the probe's row in the target's clientstats: its Address is toxiproxy's end of the upstream link.
	Upstream(ctx context.Context, driverName string) (proxy.ClientRow, error)
	// Socket reads the one established socket with these endpoints.
	Socket(ctx context.Context, local, remote netip.AddrPort) (proxy.SocketInfo, error)
}

// Timing is the fault's round bounds; production uses DefaultTiming, tests shrink it.
type Timing struct {
	// Round bounds an F-lat, F-bw or F-reset round, and each probe exchange of a control round.
	Round time.Duration
	// Silence is how long F-black and F-halfopen wait for a response that must not come.
	Silence time.Duration
	// Poll is the gap between effect rounds.
	Poll time.Duration
}

// Config describes one toxic fault.
type Config struct {
	// Kind is the fault kind.
	Kind chaos.Kind
	// Seq numbers the cell's toxic faults from 1; it names the toxics and the probe connections.
	Seq int
	// Proxy is the target's proxy name, nodeN; Node is the target's address, 127.0.1.N.
	Proxy string
	Node  netip.Addr
	// Param is F-lat's latency in ms or F-bw's rate in KB/s.
	Param int
	// Ops, Open and Sockets reach toxiproxy, the proxy and the kernel.
	Ops     Ops
	Open    Opener
	Sockets Sockets
	// Event records a toxic or effect event; may be nil.
	Event func(name string, data any)
	// Timing overrides DefaultTiming when non-zero.
	Timing Timing
}

// Fault is one toxic fault; it implements chaos.Fault and chaos.Releaser.
type Fault struct {
	cfg       Config
	t         Timing
	probes    []*probe
	installed bool
	baseline  time.Duration
}

var (
	_ chaos.Fault    = (*Fault)(nil)
	_ chaos.Releaser = (*Fault)(nil)
)

// Snapshot is one reading of a probe connection's two sockets (PLAN §59.3).
type Snapshot struct {
	// CassReceived and CassAcked are Cassandra's socket's bytes_received and bytes_acked.
	CassReceived, CassAcked int64
	// ProxyReceived and ProxyRecvQ are toxiproxy's accepted socket's bytes_received and Recv-Q.
	ProxyReceived, ProxyRecvQ int64
	// Requests is Cassandra's dispatch count for the connection, for diagnosis only.
	Requests int64
}

// Record is a toxic fault's toxics, for the toxic event.
type Record struct {
	// Fault is the fault's toxic sequence number.
	Fault int `json:"fault"`
	// Kind is the fault kind; Proxy the target's proxy.
	Kind  chaos.Kind `json:"kind"`
	Proxy string     `json:"proxy"`
	// Toxics are the toxics, in the order they are added.
	Toxics []proxy.Toxic `json:"toxics"`
}

// Round is one effect or control round's record, for the effect event.
type Round struct {
	// Fault is the fault's toxic sequence number, Stage the F-black stage ("down", "up") or "".
	Fault int    `json:"fault"`
	Stage string `json:"stage,omitempty"`
	// N counts rounds from 1; 0 is the control round.
	N int `json:"n"`
	// Held reports whether the effect held, or the control round showed the unfaulted outcome.
	Held bool `json:"held"`
	// Detail is the measurement.
	Detail string `json:"detail"`
}

// New returns a toxic fault.
//
// Parameters:
//   - cfg: the fault
//
// Returns:
//   - *Fault: the fault
//   - error: for a kind that is not a toxic kind, or F-lat and F-bw without a parameter
func New(cfg Config) (*Fault, error) {
	switch cfg.Kind {
	case chaos.FaultLat, chaos.FaultBW:
		if cfg.Param <= 0 {
			return nil, fmt.Errorf("toxic: %s needs a parameter", cfg.Kind)
		}
	case chaos.FaultBlack, chaos.FaultHalfOpen, chaos.FaultReset:
	default:
		return nil, fmt.Errorf("toxic: %s is not a toxic kind", cfg.Kind)
	}
	t := cfg.Timing
	if t == (Timing{}) {
		t = DefaultTiming()
	}
	return &Fault{cfg: cfg, t: t}, nil
}

// DefaultTiming is PLAN §59.3's: 5 s rounds, 3 s of silence, 500 ms between rounds.
func DefaultTiming() Timing {
	return Timing{Round: 5 * time.Second, Silence: 3 * time.Second, Poll: 500 * time.Millisecond}
}

// Toxics returns a kind's toxics in the order they are added (PLAN §59.3's table).
//
// Parameters:
//   - kind: the fault kind
//   - seq: the fault's sequence number
//   - param: F-lat's latency in ms or F-bw's rate in KB/s
//
// Returns:
//   - []proxy.Toxic: the toxics; nil for a kind that is not a toxic kind
func Toxics(kind chaos.Kind, seq, param int) []proxy.Toxic {
	down := fmt.Sprintf("soak-t%d-down", seq)
	up := fmt.Sprintf("soak-t%d-up", seq)
	mk := func(name, typ, stream string, attrs map[string]any) proxy.Toxic {
		return proxy.Toxic{Name: name, Type: typ, Stream: stream, Toxicity: 1, Attributes: attrs}
	}
	switch kind {
	case chaos.FaultLat:
		return []proxy.Toxic{mk(down, "latency", "downstream", map[string]any{"latency": param, "jitter": param / 5})}
	case chaos.FaultBW:
		return []proxy.Toxic{mk(down, "bandwidth", "downstream", map[string]any{"rate": param})}
	case chaos.FaultBlack:
		return []proxy.Toxic{mk(down, "timeout", "downstream", map[string]any{"timeout": 0}),
			mk(up, "timeout", "upstream", map[string]any{"timeout": 0})}
	case chaos.FaultHalfOpen:
		return []proxy.Toxic{mk(down, "timeout", "downstream", map[string]any{"timeout": 0})}
	case chaos.FaultReset:
		return []proxy.Toxic{mk(up, "reset_peer", "upstream", map[string]any{"timeout": 0})}
	}
	return nil
}

// RateLimit returns F-bw's ceiling, 2 × R × 1000 B/s; toxiproxy's KB is 1000 B.
//
// Parameters:
//   - rate: R in KB/s
//
// Returns:
//   - float64: bytes per second
func RateLimit(rate int) float64 { return 2 * float64(rate) * 1000 }

// SustainedRate is the bytes received after a transfer's first byte over the time from that byte to the last.
//
// Parameters:
//   - received: body bytes received
//   - first, last: when the frame's header and its last byte arrived
//
// Returns:
//   - float64: bytes per second; +Inf when no time passed
func SustainedRate(received int, first, last time.Time) float64 {
	span := last.Sub(first).Seconds()
	if span <= 0 {
		return math.Inf(1)
	}
	return float64(received) / span
}

// BandwidthHolds judges an F-bw transfer so far (PLAN §59.3).
//
// Parameters:
//   - received: body bytes received after the first byte
//   - first, last: when the first and the last byte arrived
//   - done: whether the transfer ended
//   - limit: RateLimit(R)
//
// Returns:
//   - decided: whether the transfer can be judged now
//   - holds: whether the effect holds; meaningful only when decided
func BandwidthHolds(received int, first, last time.Time, done bool, limit float64) (decided, holds bool) {
	span := last.Sub(first)
	if received >= MinTransfer && span >= MinSpan {
		return true, SustainedRate(received, first, last) < limit
	}
	if done {
		// A transfer that ended before both thresholds was fast, or too small to judge: it fails the round.
		return true, false
	}
	return false, false
}

// ReceiptHolds judges F-halfopen and F-black's downstream stage: Cassandra received the request and sent its
// response, which never reached the client (PLAN §59.3).
//
// Parameters:
//   - before, after: snapshots around the silence
//   - reqLen: the request frame's length
//   - respLen: the control round's response frame length
//
// Returns:
//   - bool: true when the effect holds
func ReceiptHolds(before, after Snapshot, reqLen, respLen int) bool {
	return after.CassReceived-before.CassReceived >= int64(reqLen) && after.CassAcked-before.CassAcked >= int64(respLen)
}

// DropHolds judges F-black's upstream stage: toxiproxy read the whole request and Cassandra received none of it.
//
// Parameters:
//   - before, after: snapshots around the silence
//   - reqLen: the request frame's length
//
// Returns:
//   - bool: true when the effect holds
func DropHolds(before, after Snapshot, reqLen int) bool {
	return after.ProxyReceived-before.ProxyReceived >= int64(reqLen) && after.ProxyRecvQ == 0 &&
		after.CassReceived == before.CassReceived
}

// Install opens the probe connections, runs the control round, adds the toxics and waits for the effect.
//
// Parameters:
//   - ctx: the install bound
//
// Returns:
//   - error: a failed control round, a failed add, or the effect not observed before ctx ends
func (f *Fault) Install(ctx context.Context) error {
	if err := f.open(ctx); err != nil {
		return err
	}
	if err := f.control(ctx); err != nil {
		return fmt.Errorf("control round: %w", err)
	}
	toxics := Toxics(f.cfg.Kind, f.cfg.Seq, f.cfg.Param)
	f.event("toxic", Record{Fault: f.cfg.Seq, Kind: f.cfg.Kind, Proxy: f.cfg.Proxy, Toxics: toxics})
	if f.cfg.Kind == chaos.FaultBlack {
		if err := f.add(ctx, toxics[0]); err != nil {
			return err
		}
		if err := f.rounds(ctx, "down", f.probes[0], f.receiptRound); err != nil {
			return err
		}
		if err := f.add(ctx, toxics[1]); err != nil {
			return err
		}
		if err := f.rounds(ctx, "up", f.probes[1], f.dropRound); err != nil {
			return err
		}
	} else {
		for _, t := range toxics {
			if err := f.add(ctx, t); err != nil {
				return err
			}
		}
		if err := f.rounds(ctx, "", f.probes[0], f.effectRound()); err != nil {
			return err
		}
	}
	f.installed = true
	if !f.retains() {
		f.Release()
	}
	return nil
}

// Hold waits for the active duration.
//
// Parameters:
//   - ctx: the lifecycle's outer bound
//   - d: the active duration
//
// Returns:
//   - error: ctx's error if it ends first
func (f *Fault) Hold(ctx context.Context, d time.Duration) error { return chaos.Sleep(ctx, d) }

// Remove undoes the fault: after a failed install, it deletes every toxic the fault may have added; after a
// successful one, F-black and F-halfopen first update their toxics to toxicity 0 and run the barrier (PLAN §59.3).
//
// Parameters:
//   - ctx: the remove bound
//
// Returns:
//   - error: a failed deletion, update, barrier or listing check
func (f *Fault) Remove(ctx context.Context) error {
	toxics := Toxics(f.cfg.Kind, f.cfg.Seq, f.cfg.Param)
	if !f.installed {
		return f.delete(ctx, toxics)
	}
	if f.retains() {
		if err := f.drop(ctx, toxics); err != nil {
			// The toxics are still deleted, so a failed proof leaves no toxic behind (§64 BW04).
			return errors.Join(err, f.delete(ctx, toxics))
		}
	}
	if err := f.delete(ctx, toxics); err != nil {
		return err
	}
	names, err := f.cfg.Ops.Toxics(ctx, f.cfg.Proxy)
	if err != nil {
		return fmt.Errorf("list toxics: %w", err)
	}
	for _, t := range toxics {
		if slices.Contains(names, t.Name) {
			return fmt.Errorf("toxic %s still present on %s", t.Name, f.cfg.Proxy)
		}
	}
	f.Release()
	return nil
}

// Release closes every probe connection and waits for its reader; it is idempotent.
func (f *Fault) Release() {
	for _, p := range f.probes {
		p.conn.Close()
	}
}

func (f *Fault) open(ctx context.Context) error {
	names := []string{fmt.Sprintf("soak-probe-t%d-a", f.cfg.Seq)}
	if f.cfg.Kind == chaos.FaultBlack {
		names = append(names, fmt.Sprintf("soak-probe-t%d-b", f.cfg.Seq))
	}
	for _, n := range names {
		c, err := f.cfg.Open(ctx, n)
		if err != nil {
			return fmt.Errorf("open probe %s: %w", n, err)
		}
		f.probes = append(f.probes, &probe{conn: c})
	}
	return nil
}

// retains reports whether the probe connections stay open through the active period.
func (f *Fault) retains() bool {
	return f.cfg.Kind == chaos.FaultBlack || f.cfg.Kind == chaos.FaultHalfOpen
}

func (f *Fault) add(ctx context.Context, t proxy.Toxic) error {
	if err := f.cfg.Ops.AddToxic(ctx, f.cfg.Proxy, t); err != nil {
		return fmt.Errorf("add %s: %w", t.Name, err)
	}
	return nil
}

// drop updates the toxics to toxicity 0 and runs the barrier on every retained probe connection (PLAN §59.3).
func (f *Fault) drop(ctx context.Context, toxics []proxy.Toxic) error {
	for _, t := range toxics {
		if err := f.cfg.Ops.UpdateToxic(ctx, f.cfg.Proxy, t.Name, 0); err != nil {
			return fmt.Errorf("update %s to toxicity 0: %w", t.Name, err)
		}
	}
	for i, p := range f.probes {
		if err := f.barrier(ctx, p, f.cfg.Kind == chaos.FaultBlack && i == 1); err != nil {
			return fmt.Errorf("barrier on %s: %w", p.conn.Name(), err)
		}
	}
	return nil
}

func (f *Fault) delete(ctx context.Context, toxics []proxy.Toxic) error {
	var errs []error
	for _, t := range toxics {
		if err := f.cfg.Ops.RemoveToxic(ctx, f.cfg.Proxy, t.Name); err != nil && !errors.Is(err, proxy.ErrNotFound) {
			errs = append(errs, fmt.Errorf("delete %s: %w", t.Name, err))
		}
	}
	return errors.Join(errs...)
}

// rounds repeats a round on one probe until it holds or ctx ends, recording each.
func (f *Fault) rounds(ctx context.Context, stage string, p *probe, round func(context.Context, *probe) (string, error)) error {
	var last string
	for n := 1; ; n++ {
		detail, err := round(ctx, p)
		held := err == nil
		f.event("effect", Round{Fault: f.cfg.Seq, Stage: stage, N: n, Held: held, Detail: detail})
		switch {
		case held:
			return nil
		case ctx.Err() != nil:
			// The install bound ended inside the round: the effect was not observed in time.
			return fmt.Errorf("effect not observed after %d rounds (last: %s; %v): %w", n, detail, err, ctx.Err())
		case !errors.Is(err, ErrNotHeld):
			return fmt.Errorf("round %d (%s): %w", n, detail, err)
		}
		last = detail
		select {
		case <-ctx.Done():
			return fmt.Errorf("effect not observed after %d rounds (last: %s): %w", n, last, ctx.Err())
		case <-time.After(f.t.Poll):
		}
	}
}

func (f *Fault) effectRound() func(context.Context, *probe) (string, error) {
	switch f.cfg.Kind {
	case chaos.FaultLat:
		return f.latencyRound
	case chaos.FaultBW:
		return f.bandwidthRound
	case chaos.FaultHalfOpen:
		return f.receiptRound
	default:
		return f.resetRound
	}
}

func (f *Fault) event(name string, data any) {
	if f.cfg.Event != nil {
		f.cfg.Event(name, data)
	}
}
