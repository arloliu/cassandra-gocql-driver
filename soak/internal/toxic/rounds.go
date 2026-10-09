package toxic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"slices"
	"syscall"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/proxy"
)

var (
	// releaseLen is the length of the release_version request frame.
	releaseLen = frameHeader + len(proxy.QueryBody(stmtRelease, proxy.ConsistencyOne))
	// errDeadline is next's report of the round's own deadline.
	errDeadline = errors.New("toxic: round deadline")
)

// probe is one probe connection and its exchange state (PLAN §59.3's exchange contract).
type probe struct {
	conn Conn
	// up is toxiproxy's end of the upstream link, from clientstats; set by the control round of F-black and F-halfopen.
	up netip.AddrPort
	// held is the outstanding request's stream, 0 when none; sentAt is when it was written.
	held   int16
	sentAt time.Time
	// before is the snapshot taken just before the held request.
	before Snapshot
	// respLen is the control round's response frame length.
	respLen int
	// xfer is F-bw's transfer in progress.
	xfer *transfer
}

// transfer is F-bw's response as it arrives.
type transfer struct {
	stream      int16
	first, last time.Time
	started     bool
	received    int
	done        bool
	judged      bool
}

// next reads one event for the stream; a frame of another stream or the stream's end is an error.
func next(ctx context.Context, p *probe, stream int16) (proxy.FrameEvent, error) {
	select {
	case <-ctx.Done():
		return proxy.FrameEvent{}, errDeadline
	case ev := <-p.conn.Events():
		switch {
		case ev.Err != nil:
			return ev, fmt.Errorf("connection ended: %w", ev.Err)
		case ev.Stream != stream:
			return ev, fmt.Errorf("frame for stream %d while awaiting %d", ev.Stream, stream)
		}
		return ev, nil
	}
}

// terminated reports whether a probe's end is the peer ending the connection: EOF or a reset (PLAN §59.3).
func terminated(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET)
}

// String describes the transfer so far: bytes after the first byte, and the first and last byte's times.
func (x *transfer) String() string {
	return fmt.Sprintf("stream %d: %d B after the first byte, first %s, last %s", x.stream, x.received,
		x.first.Format(time.RFC3339Nano), x.last.Format(time.RFC3339Nano))
}

// control runs the control round on every probe connection, with no toxic present.
func (f *Fault) control(ctx context.Context) error {
	var detail string
	var err error
	switch f.cfg.Kind {
	case chaos.FaultLat:
		detail, err = f.controlLatency(ctx, f.probes[0])
	case chaos.FaultBW:
		detail, err = f.controlBandwidth(ctx, f.probes[0])
	case chaos.FaultReset:
		_, err = f.exchange(ctx, f.probes[0], proxy.OpOptions, nil, f.t.Round)
		detail = "OPTIONS answered"
	default:
		for _, p := range f.probes {
			d, e := f.controlReceipt(ctx, p)
			detail += d + "; "
			if e != nil {
				err = e
				break
			}
		}
	}
	f.event("effect", Round{Fault: f.cfg.Seq, N: 0, Held: err == nil, Detail: detail})
	return err
}

func (f *Fault) controlLatency(ctx context.Context, p *probe) (string, error) {
	trips := make([]time.Duration, 0, baselineTrips)
	for range baselineTrips {
		start := time.Now()
		ev, err := f.exchange(ctx, p, proxy.OpOptions, nil, f.t.Round)
		if err != nil {
			return "", err
		}
		if ev.Opcode != proxy.OpSupported {
			return "", fmt.Errorf("OPTIONS answered with opcode %#x", ev.Opcode)
		}
		trips = append(trips, time.Since(start))
	}
	slices.Sort(trips)
	f.baseline = trips[len(trips)/2]
	return fmt.Sprintf("baseline %s of %v", f.baseline, trips), nil
}

func (f *Fault) controlBandwidth(ctx context.Context, p *probe) (string, error) {
	rctx, cancel := context.WithTimeout(ctx, f.t.Round)
	defer cancel()
	stream, err := p.conn.Request(rctx, proxy.OpQuery, proxy.QueryBody(stmtBlob, proxy.ConsistencyOne))
	if err != nil {
		return "", err
	}
	x := &transfer{stream: stream}
	var op byte
	for !x.done {
		ev, err := next(rctx, p, stream)
		if err != nil {
			return "", fmt.Errorf("blob read: %w", err)
		}
		x.observe(ev)
		op = ev.Opcode
	}
	if op != proxy.OpResult || x.received < BlobSize {
		return x.String(), fmt.Errorf("blob read answered with opcode %#x and %d B, want a RESULT of at least %d B", op, x.received, BlobSize)
	}
	rate := SustainedRate(x.received, x.first, x.last)
	detail := fmt.Sprintf("%s, sustained %.0f B/s, limit %.0f B/s", x, rate, RateLimit(f.cfg.Param))
	if rate < RateLimit(f.cfg.Param) {
		return detail, fmt.Errorf("unthrottled transfer below the limit: %s", detail)
	}
	return detail, nil
}

// controlReceipt finds the probe's upstream socket and checks that a request and its response show in its counters.
func (f *Fault) controlReceipt(ctx context.Context, p *probe) (string, error) {
	row, err := f.cfg.Sockets.Upstream(ctx, p.conn.Name())
	if err != nil {
		return "", err
	}
	p.up = row.Address
	before, err := f.snapshot(ctx, p)
	if err != nil {
		return "", err
	}
	ev, err := f.exchange(ctx, p, proxy.OpQuery, proxy.QueryBody(stmtRelease, proxy.ConsistencyOne), f.t.Silence)
	if err != nil {
		return "", err
	}
	after, err := f.snapshot(ctx, p)
	if err != nil {
		return "", err
	}
	if ev.Opcode != proxy.OpResult {
		return "", fmt.Errorf("release_version answered with opcode %#x", ev.Opcode)
	}
	p.respLen = frameHeader + ev.Length
	detail := fmt.Sprintf("%s; requests %d; response %d B; before %+v; after %+v", f.sockets(p), row.Requests,
		p.respLen, before, after)
	if !ReceiptHolds(before, after, releaseLen, p.respLen) {
		return detail, fmt.Errorf("the counters did not show the request and its response: %s", detail)
	}
	return detail, nil
}

func (f *Fault) latencyRound(ctx context.Context, p *probe) (string, error) {
	rctx, cancel := context.WithTimeout(ctx, f.t.Round)
	defer cancel()
	if p.held == 0 {
		s, err := p.conn.Request(rctx, proxy.OpOptions, nil)
		if err != nil {
			return "", err
		}
		p.held, p.sentAt = s, time.Now()
	}
	for {
		ev, err := next(rctx, p, p.held)
		if errors.Is(err, errDeadline) {
			return "OPTIONS outstanding", ErrNotHeld
		}
		if err != nil {
			return "", err
		}
		if !ev.Done {
			continue
		}
		if ev.Opcode != proxy.OpSupported {
			return "", fmt.Errorf("OPTIONS answered with opcode %#x", ev.Opcode)
		}
		rtt := ev.At.Sub(p.sentAt)
		p.held = 0
		detail := fmt.Sprintf("round trip %s, baseline %s", rtt, f.baseline)
		if rtt >= f.baseline+LatencyRise {
			return detail, nil
		}
		return detail, ErrNotHeld
	}
}

func (f *Fault) bandwidthRound(ctx context.Context, p *probe) (string, error) {
	rctx, cancel := context.WithTimeout(ctx, f.t.Round)
	defer cancel()
	if p.xfer == nil {
		s, err := p.conn.Request(rctx, proxy.OpQuery, proxy.QueryBody(stmtBlob, proxy.ConsistencyOne))
		if err != nil {
			return "", err
		}
		p.xfer = &transfer{stream: s}
	}
	x, limit := p.xfer, RateLimit(f.cfg.Param)
	for {
		ev, err := next(rctx, p, x.stream)
		if errors.Is(err, errDeadline) {
			return x.String() + " so far", ErrNotHeld
		}
		if err != nil {
			return "", err
		}
		x.observe(ev)
		if x.done && ev.Opcode != proxy.OpResult {
			p.xfer = nil
			return "", fmt.Errorf("blob read answered with opcode %#x", ev.Opcode)
		}
		detail := fmt.Sprintf("%s: %.0f B/s, limit %.0f B/s", x, SustainedRate(x.received, x.first, x.last), limit)
		if x.done {
			p.xfer = nil
		}
		if x.judged {
			if x.done {
				return detail, ErrNotHeld
			}
			continue
		}
		decided, holds := BandwidthHolds(x.received, x.first, x.last, x.done, limit)
		switch {
		case !decided:
		case holds:
			return detail, nil
		default:
			// Judged too fast: the rest of this response is drained before the next request.
			x.judged = true
			if x.done {
				return detail, ErrNotHeld
			}
		}
	}
}

// receiptRound is F-halfopen's round and F-black's downstream stage.
func (f *Fault) receiptRound(ctx context.Context, p *probe) (string, error) {
	return f.silentRound(ctx, p, func(before, after Snapshot) (bool, error) {
		return ReceiptHolds(before, after, releaseLen, p.respLen), nil
	})
}

// dropRound is F-black's upstream stage.
func (f *Fault) dropRound(ctx context.Context, p *probe) (string, error) {
	return f.silentRound(ctx, p, func(before, after Snapshot) (bool, error) {
		if after.CassReceived != before.CassReceived {
			return false, fmt.Errorf("the request reached Cassandra under the upstream toxic: received +%d",
				after.CassReceived-before.CassReceived)
		}
		return DropHolds(before, after, releaseLen), nil
	})
}

// silentRound sends one request unless one is held, waits for the silence, and judges the snapshots.
func (f *Fault) silentRound(ctx context.Context, p *probe, judge func(before, after Snapshot) (bool, error)) (string, error) {
	if p.held == 0 {
		before, err := f.snapshot(ctx, p)
		if err != nil {
			return "", err
		}
		s, err := p.conn.Request(ctx, proxy.OpQuery, proxy.QueryBody(stmtRelease, proxy.ConsistencyOne))
		if err != nil {
			return "", err
		}
		p.held, p.sentAt, p.before = s, time.Now(), before
	}
	sctx, cancel := context.WithTimeout(ctx, f.t.Silence)
	defer cancel()
	for {
		ev, err := next(sctx, p, p.held)
		if errors.Is(err, errDeadline) {
			break
		}
		if err != nil {
			return "", err
		}
		if ev.Done {
			p.held = 0
			return "a response arrived", ErrNotHeld
		}
	}
	if ctx.Err() != nil {
		return "the install bound ended during the silence", ErrNotHeld
	}
	after, err := f.snapshot(ctx, p)
	if err != nil {
		return "", err
	}
	b := p.before
	detail := fmt.Sprintf("silent %s on stream %d; %s; before %+v; after %+v", time.Since(p.sentAt).Round(time.Millisecond),
		p.held, f.sockets(p), b, after)
	held, err := judge(b, after)
	switch {
	case err != nil:
		return detail, err
	case held:
		return detail, nil
	}
	return detail, ErrNotHeld
}

func (f *Fault) resetRound(ctx context.Context, p *probe) (string, error) {
	rctx, cancel := context.WithTimeout(ctx, f.t.Round)
	defer cancel()
	if p.held == 0 {
		s, err := p.conn.Request(rctx, proxy.OpOptions, nil)
		if err != nil {
			return "", fmt.Errorf("write: %w", err)
		}
		p.held = s
	}
	for {
		select {
		case <-rctx.Done():
			return "connection open", ErrNotHeld
		case ev := <-p.conn.Events():
			switch {
			case terminated(ev.Err):
				return fmt.Sprintf("connection ended: %v", ev.Err), nil
			case ev.Err != nil:
				// A malformed frame or another error is not the proxy ending the connection (PLAN §59.3).
				return "", fmt.Errorf("the probe ended without a termination: %w", ev.Err)
			case ev.Stream != p.held:
				return "", fmt.Errorf("frame for stream %d while awaiting %d", ev.Stream, p.held)
			case ev.Done:
				p.held = 0
				return "OPTIONS answered", ErrNotHeld
			}
		}
	}
}

// barrier sends a new request on a retained probe connection under toxicity 0: the next frame must be its
// answer, and on F-black's connection b Cassandra must have received exactly that request since the upstream
// stage began (PLAN §59.3).
func (f *Fault) barrier(ctx context.Context, p *probe, upstream bool) (err error) {
	held := p.held
	detail := fmt.Sprintf("%s; held stream %d", f.sockets(p), held)
	defer func() {
		r := Round{Fault: f.cfg.Seq, Stage: "barrier", Held: err == nil, Detail: detail}
		if err != nil {
			r.Detail += "; " + err.Error()
		}
		f.event("effect", r)
	}()
	bctx, cancel := context.WithTimeout(ctx, f.t.Silence)
	defer cancel()
	s, err := p.conn.Request(bctx, proxy.OpQuery, proxy.QueryBody(stmtRelease, proxy.ConsistencyOne))
	if err != nil {
		return err
	}
	for done := false; !done; {
		select {
		case <-bctx.Done():
			return fmt.Errorf("barrier stream %d unanswered: %w", s, bctx.Err())
		case ev := <-p.conn.Events():
			switch {
			case ev.Err != nil:
				return fmt.Errorf("connection ended: %w", ev.Err)
			case held != 0 && ev.Stream == held:
				return fmt.Errorf("held stream %d answered after the update: the toxic did not drop it", held)
			case ev.Stream != s:
				return fmt.Errorf("frame for stream %d while awaiting the barrier %d", ev.Stream, s)
			case ev.Done && ev.Opcode != proxy.OpResult:
				return fmt.Errorf("barrier stream %d answered with opcode %#x", s, ev.Opcode)
			}
			done = ev.Done
		}
	}
	if !upstream {
		return nil
	}
	after, err := f.snapshot(ctx, p)
	if err != nil {
		return err
	}
	detail += fmt.Sprintf("; before %+v; after %+v", p.before, after)
	if got := after.CassReceived - p.before.CassReceived; got != int64(releaseLen) {
		return fmt.Errorf("cassandra received %d B since the upstream stage, want the barrier's %d", got, releaseLen)
	}
	return nil
}

// exchange sends one request with no other in flight and waits for its whole response.
func (f *Fault) exchange(ctx context.Context, p *probe, op byte, body []byte, bound time.Duration) (proxy.FrameEvent, error) {
	ectx, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	s, err := p.conn.Request(ectx, op, body)
	if err != nil {
		return proxy.FrameEvent{}, err
	}
	for {
		ev, err := next(ectx, p, s)
		if err != nil {
			return ev, err
		}
		if !ev.Done {
			continue
		}
		if ev.Opcode == proxy.OpError {
			return ev, fmt.Errorf("stream %d answered with an ERROR frame", s)
		}
		return ev, nil
	}
}

func (f *Fault) snapshot(ctx context.Context, p *probe) (Snapshot, error) {
	cass, err := f.cfg.Sockets.Socket(ctx, netip.AddrPortFrom(f.cfg.Node, proxy.NodePort), p.up)
	if err != nil {
		return Snapshot{}, fmt.Errorf("cassandra's socket for %s: %w", p.conn.Name(), err)
	}
	prox, err := f.cfg.Sockets.Socket(ctx, netip.AddrPortFrom(f.cfg.Node, proxy.ProxyPort), p.conn.Local())
	if err != nil {
		return Snapshot{}, fmt.Errorf("toxiproxy's socket for %s: %w", p.conn.Name(), err)
	}
	return Snapshot{CassReceived: cass.BytesReceived, CassAcked: cass.BytesAcked,
		ProxyReceived: prox.BytesReceived, ProxyRecvQ: prox.RecvQ}, nil
}

// sockets names a probe connection's two sockets, for the records (PLAN §59.5).
func (f *Fault) sockets(p *probe) string {
	return fmt.Sprintf("%s: cassandra %s<-%s, toxiproxy %s<-%s", p.conn.Name(),
		netip.AddrPortFrom(f.cfg.Node, proxy.NodePort), p.up, netip.AddrPortFrom(f.cfg.Node, proxy.ProxyPort), p.conn.Local())
}

// observe folds one event of the transfer's frame into it.
func (x *transfer) observe(ev proxy.FrameEvent) {
	if !x.started {
		x.started, x.first = true, ev.At
	}
	x.last, x.received, x.done = ev.At, ev.Received, ev.Done
}
