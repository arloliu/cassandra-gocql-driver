package proxy

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"time"
)

// CQL protocol 4 opcodes and consistencies the probe connection uses (PLAN §59.3).
const (
	// OpError and the other Op values are frame opcodes.
	OpError     byte = 0x00
	OpStartup   byte = 0x01
	OpReady     byte = 0x02
	OpOptions   byte = 0x05
	OpSupported byte = 0x06
	OpQuery     byte = 0x07
	OpResult    byte = 0x08
	// ConsistencyOne is CL ONE.
	ConsistencyOne uint16 = 0x0001
)

const (
	// probeVersion is protocol 4, request direction; responseVersion is its response direction.
	probeVersion    byte = 0x04
	responseVersion byte = 0x84
	// maxBody bounds a response body: Cassandra's own frame limit is far below it.
	maxBody = 256 << 20
	// headerLen is a protocol-4 frame header's length.
	headerLen = 9
	// keptBody is the largest body the reader keeps; larger bodies are only counted.
	keptBody = 64 << 10
	// writeBound bounds one request write.
	writeBound = 5 * time.Second
	// eventBuffer is the reader's event channel capacity.
	eventBuffer = 1024
)

var (
	// ErrProbeClosed is the terminal event's error after Close.
	ErrProbeClosed = errors.New("proxy: probe connection closed")
	// ErrMalformed is the terminal event's error, wrapped, for a frame the probe does not accept: the stream did not
	// end, the protocol did (PLAN §59.3).
	ErrMalformed = errors.New("proxy: malformed response frame")
)

// DialFunc dials a probe connection; the D8 registry's dialer under session "probe" in production.
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// FrameEvent is one step of the reader: a frame header, body progress, a complete frame, or the end of the stream.
type FrameEvent struct {
	// At is when the reader saw it.
	At time.Time
	// Stream and Opcode are the frame's, from its header.
	Stream int16
	Opcode byte
	// Length is the body length from the header; Received is how much of the body has arrived so far.
	Length, Received int
	// Done is true when the whole body has arrived.
	Done bool
	// Body is the complete body of a frame of at most 64 KiB, set with Done.
	Body []byte
	// Err ends the stream: EOF, a reset, a malformed header, or ErrProbeClosed. No event follows it.
	Err error
}

// Probe is a harness-owned CQL protocol 4 connection to one proxy, independent of the driver (PLAN §59.3).
//
// A reader goroutine turns the byte stream into FrameEvents; the caller sends requests and consumes events
// under its own deadlines, so a frame cut by one round's deadline is finished by the next.
type Probe struct {
	conn    net.Conn
	name    string
	events  chan FrameEvent
	closing chan struct{}
	done    chan struct{}
	once    sync.Once
	next    int16
}

// DialProbe opens a probe connection and completes STARTUP.
//
// Parameters:
//   - ctx: bounds the dial and the STARTUP exchange
//   - dial: the dialer
//   - addr: the proxy's listen address, e.g. 127.0.1.2:19042
//   - driverName: the DRIVER_NAME sent in STARTUP, which names the connection in nodetool clientstats
//
// Returns:
//   - *Probe: the open connection
//   - error: when the dial fails or STARTUP is not answered READY before ctx ends
func DialProbe(ctx context.Context, dial DialFunc, addr, driverName string) (*Probe, error) {
	conn, err := dial(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("probe %s: dial %s: %w", driverName, addr, err)
	}
	p := &Probe{conn: conn, name: driverName, events: make(chan FrameEvent, eventBuffer),
		closing: make(chan struct{}), done: make(chan struct{})}
	go p.read()
	stream, err := p.Request(ctx, OpStartup, StartupBody(driverName))
	if err != nil {
		p.Close()
		return nil, err
	}
	ev, err := p.Await(ctx, stream)
	switch {
	case err != nil:
		p.Close()
		return nil, fmt.Errorf("probe %s: STARTUP: %w", driverName, err)
	case ev.Opcode != OpReady:
		p.Close()
		return nil, fmt.Errorf("probe %s: STARTUP answered with opcode %#x, want READY", driverName, ev.Opcode)
	}
	return p, nil
}

// StartupBody encodes STARTUP's string map: CQL_VERSION 3.0.0 and DRIVER_NAME.
//
// Parameters:
//   - driverName: the DRIVER_NAME
//
// Returns:
//   - []byte: the body
func StartupBody(driverName string) []byte {
	var b []byte
	b = binary.BigEndian.AppendUint16(b, 2)
	for _, s := range []string{"CQL_VERSION", "3.0.0", "DRIVER_NAME", driverName} {
		b = appendString(b, s)
	}
	return b
}

// QueryBody encodes a QUERY body: a literal statement, a consistency, and no flags.
//
// Parameters:
//   - cql: the statement, with no bind markers
//   - consistency: e.g. ConsistencyOne
//
// Returns:
//   - []byte: the body
func QueryBody(cql string, consistency uint16) []byte {
	b := binary.BigEndian.AppendUint32(nil, uint32(len(cql)))
	b = append(b, cql...)
	b = binary.BigEndian.AppendUint16(b, consistency)
	return append(b, 0)
}

func appendString(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

// validHeader accepts a protocol-4 response header with an expected opcode and a body length in [0, maxBody].
func validHeader(head []byte, length int32) error {
	switch {
	case head[0] != responseVersion:
		return fmt.Errorf("version byte %#x, want %#x", head[0], responseVersion)
	case length < 0 || length > maxBody:
		return fmt.Errorf("body length %d", length)
	}
	switch head[4] {
	case OpError, OpReady, OpSupported, OpResult:
		return nil
	}
	return fmt.Errorf("opcode %#x", head[4])
}

// Name returns the DRIVER_NAME the connection sent.
//
// Returns:
//   - string: the name
func (p *Probe) Name() string { return p.name }

// Local returns the probe's own endpoint, which names toxiproxy's accepted socket for it.
//
// Returns:
//   - netip.AddrPort: the local address
func (p *Probe) Local() netip.AddrPort {
	if a, ok := p.conn.LocalAddr().(*net.TCPAddr); ok {
		return a.AddrPort()
	}
	return netip.AddrPort{}
}

// Events returns the reader's events, in stream order.
//
// Returns:
//   - <-chan FrameEvent: the events; the last one carries Err
func (p *Probe) Events() <-chan FrameEvent { return p.events }

// Request writes one request frame in full with the next stream id, within ctx's deadline and at most writeBound.
//
// Parameters:
//   - ctx: bounds the write; an ended ctx sends nothing
//   - op: the opcode
//   - body: the body
//
// Returns:
//   - int16: the stream id
//   - error: when ctx has ended, or the write fails or is short; a request counts as sent only when it returns nil
//     (PLAN §59.3)
func (p *Probe) Request(ctx context.Context, op byte, body []byte) (int16, error) {
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("probe %s: %w", p.name, err)
	}
	p.next++
	if p.next < 1 {
		p.next = 1
	}
	stream := p.next
	frame := make([]byte, headerLen, headerLen+len(body))
	frame[0] = probeVersion
	binary.BigEndian.PutUint16(frame[2:], uint16(stream))
	frame[4] = op
	binary.BigEndian.PutUint32(frame[5:], uint32(len(body)))
	frame = append(frame, body...)
	deadline := time.Now().Add(writeBound)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := p.conn.SetWriteDeadline(deadline); err != nil {
		return 0, fmt.Errorf("probe %s: write deadline: %w", p.name, err)
	}
	// Cancelling ctx mid-write ends the write at once; the callback is waited for, so it cannot touch a later
	// request's deadline (§65 BX01).
	fired := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = p.conn.SetWriteDeadline(time.Now())
		close(fired)
	})
	n, err := p.conn.Write(frame)
	if !stop() {
		<-fired
	}
	if err != nil {
		return 0, fmt.Errorf("probe %s: write stream %d: %w", p.name, stream, err)
	}
	if n != len(frame) {
		return 0, fmt.Errorf("probe %s: short write of stream %d: %d of %d bytes", p.name, stream, n, len(frame))
	}
	return stream, nil
}

// Await consumes events until the frame of one stream is complete, for exchanges with no other frame in flight.
//
// Parameters:
//   - ctx: bounds the wait
//   - stream: the stream id
//
// Returns:
//   - FrameEvent: the complete frame
//   - error: ctx's error, the stream's end, or a frame of another stream
func (p *Probe) Await(ctx context.Context, stream int16) (FrameEvent, error) {
	for {
		select {
		case <-ctx.Done():
			return FrameEvent{}, ctx.Err()
		case ev := <-p.events:
			switch {
			case ev.Err != nil:
				return ev, ev.Err
			case ev.Stream != stream:
				return ev, fmt.Errorf("probe %s: frame for stream %d while awaiting %d", p.name, ev.Stream, stream)
			case ev.Done:
				return ev, nil
			}
		}
	}
}

// Done is closed when the reader has ended.
//
// Returns:
//   - <-chan struct{}: closed after the terminal event
func (p *Probe) Done() <-chan struct{} { return p.done }

// Close closes the connection and waits for the reader; it is idempotent.
func (p *Probe) Close() {
	p.once.Do(func() {
		close(p.closing)
		_ = p.conn.Close()
	})
	<-p.done
}

// read turns the byte stream into events until it ends: a header event (Received 0), which marks the frame's
// first byte, then one event per body read, the last with Done.
func (p *Probe) read() {
	defer close(p.done)
	head := make([]byte, headerLen)
	buf := make([]byte, 64<<10)
	for {
		if _, err := io.ReadFull(p.conn, head); err != nil {
			p.end(err)
			return
		}
		length := int32(binary.BigEndian.Uint32(head[5:]))
		ev := FrameEvent{At: time.Now(), Stream: int16(binary.BigEndian.Uint16(head[2:])), Opcode: head[4], Length: int(length)}
		if err := validHeader(head, length); err != nil {
			p.end(fmt.Errorf("probe %s: %w: %w", p.name, ErrMalformed, err))
			return
		}
		var body []byte
		if ev.Length <= keptBody {
			body = make([]byte, 0, ev.Length)
		}
		ev.Done = ev.Length == 0
		if ev.Done {
			ev.Body = body
		}
		if !p.emit(ev) {
			return
		}
		for ev.Received < ev.Length {
			n, err := p.conn.Read(buf[:min(len(buf), ev.Length-ev.Received)])
			if err != nil {
				p.end(err)
				return
			}
			ev.At, ev.Received = time.Now(), ev.Received+n
			if body != nil {
				body = append(body, buf[:n]...)
			}
			ev.Done = ev.Received == ev.Length
			if ev.Done {
				ev.Body = body
			}
			if !p.emit(ev) {
				return
			}
		}
	}
}

// emit delivers an event unless the probe is closing.
func (p *Probe) emit(ev FrameEvent) bool {
	select {
	case p.events <- ev:
		return true
	case <-p.closing:
		return false
	}
}

// end delivers the terminal event.
func (p *Probe) end(err error) {
	select {
	case <-p.closing:
		err = ErrProbeClosed
	default:
	}
	p.emit(FrameEvent{At: time.Now(), Err: err})
}
