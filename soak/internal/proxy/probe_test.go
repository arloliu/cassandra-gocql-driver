package proxy

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeFrame is one request the fake server read.
type fakeFrame struct {
	stream int16
	op     byte
	body   []byte
}

// shortConn reports a short write without an error.
type shortConn struct{ net.Conn }

func TestProbeStartupAndOptions(t *testing.T) {
	startups := make(chan []byte, 1)
	addr := fakeServer(t, func(c net.Conn, f fakeFrame) {
		if f.op == OpStartup {
			startups <- f.body
		}
		healthy(0, 1, 0)(c, f)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	p, err := DialProbe(ctx, dial, addr, "soak-probe-t1-a")
	require.NoError(t, err)
	defer p.Close()
	startup := <-startups
	require.Equal(t, StartupBody("soak-probe-t1-a"), startup)
	require.Contains(t, string(startup), "CQL_VERSION")
	require.Contains(t, string(startup), "3.0.0")
	require.Contains(t, string(startup), "DRIVER_NAME")
	require.True(t, p.Local().IsValid())

	s, err := p.Request(ctx, OpOptions, nil)
	require.NoError(t, err)
	ev, err := p.Await(ctx, s)
	require.NoError(t, err)
	require.Equal(t, OpSupported, ev.Opcode)
	require.Equal(t, []byte{0, 0}, ev.Body)
}

func TestProbeStartupRefused(t *testing.T) {
	addr := fakeServer(t, func(c net.Conn, f fakeFrame) {
		_, _ = c.Write(response(f.stream, OpError, []byte{0, 0, 0, 0xa, 0, 1, 'x'}))
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := DialProbe(ctx, dial, addr, "soak-probe-t1-a")
	require.ErrorContains(t, err, "want READY")
}

func TestProbeStartupUnanswered(t *testing.T) {
	addr := fakeServer(t, func(net.Conn, fakeFrame) {})
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	_, err := DialProbe(ctx, dial, addr, "soak-probe-t1-a")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestQueryBody(t *testing.T) {
	b := QueryBody("SELECT 1", ConsistencyOne)
	require.Equal(t, uint32(8), binary.BigEndian.Uint32(b))
	require.Equal(t, "SELECT 1", string(b[4:12]))
	require.Equal(t, ConsistencyOne, binary.BigEndian.Uint16(b[12:]))
	require.Equal(t, byte(0), b[14], "no flags")
	require.Len(t, b, 15)
}

func TestProbeLargeBodyInChunks(t *testing.T) {
	const size = 100 << 10
	addr := fakeServer(t, healthy(size, 10, 5*time.Millisecond))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	p, err := DialProbe(ctx, dial, addr, "soak-probe-t1-a")
	require.NoError(t, err)
	defer p.Close()

	s, err := p.Request(ctx, OpQuery, QueryBody("SELECT payload FROM soak.blob WHERE k = 25", ConsistencyOne))
	require.NoError(t, err)
	first, err := nextEvent(ctx, p)
	require.NoError(t, err)
	require.Equal(t, s, first.Stream)
	require.Equal(t, 0, first.Received, "the header event marks the first byte")
	require.Equal(t, size, first.Length)
	progress, last := 0, first
	for !last.Done {
		last, err = nextEvent(ctx, p)
		require.NoError(t, err)
		require.GreaterOrEqual(t, last.Received, progress)
		progress = last.Received
	}
	require.Equal(t, size, last.Received)
	require.Nil(t, last.Body, "a body over 64 KiB is counted, not kept")
	require.False(t, last.At.Before(first.At))
}

func TestProbeFrameSpansRounds(t *testing.T) {
	// A round's deadline can end mid-frame; the next round's reads continue the same frame (PLAN §59.3, BR03).
	addr := fakeServer(t, healthy(30_000, 6, 40*time.Millisecond))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	p, err := DialProbe(ctx, dial, addr, "soak-probe-t1-a")
	require.NoError(t, err)
	defer p.Close()
	s, err := p.Request(ctx, OpQuery, QueryBody("SELECT 1", ConsistencyOne))
	require.NoError(t, err)

	round1, c1 := context.WithTimeout(ctx, 60*time.Millisecond)
	_, err = p.Await(round1, s)
	c1()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	ev, err := p.Await(ctx, s)
	require.NoError(t, err)
	require.Equal(t, 30_000, ev.Received)
	require.Len(t, ev.Body, 30_000)
}

func TestProbeAwaitOtherStream(t *testing.T) {
	addr := fakeServer(t, func(c net.Conn, f fakeFrame) {
		if f.op == OpStartup {
			_, _ = c.Write(response(f.stream, OpReady, nil))
			return
		}
		_, _ = c.Write(response(f.stream+5, OpResult, []byte{0, 0, 0, 1}))
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	p, err := DialProbe(ctx, dial, addr, "soak-probe-t1-a")
	require.NoError(t, err)
	defer p.Close()
	s, err := p.Request(ctx, OpOptions, nil)
	require.NoError(t, err)
	_, err = p.Await(ctx, s)
	require.ErrorContains(t, err, "while awaiting")
}

func TestProbeServerClose(t *testing.T) {
	addr := fakeServer(t, func(c net.Conn, f fakeFrame) {
		if f.op == OpStartup {
			_, _ = c.Write(response(f.stream, OpReady, nil))
			return
		}
		c.Close()
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	p, err := DialProbe(ctx, dial, addr, "soak-probe-t1-a")
	require.NoError(t, err)
	defer p.Close()
	s, err := p.Request(ctx, OpOptions, nil)
	require.NoError(t, err)
	_, err = p.Await(ctx, s)
	require.True(t, errors.Is(err, io.EOF) || strings.Contains(err.Error(), "reset"), "got %v", err)
}

func TestProbeMalformedHeader(t *testing.T) {
	for name, mutate := range map[string]func([]byte){
		"protocol 5":           func(b []byte) { b[0] = 0x85 },
		"request direction":    func(b []byte) { b[0] = 0x04 },
		"negative length":      func(b []byte) { binary.BigEndian.PutUint32(b[5:], 0xffffffff) },
		"an unexpected opcode": func(b []byte) { b[4] = 0x0c },
	} {
		t.Run(name, func(t *testing.T) {
			addr := fakeServer(t, func(c net.Conn, f fakeFrame) {
				if f.op == OpStartup {
					_, _ = c.Write(response(f.stream, OpReady, nil))
					return
				}
				bad := response(f.stream, OpResult, nil)
				mutate(bad)
				_, _ = c.Write(bad)
				time.Sleep(time.Second) // the peer stays connected
			})
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			p, err := DialProbe(ctx, dial, addr, "soak-probe-t1-a")
			require.NoError(t, err)
			defer p.Close()
			s, err := p.Request(ctx, OpOptions, nil)
			require.NoError(t, err)
			_, err = p.Await(ctx, s)
			require.ErrorIs(t, err, ErrMalformed)
		})
	}
}

func TestProbeHeaderSplit(t *testing.T) {
	// A header that arrives in two pieces yields one header event once it is whole.
	addr := fakeServer(t, func(c net.Conn, f fakeFrame) {
		frame := response(f.stream, OpReady, nil)
		if f.op == OpOptions {
			frame = response(f.stream, OpSupported, []byte{0, 0})
		}
		_, _ = c.Write(frame[:4])
		time.Sleep(50 * time.Millisecond)
		_, _ = c.Write(frame[4:])
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	p, err := DialProbe(ctx, dial, addr, "soak-probe-t1-a")
	require.NoError(t, err)
	defer p.Close()
	s, err := p.Request(ctx, OpOptions, nil)
	require.NoError(t, err)
	ev, err := p.Await(ctx, s)
	require.NoError(t, err)
	require.Equal(t, OpSupported, ev.Opcode)
}

func TestProbeWriteBounds(t *testing.T) {
	addr := fakeServer(t, healthy(0, 1, 0))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	p, err := DialProbe(ctx, dial, addr, "soak-probe-t1-a")
	require.NoError(t, err)
	defer p.Close()

	done, stop := context.WithCancel(ctx)
	stop()
	_, err = p.Request(done, OpOptions, nil)
	require.ErrorIs(t, err, context.Canceled, "an ended context sends nothing")

	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	short := &Probe{conn: shortConn{a}, name: "soak-probe-t1-a"}
	_, err = short.Request(ctx, OpOptions, nil)
	require.ErrorContains(t, err, "short write")
}

func TestProbeBlockedWriteHonoursTheDeadline(t *testing.T) {
	// A peer that never reads: the write blocks once the socket buffers fill, and the round's deadline ends it.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		head := make([]byte, headerLen)
		if _, err := io.ReadFull(c, head); err != nil {
			return
		}
		body := make([]byte, binary.BigEndian.Uint32(head[5:]))
		_, _ = io.ReadFull(c, body)
		_, _ = c.Write(response(int16(binary.BigEndian.Uint16(head[2:])), OpReady, nil))
		time.Sleep(5 * time.Second)
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	p, err := DialProbe(ctx, dial, ln.Addr().String(), "soak-probe-t1-a")
	require.NoError(t, err)
	defer p.Close()
	short, c2 := context.WithTimeout(ctx, 200*time.Millisecond)
	defer c2()
	start := time.Now()
	_, err = p.Request(short, OpQuery, QueryBody(string(make([]byte, 64<<20)), ConsistencyOne))
	require.Error(t, err)
	require.Less(t, time.Since(start), 2*time.Second, "the write ends at the round's deadline, not writeBound")
}

func TestDialProbeFailureClosesTheConnection(t *testing.T) {
	closed := make(chan struct{})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, c) // returns when the probe closes its end
		c.Close()
		close(closed)
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	_, err = DialProbe(ctx, dial, ln.Addr().String(), "soak-probe-t1-a")
	require.Error(t, err)
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("a failed DialProbe left its connection open")
	}
}

func TestProbeCloseIdempotent(t *testing.T) {
	addr := fakeServer(t, healthy(0, 1, 0))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	p, err := DialProbe(ctx, dial, addr, "soak-probe-t1-a")
	require.NoError(t, err)
	done := make(chan struct{})
	go func() {
		p.Close()
		p.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return")
	}
	_, err = p.Request(ctx, OpOptions, nil)
	require.Error(t, err, "a closed probe cannot send")
	select {
	case <-p.Done():
	default:
		t.Fatal("Close returned before the reader ended")
	}
}

func TestProbeCancelDuringABlockedWrite(t *testing.T) {
	// A peer that never reads; the write blocks, and cancelling its context ends it at once (§65 BX01).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	release := make(chan struct{})
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		head := make([]byte, headerLen)
		if _, err := io.ReadFull(c, head); err != nil {
			return
		}
		body := make([]byte, binary.BigEndian.Uint32(head[5:]))
		_, _ = io.ReadFull(c, body)
		_, _ = c.Write(response(int16(binary.BigEndian.Uint16(head[2:])), OpReady, nil))
		<-release
	}()
	defer close(release)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	p, err := DialProbe(ctx, dial, ln.Addr().String(), "soak-probe-t1-a")
	require.NoError(t, err)
	defer p.Close()
	wctx, stop := context.WithCancel(ctx) // no deadline: only the cancel can end the write before writeBound
	time.AfterFunc(200*time.Millisecond, stop)
	start := time.Now()
	_, err = p.Request(wctx, OpQuery, QueryBody(string(make([]byte, 64<<20)), ConsistencyOne))
	require.Error(t, err)
	require.Less(t, time.Since(start), 2*time.Second, "the cancel ends the write, not writeBound")
}

func TestProbeCancelAfterAWriteLeavesTheNextAlone(t *testing.T) {
	// A context cancelled after its write completed must not reach a later request's deadline (§65 BX01).
	addr := fakeServer(t, healthy(0, 1, 0))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	p, err := DialProbe(ctx, dial, addr, "soak-probe-t1-a")
	require.NoError(t, err)
	defer p.Close()
	first, stop := context.WithCancel(ctx)
	s, err := p.Request(first, OpOptions, nil)
	require.NoError(t, err)
	stop()
	_, err = p.Await(ctx, s)
	require.NoError(t, err)
	s, err = p.Request(ctx, OpOptions, nil)
	require.NoError(t, err)
	_, err = p.Await(ctx, s)
	require.NoError(t, err)
}

func TestProbeHeaderSplitAcrossRounds(t *testing.T) {
	// A round's deadline expires mid-header; the next round reads the same frame whole (§65 BX05).
	addr := fakeServer(t, func(c net.Conn, f fakeFrame) {
		frame := response(f.stream, OpReady, nil)
		if f.op == OpOptions {
			frame = response(f.stream, OpSupported, []byte{0, 0})
		}
		_, _ = c.Write(frame[:4])
		time.Sleep(300 * time.Millisecond)
		_, _ = c.Write(frame[4:])
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	p, err := DialProbe(ctx, dial, addr, "soak-probe-t1-a")
	require.NoError(t, err)
	defer p.Close()
	s, err := p.Request(ctx, OpOptions, nil)
	require.NoError(t, err)
	round1, c1 := context.WithTimeout(ctx, 100*time.Millisecond)
	_, err = p.Await(round1, s)
	c1()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	ev, err := p.Await(ctx, s)
	require.NoError(t, err)
	require.Equal(t, OpSupported, ev.Opcode)
}

// fakeServer accepts one connection and lets handle answer each request frame on it.
func fakeServer(t *testing.T, handle func(c net.Conn, f fakeFrame)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		head := make([]byte, headerLen)
		for {
			if _, err := io.ReadFull(c, head); err != nil {
				return
			}
			body := make([]byte, binary.BigEndian.Uint32(head[5:]))
			if _, err := io.ReadFull(c, body); err != nil {
				return
			}
			handle(c, fakeFrame{stream: int16(binary.BigEndian.Uint16(head[2:])), op: head[4], body: body})
		}
	}()
	return ln.Addr().String()
}

// response encodes a protocol-4 response frame.
func response(stream int16, op byte, body []byte) []byte {
	f := make([]byte, headerLen, headerLen+len(body))
	f[0] = 0x84
	binary.BigEndian.PutUint16(f[2:], uint16(stream))
	f[4] = op
	binary.BigEndian.PutUint32(f[5:], uint32(len(body)))
	return append(f, body...)
}

// healthy answers STARTUP with READY, OPTIONS with SUPPORTED, and a query with a RESULT of size bytes in chunks.
func healthy(size, chunks int, gap time.Duration) func(net.Conn, fakeFrame) {
	return func(c net.Conn, f fakeFrame) {
		switch f.op {
		case OpStartup:
			_, _ = c.Write(response(f.stream, OpReady, nil))
		case OpOptions:
			_, _ = c.Write(response(f.stream, OpSupported, []byte{0, 0}))
		case OpQuery:
			frame := response(f.stream, OpResult, make([]byte, size))
			step := (len(frame) + chunks - 1) / chunks
			for i := 0; i < len(frame); i += step {
				_, _ = c.Write(frame[i:min(i+step, len(frame))])
				time.Sleep(gap)
			}
		}
	}
}

func dial(ctx context.Context, network, addr string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, addr)
}

// nextEvent reads one event under ctx.
func nextEvent(ctx context.Context, p *Probe) (FrameEvent, error) {
	select {
	case <-ctx.Done():
		return FrameEvent{}, ctx.Err()
	case ev := <-p.Events():
		return ev, ev.Err
	}
}

// Write reports one byte fewer than it was given.
func (c shortConn) Write(b []byte) (int, error) { return len(b) - 1, nil }
