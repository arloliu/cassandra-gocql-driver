//go:build unit
// +build unit

package gocql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/internal/streams"
)

// abandonGate parks one QUERY at a time in a test server's receive loop, so a
// test can hold a request in flight, let the caller give up, and only then
// release the response.
type abandonGate struct {
	started chan struct{}
	release chan struct{}
}

func newAbandonGate() *abandonGate {
	return &abandonGate{started: make(chan struct{}, 1), release: make(chan struct{})}
}

func (g *abandonGate) hook(_ string, f *framer) {
	if f.header == nil || f.header.op != opQuery {
		return
	}
	select {
	case g.started <- struct{}{}:
	default:
	}
	<-g.release
}

// awaitParked waits for one request to reach the gate.
func (g *abandonGate) awaitParked(t *testing.T, round int) {
	t.Helper()

	select {
	case <-g.started:
	case <-time.After(fillEventBudget):
		t.Fatalf("round %d: the query never reached the server", round)
	}
}

// singleConnHarness starts a one-host harness pinned to a single connection, so
// every query in a test lands on the same stream allocator.
func singleConnHarness(t *testing.T, gate *abandonGate) (*fillHarness, *Conn) {
	t.Helper()

	h := newFillHarnessOpts(t, 1, fillHarnessOpts{
		recvHook: gate.hook,
		tune:     func(cfg *ClusterConfig) { cfg.NumConns = 1 },
	})

	pool := h.pool(t, h.hosts[0])
	return h, h.pickAnyConn(t, pool)
}

// TestConn_CancelledRequestReleasesItsStream is the end-to-end regression for a
// caller that gives up on a request whose response then arrives.
//
// This is a statistical gate, not a deterministic one, and deliberately so: the
// receive side chooses between delivering the response and observing the closed
// abandonment channel, and that choice cannot be forced from outside. Each round
// therefore exercises either reclaimer with roughly even odds, and 40 rounds put
// the chance of a broken driver passing at about 2^-40. The deterministic
// coverage of each side individually lives in the two tests below.
//
// Before the fix this leaked 21 of 40 streams on a connection that stayed
// healthy throughout.
func TestConn_CancelledRequestReleasesItsStream(t *testing.T) {
	gate := newAbandonGate()
	h, conn := singleConnHarness(t, gate)

	before := conn.streams.Available()

	const rounds = 40
	for i := 0; i < rounds; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		result := h.query(ctx, nil)

		gate.awaitParked(t, i)
		cancel()
		require.Error(t, awaitQuery(t, result), "round %d: the caller gave up, so it must report an error", i)

		// The caller is gone; only now does the server answer.
		gate.release <- struct{}{}
	}

	require.Eventually(t, func() bool { return conn.streams.Available() == before },
		10*time.Second, 20*time.Millisecond,
		"%d of %d cancelled requests never returned their stream", before-conn.streams.Available(), rounds)
}

// TestConn_AbandonedRequestKeepsItsStreamUntilTheResponseArrives is the guard on
// the other side: a stream must NOT be reclaimed while a response can still turn
// up for it, or a later request would reuse the id and receive the old answer.
func TestConn_AbandonedRequestKeepsItsStreamUntilTheResponseArrives(t *testing.T) {
	gate := newAbandonGate()
	h, conn := singleConnHarness(t, gate)
	t.Cleanup(func() { close(gate.release) })

	before := conn.streams.Available()

	ctx, cancel := context.WithCancel(context.Background())
	result := h.query(ctx, nil)
	gate.awaitParked(t, 0)
	cancel()
	require.Error(t, awaitQuery(t, result))

	// The server is still holding the request, so the response is still owed.
	require.Never(t, func() bool { return conn.streams.Available() == before },
		500*time.Millisecond, 25*time.Millisecond,
		"the stream was reclaimed while a response could still arrive for it")
}

// TestConn_ResponseDeliveredBeforeCancellationReleasesItsStream covers the caller
// side of the same race.
//
// TestConn_CancelledRequestReleasesItsStream can only exercise the receive side:
// there the caller has fully given up before the response is sent, so the receive
// side is always the one that finds it. Here the order is reversed — the response
// is already in the buffer when the caller notices its context is done — and the
// caller's select then picks between them at random. Whenever it picks
// cancellation, only the caller's own drain can return the stream.
//
// Statistical for the same reason as the other direction, with the same 40
// rounds and the same ~2^-40 chance of a broken driver passing.
func TestConn_ResponseDeliveredBeforeCancellationReleasesItsStream(t *testing.T) {
	gate := newAbandonGate()
	h, conn := singleConnHarness(t, gate)

	before := conn.streams.Available()

	const rounds = 40
	for i := 0; i < rounds; i++ {
		ctx, cancel := context.WithCancel(context.Background())

		// Just before the caller waits, let the response through and cancel: the
		// caller reaches its select with both outcomes already available.
		testBeforeCallerRecv = func() {
			testBeforeCallerRecv = nil
			gate.release <- struct{}{}
			time.Sleep(20 * time.Millisecond)
			cancel()
		}

		result := h.query(ctx, nil)
		gate.awaitParked(t, i)
		_ = awaitQuery(t, result) // either outcome is legitimate; neither may leak
		cancel()
	}
	testBeforeCallerRecv = nil

	require.Eventually(t, func() bool { return conn.streams.Available() == before },
		10*time.Second, 20*time.Millisecond,
		"%d of %d requests never returned their stream", before-conn.streams.Available(), rounds)
}

// TestConn_DrainDistinguishesACloseNotificationFromAResponse pins what the
// closing flag is for.
//
// closeWithError publishes into the same buffer a real response uses, so a drain
// that treated every value alike would report a call as finished on the strength
// of a connection teardown, and take the observer's one terminal event with it —
// leaving the closer's StreamAbandoned to be swallowed.
func TestConn_DrainDistinguishesACloseNotificationFromAResponse(t *testing.T) {
	newAbandonedCall := func(c *Conn, rec *streamEventRecorder) *callReq {
		id, ok := c.streams.GetStream()
		require.True(t, ok)
		call := &callReq{
			resp:                  make(chan callResp, 1),
			timeout:               make(chan struct{}),
			streamID:              id,
			streamObserverContext: rec,
		}
		close(call.timeout) // the caller has given up
		return call
	}

	t.Run("a close notification is discarded", func(t *testing.T) {
		c := &Conn{streams: streams.New(int(protoVersion4), 64)}
		rec := newStreamEventRecorder()
		call := newAbandonedCall(c, rec)
		before := c.streams.Available()

		call.resp <- callResp{err: ErrConnectionClosed, closing: true}
		c.drainAbandoned(call)

		require.Equal(t, before, c.streams.Available(),
			"a teardown notification is not a response and must not end the call")
		require.Equal(t, int64(0), rec.finished.Load(),
			"reporting StreamFinished here would block the StreamAbandoned the closer owes")
	})

	t.Run("a real response ends the call", func(t *testing.T) {
		c := &Conn{streams: streams.New(int(protoVersion4), 64)}
		rec := newStreamEventRecorder()
		call := newAbandonedCall(c, rec)
		before := c.streams.Available()

		call.resp <- callResp{framer: getFramer(nil, protoVersion4, GlobalTypes)}
		c.drainAbandoned(call)

		require.Equal(t, before+1, c.streams.Available(),
			"the response arrived, so its stream is free to reuse")
		require.Equal(t, int64(1), rec.finished.Load())
		require.Equal(t, int64(0), rec.abandoned.Load())
	})

	t.Run("both values are drained", func(t *testing.T) {
		// closeWithError sends to a snapshot without deleting, so it can publish to
		// a call the receive side has already taken out of the map.
		c := &Conn{streams: streams.New(int(protoVersion4), 64)}
		rec := newStreamEventRecorder()
		call := newAbandonedCall(c, rec)
		call.resp = make(chan callResp, 2)
		before := c.streams.Available()

		call.resp <- callResp{err: ErrConnectionClosed, closing: true}
		call.resp <- callResp{framer: getFramer(nil, protoVersion4, GlobalTypes)}
		c.drainAbandoned(call)

		require.Equal(t, before+1, c.streams.Available(),
			"the real response was behind the notification and must still be found")
		require.Equal(t, int64(1), rec.finished.Load())
	})
}

// TestConn_CloseWithoutErrorEndsEveryStartedStream covers Conn.Close(), which
// closes with a nil error.
//
// The teardown used to gate its whole snapshot on having an error to deliver, so
// a plain close left outstanding calls with no terminal observer event at all —
// neither finished nor abandoned — in contradiction of StreamObserverContext's
// documented contract.
func TestConn_CloseWithoutErrorEndsEveryStartedStream(t *testing.T) {
	gate := newAbandonGate()
	recorder := newStreamEventRecorder()

	h := newFillHarnessOpts(t, 1, fillHarnessOpts{
		recvHook: gate.hook,
		tune: func(cfg *ClusterConfig) {
			cfg.NumConns = 1
			cfg.StreamObserver = recorder
		},
	})
	t.Cleanup(func() { close(gate.release) })

	conn := h.pickAnyConn(t, h.pool(t, h.hosts[0]))

	// Baselines before the query: connection setup uses streams of its own.
	recorder.drain()
	startedBefore := recorder.started.Load()
	endedBefore := recorder.finished.Load() + recorder.abandoned.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := h.query(ctx, nil)
	gate.awaitParked(t, 0)

	require.Equal(t, int64(1), recorder.started.Load()-startedBefore,
		"exactly one stream should be outstanding at this point")
	abandonedBefore := recorder.abandoned.Load()

	conn.Close() // nil error

	_ = awaitQuery(t, result)
	recorder.awaitEnd(t)

	require.Equal(t, recorder.started.Load()-startedBefore,
		recorder.finished.Load()+recorder.abandoned.Load()-endedBefore,
		"every stream still outstanding at close must get exactly one terminal event")
	require.Equal(t, int64(1), recorder.abandoned.Load()-abandonedBefore,
		"a call closed before its response is abandoned, not finished")
}
