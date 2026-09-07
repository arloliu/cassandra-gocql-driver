//go:build unit
// +build unit

package gocql

import (
	"context"
	"errors"
	"sync/atomic"
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
func singleConnHarness(t *testing.T, gate *abandonGate, proto protoVersion, hooks *connTestHooks) (*fillHarness, *Conn) {
	t.Helper()

	h := newFillHarnessOpts(t, 1, fillHarnessOpts{
		recvHook: gate.hook,
		proto:    proto,
		hooks:    hooks,
		tune:     func(cfg *ClusterConfig) { cfg.NumConns = 1 },
	})

	pool := h.pool(t, h.hosts[0])
	return h, h.pickAnyConn(t, pool)
}

// isQuery reports whether a request is the test's own QUERY rather than
// background traffic. Every connection runs a heartbeat that goes through the
// same code as a query, so a hook that does not filter would fire for it too.
func isQuery(req frameBuilder) bool {
	_, ok := req.(*writeQueryFrame)
	return ok
}

// protoCases are the protocol versions the stream-lifecycle tests run under.
// v4 frames each response on its own; v5 wraps them in segments, which is a
// different receive path entirely (recvSegment rather than processFrame).
var protoCases = []struct {
	name  string
	proto protoVersion
}{
	{name: "v4", proto: protoVersion4},
	{name: "v5", proto: protoVersion5},
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
	for _, pc := range protoCases {
		t.Run(pc.name, func(t *testing.T) {
			testCancelledRequestReleasesItsStream(t, pc.proto)
		})
	}
}

func testCancelledRequestReleasesItsStream(t *testing.T, proto protoVersion) {
	gate := newAbandonGate()
	h, conn := singleConnHarness(t, gate, proto, nil)

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
	for _, pc := range protoCases {
		t.Run(pc.name, func(t *testing.T) {
			testAbandonedRequestKeepsItsStream(t, pc.proto)
		})
	}
}

func testAbandonedRequestKeepsItsStream(t *testing.T, proto protoVersion) {
	gate := newAbandonGate()
	h, conn := singleConnHarness(t, gate, proto, nil)
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

// TestConn_ReceiverReclaimsWhenItTakesTheDeliveryArm makes the receive side the
// only party that can reclaim the call, deterministically.
//
// The caller finishes abandoning against an empty buffer, so its own drain finds
// nothing; the response is then delivered, and only the drain that follows that
// delivery can return the stream. Delivery is forced because both arms of the
// receive side's select are legitimately ready once the caller has gone, and
// which one wins is not something a test can arrange from outside.
func TestConn_ReceiverReclaimsWhenItTakesTheDeliveryArm(t *testing.T) {
	for _, pc := range protoCases {
		t.Run(pc.name, func(t *testing.T) {
			testReceiverReclaims(t, pc.proto)
		})
	}
}

func testReceiverReclaims(t *testing.T, proto protoVersion) {
	var framerReleases atomic.Int64
	hooks := &connTestHooks{
		forceDeliverArm: true,
		onFramerRelease: func() { framerReleases.Add(1) },
	}

	gate := newAbandonGate()
	h, conn := singleConnHarness(t, gate, proto, hooks)

	before := conn.streams.Available()

	ctx, cancel := context.WithCancel(context.Background())
	result := h.query(ctx, nil)
	gate.awaitParked(t, 0)

	// The caller gives up and completes its own drain before the answer exists.
	cancel()
	require.Error(t, awaitQuery(t, result))

	gate.release <- struct{}{}

	require.Eventually(t, func() bool { return conn.streams.Available() == before },
		10*time.Second, 20*time.Millisecond,
		"the receive side's drain is the only reclaimer in this schedule and did not run")
	require.Equal(t, int64(1), framerReleases.Load(),
		"the response framer must be returned to the pool exactly once")
}

// TestConn_CallerReclaimsWhenItTakesItsAbandonmentArm is the mirror: the caller
// is the only party that can reclaim.
//
// The response is delivered and the receive side's drain has already run and
// found the caller still present, so nothing on that side will look again. The
// caller then takes its abandonment arm — forced, because by then its response
// and its cancellation are both ready — and only its own drain is left to return
// the stream.
func TestConn_CallerReclaimsWhenItTakesItsAbandonmentArm(t *testing.T) {
	for _, pc := range protoCases {
		t.Run(pc.name, func(t *testing.T) {
			testCallerReclaims(t, pc.proto)
		})
	}
}

func testCallerReclaims(t *testing.T, proto protoVersion) {
	var (
		framerReleases atomic.Int64
		delivered      = make(chan struct{}, 1)
		cancelQuery    context.CancelFunc
	)

	hooks := &connTestHooks{
		forceAbandonArm: isQuery,
		onFramerRelease: func() { framerReleases.Add(1) },
		afterDeliver: func(op frameOp) {
			if op != opResult {
				return // connection setup uses this path too
			}
			select {
			case delivered <- struct{}{}:
			default:
			}
		},
	}
	hooks.beforeCallerRecv = func(req frameBuilder) {
		if !isQuery(req) {
			return
		}
		// Wait for the response to be buffered and the receive side's drain to
		// have run, then cancel. No sleeping: the acknowledgement is the event.
		select {
		case <-delivered:
		case <-time.After(10 * time.Second):
			panic("the response was never delivered")
		}
		cancelQuery()
	}

	gate := newAbandonGate()
	h, conn := singleConnHarness(t, gate, proto, hooks)

	before := conn.streams.Available()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelQuery = cancel

	result := h.query(ctx, nil)
	gate.awaitParked(t, 0)
	gate.release <- struct{}{}

	require.Error(t, awaitQuery(t, result), "the forced arm returns the context error")

	require.Eventually(t, func() bool { return conn.streams.Available() == before },
		10*time.Second, 20*time.Millisecond,
		"the caller's own drain is the only reclaimer in this schedule and did not run")
	require.Equal(t, int64(1), framerReleases.Load(),
		"the response framer must be returned to the pool exactly once")
}

// TestConn_LiveCallerKeepsItsOwnResponse guards the check that keeps a drain off
// a call whose caller is still waiting.
//
// The response is delivered while the caller has been held short of receiving
// it. If the drain consulted the buffer without first checking that the caller
// had abandoned the call, it would take that response and the caller would be
// left with a cancellation it never asked for.
func TestConn_LiveCallerKeepsItsOwnResponse(t *testing.T) {
	for _, pc := range protoCases {
		t.Run(pc.name, func(t *testing.T) {
			testLiveCallerKeepsItsResponse(t, pc.proto)
		})
	}
}

func testLiveCallerKeepsItsResponse(t *testing.T, proto protoVersion) {
	var (
		delivered = make(chan struct{}, 1)
		release   = make(chan struct{})
	)

	hooks := &connTestHooks{
		forceDeliverArm: true,
		afterDeliver: func(op frameOp) {
			if op != opResult {
				return // connection setup uses this path too
			}
			select {
			case delivered <- struct{}{}:
			default:
			}
		},
	}
	hooks.beforeCallerRecv = func(req frameBuilder) {
		if !isQuery(req) {
			return
		}
		// Hold the caller short of its receive until the response, and the drain
		// that follows delivery, have both happened.
		select {
		case <-delivered:
		case <-time.After(10 * time.Second):
			panic("the response was never delivered")
		}
		<-release
	}

	gate := newAbandonGate()
	h, conn := singleConnHarness(t, gate, proto, hooks)

	before := conn.streams.Available()

	result := h.query(context.Background(), nil)
	gate.awaitParked(t, 0)
	gate.release <- struct{}{}
	close(release)

	require.NoError(t, awaitQuery(t, result),
		"a caller that never gave up must still receive its own response")
	require.Eventually(t, func() bool { return conn.streams.Available() == before },
		10*time.Second, 20*time.Millisecond, "the stream must be returned as usual")
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

		// Give the framer a non-empty buffer: release resets it on the way back to
		// the pool, so an emptied buffer is evidence that release actually ran,
		// rather than that the drain merely reached the line that calls it.
		f := getFramer(nil, protoVersion4, GlobalTypes)
		f.buf = append(f.buf, 1, 2, 3, 4)

		call.resp <- callResp{framer: f}
		c.drainAbandoned(call)

		require.Empty(t, f.buf,
			"the response framer must actually be released, not merely reached")

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

// TestConn_CloseNotificationIsNotMistakenForAResponse forces the schedule in
// which a caller receives a teardown notification through its ordinary response
// branch.
//
// closeWithError publishes into the same channel a real response uses, so the
// value has to say which it is. If it did not, the caller would treat a
// connection teardown as its answer: it would reclaim the stream and report
// StreamFinished, and because a call gets exactly one terminal event, the
// StreamAbandoned the closer owes would be swallowed.
//
// The connection context is held closed until the caller has returned, so the
// caller cannot escape through it and must take the response branch.
func TestConn_CloseNotificationIsNotMistakenForAResponse(t *testing.T) {
	var (
		recorder    = newStreamEventRecorder()
		callerDone  = make(chan struct{})
		holdCancel  = make(chan struct{})
		holdNotify  = make(chan struct{})
		closerReady = make(chan struct{}, 1)
		barrierFail = make(chan string, 4)
	)

	hooks := &connTestHooks{
		closerAfterSnapshot: func() {
			select {
			case closerReady <- struct{}{}:
			default:
			}
		},
		closerAfterSend: func() {
			// Let the caller act on the value before the closer reports the call.
			// A call gets one terminal event, so whoever reports first decides
			// what is recorded; without this the closer would always win and the
			// caller's decision would be invisible.
			select {
			case <-holdNotify:
			case <-time.After(10 * time.Second):
				// Releasing on a timeout would drop the ordering this test is
				// built on and report a pass it did not earn.
				barrierFail <- "the closer's notification barrier timed out"
			}
		},
		closerBeforeCancel: func() {
			// Keep the connection context alive until the caller has taken the
			// notification through its response branch.
			select {
			case <-holdCancel:
			case <-time.After(10 * time.Second):
				barrierFail <- "the connection-cancellation barrier timed out"
			}
		},
	}

	gate := newAbandonGate()
	h := newFillHarnessOpts(t, 1, fillHarnessOpts{
		recvHook: gate.hook,
		hooks:    hooks,
		tune: func(cfg *ClusterConfig) {
			cfg.NumConns = 1
			cfg.StreamObserver = recorder
		},
	})
	t.Cleanup(func() { close(gate.release) })

	conn := h.pickAnyConn(t, h.pool(t, h.hosts[0]))

	recorder.drain()
	finishedBefore := recorder.finished.Load()
	abandonedBefore := recorder.abandoned.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := h.query(ctx, nil)
	gate.awaitParked(t, 0)

	go func() {
		conn.closeWithError(errors.New("injected teardown"))
	}()

	select {
	case <-closerReady:
	case <-time.After(10 * time.Second):
		t.Fatal("the closer never reached its snapshot")
	}

	go func() {
		_ = awaitQuery(t, result)
		close(callerDone)
	}()

	select {
	case <-callerDone:
	case <-time.After(10 * time.Second):
		close(holdNotify)
		close(holdCancel)
		t.Fatal("the caller never returned")
	}
	close(holdNotify)
	close(holdCancel)

	select {
	case why := <-barrierFail:
		t.Fatalf("the forced schedule did not hold: %s", why)
	default:
	}

	recorder.awaitEnd(t)
	require.Equal(t, int64(0), recorder.finished.Load()-finishedBefore,
		"a teardown notification is not a response and must not be reported as a finished stream")
	require.Equal(t, int64(1), recorder.abandoned.Load()-abandonedBefore,
		"the closer owes exactly one StreamAbandoned for the outstanding call")
}

// TestConn_EndCallOnResponseJudgesTheValueNotTheConnection gates the predicate
// that decides whether a delivered value ends its call.
//
// The distinction matters exactly when a teardown is already under way. By then
// the receive side may have taken the call out of the call map, so the closer's
// snapshot cannot see it and will never report it. Asking the connection whether
// it is closed would decline to end the call, and it would get no terminal event
// at all. The value itself says which kind it is.
func TestConn_EndCallOnResponseJudgesTheValueNotTheConnection(t *testing.T) {
	newCall := func(c *Conn, rec *streamEventRecorder) *callReq {
		id, ok := c.streams.GetStream()
		require.True(t, ok)
		return &callReq{
			resp:                  make(chan callResp, 1),
			timeout:               make(chan struct{}),
			streamID:              id,
			streamObserverContext: rec,
		}
	}

	t.Run("a wire response ends the call even while closing", func(t *testing.T) {
		c := &Conn{streams: streams.New(int(protoVersion4), 64)}
		c.closed = true // a teardown is already under way
		rec := newStreamEventRecorder()
		call := newCall(c, rec)
		before := c.streams.Available()

		c.endCallOnResponse(call, callResp{err: errors.New("server error frame")})

		require.Equal(t, before+1, c.streams.Available(),
			"the answer arrived, so the call is over regardless of the connection")
		require.Equal(t, int64(1), rec.finished.Load())
	})

	t.Run("a teardown notification is left to the closer", func(t *testing.T) {
		c := &Conn{streams: streams.New(int(protoVersion4), 64)}
		c.closed = true
		rec := newStreamEventRecorder()
		call := newCall(c, rec)
		before := c.streams.Available()

		c.endCallOnResponse(call, callResp{err: ErrConnectionClosed, closing: true})

		require.Equal(t, before, c.streams.Available(),
			"the closer owns this call and takes the allocator with it")
		require.Equal(t, int64(0), rec.finished.Load())
	})
}
