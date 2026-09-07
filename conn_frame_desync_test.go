//go:build unit
// +build unit

package gocql

import (
	"context"
	"encoding/binary"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/internal/streams"
)

// streamEventRecorder counts the terminal events a StreamObserver receives, so a
// test can assert that every started stream ends exactly once and with the right
// kind of event.
type streamEventRecorder struct {
	started   atomic.Int64
	finished  atomic.Int64
	abandoned atomic.Int64

	ended chan struct{}
	once  sync.Once
}

func newStreamEventRecorder() *streamEventRecorder {
	return &streamEventRecorder{ended: make(chan struct{}, 16)}
}

func (r *streamEventRecorder) StreamContext(context.Context) StreamObserverContext { return r }

func (r *streamEventRecorder) StreamStarted(ObservedStream) { r.started.Add(1) }

func (r *streamEventRecorder) StreamFinished(ObservedStream) {
	r.finished.Add(1)
	r.signal()
}

func (r *streamEventRecorder) StreamAbandoned(ObservedStream) {
	r.abandoned.Add(1)
	r.signal()
}

func (r *streamEventRecorder) signal() {
	select {
	case r.ended <- struct{}{}:
	default:
	}
}

// drain discards terminal-event signals recorded so far.
func (r *streamEventRecorder) drain() {
	for {
		select {
		case <-r.ended:
		default:
			return
		}
	}
}

// awaitEnd waits for one terminal stream event.
func (r *streamEventRecorder) awaitEnd(t *testing.T) {
	t.Helper()

	select {
	case <-r.ended:
	case <-time.After(10 * time.Second):
		t.Fatalf("no terminal stream event arrived (started=%d finished=%d abandoned=%d)",
			r.started.Load(), r.finished.Load(), r.abandoned.Load())
	}
}

// writeTruncatedFrame answers reqFrame with a v4 header promising bodyLen bytes
// and then sends only sent of them, leaving the connection mid-frame with no way
// to find the next header.
func writeTruncatedFrame(conn net.Conn, reqFrame *framer, bodyLen, sent int) {
	head := make([]byte, 9)
	head[0] = protoVersion4 | protoDirectionMask // response
	head[1] = 0
	binary.BigEndian.PutUint16(head[2:4], uint16(reqFrame.header.stream))
	head[4] = byte(opResult)
	binary.BigEndian.PutUint32(head[5:9], uint32(bodyLen))

	_, _ = conn.Write(head)
	if sent > 0 {
		_, _ = conn.Write(make([]byte, sent))
	}
}

// TestConn_PartialBodyClosesTheConnection proves that a response whose body stops
// arriving retires the connection instead of leaving it in the pool to parse the
// leftover bytes as the next frame header, and that the detached call still gets
// its terminal observer event.
func TestConn_PartialBodyClosesTheConnection(t *testing.T) {
	recorder := newStreamEventRecorder()

	srv := newTestServerOpts{
		addr:     "127.0.0.1:0",
		protocol: defaultProto,
		rawRespFn: func(conn net.Conn, reqFrame *framer) bool {
			if reqFrame.header.op != opQuery {
				return false
			}
			// A header claiming 128 bytes of body, followed by 8 of them.
			writeTruncatedFrame(conn, reqFrame, 128, 8)
			return true
		},
	}.newServer(t, context.Background())
	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = 1
	cluster.Timeout = 500 * time.Millisecond
	cluster.StreamObserver = recorder

	session, err := cluster.CreateSession()
	require.NoError(t, err)
	defer session.Close()

	pool, ok := session.pool.getPool(session.ring.allHosts()[0])
	require.True(t, ok, "the host must have a pool")
	pool.mu.RLock()
	require.NotEmpty(t, pool.conns)
	conn := pool.conns[0]
	pool.mu.RUnlock()

	// Startup traffic uses streams of its own, so measure this query's events as
	// deltas rather than absolute counts.
	recorder.drain()
	finishedBefore := recorder.finished.Load()
	abandonedBefore := recorder.abandoned.Load()

	require.Error(t, session.Query("void").Exec(), "a truncated response cannot succeed")

	recorder.awaitEnd(t)
	require.Equal(t, int64(0), recorder.finished.Load()-finishedBefore,
		"no response was delivered, so the stream was abandoned rather than finished")
	require.Equal(t, int64(1), recorder.abandoned.Load()-abandonedBefore,
		"the detached call must still receive exactly one terminal event")

	require.Eventually(t, conn.Closed, 10*time.Second, 20*time.Millisecond,
		"a connection that lost its frame boundary must be retired, not left serving queries")
}

// TestConn_TerminalCleanupDoesNotHandOutTheStream proves the terminal path leaves
// the broken request's stream id occupied. Returning it before the connection is
// marked closed would let another caller claim it and write into a connection
// already known to be unusable.
func TestConn_TerminalCleanupDoesNotHandOutTheStream(t *testing.T) {
	var (
		mu       sync.Mutex
		breakNow bool
		brokeAt  = make(chan int, 1)
	)

	srv := newTestServerOpts{
		addr:     "127.0.0.1:0",
		protocol: defaultProto,
		rawRespFn: func(conn net.Conn, reqFrame *framer) bool {
			if reqFrame.header.op != opQuery {
				return false
			}
			mu.Lock()
			arm := breakNow
			breakNow = false
			mu.Unlock()
			if !arm {
				return false
			}
			select {
			case brokeAt <- reqFrame.header.stream:
			default:
			}
			writeTruncatedFrame(conn, reqFrame, 128, 8)
			return true
		},
	}.newServer(t, context.Background())
	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = 1
	cluster.Timeout = 500 * time.Millisecond

	session, err := cluster.CreateSession()
	require.NoError(t, err)
	defer session.Close()

	pool, ok := session.pool.getPool(session.ring.allHosts()[0])
	require.True(t, ok)
	pool.mu.RLock()
	require.NotEmpty(t, pool.conns)
	conn := pool.conns[0]
	pool.mu.RUnlock()

	mu.Lock()
	breakNow = true
	mu.Unlock()

	require.Error(t, session.Query("void").Exec())

	var broken int
	select {
	case broken = <-brokeAt:
	case <-time.After(10 * time.Second):
		t.Fatal("the server never produced a truncated response")
	}

	// The id of the request whose frame was lost must not be available again on
	// this connection, whether or not the close has completed yet.
	// Wait for the close before inspecting the allocator. closeWithError runs
	// only after processFrame has returned, so a closed connection proves the
	// terminal path has finished; without this barrier the assertion races it and
	// passes whatever the terminal path does.
	require.Eventually(t, conn.Closed, 10*time.Second, 20*time.Millisecond,
		"the connection must be retired after losing its frame boundary")

	// Clear reports whether the id was still in use: it must be, because the
	// terminal path deliberately leaves it occupied until the connection dies.
	require.True(t, conn.streams.Clear(broken),
		"stream %d was returned to the allocator by the terminal path, before the connection was retired", broken)
}

// TestConn_StreamIsNotClearedTwiceAcrossReallocation is the gate for the per-call
// claim on stream reclamation.
//
// IDGenerator.Clear only inspects the bit, so it cannot tell a stale second
// reclamation of an old call from a legitimate one: once the id has been handed
// out again, clearing it a second time frees whichever request holds it now. The
// claim on callReq is what makes reclaiming a call twice a no-op instead.
func TestConn_StreamIsNotClearedTwiceAcrossReallocation(t *testing.T) {
	c := &Conn{streams: streams.New(int(protoVersion4), 64)}

	idA, ok := c.streams.GetStream()
	require.True(t, ok, "the generator must hand out a first stream")
	callA := &callReq{streamID: idA}

	c.releaseStream(callA)

	// Take ids until the generator hands idA back out; that reallocation is the
	// whole point of the test, so give up loudly rather than silently passing.
	var reallocated bool
	for i := 0; i < c.streams.NumStreams; i++ {
		id, ok := c.streams.GetStream()
		if !ok {
			break
		}
		if id == idA {
			reallocated = true
			break
		}
	}
	require.True(t, reallocated, "the generator never reissued stream %d, so the test proves nothing", idA)

	before := c.streams.Available()

	// The old call terminates a second time — a stale receiver-side cleanup, say.
	c.releaseStream(callA)

	require.Equal(t, before, c.streams.Available(),
		"reclaiming an already-reclaimed call freed stream %d out from under its new owner", idA)
}

// writeCompressedFlagFrame answers reqFrame with a response whose body is
// complete but marked compressed. A client with no compressor reads the whole
// body and then fails to make sense of it — an error that leaves the wire
// exactly where it should be.
func writeCompressedFlagFrame(conn net.Conn, reqFrame *framer) {
	body := []byte("not really compressed")

	head := make([]byte, 9)
	head[0] = protoVersion4 | protoDirectionMask
	head[1] = flagCompress
	binary.BigEndian.PutUint16(head[2:4], uint16(reqFrame.header.stream))
	head[4] = byte(opResult)
	binary.BigEndian.PutUint32(head[5:9], uint32(len(body)))

	_, _ = conn.Write(head)
	_, _ = conn.Write(body)
}

// TestConn_NonTerminalFrameErrorKeepsTheConnection is the guard on the other side
// of the classification.
//
// A body that arrived in full leaves the frame boundary intact, so however the
// error is reported it belongs to the caller and the connection is still good.
// Closing here would turn one unreadable response into a reconnect.
//
// Asserting that the error surfaces is not enough on its own: the connection has
// to go on working, so the test runs a second query over the same one and
// requires it to succeed.
func TestConn_NonTerminalFrameErrorKeepsTheConnection(t *testing.T) {
	var breakNext atomic.Bool

	srv := newTestServerOpts{
		addr:     "127.0.0.1:0",
		protocol: defaultProto,
		rawRespFn: func(conn net.Conn, reqFrame *framer) bool {
			if reqFrame.header.op != opQuery || !breakNext.CompareAndSwap(true, false) {
				return false
			}
			writeCompressedFlagFrame(conn, reqFrame)
			return true
		},
	}.newServer(t, context.Background())
	defer srv.Stop()

	cluster := testCluster(defaultProto, srv.Address)
	cluster.NumConns = 1
	cluster.Timeout = 2 * time.Second

	session, err := cluster.CreateSession()
	require.NoError(t, err)
	defer session.Close()

	pool, ok := session.pool.getPool(session.ring.allHosts()[0])
	require.True(t, ok)
	pool.mu.RLock()
	require.NotEmpty(t, pool.conns)
	conn := pool.conns[0]
	pool.mu.RUnlock()

	breakNext.Store(true)
	require.Error(t, session.Query("void").Exec(), "an undecodable body is still an error for the caller")

	require.False(t, conn.Closed(),
		"the body arrived in full, so the frame boundary is intact and the connection must survive")
	require.NoError(t, session.Query("void").Exec(),
		"the same connection must go on serving queries")
	require.False(t, conn.Closed(), "the follow-up query must not have retired it either")
}
