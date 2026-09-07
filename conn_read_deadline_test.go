//go:build unit
// +build unit

package gocql

import (
	"bufio"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// scriptedStep is one result the scripted connection hands to a Read.
type scriptedStep struct {
	data []byte
	err  error
}

// scriptedConn is a net.Conn that records deadline calls and replays a script of
// read results, so a test can assert how many deadlines a single Read arms and
// how many times it goes back to the wire — facts that wall-clock timing cannot
// establish.
type scriptedConn struct {
	net.Conn // embedded for the methods these tests never call

	mu sync.Mutex

	script []scriptedStep
	reads  int

	deadlines    []time.Time
	deadlineErr  error
	deadlineErrs int
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reads++
	if len(c.script) == 0 {
		return 0, io.EOF
	}
	step := c.script[0]
	c.script = c.script[1:]
	n := copy(p, step.data)
	return n, step.err
}

func (c *scriptedConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.deadlines = append(c.deadlines, t)
	if c.deadlineErr != nil {
		c.deadlineErrs++
		return c.deadlineErr
	}
	return nil
}

func (c *scriptedConn) readCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reads
}

func (c *scriptedConn) deadlineCalls() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Time(nil), c.deadlines...)
}

// newScriptedReader wires a connReader over a scripted connection.
func newScriptedReader(steps ...scriptedStep) (*connReader, *scriptedConn) {
	sc := &scriptedConn{script: steps}
	// bufio would coalesce the scripted steps, so read straight from the script.
	return &connReader{conn: sc, r: bufio.NewReaderSize(sc, 1)}, sc
}

// scriptedTimeout is a net.Error reporting a timeout, as an expired read deadline does.
type scriptedTimeout struct{}

func (scriptedTimeout) Error() string   { return "i/o timeout" }
func (scriptedTimeout) Timeout() bool   { return true }
func (scriptedTimeout) Temporary() bool { return true }

// scriptedTemporary is a net.Error that is temporary but not a timeout, the only
// kind of failure the retry loop can still make progress on.
type scriptedTemporary struct{}

func (scriptedTemporary) Error() string   { return "temporary failure" }
func (scriptedTemporary) Timeout() bool   { return false }
func (scriptedTemporary) Temporary() bool { return true }

// TestConnReader_TimeoutArmsOneDeadlineAndReadsOnce is the core gate for spending
// one timeout per read rather than five.
//
// Both counts matter and they fail for different reasons: arming the deadline
// inside the retry loop shows up as five deadlines, and retrying past an expired
// deadline shows up as five reads. Elapsed time cannot separate them.
func TestConnReader_TimeoutArmsOneDeadlineAndReadsOnce(t *testing.T) {
	r, sc := newScriptedReader(
		scriptedStep{err: scriptedTimeout{}},
		scriptedStep{err: scriptedTimeout{}},
		scriptedStep{err: scriptedTimeout{}},
		scriptedStep{err: scriptedTimeout{}},
		scriptedStep{err: scriptedTimeout{}},
	)
	r.SetTimeout(200 * time.Millisecond)

	buf := make([]byte, 8)
	_, err := r.Read(buf)

	require.Error(t, err)
	var netErr net.Error
	require.True(t, errors.As(err, &netErr) && netErr.Timeout(), "got %v", err)

	require.Len(t, sc.deadlineCalls(), 1, "a read must arm exactly one deadline")
	require.Equal(t, 1, sc.readCount(), "an expired deadline must not be retried")
}

// TestConnReader_TemporaryNonTimeoutErrorIsRetriedUnderOneDeadline keeps the
// retry loop honest: a temporary failure that is not a timeout may still succeed
// on a retry, and that retry runs under the deadline already armed.
func TestConnReader_TemporaryNonTimeoutErrorIsRetriedUnderOneDeadline(t *testing.T) {
	r, sc := newScriptedReader(
		scriptedStep{err: scriptedTemporary{}},
		scriptedStep{data: []byte{1, 2, 3, 4}},
	)
	r.SetTimeout(200 * time.Millisecond)

	buf := make([]byte, 4)
	n, err := r.Read(buf)

	require.NoError(t, err, "a temporary non-timeout failure must be retried")
	require.Equal(t, 4, n)
	require.Len(t, sc.deadlineCalls(), 1, "the retry runs under the deadline already armed")
	require.Equal(t, 2, sc.readCount())
}

// TestConnReader_ZeroTimeoutClearsTheDeadline is the CASSGO-125 guard: callers
// disarm the timeout around idle reads by setting it to zero, and a deadline left
// over from an earlier read would fire on an idle connection and reconnect it.
func TestConnReader_ZeroTimeoutClearsTheDeadline(t *testing.T) {
	r, sc := newScriptedReader(scriptedStep{data: []byte{9, 9}})
	r.SetTimeout(0)

	buf := make([]byte, 2)
	n, err := r.Read(buf)

	require.NoError(t, err)
	require.Equal(t, 2, n, "the read must still deliver its bytes")
	require.Equal(t, []byte{9, 9}, buf)

	calls := sc.deadlineCalls()
	require.Len(t, calls, 1)
	require.True(t, calls[0].IsZero(), "a zero timeout must clear the deadline, not set one")
}

// TestConnReader_NegativeTimeoutTouchesNoDeadline pins the pre-existing handling
// of a negative timeout, which is neither armed nor cleared.
func TestConnReader_NegativeTimeoutTouchesNoDeadline(t *testing.T) {
	r, sc := newScriptedReader(scriptedStep{data: []byte{7}})
	r.SetTimeout(-1)

	buf := make([]byte, 1)
	_, err := r.Read(buf)

	require.NoError(t, err)
	require.Empty(t, sc.deadlineCalls(), "a negative timeout must leave the deadline alone")
}

// TestConnReader_TimeoutFlippedBetweenReadsUsesTheNewValue mirrors what
// recvSegment does through onSegmentHeader: it reads the segment header with the
// timeout disarmed and re-arms it for the payload. A deadline cached on the
// connReader would miss that flip.
func TestConnReader_TimeoutFlippedBetweenReadsUsesTheNewValue(t *testing.T) {
	r, sc := newScriptedReader(
		scriptedStep{data: []byte{1}},
		scriptedStep{data: []byte{2}},
	)

	buf := make([]byte, 1)

	r.SetTimeout(0)
	_, err := r.Read(buf)
	require.NoError(t, err)

	r.SetTimeout(150 * time.Millisecond)
	_, err = r.Read(buf)
	require.NoError(t, err)

	calls := sc.deadlineCalls()
	require.Len(t, calls, 2)
	require.True(t, calls[0].IsZero(), "the first read ran with the timeout disarmed")
	require.False(t, calls[1].IsZero(), "the second read must pick up the timeout set between them")
}

// TestConnReader_DeadlineSetterErrorIsReturned covers a connection that cannot
// carry a deadline at all — one from a caller's own Dialer or HostDialer. Reading
// on would promise a bound the connection cannot keep.
func TestConnReader_DeadlineSetterErrorIsReturned(t *testing.T) {
	setterErr := errors.New("cannot set deadline")

	for _, tt := range []struct {
		name    string
		timeout time.Duration
	}{
		{name: "arming fails", timeout: 200 * time.Millisecond},
		{name: "clearing fails", timeout: 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r, sc := newScriptedReader(scriptedStep{data: []byte{1, 2}})
			sc.deadlineErr = setterErr
			r.SetTimeout(tt.timeout)

			buf := make([]byte, 2)
			n, err := r.Read(buf)

			require.ErrorIs(t, err, setterErr)
			require.Zero(t, n)
			require.Zero(t, sc.readCount(), "no read may be attempted without a usable deadline")
		})
	}
}

// TestConnReader_RealSocketSpendsOneTimeout is an end-to-end sanity check against
// a peer that accepts and never writes. The upper bound is a hang watchdog, not a
// timing assertion: the counted evidence is in the scripted tests above.
func TestConnReader_RealSocketSpendsOneTimeout(t *testing.T) {
	const timeout = 200 * time.Millisecond

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			accepted <- conn
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	select {
	case server := <-accepted:
		defer server.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("the listener never accepted")
	}

	r := &connReader{conn: conn, r: bufio.NewReader(conn)}
	r.SetTimeout(timeout)

	start := time.Now()
	_, err = r.Read(make([]byte, 8))
	elapsed := time.Since(start)

	require.Error(t, err)
	require.GreaterOrEqual(t, elapsed, timeout)
	// Before the fix this took five timeouts; anything near that is a regression.
	require.Less(t, elapsed, 3*timeout, "the read spent more than one timeout")
}
