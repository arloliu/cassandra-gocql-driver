//go:build unit
// +build unit

package gocql

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// requestTimerGate parks one QUERY at a time in the test server's receive loop,
// so a request is in flight from the driver's point of view until it is released.
type requestTimerGate struct {
	started chan struct{}
	release chan struct{}
}

func (g *requestTimerGate) hook(_ string, f *framer) {
	if f.header == nil || f.header.op != opQuery {
		return
	}
	select {
	case g.started <- struct{}{}:
	default:
	}
	<-g.release
}

// TestConn_RequestTimeoutStillFiresWithACallerLocalTimer is the behaviour-preservation
// gate for moving the request timer out of callReq and into execInternal.
//
// The timer is now a local owned by the caller, so this proves the connection-level
// request timeout still arms, still fires at roughly the configured timeout, and still
// surfaces as ErrTimeoutNoResponse rather than a context error.
func TestConn_RequestTimeoutStillFiresWithACallerLocalTimer(t *testing.T) {
	const requestTimeout = 300 * time.Millisecond

	gate := &requestTimerGate{started: make(chan struct{}, 1), release: make(chan struct{})}
	h := newFillHarnessOpts(t, 1, fillHarnessOpts{
		recvHook: gate.hook,
		tune: func(cfg *ClusterConfig) {
			cfg.NumConns = 1
			cfg.Timeout = requestTimeout
		},
	})
	t.Cleanup(func() { close(gate.release) })

	// A caller context with no deadline of its own: the connection-level request
	// timer is the only thing that can end this wait.
	start := time.Now()
	result := h.query(context.Background(), nil)

	select {
	case <-gate.started:
	case <-time.After(fillEventBudget):
		t.Fatal("the query never reached the server")
	}

	err := awaitQuery(t, result)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, ErrTimeoutNoResponse,
		"a parked request must end on the connection request timeout, not a context error")
	require.NotErrorIs(t, err, context.DeadlineExceeded,
		"the caller context had no deadline, so its error must not appear here")
	require.GreaterOrEqual(t, elapsed, requestTimeout,
		"the timer must not fire before the configured timeout")
	// Loose upper bound: a hang watchdog, not a timing assertion.
	require.Less(t, elapsed, 10*requestTimeout, "the request timeout did not bound the wait")
}
