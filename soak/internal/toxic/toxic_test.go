package toxic

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/proxy"
)

// world simulates one node behind its proxy, as toxiproxy v2.12.0 and Cassandra behave for the probe:
// the toxics present, and per probe connection the counters of Cassandra's socket and toxiproxy's accepted socket.
// Each flag breaks one part of the fixture.
type world struct {
	mu     sync.Mutex
	toxics map[string]proxy.Toxic
	// zero holds the toxics updated to toxicity 0, which forward like a noop.
	zero map[string]bool
	// conns by probe name.
	conns map[string]*fakeConn
	calls []string
	// ignore makes the named toxic types have no effect, as if toxiproxy did not apply them.
	ignore map[string]bool
	// noProcess makes Cassandra receive requests but never answer them.
	noProcess bool
	// unconsumedUp and unconsumedDown make held requests or responses survive their own toxic's toxicity-0
	// update; silentUp forwards a surviving request but withholds its response.
	unconsumedUp, unconsumedDown, silentUp bool
	// fragment delivers every response as a header, a part and the rest.
	fragment bool
	// errorOp answers requests of this opcode with a short ERROR frame; errorDelay delays it.
	errorOp    byte
	errorDelay time.Duration
	// shortBlob answers the blob read with a RESULT shorter than the blob.
	shortBlob bool
	// updateErr fails every UpdateToxic; residue keeps deleted toxics listed.
	updateErr, residue bool
	// addErr fails AddToxic for a name.
	addErr string
	// afterUpdate, when set, runs after each update with the lock held.
	afterUpdate func(name string)
}

type sockState struct {
	cassReceived, cassAcked, proxyReceived int64
}

// fakeConn answers requests as the world's toxics dictate.
type fakeConn struct {
	w         *world
	name      string
	up, local netip.AddrPort
	events    chan proxy.FrameEvent
	next      int16
	s         sockState
	closed    int
	dead      bool
	heldReq   []heldReq
	heldResp  []int16
	// requests records each request's opcode, in order.
	requests []byte
	stop     chan struct{}
}

type heldReq struct {
	stream int16
	op     byte
	body   []byte
}

// downIgnoring adds the downstream timeout as an ignored toxic type.
type downIgnoring struct{ *world }

// upIgnoring adds the upstream timeout as an ignored toxic type.
type upIgnoring struct{ *world }

func newWorld() *world {
	return &world{toxics: map[string]proxy.Toxic{}, zero: map[string]bool{}, conns: map[string]*fakeConn{}, ignore: map[string]bool{}}
}

func TestToxicsTable(t *testing.T) {
	require.Equal(t, []proxy.Toxic{{Name: "soak-t3-down", Type: "latency", Stream: "downstream", Toxicity: 1,
		Attributes: map[string]any{"latency": 1500, "jitter": 300}}}, Toxics(chaos.FaultLat, 3, 1500))
	require.Equal(t, []proxy.Toxic{{Name: "soak-t3-down", Type: "bandwidth", Stream: "downstream", Toxicity: 1,
		Attributes: map[string]any{"rate": 16}}}, Toxics(chaos.FaultBW, 3, 16))
	black := Toxics(chaos.FaultBlack, 3, 0)
	require.Len(t, black, 2)
	require.Equal(t, []string{"downstream", "upstream"}, []string{black[0].Stream, black[1].Stream}, "downstream first")
	require.Equal(t, "soak-t3-up", black[1].Name)
	require.Equal(t, "timeout", Toxics(chaos.FaultHalfOpen, 3, 0)[0].Type)
	require.Equal(t, proxy.Toxic{Name: "soak-t3-up", Type: "reset_peer", Stream: "upstream", Toxicity: 1,
		Attributes: map[string]any{"timeout": 0}}, Toxics(chaos.FaultReset, 3, 0)[0])
	require.Nil(t, Toxics(chaos.FaultStop, 3, 0))
}

func TestNewRefusals(t *testing.T) {
	_, err := New(Config{Kind: chaos.FaultStop})
	require.Error(t, err)
	_, err = New(Config{Kind: chaos.FaultLat})
	require.Error(t, err, "F-lat needs its latency")
	_, err = New(Config{Kind: chaos.FaultBW, Param: 16})
	require.NoError(t, err)
}

func TestBandwidthHolds(t *testing.T) {
	t0 := time.Unix(1000, 0)
	limit16, limit128 := RateLimit(16), RateLimit(128)
	require.InDelta(t, 32000.0, limit16, 0)

	decided, _ := BandwidthHolds(8000, t0, t0.Add(500*time.Millisecond), false, limit16)
	require.False(t, decided, "too few bytes and too short")
	decided, holds := BandwidthHolds(17600, t0, t0.Add(1100*time.Millisecond), false, limit16)
	require.True(t, decided)
	require.True(t, holds, "16 KB/s throttled")
	decided, holds = BandwidthHolds(128000, t0, t0.Add(time.Second), false, limit128)
	require.True(t, decided)
	require.True(t, holds, "128 KB/s throttled")
	decided, holds = BandwidthHolds(264134, t0, t0.Add(2*time.Millisecond), true, limit128)
	require.True(t, decided)
	require.False(t, holds, "an unthrottled transfer")
	// A response delayed 3 s, then delivered at full speed: its first byte is at +3 s (PLAN §60 BR01).
	decided, holds = BandwidthHolds(264134, t0.Add(3*time.Second), t0.Add(3*time.Second+5*time.Millisecond), true, limit128)
	require.True(t, decided)
	require.False(t, holds, "a delayed fast response")
	decided, holds = BandwidthHolds(300000, t0, t0.Add(time.Second), false, limit16)
	require.True(t, decided)
	require.False(t, holds, "fast enough to exceed the limit")
}

func TestReceiptAndDropHold(t *testing.T) {
	before := Snapshot{CassReceived: 100, CassAcked: 200, ProxyReceived: 100}
	req, resp := 50, 69
	require.True(t, ReceiptHolds(before, Snapshot{CassReceived: 150, CassAcked: 269}, req, resp))
	require.False(t, ReceiptHolds(before, Snapshot{CassReceived: 150, CassAcked: 200}, req, resp), "received, not answered")
	require.False(t, ReceiptHolds(before, Snapshot{CassReceived: 100, CassAcked: 269}, req, resp), "not received")

	require.True(t, DropHolds(before, Snapshot{CassReceived: 100, ProxyReceived: 150}, req))
	require.False(t, DropHolds(before, Snapshot{CassReceived: 100, ProxyReceived: 150, ProxyRecvQ: 50}, req), "unread by toxiproxy")
	require.False(t, DropHolds(before, Snapshot{CassReceived: 150, ProxyReceived: 150}, req), "forwarded")
	require.False(t, DropHolds(before, Snapshot{CassReceived: 100, ProxyReceived: 120}, req), "not all written")
}

func TestLatency(t *testing.T) {
	w := newWorld()
	f := fault(t, w, chaos.FaultLat, 200)
	ierr, rerr := lifecycle(t, f, 2*time.Second)
	require.NoError(t, ierr)
	require.NoError(t, rerr)
	require.Equal(t, []string{"add soak-t3-down", "delete soak-t3-down"}, w.calls)
	closedAll(t, w)
}

func TestLatencyIgnoredFails(t *testing.T) {
	w := newWorld()
	w.ignore["latency"] = true
	f := fault(t, w, chaos.FaultLat, 200)
	err := f.Install(installCtx(t, 300*time.Millisecond))
	require.ErrorContains(t, err, "effect not observed")
	f.Release()
}

func TestBandwidthAcrossRounds(t *testing.T) {
	// The response arrives in real time, 100 ms per chunk, over several 300 ms rounds: one request serves them all.
	for _, rate := range []int{16, 128} {
		w := newWorld()
		f := fault(t, w, chaos.FaultBW, rate)
		ierr, rerr := lifecycle(t, f, 5*time.Second)
		require.NoError(t, ierr, "R = %d", rate)
		require.NoError(t, rerr)
		c := w.conn("soak-probe-t3-a")
		w.mu.Lock()
		require.Equal(t, []byte{proxy.OpStartup, proxy.OpQuery, proxy.OpQuery}, c.requests,
			"R = %d: the control transfer, then one throttled transfer kept across rounds", rate)
		w.mu.Unlock()
		closedAll(t, w)
	}
}

func TestBandwidthIgnoredFails(t *testing.T) {
	w := newWorld()
	w.ignore["bandwidth"] = true
	f := fault(t, w, chaos.FaultBW, 16)
	err := f.Install(installCtx(t, 300*time.Millisecond))
	require.ErrorContains(t, err, "effect not observed")
	f.Release()
}

func TestHalfOpen(t *testing.T) {
	w := newWorld()
	var mu sync.Mutex
	var effects []Round
	f := fault(t, w, chaos.FaultHalfOpen, 0)
	f.cfg.Event = func(name string, data any) {
		if r, ok := data.(Round); ok && name == "effect" {
			mu.Lock()
			effects = append(effects, r)
			mu.Unlock()
		}
	}
	require.NoError(t, f.Install(installCtx(t, 2*time.Second)))
	w.mu.Lock()
	require.Zero(t, w.conns["soak-probe-t3-a"].closed, "F-halfopen keeps its probe through active")
	w.mu.Unlock()
	require.NoError(t, f.Remove(installCtx(t, 2*time.Second)))
	require.Equal(t, []string{"add soak-t3-down", "update soak-t3-down 0", "delete soak-t3-down"}, w.calls)
	closedAll(t, w)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 0, effects[0].N, "the control round comes first")
	last := effects[len(effects)-1]
	require.Equal(t, "barrier", last.Stage, "the barrier is recorded")
	require.True(t, last.Held)
	require.Contains(t, last.Detail, "cassandra 127.0.1.2:9042<-", "with the sockets it read")
}

func TestHalfOpenResponseArrivingFails(t *testing.T) {
	// A downstream toxic that does not drop: every round's response arrives, so the effect never holds.
	w := newWorld()
	f := fault(t, w, chaos.FaultHalfOpen, 0)
	f.cfg.Ops = &downIgnoring{world: w}
	err := f.Install(installCtx(t, 500*time.Millisecond))
	require.ErrorContains(t, err, "effect not observed")
	f.Release()
}

func TestHalfOpenUnprocessedControlFails(t *testing.T) {
	w := newWorld()
	w.noProcess = true
	f := fault(t, w, chaos.FaultHalfOpen, 0)
	err := f.Install(installCtx(t, 500*time.Millisecond))
	require.ErrorContains(t, err, "control round")
	require.Empty(t, w.calls, "a failed control round adds no toxic")
	f.Release()
}

func TestHalfOpenNoProcessingAfterInstall(t *testing.T) {
	w := newWorld()
	f := fault(t, w, chaos.FaultHalfOpen, 0)
	ctx := installCtx(t, 600*time.Millisecond)
	require.NoError(t, f.open(ctx))
	require.NoError(t, f.control(ctx))
	w.mu.Lock()
	w.noProcess = true
	w.mu.Unlock()
	require.NoError(t, f.add(ctx, Toxics(chaos.FaultHalfOpen, 3, 0)[0]))
	err := f.rounds(ctx, "", f.probes[0], f.receiptRound)
	require.ErrorContains(t, err, "effect not observed", "received but never answered stays inconclusive")
	f.Release()
}

func TestBlack(t *testing.T) {
	w := newWorld()
	f := fault(t, w, chaos.FaultBlack, 0)
	ierr, rerr := lifecycle(t, f, 3*time.Second)
	require.NoError(t, ierr)
	require.NoError(t, rerr)
	require.Equal(t, []string{"add soak-t3-down", "add soak-t3-up", "update soak-t3-down 0", "update soak-t3-up 0",
		"delete soak-t3-down", "delete soak-t3-up"}, w.calls)
	closedAll(t, w)
}

func TestBlackUpstreamOnlyFailsDownstreamStage(t *testing.T) {
	w := newWorld()
	f := fault(t, w, chaos.FaultBlack, 0)
	f.cfg.Ops = &downIgnoring{world: w}
	err := f.Install(installCtx(t, 500*time.Millisecond))
	require.ErrorContains(t, err, "effect not observed")
	require.NotContains(t, w.calls, "add soak-t3-up", "the upstream toxic waits for the downstream stage")
	f.Release()
}

func TestBlackDownstreamOnlyFailsUpstreamStage(t *testing.T) {
	w := newWorld()
	f := fault(t, w, chaos.FaultBlack, 0)
	f.cfg.Ops = &upIgnoring{world: w}
	err := f.Install(installCtx(t, 2*time.Second))
	require.ErrorContains(t, err, "reached Cassandra under the upstream toxic")
	f.Release()
}

func TestBlackUnconsumedFailsBarrierAndStillDeletes(t *testing.T) {
	for name, set := range map[string]func(*world){
		"a held response": func(w *world) { w.unconsumedDown = true },
		"a held request":  func(w *world) { w.unconsumedUp = true },
	} {
		t.Run(name, func(t *testing.T) {
			w := newWorld()
			f := fault(t, w, chaos.FaultBlack, 0)
			require.NoError(t, f.Install(installCtx(t, 3*time.Second)))
			w.mu.Lock()
			set(w)
			w.mu.Unlock()
			err := f.Remove(installCtx(t, 2*time.Second))
			require.Error(t, err, "held traffic forwarded by the update is not a drop")
			require.Contains(t, w.calls, "delete soak-t3-down", "a failed proof still deletes the toxics (§64 BW04)")
			require.Contains(t, w.calls, "delete soak-t3-up")
			f.Release()
			closedAll(t, w)
		})
	}
}

func TestBarrierDeadLink(t *testing.T) {
	w := newWorld()
	f := fault(t, w, chaos.FaultHalfOpen, 0)
	require.NoError(t, f.Install(installCtx(t, 2*time.Second)))
	w.mu.Lock()
	w.noProcess = true // the barrier is never answered
	w.mu.Unlock()
	err := f.Remove(installCtx(t, time.Second))
	require.ErrorContains(t, err, "unanswered")
	require.Contains(t, w.calls, "delete soak-t3-down")
	f.Release()
}

func TestReset(t *testing.T) {
	w := newWorld()
	f := fault(t, w, chaos.FaultReset, 0)
	ierr, rerr := lifecycle(t, f, 2*time.Second)
	require.NoError(t, ierr)
	require.NoError(t, rerr)
	require.Equal(t, []string{"add soak-t3-up", "delete soak-t3-up"}, w.calls)
}

func TestResetRefusesAMalformedFrame(t *testing.T) {
	// A protocol error is not the proxy ending the connection (§64 BW01).
	w := newWorld()
	f := fault(t, w, chaos.FaultReset, 0)
	ctx := installCtx(t, time.Second)
	require.NoError(t, f.open(ctx))
	w.conn("soak-probe-t3-a").events <- proxy.FrameEvent{At: time.Now(), Err: fmt.Errorf("probe: %w: version byte 0x85", proxy.ErrMalformed)}
	_, err := f.resetRound(ctx, f.probes[0])
	require.ErrorIs(t, err, proxy.ErrMalformed)
	require.NotErrorIs(t, err, ErrNotHeld)
	f.Release()
}

func TestTerminated(t *testing.T) {
	require.True(t, terminated(io.EOF))
	require.True(t, terminated(fmt.Errorf("read: %w", io.ErrUnexpectedEOF)))
	require.False(t, terminated(fmt.Errorf("read: %w", net.ErrClosed)), "a local close is not the peer's")
	require.False(t, terminated(proxy.ErrMalformed))
	require.False(t, terminated(errors.New("boom")))
}

func TestFailedSecondAddIsUndone(t *testing.T) {
	w := newWorld()
	w.addErr = "soak-t3-up"
	f := fault(t, w, chaos.FaultBlack, 0)
	ierr, rerr := lifecycle(t, f, 3*time.Second)
	require.ErrorContains(t, ierr, "add soak-t3-up")
	require.NoError(t, rerr, "the undo accepts the 404 of the toxic never added")
	require.Equal(t, []string{"add soak-t3-down", "add soak-t3-up", "delete soak-t3-down", "delete soak-t3-up"}, w.calls,
		"no update and no barrier in an undo")
	closedAll(t, w)
}

func TestFailedControlAddsNothing(t *testing.T) {
	w := newWorld()
	f := fault(t, w, chaos.FaultBW, 16)
	// A control transfer already slower than the limit means the measurement cannot tell a working toxic
	// from the fixture: simulate it with a bandwidth toxic present before the fault.
	w.toxics["preexisting"] = proxy.Toxic{Name: "preexisting", Type: "bandwidth", Stream: "downstream",
		Attributes: map[string]any{"rate": 8}}
	ierr, rerr := lifecycle(t, f, 3*time.Second)
	require.ErrorContains(t, ierr, "control round")
	require.NoError(t, rerr)
	require.Equal(t, []string{"delete soak-t3-down"}, w.calls)
}

func TestMismatchedStreamFailsTheRound(t *testing.T) {
	w := newWorld()
	f := fault(t, w, chaos.FaultLat, 200)
	ctx := installCtx(t, time.Second)
	require.NoError(t, f.open(ctx))
	w.conn("soak-probe-t3-a").events <- proxy.FrameEvent{At: time.Now(), Stream: 99, Opcode: proxy.OpResult, Done: true}
	_, err := f.latencyRound(ctx, f.probes[0])
	require.ErrorContains(t, err, "frame for stream 99")
	require.NotErrorIs(t, err, ErrNotHeld, "a framing error ends the install, it is not retried")
	f.Release()
}

func TestWriteErrorFailsTheRound(t *testing.T) {
	w := newWorld()
	f := fault(t, w, chaos.FaultReset, 0)
	ctx := installCtx(t, time.Second)
	require.NoError(t, f.open(ctx))
	w.mu.Lock()
	w.conns["soak-probe-t3-a"].dead = true
	w.mu.Unlock()
	_, err := f.resetRound(ctx, f.probes[0])
	require.ErrorContains(t, err, "write")
	f.Release()
}

func TestReleaseWithRealProbes(t *testing.T) {
	// Real probe connections, each with its reader goroutine: Release closes them and returns once the readers end,
	// after a failed install.
	addr := cqlServer(t)
	w := newWorld()
	w.addErr = "soak-t3-down"
	f := fault(t, w, chaos.FaultLat, 200)
	var dialer net.Dialer
	f.cfg.Open = func(ctx context.Context, name string) (Conn, error) {
		return proxy.DialProbe(ctx, dialer.DialContext, addr, name)
	}
	err := f.Install(installCtx(t, 2*time.Second))
	require.ErrorContains(t, err, "add soak-t3-down")
	done := make(chan struct{})
	go func() {
		f.Release()
		f.Release()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Release did not return")
	}
	_, err = f.probes[0].conn.Request(t.Context(), proxy.OpOptions, nil)
	require.Error(t, err, "the released probe is closed")
	real, ok := f.probes[0].conn.(*proxy.Probe)
	require.True(t, ok)
	select {
	case <-real.Done():
	default:
		t.Fatal("Release returned before the probe's reader ended")
	}
}

func TestBlackHeldRequestReleasedOnlyByItsUpdate(t *testing.T) {
	// The upstream toxic's own update releases the held request, not the downstream one (§65 BX04).
	w := newWorld()
	w.unconsumedUp = true
	f := fault(t, w, chaos.FaultBlack, 0)
	require.NoError(t, f.Install(installCtx(t, 3*time.Second)))
	b := w.conn("soak-probe-t3-b")
	before := b.s.cassReceived
	var afterDown int64 = -1
	w.afterUpdate = func(name string) {
		if name == "soak-t3-down" {
			afterDown = b.s.cassReceived
		}
	}
	err := f.Remove(installCtx(t, 2*time.Second))
	require.Equal(t, before, afterDown, "nothing reached Cassandra on b before the upstream update")
	require.ErrorContains(t, err, "held stream", "the released request is answered before the barrier")
	f.Release()
}

func TestBlackHeldRequestForwardedSilently(t *testing.T) {
	// A held request forwarded at the update whose response is withheld: only the exact receipt check sees it.
	w := newWorld()
	w.silentUp = true
	f := fault(t, w, chaos.FaultBlack, 0)
	require.NoError(t, f.Install(installCtx(t, 3*time.Second)))
	err := f.Remove(installCtx(t, 2*time.Second))
	require.ErrorContains(t, err, "want the barrier's")
	f.Release()
}

func TestFragmentedBarrier(t *testing.T) {
	w := newWorld()
	w.fragment = true
	f := fault(t, w, chaos.FaultHalfOpen, 0)
	ierr, rerr := lifecycle(t, f, 2*time.Second)
	require.NoError(t, ierr)
	require.NoError(t, rerr, "a barrier answer in fragments is one frame")
}

func TestFailedUpdateStillDeletes(t *testing.T) {
	w := newWorld()
	f := fault(t, w, chaos.FaultHalfOpen, 0)
	require.NoError(t, f.Install(installCtx(t, 2*time.Second)))
	w.mu.Lock()
	w.updateErr = true
	w.mu.Unlock()
	err := f.Remove(installCtx(t, 2*time.Second))
	require.ErrorContains(t, err, "update refused")
	require.Contains(t, w.calls, "delete soak-t3-down")
	f.Release()
}

func TestListingResidueFailsTheRemoval(t *testing.T) {
	w := newWorld()
	w.residue = true
	f := fault(t, w, chaos.FaultLat, 200)
	ierr, rerr := lifecycle(t, f, 2*time.Second)
	require.NoError(t, ierr)
	require.ErrorContains(t, rerr, "still present")
}

func TestErrorResponsesAreNotEffects(t *testing.T) {
	cases := map[string]struct {
		kind  chaos.Kind
		param int
		set   func(*world)
		want  string
	}{
		"an ERROR to the bandwidth control":       {chaos.FaultBW, 16, func(w *world) { w.errorOp = proxy.OpQuery }, "control round"},
		"a short RESULT to the bandwidth control": {chaos.FaultBW, 16, func(w *world) { w.shortBlob = true }, "control round"},
		"an ERROR to the latency control":         {chaos.FaultLat, 200, func(w *world) { w.errorOp = proxy.OpOptions }, "control round"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			w := newWorld()
			tc.set(w)
			f := fault(t, w, tc.kind, tc.param)
			err := f.Install(installCtx(t, time.Second))
			require.ErrorContains(t, err, tc.want)
			require.Empty(t, w.calls, "a failed control round adds no toxic")
			f.Release()
		})
	}
}

func TestDelayedErrorIsNotLatency(t *testing.T) {
	// A delayed ERROR to OPTIONS is a fixture failure, not F-lat's effect (§65 BX02).
	w := newWorld()
	f := fault(t, w, chaos.FaultLat, 200)
	ctx := installCtx(t, time.Second)
	require.NoError(t, f.open(ctx))
	require.NoError(t, f.control(ctx))
	w.mu.Lock()
	w.errorOp, w.errorDelay = proxy.OpOptions, time.Second
	w.mu.Unlock()
	_, err := f.latencyRound(ctx, f.probes[0])
	require.ErrorContains(t, err, "opcode")
	require.NotErrorIs(t, err, ErrNotHeld)
	f.Release()
}

func fault(t *testing.T, w *world, kind chaos.Kind, param int) *Fault {
	t.Helper()
	f, err := New(Config{Kind: kind, Seq: 3, Proxy: "node2", Node: netip.MustParseAddr("127.0.1.2"), Param: param,
		Ops: w, Open: w.open, Sockets: w,
		Timing: Timing{Round: 300 * time.Millisecond, Silence: 100 * time.Millisecond, Poll: 5 * time.Millisecond}})
	require.NoError(t, err)
	t.Cleanup(w.stopAll)
	return f
}

func installCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	t.Cleanup(cancel)
	return ctx
}

// lifecycle installs a fault, then removes it, returning both errors.
func lifecycle(t *testing.T, f *Fault, d time.Duration) (installErr, removeErr error) {
	t.Helper()
	installErr = f.Install(installCtx(t, d))
	removeErr = f.Remove(installCtx(t, d))
	f.Release()
	return installErr, removeErr
}

func closedAll(t *testing.T, w *world) {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, c := range w.conns {
		require.Positive(t, c.closed, "probe %s closed", c.name)
	}
}

// cqlServer answers STARTUP with READY and OPTIONS with SUPPORTED on every connection, as a healthy proxy and node.
func cqlServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go serveCQL(c)
		}
	}()
	return ln.Addr().String()
}

func serveCQL(c net.Conn) {
	defer c.Close()
	head := make([]byte, frameHeader)
	for {
		if _, err := io.ReadFull(c, head); err != nil {
			return
		}
		body := make([]byte, int(head[5])<<24|int(head[6])<<16|int(head[7])<<8|int(head[8]))
		if _, err := io.ReadFull(c, body); err != nil {
			return
		}
		op := proxy.OpReady
		if head[4] == proxy.OpOptions {
			op = proxy.OpSupported
		}
		_, _ = c.Write([]byte{0x84, 0, head[2], head[3], op, 0, 0, 0, 0})
	}
}

// AddToxic records the toxic, or fails for addErr.
func (w *world) AddToxic(_ context.Context, _ string, t proxy.Toxic) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls = append(w.calls, "add "+t.Name)
	if t.Name == w.addErr {
		return errors.New("add refused")
	}
	w.toxics[t.Name] = t
	return nil
}

// UpdateToxic sets a toxic's toxicity; with unconsumed traffic, the update forwards what was held.
func (w *world) UpdateToxic(_ context.Context, _, name string, toxicity float64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls = append(w.calls, fmt.Sprintf("update %s %g", name, toxicity))
	if w.updateErr {
		return errors.New("update refused")
	}
	t, ok := w.toxics[name]
	if !ok {
		return fmt.Errorf("update: %w", proxy.ErrNotFound)
	}
	w.zero[name] = toxicity == 0
	// toxiproxy restarts only the updated toxic's stubs (link.go:194), so only its direction can release anything.
	for _, c := range w.conns {
		c.flushHeld(t.Stream)
	}
	if w.afterUpdate != nil {
		w.afterUpdate(name)
	}
	return nil
}

// RemoveToxic deletes a toxic; deleting a timeout toxic closes every link, as TimeoutToxic.Cleanup does.
func (w *world) RemoveToxic(_ context.Context, _, name string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls = append(w.calls, "delete "+name)
	t, ok := w.toxics[name]
	if !ok {
		return fmt.Errorf("delete: %w", proxy.ErrNotFound)
	}
	if !w.residue {
		delete(w.toxics, name)
	}
	if t.Type == "timeout" {
		for _, c := range w.conns {
			if !c.dead {
				c.dead = true
				c.events <- proxy.FrameEvent{At: time.Now(), Err: io.EOF}
			}
		}
	}
	return nil
}

// Toxics lists the toxics present.
func (w *world) Toxics(context.Context, string) ([]string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var names []string
	for n := range w.toxics {
		names = append(names, n)
	}
	slices.Sort(names)
	return names, nil
}

// Upstream answers clientstats for a probe.
func (w *world) Upstream(_ context.Context, name string) (proxy.ClientRow, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	c, ok := w.conns[name]
	if !ok {
		return proxy.ClientRow{}, proxy.ErrNoRow
	}
	return proxy.ClientRow{Address: c.up, DriverName: name}, nil
}

// Socket answers ss for a probe's two sockets.
func (w *world) Socket(_ context.Context, local, remote netip.AddrPort) (proxy.SocketInfo, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, c := range w.conns {
		switch {
		case local.Port() == proxy.NodePort && remote == c.up:
			return proxy.SocketInfo{Local: local, Remote: remote, BytesReceived: c.s.cassReceived, BytesAcked: c.s.cassAcked}, nil
		case local.Port() == proxy.ProxyPort && remote == c.local:
			return proxy.SocketInfo{Local: local, Remote: remote, BytesReceived: c.s.proxyReceived}, nil
		}
	}
	return proxy.SocketInfo{}, errors.New("no such socket")
}

// Close counts the probe's closes.
func (c *fakeConn) Close() {
	c.w.mu.Lock()
	defer c.w.mu.Unlock()
	c.closed++
}

// Events returns the probe's events.
func (c *fakeConn) Events() <-chan proxy.FrameEvent { return c.events }

// Local returns the probe's own endpoint.
func (c *fakeConn) Local() netip.AddrPort { return c.local }

// Name returns the probe's DRIVER_NAME.
func (c *fakeConn) Name() string { return c.name }

// Request sends a request through the world's toxics.
func (c *fakeConn) Request(ctx context.Context, op byte, body []byte) (int16, error) {
	c.w.mu.Lock()
	defer c.w.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if c.dead {
		return 0, errors.New("broken pipe")
	}
	c.next++
	s, n, now := c.next, frameHeader+len(body), time.Now()
	c.requests = append(c.requests, op)
	c.s.proxyReceived += int64(n)
	if _, ok := c.w.active("reset_peer", "upstream"); ok {
		c.dead = true
		c.events <- proxy.FrameEvent{At: now, Err: io.EOF}
		return s, nil
	}
	if _, ok := c.w.active("timeout", "upstream"); ok {
		c.heldReq = append(c.heldReq, heldReq{s, op, body})
		return s, nil
	}
	c.s.cassReceived += int64(n)
	if c.w.noProcess {
		return s, nil
	}
	c.respond(s, op, body, now)
	return s, nil
}

// AddToxic adds the downstream timeout as an ignored type.
func (d *downIgnoring) AddToxic(ctx context.Context, p string, t proxy.Toxic) error {
	if t.Type == "timeout" && t.Stream == "downstream" {
		t.Type = "ignored"
	}
	return d.world.AddToxic(ctx, p, t)
}

// AddToxic adds the upstream timeout as an ignored type.
func (u *upIgnoring) AddToxic(ctx context.Context, p string, t proxy.Toxic) error {
	if t.Type == "timeout" && t.Stream == "upstream" {
		t.Type = "ignored"
	}
	return u.world.AddToxic(ctx, p, t)
}

// active reports whether a toxic type on a stream is in effect; the caller holds the lock.
func (w *world) active(typ, stream string) (proxy.Toxic, bool) {
	for _, t := range w.toxics {
		if t.Type == typ && t.Stream == stream && !w.zero[t.Name] && !w.ignore[typ] {
			return t, true
		}
	}
	return proxy.Toxic{}, false
}

func (w *world) open(_ context.Context, name string) (Conn, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := len(w.conns)
	c := &fakeConn{w: w, name: name, events: make(chan proxy.FrameEvent, 4096), stop: make(chan struct{}),
		up: netip.MustParseAddrPort(fmt.Sprintf("127.0.0.1:%d", 50000+n)), local: netip.MustParseAddrPort(fmt.Sprintf("127.0.0.1:%d", 40000+n))}
	c.requests = append(c.requests, proxy.OpStartup)
	w.conns[name] = c
	return c, nil
}

func (w *world) conn(name string) *fakeConn {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.conns[name]
}

// stopAll ends the world's delivery goroutines.
func (w *world) stopAll() {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, c := range w.conns {
		select {
		case <-c.stop:
		default:
			close(c.stop)
		}
	}
}

// respond sends a response through the downstream toxics; the caller holds the world's lock.
func (c *fakeConn) respond(s int16, op byte, body []byte, now time.Time) {
	length := 2
	if op == proxy.OpQuery {
		length = 60
		if string(body) == string(proxy.QueryBody(stmtBlob, proxy.ConsistencyOne)) {
			length = 264134
		}
	}
	c.s.cassAcked += int64(frameHeader + length)
	if _, ok := c.w.active("timeout", "downstream"); ok {
		c.heldResp = append(c.heldResp, s)
		return
	}
	resp := proxy.OpResult
	if op == proxy.OpOptions {
		resp = proxy.OpSupported
	}
	switch {
	case op == c.w.errorOp:
		resp, length, now = proxy.OpError, 20, now.Add(c.w.errorDelay)
	case c.w.shortBlob && length == BlobSize:
		length = 1000
	}
	if t, ok := c.w.active("bandwidth", "downstream"); ok {
		rate, ok := t.Attributes["rate"].(int)
		if !ok {
			panic("bandwidth toxic without an int rate")
		}
		go c.trickle(s, resp, length, rate*100)
		return
	}
	at := now
	if t, ok := c.w.active("latency", "downstream"); ok {
		ms, ok := t.Attributes["latency"].(int)
		if !ok {
			panic("latency toxic without an int latency")
		}
		at = now.Add(time.Duration(ms) * time.Millisecond)
	}
	c.events <- proxy.FrameEvent{At: at, Stream: s, Opcode: resp, Length: length}
	if c.w.fragment {
		c.events <- proxy.FrameEvent{At: at, Stream: s, Opcode: resp, Length: length, Received: length / 2}
	}
	c.events <- proxy.FrameEvent{At: at.Add(time.Millisecond), Stream: s, Opcode: resp, Length: length, Received: length, Done: true}
}

// trickle delivers a response in real time, chunk bytes every 100 ms, as toxiproxy's bandwidth toxic does.
func (c *fakeConn) trickle(s int16, op byte, length, chunk int) {
	send := func(ev proxy.FrameEvent) bool {
		select {
		case c.events <- ev:
			return true
		case <-c.stop:
			return false
		}
	}
	if !send(proxy.FrameEvent{At: time.Now(), Stream: s, Opcode: op, Length: length}) {
		return
	}
	for got := 0; got < length; {
		select {
		case <-time.After(100 * time.Millisecond):
		case <-c.stop:
			return
		}
		got = min(got+chunk, length)
		if !send(proxy.FrameEvent{At: time.Now(), Stream: s, Opcode: op, Length: length, Received: got, Done: got == length}) {
			return
		}
	}
}

// flushHeld forwards the held traffic of one direction that the world marks unconsumed; the caller holds the lock.
func (c *fakeConn) flushHeld(stream string) {
	if stream == "upstream" && (c.w.unconsumedUp || c.w.silentUp) {
		for _, r := range c.heldReq {
			c.s.cassReceived += int64(frameHeader + len(r.body))
			if !c.w.silentUp {
				c.respond(r.stream, r.op, r.body, time.Now())
			}
		}
		c.heldReq = nil
	}
	if stream == "downstream" && c.w.unconsumedDown {
		for _, s := range c.heldResp {
			c.events <- proxy.FrameEvent{At: time.Now(), Stream: s, Opcode: proxy.OpResult, Length: 60, Received: 60, Done: true}
		}
		c.heldResp = nil
	}
}
