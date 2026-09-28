package canary

import (
	"context"
	"errors"
	"net/netip"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/view"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// fakeClock records every wait and lets the first n of them return at once; the next one cancels the run.
type fakeClock struct {
	mu      sync.Mutex
	epoch   time.Time
	now     time.Time
	waits   []time.Duration
	allowed int
	cancel  context.CancelFunc
}

func (c *fakeClock) sleepUntil(_ context.Context, t time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.waits = append(c.waits, t.Sub(c.epoch))
	if len(c.waits) > c.allowed {
		c.cancel()
		return false
	}
	c.now = t
	return true
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// runFor runs a canary until its (ticks+1)th wait, and returns its events and the waits it asked for.
func runFor(t *testing.T, id string, ticks int, d Deps) ([]Event, []time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	epoch := time.Unix(1_000_000, 0)
	clock := &fakeClock{epoch: epoch, now: epoch, allowed: ticks, cancel: cancel}
	var mu sync.Mutex
	var events []Event
	d.Epoch, d.SleepUntil, d.Now = epoch, clock.sleepUntil, clock.Now
	d.Event = func(e Event) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e)
	}
	wait := Start(ctx, id, d)
	wait()
	return events, clock.waits
}

func kinds(events []Event) []string {
	var out []string
	for _, e := range events {
		out = append(out, e.Kind)
	}
	return out
}

// The canary goroutine arms at its minute and not before: its first wait is for minute 12, and nothing happens until then.
func TestArmsAtItsMinuteAndInjectsEveryMinute(t *testing.T) {
	events, waits := runFor(t, "K4", 0, Deps{})
	require.Equal(t, []time.Duration{12 * time.Minute}, waits)
	require.Empty(t, events, "nothing before the arm minute")

	before := heldHeap()
	events, waits = runFor(t, "K4", 3, Deps{})
	require.Equal(t, []time.Duration{12 * time.Minute, 13 * time.Minute, 14 * time.Minute, 15 * time.Minute}, waits)
	require.Equal(t, []string{KindArm, KindInject, KindInject, KindInject}, kinds(events))
	require.Equal(t, 12*time.Minute, events[0].Time.Sub(time.Unix(1_000_000, 0)))
	require.Equal(t, 3*k4Bytes, heldHeap()-before)
}

func TestNoGoroutineWithoutAnInjection(t *testing.T) {
	for _, id := range []string{"", "K12", "K10", "K99"} {
		events, waits := runFor(t, id, 5, Deps{})
		require.Empty(t, events, id)
		require.Empty(t, waits, id)
	}
}

func TestK1LeaksAGoroutineEachMinute(t *testing.T) {
	leaks0, ok, err := probe.GoroutineLeaks(nil)
	require.NoError(t, err)
	if !ok {
		t.Skip("no goroutineleak profile in this toolchain")
	}
	_, total0, err := probe.GoroutineGroups()
	require.NoError(t, err)
	events, _ := runFor(t, "K1", 2, Deps{})
	require.Equal(t, []string{KindArm, KindInject, KindInject}, kinds(events))
	runtime.GC()
	leaks, _, err := probe.GoroutineLeaks(nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, leaks-leaks0, 2, "each K1 goroutine blocks on a channel only it references")
	_, total, err := probe.GoroutineGroups()
	require.NoError(t, err)
	require.GreaterOrEqual(t, total-total0, 2)
}

// K2 leaks a socket pair per minute: two fds and a blocked reader, reachable (no G1), and Control to R2.
func TestK2LeaksPairsThatAreControlSockets(t *testing.T) {
	fd0, err := probe.FDCount("/proc/self/fd")
	require.NoError(t, err)
	leaks0, hasLeaks, err := probe.GoroutineLeaks(nil)
	require.NoError(t, err)
	_, total0, err := probe.GoroutineGroups()
	require.NoError(t, err)

	events, _ := runFor(t, "K2", 2, Deps{})
	require.Equal(t, []string{KindArm, KindInject, KindInject}, kinds(events))
	for _, e := range events[1:] {
		require.NotContains(t, e.Detail, "failed")
	}
	// The accept loop may still be storing the second accepted end.
	require.Eventually(t, func() bool {
		fd, _ := probe.FDCount("/proc/self/fd")
		_, total, _ := probe.GoroutineGroups()
		return fd-fd0 >= 5 && total-total0 >= 3 // listener + 2 pairs; accept loop + 2 readers
	}, 5*time.Second, 10*time.Millisecond)
	if hasLeaks {
		runtime.GC()
		leaks, _, err := probe.GoroutineLeaks(nil)
		require.NoError(t, err)
		require.Equal(t, leaks0, leaks, "K2's goroutines are not leaks in goroutineleak's sense")
	}

	owned, err := view.OwnedSocketInodes("/proc/self/fd")
	require.NoError(t, err)
	socks, err := view.ReadSockets("/proc/net")
	require.NoError(t, err)
	rep := view.EvaluateR2(view.R2Input{Sockets: socks, Owned: owned, Registry: view.NewRegistry(), Primary: "primary",
		NodePort: 9042, ProxyPort: 19042, DriverAddrs: []netip.Addr{netip.MustParseAddr("127.0.1.1")}, NumConns: 2})
	require.Empty(t, rep.Unattributed)
	require.Empty(t, rep.Direct)
	control := 0
	for _, s := range rep.Control {
		if s.Remote.Addr() == netip.MustParseAddr("127.0.0.1") {
			control++
		}
	}
	require.GreaterOrEqual(t, control, 4, "both ends of both pairs")
}

func TestK3LeaksAnFDEachMinute(t *testing.T) {
	fd0, err := probe.FDCount("/proc/self/fd")
	require.NoError(t, err)
	events, _ := runFor(t, "K3", 3, Deps{})
	require.Equal(t, []string{KindArm, KindInject, KindInject, KindInject}, kinds(events))
	fd, err := probe.FDCount("/proc/self/fd")
	require.NoError(t, err)
	require.Equal(t, 3, fd-fd0)
}

// K7 records a terminal error through the primary Env's error path, and skips a minute inside a window.
func TestK7RecordsThroughTheEnvAndSkipsWindows(t *testing.T) {
	var recs []workload.ErrorRecord
	env := &workload.Env{SessionID: "primary", OpIDs: &atomic.Uint64{}, Errors: func(r workload.ErrorRecord) { recs = append(recs, r) }}
	env.OpIDs.Store(100)
	epoch := time.Unix(1_000_000, 0)
	inWindow := func(at time.Time) bool { return at.Sub(epoch) == 13*time.Minute }
	events, _ := runFor(t, "K7", 3, Deps{Env: env, InWindow: inWindow})
	require.Equal(t, []string{KindArm, KindInject, KindSkip, KindInject}, kinds(events))
	require.Len(t, recs, 2)
	for i, r := range recs {
		require.Equal(t, workload.ClassRead, r.Class)
		require.Equal(t, "primary", r.Session)
		require.EqualError(t, r.Err, "canary")
		require.Equal(t, uint64(101+i), r.OpID, "a fresh op id")
	}
	require.Equal(t, []uint64{101, 102}, []uint64{events[1].OpID, events[3].OpID})
	require.True(t, errors.Is(recs[0].Err, recs[0].Err))
}

// K8 poisons its pinned key once, at minute 12.
func TestK8PoisonsThePinnedKeyOnce(t *testing.T) {
	ledger := workload.NewLedger(0)
	env := &workload.Env{Ledger: ledger}
	key := PinnedKey()
	events, waits := runFor(t, "K8", 3, Deps{Env: env})
	require.Equal(t, []time.Duration{12 * time.Minute}, waits, "one-shot: no wait after the injection")
	require.Equal(t, []string{KindArm, KindInject}, kinds(events))
	require.Equal(t, key, *events[1].Key)
	require.Equal(t, int64(1), ledger.Acked(key))
	r := workload.PrimaryRange()
	require.True(t, key.P >= r.FirstP && key.P < r.FirstP+r.Partitions && key.C >= 0 && key.C < workload.ClusteringPerPartition,
		"a preloaded primary key")
	require.True(t, slices.ContainsFunc(workload.SampleKeys(nil, []workload.Range{workload.PrimaryRange()}, 0, &key),
		func(k workload.Key) bool { return k == key }))
}

// K6b arms the primary's stream-counter drop at minute 12, and not before (Codex AT01).
func TestK6bArmsTheDropAtMinute12(t *testing.T) {
	finish := func(c *probe.StreamCounters, n int) int64 {
		sc := c.StreamContext(context.Background())
		for range n {
			sc.StreamFinished(gocql.ObservedStream{})
		}
		_, f, _ := c.Snapshot()
		return f
	}
	early := &probe.StreamCounters{}
	events, _ := runFor(t, "K6b", 0, Deps{Streams: early})
	require.Empty(t, events)
	require.Equal(t, int64(1000), finish(early, 1000), "not armed before minute 12")

	c := &probe.StreamCounters{}
	events, waits := runFor(t, "K6b", 3, Deps{Streams: c})
	require.Equal(t, []time.Duration{12 * time.Minute}, waits, "armed once, nothing after")
	require.Equal(t, []string{KindArm, KindInject}, kinds(events))
	require.Equal(t, int64(998), finish(c, 1000))
}

// K15 and K17 record their activation at the workload epoch; their switches are set with the Env (Codex AT03).
func TestConfigurationCanariesRecordTheirArm(t *testing.T) {
	for _, id := range []string{"K15", "K17"} {
		events, waits := runFor(t, id, 3, Deps{})
		require.Equal(t, []time.Duration{0}, waits, id)
		require.Equal(t, []string{KindArm}, kinds(events), id)
		require.NotEmpty(t, events[0].Detail, id)
	}
}

// K12's activation is recorded before G0 runs, with the version G0 will expect (Codex AT03).
func TestK12ArmBeforeG0(t *testing.T) {
	at := time.Unix(5, 0)
	e, ok := ArmBeforeG0("K12", at)
	require.True(t, ok)
	require.Equal(t, Event{Canary: "K12", Kind: KindArm, Time: at, Detail: "G0 expects release_version 0.0.0-k12"}, e)
	for _, id := range []string{"", "K7", "K13"} {
		_, ok := ArmBeforeG0(id, at)
		require.False(t, ok, id)
	}
}

// K11 drops LWT offers from minute 20, and only them; before its arm, and for every other canary, nothing is dropped.
func TestK11DropsLWTOffersFromMinute20(t *testing.T) {
	for _, id := range []string{"", "K7", "K10", "K16"} {
		require.Nil(t, NewHooks(id).Drop(), "%q installs no drop", id)
	}
	h := NewHooks("K11")
	drop := h.Drop()
	require.NotNil(t, drop)
	events, waits := runFor(t, "K11", 0, Deps{Hooks: h})
	require.Equal(t, []time.Duration{20 * time.Minute}, waits)
	require.Empty(t, events)
	require.False(t, drop(workload.ClassLWT), "not before minute 20")

	events, waits = runFor(t, "K11", 3, Deps{Hooks: h})
	require.Equal(t, []time.Duration{20 * time.Minute}, waits, "armed once, nothing after")
	require.Equal(t, []string{KindArm}, kinds(events))
	require.True(t, drop(workload.ClassLWT))
	for _, s := range workload.DefaultMix() {
		if s.Class != workload.ClassLWT {
			require.False(t, drop(s.Class), s.Class)
		}
	}
}

// K13 arms once G0 has passed: a one-shot rewrite of the primary's next dial to the pinned node's proxy port,
// recorded as its injection with the rewritten destination and whether the dial succeeded (PLAN §44.2, §44.8).
func TestK13ArmAfterG0(t *testing.T) {
	node := netip.MustParseAddr("127.0.1.2")
	at := time.Unix(5, 0)
	for _, id := range []string{"", "K7", "K12", "K16"} {
		reg := view.NewRegistry()
		_, ok := ArmAfterG0(id, G0Deps{Registry: reg, Node: node, Now: at, Event: func(Event) { t.Fatal("no event") }})
		require.False(t, ok, id)
	}
	reg := view.NewRegistry()
	var events []Event
	e, ok := ArmAfterG0("K13", G0Deps{Registry: reg, Node: node, Now: at, Event: func(e Event) { events = append(events, e) }})
	require.True(t, ok)
	require.Equal(t, Event{Canary: "K13", Kind: KindArm, Time: at,
		Detail: "the primary's next dial to 127.0.1.2:19042 is rewritten to 127.0.1.2:9042"}, e)

	d := reg.Dialer("aux1", 0, 200*time.Millisecond, 0)
	c, _ := d.DialContext(t.Context(), "tcp", "127.0.1.2:19042")
	if c != nil {
		c.Close()
	}
	require.Empty(t, events, "an aux dial is not rewritten")
	d = reg.Dialer("primary", 0, 200*time.Millisecond, 0)
	_, err := d.DialContext(t.Context(), "tcp", "127.0.1.2:19042")
	require.ErrorContains(t, err, "rewritten")
	require.Len(t, events, 1)
	require.Equal(t, "K13", events[0].Canary)
	require.Equal(t, KindInject, events[0].Kind)
	require.Equal(t, "127.0.1.2:9042", events[0].Dest)
	require.NotNil(t, events[0].DialOK)
	require.Len(t, reg.DialsToPort("9042"), 1)
}

// K10's churner close hook leaves 50 goroutines per aux session, under one creator group, blocked on a package-level
// channel, so they are not leaks in goroutineleak's sense (PLAN §44.4); no other canary installs the hook.
func TestK10LeavesGoroutinesAfterEachAuxClose(t *testing.T) {
	for _, id := range []string{"", "K1", "K11", "K13"} {
		_, _, ok := AuxClose(id, time.Now, func(Event) { t.Fatal("no event") })
		require.False(t, ok, id)
	}
	at := time.Unix(7, 0)
	var events []Event
	hook, arm, ok := AuxClose("K10", func() time.Time { return at }, func(e Event) { events = append(events, e) })
	require.True(t, ok)
	require.Equal(t, Event{Canary: "K10", Kind: KindArm, Time: at, Detail: "every aux session leaves 50 goroutines after its Close"}, arm)

	leaks0, hasLeaks, err := probe.GoroutineLeaks(nil)
	require.NoError(t, err)
	groups0, _, err := probe.GoroutineGroups()
	require.NoError(t, err)
	hook("aux3")
	hook("aux4")
	groups, _, err := probe.GoroutineGroups()
	require.NoError(t, err)
	require.Equal(t, 2*k10Goroutines, groups[K10Group]-groups0[K10Group], "one creator group")
	require.Equal(t, []Event{
		{Canary: "K10", Kind: KindInject, Time: at, Detail: "aux3: 50 goroutines outlive its Close"},
		{Canary: "K10", Kind: KindInject, Time: at, Detail: "aux4: 50 goroutines outlive its Close"},
	}, events)
	if hasLeaks {
		leaks, _, err := probe.GoroutineLeaks(nil)
		require.NoError(t, err)
		require.Equal(t, leaks0, leaks, "K10's goroutines are not leaks in goroutineleak's sense")
	}
}
