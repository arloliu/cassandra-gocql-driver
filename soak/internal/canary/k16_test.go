package canary

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// warmLatency is a primary latency record whose warm-up holds 1000 operations of latency d per class of the mix,
// plus one slow operation after the warm-up, which no warm-up statistic may see.
func warmLatency(epoch time.Time, warmup, d time.Duration) *probe.Latency {
	l := probe.NewLatencySlice(epoch, 5*time.Second)
	for _, s := range workload.DefaultMix() {
		for i := range 1000 {
			l.Observe(string(s.Class), epoch.Add(time.Duration(i)*warmup/1000), d, false)
		}
		l.Observe(string(s.Class), epoch.Add(warmup+time.Second), time.Hour, false)
	}
	return l
}

func k16Params(l *probe.Latency, workers int) K16Params {
	return K16Params{Latency: l, Warmup: 10 * time.Minute, KL: 0.5, Rate: 1500, Workers: workers, Mix: workload.DefaultMix()}
}

// T_c, d_c, the occupancy estimate and the smallest n in [20, 50] that passes the capacity check (PLAN §44.6).
func TestK16Select(t *testing.T) {
	epoch := time.Unix(1_000_000, 0)
	l := warmLatency(epoch, 10*time.Minute, time.Millisecond)
	p99, _, ok := l.Quantile("write", 0, 600, 0.99)
	require.True(t, ok)
	upper := p99.Seconds() // every warm-up operation sits in the 1 ms bucket

	sel, err := Select(k16Params(l, 32))
	require.NoError(t, err)
	require.Equal(t, 20, sel.N, "ample capacity: the smallest n")
	require.Len(t, sel.Classes, len(workload.DefaultMix()))
	for _, s := range workload.DefaultMix() {
		c := sel.Classes[string(s.Class)]
		require.InDelta(t, upper, c.WarmupP99, 1e-12, s.Class)
		require.InDelta(t, upper*1.5, c.T, 1e-12, s.Class)
		require.InDelta(t, 2*c.T, c.D, 1e-12, s.Class)
		require.InDelta(t, 1500*float64(s.Weight)/100, c.Rate, 1e-9, s.Class)
	}
	// 1000 ops per class, each counted at its bucket's upper bound, over the 600 s warm-up.
	occupancy := float64(len(workload.DefaultMix())) * 1000 * upper / 600
	require.InDelta(t, occupancy, sel.Occupancy, 1e-9)
	// Σ rate_c × d_c = 1500 × 3 × upper; at n: that / n + occupancy.
	delayWork := 1500 * 3 * upper
	require.InDelta(t, delayWork/20+occupancy, sel.LHS, 1e-9)
	require.InDelta(t, 0.75*32, sel.RHS, 1e-12)

	// A W that n = 20 overloads: the smallest n whose check passes.
	// One-second operations make the steps between consecutive n larger than one worker.
	slow := warmLatency(epoch, 10*time.Minute, time.Second)
	p99, _, _ = slow.Quantile("write", 0, 600, 0.99)
	slowWork, slowOcc := 1500*3*p99.Seconds(), float64(len(workload.DefaultMix()))*1000*p99.Seconds()/600
	w := func(n int) float64 { return (slowWork/float64(n) + slowOcc) / 0.75 }
	workers := int(w(30)) + 1 // passes at 30 (and after), not at 29
	require.Less(t, float64(workers), w(29))
	sel, err = Select(k16Params(slow, workers))
	require.NoError(t, err)
	require.Equal(t, 30, sel.N)
	require.LessOrEqual(t, sel.LHS, sel.RHS)

	// No n in [20, 50] passes: N is 0 and the sides are n = 50's.
	sel, err = Select(k16Params(l, 0))
	require.NoError(t, err)
	require.Zero(t, sel.N)
	require.InDelta(t, delayWork/50+occupancy, sel.LHS, 1e-9)
	require.Greater(t, sel.LHS, sel.RHS)

	// A class with no warm-up p99 cannot be selected.
	empty := probe.NewLatencySlice(epoch, 5*time.Second)
	_, err = Select(k16Params(empty, 32))
	require.ErrorContains(t, err, "no warm-up p99")
}

// The selector is per class: the nth, 2nth, … Inject call of each class after the arm, and nothing before it.
func TestK16SelectorPerClass(t *testing.T) {
	for _, id := range []string{"", "K7", "K11"} {
		require.Nil(t, NewHooks(id).Inject(), "%q installs no Inject", id)
	}
	h := NewHooks("K16")
	inject := h.Inject()
	require.NotNil(t, inject)
	for i := range 100 {
		require.Zero(t, inject(workload.ClassWrite, uint64(i)), "not before the arm")
	}
	h.arm(Selection{N: 20, Classes: map[string]ClassDelay{"write": {D: 0.006}, "read": {D: 0.004}}})
	var writes, reads []int
	for i := 1; i <= 60; i++ {
		if d := inject(workload.ClassWrite, 0); d > 0 {
			require.Equal(t, 6*time.Millisecond, d)
			writes = append(writes, i)
		}
		if i <= 40 {
			if d := inject(workload.ClassRead, 0); d > 0 {
				require.Equal(t, 4*time.Millisecond, d)
				reads = append(reads, i)
			}
		}
		require.Zero(t, inject(workload.ClassScan, 0), "a class without a delay")
	}
	require.Equal(t, []int{20, 40, 60}, writes, "counted per class, from the arm")
	require.Equal(t, []int{20, 40}, reads)
}

// K16 arms at minute 20 from the warm-up histogram and records its selection; with no passing n it installs no delay
// and reports invalid-config (PLAN §44.5).
func TestK16ArmsAtMinute20(t *testing.T) {
	epoch := time.Unix(1_000_000, 0)
	l := warmLatency(epoch, 10*time.Minute, time.Millisecond)
	h := NewHooks("K16")
	events, waits := runFor(t, "K16", 3, Deps{Hooks: h, K16: k16Params(l, 32)})
	require.Equal(t, []time.Duration{20 * time.Minute}, waits, "armed once, nothing after")
	require.Equal(t, []string{KindArm, KindSelect}, kinds(events))
	require.NotNil(t, events[1].Selection)
	require.Equal(t, 20, events[1].Selection.N)
	require.Empty(t, h.InvalidConfig())
	inject := h.Inject()
	for range 19 {
		require.Zero(t, inject(workload.ClassLWT, 0))
	}
	require.Positive(t, inject(workload.ClassLWT, 0))

	h = NewHooks("K16")
	events, _ = runFor(t, "K16", 3, Deps{Hooks: h, K16: k16Params(l, 0)})
	require.Equal(t, []string{KindArm, KindSelect}, kinds(events))
	require.Zero(t, events[1].Selection.N)
	require.Len(t, h.InvalidConfig(), 1)
	require.Contains(t, h.InvalidConfig()[0], "no n in [20, 50]")
	inject = h.Inject()
	for range 100 {
		require.Zero(t, inject(workload.ClassLWT, 0), "no delay without a passing n")
	}

	// A missing threshold is an arm failure: nothing is selected.
	h = NewHooks("K16")
	p := k16Params(l, 32)
	p.KLErr = errors.New("gates.json has no kL")
	events, _ = runFor(t, "K16", 3, Deps{Hooks: h, K16: p})
	require.Equal(t, []string{KindArm}, kinds(events))
	require.Contains(t, events[0].Detail, "arm failed")
}

// The final-counts event holds each class's cool-down completions and delayed completions (PLAN §44.8).
func TestFinalCounts(t *testing.T) {
	epoch := time.Unix(1_000_000, 0)
	l := probe.NewLatencySlice(epoch, 5*time.Second)
	for i := range 100 {
		l.Observe("write", epoch.Add(2400*time.Second+time.Duration(i)*time.Second), time.Millisecond, i%20 == 0)
	}
	l.Observe("write", epoch.Add(100*time.Second), time.Millisecond, true)
	at := time.Unix(5, 0)
	_, ok := FinalCounts("K7", l, []string{"write"}, 2400*time.Second, 2700*time.Second, at)
	require.False(t, ok)
	e, ok := FinalCounts("K16", l, []string{"write", "read"}, 2400*time.Second, 2700*time.Second, at)
	require.True(t, ok)
	require.Equal(t, Event{Canary: "K16", Kind: KindFinalCounts, Time: at,
		Counts: map[string]ClassCounts{"write": {Total: 100, Delayed: 5}, "read": {}}}, e)
}
