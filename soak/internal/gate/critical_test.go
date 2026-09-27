package gate

import (
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

// generous holds every threshold far above any test value, so a boundary test isolates one threshold.
func generous() Thresholds {
	th := Thresholds{}
	for _, k := range AllThresholds {
		th[k] = 1e12
	}
	return th
}

// with returns generous thresholds with one of them set.
func with(k string, v float64) Thresholds {
	th := generous()
	th[k] = v
	return th
}

// requireBoundary checks PLAN §41.1's definition: the gate passes with the threshold at the critical value
// and fails with it one float step below.
func requireBoundary(t *testing.T, c Critical, eval func(k float64) Status) {
	t.Helper()
	require.True(t, c.OK, "critical value must exist")
	require.Equal(t, StatusPass, eval(c.Value), "passes at the critical value %v", c.Value)
	require.Equal(t, StatusFail, eval(math.Nextafter(c.Value, math.Inf(-1))), "fails just below the critical value %v", c.Value)
}

func TestCriticalG2(t *testing.T) {
	in := G2Input{
		Groups: map[string]Series{
			"a": profileSeries(perHour(10, 3)),
			"b": profileSeries(perHour(4, 7)),
			"c": profileSeries(flat(2)),
		},
		QuietTotals: Series{{T: 900, V: 40}, {T: 1800, V: 47}, {T: 2700, V: 43}},
		Base:        5, Hosts: 3, NumConns: 2,
	}
	kG, kG0 := G2Critical(in)
	require.InDelta(t, 7, kG.Value, 1e-9)
	require.InDelta(t, 47-5-24, kG0.Value, 0)
	requireBoundary(t, kG, func(k float64) Status { return G2(in, with(KG, k)).Status })
	requireBoundary(t, kG0, func(k float64) Status { return G2(in, with(KG0, k)).Status })

	kG, kG0 = G2Critical(G2Input{Groups: map[string]Series{"a": {{T: 0, V: 1}}}})
	require.False(t, kG.OK, "one point has no slope")
	require.False(t, kG0.OK, "no quiet totals")
}

func TestCriticalG3(t *testing.T) {
	heap := profileSeries(perHour(100, 4))
	from, to := 0.0, 105*60.0
	kH, kHr := G3Critical(heap, from, to)
	require.InDelta(t, 4, kH.Value, 1e-9)
	require.Greater(t, kHr.Value, 0.0)
	requireBoundary(t, kH, func(k float64) Status { return G3(heap, from, to, with(KH, k)).Status })
	requireBoundary(t, kHr, func(k float64) Status { return G3(heap, from, to, with(KHr, k)).Status })

	// A falling heap gives a negative critical value; the gate passes there too.
	falling := profileSeries(perHour(100, -4))
	kH, kHr = G3Critical(falling, from, to)
	require.Less(t, kH.Value, 0.0)
	require.Less(t, kHr.Value, 0.0)
	requireBoundary(t, kHr, func(k float64) Status { return G3(falling, from, to, with(KHr, k)).Status })

	kH, kHr = G3Critical(nil, from, to)
	require.False(t, kH.OK)
	require.False(t, kHr.OK)
}

func TestCriticalG4(t *testing.T) {
	fds := Series{{T: 900, V: 30}, {T: 1800, V: 33}, {T: 2700, V: 31}, {T: 3600, V: 36}}
	kF, kFs := G4Critical(fds, 25)
	require.InDelta(t, 11, kF.Value, 0)
	requireBoundary(t, kF, func(k float64) Status { return G4(fds, 25, with(KF, k)).Status })
	requireBoundary(t, kFs, func(k float64) Status { return G4(fds, 25, with(KFs, k)).Status })
}

func TestCriticalG6(t *testing.T) {
	bal := map[string]Series{"primary": {{T: 900, V: 9}, {T: 1800, V: 3}, {T: 2700, V: 14}, {T: 3600, V: 20}}}
	kS, kSs := G6Critical(bal)
	require.InDelta(t, 20, kS.Value, 0)
	requireBoundary(t, kS, func(k float64) Status { return G6(bal, with(KS, k)).Status })
	requireBoundary(t, kSs, func(k float64) Status { return G6(bal, with(KSs, k)).Status })
}

func TestCriticalG7(t *testing.T) {
	logs := LogCounts{EventBufferFull: 4}
	kE := G7Critical(logs)
	require.InDelta(t, 4, kE.Value, 0)
	requireBoundary(t, kE, func(k float64) Status { return G7(logs, with(KE, k)).Status })
}

func TestCriticalG10(t *testing.T) {
	kU := G10Critical(6820)
	require.InDelta(t, 6820, kU.Value, 0)
	requireBoundary(t, kU, func(k float64) Status { return G10(nil, nil, nil, 6820, with(KLWTu, k)).Status })
}

func TestCriticalG12(t *testing.T) {
	in := G12Input{
		CloseElapsed: 1, CloseReturned: true,
		Baseline: map[string]int{"a": 1, "b": 4}, After: map[string]int{"a": 3, "c": 1},
		FD0: 15, FDs: 12,
	}
	kC, kF := G12Critical(in)
	require.InDelta(t, 4, kC.Value, 0, "group b went 4 → 0")
	require.InDelta(t, 3, kF.Value, 0)
	requireBoundary(t, kC, func(k float64) Status { return G12(in, with(KC, k)).Status })
	requireBoundary(t, kF, func(k float64) Status { return G12(in, with(KF, k)).Status })

	kC, kF = G12Critical(G12Input{FD0: -1, FDs: 3})
	require.False(t, kC.OK, "no goroutine dumps")
	require.False(t, kF.OK, "no fd0")
}

func TestCriticalG13(t *testing.T) {
	churn := func(i, grew int) ChurnResidue {
		return ChurnResidue{Slot: "C", Index: i, Measured: true,
			Before: map[string]int{"a": 10, "b": 2}, After: map[string]int{"a": 10 + grew, "b": 2}}
	}
	churns := []ChurnResidue{churn(0, 1), churn(1, 5), churn(2, 2), churn(3, 4), {Slot: "C", Index: 4}}
	kC, kCh := G13Critical(churns)
	require.InDelta(t, 5, kC.Value, 0)
	requireBoundary(t, kC, func(k float64) Status { return G13(churns[:4], nil, with(KC, k)).Status })
	requireBoundary(t, kCh, func(k float64) Status { return G13(churns[:4], nil, with(KCh, k)).Status })

	kC, kCh = G13Critical([]ChurnResidue{{Index: 0}})
	require.False(t, kC.OK, "an unmeasured churn gives nothing")
	require.False(t, kCh.OK)
}

func TestCriticalG14(t *testing.T) {
	warm := map[string]float64{"read": 0.002, "write": 0.004, "scan": 0.2}
	cool := map[string]float64{"read": 0.0021, "write": 0.0038, "scan": 0.25}
	classes := []string{"read", "write", "scan"}
	kL := G14Critical(warm, cool)
	require.InDelta(t, 0.25, kL.Value, 1e-12)
	requireBoundary(t, kL, func(k float64) Status { return G14(warm, cool, classes, false, with(KL, k)).Status })

	kL = G14Critical(map[string]float64{"read": 0.002}, map[string]float64{"read": 0.001})
	require.InDelta(t, -0.5, kL.Value, 1e-12, "a faster cool-down gives a negative critical value")
	require.False(t, G14Critical(nil, map[string]float64{"read": 1}).OK)
}

func TestCriticalG16(t *testing.T) {
	pid := map[string]int{"node1": 1, "node2": 2}
	tp := []TPStatsSample{
		{Start: 0, T: 1, PID: pid, Dropped: map[string]float64{"node1": 0, "node2": 10}},
		{Start: 60, T: 61, PID: pid, Dropped: map[string]float64{"node1": 7, "node2": 12}},
		{Start: 120, T: 121, PID: pid, Dropped: map[string]float64{"node1": 9000, "node2": 9000}}, // inside a window
	}
	gc := []GCStatsSample{
		{Start: 60, T: 61, Node: "node1", IntervalMs: 60000, MaxPauseMs: 40, TotalMs: 600, OK: true},
		{Start: 60, T: 61, Node: "node2", IntervalMs: 50000, MaxPauseMs: 25, TotalMs: 900, OK: true},
		{Start: 0, T: 1, Node: "node1", IntervalMs: 1000, MaxPauseMs: 9000, TotalMs: 900, OK: true, Prime: true},
		{Start: 130, T: 131, Node: "node2", IntervalMs: 5000, MaxPauseMs: 9000, TotalMs: 5000, OK: true}, // inside a window
	}
	windows := []Interval{{Start: 100, End: 140}}
	kD, kGp, kGt := G16Critical(tp, gc, windows)
	require.InDelta(t, 7, kD.Value, 0)
	require.InDelta(t, 40, kGp.Value, 0)
	require.InDelta(t, 900.0/50000, kGt.Value, 0)
	eval := func(name string) func(float64) Status {
		return func(k float64) Status { r, _ := G16(tp, gc, windows, with(name, k)); return r.Status }
	}
	requireBoundary(t, kD, eval(KD))
	requireBoundary(t, kGp, eval(KGp))
	requireBoundary(t, kGt, eval(KGt))

	kD, kGp, kGt = G16Critical(nil, nil, nil)
	require.False(t, kD.OK)
	require.False(t, kGp.OK)
	require.False(t, kGt.OK)
}

// A ratio with a zero denominator, or any non-finite value, is not a usable observation (Codex AK06):
// it is flagged, and never replaces or blocks a finite maximum.
func TestCriticalNonFinite(t *testing.T) {
	zeroEarly := Series{{T: 0, V: 0}, {T: 600, V: 0}, {T: 5400, V: 4}, {T: 6000, V: 4}}
	_, kHr := G3Critical(zeroEarly, 0, 6300)
	require.True(t, kHr.NonFinite, "late/0 − 1 is +Inf")
	require.False(t, kHr.OK)

	flat0 := Series{{T: 0, V: 0}, {T: 600, V: 0}, {T: 5400, V: 0}, {T: 6000, V: 0}}
	_, kHr = G3Critical(flat0, 0, 6300)
	require.True(t, kHr.NonFinite, "0/0 − 1 is NaN")

	var c Critical
	c.observe(math.NaN())
	c.observe(5)
	c.observe(math.Inf(1))
	c.observe(3)
	require.True(t, c.OK)
	require.True(t, c.NonFinite)
	require.InDelta(t, 5, c.Value, 0)

	require.True(t, MaxCritical(c, Critical{Value: 9, OK: true}).NonFinite, "MaxCritical keeps the flag")
}

// G2's and G4's bounds now compare value − base > k instead of value > base + k (Codex AK05).
// The two agree whenever base, values and k are integers, which is what the harness records and derives.
func TestBoundFormsAgreeOnIntegers(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for range 100000 {
		v, base, k := float64(rng.IntN(1<<20)), float64(rng.IntN(1<<20)), float64(rng.IntN(1<<21)-(1<<20))
		require.Equal(t, v > base+k, v-base > k, "v %v base %v k %v", v, base, k)
	}
}
