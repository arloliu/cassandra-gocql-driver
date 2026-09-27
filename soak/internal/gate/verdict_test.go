package gate

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func passes(gates ...string) []Result {
	out := make([]Result, len(gates))
	for i, g := range gates {
		out[i] = Result{Gate: g, Status: StatusPass}
	}
	return out
}

func TestDecide(t *testing.T) {
	g16Pass := Result{Gate: "G16", Status: StatusPass}
	g16Fail := Result{Gate: "G16", Status: StatusFail, Details: []string{"dropped"}}
	green := passes("G0", "G1", "G2", "G3")
	withFail := append(passes("G0", "G1"), Result{Gate: "G3", Status: StatusFail})

	tests := []struct {
		name        string
		in          VerdictInput
		want        VerdictStatus
		failing     []string
		annotations []string
	}{
		{"all green", VerdictInput{Mode: ModeNight, Gates: green, G16: g16Pass, WorkloadSeconds: 7200}, VerdictPass, nil, nil},
		{"driver gate fails", VerdictInput{Mode: ModeNight, Gates: withFail, G16: g16Pass, WorkloadSeconds: 7200},
			VerdictFail, []string{"G3"}, nil},
		{"driver gate and G16 fail", VerdictInput{Mode: ModeNight, Gates: withFail, G16: g16Fail, WorkloadSeconds: 7200},
			VerdictFail, []string{"G3"}, []string{"fixture-suspect"}},
		{"G16 alone", VerdictInput{Mode: ModeNight, Gates: green, G16: g16Fail, WorkloadSeconds: 7200},
			VerdictFixtureInvalid, nil, nil},
		{"G0 fails", VerdictInput{Mode: ModeNight, Gates: append(passes("G1"), Result{Gate: "G0", Status: StatusFail}),
			G16: g16Pass, WorkloadSeconds: 7200}, VerdictFixtureInvalid, nil, nil},
		{"fixture failure beats a driver failure", VerdictInput{Mode: ModeNight, Gates: withFail, G16: g16Pass,
			WorkloadSeconds: 7200, FixtureInvalid: []string{"preload over budget"}}, VerdictFixtureInvalid, nil, nil},
		{"fixture abort ends the night early", VerdictInput{Mode: ModeNight, G16: g16Pass, WorkloadSeconds: 1800,
			FixtureInvalid: []string{"M3 F-stop remove overran"}}, VerdictFixtureInvalid, nil, nil},
		{"watchdog beats a fixture failure", VerdictInput{Mode: ModeNight, G16: g16Pass, WorkloadSeconds: 1800,
			FixtureInvalid: []string{"M3 F-stop remove overran"}, Incomplete: []string{"watchdog fired"}}, VerdictIncomplete, nil, nil},
		{"short night workload", VerdictInput{Mode: ModeNight, Gates: green, G16: g16Pass, WorkloadSeconds: 7199},
			VerdictIncomplete, nil, nil},
		{"validation mode ignores the 7200 s rule", VerdictInput{Mode: ModeValidate, Gates: green, G16: g16Pass,
			WorkloadSeconds: 2880}, VerdictPass, nil, nil},
		{"watchdog", VerdictInput{Mode: ModeNight, Gates: withFail, G16: g16Pass, WorkloadSeconds: 7200,
			Incomplete: []string{"watchdog fired"}}, VerdictIncomplete, nil, nil},
		{"missing threshold beats everything", VerdictInput{Mode: ModeNight,
			Gates: append(withFail, Result{Gate: "G4", Status: StatusInvalidConfig}), G16: g16Fail,
			WorkloadSeconds: 10, Incomplete: []string{"watchdog fired"}}, VerdictInvalidConfig, nil, nil},
		{"invalid G16 threshold", VerdictInput{Mode: ModeNight, Gates: green,
			G16: Result{Gate: "G16", Status: StatusInvalidConfig}, WorkloadSeconds: 7200}, VerdictInvalidConfig, nil, nil},
		{"config rejected before the run", VerdictInput{Mode: ModeNight, InvalidConfig: []string{"slot overlap"}},
			VerdictInvalidConfig, nil, nil},
		{"not evaluated is not a failure", VerdictInput{Mode: ModeNight,
			Gates: append(passes("G0"), Result{Gate: "G14", Status: StatusNotEvaluated}), G16: g16Pass,
			WorkloadSeconds: 7200}, VerdictPass, nil, nil},
		{"no gates cannot pass", VerdictInput{Mode: ModeNight, G16: g16Pass, WorkloadSeconds: 7200},
			VerdictIncomplete, nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := Decide(tt.in)
			require.Equal(t, tt.want, v.Status)
			require.Equal(t, tt.failing, v.FailingGates)
			require.Equal(t, tt.annotations, v.Annotations)
		})
	}
}

func TestNightVerdict(t *testing.T) {
	pass := Verdict{Status: VerdictPass}
	require.Equal(t, VerdictPass, NightVerdict([]Verdict{pass, pass, pass, pass}))
	require.Equal(t, VerdictFail, NightVerdict([]Verdict{pass, {Status: VerdictFail}, pass, pass}))
	require.Equal(t, VerdictFail, NightVerdict([]Verdict{pass, {Status: VerdictIncomplete}, pass, pass}))
	require.Equal(t, VerdictFail, NightVerdict([]Verdict{pass, pass, pass}), "fewer than four cells")
}

func TestG16(t *testing.T) {
	th := fullThresholds() // kD 100, kGp 500 ms, kGt 0.05
	pid := func(p int) map[string]int { return map[string]int{"node1": p} }
	dropped := func(v float64) map[string]float64 { return map[string]float64{"node1": v} }

	t.Run("steady and quiet", func(t *testing.T) {
		tp := []TPStatsSample{{T: 0, PID: pid(1), Dropped: dropped(0)}, {T: 60, PID: pid(1), Dropped: dropped(50)}}
		gc := []GCStatsSample{{T: 60, Node: "node1", IntervalMs: 60000, MaxPauseMs: 100, TotalMs: 600, OK: true}}
		r, missing := G16(tp, gc, nil, th)
		require.Equal(t, StatusPass, r.Status)
		require.Zero(t, missing)
	})
	t.Run("dropped messages over kD", func(t *testing.T) {
		tp := []TPStatsSample{{T: 0, PID: pid(1), Dropped: dropped(0)}, {T: 60, PID: pid(1), Dropped: dropped(101)}}
		r, _ := G16(tp, nil, nil, th)
		require.Equal(t, StatusFail, r.Status)
	})
	t.Run("restart is detected by pid, never by a negative delta", func(t *testing.T) {
		tp := []TPStatsSample{
			{T: 0, PID: pid(1), Dropped: dropped(5000)},
			{T: 60, PID: pid(2), Dropped: dropped(3)}, // restarted: not a -4997 delta, not a violation
			{T: 120, PID: pid(2), Dropped: dropped(20)},
		}
		r, missing := G16(tp, nil, nil, th)
		require.Equal(t, StatusPass, r.Status)
		require.Equal(t, 1, missing)
	})
	t.Run("negative delta without a restart is missing, not zero", func(t *testing.T) {
		tp := []TPStatsSample{{T: 0, PID: pid(1), Dropped: dropped(50)}, {T: 60, PID: pid(1), Dropped: dropped(10)}}
		r, missing := G16(tp, nil, nil, th)
		require.Equal(t, StatusPass, r.Status)
		require.Equal(t, 1, missing)
	})
	t.Run("interval overlapping a window is not evaluated", func(t *testing.T) {
		tp := []TPStatsSample{{T: 0, PID: pid(1), Dropped: dropped(0)}, {T: 60, PID: pid(1), Dropped: dropped(10000)}}
		gc := []GCStatsSample{{T: 60, Node: "node1", IntervalMs: 60000, MaxPauseMs: 9000, TotalMs: 30000, OK: true}}
		r, _ := G16(tp, gc, []Interval{{Start: 30, End: 40}}, th)
		require.Equal(t, StatusPass, r.Status)
	})
	t.Run("gc pause and gc share", func(t *testing.T) {
		gc := []GCStatsSample{{T: 60, Node: "node1", IntervalMs: 60000, MaxPauseMs: 501, TotalMs: 100, OK: true}}
		r, _ := G16(nil, gc, nil, th)
		require.Equal(t, StatusFail, r.Status)
		gc = []GCStatsSample{{T: 60, Node: "node1", IntervalMs: 60000, MaxPauseMs: 10, TotalMs: 3001, OK: true}}
		r, _ = G16(nil, gc, nil, th)
		require.Equal(t, StatusFail, r.Status)
	})
	t.Run("unparsable or non-finite gc sample is missing", func(t *testing.T) {
		gc := []GCStatsSample{
			{T: 60, Node: "node1", OK: false},
			{T: 120, Node: "node1", IntervalMs: 60000, MaxPauseMs: math.NaN(), TotalMs: 1, OK: true},
			{T: 180, Node: "node1", IntervalMs: 0, MaxPauseMs: 1, TotalMs: 1, OK: true},
		}
		r, missing := G16(nil, gc, nil, th)
		require.Equal(t, StatusPass, r.Status)
		require.Equal(t, 3, missing)
	})
	t.Run("missing threshold", func(t *testing.T) {
		r, _ := G16(nil, nil, nil, Thresholds{})
		require.Equal(t, StatusInvalidConfig, r.Status)
	})
}

// A delta covers [previous round start, this round end], and a gcstats interval ends when its command ends:
// a window touching either end excludes the reading (Codex I07).
func TestG16ConservativeIntervals(t *testing.T) {
	th := fullThresholds()
	pid := map[string]int{"node1": 1}
	tp := []TPStatsSample{
		{Start: 0, T: 10, PID: pid, Dropped: map[string]float64{"node1": 0}},
		{Start: 60, T: 70, PID: pid, Dropped: map[string]float64{"node1": 5000}},
	}
	r, _ := G16(tp, nil, []Interval{{Start: 5, End: 8}}, th)
	require.Equal(t, StatusPass, r.Status, "a window during the earlier round's commands excludes the delta")
	r, _ = G16(tp, nil, nil, th)
	require.Equal(t, StatusFail, r.Status)

	gc := []GCStatsSample{{Start: 55, T: 70, Node: "node1", IntervalMs: 10000, MaxPauseMs: 9000, TotalMs: 9000, OK: true}}
	r, _ = G16(nil, gc, []Interval{{Start: 42, End: 46}}, th)
	require.Equal(t, StatusPass, r.Status, "the interval starts IntervalMs before the command started")
	r, _ = G16(nil, gc, []Interval{{Start: 30, End: 40}}, th)
	require.Equal(t, StatusFail, r.Status)
}

// A nil or empty reading map on either side of a pair counts the other side's nodes as missing, without panicking (Codex M03).
func TestG16NilDroppedMaps(t *testing.T) {
	full := TPStatsSample{Start: 0, T: 1, PID: map[string]int{"node1": 1}, Dropped: map[string]float64{"node1": 0}}
	for _, empty := range []map[string]float64{nil, {}} {
		hole := TPStatsSample{Start: 60, T: 61, PID: map[string]int{}, Dropped: empty}
		for _, th := range []Thresholds{fullThresholds(), {}} {
			_, missing := G16([]TPStatsSample{full, hole}, nil, nil, th)
			require.Equal(t, 1, missing)
			hole.Start, hole.T = -60, -59
			_, missing = G16([]TPStatsSample{hole, full}, nil, nil, th)
			require.Equal(t, 1, missing)
			hole.Start, hole.T = 60, 61
		}
	}
}
