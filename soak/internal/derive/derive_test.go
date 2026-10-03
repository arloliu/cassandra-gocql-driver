package derive

import (
	"cmp"
	"maps"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cellrun"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/config"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
)

const fixture = "../../testdata/calibration/c50p5"

var cellIDs = []string{"c41p4", "c41p5", "c50p4", "c50p5"}

func crit(v float64) gate.Critical { return gate.Critical{Value: v, OK: true} }

// observations gives every cell the same observation for every threshold, then applies set.
func observations(base Observation, set func(id string, o map[string]Observation)) map[string]map[string]Observation {
	out := map[string]map[string]Observation{}
	for _, id := range cellIDs {
		o := map[string]Observation{}
		for _, k := range gate.AllThresholds {
			o[k] = base
		}
		if set != nil {
			set(id, o)
		}
		out[id] = o
	}
	return out
}

func derivedOf(t *testing.T, ds []Derived, k string) Derived {
	t.Helper()
	i := slices.IndexFunc(ds, func(d Derived) bool { return d.Name == k })
	require.GreaterOrEqual(t, i, 0, k)
	return ds[i]
}

func TestCombineMaxRule(t *testing.T) {
	obs := observations(Observation{Full: crit(10), Windows: []float64{1}}, func(id string, o map[string]Observation) {
		if id == "c50p4" {
			o[gate.KS] = Observation{Full: crit(20)}
			o[gate.KHr] = Observation{Full: crit(0.5), Windows: []float64{0.25, 30}}
		}
	})
	th, ds, problems := Combine(cellIDs, obs, nil)
	require.Empty(t, problems)
	require.InDelta(t, 40, th[gate.KS], 0, "2 × the largest cell")
	require.InDelta(t, 20, th[gate.KE], 0)
	require.InDelta(t, 60, th[gate.KHr], 0, "kHr's windows count, not only the whole trend window")
	require.Equal(t, "max", derivedOf(t, ds, gate.KS).Rule)
}

func TestCombineSlopeRule(t *testing.T) {
	// 20 windows per cell valued 0..19, so the pooled p95 of 80 values is the 76th: 18.
	var ws []float64
	for i := range 20 {
		ws = append(ws, float64(i))
	}
	obs := observations(Observation{Full: crit(1), Windows: ws}, nil)
	th, ds, problems := Combine(cellIDs, obs, nil)
	require.Empty(t, problems)
	require.InDelta(t, 36, th[gate.KG], 0)
	d := derivedOf(t, ds, gate.KG)
	require.Equal(t, "slope", d.Rule)
	require.InDelta(t, 18, *d.P95, 0)

	// The whole-window slope is a backstop above the p95.
	obs = observations(Observation{Full: crit(1), Windows: ws}, func(id string, o map[string]Observation) {
		if id == "c41p5" {
			o[gate.KH] = Observation{Full: crit(25), Windows: ws}
		}
	})
	th, _, _ = Combine(cellIDs, obs, nil)
	require.InDelta(t, 50, th[gate.KH], 0)
}

func TestCombineFloorsAndNegatives(t *testing.T) {
	obs := observations(Observation{Full: crit(-3), Windows: []float64{-1, -2}}, nil)
	th, ds, problems := Combine(cellIDs, obs, map[string]string{
		gate.KG0: "r", gate.KHr: "r", gate.KF: "r", gate.KS: "r", gate.KC: "r", gate.KD: "r",
	})
	require.Empty(t, problems)
	require.InDelta(t, 0.48674869540553, th[gate.KL], 0, "PLAN v7.16 §55.2")
	require.InDelta(t, 3600.0/2100, th[gate.KG], 1e-12)
	require.InDelta(t, 3600.0/2100, th[gate.KH], 1e-12)
	require.InDelta(t, 3600.0/2100, th[gate.KSs], 1e-12)
	require.InDelta(t, 6, th[gate.KFs], 0, "PLAN v7.14 §48.2")
	require.InDelta(t, 0.5, th[gate.KCh], 0)
	require.InDelta(t, 0, th[gate.KE], 0, "kE is exempt from degeneracy")
	require.True(t, derivedOf(t, ds, gate.KL).FloorApplied)
	require.True(t, derivedOf(t, ds, gate.KS).Degenerate)
	require.False(t, derivedOf(t, ds, gate.KE).Degenerate)

	// A larger observation lifts kL over its floor; one whose double is the floor leaves the floor.
	obs = observations(Observation{Full: crit(0.3)}, nil)
	th, ds, _ = Combine(cellIDs, obs, nil)
	require.InDelta(t, 0.6, th[gate.KL], 1e-15)
	require.False(t, derivedOf(t, ds, gate.KL).FloorApplied)
	obs = observations(Observation{Full: crit(0.243374347702765)}, nil)
	th, ds, _ = Combine(cellIDs, obs, nil)
	require.InDelta(t, Floors[gate.KL], th[gate.KL], 1e-15)
	require.False(t, derivedOf(t, ds, gate.KL).FloorApplied, "equal is not below")
}

// kFs's floor holds a one-fd step in the quiet series of batch 1's validation timings, and no more (PLAN v7.14 §48.2).
func TestFloorKFsContract(t *testing.T) {
	th := gate.Thresholds{gate.KF: 20, gate.KFs: Floors[gate.KFs]}
	pass := map[string]gate.Series{
		"three points (K1)":           {{T: 1223, V: 22}, {T: 1732, V: 22}, {T: 2400, V: 23}},
		"three points (K4)":           {{T: 1178, V: 22}, {T: 1748, V: 22}, {T: 2400, V: 23}},
		"a step at the middle point":  {{T: 1152, V: 22}, {T: 1744, V: 23}, {T: 2400, V: 23}},
		"four points with 2700 s":     {{T: 1162, V: 22}, {T: 1736, V: 22}, {T: 2400, V: 23}, {T: 2700, V: 23}},
		"four points, step at 2700 s": {{T: 1162, V: 22}, {T: 1736, V: 22}, {T: 2400, V: 22}, {T: 2700, V: 23}},
	}
	for name, s := range pass {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, gate.StatusPass, gate.G4(s, 22, th).Status)
		})
	}
	short := gate.Series{{T: 1225, V: 22}, {T: 1711, V: 23}}
	require.Equal(t, gate.StatusFail, gate.G4(short, 22, th).Status, "a two-point series shorter than 600 s is the stated limit")
}

func TestCombineDegenerateNeedsAcceptance(t *testing.T) {
	obs := observations(Observation{Full: crit(0), Windows: []float64{0}}, nil)
	_, _, problems := Combine(cellIDs, obs, nil)
	for _, k := range degenerateAtZero {
		require.True(t, slices.ContainsFunc(problems, func(p string) bool { return strings.HasPrefix(p, k+": degenerate") }), k)
	}
	require.Len(t, problems, len(degenerateAtZero))

	accept := map[string]string{}
	for _, k := range degenerateAtZero {
		accept[k] = "accepted"
	}
	_, ds, problems := Combine(cellIDs, obs, accept)
	require.Empty(t, problems)
	require.Equal(t, "accepted", derivedOf(t, ds, gate.KD).AcceptedZero)

	accept[gate.KE] = "not degenerate"
	_, _, problems = Combine(cellIDs, obs, accept)
	require.Len(t, problems, 1, "-accept-zero may only name a degenerate-capable threshold")
}

func TestCombineRefusesNonFinite(t *testing.T) {
	obs := observations(Observation{Full: crit(1), Windows: []float64{1}}, func(id string, o map[string]Observation) {
		if id == "c50p4" {
			o[gate.KHr] = Observation{Full: gate.Critical{Value: 1, OK: true, NonFinite: true}, Windows: []float64{1}}
			o[gate.KH] = Observation{Full: crit(1), Windows: []float64{1}, NonFinite: 1}
		}
	})
	_, _, problems := Combine(cellIDs, obs, nil)
	require.Equal(t, []string{"kH: 1 non-finite window values on c50p4", "kHr: a non-finite observation on c50p4"}, problems)
}

func TestCombineMissingObservations(t *testing.T) {
	obs := observations(Observation{Full: crit(1), Windows: []float64{1}}, func(id string, o map[string]Observation) {
		if id == "c41p4" {
			o[gate.KF] = Observation{}
			o[gate.KL] = Observation{Excluded: true}
		}
	})
	_, ds, problems := Combine(cellIDs, obs, nil)
	require.Equal(t, []string{"kF: no observation on c41p4"}, problems, "an excluded kL cell is not missing")
	require.Equal(t, []string{"c41p4"}, derivedOf(t, ds, gate.KL).Excluded)

	obs = observations(Observation{Full: crit(1)}, nil)
	_, _, problems = Combine(cellIDs, obs, nil)
	for _, k := range slopeThresholds {
		require.Contains(t, problems, k+": every window was skipped")
	}
	obs = observations(Observation{Excluded: true}, nil)
	_, _, problems = Combine(cellIDs, obs, map[string]string{})
	require.Contains(t, problems, "kL: no cell contributes")
}

// syntheticCell is a night cell with a profile every 300 s: heap rising 6 MiB/h, one goroutine group rising 2/h,
// fds and balance flat, quiet every other profile, and a latency histogram.
func syntheticCell(t *testing.T, id string) Cell {
	t.Helper()
	col := cellrun.Collected{
		Mode:            gate.ModeNight,
		Timeline:        config.Timeline{Warmup: 15 * time.Minute, Cooldown: 105 * time.Minute, Workload: 120 * time.Minute},
		WorkloadSeconds: 7200, Ran: true,
		Classes: []string{"read"},
	}
	for i := 0; i <= 24; i++ {
		ts := float64(i * 300)
		col.Profiles = append(col.Profiles, cellrun.Profile{
			Kind: "profile", T: ts, Reason: cellrun.ReasonPeriodic, Quiet: i%2 == 0,
			Groups: map[string]int{"a": 10 + int(2*ts/3600), "b": 3}, Total: 13, HeapMiB: 50 + 6*ts/3600,
			FDs: 20, Balance: map[string]int64{"primary": 1},
		})
	}
	for i := range 7 {
		col.Churns = append(col.Churns, gate.ChurnResidue{Index: i, Measured: true, Before: map[string]int{"a": 5}, After: map[string]int{"a": 5 + i%2}})
	}
	lat := probe.NewLiveLatency(time.Unix(0, 0), 5*time.Second)
	for s := 0; s < 7200; s += 5 {
		d := 2 * time.Millisecond
		switch {
		case s >= 600 && s < 900:
			d = 2500 * time.Microsecond // the night's warm-up is slower after the validation warm-up's 10 min
		case s >= 6600 && s < 6900:
			d = 3 * time.Millisecond // one slow cool-down window
		}
		lat.Observe("read", time.Unix(int64(s), 0), d, false)
	}
	lat = rebuilt(lat)
	warm, _, _ := lat.Quantile("read", probe.Window{From: 0, To: 900}, 0.99)
	cool, _, _ := lat.Quantile("read", probe.Window{From: 6300, To: 7200}, 0.99)
	col.WarmupP99, col.CooldownP99 = map[string]float64{"read": warm.Seconds()}, map[string]float64{"read": cool.Seconds()}
	return Cell{ID: id, Cal: cellrun.Calibration{Collected: col, Latency: lat}}
}

func TestObserveWindows(t *testing.T) {
	c := syntheticCell(t, "c50p5")
	obs, problems := Observe(c)
	require.Empty(t, problems)

	// The trend window [900, 7200] holds 15 windows of 2100 s stepping by 300 s.
	require.Len(t, obs[gate.KH].Windows, 15)
	for _, w := range obs[gate.KH].Windows {
		require.InDelta(t, 6, w, 1e-9)
	}
	require.Len(t, obs[gate.KG].Windows, 15)
	require.InDelta(t, 2, slices.Max(obs[gate.KG].Windows), 0.5, "group a's slope, the max over groups")
	require.Len(t, obs[gate.KHr].Windows, 15)
	// Quiet every other profile: a 2100 s window holds 3 or 4 quiet points, never fewer.
	require.Len(t, obs[gate.KFs].Windows, 15)
	require.Zero(t, obs[gate.KFs].Skipped)
	require.Len(t, obs[gate.KSs].Windows, 15)

	// Seven churns give five runs of three.
	require.Len(t, obs[gate.KCh].Windows, 5)

	// kL: three 5-min cool-down windows against the 10-min warm-up, plus the night's own comparison.
	require.Len(t, obs[gate.KL].Windows, 4)
	require.InDelta(t, 0.5, slices.Max(obs[gate.KL].Windows), 0.03, "the slow window, 3 ms, against the 10-min warm-up, 2 ms")
	require.InDelta(t, 0.2, obs[gate.KL].Full.Value, 0.03, "the night's own comparison: 3 ms against 2.5 ms")

	// Each comparison keeps its class and ranges for attribution (PLAN v7.16 §55.2).
	src := obs[gate.KL].Sources
	require.Len(t, src, 4)
	top := slices.MaxFunc(src, func(a, b KLSource) int { return cmp.Compare(a.Value, b.Value) })
	require.Equal(t, KLSource{Kind: "night", Cell: "c50p5", Class: "read", Warm: &probe.Window{From: 0, To: 600}, Cool: &probe.Window{From: 6600, To: 6900}, Value: top.Value}, top)
	require.Equal(t, probe.Window{From: 0, To: 900}, *src[3].Warm, "the night's own comparison")
	require.InDelta(t, obs[gate.KL].Full.Value, src[3].Value, 1e-15)
}

func TestObserveSkipsSparseWindows(t *testing.T) {
	c := syntheticCell(t, "c50p5")
	for i := range c.Cal.Collected.Profiles {
		c.Cal.Collected.Profiles[i].Quiet = i%5 == 0 // quiet every 1500 s: a 2100 s window holds 1 or 2
	}
	obs, _ := Observe(c)
	require.Empty(t, obs[gate.KFs].Windows)
	require.Equal(t, 15, obs[gate.KFs].Skipped)
	require.Len(t, obs[gate.KH].Windows, 15, "heap uses every trend profile")
}

func TestObserveLatencyProblems(t *testing.T) {
	c := syntheticCell(t, "c50p5")
	c.Cal.Collected.Classes = []string{"read", "write"}
	_, problems := Observe(c)
	require.True(t, slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, "class write has no p99") }))

	for _, recorded := range []func(c *cellrun.Collected) map[string]float64{
		func(c *cellrun.Collected) map[string]float64 { return c.WarmupP99 },
		func(c *cellrun.Collected) map[string]float64 { return c.CooldownP99 },
	} {
		c = syntheticCell(t, "c50p5")
		recorded(&c.Cal.Collected)["read"] *= 1.02
		_, problems = Observe(c)
		require.Len(t, problems, 1)
		require.Contains(t, problems[0], "differs from the recorded")
	}

	c = syntheticCell(t, "c50p5")
	c.Cal.Collected.Windows = []gate.Interval{{Start: 6500, End: 6700}}
	obs, problems := Observe(c)
	require.Empty(t, problems)
	require.True(t, obs[gate.KL].Excluded, "a contaminated cool-down contributes nothing to kL")
	require.False(t, obs[gate.KL].Full.OK)

	// The cross-check still runs on a contaminated cell (Codex AK04).
	c.Cal.Collected.WarmupP99["read"] *= 1.02
	_, problems = Observe(c)
	require.Len(t, problems, 1)
	require.Contains(t, problems[0], "differs from the recorded")
}

// night loads the fixture as all four cells and a matching, valid summary.
func night(t *testing.T) (Summary, []Cell) {
	t.Helper()
	cal, err := cellrun.LoadCalibration(fixture)
	require.NoError(t, err)
	s := Summary{Mode: "night", Kind: "calib", State: "completed", RunnerToken: "tok"}
	var cells []Cell
	exit := 1
	for i, id := range cellIDs {
		c := cal
		c.Verdict.Cell, c.Build.Cell = id, id
		c.Build.Attempt = "a" + id
		c.Build.SourceClean = "true"
		c.Verdict.Gates = slices.Clone(cal.Verdict.Gates)
		cells = append(cells, Cell{ID: id, Cal: c})
		s.Cells = append(s.Cells, SummaryCell{Cell: id, Completion: "done", Report: &CleanupReport{Unresolved: &[]any{}}, Problems: &[]string{}, Receipt: Receipt{
			State: "done", Cell: id, Mode: "night", Kind: "calib", Attempt: "a" + id, ExecDir: "dir" + string(rune('0'+i)),
			Digest: cal.Build.SoakSHA256, LauncherExit: &exit, Proven: true,
		}})
	}
	return s, cells
}

func unchanged(string) (bool, error) { return true, nil }

func TestSuitabilityAcceptsAValidNight(t *testing.T) {
	s, cells := night(t)
	require.Empty(t, Suitability(s, cells, Options{SourceUnchanged: unchanged}))
}

func TestSuitabilityRefusals(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(s *Summary, cells []Cell, o *Options)
		want   string
	}{
		{"mode", func(s *Summary, _ []Cell, _ *Options) { s.Kind = "night" }, "not -mode night -kind calib"},
		{"three cells", func(s *Summary, cells []Cell, _ *Options) { s.Cells = s.Cells[:3] }, "not exactly"},
		{"not done", func(s *Summary, _ []Cell, _ *Options) { s.Cells[0].Completion = "unknown" }, "completion"},
		{"unproven", func(s *Summary, _ []Cell, _ *Options) { s.Cells[1].Receipt.Proven = false }, "proven false"},
		{"launcher exit", func(s *Summary, _ []Cell, _ *Options) { e := 3; s.Cells[1].Receipt.LauncherExit = &e }, "launcher exit"},
		{"unresolved", func(s *Summary, _ []Cell, _ *Options) { s.Cells[2].Report.Unresolved = &[]any{"x"} }, "unresolved resources"},
		{"no report", func(s *Summary, _ []Cell, _ *Options) { s.Cells[2].Report = nil }, "no validated cleanup report"},
		{"no unresolved list", func(s *Summary, _ []Cell, _ *Options) { s.Cells[2].Report.Unresolved = nil }, "no validated cleanup report"},
		{"runner problems", func(s *Summary, _ []Cell, _ *Options) {
			s.Cells[1].Problems = &[]string{"launcher report unreadable"}
		}, "runner recorded problems"},
		{"missing evidence", func(_ *Summary, cells []Cell, _ *Options) { cells[0].Cal.Missing = []string{"no oracles event"} }, "no oracles event"},
		{"no runner problems list", func(s *Summary, _ []Cell, _ *Options) { s.Cells[1].Problems = nil }, "no problems list"},
		{"empty attempt", func(s *Summary, cells []Cell, _ *Options) {
			s.Cells[0].Receipt.Attempt, cells[0].Cal.Build.Attempt = "", ""
		}, "names no attempt"},
		{"empty digest", func(s *Summary, cells []Cell, _ *Options) {
			for i := range cells {
				s.Cells[i].Receipt.Digest, cells[i].Cal.Build.SoakSHA256 = "", ""
			}
		}, "digest"},
		{"bad digest", func(s *Summary, cells []Cell, _ *Options) {
			for i := range cells {
				s.Cells[i].Receipt.Digest, cells[i].Cal.Build.SoakSHA256 = "abc", "abc"
			}
		}, "digest"},
		{"empty exec dir", func(s *Summary, _ []Cell, _ *Options) { s.Cells[2].Receipt.ExecDir = "" }, "names no execution directory"},
		{"attempt mismatch", func(_ *Summary, cells []Cell, _ *Options) { cells[0].Cal.Build.Attempt = "other" }, "does not match the receipt"},
		{"digest mismatch", func(_ *Summary, cells []Cell, _ *Options) { cells[3].Cal.Build.SoakSHA256 = "x" }, "different binaries"},
		{"driver mismatch", func(_ *Summary, cells []Cell, _ *Options) { cells[3].Cal.Build.DriverSHA = "x" }, "different driver commits"},
		{"source changed", func(_ *Summary, _ []Cell, o *Options) {
			o.SourceUnchanged = func(string) (bool, error) { return false, nil }
		}, "changed between"},
		{"source not clean", func(_ *Summary, cells []Cell, _ *Options) { cells[0].Cal.Build.SourceClean = "false" }, "-accept-provenance"},
		{"source unknown", func(_ *Summary, cells []Cell, _ *Options) { cells[0].Cal.Build.SourceClean = "unknown" }, "-accept-provenance"},
		{"source unverified", func(_ *Summary, cells []Cell, _ *Options) { cells[0].Cal.Build.SourceClean = "unverified" }, "-accept-provenance"},
		{"source clean as a boolean", func(_ *Summary, cells []Cell, _ *Options) { cells[0].Cal.Build.SourceClean = true }, "-accept-provenance"},
		{"late observations", func(_ *Summary, cells []Cell, _ *Options) {
			cells[1].Cal.Late = maps.Clone(cells[1].Cal.Late)
			cells[1].Cal.Late["read"] = 3
		}, "3 late latency observations of class read"},
		{"not final", func(_ *Summary, cells []Cell, _ *Options) { cells[1].Cal.Verdict.Final = false }, "not final"},
		{"incomplete", func(_ *Summary, cells []Cell, _ *Options) { cells[1].Cal.Verdict.Evidence.Complete = false }, "incomplete"},
		{"short", func(_ *Summary, cells []Cell, _ *Options) { cells[1].Cal.Verdict.WorkloadSeconds = 7000 }, "less than 7200"},
		{"failing gate", func(_ *Summary, cells []Cell, _ *Options) {
			cells[2].Cal.Verdict.Gates[slices.IndexFunc(cells[2].Cal.Verdict.Gates, func(r gate.Result) bool { return r.Gate == "G11" })].Status = gate.StatusFail
		}, "G11 is fail"},
		{"faults stopped", func(_ *Summary, cells []Cell, _ *Options) { cells[0].Cal.Collected.Report.FaultsStopped = "M3" }, "faults stopped"},
		{"mandatory", func(_ *Summary, cells []Cell, _ *Options) { cells[0].Cal.Collected.Report.MandatoryExecuted = 7 }, "mandatory faults"},
		{"no baseline", func(_ *Summary, cells []Cell, _ *Options) { cells[0].Cal.HasBaseline = false }, "baseline"},
		{"num conns", func(_ *Summary, cells []Cell, _ *Options) { cells[0].Cal.NumConns = 3 }, "num_conns"},
		{"churns", func(_ *Summary, cells []Cell, _ *Options) {
			cells[0].Cal.Collected.Churns = cells[0].Cal.Collected.Churns[:6]
		}, "churn residues"},
		{"classes", func(_ *Summary, cells []Cell, _ *Options) {
			cells[0].Cal.Collected.Classes = cells[0].Cal.Collected.Classes[1:]
		}, "classes"},
		{"bad accept", func(_ *Summary, _ []Cell, o *Options) { o.Accept = []Accept{{Cell: "c99", Gate: "G8", Reason: "r"}} }, "names no cell"},
		{"no source_clean", func(_ *Summary, cells []Cell, _ *Options) { cells[2].Cal.Build.SourceClean = nil }, "-accept-provenance"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, cells := night(t)
			o := Options{SourceUnchanged: unchanged}
			tc.mutate(&s, cells, &o)
			problems := Suitability(s, cells, o)
			require.True(t, slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, tc.want) }), "%v", problems)
		})
	}
}

func TestSuitabilityAcceptances(t *testing.T) {
	s, cells := night(t)
	i := slices.IndexFunc(cells[0].Cal.Verdict.Gates, func(r gate.Result) bool { return r.Gate == "G8" })
	cells[0].Cal.Verdict.Gates[i].Status = gate.StatusFail
	cells[1].Cal.Build.SourceClean = "unverified"
	o := Options{SourceUnchanged: unchanged}
	require.Len(t, Suitability(s, cells, o), 2)
	o.Accept = []Accept{{Cell: "c41p4", Gate: "G8", Reason: "admitted by v7.11"}}
	o.AcceptProvenance = "no uncommitted driver change"
	require.Empty(t, Suitability(s, cells, o))

	// An absent source_clean goes through the same acceptance (Codex AO05); driver_dirty is no longer judged (PLAN §51.4).
	cells[2].Cal.Build.SourceClean = nil
	cells[3].Cal.Build.DriverDirty = "true"
	require.Empty(t, Suitability(s, cells, o))
}

// A late latency observation refuses an otherwise suitable cell, and no acceptance admits it (PLAN §51.2).
func TestSuitabilityRefusesLateObservations(t *testing.T) {
	s, cells := night(t)
	o := Options{SourceUnchanged: unchanged, AcceptProvenance: "r", Accept: []Accept{{Cell: "c41p4", Gate: "G8", Reason: "r"}},
		AcceptZero: map[string]string{gate.KL: "r", gate.KE: "r"}}
	require.Empty(t, Suitability(s, cells, o), "the fixture night is suitable")
	cells[2].Cal.Late = maps.Clone(cells[2].Cal.Late) // night shares one loaded map
	cells[2].Cal.Late["lwt"] = 1
	problems := Suitability(s, cells, o)
	require.Len(t, problems, 1)
	require.Contains(t, problems[0], "c50p4: 1 late latency observations of class lwt")
}

func TestSelfCheck(t *testing.T) {
	_, cells := night(t)
	th := gate.Thresholds{}
	for k := range cells[0].Cal.Collected.Critical() {
		th[k] = 1e9
	}
	require.Empty(t, SelfCheck(cells[:1], th), "the fixture keeps every health round")

	// Health rounds lost after 5 min leave G16's coverage incomplete: G16 still passes, but its missing evidence refuses (Codex AK03).
	short := cells[0]
	short.Cal.Collected.TP, short.Cal.Collected.GC = short.Cal.Collected.TP[:6], short.Cal.Collected.GC[:18]
	problems := SelfCheck([]Cell{short}, th)
	require.NotEmpty(t, problems)
	for _, p := range problems {
		require.Contains(t, p, "G16 missing evidence")
	}

	th[gate.KC] = 0
	problems = SelfCheck(cells[:1], th)
	require.Len(t, problems, 1, "a threshold below the data fails its gate")
	require.Contains(t, problems[0], "G13", "the fixture's kC comes from the churn residue, its teardown drift is 0")

	th[gate.KC] = 1e9
	cells[0].Cal.Collected.Runtime = []string{"a register went backwards"}
	problems = SelfCheck(cells[:1], th)
	require.Len(t, problems, 1, "a fixed predicate is never hidden by the thresholds")
	require.Contains(t, problems[0], "G10")
}

// withLatency replaces a cell's latency with 2 ms everywhere over the ranges kL reads,
// and its recorded p99 with the rebuilt ones, so the fixture can be derived end to end.
func withLatency(c Cell) Cell {
	lat := probe.NewLiveLatency(time.Unix(0, 0), 5*time.Second)
	for _, class := range c.Cal.Collected.Classes {
		for s := 0; s < 7200; s += 5 {
			lat.Observe(class, time.Unix(int64(s), 0), 2*time.Millisecond, false)
		}
	}
	lat = rebuilt(lat)
	c.Cal.Latency = lat
	c.Cal.Collected.WarmupP99, c.Cal.Collected.CooldownP99 = map[string]float64{}, map[string]float64{}
	for _, class := range c.Cal.Collected.Classes {
		w, _, _ := lat.Quantile(class, probe.Window{From: 0, To: 900}, 0.99)
		k, _, _ := lat.Quantile(class, probe.Window{From: 6300, To: 7200}, 0.99)
		c.Cal.Collected.WarmupP99[class], c.Cal.Collected.CooldownP99[class] = w.Seconds(), k.Seconds()
	}
	return c
}

func TestApplyRaises(t *testing.T) {
	base := func() (gate.Thresholds, []Derived) {
		th := gate.Thresholds{}
		var ds []Derived
		for _, k := range gate.AllThresholds {
			th[k] = 1
			ds = append(ds, Derived{Name: k, Value: 1})
		}
		return th, ds
	}

	th, ds := base()
	problems := ApplyRaises(th, ds, []Raise{{Name: gate.KL, Value: 0.5, Reason: "too low"}, {Name: gate.KFs, Value: 2, Reason: "one fd"}})
	require.Empty(t, problems)
	require.InDelta(t, 1, th[gate.KL], 0, "a raise never lowers")
	require.InDelta(t, 2, th[gate.KFs], 0)
	kl, kfs := derivedOf(t, ds, gate.KL), derivedOf(t, ds, gate.KFs)
	require.Equal(t, &Raised{From: 1, Given: 0.5, Reason: "too low", Applied: false}, kl.Raised)
	require.InDelta(t, 1, kl.Value, 0)
	require.Equal(t, &Raised{From: 1, Given: 2, Reason: "one fd", Applied: true}, kfs.Raised)
	require.InDelta(t, 2, kfs.Value, 0, "the report's value is gates.json's")
	require.Nil(t, derivedOf(t, ds, gate.KG).Raised)

	for name, tc := range map[string]struct {
		raises []Raise
		want   string
	}{
		"unknown":   {[]Raise{{Name: "kX", Value: 1, Reason: "r"}}, "kX"},
		"duplicate": {[]Raise{{Name: gate.KL, Value: 1, Reason: "r"}, {Name: gate.KL, Value: 2, Reason: "r"}}, "twice"},
		"negative":  {[]Raise{{Name: gate.KL, Value: -1, Reason: "r"}}, "finite"},
		"NaN":       {[]Raise{{Name: gate.KL, Value: math.NaN(), Reason: "r"}}, "finite"},
		"infinite":  {[]Raise{{Name: gate.KL, Value: math.Inf(1), Reason: "r"}}, "finite"},
		"no reason": {[]Raise{{Name: gate.KL, Value: 2, Reason: " "}}, "reason"},
	} {
		t.Run(name, func(t *testing.T) {
			th, ds := base()
			problems := ApplyRaises(th, ds, tc.raises)
			require.Len(t, problems, 1)
			require.Contains(t, problems[0], tc.want)
			require.InDelta(t, 1, th[gate.KL], 0, "a refused raise changes nothing")
		})
	}
}

// A reason cannot break derivation.md's table (Codex AY01).
func TestMarkdownEscapesRaiseReasons(t *testing.T) {
	r := Report{Thresholds: []Derived{
		{Name: gate.KL, Value: 2, Raised: &Raised{From: 1, Given: 2, Reason: "warm | cool\\ne\nline", Applied: true}},
		{Name: gate.KFs, Value: 6, Raised: &Raised{From: 6, Given: 1, Reason: "a|b\nc", Applied: false}},
	}}
	rows := 0
	for _, line := range strings.Split(r.Markdown(), "\n") {
		if !strings.HasPrefix(line, "| "+gate.KL+" |") && !strings.HasPrefix(line, "| "+gate.KFs+" |") {
			continue
		}
		unescaped := strings.Count(line, "|") - strings.Count(line, "\\|")
		require.Equal(t, 10, unescaped, "a row keeps its nine cells: %s", line)
		require.True(t, strings.HasSuffix(line, " |"), "a row ends on its own line: %s", line)
		rows++
	}
	require.Equal(t, 2, rows)
	md := r.Markdown()
	require.Contains(t, md, `warm \| cool\\ne<br>line`)
	require.Contains(t, md, `a\|b<br>c`)
}

// A raise lands in gates.json and the report, and never bypasses a refusal.
func TestEvaluateRaise(t *testing.T) {
	s, cells := night(t)
	for i := range cells {
		cells[i] = withLatency(cells[i])
	}
	o := Options{SourceUnchanged: unchanged, AcceptZero: map[string]string{gate.KD: "no drops"}, Raise: []Raise{{Name: gate.KL, Value: 0.6, Reason: "§48.1"}}}
	th, r := Evaluate("summary.json", s, cells, o)
	require.False(t, r.Refused, "%v", r.Problems)
	require.InDelta(t, 0.6, th[gate.KL], 0)
	d := derivedOf(t, r.Thresholds, gate.KL)
	require.InDelta(t, 0.6, d.Value, 0)
	require.Equal(t, &Raised{From: Floors[gate.KL], Given: 0.6, Reason: "§48.1", Applied: true}, d.Raised)

	// A raise below the floor is a no-op, reported.
	o.Raise = []Raise{{Name: gate.KL, Value: 0.4, Reason: "§48.1"}}
	th, r = Evaluate("summary.json", s, cells, o)
	require.False(t, r.Refused, "%v", r.Problems)
	require.InDelta(t, Floors[gate.KL], th[gate.KL], 0)
	require.Equal(t, &Raised{From: Floors[gate.KL], Given: 0.4, Reason: "§48.1", Applied: false}, derivedOf(t, r.Thresholds, gate.KL).Raised)
	require.Contains(t, r.Markdown(), "raise to 0.4 not applied")

	o.Raise = []Raise{{Name: "kX", Value: 1, Reason: "r"}}
	th, r = Evaluate("summary.json", s, cells, o)
	require.True(t, r.Refused)
	require.Nil(t, th)

	o = Options{SourceUnchanged: unchanged, Raise: []Raise{{Name: gate.KD, Value: 5, Reason: "r"}}}
	th, r = Evaluate("summary.json", s, cells, o)
	require.True(t, r.Refused, "a raise does not accept a degenerate zero")
	require.Nil(t, th)
	require.True(t, slices.ContainsFunc(r.Problems, func(p string) bool { return strings.HasPrefix(p, gate.KD+": degenerate") }), "%v", r.Problems)
}

func TestEvaluateSucceedsOnTheFixture(t *testing.T) {
	s, cells := night(t)
	for i := range cells {
		cells[i] = withLatency(cells[i])
	}
	th, r := Evaluate("summary.json", s, cells, Options{SourceUnchanged: unchanged, AcceptZero: map[string]string{gate.KD: "no drops"}})
	require.False(t, r.Refused, "%v", r.Problems)
	require.Len(t, th, len(gate.AllThresholds))
	require.InDelta(t, 16, th[gate.KC], 0)
	require.InDelta(t, 2*6820, th[gate.KLWTu], 0)
	require.InDelta(t, Floors[gate.KL], th[gate.KL], 0, "flat latency: the floor")
	for _, k := range gate.AllThresholds {
		require.False(t, math.IsNaN(th[k]) || math.IsInf(th[k], 0), k)
	}
}

// The fixture's own derivation passes its self-check: the loader, the critical values and the gates agree.
func TestEvaluateOnTheFixture(t *testing.T) {
	s, cells := night(t)
	th, r := Evaluate("summary.json", s, cells, Options{SourceUnchanged: unchanged, AcceptZero: map[string]string{gate.KD: "no drops"}})
	// The fixture keeps three latency slices, so kL's windows have no p99: that is its only refusal.
	require.True(t, r.Refused)
	require.Nil(t, th)
	for _, p := range r.Problems {
		require.Contains(t, p, "p99", p)
	}
	for _, d := range r.Thresholds {
		if d.Name == gate.KL {
			continue
		}
		require.False(t, math.IsNaN(d.Value), d.Name)
	}
}

// rebuilt seals a live record and rebuilds it the way the derivation's loader does, so any window can be queried.
func rebuilt(live *probe.Latency) *probe.Latency {
	byClass := map[string][]probe.SliceHistogram{}
	for _, s := range live.Seal(math.MaxInt) {
		for class, h := range s.Classes {
			byClass[class] = append(byClass[class], h)
		}
	}
	l, err := probe.RebuildLatency(5, byClass)
	if err != nil {
		panic(err)
	}
	return l
}
