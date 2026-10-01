package cellrun

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/artifact"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/ccmctl"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cell"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/config"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/view"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

func TestFaultFree(t *testing.T) {
	iv := func(a, b float64) gate.Interval { return gate.Interval{Start: a, End: b} }
	require.Equal(t, []gate.Interval{iv(900, 7200)}, FaultFree(900, 7200, nil))
	require.Equal(t, []gate.Interval{iv(900, 1000), iv(1200, 1300), iv(1500, 7200)},
		FaultFree(900, 7200, []gate.Interval{iv(1300, 1500), iv(1000, 1200)}), "windows in any order")
	require.Equal(t, []gate.Interval{iv(1000, 1100)},
		FaultFree(900, 7200, []gate.Interval{iv(0, 1000), iv(1100, 7200)}), "a failed window runs to the end")
	require.Equal(t, []gate.Interval{iv(900, 1000), iv(1400, 7200)},
		FaultFree(900, 7200, []gate.Interval{iv(1000, 1300), iv(1200, 1400)}), "overlapping windows merge")
	require.Empty(t, FaultFree(900, 7200, []gate.Interval{iv(0, 8000)}))
}

func thresholds() gate.Thresholds {
	return gate.Thresholds{
		gate.KG: 10, gate.KG0: 20, gate.KH: 5, gate.KHr: 0.1, gate.KF: 10, gate.KFs: 2, gate.KS: 8, gate.KSs: 2,
		gate.KE: 3, gate.KC: 5, gate.KCh: 1, gate.KL: 0.5, gate.KLWTu: 10, gate.KD: 100, gate.KGp: 500, gate.KGt: 0.05,
	}
}

var testClasses = []string{"write", "read", "lwt"}

func steady(n int, rate float64) map[string][]float64 {
	out := map[string][]float64{}
	for _, c := range testClasses {
		secs := make([]float64, n)
		for i := range secs {
			secs[i] = rate
		}
		out[c] = secs
	}
	return out
}

// green is a night cell's record on which every gate holds, with every piece of evidence present.
func green() Collected {
	tl := config.Timeline{Warmup: config.NightWarmup, Cooldown: config.NightCooldown, Workload: config.NightWorkload}
	var profiles []Profile
	for t := 0.0; t <= 7200; t += 300 {
		quiet := t < 900 || t >= 6300
		profiles = append(profiles, Profile{T: t, Reason: ReasonPeriodic, Quiet: quiet, R2Checked: quiet,
			Groups: map[string]int{"main": 1, "gocql": 40}, Total: 41, HeapMiB: 50, FDs: 30, Balance: map[string]int64{"primary": 0}})
	}
	profiles = append(profiles, Profile{T: 7204, Reason: ReasonFinal, Quiet: true, R2Checked: true,
		Groups: map[string]int{"main": 1}, Total: 1, HeapMiB: 5, FDs: 20, Balance: map[string]int64{"primary": 0}})
	p99 := func(v float64) map[string]float64 {
		out := map[string]float64{}
		for _, c := range testClasses {
			out[c] = v
		}
		return out
	}
	var churns []gate.ChurnResidue
	for i := range 7 {
		churns = append(churns, gate.ChurnResidue{Slot: fmt.Sprintf("C%d", i+1), Index: i, Measured: true,
			Before: map[string]int{"main": 1}, After: map[string]int{"main": 1}})
	}
	return Collected{
		Mode: gate.ModeNight, Timeline: tl, G15Scale: 1, Thresholds: thresholds(), WorkloadSeconds: 7200, Ran: true,
		G0Checked: true, Baseline: Baseline{Groups: map[string]int{"main": 1}, Total: 30, FDs: 25}, Profiles: profiles,
		G8:       gate.Bool("G8", nil),
		Report:   chaos.Report{MandatoryFaults: 8, MandatoryExecuted: 8, ChurnSlots: 7, ChurnExecuted: 7},
		Windows:  []gate.Interval{{Start: 1350, End: 1600}, {Start: 3750, End: 4900}},
		Progress: gate.Progress{Offered: steady(7200, 10), Completed: steady(7200, 10)}, Classes: testClasses,
		WarmupP99: p99(0.004), CooldownP99: p99(0.005), Churns: churns,
		G12: gate.G12Input{CloseElapsed: time.Second, CloseReturned: true, Baseline: map[string]int{"main": 1},
			After: map[string]int{"main": 2}, FD0: 25, FDs: 27}, G12Checked: true,
		Settlement:            workload.SettlementTotals{PrefetchProven: 5000, SpeculationProven: 2000},
		ShortDeadlineTimeouts: 150,
		Nodes:                 testNodes, TP: healthyTP(), GC: healthyGC(),
	}
}

var testNodes = []string{"node1", "node2", "node3"}

// healthyTP is a round every 60 s over the whole night, the first being the prime.
func healthyTP() []gate.TPStatsSample {
	var out []gate.TPStatsSample
	for t := 0.0; t <= 7200; t += 60 {
		r := gate.TPStatsSample{Start: t, T: t + 2, PID: map[string]int{}, Dropped: map[string]float64{}}
		for _, n := range testNodes {
			r.PID[n], r.Dropped[n] = 1, 0
		}
		out = append(out, r)
	}
	return out
}

// healthyGC is a quiet gcstats reading per node for every round, the first being the prime reset.
func healthyGC() []gate.GCStatsSample {
	var out []gate.GCStatsSample
	for t := 0.0; t <= 7200; t += 60 {
		for _, n := range testNodes {
			out = append(out, gate.GCStatsSample{Start: t + 1, T: t + 2, Node: n, IntervalMs: 60000, MaxPauseMs: 5, TotalMs: 60,
				OK: true, Prime: t == 0, PID: 1})
		}
	}
	return out
}

func statusOf(v gate.Verdict, g string) gate.Status {
	for _, r := range v.Gates {
		if r.Gate == g {
			return r.Status
		}
	}
	return ""
}

func TestEvaluateGreen(t *testing.T) {
	v := Evaluate(green())
	require.Equal(t, gate.VerdictPass, v.Status, "%+v", v)
	require.Len(t, v.Gates, 17, "G0–G16")
}

func TestEvaluateFailures(t *testing.T) {
	cases := map[string]struct {
		mutate func(*Collected)
		gate   string
	}{
		"leak":       {func(c *Collected) { c.Profiles[3].Leaks = 1 }, "G1"},
		"leak after": {func(c *Collected) { c.G12.Leaks = 2 }, "G1"},
		"dial 9042":  {func(c *Collected) { c.Dials9042 = []string{"x"} }, "G5"},
		"quiet R2":   {func(c *Collected) { c.Profiles[len(c.Profiles)-2].R2 = []string{"node2 has 1"} }, "G5"},
		"unexpected": {func(c *Collected) { c.G8 = gate.Bool("G8", []string{"1 unexpected"}) }, "G8"},
		"no G8":      {func(c *Collected) { c.G8 = gate.Result{} }, "G8"},
		"g9":         {func(c *Collected) { c.Report.G9Failures = []string{"M2 F-kill"} }, "G9"},
		"register":   {func(c *Collected) { c.Register = []string{"k"} }, "G10"},
		"progress": {func(c *Collected) {
			c.Progress.Completed = steady(7200, 10)
			for i := 5000; i < 5060; i++ {
				c.Progress.Completed["lwt"][i] = 0
			}
		}, "G11"},
		"close":       {func(c *Collected) { c.G12.CloseReturned = false }, "G12"},
		"churn":       {func(c *Collected) { c.Report.ChurnFailures = []string{"C3 overran"} }, "G13"},
		"latency":     {func(c *Collected) { c.CooldownP99["write"] = 0.01 }, "G14"},
		"coverage":    {func(c *Collected) { c.Settlement.PrefetchProven = 10 }, "G15"},
		"quiet total": {func(c *Collected) { c.Profiles[len(c.Profiles)-2].Total = 200 }, "G2"},
	}
	for name, tc := range cases {
		c := green()
		tc.mutate(&c)
		v := Evaluate(c)
		require.Equal(t, gate.VerdictFail, v.Status, name)
		require.Contains(t, v.FailingGates, tc.gate, name)
	}
}

// Removing any piece of required evidence from a green record must fail the gate that needs it (Codex I05).
func TestEvaluateMissingEvidenceFails(t *testing.T) {
	last := func(c *Collected) *Profile { return &c.Profiles[len(c.Profiles)-2] } // the last periodic, quiet one
	cases := map[string]struct {
		mutate func(*Collected)
		gate   string
	}{
		"fd read failed":      {func(c *Collected) { last(c).FDs = -1 }, "G4"},
		"no goroutine dump":   {func(c *Collected) { c.Profiles[10].Groups = nil }, "G2"},
		"no heap":             {func(c *Collected) { c.Profiles[10].HeapMiB = -1 }, "G3"},
		"R2 not checked":      {func(c *Collected) { last(c).R2Checked = false }, "G5"},
		"no balance":          {func(c *Collected) { last(c).Balance = map[string]int64{} }, "G6"},
		"a cool-down class":   {func(c *Collected) { delete(c.CooldownP99, "lwt") }, "G14"},
		"a churn residue":     {func(c *Collected) { c.Churns = c.Churns[:6] }, "G13"},
		"a class never ran":   {func(c *Collected) { delete(c.Progress.Offered, "lwt") }, "G11"},
		"too few quiet":       {func(c *Collected) { onlyQuiet(c, 1) }, "G4"},
		"leak after close -1": {func(c *Collected) { c.G12.Leaks = -1 }, "G1"},
	}
	for name, tc := range cases {
		c := green()
		tc.mutate(&c)
		v := Evaluate(c)
		require.Equal(t, gate.VerdictFail, v.Status, name)
		require.Contains(t, v.FailingGates, tc.gate, name)
	}
}

// After a fault fails G9 its window stays open, so too few quiet checkpoints is an exclusion, not missing evidence (Codex J09).
func TestEvaluateQuietExclusionAfterG9(t *testing.T) {
	c := green()
	onlyQuiet(&c, 1)
	c.Report.G9Failures = []string{"M1 F-kill: not recovered"}
	c.Report.FaultsStopped = "M1 failed G9"
	v := Evaluate(c)
	require.Equal(t, gate.VerdictFail, v.Status)
	require.Equal(t, []string{"G9"}, v.FailingGates, "only G9 fails; the quiet-checkpoint gates report the exclusion")
	require.Equal(t, gate.StatusPass, statusOf(v, "G4"))
}

// onlyQuiet keeps n quiet checkpoints in the trend window.
func onlyQuiet(c *Collected, n int) {
	kept := 0
	for i := range c.Profiles {
		p := &c.Profiles[i]
		if p.Quiet && p.T >= c.Timeline.Warmup.Seconds() && p.Reason != ReasonFinal {
			if kept >= n {
				p.Quiet, p.R2Checked = false, false
			}
			kept++
		}
	}
}

// The final sample, taken after the workload stopped, stays out of the trend series (Codex I14).
func TestEvaluateFinalSampleOutsideTrends(t *testing.T) {
	c := green()
	c.Profiles[len(c.Profiles)-1].HeapMiB = 5000
	c.Profiles[len(c.Profiles)-1].Total = 5000
	require.Equal(t, gate.VerdictPass, Evaluate(c).Status)
}

func TestEvaluateCooldownContamination(t *testing.T) {
	c := green()
	c.Windows = append(c.Windows, gate.Interval{Start: 6000, End: 7200})
	c.CooldownP99["write"] = 1
	v := Evaluate(c)
	require.Equal(t, gate.StatusNotEvaluated, statusOf(v, "G14"))
}

func TestEvaluateMissingThresholds(t *testing.T) {
	c := green()
	c.Thresholds = gate.Thresholds{}
	require.Equal(t, gate.VerdictInvalidConfig, Evaluate(c).Status)
}

func TestEvaluateAborted(t *testing.T) {
	c := Collected{Mode: gate.ModeNight, FixtureInvalid: []string{"ccm create: boom"}}
	require.Equal(t, gate.VerdictFixtureInvalid, Evaluate(c).Status)

	c = Collected{Mode: gate.ModeNight, G0Checked: true, G0: []string{"frame versions [4]"}}
	require.Equal(t, gate.VerdictFixtureInvalid, Evaluate(c).Status)

	c = Collected{Mode: gate.ModeNight, G0Checked: true, Incomplete: []string{"interrupted"}}
	require.Equal(t, gate.VerdictIncomplete, Evaluate(c).Status)

	c = green()
	c.FixtureInvalid = []string{"fault: M3 remove overran"}
	c.WorkloadSeconds = 2000
	require.Equal(t, gate.VerdictFixtureInvalid, Evaluate(c).Status, "a fixture abort mid-run is not incomplete")
}

func TestEvaluateValidationScale(t *testing.T) {
	c := green()
	c.Mode, c.G15Scale, c.WorkloadSeconds = gate.ModeValidate, config.ValidationG15Scale, 2700
	c.Timeline = config.Timeline{Warmup: config.ValidationWarmup, Cooldown: config.ValidationCooldown, Workload: config.ValidationWorkload}
	c.Settlement = workload.SettlementTotals{PrefetchProven: 67, SpeculationProven: 100}
	c.ShortDeadlineTimeouts = 34
	c.Profiles = c.Profiles[:10]
	v := Evaluate(c)
	require.Equal(t, gate.StatusPass, statusOf(v, "G15"), "%+v", v.Gates)
	c.ShortDeadlineTimeouts = 33
	require.Equal(t, gate.StatusFail, statusOf(Evaluate(c), "G15"))
}

func readLines(t *testing.T, path string) []map[string]any {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	var out []map[string]any
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var m map[string]any
		require.NoError(t, json.Unmarshal(sc.Bytes(), &m))
		out = append(out, m)
	}
	return out
}

// On protocol 4 an LWT's CAS write timeout under 1 s is row 4 (PLAN §36.4); at 1 s or with an unknown elapsed it stays row 3.
func TestRecorderProtocolFourPartialAccept(t *testing.T) {
	for _, proto := range []int{4, 5} {
		t.Run(fmt.Sprintf("proto %d", proto), func(t *testing.T) {
			dir := t.TempDir()
			errs, err := artifact.OpenJSONL(filepath.Join(dir, "errors.jsonl"))
			require.NoError(t, err)
			events, err := artifact.OpenJSONL(filepath.Join(dir, "events.jsonl"))
			require.NoError(t, err)
			rec := NewRecorder(errs, events, &chaos.Windows{}, proto)
			epoch := time.Now()
			rec.SetEpoch(epoch)
			cas := func() error {
				return &gocql.RequestErrWriteTimeout{WriteType: "CAS", Received: 1, BlockFor: 2, Consistency: gocql.Serial}
			}
			rec.Error(workload.ErrorRecord{Time: epoch, Session: "primary", Class: workload.ClassLWT, Step: "lwt-update", Err: cas(), Elapsed: 3 * time.Millisecond})
			rec.Error(workload.ErrorRecord{Time: epoch, Session: "primary", Class: workload.ClassLWT, Step: "lwt-read", Err: cas(), Elapsed: time.Second})
			rec.Error(workload.ErrorRecord{Time: epoch, Session: "primary", Class: workload.ClassLWT, Step: "lwt-update", Err: cas()})
			require.NoError(t, errs.Close())
			require.NoError(t, events.Close())

			lines := readLines(t, filepath.Join(dir, "errors.jsonl"))
			require.Len(t, lines, 3)
			require.InDelta(t, 3, lines[0]["elapsed_ms"], 1e-9)
			require.InDelta(t, 1000, lines[1]["elapsed_ms"], 1e-9)
			require.NotContains(t, lines[2], "elapsed_ms", "an unknown elapsed is not recorded")
			for _, l := range lines {
				require.Equal(t, "*gocql.RequestErrWriteTimeout", l["type"])
			}
			if proto == 4 {
				require.Equal(t, "cas-unknown", lines[0]["class"])
				require.Equal(t, true, lines[0]["p4_cas_unknown"])
				require.Equal(t, true, lines[0]["admitted"])
				require.EqualValues(t, 2, rec.Unexpected(), "1 s and unknown elapsed stay row 3")
				require.Equal(t, map[string]int64{"cas-unknown": 1, "server-timeout": 2}, rec.ClassCounts())
			} else {
				require.Equal(t, "server-timeout", lines[0]["class"])
				require.EqualValues(t, 3, rec.Unexpected(), "protocol 5 never applies the rule")
				require.Equal(t, map[string]int64{"server-timeout": 3}, rec.ClassCounts())
			}
			for i := 1; i < 3; i++ {
				require.Equal(t, "server-timeout", lines[i]["class"])
				require.NotContains(t, lines[i], "p4_cas_unknown")
				require.Equal(t, false, lines[i]["admitted"])
			}
		})
	}
}

func TestRecorderClassifiesAgainstWindows(t *testing.T) {
	dir := t.TempDir()
	errs, err := artifact.OpenJSONL(filepath.Join(dir, "errors.jsonl"))
	require.NoError(t, err)
	events, err := artifact.OpenJSONL(filepath.Join(dir, "events.jsonl"))
	require.NoError(t, err)
	windows := &chaos.Windows{}
	epoch := time.Now()
	windows.Open("M1", chaos.FaultStop, 1, epoch.Add(100*time.Second))
	windows.Close("M1", epoch.Add(200*time.Second))
	rec := NewRecorder(errs, events, windows, 5)
	rec.SetEpoch(epoch)

	at := func(s int) time.Time { return epoch.Add(time.Duration(s) * time.Second) }
	rec.Error(workload.ErrorRecord{Time: at(150), Session: "primary", Class: workload.ClassLWT, Err: &gocql.RequestErrOverloaded{}})
	rec.Error(workload.ErrorRecord{Time: at(150), Session: "primary", Class: workload.ClassRead,
		Err: &gocql.RequestErrUnavailable{Required: 2, Alive: 0}})
	rec.Error(workload.ErrorRecord{Time: at(300), Session: "primary", Class: workload.ClassRead, Err: io.EOF})
	rec.Error(workload.ErrorRecord{Time: at(300), Session: "primary", Class: workload.ClassShortDeadline, Err: context.DeadlineExceeded})
	rec.Error(workload.ErrorRecord{Time: at(205), Session: "aux0", Class: workload.ClassChurn, Err: errors.New("canary")})
	rec.Error(workload.ErrorRecord{Time: at(400), Session: "primary", Class: workload.ClassLWT, Err: &gocql.RequestErrCASWriteUnknown{Received: 1, BlockFor: 2}})
	rec.Event("phase", at(900), "chaos")
	require.NoError(t, errs.Close())
	require.NoError(t, events.Close())

	require.EqualValues(t, 2, rec.Unexpected(), "EOF outside a window and the unknown error inside one")
	g8 := rec.G8()
	require.Equal(t, gate.StatusFail, g8.Status)
	require.Len(t, g8.Details, 3)
	require.Equal(t, map[string]int64{"overloaded": 1, "unavailable": 1, "transport": 1, "deadline": 1, "unknown": 1, "cas-unknown": 1},
		rec.ClassCounts(), "an LWT cas-unknown outside a window is admitted (v7.6)")

	lines := readLines(t, filepath.Join(dir, "errors.jsonl"))
	require.Len(t, lines, 6)
	require.Equal(t, true, lines[5]["admitted"])
	require.InDelta(t, 2, lines[5]["block_for"], 0)
	require.Equal(t, "M1", lines[0]["window"])
	require.Equal(t, true, lines[0]["admitted"])
	require.Equal(t, "*gocql.RequestErrOverloaded", lines[0]["type"])
	require.Equal(t, true, lines[1]["evidence"], "Alive < Required−1 in a one-node window")
	require.InDelta(t, 2, lines[1]["required"], 0)
	require.Equal(t, false, lines[2]["admitted"])
	require.Equal(t, true, lines[3]["admitted"], "a short-deadline timeout outside a window")
	require.InDelta(t, 300, lines[3]["t"], 0.001)
	ev := readLines(t, filepath.Join(dir, "events.jsonl"))
	require.Equal(t, "phase", ev[0]["kind"])
	require.InDelta(t, 900, ev[0]["t"], 0.001)
}

func TestHealthReadsOnlyOutsideWindows(t *testing.T) {
	tp, err := os.ReadFile("../probe/testdata/tpstats-5.0.3.txt")
	require.NoError(t, err)
	gc, err := os.ReadFile("../probe/testdata/gcstats-5.0.3.txt")
	require.NoError(t, err)
	dir := t.TempDir()
	out, err := artifact.OpenJSONL(filepath.Join(dir, "health.jsonl"))
	require.NoError(t, err)
	raw, err := artifact.OpenJSONL(filepath.Join(dir, "nodetool.jsonl"))
	require.NoError(t, err)
	calls := 0
	h := &Health{
		Nodes: []string{"node1", "node2"}, Windows: &chaos.Windows{}, Out: out, Raw: raw,
		PID: func(node string) (int, error) { return 100, nil },
		Nodetool: func(_ context.Context, node string, args ...string) (string, error) {
			calls++
			if node == "node2" && args[0] == "gcstats" {
				return "", errors.New("node2 is down")
			}
			if args[0] == "tpstats" {
				return string(tp), nil
			}
			return string(gc), nil
		},
	}
	h.epoch = time.Now()
	h.round(context.Background(), true)
	h.round(context.Background(), false)
	require.NoError(t, out.Close())
	require.NoError(t, raw.Close())
	require.Equal(t, 8, calls)

	tps, gcs := h.Samples()
	require.Len(t, tps, 2)
	require.InDelta(t, 0, tps[1].Dropped["node1"], 0)
	require.LessOrEqual(t, tps[1].Start, tps[1].T, "a round spans its commands")
	require.Greater(t, tps[1].Start, tps[0].Start)
	require.Len(t, gcs, 4, "the prime reads are kept as reset boundaries")
	require.True(t, gcs[0].Prime)
	require.False(t, gcs[2].Prime)
	require.True(t, gcs[2].OK)
	require.False(t, gcs[3].OK, "a failed read is missing")

	lines := readLines(t, filepath.Join(dir, "health.jsonl"))
	require.Len(t, lines, 2, "one line per round")
	node2 := lines[1]["nodes"].(map[string]any)["node2"].(map[string]any)
	require.Equal(t, false, node2["gc"].(map[string]any)["ok"], "a failed read is recorded as not evaluable")
	require.Len(t, readLines(t, filepath.Join(dir, "nodetool.jsonl")), 8)

	// The artifact reproduces exactly what the live run evaluated (Codex J06).
	loadedTP, loadedGC, err := LoadHealth(filepath.Join(dir, "health.jsonl"))
	require.NoError(t, err)
	require.Equal(t, gcs, loadedGC)
	require.Len(t, loadedTP, len(tps))
	for i := range tps {
		require.Equal(t, tps[i].Start, loadedTP[i].Start)
		require.Equal(t, tps[i].T, loadedTP[i].T)
		require.Equal(t, tps[i].PID, loadedTP[i].PID)
		for node, v := range tps[i].Dropped {
			if math.IsNaN(v) {
				require.True(t, math.IsNaN(loadedTP[i].Dropped[node]))
			} else {
				require.InDelta(t, v, loadedTP[i].Dropped[node], 0)
			}
		}
	}
	th := thresholds()
	for _, windows := range [][]gate.Interval{
		nil,
		{{Start: tps[0].Start, End: tps[1].T}},
		{{Start: tps[1].Start - 1e-6, End: tps[1].Start}},    // touching the second round's start only
		{{Start: tps[0].T + 1e-9, End: tps[1].Start - 1e-9}}, // between the rounds
	} {
		live, liveMissing := gate.G16(tps, gcs, windows, th)
		offline, offlineMissing := gate.G16(loadedTP, loadedGC, windows, th)
		require.Equal(t, live, offline, "windows %v", windows)
		require.Equal(t, liveMissing, offlineMissing)
	}
}

func testChurner(t *testing.T) (*Churner, *artifact.JSONL, string) {
	t.Helper()
	dir := t.TempDir()
	errs, err := artifact.OpenJSONL(filepath.Join(dir, "errors.jsonl"))
	require.NoError(t, err)
	events, err := artifact.OpenJSONL(filepath.Join(dir, "events.jsonl"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = errs.Close() })
	c := &Churner{
		Recorder: NewRecorder(errs, events, &chaos.Windows{}, 5), Sampler: &Sampler{}, Registry: view.NewRegistry(),
		FDDir: "/proc/self/fd", NetDir: "/proc/net",
		Spec: cell.SessionSpec{DriverLog: io.Discard, Settlement: workload.NewSettlement()},
	}
	return c, events, filepath.Join(dir, "events.jsonl")
}

func shortBudgets(t *testing.T) {
	t.Helper()
	orig := chaos.ChurnBudget
	chaos.ChurnBudget.Create, chaos.ChurnBudget.Close, chaos.ChurnBudget.Grace = 10*time.Millisecond, 10*time.Millisecond, 10*time.Millisecond
	t.Cleanup(func() { chaos.ChurnBudget = orig })
}

func steps(t *testing.T, path string) []string {
	var out []string
	for _, e := range readLines(t, path) {
		out = append(out, e["data"].(map[string]any)["step"].(string))
	}
	return out
}

// A CreateSession still failing at the slot deadline ends the churn with ErrChurnOverrun,
// and the churner stays Busy until the owner returns (Codex I03).
func TestChurnOwnerOutlivesTheSlot(t *testing.T) {
	shortBudgets(t)
	c, events, path := testChurner(t)
	release := make(chan struct{})
	c.Create = func(*gocql.ClusterConfig) (*gocql.Session, error) {
		<-release
		return nil, errors.New("gave up")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := c.Churn(ctx, chaos.Slot{ID: "C1"})
	require.ErrorIs(t, err, chaos.ErrChurnOverrun)
	require.True(t, c.Busy(), "the constructor is still outstanding")
	close(release)
	require.Eventually(t, func() bool { return !c.Busy() }, time.Second, time.Millisecond)
	logs, residue, busy := c.Snapshot()
	require.Len(t, logs, 1, "the failed constructor's logger is kept (Codex J04)")
	require.Len(t, residue, 1)
	require.False(t, busy)
	res := c.Residue()
	require.Len(t, res, 1)
	require.False(t, res[0].Measured)
	require.NoError(t, events.Close())
	require.Equal(t, []string{"create", "late-session"}, steps(t, path))
}

// A session arriving after the slot deadline is sampled, reported on arrival, closed by its owner even when
// Close is slow, and its residue is measured into the slot's record afterwards (Codex J04).
func TestChurnLateSessionLifecycle(t *testing.T) {
	shortBudgets(t)
	c, events, path := testChurner(t)
	arrive, closeGate := make(chan struct{}), make(chan struct{})
	c.Create = func(*gocql.ClusterConfig) (*gocql.Session, error) { <-arrive; return nil, nil }
	var closed atomic.Bool
	c.CloseSession = func(*cell.Session) { <-closeGate; closed.Store(true) }
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, c.Churn(ctx, chaos.Slot{ID: "C1"}), chaos.ErrChurnOverrun)

	close(arrive)
	require.Eventually(t, func() bool {
		c.Sampler.mu.Lock()
		defer c.Sampler.mu.Unlock()
		return c.Sampler.sessions["aux0"] != nil
	}, time.Second, time.Millisecond, "the late session is sampled while it lives")
	require.True(t, c.Busy(), "Close has not returned")
	close(closeGate)
	require.Eventually(t, func() bool { return !c.Busy() }, 5*time.Second, time.Millisecond)
	require.True(t, closed.Load())
	res := c.Residue()
	require.True(t, res[0].Measured, "the residue is measured after the slot")
	require.Contains(t, res[0].Failures, "residue measured after the slot ended")
	c.Sampler.mu.Lock()
	require.Nil(t, c.Sampler.sessions["aux0"])
	c.Sampler.mu.Unlock()
	require.NoError(t, events.Close())
	require.Equal(t, []string{"create", "late-session", "late-close", "residue"}, steps(t, path), "arrival is reported before Close")
}

func TestHealthPIDChangeMakesReadingsMissing(t *testing.T) {
	tp, err := os.ReadFile("../probe/testdata/tpstats-5.0.3.txt")
	require.NoError(t, err)
	gc, err := os.ReadFile("../probe/testdata/gcstats-5.0.3.txt")
	require.NoError(t, err)
	dir := t.TempDir()
	out, err := artifact.OpenJSONL(filepath.Join(dir, "health.jsonl"))
	require.NoError(t, err)
	raw, err := artifact.OpenJSONL(filepath.Join(dir, "nodetool.jsonl"))
	require.NoError(t, err)
	pid := 100
	h := &Health{
		Nodes: []string{"node1"}, Windows: &chaos.Windows{}, Out: out, Raw: raw, epoch: time.Now(),
		PID: func(string) (int, error) { pid++; return pid, nil },
		Nodetool: func(_ context.Context, _ string, args ...string) (string, error) {
			if args[0] == "tpstats" {
				return string(tp), nil
			}
			return string(gc), nil
		},
	}
	h.round(context.Background(), false)
	tps, gcs := h.Samples()
	require.True(t, math.IsNaN(tps[0].Dropped["node1"]))
	require.NotContains(t, tps[0].PID, "node1")
	require.False(t, gcs[0].OK)
	require.Zero(t, gcs[0].PID, "an unattributable read carries no pid")
	require.NoError(t, out.Close())
	lines := readLines(t, filepath.Join(dir, "health.jsonl"))
	node1 := lines[0]["nodes"].(map[string]any)["node1"].(map[string]any)
	require.Nil(t, node1["dropped"], "an unattributable reading is null in the artifact too (Codex J06)")
	require.InDelta(t, 0, node1["pid"], 0)
}

// While a window is open the loop reads nothing; once it closes, rounds resume (Codex review test gap).
func TestHealthLoopSkipsOpenWindows(t *testing.T) {
	dir := t.TempDir()
	out, err := artifact.OpenJSONL(filepath.Join(dir, "health.jsonl"))
	require.NoError(t, err)
	raw, err := artifact.OpenJSONL(filepath.Join(dir, "nodetool.jsonl"))
	require.NoError(t, err)
	windows := &chaos.Windows{}
	windows.Open("M1", chaos.FaultPause, 1, time.Now())
	var calls atomic.Int32
	h := &Health{
		Nodes: []string{"node1"}, Windows: windows, Out: out, Raw: raw, Interval: 5 * time.Millisecond,
		PID: func(string) (int, error) { return 1, nil },
		Nodetool: func(context.Context, string, ...string) (string, error) {
			calls.Add(1)
			return "", errors.New("no output")
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { h.Run(ctx, time.Now()); close(done) }()
	time.Sleep(50 * time.Millisecond)
	require.EqualValues(t, 2, calls.Load(), "only the prime round runs while the window is open")
	windows.Close("M1", time.Now().Add(-time.Minute))
	require.Eventually(t, func() bool { return calls.Load() > 2 }, time.Second, time.Millisecond)
	cancel()
	<-done
}

func TestForeignProcesses(t *testing.T) {
	dir := t.TempDir()
	proc := func(pid, cmdline string) {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, pid), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, pid, "cmdline"), []byte(cmdline), 0o644))
	}
	proc("100", "/usr/bin/java\x00-Xmx1G\x00org.apache.cassandra.service.CassandraDaemon\x00")
	proc("200", "/home/x/toxiproxy-server\x00-port\x008474\x00")
	proc("300", "/usr/bin/bash\x00-c\x00grep CassandraDaemon\x00")
	proc("400", "")
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "self"), 0o755))
	require.ElementsMatch(t, []string{"100: Cassandra", "200: toxiproxy-server"}, ForeignProcesses(dir))
}

// The production pre-flight ends the timetable at the cool-down, not at the workload end (Codex I15).
func TestPreflightEndsAtCooldown(t *testing.T) {
	c, err := config.CellByID("c50p5")
	require.NoError(t, err)
	for _, o := range []config.Overrides{{Mode: gate.ModeNight}, config.ValidationOverrides("")} {
		conf, err := config.New(config.NightBase(c, config.Driver{}, 1500, 32), o, 1)
		require.NoError(t, err)
		tl, slots, a, fixed := conf.Effective()
		sch, err := chaos.Plan(1, slots, a, !fixed, []string{"node1", "node2", "node3"}, nil, chaos.Specs())
		require.NoError(t, err)
		require.NoError(t, Preflight(sch, tl), o.Mode)

		late := sch
		late.Slots = append([]chaos.Slot(nil), sch.Slots...)
		last := &late.Slots[len(late.Slots)-1]
		last.Start, last.End = tl.Cooldown-time.Minute, tl.Cooldown+2*time.Minute
		require.Error(t, Preflight(late, tl), "a slot reaching into the cool-down")
	}
}

// A lossy stream, a failed pprof write or a failed manifest write each make the artifacts incomplete (Codex I08).
func TestArtifactFailuresAreCollected(t *testing.T) {
	dir := t.TempDir()
	s, err := artifact.OpenJSONL(filepath.Join(dir, "samples.jsonl"))
	require.NoError(t, err)
	r := &cellRun{streams: map[string]*artifact.JSONL{"samples.jsonl": s}, logf: func(string, ...any) {}}
	require.Empty(t, r.artifactFailures())
	s.Write(math.NaN())
	r.col.Profiles = []Profile{{T: 300, WriteErrors: []string{"disk full"}}}
	r.artifactFailure("resources.json: disk full")
	got := r.artifactFailures()
	require.Len(t, got, 3)
	require.Contains(t, got[0], "1 records could not be encoded")

	r.col = green().withIncomplete(got)
	require.Equal(t, gate.VerdictIncomplete, Evaluate(r.col).Status)
}

func (c Collected) withIncomplete(reasons []string) Collected {
	c.Incomplete = append(c.Incomplete, reasons...)
	return c
}

// Without thresholds every run is invalid-config; the evidence field still tells a damaged run apart (Codex J02).
func TestEvidenceIndependentOfThresholds(t *testing.T) {
	c := green()
	c.Thresholds = gate.Thresholds{}
	clean := Evaluate(c)
	require.Equal(t, gate.VerdictInvalidConfig, clean.Status)
	require.True(t, EvidenceOf(clean, c.Incomplete).Complete)

	c.Incomplete = []string{"artifact samples.jsonl: 3 records could not be encoded"}
	c.Profiles[len(c.Profiles)-2].FDs = -1
	damaged := Evaluate(c)
	require.Equal(t, gate.VerdictInvalidConfig, damaged.Status, "the verdict alone cannot tell them apart")
	ev := EvidenceOf(damaged, c.Incomplete)
	require.False(t, ev.Complete)
	require.Len(t, ev.Problems, 2)
	require.Contains(t, ev.Problems[1], "G4: missing evidence")
}

// Every required measurement that goes missing makes the evidence incomplete, with or without thresholds (Codex K01).
func TestEvidenceDeletionMatrix(t *testing.T) {
	cases := map[string]func(*Collected){
		"G12 dump":            func(c *Collected) { c.G12.After = nil },
		"G12 fds":             func(c *Collected) { c.G12.FDs = -1 },
		"G12 sockets":         func(c *Collected) { c.G12.ProxySockets = -1 },
		"G12 leaks":           func(c *Collected) { c.G12.Leaks = -1 },
		"G12 not measured":    func(c *Collected) { c.G12Checked = false },
		"G13 residue":         func(c *Collected) { c.Churns[2].Measured = false },
		"G13 count":           func(c *Collected) { c.Churns = c.Churns[:5] },
		"G14 cool-down":       func(c *Collected) { delete(c.CooldownP99, "lwt") },
		"G14 warm-up":         func(c *Collected) { delete(c.WarmupP99, "read") },
		"quiet fds":           func(c *Collected) { c.Profiles[len(c.Profiles)-2].FDs = -1 },
		"quiet R2":            func(c *Collected) { c.Profiles[len(c.Profiles)-2].R2Checked = false },
		"trend heap":          func(c *Collected) { c.Profiles[10].HeapMiB = -1 },
		"G16 unparsable read": func(c *Collected) { c.TP[40].Dropped["node2"] = math.NaN() },
		"no health data":      func(c *Collected) { c.TP, c.GC = nil, nil },
		"a node in a round":   func(c *Collected) { delete(c.TP[40].Dropped, "node3") },
		// gcstats resets on read, so an absent reading is covered by the next one; a failed read is what is lost.
		"a gcstats reading": func(c *Collected) { c.GC[30].OK = false },
		// The read happened and reset the counters; its result is gone (Codex N01).
		"a lost gcstats read":  func(c *Collected) { c.GC = append(slices.Clone(c.GC[:30]), c.GC[31:]...) },
		"a gap in rounds":      func(c *Collected) { c.TP = append(slices.Clone(c.TP[:100]), c.TP[104:]...) },
		"a periodic leak read": func(c *Collected) { c.Profiles[5].Leaks = -1 },
	}
	for name, mutate := range cases {
		for _, withThresholds := range []bool{true, false} {
			c := green()
			if !withThresholds {
				c.Thresholds = gate.Thresholds{}
			}
			mutate(&c)
			v := Evaluate(c)
			ev := EvidenceOf(v, c.Incomplete)
			require.False(t, ev.Complete, "%s, thresholds %v", name, withThresholds)
			// Each deletion of one measurement is counted once, whatever the thresholds (Codex L03);
			// wiping the whole health record leaves every node's steady stretches uncovered, one problem each.
			switch name {
			case "no health data":
			case "a gap in rounds": // whole rounds: each node's readings are missing, one problem per node
				require.Len(t, ev.Problems, len(testNodes), "%s, thresholds %v: %v", name, withThresholds, ev.Problems)
			default:
				require.Len(t, ev.Problems, 1, "%s, thresholds %v: %v", name, withThresholds, ev.Problems)
			}
		}
	}
	// A gap a fault window covers is an exclusion, not missing evidence.
	c := green()
	c.TP = append(slices.Clone(c.TP[:30]), c.TP[34:]...)
	c.Windows = append(c.Windows, gate.Interval{Start: 1800, End: 2040})
	require.True(t, EvidenceOf(Evaluate(c), c.Incomplete).Complete)
	for _, withThresholds := range []bool{true, false} {
		c := green()
		if !withThresholds {
			c.Thresholds = gate.Thresholds{}
		}
		require.True(t, EvidenceOf(Evaluate(c), c.Incomplete).Complete, "the green record is complete")
	}
}

// A constructor and a Close that each finish over their budget but inside the slot still get the grace period,
// the residue measurement and exactly one sampler removal (Codex K02).
func TestChurnOverBudgetInsideTheSlot(t *testing.T) {
	shortBudgets(t)
	c, events, path := testChurner(t)
	c.Create = func(*gocql.ClusterConfig) (*gocql.Session, error) { time.Sleep(30 * time.Millisecond); return nil, nil }
	var closes atomic.Int32
	c.CloseSession = func(*cell.Session) { closes.Add(1); time.Sleep(30 * time.Millisecond) }
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := c.Churn(ctx, chaos.Slot{ID: "C1"})
	require.Error(t, err)
	require.NotErrorIs(t, err, chaos.ErrChurnOverrun, "everything finished inside the slot")
	require.Contains(t, err.Error(), "CreateSession did not return within")
	require.Contains(t, err.Error(), "close did not return within")
	require.False(t, c.Busy())
	require.EqualValues(t, 1, closes.Load())
	res := c.Residue()
	require.True(t, res[0].Measured, "the residue is measured although both steps overran their budgets")
	c.Sampler.mu.Lock()
	require.Empty(t, c.Sampler.sessions, "the session left the sampler")
	c.Sampler.mu.Unlock()
	require.NoError(t, events.Close())
	require.Equal(t, []string{"create", "late-session", "close", "late-close", "residue"}, steps(t, path))
}

// A slot deadline during the grace period hands the residue to an owner and reports an overrun (Codex K02).
func TestChurnSlotEndsDuringGrace(t *testing.T) {
	shortBudgets(t)
	chaos.ChurnBudget.Grace = 80 * time.Millisecond
	c, events, path := testChurner(t)
	c.Create = func(*gocql.ClusterConfig) (*gocql.Session, error) { time.Sleep(15 * time.Millisecond); return nil, nil }
	c.CloseSession = func(*cell.Session) {}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, c.Churn(ctx, chaos.Slot{ID: "C1"}), chaos.ErrChurnOverrun)
	require.True(t, c.Busy())
	require.Eventually(t, func() bool { return !c.Busy() }, 2*time.Second, time.Millisecond)
	require.True(t, c.Residue()[0].Measured)
	require.NoError(t, events.Close())
	require.Equal(t, []string{"create", "late-session", "close", "residue"}, steps(t, path))
}

// The pid watcher publishes a changed pid file to resources.json within its interval, and stops when told (Codex J05).
func TestPIDWatcherPublishesChanges(t *testing.T) {
	cfgDir, dir := t.TempDir(), t.TempDir()
	cl := ccmctl.New(ccmctl.Config{ConfigDir: cfgDir}).Cluster("gocql_soak_c50p5")
	writePID := func(node string, pid int) {
		require.NoError(t, os.MkdirAll(filepath.Join(cfgDir, "gocql_soak_c50p5", node), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(cfgDir, "gocql_soak_c50p5", node, "cassandra.pid"), []byte(strconv.Itoa(pid)), 0o644))
	}
	writePID("node1", 100)
	r := &cellRun{dir: dir, cl: cl, nodes: []string{"node1", "node2"}, logf: func(string, ...any) {}}
	read := func() map[string]int {
		var res artifact.Resources
		raw, err := os.ReadFile(filepath.Join(dir, "resources.json"))
		if err != nil {
			return nil
		}
		require.NoError(t, json.Unmarshal(raw, &res))
		return res.NodePIDs
	}
	r.watchPIDs()
	require.Eventually(t, func() bool { return read()["node1"] == 100 }, 3*time.Second, 10*time.Millisecond)
	writePID("node2", 200) // a node starting while ccm still waits
	writePID("node1", 101) // a restart
	require.Eventually(t, func() bool { p := read(); return p["node1"] == 101 && p["node2"] == 200 }, 3*time.Second, 10*time.Millisecond)
	r.stopWatchingPIDs()
	writePID("node1", 102)
	time.Sleep(1500 * time.Millisecond)
	require.Equal(t, 101, read()["node1"], "a stopped watcher publishes nothing")
	r.stopWatchingPIDs() // idempotent
}

// Each 5 s slice is sealed and written once; the flush writes the rest, skipping empty slices (Codex J08, PLAN §51.2).
func TestSamplerLatencySlices(t *testing.T) {
	path := filepath.Join(t.TempDir(), "samples.jsonl")
	out, err := artifact.OpenJSONL(path)
	require.NoError(t, err)
	epoch := time.Now()
	lat := probe.NewLiveLatency(epoch, 5*time.Second)
	for _, sec := range []int{1, 7, 17} {
		lat.Observe("read", epoch.Add(time.Duration(sec)*time.Second), time.Millisecond, false)
	}
	s := &Sampler{Out: out}
	s.Start(epoch, &cell.Session{ID: "primary"}, nil, lat)
	s.writeLatency(2)
	s.writeLatency(2)
	s.FlushLatency()
	require.NoError(t, out.Close())
	var slices []float64
	for _, l := range readLines(t, path) {
		slices = append(slices, l["slice"].(float64))
	}
	require.Equal(t, []float64{0, 1, 3}, slices, "slice 2 had no observation; nothing is written twice")
}

// A tick seals a slice only one full slice after it ends; an observation filed after its slice was sealed
// is counted late and never written (PLAN §51.2).
func TestSamplerSealsWithGrace(t *testing.T) {
	require.Equal(t, -1, sealBound(4*time.Second))
	require.Equal(t, 0, sealBound(5*time.Second+6*time.Millisecond), "the tick just after slice 0 ends seals nothing")
	require.Equal(t, 0, sealBound(9999*time.Millisecond))
	require.Equal(t, 1, sealBound(10*time.Second), "slice 0 is sealed 5 s after it ends")
	require.Equal(t, 3, sealBound(21*time.Second), "a late tick seals every slice it owes")

	path := filepath.Join(t.TempDir(), "samples.jsonl")
	out, err := artifact.OpenJSONL(path)
	require.NoError(t, err)
	epoch := time.Now()
	lat := probe.NewLiveLatency(epoch, CheapInterval)
	s := &Sampler{Out: out}
	s.Start(epoch, &cell.Session{ID: "primary"}, nil, lat)
	lat.Observe("read", epoch.Add(4*time.Second), time.Millisecond, false)
	s.writeLatency(sealBound(5*time.Second + 6*time.Millisecond)) // the old tick would have written slice 0 here
	lat.Observe("read", epoch.Add(4900*time.Millisecond), time.Millisecond, false)
	s.writeLatency(sealBound(10 * time.Second))
	lat.Observe("read", epoch.Add(3*time.Second), time.Millisecond, false) // a worker stalled past the grace
	s.FlushLatency()
	require.NoError(t, out.Close())
	lines := readLines(t, path)
	require.Len(t, lines, 1)
	require.EqualValues(t, 0, lines[0]["slice"])
	require.EqualValues(t, 2, lines[0]["classes"].(map[string]any)["read"].(map[string]any)["n"], "both on-time observations")
	require.Equal(t, map[string]int64{"read": 1}, lat.Late())
}

// blockingStop is a load whose Stop waits for release, like a driver call that ignores its deadline.
type blockingStop struct{ release chan struct{} }

func (b blockingStop) Stop() { <-b.release }

// A load drain still blocked at the slot deadline returns an overrun at once; the owner then closes once,
// measures the residue once and removes the session once (Codex L01).
func TestChurnBlockedLoadDrain(t *testing.T) {
	shortBudgets(t)
	chaos.ChurnBudget.Load = 10 * time.Millisecond
	c, events, path := testChurner(t)
	c.Create = func(*gocql.ClusterConfig) (*gocql.Session, error) { return nil, nil }
	var closes atomic.Int32
	c.CloseSession = func(*cell.Session) { closes.Add(1) }
	release := make(chan struct{})
	var loaded atomic.Bool
	c.StartLoad = func(context.Context, *workload.Env, float64, int, workload.Mix, uint64) (Stopper, error) {
		loaded.Store(true)
		return blockingStop{release}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	start := time.Now()
	require.ErrorIs(t, c.Churn(ctx, chaos.Slot{ID: "C1"}), chaos.ErrChurnOverrun)
	require.Less(t, time.Since(start), 500*time.Millisecond, "the slot returns at its deadline")
	require.True(t, loaded.Load(), "the normal path ran the load")
	require.True(t, c.Busy())
	c.Sampler.mu.Lock()
	require.NotNil(t, c.Sampler.sessions["aux0"], "the session stays sampled while its cleanup is pending")
	c.Sampler.mu.Unlock()
	close(release)
	require.Eventually(t, func() bool { return !c.Busy() }, 2*time.Second, time.Millisecond)
	require.EqualValues(t, 1, closes.Load())
	require.True(t, c.Residue()[0].Measured)
	c.Sampler.mu.Lock()
	require.Empty(t, c.Sampler.sessions)
	c.Sampler.mu.Unlock()
	require.NoError(t, events.Close())
	require.Equal(t, []string{"create", "load", "late-close", "residue"}, steps(t, path))
}

// The normal path: construct, load, Close, grace, residue, all inside the slot.
func TestChurnNormalPath(t *testing.T) {
	shortBudgets(t)
	chaos.ChurnBudget.Load = 10 * time.Millisecond
	c, events, path := testChurner(t)
	c.Create = func(*gocql.ClusterConfig) (*gocql.Session, error) { return nil, nil }
	var closes atomic.Int32
	c.CloseSession = func(*cell.Session) { closes.Add(1) }
	c.StartLoad = func(context.Context, *workload.Env, float64, int, workload.Mix, uint64) (Stopper, error) {
		release := make(chan struct{})
		close(release)
		return blockingStop{release}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, c.Churn(ctx, chaos.Slot{ID: "C1"}))
	require.False(t, c.Busy())
	require.EqualValues(t, 1, closes.Load())
	require.True(t, c.Residue()[0].Measured)
	require.Empty(t, c.Residue()[0].Failures)
	require.NoError(t, events.Close())
	require.Equal(t, []string{"create", "load", "close", "residue"}, steps(t, path))
}

// A residue probe still running at the slot deadline returns an overrun; the churn stays Busy until it ends (Codex L01).
func TestChurnBlockedResidueProbe(t *testing.T) {
	shortBudgets(t)
	c, events, _ := testChurner(t)
	c.Create = func(*gocql.ClusterConfig) (*gocql.Session, error) { time.Sleep(15 * time.Millisecond); return nil, nil }
	c.CloseSession = func(*cell.Session) {}
	release := make(chan struct{})
	c.Measure = func(string) (bool, int, int, map[string]int, error) {
		<-release
		return true, 0, 0, map[string]int{}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	require.ErrorIs(t, c.Churn(ctx, chaos.Slot{ID: "C1"}), chaos.ErrChurnOverrun)
	require.Less(t, time.Since(start), 500*time.Millisecond)
	require.True(t, c.Busy())
	close(release)
	require.Eventually(t, func() bool { return !c.Busy() }, 2*time.Second, time.Millisecond)
	require.True(t, c.Residue()[0].Measured)
	require.Contains(t, c.Residue()[0].Failures, lateNote, "a measurement ending after the slot is late (Codex M04)")
	require.NoError(t, events.Close())
}

// dev3's windows: V1 F-stop and V2 F-pause, [install, done + 10 s] in seconds since the epoch.
var dev3Windows = []gate.Interval{{Start: 1050.000031117, End: 1213.167097167}, {Start: 1650.000529997, End: 1723.231043315}}

// A real 45-minute run's health record is complete; truncating it or hiding a node between the faults is not,
// although both gaps touch a fault window (Codex M01, M02).
func TestHealthGapsOnRealData(t *testing.T) {
	tp, gc, err := LoadHealth("testdata/dev3-health.jsonl")
	require.NoError(t, err)
	require.Len(t, tp, 41)
	require.Empty(t, HealthGaps(tp, gc, dev3Windows, 2700, testNodes), "the healthy record is complete")

	// M01: only the first 18 rounds survive; the lost 28 minutes touch both windows but are mostly steady.
	var gc18 []gate.GCStatsSample
	for _, g := range gc {
		if g.T <= tp[17].T {
			gc18 = append(gc18, g)
		}
	}
	require.NotEmpty(t, HealthGaps(tp[:18], gc18, dev3Windows, 2700, testNodes))

	// M02: node2 missing from the tpstats of rounds 19–25, whose ends touch the windows.
	hidden := slices.Clone(tp)
	for i := 18; i <= 24; i++ {
		d := maps.Clone(hidden[i].Dropped)
		delete(d, "node2")
		hidden[i].Dropped = d
	}
	gaps := HealthGaps(hidden, gc, dev3Windows, 2700, testNodes)
	require.Len(t, gaps, 1)
	require.Contains(t, gaps[0], "node2 tpstats")

	// N01: a completed read whose result is lost leaves its interval uncovered; the next reading cannot give it back.
	var lost []gate.GCStatsSample
	for _, g := range gc {
		if !(g.Node == "node2" && g.Start > tp[10].Start && g.T <= tp[10].T) {
			lost = append(lost, g)
		}
	}
	require.Len(t, lost, len(gc)-1)
	gaps = HealthGaps(tp, lost, dev3Windows, 2700, testNodes)
	require.Len(t, gaps, 1)
	require.Contains(t, gaps[0], "node2 gcstats")
}

// A slow prime read bounds the evidence: a first regular reading chaining to it is complete (Codex N02);
// a restart inside a window breaks the chain legitimately; a lost read does not.
func TestGCChain(t *testing.T) {
	nodes := []string{"node3"}
	tp := []gate.TPStatsSample{
		{Start: 0, T: 60, Dropped: map[string]float64{"node3": 0}},
		{Start: 120, T: 180, Dropped: map[string]float64{"node3": 0}},
	}
	gc := []gate.GCStatsSample{
		{Start: 50, T: 60, Node: "node3", Prime: true, OK: true, PID: 7},
		{Start: 170, T: 180, Node: "node3", IntervalMs: 120000, OK: true, PID: 7},
	}
	require.Empty(t, HealthGaps(tp, gc, nil, 200, nodes))

	restarted := slices.Clone(gc)
	restarted[1].PID, restarted[1].IntervalMs = 8, 20000 // the JVM started at 150
	require.Empty(t, HealthGaps(tp, restarted, []gate.Interval{{Start: 70, End: 155}}, 200, nodes))

	skipped := slices.Clone(gc)
	skipped[1].IntervalMs = 60000 // counters reset at 110 by a read whose result was lost
	require.NotEmpty(t, HealthGaps(tp, skipped, nil, 200, nodes))
}

// Codex P01: a restart excuses only the new JVM's reset, never reads lost before the fault.
func TestHealthGapsAcrossARestart(t *testing.T) {
	tp, gc, err := LoadHealth("testdata/dev3-health.jsonl")
	require.NoError(t, err)
	without := func(rounds ...int) []gate.GCStatsSample {
		var out []gate.GCStatsSample
		for _, g := range gc {
			drop := false
			for _, i := range rounds {
				drop = drop || (g.Node == "node2" && g.Start >= tp[i].Start && g.Start <= tp[i].T)
			}
			if !drop {
				out = append(out, g)
			}
		}
		return out
	}
	require.Len(t, HealthGaps(tp, without(17), dev3Windows, 2700, testNodes), 1, "the last read before node2's restart")
	require.Len(t, HealthGaps(tp, without(10, 11, 12, 13, 14, 15, 16, 17), dev3Windows, 2700, testNodes), 8)
}

// Codex P02: a slow but successful prime anchors the chain; nothing before it is owed.
func TestHealthGapsSlowPrime(t *testing.T) {
	tp := []gate.TPStatsSample{
		{Start: 0, T: 186, Dropped: map[string]float64{"node3": 0}},
		{Start: 246, T: 432, Dropped: map[string]float64{"node3": 0}},
	}
	gc := []gate.GCStatsSample{
		{Start: 155, T: 186, Node: "node3", Prime: true, OK: true, PID: 1},
		{Start: 401, T: 432, Node: "node3", IntervalMs: 246000, OK: true, PID: 1},
	}
	require.Empty(t, HealthGaps(tp, gc, nil, 500, []string{"node3"}))
}

// Codex P03, Q01: at the end of the workload, what a round already read counts, a violation included;
// the nodes and reads it did not finish are neither evaluated nor missing.
func TestHealthShutdownRound(t *testing.T) {
	tpData, err := os.ReadFile("../probe/testdata/tpstats-5.0.3.txt")
	require.NoError(t, err)
	gcData := "Interval (ms) Max GC Elapsed (ms)Total GC Elapsed (ms)\n 60000 1000 1200\n"
	dir := t.TempDir()
	out, err := artifact.OpenJSONL(filepath.Join(dir, "health.jsonl"))
	require.NoError(t, err)
	raw, err := artifact.OpenJSONL(filepath.Join(dir, "nodetool.jsonl"))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	h := &Health{Nodes: testNodes, Windows: &chaos.Windows{}, Out: out, Raw: raw, epoch: time.Now(),
		PID: func(string) (int, error) { return 1, nil },
		Nodetool: func(c context.Context, node string, args ...string) (string, error) {
			if node == "node2" && args[0] == "gcstats" {
				cancel() // the workload ends while node2's gcstats runs
				return "", c.Err()
			}
			if args[0] == "tpstats" {
				return string(tpData), nil
			}
			return gcData, nil // node1 completed with a 1000 ms pause
		},
	}
	h.round(ctx, false)
	tps, gcs := h.Samples()
	require.Len(t, tps, 1)
	require.Contains(t, tps[0].Dropped, "node1")
	require.Contains(t, tps[0].Dropped, "node2", "node2's tpstats finished before the cutoff")
	require.True(t, tps[0].Skipped["node3"], "node3 was never read")
	require.Len(t, gcs, 2)
	require.True(t, gcs[0].OK)
	require.True(t, gcs[1].Cancelled)
	require.NoError(t, out.Close())
	lTP, lGC, err := LoadHealth(filepath.Join(dir, "health.jsonl"))
	require.NoError(t, err)
	require.Equal(t, gcs, lGC, "replay keeps the same readings")
	require.Equal(t, tps[0].Skipped, lTP[0].Skipped)

	// The completed pause still fails G16; the skipped and cancelled reads add nothing missing.
	c := green()
	c.TP = c.TP[:len(c.TP)-1] // the 7200 s round starts at the horizon; the 7140 s one is the interrupted round
	last := c.TP[len(c.TP)-1]
	c.TP = append(c.TP[:len(c.TP)-1], gate.TPStatsSample{Start: last.Start, T: last.T,
		PID: map[string]int{"node1": 1, "node2": 1}, Dropped: map[string]float64{"node1": 0, "node2": 0}, Skipped: map[string]bool{"node3": true}})
	var gc []gate.GCStatsSample
	for _, g := range c.GC {
		if g.Start < last.Start {
			gc = append(gc, g)
		}
	}
	gc = append(gc,
		gate.GCStatsSample{Start: last.Start + 1, T: last.Start + 2, Node: "node1", IntervalMs: 60000, MaxPauseMs: 1000, TotalMs: 1200, OK: true, PID: 1},
		gate.GCStatsSample{Start: last.Start + 1, T: last.Start + 2, Node: "node2", Cancelled: true, PID: 1})
	c.GC = gc
	v := Evaluate(c)
	require.Equal(t, gate.StatusFail, statusOf(v, "G16"), "the pause read before the cutoff counts")
	require.True(t, EvidenceOf(v, c.Incomplete).Complete, "%v", EvidenceOf(v, c.Incomplete).Problems)

	// Readings started after the horizon are not evidence either way.
	c = green()
	c.TP = append(c.TP, gate.TPStatsSample{Start: 7201, T: 7203, PID: map[string]int{}, Dropped: map[string]float64{"node1": math.NaN()}})
	c.GC = append(c.GC, gate.GCStatsSample{Start: 7202, T: 7203, Node: "node1"})
	require.True(t, EvidenceOf(Evaluate(c), c.Incomplete).Complete)
}

// Codex Q02: after a failed prime, the first successful read only establishes the baseline:
// it is never evaluated, and the time before it is missing evidence.
func TestHealthFailedPrime(t *testing.T) {
	c := green()
	for i := range c.GC {
		if c.GC[i].Node == "node2" && c.GC[i].Prime {
			c.GC[i].OK = false
		}
		if c.GC[i].Node == "node2" && c.GC[i].Start > 60 && c.GC[i].Start < 62 {
			c.GC[i].MaxPauseMs = 9000 // a setup-length interval with a boot pause in it
		}
	}
	v := Evaluate(c)
	require.NotEqual(t, gate.StatusFail, statusOf(v, "G16"), "the rebased read is not evaluated")
	ev := EvidenceOf(v, c.Incomplete)
	require.False(t, ev.Complete)
	// One problem: the stretch the rebased read cannot vouch for; a failed prime is not a missing G16 interval itself.
	require.Len(t, ev.Problems, 1, "%v", ev.Problems)
	require.Contains(t, ev.Problems[0], "node2 gcstats")
}

// Codex R01: a round whose tpstats came before the horizon and whose gcstats started after it keeps that read as cancelled,
// so the round is complete.
func TestHealthGCAfterHorizon(t *testing.T) {
	c := green()
	c.TP = c.TP[:len(c.TP)-1] // drop the round at 7200 s
	c.TP = append(c.TP, gate.TPStatsSample{Start: 7199, T: 7200.5, PID: map[string]int{"node1": 1, "node2": 1, "node3": 1},
		Dropped: map[string]float64{"node1": 0, "node2": 0, "node3": 0}})
	var gc []gate.GCStatsSample
	for _, g := range c.GC {
		if g.Start < 7199 {
			gc = append(gc, g)
		}
	}
	gc = append(gc,
		gate.GCStatsSample{Start: 7199.5, T: 7199.6, Node: "node1", IntervalMs: 59000, MaxPauseMs: 5, TotalMs: 60, OK: true, PID: 1},
		gate.GCStatsSample{Start: 7199.8, T: 7199.9, Node: "node2", IntervalMs: 59000, MaxPauseMs: 5, TotalMs: 60, OK: true, PID: 1},
		gate.GCStatsSample{Start: 7200.001, T: 7200.4, Node: "node3", Cancelled: true, PID: 1})
	c.GC = gc
	require.True(t, EvidenceOf(Evaluate(c), c.Incomplete).Complete, "%v", EvidenceOf(Evaluate(c), c.Incomplete).Problems)
}

// Codex R02: a failed prime's uncertainty ends with its JVM; the replacement JVM's reads are evaluated.
func TestRebaseEndsWithTheJVM(t *testing.T) {
	gc := []gate.GCStatsSample{
		{Start: 1, T: 2, Node: "node2", Prime: true, PID: 1},
		{Start: 61, T: 62, Node: "node2", PID: 1},
		{Start: 1221, T: 1222, Node: "node2", PID: 2},
		{Start: 1281, T: 1282, Node: "node2", IntervalMs: 60000, MaxPauseMs: 1000, OK: true, PID: 2},
	}
	out := rebase(gc)
	require.False(t, out[3].Rebased, "a read in the replacement JVM is evaluated as usual")
	r, _ := gate.G16(nil, out, []gate.Interval{{Start: 1050, End: 1210}}, thresholds())
	require.Equal(t, gate.StatusFail, r.Status, "its 1000 ms pause counts")

	same := slices.Clone(gc)
	same[2].PID, same[3].PID = 1, 1
	same[2].OK = true
	require.True(t, rebase(same)[2].Rebased, "within the prime's JVM the first success is the baseline")
}

// The driver's capped re-prepare error is row 7, unprepared: allowed inside a window, unexpected outside (PLAN §39).
func TestRecorderUnpreparedRow(t *testing.T) {
	dir := t.TempDir()
	errs, err := artifact.OpenJSONL(filepath.Join(dir, "errors.jsonl"))
	require.NoError(t, err)
	events, err := artifact.OpenJSONL(filepath.Join(dir, "events.jsonl"))
	require.NoError(t, err)
	windows := &chaos.Windows{}
	epoch := time.Now()
	windows.Open("M1", chaos.FaultStop, 2, epoch.Add(100*time.Second))
	windows.Close("M1", epoch.Add(200*time.Second))
	rec := NewRecorder(errs, events, windows, 4)
	rec.SetEpoch(epoch)
	capped := func() error {
		var server error = &gocql.RequestErrUnprepared{}
		return fmt.Errorf("gocql: failed to execute prepared statement after 6 re-prepare attempts: %w", server)
	}
	rec.Error(workload.ErrorRecord{Time: epoch.Add(150 * time.Second), Session: "primary", Class: workload.ClassChurn, Err: capped()})
	rec.Error(workload.ErrorRecord{Time: epoch.Add(300 * time.Second), Session: "primary", Class: workload.ClassChurn, Err: capped()})
	require.NoError(t, errs.Close())
	require.NoError(t, events.Close())

	require.Equal(t, map[string]int64{"unprepared": 2}, rec.ClassCounts())
	require.EqualValues(t, 1, rec.Unexpected(), "only the one outside a window")
	lines := readLines(t, filepath.Join(dir, "errors.jsonl"))
	require.Len(t, lines, 2)
	require.Equal(t, true, lines[0]["admitted"])
	require.Equal(t, "M1", lines[0]["window"])
	require.Equal(t, false, lines[1]["admitted"])
}
