package cellrun

import (
	"cmp"
	"fmt"
	"slices"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/config"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// clusterHosts is the node count the G2 total bound allows connections for.
const clusterHosts = 3

// Collected is everything a cell recorded that the gates read.
type Collected struct {
	// Mode, Timeline and G15Scale come from the effective configuration.
	Mode     gate.Mode
	Timeline config.Timeline
	G15Scale float64
	// Thresholds is gates.json; missing values make the gates that need them invalid-config.
	Thresholds gate.Thresholds
	// InvalidConfig, Incomplete and FixtureInvalid are the run's non-gate reasons.
	InvalidConfig, Incomplete, FixtureInvalid []string
	// WorkloadSeconds is how long the workload ran; Ran is false when it never started.
	WorkloadSeconds float64
	Ran             bool

	// G0 lists the cell identity violations; G0Checked is false when setup failed before the check.
	G0        []string
	G0Checked bool
	// Baseline is baseline₀; Profiles every profile sample, the final one included.
	Baseline Baseline
	Profiles []Profile
	// Dials9042 lists every dial that bypassed a proxy (G5).
	Dials9042 []string
	// G8 is the recorder's result.
	G8 gate.Result
	// Report is the executor's report; Windows the fault windows in seconds since the epoch.
	Report  chaos.Report
	Windows []gate.Interval
	// Oracles (G10).
	Register, LWT, Runtime []string
	Uncertain              int64
	// Logs is the G7 counts summed over every session.
	Logs gate.LogCounts
	// Progress is the primary's per-second offered and completed operations (G11).
	Progress gate.Progress
	// Classes is every class the primary's mix offers (G11, G14).
	Classes []string
	// WarmupP99 and CooldownP99 are the primary's latency per class, seconds (G14).
	WarmupP99, CooldownP99 map[string]float64
	// G12 is the primary's teardown; G12Checked false when it did not run.
	G12        gate.G12Input
	G12Checked bool
	// Churns is every churn's residue (G13).
	Churns []gate.ChurnResidue
	// Settlement and ShortDeadlineTimeouts feed G15.
	Settlement            workload.SettlementTotals
	ShortDeadlineTimeouts int
	// TP and GC are the fixture health samples (G16); Nodes the node names every round must cover.
	TP    []gate.TPStatsSample
	GC    []gate.GCStatsSample
	Nodes []string
}

// Evaluate runs every gate over what a cell recorded and decides its verdict (PLAN §6).
// When the workload never ran, only the gates that setup already decided are evaluated.
//
// Parameters:
//   - c: the recorded data
//
// Returns:
//   - gate.Verdict: the verdict, carrying every gate result
func Evaluate(c Collected) gate.Verdict {
	var gates []gate.Result
	if c.G0Checked {
		gates = append(gates, gate.Bool("G0", c.G0))
	}
	g16 := gate.Result{Gate: "G16", Status: gate.StatusNotEvaluated, Details: []string{"the workload did not run"}}
	if c.Ran {
		gates = append(gates, c.runGates()...)
		tp, gc := healthWithin(c.TP, c.GC, c.WorkloadSeconds)
		var missing int
		g16, missing = gate.G16(tp, gc, c.Windows, c.Thresholds)
		if missing > 0 {
			// G16 stays advisory, but a missing interval is still missing evidence for calibration.
			g16.Details = append(g16.Details, fmt.Sprintf("missing evidence: %d G16 intervals missing", missing))
		}
		for _, gap := range HealthGaps(tp, gc, c.Windows, c.WorkloadSeconds, c.Nodes) {
			g16.Details = append(g16.Details, "missing evidence: "+gap)
		}
	}
	return gate.Decide(gate.VerdictInput{
		Mode: c.Mode, Gates: gates, G16: g16, InvalidConfig: c.InvalidConfig, Incomplete: c.Incomplete,
		FixtureInvalid: c.FixtureInvalid, WorkloadSeconds: c.WorkloadSeconds,
	})
}

// minQuietCheckpoints is the fewest quiet checkpoints in the trend window that G2, G4, G5 and G6 accept:
// a night has at least one per done fault plus the cool-down's, a validation run at least three.
const minQuietCheckpoints = 2

// MinQuietCheckpoints is minQuietCheckpoints, for the calibration suitability check (PLAN §41.5).
const MinQuietCheckpoints = minQuietCheckpoints

// TrendSeries is what the trend-window gates read, built from the profiles exactly as the gates build it (PLAN §41.2).
type TrendSeries struct {
	// Warm and End bound the trend window, seconds since the epoch.
	Warm, End float64
	// Groups is each goroutine group's count over the trend profiles (G2).
	Groups map[string]gate.Series
	// Heap is the heap after GC over the trend profiles (G3).
	Heap gate.Series
	// QuietTotals and QuietFDs are the goroutine total and the fd count at quiet trend checkpoints (G2, G4).
	QuietTotals, QuietFDs gate.Series
	// QuietBalance is each session's stream balance at quiet trend checkpoints (G6).
	QuietBalance map[string]gate.Series
	// Quiet counts the quiet trend checkpoints.
	Quiet int
}

// seriesBuild is everything runGates derives from the profiles.
type seriesBuild struct {
	TrendSeries
	leaks   gate.Series
	r2      []string
	missing map[string][]string
	notes   map[string][]string
}

// Series returns the trend-window series the gates read.
//
// Returns:
//   - TrendSeries: the series, with the trend window's bounds
func (c Collected) Series() TrendSeries {
	return c.buildSeries().TrendSeries
}

func (c Collected) buildSeries() seriesBuild {
	warm, end := c.Timeline.Warmup.Seconds(), c.WorkloadSeconds
	// The trend window is [warm-up end, workload end]; the final sample, taken after the workload stopped,
	// only feeds G1 (Codex I14).
	trend := func(p Profile) bool { return p.T >= warm && p.T <= end && p.Reason != ReasonFinal }
	var leaks, heap, quietFDs, quietTotals gate.Series
	quietBalance := map[string]gate.Series{}
	groups := map[string]gate.Series{}
	var r2 []string
	var trendProfiles []Profile
	missing := map[string][]string{}
	miss := func(g, format string, args ...any) { missing[g] = append(missing[g], fmt.Sprintf(format, args...)) }
	quiet := 0
	for _, p := range c.Profiles {
		leaks = append(leaks, gate.Point{T: p.T, V: float64(p.Leaks)})
		if p.Leaks < 0 {
			miss("G1", "t=%.0fs: no goroutineleak profile", p.T)
		}
		if !trend(p) {
			continue
		}
		trendProfiles = append(trendProfiles, p)
		if p.Groups == nil {
			miss("G2", "t=%.0fs: no goroutine dump", p.T)
		}
		if p.HeapMiB >= 0 {
			heap = append(heap, gate.Point{T: p.T, V: p.HeapMiB})
		} else {
			miss("G3", "t=%.0fs: no heap measurement", p.T)
		}
		if !p.Quiet {
			continue
		}
		quiet++
		if p.Groups != nil {
			quietTotals = append(quietTotals, gate.Point{T: p.T, V: float64(p.Total)})
		}
		if p.FDs >= 0 {
			quietFDs = append(quietFDs, gate.Point{T: p.T, V: float64(p.FDs)})
		} else {
			miss("G4", "t=%.0fs: no fd count at a quiet checkpoint", p.T)
		}
		if b, ok := p.Balance["primary"]; ok {
			quietBalance["primary"] = append(quietBalance["primary"], gate.Point{T: p.T, V: float64(b)})
		} else {
			miss("G6", "t=%.0fs: no primary stream balance at a quiet checkpoint", p.T)
		}
		if !p.R2Checked {
			miss("G5", "t=%.0fs: R2 not checked at a quiet checkpoint", p.T)
		}
		for _, pr := range p.R2 {
			r2 = append(r2, fmt.Sprintf("t=%.0fs %s", p.T, pr))
		}
	}
	notes := map[string][]string{}
	if quiet < minQuietCheckpoints {
		for _, g := range []string{"G2", "G4", "G5", "G6"} {
			if c.Report.FaultsStopped != "" {
				// A failed fault keeps its window open to the end, so later checkpoints are rightly not quiet:
				// that is an exclusion, reported, not missing evidence (Codex J09); G9 or G13 already fails.
				notes[g] = append(notes[g], fmt.Sprintf("only %d quiet checkpoints: none are taken after %s", quiet, c.Report.FaultsStopped))
				continue
			}
			miss(g, "%d quiet checkpoints in the trend window, need %d", quiet, minQuietCheckpoints)
		}
	}
	for _, p := range trendProfiles {
		for name := range p.Groups {
			groups[name] = nil
		}
	}
	for name := range groups {
		for _, p := range trendProfiles {
			if p.Groups != nil {
				groups[name] = append(groups[name], gate.Point{T: p.T, V: float64(p.Groups[name])})
			}
		}
	}
	return seriesBuild{
		TrendSeries: TrendSeries{Warm: warm, End: end, Groups: groups, Heap: heap, QuietTotals: quietTotals, QuietFDs: quietFDs, QuietBalance: quietBalance, Quiet: quiet},
		leaks:       leaks, r2: r2, missing: missing, notes: notes,
	}
}

func (c Collected) runGates() []gate.Result {
	b := c.buildSeries()
	warm, end := b.Warm, b.End
	leaks, heap, quietFDs, quietBalance := b.leaks, b.Heap, b.QuietFDs, b.QuietBalance
	r2, missing, notes := b.r2, b.missing, b.notes
	miss := func(g, format string, args ...any) { missing[g] = append(missing[g], fmt.Sprintf(format, args...)) }
	if c.G12Checked {
		leaks = append(leaks, gate.Point{T: end, V: float64(c.G12.Leaks)})
	}
	if len(c.Churns) != c.Report.ChurnExecuted {
		miss("G13", "%d churn residues for %d executed churn slots", len(c.Churns), c.Report.ChurnExecuted)
	}
	// Presence is checked here, independent of thresholds,
	// so a calibration run without gates.json still reports its missing measurements in verdict.json's evidence field (Codex K01).
	for _, ch := range c.Churns {
		if !ch.Measured {
			miss("G13", "%s: residue not measured", ch.Slot)
		}
	}
	switch {
	case !c.G12Checked:
		miss("G12", "the teardown was not measured")
	default:
		if c.G12.Baseline == nil || c.G12.After == nil {
			miss("G12", "no goroutine dump before the primary opened or after Close")
		}
		if c.G12.FDs < 0 || c.G12.FD0 < 0 {
			miss("G12", "no fd count before the primary opened or after Close")
		}
		if c.G12.ProxySockets < 0 {
			miss("G12", "process-owned sockets could not be read after Close")
		}
		if c.G12.Leaks < 0 {
			miss("G12", "no goroutineleak profile after Close")
		}
	}
	if !c.cooldownContaminated() {
		for _, class := range c.Classes {
			if _, ok := c.WarmupP99[class]; !ok {
				miss("G14", "class %s has no warm-up p99", class)
			}
			if _, ok := c.CooldownP99[class]; !ok {
				miss("G14", "class %s has no cool-down p99", class)
			}
		}
	}

	out := []gate.Result{
		gate.G1(leaks),
		gate.G2(c.g2Input(b.TrendSeries), c.Thresholds),
		gate.G3(heap, warm, end, c.Thresholds),
		gate.G4(quietFDs, float64(c.Baseline.FDs), c.Thresholds),
		gate.Bool("G5", append(slices.Clone(c.Dials9042), r2...)),
		gate.G6(quietBalance, c.Thresholds),
		gate.G7(c.Logs, c.Thresholds),
		c.g8(),
		gate.Bool("G9", c.Report.G9Failures),
		gate.G10(c.Register, c.LWT, c.Runtime, c.Uncertain, c.Thresholds),
		gate.G11(c.Progress, FaultFree(warm, end, c.Windows), c.Classes),
	}
	if c.G12Checked {
		out = append(out, gate.G12(c.G12, c.Thresholds))
	} else {
		out = append(out, gate.Bool("G12", []string{"teardown was not measured"}))
	}
	out = append(out,
		gate.G13(c.Churns, c.Report.ChurnFailures, c.Thresholds),
		gate.G14(c.WarmupP99, c.CooldownP99, c.Classes, c.cooldownContaminated(), c.Thresholds),
		gate.G15(gate.Coverage{
			MandatoryFaults: c.Report.MandatoryFaults, MandatoryExecuted: c.Report.MandatoryExecuted,
			ChurnSlots: c.Report.ChurnSlots, ChurnExecuted: c.Report.ChurnExecuted,
			ProvenPrefetch: c.Settlement.PrefetchProven, ProvenSpeculation: c.Settlement.SpeculationProven,
			ShortDeadlineTimeouts: c.ShortDeadlineTimeouts,
		}, c.G15Scale),
	)
	for i, r := range out {
		r.Details = append(r.Details, notes[r.Gate]...)
		out[i] = withMissing(r, missing[r.Gate])
	}
	return out
}

// withMissing makes a gate fail when evidence it needs is missing (Codex I05):
// a measurement that failed is left out of the series, never read as zero,
// and its absence must not let the gate pass.
// A gate that is already invalid-config or not evaluated keeps its status and records the missing evidence too.
func withMissing(r gate.Result, missing []string) gate.Result {
	if len(missing) == 0 {
		return r
	}
	// A gate may already name the same missing measurement; each is counted once (Codex L03).
	details := slices.Clone(r.Details)
	for _, m := range missing {
		if d := "missing evidence: " + m; !slices.Contains(details, d) {
			details = append(details, d)
		}
	}
	if r.Status == gate.StatusInvalidConfig || r.Status == gate.StatusNotEvaluated {
		return gate.Result{Gate: r.Gate, Status: r.Status, Details: details}
	}
	return gate.Result{Gate: r.Gate, Status: gate.StatusFail, Details: details}
}

// g8 returns the recorder's G8 result, failing closed when none was recorded.
func (c Collected) g8() gate.Result {
	if c.G8.Gate == "" {
		return gate.Bool("G8", []string{"G8 was not recorded"})
	}
	return c.G8
}

// cooldownContaminated reports whether any window reaches into the cool-down (PLAN §5.4):
// only a fault that already failed G9 can, since admission keeps the rest out.
func (c Collected) cooldownContaminated() bool {
	cool := gate.Interval{Start: c.Timeline.Cooldown.Seconds(), End: c.WorkloadSeconds}
	for _, w := range c.Windows {
		if w.Start <= cool.End && cool.Start <= w.End {
			return true
		}
	}
	return false
}

// FaultFree returns the parts of [from, to] that no window covers, in order.
//
// Parameters:
//   - from, to: the span, seconds since the epoch
//   - windows: the fault windows
//
// Returns:
//   - []gate.Interval: the uncovered spans
func FaultFree(from, to float64, windows []gate.Interval) []gate.Interval {
	ws := slices.Clone(windows)
	slices.SortFunc(ws, func(a, b gate.Interval) int { return cmp.Compare(a.Start, b.Start) })
	var out []gate.Interval
	cur := from
	for _, w := range ws {
		if w.End < cur {
			continue
		}
		if w.Start > to {
			break
		}
		if w.Start > cur {
			out = append(out, gate.Interval{Start: cur, End: w.Start})
		}
		cur = max(cur, w.End)
	}
	if cur < to {
		out = append(out, gate.Interval{Start: cur, End: to})
	}
	return out
}

// healthWithin keeps the health readings whose commands started before the end of the workload,
// the measurement horizon; anything later belongs to the shutdown (Codex P03).
func healthWithin(tp []gate.TPStatsSample, gc []gate.GCStatsSample, end float64) ([]gate.TPStatsSample, []gate.GCStatsSample) {
	var tpOut []gate.TPStatsSample
	for _, r := range tp {
		if r.Start < end {
			tpOut = append(tpOut, r)
		}
	}
	// A gcstats read that started after the horizon, in a round whose tpstats came before it, is kept as cancelled:
	// never evaluated, but still the round's read, so the round is not short of one (Codex R01).
	gcOut := slices.Clone(gc)
	for i := range gcOut {
		if gcOut[i].Start >= end {
			gcOut[i].Cancelled, gcOut[i].OK = true, false
		}
	}
	return tpOut, rebase(gcOut)
}

// rebase marks, per node, the first successful read after a failed prime as the baseline (Codex Q02):
// the failed prime may not have reset the counters, so that read can cover the JVM's whole life, setup included.
// The uncertainty belongs to the prime's JVM: a new pid ends it, and the new JVM's reads are evaluated as usual (Codex R02).
func rebase(gc []gate.GCStatsSample) []gate.GCStatsSample {
	out := slices.Clone(gc)
	slices.SortStableFunc(out, func(a, b gate.GCStatsSample) int { return cmp.Compare(a.Start, b.Start) })
	pending := map[string]int{} // node → the pid whose prime failed
	for i := range out {
		g := &out[i]
		pid, uncertain := pending[g.Node]
		switch {
		case g.Prime && !g.OK:
			pending[g.Node] = g.PID
		case uncertain && pid != 0 && g.PID != 0 && g.PID != pid:
			delete(pending, g.Node)
		case uncertain && g.OK && !g.Cancelled:
			g.Rebased = true
			delete(pending, g.Node)
		}
	}
	return out
}

// maxHealthGap is the longest steady-state stretch without a G16 round: two read intervals plus the rounds' own time.
const maxHealthGap = 2*HealthInterval + 30*time.Second

// gcResetSlack absorbs where inside a gcstats command the counters were reset:
// a reading's reported interval ends somewhere between its command's start and end.
const gcResetSlack = 5 * time.Second

// HealthGaps checks that G16's evidence covers the run, node by node (Codex L02, M01, M02, N01, N02, P01, P02).
// tpstats: the stretches between a node's readings, minus the fault windows, must each be at most maxHealthGap,
// the stretch before the first and after the last reading too.
// gcstats resets on every read, so its evidence is the chain of reported intervals, checked by gcChainGaps.
// Only the part of a stretch that a window covers is excused, never the whole stretch.
// A failed read is G16's own missing count, and a node missing from a single round too.
//
// Parameters:
//   - tp: the tpstats rounds, the prime round included
//   - gc: the gcstats readings, the prime reads included (Prime)
//   - windows: the fault windows, seconds since the epoch
//   - end: the end of the workload
//   - nodes: the node names
//
// Returns:
//   - []string: one entry per missing measurement or uncovered stretch; empty when the evidence is complete
func HealthGaps(tp []gate.TPStatsSample, gc []gate.GCStatsSample, windows []gate.Interval, end float64, nodes []string) []string {
	var gaps []string
	for _, n := range nodes {
		var tpObs, rounds []gate.Interval
		for _, r := range tp {
			if _, ok := r.Dropped[n]; ok {
				tpObs = append(tpObs, gate.Interval{Start: r.Start, End: r.T})
				rounds = append(rounds, gate.Interval{Start: r.Start, End: r.T})
			}
		}
		var reads []gate.GCStatsSample
		for _, g := range gc {
			if g.Node == n {
				reads = append(reads, g)
			}
		}
		gaps = append(gaps, coverageGaps(n+" tpstats", tpObs, windows, end)...)
		gaps = append(gaps, gcChainGaps(n, reads, rounds, windows, end)...)
	}
	return gaps
}

// gcStep is one step of a node's gcstats chain: a read, or a round whose read is missing.
type gcStep struct {
	read gate.GCStatsSample
	lost bool
}

// gcChainGaps checks one node's gcstats evidence.
// Every round that read the node's tpstats must hold its gcstats read too: one that does not lost a read,
// which ran and reset the counters, so the round's end becomes the chain's boundary (counted once, here).
// The chain starts at the node's prime read, or, without one, at the first reading's reported start.
// Within one JVM, each reading's reported interval must start where the previous step left off,
// within gcResetSlack; across a restart (a new pid), only the old JVM's unread tail outside the windows is checked,
// at the normal read cadence.
// The stretch after the last step must be short too.
func gcChainGaps(node string, reads []gate.GCStatsSample, rounds []gate.Interval, windows []gate.Interval, end float64) []string {
	var gaps []string
	steady := func(from, to, allowed float64) {
		for _, s := range FaultFree(from, to, windows) {
			if s.End-s.Start > allowed {
				gaps = append(gaps, fmt.Sprintf("no %s gcstats evidence in %.0f–%.0fs", node, s.Start, s.End))
			}
		}
	}
	var steps []gcStep
	for _, r := range reads {
		steps = append(steps, gcStep{read: r})
	}
	for _, rd := range rounds {
		found := false
		for _, r := range reads {
			found = found || (r.Start >= rd.Start && r.Start <= rd.End)
		}
		if !found {
			gaps = append(gaps, fmt.Sprintf("round at %.0fs has no %s gcstats reading", rd.Start, node))
			steps = append(steps, gcStep{read: gate.GCStatsSample{Start: rd.Start, T: rd.End}, lost: true})
		}
	}
	slices.SortFunc(steps, func(a, b gcStep) int { return cmp.Compare(a.read.Start, b.read.Start) })
	if len(steps) == 0 {
		steady(0, end, maxHealthGap.Seconds())
		return gaps
	}
	slack := gcResetSlack.Seconds()
	covered, pid := 0.0, 0
	for i, st := range steps {
		r := st.read
		from := r.Start - r.IntervalMs/1000 // the earliest the reported interval can start
		switch {
		case i == 0 && !st.lost && !r.Prime && r.OK:
			steady(0, from, slack) // no prime: the first reading must reach back to the start
		case i == 0:
		case r.Rebased:
			// The baseline after a failed prime: nothing before it is evidence (Codex Q02).
			steady(covered, r.T, slack)
		case st.lost || !r.OK:
			// A lost or failed read is already counted; it still reset the counters, so it is a boundary.
		case pid != 0 && r.PID != 0 && r.PID != pid:
			// A restart: the old JVM's unread tail is checked at the read cadence; its end lies in a window.
			steady(covered, from, maxHealthGap.Seconds())
		case from > covered+slack:
			steady(covered, from, slack)
		}
		covered = max(covered, r.T)
		if r.PID != 0 {
			pid = r.PID
		}
	}
	steady(covered, end, maxHealthGap.Seconds())
	return gaps
}

// coverageGaps lists the steady-state stretches longer than maxHealthGap between observations over [0, end].
func coverageGaps(what string, obs []gate.Interval, windows []gate.Interval, end float64) []string {
	obs = slices.Clone(obs)
	slices.SortFunc(obs, func(a, b gate.Interval) int { return cmp.Compare(a.Start, b.Start) })
	var gaps []string
	check := func(from, to float64) {
		for _, steady := range FaultFree(from, to, windows) {
			if steady.End-steady.Start > maxHealthGap.Seconds() {
				gaps = append(gaps, fmt.Sprintf("no %s reading in %.0f–%.0fs", what, steady.Start, steady.End))
			}
		}
	}
	prevEnd := 0.0
	for _, o := range obs {
		check(prevEnd, o.Start)
		prevEnd = max(prevEnd, o.End)
	}
	check(prevEnd, end)
	return gaps
}

func overlaps(iv gate.Interval, windows []gate.Interval) bool {
	for _, w := range windows {
		if iv.Start <= w.End && w.Start <= iv.End {
			return true
		}
	}
	return false
}

// SumLogs adds G7 counts.
//
// Parameters:
//   - counts: per-session counts
//
// Returns:
//   - gate.LogCounts: the sums
func SumLogs(counts ...gate.LogCounts) gate.LogCounts {
	var s gate.LogCounts
	for _, c := range counts {
		s.GoroutinePanicked += c.GoroutinePanicked
		s.NoHandler += c.NoHandler
		s.IterNotClosed += c.IterNotClosed
		s.SchemaAgreement += c.SchemaAgreement
		s.EventBufferFull += c.EventBufferFull
	}
	return s
}
