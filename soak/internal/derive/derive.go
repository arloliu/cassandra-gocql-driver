// Package derive derives gates.json from a calibration night (PLAN §41).
package derive

import (
	"cmp"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cellrun"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/config"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
)

// Windows of PLAN §41.4.
const (
	// SlopeWindow is the validation trend window's length, seconds: the per-window slopes are measured over it.
	SlopeWindow = float64((config.ValidationWorkload - config.ValidationWarmup) / time.Second)
	// WindowStep is the periodic profile cadence the slope windows step by, seconds.
	WindowStep = 300.0
	// MinWindowPoints is the fewest points a slope window needs; fewer and it is skipped.
	MinWindowPoints = 3
	// ChurnRun is how many consecutive churns make one kCh window: a validation run's churn slots.
	ChurnRun = 3
	// LatencyWarmup and LatencyCooldown are the validation run's warm-up and cool-down lengths, seconds (kL).
	LatencyWarmup   = float64(config.ValidationWarmup / time.Second)
	LatencyCooldown = float64((config.ValidationWorkload - config.ValidationCooldown) / time.Second)
	// Factor is the rule's multiplier.
	Factor = 2.0
)

// Floors are PLAN §41.3's policy floors, applied to the final value; every other threshold's floor is 0.
// kFs's is one fd across a 600 s quiet span, since G4's slope rests on about three quiet checkpoints (PLAN v7.14 §48.2).
var Floors = map[string]float64{
	gate.KL:  1.02*1.02 - 1,
	gate.KG:  3600 / SlopeWindow,
	gate.KFs: 6,
	gate.KSs: 3600 / SlopeWindow,
	gate.KH:  3600 / SlopeWindow,
	gate.KCh: 1.0 / (ChurnRun - 1),
}

// slopeThresholds are the thresholds derived from per-window slopes; kCh's windows are churn runs.
var slopeThresholds = []string{gate.KG, gate.KH, gate.KFs, gate.KSs, gate.KCh}

// degenerateAtZero are the thresholds that need the maintainer's written acceptance when they come out 0.
var degenerateAtZero = []string{gate.KG0, gate.KHr, gate.KF, gate.KS, gate.KC, gate.KD}

// Cell is one calibration cell with its id.
type Cell struct {
	ID  string
	Cal cellrun.Calibration
}

// Observation is what one cell contributes to one threshold.
type Observation struct {
	// Full is the critical value over the whole cell (§41.1); not OK means none.
	Full gate.Critical
	// Windows are the per-window values (§41.4): slopes, kHr's window rises, kL's comparisons.
	Windows []float64
	// Skipped counts the windows with too few points.
	Skipped int
	// Excluded is true when the cell does not contribute (kL on a contaminated cool-down).
	Excluded bool
	// NonFinite counts the windows whose value was a NaN or an infinity; any refuses the derivation.
	NonFinite int
}

// Observe computes a cell's observations for every threshold, and the problems that refuse the derivation.
//
// Parameters:
//   - c: the cell
//
// Returns:
//   - map[string]Observation: by threshold name
//   - []string: problems, e.g. an absent or ambiguous p99
func Observe(c Cell) (map[string]Observation, []string) {
	col := c.Cal.Collected
	full := col.Critical()
	out := map[string]Observation{}
	for _, k := range gate.AllThresholds {
		out[k] = Observation{Full: full[k]}
	}
	s := col.Series()
	for from := s.Warm; from+SlopeWindow <= s.End; from += WindowStep {
		to := from + SlopeWindow
		groups := map[string]gate.Series{}
		points := 0
		for name, g := range s.Groups {
			w := g.Window(from, to)
			groups[name] = w
			points = max(points, len(w))
		}
		addWindow(out, gate.KG, points, func() gate.Critical { kG, _ := gate.G2Critical(gate.G2Input{Groups: groups}); return kG })
		heap := s.Heap.Window(from, to)
		kH, kHr := gate.G3Critical(heap, from, to)
		addWindow(out, gate.KH, len(heap), func() gate.Critical { return kH })
		addWindow(out, gate.KHr, len(heap), func() gate.Critical { return kHr })
		fds := s.QuietFDs.Window(from, to)
		addWindow(out, gate.KFs, len(fds), func() gate.Critical { _, kFs := gate.G4Critical(fds, 0); return kFs })
		balance := map[string]gate.Series{}
		points = 0
		for id, b := range s.QuietBalance {
			balance[id] = b.Window(from, to)
			points = max(points, len(balance[id]))
		}
		addWindow(out, gate.KSs, points, func() gate.Critical { _, kSs := gate.G6Critical(balance); return kSs })
	}
	churns := slices.Clone(col.Churns)
	slices.SortFunc(churns, func(a, b gate.ChurnResidue) int { return cmp.Compare(a.Index, b.Index) })
	for i := 0; i+ChurnRun <= len(churns); i++ {
		run := churns[i : i+ChurnRun]
		measured := 0
		for _, ch := range run {
			if ch.Measured {
				measured++
			}
		}
		addWindow(out, gate.KCh, measured, func() gate.Critical { _, kCh := gate.G13Critical(run); return kCh })
	}
	problems := observeLatency(c, out)
	return out, problems
}

// addWindow records one window's value, or a skip when the window has too few points.
func addWindow(out map[string]Observation, k string, points int, value func() gate.Critical) {
	o := out[k]
	v := value()
	switch {
	case points < MinWindowPoints:
		o.Skipped++
	case v.NonFinite:
		o.NonFinite++
	case v.OK:
		o.Windows = append(o.Windows, v.Value)
	default:
		o.Skipped++
	}
	out[k] = o
}

// observeLatency adds kL's comparisons (§41.4): the validation-length warm-up against each validation-length
// cool-down window of the night's cool-down, and the night's own comparison.
func observeLatency(c Cell, out map[string]Observation) []string {
	col := c.Cal.Collected
	o := out[gate.KL]
	tl := col.Timeline
	var problems []string
	warmEnd, coolStart, end := tl.Warmup.Seconds(), tl.Cooldown.Seconds(), tl.Workload.Seconds()
	p99 := func(from, to float64) map[string]float64 {
		m := map[string]float64{}
		for _, class := range col.Classes {
			d, _, ok, ambiguous := c.Cal.Latency.QuantileChecked(class, from, to, 0.99)
			switch {
			case !ok:
				problems = append(problems, fmt.Sprintf("%s: class %s has no p99 in [%.0f, %.0f) s", c.ID, class, from, to))
			case ambiguous:
				problems = append(problems, fmt.Sprintf("%s: class %s's p99 in [%.0f, %.0f) s falls in an ambiguous bucket", c.ID, class, from, to))
			default:
				m[class] = d.Seconds()
			}
		}
		return m
	}
	// The cross-check runs on every cell, a contaminated one too (Codex AK04).
	nightWarm, nightCool := p99(0, warmEnd), p99(coolStart, end)
	for _, class := range col.Classes {
		if nightWarm[class] != col.WarmupP99[class] || nightCool[class] != col.CooldownP99[class] {
			problems = append(problems, fmt.Sprintf("%s: class %s's rebuilt p99 (%v, %v) differs from the recorded (%v, %v)",
				c.ID, class, nightWarm[class], nightCool[class], col.WarmupP99[class], col.CooldownP99[class]))
		}
	}
	if col.CooldownContaminated() {
		o.Excluded, o.Full = true, gate.Critical{}
		out[gate.KL] = o
		return problems
	}
	addComparison := func(v gate.Critical) {
		switch {
		case v.NonFinite:
			o.NonFinite++
		case v.OK:
			o.Windows = append(o.Windows, v.Value)
		}
	}
	short := p99(0, LatencyWarmup)
	for from := coolStart; from+LatencyCooldown <= end; from += LatencyCooldown {
		addComparison(gate.G14Critical(short, p99(from, from+LatencyCooldown)))
	}
	addComparison(gate.G14Critical(nightWarm, nightCool))
	out[gate.KL] = o
	return problems
}

// Derived is one threshold's derivation, for the report.
type Derived struct {
	Name string `json:"name"`
	// Rule is "max" or "slope".
	Rule string `json:"rule"`
	// PerCell is each cell's whole-cell critical value; nil means none.
	PerCell map[string]*float64 `json:"per_cell"`
	// WindowsUsed and WindowsSkipped count the per-window values per cell.
	WindowsUsed    map[string]int `json:"windows_used,omitempty"`
	WindowsSkipped map[string]int `json:"windows_skipped,omitempty"`
	// Excluded lists the cells that do not contribute (kL on a contaminated cool-down).
	Excluded []string `json:"excluded,omitempty"`
	// Observed is max(0, the pooled statistic); for a slope it is max(P95, the largest whole-window slope).
	Observed float64 `json:"observed"`
	// P95 is the pooled per-window p95, for slopes.
	P95 *float64 `json:"p95,omitempty"`
	// Floor is the policy floor and FloorApplied whether it set the value.
	Floor        float64 `json:"floor"`
	FloorApplied bool    `json:"floor_applied"`
	// Value is the derived threshold.
	Value float64 `json:"value"`
	// Degenerate marks a 0 that needs written acceptance; AcceptedZero is that acceptance.
	Degenerate   bool   `json:"degenerate"`
	AcceptedZero string `json:"accepted_zero,omitempty"`
	// Raised records a -raise of this threshold (PLAN v7.14 §48.5); nil when none was given.
	Raised *Raised `json:"raised,omitempty"`
}

// Raise is the maintainer's minimum for one threshold, applied after the rule and the floors.
type Raise struct {
	Name   string
	Value  float64
	Reason string
}

// Raised is how a raise met the derived value.
type Raised struct {
	// From is the value after the rule and the floors, before the raise; Given the raise's value.
	From  float64 `json:"from"`
	Given float64 `json:"given"`
	// Reason is the maintainer's written reason; Applied whether the raise set the value.
	Reason  string `json:"reason"`
	Applied bool   `json:"applied"`
}

// Combine applies PLAN §41.3 to every cell's observations.
//
// Parameters:
//   - cells: the cell ids, in order
//   - obs: each cell's observations, by cell id
//   - acceptZero: the maintainer's written acceptance of degenerate thresholds, by name
//
// Returns:
//   - gate.Thresholds: every threshold's value
//   - []Derived: how each was derived
//   - []string: problems that refuse the derivation
func Combine(cells []string, obs map[string]map[string]Observation, acceptZero map[string]string) (gate.Thresholds, []Derived, []string) {
	th := gate.Thresholds{}
	var derived []Derived
	var problems []string
	for _, k := range gate.AllThresholds {
		d := Derived{Name: k, Rule: "max", PerCell: map[string]*float64{}, Floor: Floors[k]}
		slope := slices.Contains(slopeThresholds, k)
		if slope {
			d.Rule = "slope"
		}
		windowed := slope || k == gate.KHr || k == gate.KL
		if windowed {
			d.WindowsUsed, d.WindowsSkipped = map[string]int{}, map[string]int{}
		}
		var pool []float64
		stat, contributing := 0.0, 0
		for _, id := range cells {
			o := obs[id][k]
			if windowed {
				d.WindowsUsed[id], d.WindowsSkipped[id] = len(o.Windows), o.Skipped
			}
			if o.Excluded {
				d.Excluded = append(d.Excluded, id)
				continue
			}
			if o.Full.NonFinite {
				problems = append(problems, fmt.Sprintf("%s: a non-finite observation on %s", k, id))
			}
			if o.NonFinite > 0 {
				problems = append(problems, fmt.Sprintf("%s: %d non-finite window values on %s", k, o.NonFinite, id))
			}
			if !o.Full.OK {
				d.PerCell[id] = nil
				problems = append(problems, fmt.Sprintf("%s: no observation on %s", k, id))
				continue
			}
			v := o.Full.Value
			d.PerCell[id] = &v
			contributing++
			stat = math.Max(stat, v)
			pool = append(pool, o.Windows...)
			if !slope {
				for _, w := range o.Windows {
					stat = math.Max(stat, w)
				}
			}
		}
		if contributing == 0 {
			problems = append(problems, fmt.Sprintf("%s: no cell contributes", k))
		}
		if slope {
			if len(pool) == 0 {
				problems = append(problems, fmt.Sprintf("%s: every window was skipped", k))
			} else {
				p, _ := gate.Percentile(pool, 0.95)
				d.P95 = &p
				stat = math.Max(stat, p)
			}
		}
		d.Observed = stat
		d.Value = Factor * stat
		if d.Floor > d.Value {
			d.Value, d.FloorApplied = d.Floor, true
		}
		if d.Value == 0 && slices.Contains(degenerateAtZero, k) {
			d.Degenerate = true
			if reason, ok := acceptZero[k]; ok {
				d.AcceptedZero = reason
			} else {
				problems = append(problems, fmt.Sprintf("%s: degenerate (0) and not accepted with -accept-zero", k))
			}
		}
		th[k] = d.Value
		derived = append(derived, d)
	}
	for k := range acceptZero {
		if !slices.Contains(degenerateAtZero, k) {
			problems = append(problems, fmt.Sprintf("-accept-zero names %s, which is not one of %v", k, degenerateAtZero))
		}
	}
	sort.Strings(problems)
	return th, derived, problems
}

// ApplyRaises raises each named threshold to at least its given value (PLAN v7.14 §48.5).
// It never lowers a threshold, and it applies nothing when any raise is invalid.
//
// Parameters:
//   - th: the derived thresholds, updated in place
//   - derived: the derivation records, updated in place
//   - raises: the maintainer's raises
//
// Returns:
//   - []string: one entry per invalid raise; empty when every raise was applied or was a no-op
func ApplyRaises(th gate.Thresholds, derived []Derived, raises []Raise) []string {
	var problems []string
	seen := map[string]bool{}
	for _, r := range raises {
		switch {
		case !slices.Contains(gate.AllThresholds, r.Name):
			problems = append(problems, fmt.Sprintf("-raise names %s, which is not a threshold", r.Name))
		case seen[r.Name]:
			problems = append(problems, fmt.Sprintf("-raise names %s twice", r.Name))
		case math.IsNaN(r.Value) || math.IsInf(r.Value, 0) || r.Value < 0:
			problems = append(problems, fmt.Sprintf("-raise %s: the value must be finite and ≥ 0, got %v", r.Name, r.Value))
		case strings.TrimSpace(r.Reason) == "":
			problems = append(problems, fmt.Sprintf("-raise %s: a reason is required", r.Name))
		}
		seen[r.Name] = true
	}
	if len(problems) > 0 {
		return problems
	}
	for _, r := range raises {
		i := slices.IndexFunc(derived, func(d Derived) bool { return d.Name == r.Name })
		d := &derived[i]
		rec := &Raised{From: d.Value, Given: r.Value, Reason: strings.TrimSpace(r.Reason), Applied: r.Value > d.Value}
		if rec.Applied {
			d.Value = r.Value
		}
		d.Raised = rec
		th[r.Name] = d.Value
	}
	return nil
}

// calibratedGates are the gates whose thresholds gates.json holds.
var calibratedGates = []string{"G2", "G3", "G4", "G6", "G7", "G10", "G12", "G13", "G14", "G16"}

// SelfCheck evaluates every cell's calibrated gates with the derived thresholds (PLAN §41.7).
// A calibrated gate must pass, or G14 be not evaluated on a contaminated cool-down;
// a fixed predicate that fails is a cell problem too, never hidden by the thresholds.
//
// Parameters:
//   - cells: the cells
//   - th: the derived thresholds
//
// Returns:
//   - []string: one entry per gate that did not pass, with its details
func SelfCheck(cells []Cell, th gate.Thresholds) []string {
	var problems []string
	for _, c := range cells {
		col := c.Cal.Collected
		col.Thresholds = th
		v := cellrun.Evaluate(col)
		byGate := map[string]gate.Result{}
		for _, r := range v.Gates {
			byGate[r.Gate] = r
		}
		for _, g := range calibratedGates {
			r, ok := byGate[g]
			switch {
			case !ok:
				problems = append(problems, fmt.Sprintf("%s: self-check: %s was not evaluated", c.ID, g))
			case r.Status == gate.StatusPass, g == "G14" && r.Status == gate.StatusNotEvaluated && col.CooldownContaminated():
				// Missing evidence found while replaying a calibrated gate refuses even when the gate passes,
				// e.g. G16, which stays advisory (Codex AK03).
				for _, d := range r.Details {
					if strings.HasPrefix(d, "missing evidence:") {
						problems = append(problems, fmt.Sprintf("%s: self-check: %s %s", c.ID, g, d))
					}
				}
			default:
				problems = append(problems, fmt.Sprintf("%s: self-check: %s %s: %v", c.ID, g, r.Status, r.Details))
			}
		}
	}
	return problems
}
