package gate

import (
	"cmp"
	"fmt"
	"maps"
	"math"
	"slices"
)

// Interval is a closed time range in seconds since the cell epoch.
type Interval struct {
	// Start and End bound the range.
	Start, End float64
}

// TPStatsSample is one `nodetool tpstats` round over every node.
type TPStatsSample struct {
	// Start is when the round's first command started; T when its last command ended.
	// A delta between two rounds covers [previous Start, this T], so any window touching it excludes it.
	Start, T float64
	// PID is each node's Cassandra process id at T; a change means the node restarted.
	PID map[string]int
	// Dropped is each node's cumulative dropped-message count; NaN or absent when unparsable.
	Dropped map[string]float64
	// Skipped names the nodes intentionally not read in this round (the end of the workload came first):
	// a pair touching a skipped reading is neither evaluated nor missing.
	Skipped map[string]bool
}

// GCStatsSample is one node's `nodetool gcstats` reading.
// gcstats already reports an interval (it calls getAndResetGCStats), so it is never differenced.
type GCStatsSample struct {
	// Start is when the command started and T when it ended; the interval covers
	// [Start − IntervalMs, T], widened by the command's own duration.
	Start, T float64
	// Node is the node name.
	Node string
	// IntervalMs is the length of the reported interval.
	IntervalMs float64
	// MaxPauseMs is the longest GC pause in the interval.
	MaxPauseMs float64
	// TotalMs is the total GC time in the interval.
	TotalMs float64
	// OK is false when the output could not be parsed.
	OK bool
	// Prime marks the first read of a node, which only resets the interval: it bounds the evidence, but is never evaluated.
	Prime bool
	// PID is the node's pid at the read, zero when unknown; a new pid is a new JVM, whose counters restarted.
	PID int
	// Cancelled marks a read the end of the workload interrupted: never evaluated, never missing.
	Cancelled bool
	// Rebased marks the first successful read after a failed prime: its interval may reach back to the JVM's start,
	// so it only establishes the baseline and is never evaluated (Codex Q02).
	Rebased bool
}

// G16 evaluates fixture health, which is advisory (PLAN §6.2).
//
// Only steady-state intervals are evaluated: those that overlap no fault window.
// tpstats counters are cumulative, so consecutive samples are differenced per node;
// a PID change marks a restart and the interval is skipped, never read as a negative delta.
// A missing, unparsable or non-finite reading is counted as missing, never as zero.
//
// Parameters:
//   - tp: tpstats rounds, in any order
//   - gc: gcstats readings, in any order
//   - windows: the fault windows to exclude
//   - th: needs KD, KGp and KGt
//
// Returns:
//   - Result: fail when an interval drops more than kD messages, pauses longer than kGp ms,
//     or spends more than kGt of its time in GC
//   - int: how many intervals were missing
func G16(tp []TPStatsSample, gc []GCStatsSample, windows []Interval, th Thresholds) (Result, int) {
	// Missing intervals are counted even without thresholds: they are evidence for calibration (Codex K01).
	v, err := th.getAll(KD, KGp, KGt)
	evaluate := err == nil
	kD, kGp, kGt := math.Inf(1), math.Inf(1), math.Inf(1)
	if evaluate {
		kD, kGp, kGt = v[0], v[1], v[2]
	}
	var details []string
	drops, gcs, missing := g16Observations(tp, gc, windows)
	for _, d := range drops {
		if d.Dropped > kD {
			details = append(details, fmt.Sprintf("%s dropped %.0f messages in %.0f–%.0fs", d.Node, d.Dropped, d.From, d.To))
		}
	}
	for _, s := range gcs {
		if s.MaxPauseMs > kGp {
			details = append(details, fmt.Sprintf("%s GC pause %.0f ms at %.0fs", s.Node, s.MaxPauseMs, s.T))
		}
		if share := gcShare(s); share > kGt {
			details = append(details, fmt.Sprintf("%s GC share %.3f at %.0fs", s.Node, share, s.T))
		}
	}
	if !evaluate {
		return invalid("G16", err), missing
	}
	return result("G16", details), missing
}

// g16Drop is one node's dropped-message delta over one evaluated steady-state tpstats interval.
type g16Drop struct {
	Node     string
	From, To float64
	Dropped  float64
}

// g16Observations returns what G16 evaluates: the dropped-message deltas and the gcstats readings of the steady-state intervals,
// and how many intervals were missing.
func g16Observations(tp []TPStatsSample, gc []GCStatsSample, windows []Interval) ([]g16Drop, []GCStatsSample, int) {
	var drops []g16Drop
	var gcs []GCStatsSample
	missing := 0

	sorted := slices.Clone(tp)
	slices.SortFunc(sorted, func(a, b TPStatsSample) int { return cmp.Compare(a.T, b.T) })
	for i := 1; i < len(sorted); i++ {
		prev, cur := sorted[i-1], sorted[i]
		if overlapsAny(Interval{prev.Start, cur.T}, windows) {
			continue
		}
		// A node absent from either round of the pair is a missing reading, not a skipped one (Codex L02).
		nodes := map[string]bool{}
		for n := range cur.Dropped {
			nodes[n] = true
		}
		for n := range prev.Dropped {
			nodes[n] = true
		}
		for _, node := range slices.Sorted(maps.Keys(nodes)) {
			if prev.Skipped[node] || cur.Skipped[node] {
				continue
			}
			before, okB := prev.Dropped[node]
			after, okA := cur.Dropped[node]
			pidB, okP := prev.PID[node]
			switch {
			case !okB || !okA || !okP || !isFinite(before) || !isFinite(after) || pidB != cur.PID[node]:
				missing++
			case after < before:
				missing++
			default:
				drops = append(drops, g16Drop{Node: node, From: prev.T, To: cur.T, Dropped: after - before})
			}
		}
	}

	for _, s := range gc {
		if s.Prime || s.Rebased || s.Cancelled || overlapsAny(Interval{s.Start - s.IntervalMs/1000, s.T}, windows) {
			continue
		}
		if !s.OK || s.IntervalMs <= 0 || !isFinite(s.IntervalMs) || !isFinite(s.MaxPauseMs) || !isFinite(s.TotalMs) {
			missing++
			continue
		}
		gcs = append(gcs, s)
	}
	return drops, gcs, missing
}

// gcShare is the fraction of a gcstats interval spent in GC.
func gcShare(s GCStatsSample) float64 {
	return s.TotalMs / s.IntervalMs
}

func overlapsAny(iv Interval, windows []Interval) bool {
	for _, w := range windows {
		if iv.Start <= w.End && w.Start <= iv.End {
			return true
		}
	}
	return false
}
