package gate

import (
	"fmt"
	"maps"
	"math"
	"slices"
)

// StatusPass and the other Status values are a gate's outcome.
const (
	// StatusPass means the invariant held.
	StatusPass Status = "pass"
	// StatusFail means the invariant was violated; Result.Details says where.
	StatusFail Status = "fail"
	// StatusInvalidConfig means a threshold was missing, so the gate could not be evaluated.
	StatusInvalidConfig Status = "invalid-config"
	// StatusNotEvaluated means the plan exempts this run from the gate (e.g. a contaminated cool-down).
	StatusNotEvaluated Status = "not-evaluated"
)

const (
	secondsPerHour = 3600.0
	// heapMedianSpan is the length of the early and late heap windows compared by G3.
	heapMedianSpan = 20 * 60.0
	// progressMinSpan is the shortest fault-free span G11 evaluates.
	progressMinSpan = 60.0
	// progressMinRatio is the completed share of the offered rate G11 requires.
	progressMinRatio = 0.8
	// goroutinesPerConn is the per-connection goroutine allowance in the G2 total bound.
	goroutinesPerConn = 4
)

// Status is a gate's outcome.
type Status string

// Result is one gate's outcome with its evidence.
type Result struct {
	// Gate is the gate id, e.g. "G3".
	Gate string `json:"gate"`
	// Status is the outcome.
	Status Status `json:"status"`
	// Details lists each violation, or why the gate was not evaluated.
	Details []string `json:"details,omitempty"`
}

// G2Input is the goroutine data G2 evaluates.
type G2Input struct {
	// Groups is the goroutine count per `created by` function, sampled over the trend window.
	Groups map[string]Series
	// QuietTotals is the total goroutine count at quiet checkpoints.
	QuietTotals Series
	// Base is the goroutine count before the primary session opened.
	Base float64
	// Hosts is the number of cluster nodes.
	Hosts int
	// NumConns is the primary session's connections per host.
	NumConns int
}

// LogCounts are the driver log lines G7 counts.
type LogCounts struct {
	// GoroutinePanicked counts "Goroutine panicked."
	GoroutinePanicked int
	// NoHandler counts "Received response for stream which has no handler."
	NoHandler int
	// IterNotClosed counts "Iter was garbage-collected without Close()", after a forced GC.
	IterNotClosed int
	// SchemaAgreement counts "Error while awaiting for schema agreement after a schema change event."
	SchemaAgreement int
	// EventBufferFull counts "Event buffer full, dropping event frame."
	EventBufferFull int
}

// Progress is the per-second offered and completed operations of each class, for G11.
type Progress struct {
	// Offered and Completed map class → count per second since the cell epoch.
	Offered, Completed map[string][]float64
}

// Coverage is what a cell executed, for G15.
type Coverage struct {
	// MandatoryFaults and MandatoryExecuted count the assignment's faults and those that ran.
	MandatoryFaults, MandatoryExecuted int
	// ChurnSlots and ChurnExecuted count the reserved churn slots and those that ran.
	ChurnSlots, ChurnExecuted int
	// ProvenPrefetch counts proven early-close scans (§4.2).
	ProvenPrefetch int
	// ProvenSpeculation counts proven spec-reads (§4.2).
	ProvenSpeculation int
	// ShortDeadlineTimeouts counts short-deadline operations that timed out.
	ShortDeadlineTimeouts int
}

// G1 checks that goroutineleak reported nothing at any profile sample or after Close.
//
// Parameters:
//   - leaks: the goroutineleak count at each profile sample
//
// Returns:
//   - Result: fail at the first non-zero sample
func G1(leaks Series) Result {
	var details []string
	for _, p := range leaks {
		if p.V != 0 {
			details = append(details, fmt.Sprintf("t=%.0fs goroutineleak=%.0f", p.T, p.V))
		}
	}
	return result("G1", details)
}

// G2 checks goroutine growth per group and the total at quiet checkpoints.
//
// Parameters:
//   - in: the goroutine series and the bound's inputs
//   - th: needs KG and KG0
//
// Returns:
//   - Result: fail when a group's slope exceeds kG per hour or a quiet total exceeds its bound
func G2(in G2Input, th Thresholds) Result {
	v, err := th.getAll(KG, KG0)
	if err != nil {
		return invalid("G2", err)
	}
	kG, kG0 := v[0], v[1]
	var details []string
	for _, name := range slices.Sorted(maps.Keys(in.Groups)) {
		if slope, ok := TheilSenSlope(in.Groups[name]); ok && slope*secondsPerHour > kG {
			details = append(details, fmt.Sprintf("group %q slope %.2f/h > %.2f/h", name, slope*secondsPerHour, kG))
		}
	}
	bound := in.Base + float64(in.Hosts*in.NumConns*goroutinesPerConn) + kG0
	for _, p := range in.QuietTotals {
		if p.V > bound {
			details = append(details, fmt.Sprintf("t=%.0fs total %.0f > %.0f", p.T, p.V, bound))
		}
	}
	return result("G2", details)
}

// G3 checks the heap after GC over the trend window.
// The medians compare the first and last 20 minutes of the window itself, not of whatever samples exist,
// so a missing sample at either end cannot shift them.
//
// Parameters:
//   - heapMiB: heap in MiB at each profile sample of the trend window
//   - from, to: the trend window, seconds since the epoch
//   - th: needs KH and KHr
//
// Returns:
//   - Result: fail when the slope exceeds kH MiB/h, the late median exceeds the early one by more than kHr,
//     or the window is too short to compare
func G3(heapMiB Series, from, to float64, th Thresholds) Result {
	v, err := th.getAll(KH, KHr)
	if err != nil {
		return invalid("G3", err)
	}
	kH, kHr := v[0], v[1]
	var details []string
	slope, ok := TheilSenSlope(heapMiB)
	switch {
	case !ok:
		details = append(details, "too few heap samples for a slope")
	case slope*secondsPerHour > kH:
		details = append(details, fmt.Sprintf("heap slope %.2f MiB/h > %.2f MiB/h", slope*secondsPerHour, kH))
	}
	early, okE := Median(heapMiB.Window(from, from+heapMedianSpan).Values())
	late, okL := Median(heapMiB.Window(to-heapMedianSpan, to).Values())
	switch {
	case !okE || !okL:
		details = append(details, "too few heap samples for the median comparison")
	case late > early*(1+kHr):
		details = append(details, fmt.Sprintf("late median %.1f MiB > early %.1f MiB × (1+%.2f)", late, early, kHr))
	}
	return result("G3", details)
}

// G4 checks the process fd count at quiet checkpoints.
//
// Parameters:
//   - quietFDs: fd count at each quiet checkpoint
//   - fd0: fd count before the primary session opened
//   - th: needs KF and KFs
//
// Returns:
//   - Result: fail when a checkpoint exceeds fd0+kF or the slope exceeds kFs per hour
func G4(quietFDs Series, fd0 float64, th Thresholds) Result {
	v, err := th.getAll(KF, KFs)
	if err != nil {
		return invalid("G4", err)
	}
	return result("G4", boundAndSlope(quietFDs, fd0+v[0], v[1], "fds"))
}

// G6 checks each session's stream balance, started − finished − abandoned, at quiet checkpoints.
//
// Parameters:
//   - quietBalance: the balance series per session id
//   - th: needs KS and KSs
//
// Returns:
//   - Result: fail when a balance exceeds kS or its slope exceeds kSs per hour
func G6(quietBalance map[string]Series, th Thresholds) Result {
	v, err := th.getAll(KS, KSs)
	if err != nil {
		return invalid("G6", err)
	}
	var details []string
	for _, id := range slices.Sorted(maps.Keys(quietBalance)) {
		for _, d := range boundAndSlope(quietBalance[id], v[0], v[1], "streams") {
			details = append(details, fmt.Sprintf("session %s: %s", id, d))
		}
	}
	return result("G6", details)
}

// G7 checks the driver log invariants.
//
// Parameters:
//   - c: the counted log lines
//   - th: needs KE
//
// Returns:
//   - Result: fail on any panic, orphan response, unclosed Iter or schema agreement error,
//     or more than kE dropped event frames
func G7(c LogCounts, th Thresholds) Result {
	kE, err := th.Get(KE)
	if err != nil {
		return invalid("G7", err)
	}
	var details []string
	for _, z := range []struct {
		name string
		n    int
	}{
		{"Goroutine panicked.", c.GoroutinePanicked},
		{"Received response for stream which has no handler.", c.NoHandler},
		{"Iter was garbage-collected without Close()", c.IterNotClosed},
		{"Error while awaiting for schema agreement after a schema change event.", c.SchemaAgreement},
	} {
		if z.n != 0 {
			details = append(details, fmt.Sprintf("%q × %d", z.name, z.n))
		}
	}
	if float64(c.EventBufferFull) > kE {
		details = append(details, fmt.Sprintf("dropped event frames %d > %.0f", c.EventBufferFull, kE))
	}
	return result("G7", details)
}

// G11 checks progress over every fault-free span of at least 60 s (PLAN G11): every span, not only 60 s windows,
// since windows that each reach 80% can still add up to a longer span below it (Codex J01).
// With the running deficit D(t) = 5 × completed(0,t) − 4 × offered(0,t),
// in integers so exactly 80% never fails by rounding (Codex K04),
// a span [a, b) fails exactly when D(b) < D(a);
// each end b is checked against the largest D(a) over the starts a ≤ b − 60 in the same fault-free interval.
// Every 60 s window must also offer each class at least once.
//
// Parameters:
//   - p: the per-second counts, whole numbers
//   - intervals: the fault-free intervals after warm-up, seconds since the epoch
//   - classes: every class the mix offers
//
// Returns:
//   - Result: fail when a class completed less than 80% of its offered operations over some span of at least 60 s,
//     or was not offered in some 60 s window; the details name the worst span per class and interval
func G11(p Progress, intervals []Interval, classes []string) Result {
	var details []string
	span := int(progressMinSpan)
	for _, iv := range intervals {
		from, to := int(math.Ceil(iv.Start)), int(math.Floor(iv.End))
		if to-from < span {
			continue
		}
		for _, class := range classes {
			if d := g11Worst(from, to, span, prefix(p.Offered[class], to), prefix(p.Completed[class], to)); d != "" {
				details = append(details, fmt.Sprintf("class %s %s", class, d))
			}
		}
	}
	return result("G11", details)
}

// g11Worst finds, in [from, to], the span of at least span seconds with the largest deficit, or a window with no offer.
func g11Worst(from, to, span int, offered, completed []int64) string {
	deficit := func(t int) int64 { return g11Den*completed[t] - g11Num*offered[t] }
	bestA, worst, worstA, worstB := from, int64(0), -1, -1
	for b := from + span; b <= to; b++ {
		if offered[b]-offered[b-span] <= 0 {
			return fmt.Sprintf("was not offered in %d–%ds", b-span, b)
		}
		if a := b - span; deficit(a) > deficit(bestA) {
			bestA = a
		}
		if gap := deficit(bestA) - deficit(b); gap > worst {
			worst, worstA, worstB = gap, bestA, b
		}
	}
	if worstA < 0 {
		return ""
	}
	o, c := offered[worstB]-offered[worstA], completed[worstB]-completed[worstA]
	return fmt.Sprintf("in %d–%ds: completed %d of %d offered (%.1f%% < 80%%)", worstA, worstB, c, o, 100*float64(c)/float64(o))
}

// g11Num / g11Den is progressMinRatio as a fraction, so G11 compares integers.
const (
	g11Num = 4
	g11Den = 5
)

// prefix returns the running sums of the first n seconds, rounded to whole operations: out[i] is the sum of per[0:i].
func prefix(per []float64, n int) []int64 {
	out := make([]int64, n+1)
	for i := range n {
		var v int64
		if i < len(per) {
			v = int64(math.Round(per[i]))
		}
		out[i+1] = out[i] + v
	}
	return out
}

// G14 checks that the cool-down p99 of each class has not degraded from warm-up.
//
// Parameters:
//   - warmupP99: p99 latency per class in warm-up, seconds
//   - cooldownP99: p99 latency per class in cool-down, seconds
//   - classes: every class the mix offers; each must have both measurements
//   - contaminated: the cool-down overlapped a fault that had already failed G9
//   - th: needs KL
//
// Returns:
//   - Result: not evaluated when contaminated; fail when a class exceeds warm-up × (1+kL)
//     or lacks either measurement
func G14(warmupP99, cooldownP99 map[string]float64, classes []string, contaminated bool, th Thresholds) Result {
	if contaminated {
		return Result{Gate: "G14", Status: StatusNotEvaluated, Details: []string{"cool-down contaminated"}}
	}
	kL, err := th.Get(KL)
	if err != nil {
		return invalid("G14", err)
	}
	var details []string
	for _, class := range classes {
		if _, ok := cooldownP99[class]; !ok {
			details = append(details, fmt.Sprintf("class %s has no cool-down p99", class))
		}
	}
	for _, class := range slices.Sorted(maps.Keys(cooldownP99)) {
		warm, ok := warmupP99[class]
		if !ok {
			details = append(details, fmt.Sprintf("class %s has no warm-up p99", class))
			continue
		}
		if cool := cooldownP99[class]; cool > warm*(1+kL) {
			details = append(details, fmt.Sprintf("class %s cool-down p99 %.4fs > warm-up %.4fs × (1+%.2f)", class, cool, warm, kL))
		}
	}
	return result("G14", details)
}

// G15 checks that a cell executed enough to be able to pass.
//
// Parameters:
//   - c: the executed counts
//   - scale: 1 in night mode, 30/90 in validation mode; the minimums are scaled and rounded up
//
// Returns:
//   - Result: fail when any mandatory fault or churn slot did not run, or a minimum was not reached
func G15(c Coverage, scale float64) Result {
	var details []string
	if c.MandatoryExecuted < c.MandatoryFaults {
		details = append(details, fmt.Sprintf("mandatory faults executed %d of %d", c.MandatoryExecuted, c.MandatoryFaults))
	}
	if c.ChurnExecuted < c.ChurnSlots {
		details = append(details, fmt.Sprintf("churn slots executed %d of %d", c.ChurnExecuted, c.ChurnSlots))
	}
	for _, m := range []struct {
		name       string
		got, floor int
	}{
		{"proven prefetch early-closes", c.ProvenPrefetch, 200},
		{"proven speculations", c.ProvenSpeculation, 300},
		{"short-deadline timeouts", c.ShortDeadlineTimeouts, 100},
	} {
		if need := int(math.Ceil(float64(m.floor) * scale)); m.got < need {
			details = append(details, fmt.Sprintf("%s %d < %d", m.name, m.got, need))
		}
	}
	return result("G15", details)
}

// Bool builds the result of a gate whose component already decided it, such as G0, G5, G9 or G10.
//
// Parameters:
//   - gate: the gate id
//   - violations: one entry per violation; empty means pass
//
// Returns:
//   - Result: pass when violations is empty, otherwise fail with them as details
func Bool(gate string, violations []string) Result {
	return result(gate, violations)
}

func boundAndSlope(s Series, bound, slopePerHour float64, unit string) []string {
	var details []string
	for _, p := range s {
		if p.V > bound {
			details = append(details, fmt.Sprintf("t=%.0fs %.0f %s > %.0f", p.T, p.V, unit, bound))
		}
	}
	if slope, ok := TheilSenSlope(s); ok && slope*secondsPerHour > slopePerHour {
		details = append(details, fmt.Sprintf("slope %.2f %s/h > %.2f/h", slope*secondsPerHour, unit, slopePerHour))
	}
	return details
}

func result(gate string, details []string) Result {
	if len(details) == 0 {
		return Result{Gate: gate, Status: StatusPass}
	}
	return Result{Gate: gate, Status: StatusFail, Details: details}
}

func invalid(gate string, err error) Result {
	return Result{Gate: gate, Status: StatusInvalidConfig, Details: []string{err.Error()}}
}
