package gate

import (
	"maps"
	"math"
	"slices"
)

// Critical is one threshold's critical value on one cell's data (PLAN §41.1):
// the smallest threshold at which every term comparing a measurement with it passes.
// Each gate compares with ">", so it passes at exactly this value.
// The gates compute their terms with the same helpers, so the two cannot drift.
type Critical struct {
	// Value is the critical value; meaningful only when OK.
	Value float64
	// OK is false when the term had no finite inputs ("none" in §41.1), e.g. fewer than two points for a slope.
	OK bool
	// NonFinite is true when an input gave a NaN or an infinity, e.g. a ratio over a zero median:
	// such a value is not a usable observation, and the derivation refuses it (Codex AK06).
	NonFinite bool
}

// observe raises c to v when v is larger, or sets it when c has no value yet; a non-finite v only sets NonFinite.
func (c *Critical) observe(v float64) {
	switch {
	case !isFinite(v):
		c.NonFinite = true
	case !c.OK || v > c.Value:
		c.Value, c.OK = v, true
	}
}

// MaxCritical returns the largest of several critical values of one threshold, e.g. from the two gates that read kF or kC.
//
// Parameters:
//   - cs: the values; those that are not OK are skipped
//
// Returns:
//   - Critical: the largest, not OK when none is
func MaxCritical(cs ...Critical) Critical {
	var out Critical
	for _, c := range cs {
		if c.OK {
			out.observe(c.Value)
		}
		out.NonFinite = out.NonFinite || c.NonFinite
	}
	return out
}

// G2Critical returns the critical values of kG and kG0 (PLAN §41.1).
//
// Parameters:
//   - in: what G2 evaluates
//
// Returns:
//   - kG: the largest per-group slope, per hour
//   - kG0: the largest quiet-checkpoint total above the fixed part of the bound
func G2Critical(in G2Input) (kG, kG0 Critical) {
	for _, slope := range groupSlopes(in.Groups) {
		kG.observe(slope)
	}
	fixed := g2Fixed(in)
	for _, p := range in.QuietTotals {
		kG0.observe(p.V - fixed)
	}
	return kG, kG0
}

// G3Critical returns the critical values of kH and kHr (PLAN §41.1).
//
// Parameters:
//   - heapMiB: heap after GC over the trend window
//   - from, to: the trend window, whose first and last 20 min give the medians
//
// Returns:
//   - kH: the heap slope, MiB per hour
//   - kHr: the late median's rise over the early one, as a fraction
func G3Critical(heapMiB Series, from, to float64) (kH, kHr Critical) {
	if slope, ok := slopePerHour(heapMiB); ok {
		kH.observe(slope)
	}
	if early, late, ok := heapMedians(heapMiB, from, to); ok {
		kHr.observe(rise(early, late))
	}
	return kH, kHr
}

// G4Critical returns G4's critical values of kF and kFs (PLAN §41.1); G12 also reads kF (G12Critical).
//
// Parameters:
//   - quietFDs: the fd count at quiet checkpoints
//   - fd0: the fd count before the primary opened
//
// Returns:
//   - kF: the largest count above fd0
//   - kFs: the fd slope, per hour
func G4Critical(quietFDs Series, fd0 float64) (kF, kFs Critical) {
	return boundAndSlopeCritical(quietFDs, fd0)
}

// G6Critical returns the critical values of kS and kSs over every session (PLAN §41.1).
//
// Parameters:
//   - quietBalance: each session's stream balance at quiet checkpoints
//
// Returns:
//   - kS: the largest balance
//   - kSs: the largest balance slope, per hour
func G6Critical(quietBalance map[string]Series) (kS, kSs Critical) {
	for _, s := range quietBalance {
		b, sl := boundAndSlopeCritical(s, 0)
		kS, kSs = MaxCritical(kS, b), MaxCritical(kSs, sl)
	}
	return kS, kSs
}

// G7Critical returns the critical value of kE: the dropped event frame count (PLAN §41.1).
//
// Parameters:
//   - c: the log counts over every session
//
// Returns:
//   - Critical: the count
func G7Critical(c LogCounts) Critical {
	return Critical{Value: float64(c.EventBufferFull), OK: true}
}

// G10Critical returns the critical value of kLWTu: the uncertain LWT count (PLAN §41.1).
//
// Parameters:
//   - uncertain: the uncertain LWT operations
//
// Returns:
//   - Critical: the count
func G10Critical(uncertain int64) Critical {
	return Critical{Value: float64(uncertain), OK: true}
}

// G12Critical returns G12's critical values of kC and kF (PLAN §41.1); G13 also reads kC and G4 kF.
//
// Parameters:
//   - in: the teardown measurements
//
// Returns:
//   - kC: the largest goroutine group drift from baseline₀, none without both dumps
//   - kF: the fd drift from fd0, none without both counts
func G12Critical(in G12Input) (kC, kF Critical) {
	if in.Baseline != nil && in.After != nil {
		kC = maxDrift(in.Baseline, in.After)
	}
	if in.FDs >= 0 && in.FD0 >= 0 {
		kF.observe(math.Abs(float64(in.FDs - in.FD0)))
	}
	return kC, kF
}

// G13Critical returns G13's critical values of kC and kCh over the measured churns (PLAN §41.1).
//
// Parameters:
//   - churns: every churn's residue; unmeasured ones contribute nothing
//
// Returns:
//   - kC: the largest goroutine group drift over any churn
//   - kCh: the residue slope over the churn index, goroutines per churn
func G13Critical(churns []ChurnResidue) (kC, kCh Critical) {
	for _, c := range churns {
		if !c.Measured {
			continue
		}
		kC = MaxCritical(kC, maxDrift(c.Before, c.After))
	}
	if slope, ok := TheilSenSlope(residueSeries(churns)); ok {
		kCh.observe(slope)
	}
	return kC, kCh
}

// G14Critical returns the critical value of kL (PLAN §41.1):
// the largest rise of a class's cool-down p99 over its warm-up p99.
//
// Parameters:
//   - warmupP99, cooldownP99: p99 per class, seconds; a class needs both
//
// Returns:
//   - Critical: the largest rise, as a fraction; none when no class has both
func G14Critical(warmupP99, cooldownP99 map[string]float64) Critical {
	var kL Critical
	for class, cool := range cooldownP99 {
		if warm, ok := warmupP99[class]; ok {
			kL.observe(rise(warm, cool))
		}
	}
	return kL
}

// G16Critical returns the critical values of kD, kGp and kGt over the intervals G16 evaluates (PLAN §41.1).
//
// Parameters:
//   - tp, gc: the health readings, as G16 takes them
//   - windows: the fault windows to exclude
//
// Returns:
//   - kD: the largest dropped-message delta of a node over an interval
//   - kGp: the largest GC pause, milliseconds
//   - kGt: the largest GC time fraction
func G16Critical(tp []TPStatsSample, gc []GCStatsSample, windows []Interval) (kD, kGp, kGt Critical) {
	drops, gcs, _ := g16Observations(tp, gc, windows)
	for _, d := range drops {
		kD.observe(d.Dropped)
	}
	for _, s := range gcs {
		kGp.observe(s.MaxPauseMs)
		kGt.observe(gcShare(s))
	}
	return kD, kGp, kGt
}

// groupSlopes is each goroutine group's Theil–Sen slope per hour; a group without a slope is left out.
func groupSlopes(groups map[string]Series) map[string]float64 {
	out := map[string]float64{}
	for name, s := range groups {
		if slope, ok := slopePerHour(s); ok {
			out[name] = slope
		}
	}
	return out
}

// g2Fixed is the part of G2's total bound that does not come from gates.json: baseline₀ plus the pool's goroutines.
func g2Fixed(in G2Input) float64 {
	return in.Base + float64(in.Hosts*in.NumConns*goroutinesPerConn)
}

// slopePerHour is the Theil–Sen slope of s, per hour.
func slopePerHour(s Series) (float64, bool) {
	slope, ok := TheilSenSlope(s)
	return slope * secondsPerHour, ok
}

// heapMedians are G3's early and late medians: the first and last 20 min of [from, to].
func heapMedians(heapMiB Series, from, to float64) (early, late float64, ok bool) {
	early, okE := Median(heapMiB.Window(from, from+heapMedianSpan).Values())
	late, okL := Median(heapMiB.Window(to-heapMedianSpan, to).Values())
	return early, late, okE && okL
}

// rise is after's relative rise over before, as a fraction.
func rise(before, after float64) float64 {
	return after/before - 1
}

// maxDrift is the largest goroutine group drift from before to after.
func maxDrift(before, after map[string]int) Critical {
	var c Critical
	names := slices.Concat(slices.Collect(maps.Keys(before)), slices.Collect(maps.Keys(after)))
	for _, name := range names {
		c.observe(drift(before, after, name))
	}
	return c
}

// boundAndSlopeCritical is the critical value pair of boundAndSlope: the largest excess over base, and the slope per hour.
func boundAndSlopeCritical(s Series, base float64) (bound, slope Critical) {
	for _, p := range s {
		bound.observe(p.V - base)
	}
	if v, ok := slopePerHour(s); ok {
		slope.observe(v)
	}
	return bound, slope
}
