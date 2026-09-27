package gate

import (
	"fmt"
	"maps"
	"math"
	"slices"
	"time"
)

// closeBound is how long a session's Close may take (G12, G13).
const closeBound = 30 * time.Second

// G12Input is the primary session's teardown, measured after Close, the grace period and a GC.
type G12Input struct {
	// CloseElapsed is how long Close took; CloseReturned is false when it had not returned by the bound.
	CloseElapsed  time.Duration
	CloseReturned bool
	// Baseline is the goroutine count per group before the primary session opened (baseline₀).
	Baseline map[string]int
	// After is the goroutine count per group after the grace period.
	After map[string]int
	// FD0 and FDs are the fd counts before the primary opened and after the grace period.
	FD0, FDs int
	// ProxySockets counts process-owned sockets still connected to a proxy port.
	ProxySockets int
	// Leaks is the goroutineleak count after Close.
	Leaks int
}

// ChurnResidue is one churn slot's teardown, measured after the aux session's grace period (G13).
type ChurnResidue struct {
	// Slot is the churn slot id; Index its position in the cell, 0-based.
	Slot  string `json:"slot"`
	Index int    `json:"index"`
	// Failures lists what the churn itself reported: a create or Close over budget, an overrun.
	Failures []string `json:"failures,omitempty"`
	// Measured is false when the churn failed before its residue check ran.
	Measured bool `json:"measured"`
	// LiveEntries counts the aux session's registry entries still open after pruning.
	LiveEntries int `json:"live_entries"`
	// AttributedSockets counts owned sockets the registry attributes to the aux session.
	AttributedSockets int `json:"attributed_sockets"`
	// Balance is the aux session's stream balance.
	Balance int64 `json:"balance"`
	// Before and After are the goroutine groups just before the aux opened and after its grace period.
	Before map[string]int `json:"before"`
	After  map[string]int `json:"after"`
}

// G10 checks the register and LWT oracles (PLAN §4.3, §4.4).
//
// Parameters:
//   - register: the register violations of the final check
//   - lwt: the LWT bound violations of the final check
//   - runtime: violations seen while the workload ran, e.g. a preloaded key not found
//   - uncertain: the uncertain LWT count
//   - th: needs KLWTu
//
// Returns:
//   - Result: fail on any violation, or when the uncertain count exceeds kLWTu
func G10(register, lwt, runtime []string, uncertain int64, th Thresholds) Result {
	kU, err := th.Get(KLWTu)
	if err != nil {
		return invalid("G10", err)
	}
	details := slices.Concat(register, lwt, runtime)
	if float64(uncertain) > kU {
		details = append(details, fmt.Sprintf("uncertain LWT %d > %.0f", uncertain, kU))
	}
	return result("G10", details)
}

// G12 checks the primary session's Close (PLAN §6).
//
// Parameters:
//   - in: the teardown measurements
//   - th: needs KC and KF
//
// Returns:
//   - Result: fail when Close took longer than 30 s, a goroutine group moved more than kC from baseline₀,
//     the fds moved more than kF from fd0, a proxy socket is still owned, or goroutineleak reported any
func G12(in G12Input, th Thresholds) Result {
	v, err := th.getAll(KC, KF)
	if err != nil {
		return invalid("G12", err)
	}
	kC, kF := v[0], v[1]
	var details []string
	if !in.CloseReturned || in.CloseElapsed > closeBound {
		details = append(details, fmt.Sprintf("Close took %s (returned %v), bound %s", in.CloseElapsed, in.CloseReturned, closeBound))
	}
	// A measurement that failed is missing, never a value within tolerance (Codex J03).
	switch {
	case in.Baseline == nil || in.After == nil:
		details = append(details, "missing evidence: no goroutine dump before the primary opened or after Close")
	default:
		details = append(details, groupDrift(in.Baseline, in.After, kC)...)
	}
	switch {
	case in.FDs < 0 || in.FD0 < 0:
		details = append(details, "missing evidence: no fd count before the primary opened or after Close")
	case math.Abs(float64(in.FDs-in.FD0)) > kF:
		details = append(details, fmt.Sprintf("fds %d vs fd0 %d, beyond ±%.0f", in.FDs, in.FD0, kF))
	}
	switch {
	case in.ProxySockets < 0:
		details = append(details, "missing evidence: process-owned sockets could not be read after Close")
	case in.ProxySockets != 0:
		details = append(details, fmt.Sprintf("%d process-owned sockets to proxy ports", in.ProxySockets))
	}
	switch {
	case in.Leaks < 0:
		details = append(details, "missing evidence: no goroutineleak profile after Close")
	case in.Leaks != 0:
		details = append(details, fmt.Sprintf("goroutineleak %d after Close", in.Leaks))
	}
	return result("G12", details)
}

// G13 checks every churn slot (PLAN §6).
//
// Parameters:
//   - churns: one residue per executed churn slot, in index order
//   - failures: churn failures the executor recorded (overrun, error)
//   - th: needs KC and KCh
//
// Returns:
//   - Result: fail on any churn failure, unmeasured churn, open registry entry, attributed socket,
//     non-zero stream balance, a group drift beyond kC, or a residue slope over the churn index above kCh
func G13(churns []ChurnResidue, failures []string, th Thresholds) Result {
	v, err := th.getAll(KC, KCh)
	if err != nil {
		return invalid("G13", err)
	}
	kC, kCh := v[0], v[1]
	details := slices.Clone(failures)
	for _, c := range churns {
		for _, f := range c.Failures {
			details = append(details, fmt.Sprintf("%s: %s", c.Slot, f))
		}
		if !c.Measured {
			details = append(details, fmt.Sprintf("%s: residue not measured", c.Slot))
			continue
		}
		if c.LiveEntries != 0 {
			details = append(details, fmt.Sprintf("%s: %d registry entries still open", c.Slot, c.LiveEntries))
		}
		if c.AttributedSockets != 0 {
			details = append(details, fmt.Sprintf("%s: %d sockets still attributed", c.Slot, c.AttributedSockets))
		}
		if c.Balance != 0 {
			details = append(details, fmt.Sprintf("%s: stream balance %d", c.Slot, c.Balance))
		}
		for _, d := range groupDrift(c.Before, c.After, kC) {
			details = append(details, fmt.Sprintf("%s: %s", c.Slot, d))
		}
	}
	if slope, ok := TheilSenSlope(residueSeries(churns)); ok && slope > kCh {
		details = append(details, fmt.Sprintf("residue slope %.2f goroutines per churn > %.2f", slope, kCh))
	}
	return result("G13", details)
}

// groupDrift lists every goroutine group whose count moved more than tol from before to after.
// A group missing on one side counts as zero there.
func groupDrift(before, after map[string]int, tol float64) []string {
	names := map[string]bool{}
	for k := range before {
		names[k] = true
	}
	for k := range after {
		names[k] = true
	}
	var details []string
	for _, name := range slices.Sorted(maps.Keys(names)) {
		if drift(before, after, name) > tol {
			details = append(details, fmt.Sprintf("group %q %d → %d, beyond ±%.0f", name, before[name], after[name], tol))
		}
	}
	return details
}

// drift is how far one goroutine group moved from before to after, a missing side counting as zero.
func drift(before, after map[string]int, name string) float64 {
	return math.Abs(float64(after[name] - before[name]))
}

// residueSeries is the per-churn residue G13 fits a slope to: the goroutine total's change over each measured churn, by churn index.
func residueSeries(churns []ChurnResidue) Series {
	var residue Series
	for _, c := range churns {
		if c.Measured {
			residue = append(residue, Point{T: float64(c.Index), V: float64(total(c.After) - total(c.Before))})
		}
	}
	return residue
}

func total(groups map[string]int) int {
	n := 0
	for _, v := range groups {
		n += v
	}
	return n
}
