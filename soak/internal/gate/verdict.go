package gate

import (
	"fmt"
	"slices"
)

// ModeNight and ModeValidate are the harness's run modes.
const (
	// ModeNight is a 2 h cell of the nightly run.
	ModeNight Mode = "night"
	// ModeValidate is a detector-validation run (PLAN §7).
	ModeValidate Mode = "validate"
)

// VerdictRunning and the other VerdictStatus values are a cell's verdict (PLAN §6.2).
const (
	// VerdictRunning is written at start and while the cell runs.
	VerdictRunning VerdictStatus = "running"
	// VerdictPass means every gate held.
	VerdictPass VerdictStatus = "pass"
	// VerdictFail means a driver gate failed.
	VerdictFail VerdictStatus = "fail"
	// VerdictFixtureInvalid means the fixture, not the driver, was at fault.
	VerdictFixtureInvalid VerdictStatus = "fixture-invalid"
	// VerdictIncomplete means the run did not finish.
	VerdictIncomplete VerdictStatus = "incomplete"
	// VerdictInvalidConfig means the configuration or thresholds could not be evaluated.
	VerdictInvalidConfig VerdictStatus = "invalid-config"
)

// AnnotationFixtureSuspect marks a driver failure that coincided with a G16 violation.
const AnnotationFixtureSuspect = "fixture-suspect"

// nightWorkloadSeconds is the workload a night cell must run to be complete.
const nightWorkloadSeconds = 7200

// cellsPerNight is the number of cells a night runs.
const cellsPerNight = 4

// Mode is the harness's run mode.
type Mode string

// VerdictStatus is a cell's verdict.
type VerdictStatus string

// Verdict is a cell's outcome, written to verdict.json.
type Verdict struct {
	// Status is the verdict.
	Status VerdictStatus `json:"status"`
	// FailingGates lists the driver gates that failed, when Status is fail.
	FailingGates []string `json:"failing_gates,omitempty"`
	// Annotations qualifies the verdict, e.g. fixture-suspect.
	Annotations []string `json:"annotations,omitempty"`
	// Reasons explains a non-gate verdict: invalid-config, incomplete or fixture-invalid.
	Reasons []string `json:"reasons,omitempty"`
	// Gates holds every gate's result, G16 included.
	Gates []Result `json:"gates"`
}

// VerdictInput is everything Decide needs.
type VerdictInput struct {
	// Mode is the run mode.
	Mode Mode
	// Gates holds the results of G0–G15.
	Gates []Result
	// G16 is the advisory fixture-health result.
	G16 Result
	// InvalidConfig lists configuration errors found before or during the run.
	InvalidConfig []string
	// Incomplete lists why the run did not finish: watchdog, kill, early stop.
	Incomplete []string
	// FixtureInvalid lists fixture failures: preload budget, disk guard, a fault's install or remove step.
	FixtureInvalid []string
	// WorkloadSeconds is how long the workload ran.
	WorkloadSeconds float64
}

// Decide combines gate results into a cell verdict (PLAN §6.2).
//
// The precedence is invalid-config, incomplete (an explicit reason, or a short night or no gates without a fixture failure),
// fixture-invalid (G0 or a fixture failure),
// fail (a driver gate), then fixture-invalid for a G16 violation on its own, then pass.
// A driver failure together with a G16 violation is a fail annotated fixture-suspect.
// A run with no gate results cannot pass.
//
// Parameters:
//   - in: the gate results and the run's circumstances
//
// Returns:
//   - Verdict: the cell verdict, carrying every gate result
func Decide(in VerdictInput) Verdict {
	v := Verdict{Gates: append(slices.Clone(in.Gates), in.G16)}
	invalid := slices.Clone(in.InvalidConfig)
	for _, r := range v.Gates {
		if r.Status == StatusInvalidConfig {
			invalid = append(invalid, fmt.Sprintf("%s: %v", r.Gate, r.Details))
		}
	}
	if len(invalid) > 0 {
		v.Status, v.Reasons = VerdictInvalidConfig, invalid
		return v
	}

	fixture := slices.Clone(in.FixtureInvalid)
	var failing []string
	for _, r := range in.Gates {
		if r.Status != StatusFail {
			continue
		}
		if r.Gate == "G0" {
			fixture = append(fixture, fmt.Sprintf("G0: %v", r.Details))
			continue
		}
		failing = append(failing, r.Gate)
	}

	// A fixture failure, G0 included, aborts the cell (PLAN §3.5, §5.2), so the short workload and missing gates
	// that follow are its consequence, not a separate reason: they make the run incomplete only without one.
	incomplete := slices.Clone(in.Incomplete)
	if len(fixture) == 0 {
		if in.Mode == ModeNight && in.WorkloadSeconds < nightWorkloadSeconds {
			incomplete = append(incomplete, fmt.Sprintf("workload ran %.0f s < %d s", in.WorkloadSeconds, nightWorkloadSeconds))
		}
		if len(in.Gates) == 0 {
			incomplete = append(incomplete, "no gate results")
		}
	}
	if len(incomplete) > 0 {
		v.Status, v.Reasons = VerdictIncomplete, incomplete
		return v
	}
	if len(fixture) > 0 {
		v.Status, v.Reasons = VerdictFixtureInvalid, fixture
		return v
	}

	g16Failed := in.G16.Status == StatusFail
	switch {
	case len(failing) > 0:
		v.Status, v.FailingGates = VerdictFail, failing
		if g16Failed {
			v.Annotations = []string{AnnotationFixtureSuspect}
		}
	case g16Failed:
		v.Status, v.Reasons = VerdictFixtureInvalid, []string{fmt.Sprintf("G16: %v", in.G16.Details)}
	default:
		v.Status = VerdictPass
	}
	return v
}

// NightVerdict combines the four cell verdicts.
//
// Parameters:
//   - cells: one verdict per cell
//
// Returns:
//   - VerdictStatus: pass only when all four cells passed, otherwise fail
func NightVerdict(cells []Verdict) VerdictStatus {
	if len(cells) != cellsPerNight {
		return VerdictFail
	}
	for _, c := range cells {
		if c.Status != VerdictPass {
			return VerdictFail
		}
	}
	return VerdictPass
}
