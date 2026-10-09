package cellrun

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/config"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
)

// RehearsalWorkload is the workload a rehearsal must have run (PLAN §59.7).
const RehearsalWorkload = 7200.0

// rehearsalFixedGates are the gates whose thresholds are fixed, so a rehearsal judges them although the other
// gates' thresholds are provisional; G16 is judged because a rehearsal exists to show the fixture healthy.
var rehearsalFixedGates = []string{"G0", "G1", "G5", "G8", "G9", "G16"}

// Rehearsal is a rehearsal cell's judgment (PLAN §59.7).
type Rehearsal struct {
	// Green is true when no reason was found.
	Green bool `json:"green"`
	// Reasons lists every condition that failed.
	Reasons []string `json:"reasons,omitempty"`
}

// JudgeRehearsal judges a rehearsal from its own artifacts, never from the launcher's exit status (PLAN §59.7).
//
// Parameters:
//   - dir: the execution directory
//   - cell: the rehearsal's cell id
//
// Returns:
//   - Rehearsal: green, or the reasons it is not
//   - error: when the artifacts cannot be read at all
func JudgeRehearsal(dir, cell string) (Rehearsal, error) {
	cal, err := LoadCalibration(dir)
	if err != nil {
		return Rehearsal{}, err
	}
	var reasons []string
	for _, m := range cal.Missing {
		reasons = append(reasons, "missing evidence: "+m)
	}
	var vfile VerdictFile
	var conf config.Config
	var sch chaos.Schedule
	for _, f := range []struct {
		name string
		v    any
	}{{"verdict.json", &vfile}, {"config.json", &conf}, {"schedule.json", &sch}} {
		raw, err := os.ReadFile(filepath.Join(dir, f.name))
		if err != nil {
			return Rehearsal{}, err
		}
		switch d, err := strictDecode(raw, f.v); {
		case err != nil:
			return Rehearsal{}, fmt.Errorf("%s: %w", f.name, err)
		case d != "":
			reasons = append(reasons, fmt.Sprintf("%s is not what the harness writes: %s", f.name, d))
		}
	}
	reasons = append(reasons, rehearsalVerdict(vfile, cell)...)
	reasons = append(reasons, rehearsalEvents(cal.Collected)...)
	reasons = append(reasons, rehearsalSchedule(conf, sch, cal.Collected.Report)...)
	return Rehearsal{Green: len(reasons) == 0, Reasons: reasons}, nil
}

// rehearsalVerdict checks verdict.json: final, the cell, complete evidence, the workload, the status and the fixed gates.
func rehearsalVerdict(v VerdictFile, cell string) []string {
	var reasons []string
	if !v.Final {
		reasons = append(reasons, "verdict.json is not final")
	}
	if v.Cell != cell {
		reasons = append(reasons, fmt.Sprintf("verdict.json is cell %q, want %q", v.Cell, cell))
	}
	if !v.Evidence.Complete {
		reasons = append(reasons, fmt.Sprintf("evidence is incomplete: %v", v.Evidence.Problems))
	}
	if v.WorkloadSeconds < RehearsalWorkload {
		reasons = append(reasons, fmt.Sprintf("the workload ran %.0f s, want at least %.0f", v.WorkloadSeconds, RehearsalWorkload))
	}
	if v.Status != gate.VerdictPass && v.Status != gate.VerdictFail {
		reasons = append(reasons, fmt.Sprintf("status %s: %v", v.Status, v.Reasons))
	}
	if slices.Contains(v.Annotations, gate.AnnotationFixtureSuspect) {
		reasons = append(reasons, "annotated fixture-suspect")
	}
	for _, id := range rehearsalFixedGates {
		i := slices.IndexFunc(v.Gates, func(r gate.Result) bool { return r.Gate == id })
		switch {
		case i < 0:
			reasons = append(reasons, id+" has no result")
		case v.Gates[i].Status != gate.StatusPass:
			reasons = append(reasons, fmt.Sprintf("%s is %s: %v", id, v.Gates[i].Status, v.Gates[i].Details))
		}
	}
	return reasons
}

// rehearsalEvents checks G7's fixed counters and the oracles' lists, whatever kE and kLWTu say.
func rehearsalEvents(c Collected) []string {
	var reasons []string
	l := c.Logs
	if l.GoroutinePanicked+l.NoHandler+l.IterNotClosed+l.SchemaAgreement > 0 {
		reasons = append(reasons, fmt.Sprintf("G7 counters: %+v", l))
	}
	if len(c.Register) > 0 || len(c.LWT) > 0 || len(c.Runtime) > 0 {
		reasons = append(reasons, fmt.Sprintf("oracle violations: register %d, LWT %d, runtime %d", len(c.Register), len(c.LWT), len(c.Runtime)))
	}
	return reasons
}

// rehearsalSchedule binds schedule.json to the plan its configuration and seed give, and the report to both.
func rehearsalSchedule(conf config.Config, sch chaos.Schedule, rep chaos.Report) []string {
	var reasons []string
	_, slots, a, fixed := conf.Effective()
	var optional []chaos.Kind
	if !fixed {
		optional = conf.Base.Shared.Optional
	}
	want, err := chaos.Plan(conf.Seed, slots, a, !fixed, slices.Clone(cellNodes), optional, conf.Base.Shared.Faults)
	switch {
	case err != nil:
		reasons = append(reasons, fmt.Sprintf("the configuration does not plan: %v", err))
	case !reflect.DeepEqual(want.Mandatory, sch.Mandatory):
		reasons = append(reasons, "schedule.json's mandatory faults are not the ones its configuration and seed plan")
	}
	if rep.Fixture != "" {
		reasons = append(reasons, "fixture: "+rep.Fixture)
	}
	if len(rep.G9Failures) > 0 {
		reasons = append(reasons, fmt.Sprintf("G9 failures: %v", rep.G9Failures))
	}
	if rep.MandatoryFaults != len(want.Mandatory) || rep.MandatoryExecuted != len(want.Mandatory) {
		reasons = append(reasons, fmt.Sprintf("report: %d mandatory faults, %d executed, want %d", rep.MandatoryFaults,
			rep.MandatoryExecuted, len(want.Mandatory)))
	}
	for _, p := range want.Mandatory {
		reasons = append(reasons, mandatoryDone(p, conf.Base.Shared.Faults, rep.Outcomes)...)
	}
	return reasons
}

// mandatoryDone checks that one mandatory fault reached done with its own kind; F-roll as its three F-stop outcomes.
func mandatoryDone(p chaos.Planned, specs map[chaos.Kind]chaos.Spec, outcomes []chaos.Outcome) []string {
	type want struct {
		id   string
		kind chaos.Kind
	}
	wants := []want{{p.Slot, p.Kind}}
	if n := specs[p.Kind].SubFaults; n > 0 {
		wants = nil
		for i := range n {
			wants = append(wants, want{fmt.Sprintf("%s/%d", p.Slot, i+1), chaos.FaultStop})
		}
	}
	var reasons []string
	for _, w := range wants {
		i := slices.IndexFunc(outcomes, func(o chaos.Outcome) bool { return o.ID == w.id })
		switch {
		case i < 0:
			reasons = append(reasons, fmt.Sprintf("no outcome for %s %s", w.id, w.kind))
		case outcomes[i].Kind != w.kind:
			reasons = append(reasons, fmt.Sprintf("outcome %s is %s, want %s", w.id, outcomes[i].Kind, w.kind))
		case outcomes[i].Final != chaos.StateDone:
			reasons = append(reasons, fmt.Sprintf("outcome %s %s is %s: %s", w.id, w.kind, outcomes[i].Final, outcomes[i].Reason))
		}
	}
	return reasons
}
