package cellrun

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/canary"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// coverageEvent is the coverage event's payload in events.jsonl: its writer type (run.go) and its reader's.
type coverageEvent struct {
	Settlement            workload.SettlementTotals `json:"settlement"`
	ShortDeadlineTimeouts int                       `json:"short_deadline_timeouts"`
	ErrorClasses          map[string]int64          `json:"error_classes"`
}

// inputs says which records an assertion reads beyond the canary and coverage events.
type inputs struct {
	// errors is errors.jsonl; faults the fault and report events; samples samples.jsonl; schedule schedule.json.
	errors, faults, samples, schedule bool
}

// applyEnvHooks installs a canary's Env hooks (PLAN §44.5): K8's pinned key on the primary,
// K15's and K17's switches on the primary and, through the churner, on every aux Env; a zero Spec installs none.
func applyEnvHooks(spec canary.Spec, primary *workload.Env, churner *Churner) {
	if spec.PinKey {
		k := canary.PinnedKey()
		primary.Excluded = &k
	}
	primary.Switches, churner.Switches = spec.Switches, spec.Switches
}

// judgeRun decides a validation run (PLAN §44.3) from its final verdict and evidence,
// and from the persisted records its canary's extra assertion reads (§44.8, §67.7):
// the canary's events (K16's final counts among them), the coverage event, for K7 the errors.jsonl lines,
// for K6a and K9 the fault transitions and the report event, for K6a the cheap samples and for K9 schedule.json.
// Every record is strict-decoded with its writer's type;
// a missing or altered input is a reason, never a zero.
//
// Parameters:
//   - dir: the execution directory, its streams closed
//   - id: the canary id, empty for the control
//   - v: the final gate verdict
//   - ev: the run's evidence
//   - th: the run's thresholds, which K6a's assertion reads kS from
//
// Returns:
//   - canary.Validation: the judgment
func judgeRun(dir, id string, v gate.Verdict, ev Evidence, th gate.Thresholds) canary.Validation {
	var assertion []string
	switch id {
	case "K6a", "K7", "K8", "K9", "K10", "K13", "K15", "K16", "K17":
		rec, problems := readRecords(dir, inputsOf(id))
		rec.Thresholds = th
		assertion = append(problems, canary.Assert(id, v, rec)...)
	}
	return canary.Judge(canary.Input{Canary: id, Verdict: v, Complete: ev.Complete, Problems: ev.Problems, Assertion: assertion})
}

// inputsOf returns a canary's inputs (PLAN §44.8, §67.7).
func inputsOf(id string) inputs {
	switch id {
	case "K7":
		return inputs{errors: true}
	case "K6a":
		return inputs{faults: true, samples: true}
	case "K9":
		return inputs{faults: true, schedule: true}
	}
	return inputs{}
}

// readRecords reads the canary events and the coverage event from events.jsonl, and the other inputs asked for.
func readRecords(dir string, in inputs) (canary.Records, []string) {
	var rec canary.Records
	var problems []string
	strict := func(what string, raw []byte, v any) bool {
		d, err := strictDecode(raw, v)
		switch {
		case err != nil:
			problems = append(problems, fmt.Sprintf("%s: %v", what, err))
			return false
		case d != "":
			problems = append(problems, fmt.Sprintf("%s is not what the harness writes: %s", what, d))
			return false
		}
		return true
	}
	n := 0
	err := eachRecord(filepath.Join(dir, "events.jsonl"), func(line []byte) error {
		n++
		// Every line is decoded before it is dispatched, so a corrupt line is a reason wherever it is (Codex AT02).
		var e envelope
		what := fmt.Sprintf("events.jsonl line %d", n)
		if !strict(what, line, &e) {
			return nil
		}
		switch e.Kind {
		case "":
			problems = append(problems, what+": no kind")
		case "canary":
			var c canary.Event
			if strict(what+" (canary)", e.Data, &c) {
				rec.Events = append(rec.Events, c)
			}
		case "coverage":
			var c coverageEvent
			if strict(what+" (coverage)", e.Data, &c) {
				if rec.Coverage != nil {
					problems = append(problems, what+": a second coverage event")
				}
				rec.Coverage = &c.Settlement
			}
		case "fault":
			var tr chaos.Transition
			switch {
			case !in.faults || !strict(what+" (fault)", e.Data, &tr):
			case !e.Time.Equal(tr.Time):
				// The recorder stamps a transition's event with the transition's own time (run.go, onTransition).
				problems = append(problems, fmt.Sprintf("%s (fault): the event's time %s is not its transition's %s",
					what, e.Time.Format(time.RFC3339Nano), tr.Time.Format(time.RFC3339Nano)))
			default:
				rec.Transitions = append(rec.Transitions, canary.Transition{T: e.T, Transition: tr})
			}
		case "report":
			var rep chaos.Report
			if in.faults && strict(what+" (report)", e.Data, &rep) {
				if rec.Report != nil {
					problems = append(problems, what+": a second report event")
				}
				rec.Report = &rep
			}
		}
		return nil
	})
	if err != nil {
		problems = append(problems, "events.jsonl: "+err.Error())
	}
	if in.samples {
		readStreams(dir, &rec, strict, &problems)
	}
	if in.schedule {
		if raw, err := os.ReadFile(filepath.Join(dir, "schedule.json")); err != nil {
			problems = append(problems, "schedule.json: "+err.Error())
		} else if sch := (chaos.Schedule{}); strict("schedule.json", raw, &sch) {
			rec.Schedule = &sch
		}
	}
	if !in.errors {
		return rec, problems
	}
	rec.Errors = []canary.ErrorRef{}
	n = 0
	err = eachRecord(filepath.Join(dir, "errors.jsonl"), func(line []byte) error {
		n++
		var l ErrorLine
		if strict(fmt.Sprintf("errors.jsonl line %d", n), line, &l) {
			rec.Errors = append(rec.Errors, canary.ErrorRef{OpID: l.OpID, InWindow: l.InWindow, Class: l.Class})
		}
		return nil
	})
	if err != nil {
		problems = append(problems, "errors.jsonl: "+err.Error())
	}
	return rec, problems
}

// readStreams reads the primary's stream counters from samples.jsonl's cheap samples (K6a);
// every line is decoded with its kind's writer type, so a corrupt line of any kind is a reason.
func readStreams(dir string, rec *canary.Records, strict func(string, []byte, any) bool, problems *[]string) {
	n := 0
	err := eachRecord(filepath.Join(dir, "samples.jsonl"), func(line []byte) error {
		n++
		what := fmt.Sprintf("samples.jsonl line %d", n)
		var head struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(line, &head); err != nil {
			*problems = append(*problems, fmt.Sprintf("%s: %v", what, err))
			return nil
		}
		switch head.Kind {
		case "cheap":
			var c Cheap
			if strict(what+" (cheap)", line, &c) {
				s := canary.StreamSample{T: c.T}
				if p, ok := c.Streams["primary"]; ok {
					s.Present, s.Started, s.Finished, s.Abandoned, s.Balance = true, p.Started, p.Finished, p.Abandoned, p.Balance
				}
				rec.Streams = append(rec.Streams, s)
			}
		case "profile":
			strict(what+" (profile)", line, &Profile{})
		case "latency":
			strict(what+" (latency)", line, &latencyRecord{})
		default:
			*problems = append(*problems, fmt.Sprintf("%s: kind %q is not one the sampler writes", what, head.Kind))
		}
		return nil
	})
	if err != nil {
		*problems = append(*problems, "samples.jsonl: "+err.Error())
	}
}

// eachRecord calls fn with every line of a JSONL file, blank ones included: the writers never write one,
// so a blank line reaches the strict decoder and is a reason (Codex AU03).
func eachRecord(path string, fn func([]byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	for sc.Scan() {
		if err := fn(sc.Bytes()); err != nil {
			return err
		}
	}
	return sc.Err()
}
