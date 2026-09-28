package cellrun

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/canary"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// coverageEvent is the coverage event's payload in events.jsonl: its writer type (run.go) and its reader's.
type coverageEvent struct {
	Settlement            workload.SettlementTotals `json:"settlement"`
	ShortDeadlineTimeouts int                       `json:"short_deadline_timeouts"`
	ErrorClasses          map[string]int64          `json:"error_classes"`
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
// and from the persisted records its canary's extra assertion reads (§44.8): the canary's events (K16's final counts among them),
// the coverage event, and for K7 the errors.jsonl lines. Every record is strict-decoded with its writer's type;
// a missing or altered input is a reason, never a zero.
//
// Parameters:
//   - dir: the execution directory, its streams closed
//   - id: the canary id, empty for the control
//   - v: the final gate verdict
//   - ev: the run's evidence
//
// Returns:
//   - canary.Validation: the judgment
func judgeRun(dir, id string, v gate.Verdict, ev Evidence) canary.Validation {
	var assertion []string
	switch id {
	case "K7", "K8", "K10", "K13", "K15", "K16", "K17":
		rec, problems := readRecords(dir, id == "K7")
		assertion = append(problems, canary.Assert(id, v, rec)...)
	}
	return canary.Judge(canary.Input{Canary: id, Verdict: v, Complete: ev.Complete, Problems: ev.Problems, Assertion: assertion})
}

// readRecords reads the canary events and the coverage event from events.jsonl, and errors.jsonl when asked.
func readRecords(dir string, withErrors bool) (canary.Records, []string) {
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
		}
		return nil
	})
	if err != nil {
		problems = append(problems, "events.jsonl: "+err.Error())
	}
	if !withErrors {
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
