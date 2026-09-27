package canary

import (
	"fmt"
	"slices"
	"strings"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
)

// gateCount is the number of gates a verdict carries: G0–G16.
const gateCount = 17

// Validation is verdict.json's validation object (PLAN §44.3), written only in validation mode.
type Validation struct {
	// Canary is the canary id; empty for a control run, and always written.
	Canary string `json:"canary"`
	// Result is validated or not-validated.
	Result string `json:"result"`
	// Reasons lists every rule that failed, and notes such as a fixture-suspect annotation.
	Reasons []string `json:"reasons,omitempty"`
}

// Input is what the judgment reads: the final gate verdict, the evidence, and the extra assertion's outcome.
type Input struct {
	// Canary is the canary id, empty for the control.
	Canary string
	// Verdict is the unchanged gate verdict.
	Verdict gate.Verdict
	// Complete and Problems are verdict.json's evidence.
	Complete bool
	Problems []string
	// Assertion lists why the canary's extra assertion (§44.2) failed, or why its input is missing; empty when it held.
	Assertion []string
}

// Judge decides a validation run (PLAN §44.3).
// The rules are evaluated in order, and every failing one is listed.
//
// Parameters:
//   - in: the verdict, the evidence and the assertion
//
// Returns:
//   - Validation: validated only when no rule failed
func Judge(in Input) Validation {
	out := Validation{Canary: in.Canary}
	var failed, notes []string
	fail := func(format string, args ...any) { failed = append(failed, fmt.Sprintf(format, args...)) }
	v := in.Verdict

	// Rule 1.
	if v.Status == gate.VerdictInvalidConfig || v.Status == gate.VerdictIncomplete {
		fail("rule 1: the verdict is %s: %s", v.Status, strings.Join(v.Reasons, "; "))
	}
	spec, known := Lookup(in.Canary)
	switch {
	case in.Canary != "" && !known:
		fail("canary %q is not a phase-1 canary", in.Canary)
	case in.Canary == "K12":
		rule2(v, fail)
	default:
		inventory(v, fail)
		evidence(spec, in.Complete, in.Problems, fail)
		fixtureAndAnnotations(v, fail)
		if in.Canary == "" {
			for _, r := range v.Gates {
				if r.Status != gate.StatusPass {
					fail("rule 4: control gate %s is %s", r.Gate, r.Status)
				}
			}
		} else {
			rule5(spec, v, fail)
			for _, a := range in.Assertion {
				fail("rule 5: extra assertion: %s", a)
			}
			if slices.Contains(v.Annotations, gate.AnnotationFixtureSuspect) {
				notes = append(notes, "note (rule 6): the verdict is annotated "+gate.AnnotationFixtureSuspect+": G16 failed beside the driver gates")
			}
		}
	}
	out.Reasons = append(failed, notes...)
	out.Result = Validated
	if len(failed) > 0 {
		out.Result = NotValidated
	}
	return out
}

// rule2 is K12's rule: G0 alone made the run fixture-invalid, and the workload never ran.
func rule2(v gate.Verdict, fail func(string, ...any)) {
	if v.Status != gate.VerdictFixtureInvalid {
		fail("rule 2: the verdict is %s, want fixture-invalid", v.Status)
	}
	if len(v.Reasons) == 0 {
		fail("rule 2: no reason")
	}
	for _, r := range v.Reasons {
		if !strings.HasPrefix(r, "G0: ") {
			fail("rule 2: a reason not from G0: %s", r)
		}
	}
	if s, ok := statusOf(v, "G0"); !ok || s != gate.StatusFail {
		fail("rule 2: G0 is %s, want fail", show(s, ok))
	}
	if s, ok := statusOf(v, "G16"); !ok || s != gate.StatusNotEvaluated {
		fail("rule 2: G16 is %s, want not-evaluated", show(s, ok))
	}
}

// inventory is rule 3's first clause: exactly one result for each of G0–G16.
func inventory(v gate.Verdict, fail func(string, ...any)) {
	seen := map[string]int{}
	for _, r := range v.Gates {
		seen[r.Gate]++
	}
	for i := range gateCount {
		g := fmt.Sprintf("G%d", i)
		if n := seen[g]; n != 1 {
			fail("rule 3: %d results for %s, want 1", n, g)
		}
		delete(seen, g)
	}
	for _, g := range slices.Sorted(func(yield func(string) bool) {
		for g := range seen {
			if !yield(g) {
				return
			}
		}
	}) {
		fail("rule 3: a result for %s, which is not a gate", g)
	}
}

// evidence is rule 3's second clause: complete evidence, or only the canary's declared entries.
func evidence(spec Spec, complete bool, problems []string, fail func(string, ...any)) {
	if !complete && len(problems) == 0 {
		fail("rule 3: the evidence is incomplete and names no problem")
	}
	for _, p := range problems {
		if !slices.Contains(spec.Declared, p) {
			fail("rule 3: undeclared evidence problem: %s", p)
		}
	}
}

// fixtureAndAnnotations is rule 3's last clause: no fixture-invalid reason but G16's, no annotation but fixture-suspect.
func fixtureAndAnnotations(v gate.Verdict, fail func(string, ...any)) {
	if v.Status == gate.VerdictFixtureInvalid {
		for _, r := range v.Reasons {
			if !strings.HasPrefix(r, "G16: ") {
				fail("rule 3: a fixture-invalid reason other than G16's: %s", r)
			}
		}
	}
	for _, a := range v.Annotations {
		if a != gate.AnnotationFixtureSuspect {
			fail("rule 3: annotation %q", a)
		}
	}
}

// rule5 checks the gate statuses of a canary run.
func rule5(spec Spec, v gate.Verdict, fail func(string, ...any)) {
	for _, r := range v.Gates {
		switch {
		case slices.Contains(spec.MustFail, r.Gate):
			if r.Status != gate.StatusFail {
				fail("rule 5: must-fail gate %s is %s", r.Gate, r.Status)
			}
		case slices.Contains(spec.Collateral, r.Gate):
			if r.Status != gate.StatusPass && r.Status != gate.StatusFail {
				fail("rule 5: collateral gate %s is %s", r.Gate, r.Status)
			}
		case r.Gate == "G16":
			if r.Status != gate.StatusPass && r.Status != gate.StatusFail {
				fail("rule 5: G16 is %s, want pass or fail", r.Status)
			}
		default:
			if r.Status != gate.StatusPass {
				fail("rule 5: gate %s is %s, and is neither must-fail nor collateral", r.Gate, r.Status)
			}
		}
	}
}

func statusOf(v gate.Verdict, g string) (gate.Status, bool) {
	for _, r := range v.Gates {
		if r.Gate == g {
			return r.Status, true
		}
	}
	return "", false
}

func show(s gate.Status, ok bool) string {
	if !ok {
		return "missing"
	}
	return string(s)
}
