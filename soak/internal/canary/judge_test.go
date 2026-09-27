package canary

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
)

// allPass is a verdict with one passing result per gate G0–G16.
func allPass() gate.Verdict {
	v := gate.Verdict{Status: gate.VerdictPass}
	for i := range 17 {
		v.Gates = append(v.Gates, gate.Result{Gate: fmt.Sprintf("G%d", i), Status: gate.StatusPass})
	}
	return v
}

// withGates returns v with the named gates set to a status; a fail status also makes the verdict fail and lists the gate.
func withGates(v gate.Verdict, status gate.Status, gates ...string) gate.Verdict {
	v.Gates = append([]gate.Result(nil), v.Gates...)
	for i, r := range v.Gates {
		for _, g := range gates {
			if r.Gate == g {
				v.Gates[i].Status = status
				if status == gate.StatusFail {
					v.Gates[i].Details = []string{"fired"}
				}
			}
		}
	}
	if status == gate.StatusFail {
		v.Status = gate.VerdictFail
		v.FailingGates = append(append([]string(nil), v.FailingGates...), gates...)
	}
	return v
}

func judge(id string, v gate.Verdict) Validation {
	return Judge(Input{Canary: id, Verdict: v, Complete: true})
}

func requireValidated(t *testing.T, got Validation) {
	t.Helper()
	require.Equal(t, Validated, got.Result, "%v", got.Reasons)
}

func requireNot(t *testing.T, got Validation, reason string) {
	t.Helper()
	require.Equal(t, NotValidated, got.Result)
	for _, r := range got.Reasons {
		if strings.Contains(r, reason) {
			return
		}
	}
	require.Failf(t, "reason missing", "want a reason containing %q, got %v", reason, got.Reasons)
}

func TestControl(t *testing.T) {
	got := judge("", allPass())
	requireValidated(t, got)
	require.Equal(t, "", got.Canary)
	require.Empty(t, got.Reasons)

	requireNot(t, judge("", withGates(allPass(), gate.StatusFail, "G8")), "G8")
	requireNot(t, judge("", withGates(allPass(), gate.StatusNotEvaluated, "G14")), "G14")
	g16 := withGates(allPass(), gate.StatusFail, "G16")
	g16.Status, g16.FailingGates, g16.Reasons = gate.VerdictFixtureInvalid, nil, []string{"G16: [fired]"}
	requireNot(t, judge("", g16), "G16")
}

func TestRule1InvalidConfigAndIncomplete(t *testing.T) {
	for _, s := range []gate.VerdictStatus{gate.VerdictInvalidConfig, gate.VerdictIncomplete} {
		v := withGates(allPass(), gate.StatusFail, "G8")
		v.Status, v.FailingGates, v.Reasons = s, nil, []string{"something"}
		requireNot(t, judge("K7", v), string(s))
		requireNot(t, judge("", v), string(s))
		k12 := k12Verdict()
		k12.Status = s
		requireNot(t, judge("K12", k12), string(s))
	}
}

// k12Verdict is what a G0 failure produces: fixture-invalid from G0 alone, the workload never ran.
func k12Verdict() gate.Verdict {
	return gate.Verdict{Status: gate.VerdictFixtureInvalid, Reasons: []string{"G0: [127.0.1.1 release_version 5.0.3, want 0.0.0-k12]"},
		Gates: []gate.Result{{Gate: "G0", Status: gate.StatusFail, Details: []string{"x"}},
			{Gate: "G16", Status: gate.StatusNotEvaluated, Details: []string{"the workload did not run"}}}}
}

func TestRule2K12(t *testing.T) {
	requireValidated(t, Judge(Input{Canary: "K12", Verdict: k12Verdict(), Complete: false, Problems: []string{"anything"}}))

	v := k12Verdict()
	v.Reasons = append(v.Reasons, "ccm create: boom")
	requireNot(t, judge("K12", v), "G0")

	v = k12Verdict()
	v.Gates[0].Status = gate.StatusPass
	requireNot(t, judge("K12", v), "G0")

	v = k12Verdict()
	v.Gates[1].Status = gate.StatusPass
	requireNot(t, judge("K12", v), "G16")

	v = k12Verdict()
	v.Status = gate.VerdictFail
	requireNot(t, judge("K12", v), "fixture-invalid")

	v = k12Verdict()
	v.Gates = v.Gates[1:]
	requireNot(t, judge("K12", v), "G0")
}

func TestRule3Inventory(t *testing.T) {
	v := withGates(allPass(), gate.StatusFail, "G8")
	requireValidated(t, judge("K7", v))

	short := v
	short.Gates = v.Gates[:16]
	requireNot(t, judge("K7", short), "G16")
	requireNot(t, judge("", func() gate.Verdict { c := allPass(); c.Gates = c.Gates[1:]; return c }()), "G0")

	dup := v
	dup.Gates = append(append([]gate.Result(nil), v.Gates...), gate.Result{Gate: "G8", Status: gate.StatusFail})
	requireNot(t, judge("K7", dup), "G8")

	extra := v
	extra.Gates = append(append([]gate.Result(nil), v.Gates...), gate.Result{Gate: "G17", Status: gate.StatusPass})
	requireNot(t, judge("K7", extra), "G17")
}

// A must-fail gate that failed only because its evidence is missing is never a detector firing (AQ04).
func TestRule3MissingEvidenceIsNeverADetection(t *testing.T) {
	v := withGates(allPass(), gate.StatusFail, "G1", "G2")
	in := Input{Canary: "K1", Verdict: v, Complete: false, Problems: []string{"G1: missing evidence: t=900s: no goroutineleak profile"}}
	requireNot(t, Judge(in), "missing evidence")

	in = Input{Canary: "K1", Verdict: v, Complete: false}
	requireNot(t, Judge(in), "evidence")
}

func TestRule3DeclaredEvidenceOnlyForItsCanary(t *testing.T) {
	declared := "G14: missing evidence: class lwt has no cool-down p99"
	k11 := withGates(allPass(), gate.StatusFail, "G11", "G14")
	requireValidated(t, Judge(Input{Canary: "K11", Verdict: k11, Complete: false, Problems: []string{declared}}))
	requireNot(t, Judge(Input{Canary: "K11", Verdict: k11, Complete: false, Problems: []string{declared, "G14: missing evidence: class read has no cool-down p99"}}), "class read")
	requireNot(t, Judge(Input{Canary: "K11", Verdict: k11, Complete: false, Problems: []string{declared + " "}}), "missing evidence")

	k16 := withGates(allPass(), gate.StatusFail, "G14")
	requireNot(t, Judge(Input{Canary: "K16", Verdict: k16, Complete: false, Problems: []string{declared}}), "missing evidence")
	requireNot(t, Judge(Input{Canary: "", Verdict: allPass(), Complete: false, Problems: []string{declared}}), "missing evidence")
}

func TestRule3FixtureReasonsAndAnnotations(t *testing.T) {
	v := withGates(allPass(), gate.StatusFail, "G8")
	v.Annotations = []string{"something-else"}
	requireNot(t, judge("K7", v), "annotation")

	fx := allPass()
	fx.Status, fx.Reasons = gate.VerdictFixtureInvalid, []string{"fault: F-stop install failed"}
	requireNot(t, judge("K7", fx), "fault: F-stop")
}

func TestRule5MustFailCollateralAndOthers(t *testing.T) {
	k1 := withGates(allPass(), gate.StatusFail, "G1", "G2")
	requireValidated(t, judge("K1", k1))
	requireValidated(t, judge("K1", withGates(k1, gate.StatusFail, "G12")))

	requireNot(t, judge("K1", withGates(allPass(), gate.StatusFail, "G1")), "G2")
	requireNot(t, judge("K1", withGates(k1, gate.StatusFail, "G4")), "G4")
	requireNot(t, judge("K1", withGates(k1, gate.StatusNotEvaluated, "G12")), "G12")
	requireNot(t, judge("K1", withGates(k1, gate.StatusNotEvaluated, "G2")), "G2")
	requireNot(t, judge("K1", withGates(k1, gate.StatusNotEvaluated, "G16")), "G16")

	// K17 has no must-fail gate: G15 may fail or pass.
	requireValidated(t, Judge(Input{Canary: "K17", Verdict: allPass(), Complete: true}))
	requireValidated(t, Judge(Input{Canary: "K17", Verdict: withGates(allPass(), gate.StatusFail, "G15"), Complete: true}))
}

func TestRule5Assertion(t *testing.T) {
	v := withGates(allPass(), gate.StatusFail, "G8")
	requireNot(t, Judge(Input{Canary: "K7", Verdict: v, Complete: true, Assertion: []string{"no canary error record out of a window with class unknown"}}), "class unknown")
}

// Rule 6: a G16 violation beside the must-fail gates keeps the canary validated, with the annotation as a note.
func TestRule6FixtureSuspect(t *testing.T) {
	v := withGates(allPass(), gate.StatusFail, "G8")
	v = withGates(v, gate.StatusFail, "G16")
	v.FailingGates = []string{"G8"}
	v.Annotations = []string{gate.AnnotationFixtureSuspect}
	got := judge("K7", v)
	requireValidated(t, got)
	require.Len(t, got.Reasons, 1)
	require.Contains(t, got.Reasons[0], "note")
	require.Contains(t, got.Reasons[0], gate.AnnotationFixtureSuspect)
}

func TestUnknownCanary(t *testing.T) {
	requireNot(t, judge("K9", allPass()), "K9")
}
