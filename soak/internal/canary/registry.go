// Package canary holds the phase-1 detector-validation canaries (PLAN §44):
// the registry of §44.2, the validation judgment of §44.3, and the injections of §44.4.
package canary

import (
	"slices"
	"strings"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// Validated and NotValidated are a validation run's results (PLAN §44.3).
const (
	// Validated means the run showed what its canary is for.
	Validated = "validated"
	// NotValidated means it did not, for the listed reasons.
	NotValidated = "not-validated"
)

// minute is one minute of the workload timeline.
const minute = time.Minute

// k11Declared is K11's one declared evidence entry: no LWT completes in the cool-down, so G14 has no p99 for it.
const k11Declared = "G14: missing evidence: class lwt has no cool-down p99"

// Spec is one canary of the table in PLAN §44.2.
type Spec struct {
	// ID is the canary id, e.g. K7.
	ID string
	// Start is when the injection is armed, as an offset from the workload epoch; zero for K10, K12 and K13,
	// whose injections are armed by an event (a churn, G0) rather than a minute.
	Start time.Duration
	// MustFail lists the gates that must all have the status fail.
	MustFail []string
	// Collateral lists the gates that may fail too.
	Collateral []string
	// Declared lists the exact evidence.problems entries the canary may cause, each of a collateral gate (§44.3 rule 3).
	Declared []string
	// ExpectedVersion replaces G0's expected Cassandra version (K12); empty keeps the cell's.
	ExpectedVersion string
	// SkipFinished makes the primary's stream counters drop one in this many finished increments (K6b); 0 for none.
	SkipFinished int64
	// PinKey pins PinnedKey in the primary Env's Excluded and the oracle's sample (K8).
	PinKey bool
	// Switches are set on every Env, primary and aux (K15, K17).
	Switches workload.Switches
	// Implemented is false for the canaries of the second batch (§44.1), which cmd/soak refuses.
	Implemented bool
}

// registry is the table of PLAN §44.2, in its order.
var registry = []Spec{
	{ID: "K1", Start: 12 * minute, MustFail: []string{"G1", "G2"}, Collateral: []string{"G12"}, Implemented: true},
	{ID: "K2", Start: 12 * minute, MustFail: []string{"G2", "G4"}, Collateral: []string{"G12"}, Implemented: true},
	{ID: "K3", Start: 12 * minute, MustFail: []string{"G4"}, Collateral: []string{"G12"}, Implemented: true},
	{ID: "K4", Start: 12 * minute, MustFail: []string{"G3"}, Collateral: []string{"G14"}, Implemented: true},
	{ID: "K5", Start: 12 * minute, MustFail: []string{"G7"}, Implemented: true},
	{ID: "K6b", Start: 12 * minute, MustFail: []string{"G6"}, SkipFinished: 500, Implemented: true},
	{ID: "K7", Start: 12 * minute, MustFail: []string{"G8"}, Implemented: true},
	{ID: "K8", Start: 12 * minute, MustFail: []string{"G10"}, PinKey: true, Implemented: true},
	{ID: "K10", MustFail: []string{"G12", "G13"}, Collateral: []string{"G2"}},
	{ID: "K11", Start: 20 * minute, MustFail: []string{"G11"}, Collateral: []string{"G14"}, Declared: []string{k11Declared}},
	{ID: "K12", ExpectedVersion: "0.0.0-k12", Implemented: true},
	{ID: "K13", MustFail: []string{"G5"}},
	{ID: "K15", MustFail: []string{"G15"}, Switches: workload.Switches{NoEarlyClose: true}, Implemented: true},
	{ID: "K16", Start: 20 * minute, MustFail: []string{"G14"}},
	{ID: "K17", Collateral: []string{"G15"}, Switches: workload.Switches{NoSpeculation: true}, Implemented: true},
}

// All returns every phase-1 canary in the table's order (PLAN §44.2).
//
// Returns:
//   - []Spec: the canaries; the caller may modify the copy
func All() []Spec {
	out := make([]Spec, len(registry))
	for i, s := range registry {
		s.MustFail, s.Collateral, s.Declared = slices.Clone(s.MustFail), slices.Clone(s.Collateral), slices.Clone(s.Declared)
		out[i] = s
	}
	return out
}

// Lookup returns one canary by its id.
//
// Parameters:
//   - id: e.g. K7; ids are case-sensitive
//
// Returns:
//   - Spec: the canary
//   - bool: false for an id that is not a phase-1 canary
func Lookup(id string) (Spec, bool) {
	for _, s := range All() {
		if s.ID == id {
			return s, true
		}
	}
	return Spec{}, false
}

// Kind returns the execution directory's run kind for a canary (PLAN §8.2): the lowercase id, or control for none.
//
// Parameters:
//   - id: the canary id, empty for the control
//
// Returns:
//   - string: e.g. k7 or control
func Kind(id string) string {
	if id == "" {
		return "control"
	}
	return strings.ToLower(id)
}

// cut splits a problem entry "G14: missing evidence: …" into its gate and the rest.
func cut(problem string) (gate, rest string, ok bool) {
	return strings.Cut(problem, ": ")
}
