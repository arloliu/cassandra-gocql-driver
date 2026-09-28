package main

import (
	"flag"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/canary"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cellrun"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
)

func TestCellCanaryFlag(t *testing.T) {
	parse := func(args ...string) (cellrun.Options, error) { return parseCellArgs(args, flag.ContinueOnError) }

	o, err := parse("-mode", "validate", "-canary", "K7")
	require.NoError(t, err)
	require.Equal(t, "K7", o.Canary)
	require.Equal(t, "k7", o.RunKind, "the run kind is the lowercase id (PLAN §8.2)")

	o, err = parse("-mode", "validate", "-canary", "K6b", "-kind", "k6b")
	require.NoError(t, err)
	require.Equal(t, "k6b", o.RunKind)

	o, err = parse("-mode", "validate")
	require.NoError(t, err)
	require.Equal(t, "control", o.RunKind)
	require.Empty(t, o.Canary)

	o, err = parse("-mode", "night")
	require.NoError(t, err)
	require.Equal(t, "night", o.RunKind)

	for name, args := range map[string][]string{
		"night mode":           {"-mode", "night", "-canary", "K7"},
		"default mode (night)": {"-canary", "K7"},
		"a disagreeing kind":   {"-mode", "validate", "-canary", "K7", "-kind", "control"},
		"a canary kind, none":  {"-mode", "validate", "-kind", "k7"},
		"unknown id":           {"-mode", "validate", "-canary", "K9"},
		"lowercase id":         {"-mode", "validate", "-canary", "k7"},
		"unknown mode":         {"-mode", "calibrate"},
	} {
		_, err := parse(args...)
		require.Error(t, err, name)
	}
	for _, s := range canary.All() {
		_, err := parse("-mode", "validate", "-canary", s.ID)
		require.Equal(t, s.Implemented, err == nil, "%s: %v", s.ID, err)
	}
}

// Validation mode exits 0 on validated and 1 on not-validated (PLAN §44.3); night mode is unchanged.
func TestCellExitCodes(t *testing.T) {
	pass := cellrun.VerdictFile{Verdict: gate.Verdict{Status: gate.VerdictPass}}
	fail := cellrun.VerdictFile{Verdict: gate.Verdict{Status: gate.VerdictFail}}
	require.Equal(t, 0, cellExit(gate.ModeNight, pass))
	require.Equal(t, 1, cellExit(gate.ModeNight, fail))

	validated := fail
	validated.Validation = &canary.Validation{Canary: "K7", Result: canary.Validated}
	require.Equal(t, 0, cellExit(gate.ModeValidate, validated), "a canary's fail verdict is its success")
	notValidated := pass
	notValidated.Validation = &canary.Validation{Result: canary.NotValidated}
	require.Equal(t, 1, cellExit(gate.ModeValidate, notValidated))
	require.Equal(t, 2, cellExit(gate.ModeValidate, pass), "no judgment: it could not run")
}

// run-validation.sh's canonical slot order is the registry's (PLAN §44.2).
func TestRunValidationCanonicalOrder(t *testing.T) {
	raw, err := os.ReadFile("../../run-validation.sh")
	require.NoError(t, err)
	m := regexp.MustCompile(`(?m)^CANONICAL=\(([^)]*)\)$`).FindSubmatch(raw)
	require.NotNil(t, m, "run-validation.sh declares CANONICAL=(…)")
	var ids []string
	for _, s := range canary.All() {
		ids = append(ids, s.ID)
	}
	require.Equal(t, ids, strings.Fields(string(m[1])))
}
