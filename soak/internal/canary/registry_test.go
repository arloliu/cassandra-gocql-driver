package canary

import (
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// The registry is PLAN §44.2 as data: every phase-1 canary, in the table's order.
func TestRegistryIsTheCanaryTable(t *testing.T) {
	var ids []string
	for _, s := range All() {
		ids = append(ids, s.ID)
	}
	require.Equal(t, []string{"K1", "K2", "K3", "K4", "K5", "K6b", "K7", "K8", "K10", "K11", "K12", "K13", "K15", "K16", "K17"}, ids)

	type row struct {
		start          time.Duration
		must, coll     []string
		declared       []string
		implemented    bool
		expectsVersion bool
	}
	min := func(m float64) time.Duration { return time.Duration(m * float64(time.Minute)) }
	want := map[string]row{
		"K1":  {min(12), []string{"G1", "G2"}, []string{"G12"}, nil, true, false},
		"K2":  {min(12), []string{"G2", "G4"}, []string{"G12"}, nil, true, false},
		"K3":  {min(12), []string{"G4"}, []string{"G12"}, nil, true, false},
		"K4":  {min(12), []string{"G3"}, nil, nil, true, false},
		"K5":  {min(12), []string{"G7"}, nil, nil, true, false},
		"K6b": {min(12), []string{"G6"}, nil, nil, true, false},
		"K7":  {min(12), []string{"G8"}, nil, nil, true, false},
		"K8":  {min(12), []string{"G10"}, nil, nil, true, false},
		"K10": {0, []string{"G12", "G13"}, []string{"G2"}, nil, false, false},
		"K11": {min(20), []string{"G11"}, []string{"G14"}, []string{"G14: missing evidence: class lwt has no cool-down p99"}, false, false},
		"K12": {0, nil, nil, nil, true, true},
		"K13": {0, []string{"G5"}, nil, nil, false, false},
		"K15": {0, []string{"G15"}, nil, nil, true, false},
		"K16": {min(20), []string{"G14"}, nil, nil, false, false},
		"K17": {0, nil, []string{"G15"}, nil, true, false},
	}
	for _, s := range All() {
		w := want[s.ID]
		require.Equal(t, w.start, s.Start, s.ID)
		require.Equal(t, w.must, s.MustFail, s.ID)
		require.Equal(t, w.coll, s.Collateral, s.ID)
		require.Equal(t, w.declared, s.Declared, s.ID)
		require.Equal(t, w.implemented, s.Implemented, s.ID)
		require.Equal(t, w.expectsVersion, s.ExpectedVersion != "", s.ID)
		for _, d := range s.Declared {
			gate, _, ok := cut(d)
			require.True(t, ok, d)
			require.Contains(t, s.Collateral, gate, "%s: a declared evidence entry belongs to a collateral gate", s.ID)
		}
		require.Empty(t, intersect(s.MustFail, s.Collateral), s.ID)
	}
}

// The session and Env hooks are data too, one seam each (PLAN §44.5); every other canary installs none of them.
func TestRegistryHooks(t *testing.T) {
	for _, s := range All() {
		wantSkip, wantPin, wantSwitches := int64(0), false, workload.Switches{}
		switch s.ID {
		case "K6b":
			wantSkip = 500
		case "K8":
			wantPin = true
		case "K15":
			wantSwitches.NoEarlyClose = true
		case "K17":
			wantSwitches.NoSpeculation = true
		}
		require.Equal(t, wantSkip, s.SkipFinished, s.ID)
		require.Equal(t, wantPin, s.PinKey, s.ID)
		require.Equal(t, wantSwitches, s.Switches, s.ID)
	}
	none, ok := Lookup("")
	require.False(t, ok)
	require.Zero(t, none, "no canary, no hook")
}

func TestLookupAndKind(t *testing.T) {
	s, ok := Lookup("K6b")
	require.True(t, ok)
	require.Equal(t, "K6b", s.ID)
	require.Equal(t, "k6b", Kind("K6b"))
	require.Equal(t, "control", Kind(""))
	_, ok = Lookup("k7")
	require.False(t, ok, "ids are case-sensitive")
	_, ok = Lookup("K9")
	require.False(t, ok, "K9 is phase 2")
}

func TestK12ExpectsAVersionNoCellRuns(t *testing.T) {
	s, _ := Lookup("K12")
	for _, v := range []string{"4.1.12", "5.0.3"} {
		require.NotEqual(t, v, s.ExpectedVersion)
	}
}

func intersect(a, b []string) []string {
	var out []string
	for _, x := range a {
		if slices.Contains(b, x) {
			out = append(out, x)
		}
	}
	return out
}
