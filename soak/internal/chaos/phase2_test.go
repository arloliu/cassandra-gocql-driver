package chaos

import (
	"encoding/json"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlanPhaseOneUnchanged(t *testing.T) {
	// Drawing toxic parameters must not move a phase-1 schedule's draws (PLAN §59.4).
	raw, err := os.ReadFile("testdata/plan-phase1-golden.json")
	require.NoError(t, err)
	var want []Schedule
	require.NoError(t, json.Unmarshal(raw, &want))
	nodes := []string{"node1", "node2", "node3"}
	for i, seed := range []uint64{1, 7, 42, 0xdeadbeef} {
		sch, err := Plan(seed, NightTimetable(), PhaseOneAssignment(), true, nodes, PhaseOneOptional(), Specs())
		require.NoError(t, err)
		require.Equal(t, want[i], sch, "seed %d", seed)
	}
}

func TestPlanDrawsToxicParams(t *testing.T) {
	nodes := []string{"node1", "node2", "node3"}
	specs := Specs()
	for _, cell := range []string{"c41p4", "c41p5", "c50p4", "c50p5"} {
		a, err := PhaseTwoAssignment(cell)
		require.NoError(t, err)
		for seed := range uint64(20) {
			sch, err := Plan(seed, NightTimetable(), a, true, nodes, PhaseTwoOptional(), specs)
			require.NoError(t, err)
			for _, p := range append(slices.Clone(sch.Mandatory), sch.Optional...) {
				sp := specs[p.Kind]
				if p.Kind == FaultLat || p.Kind == FaultBW {
					require.GreaterOrEqual(t, p.Param, sp.ParamMin, "%s", p.Kind)
					require.LessOrEqual(t, p.Param, sp.ParamMax, "%s", p.Kind)
				} else {
					require.Zero(t, p.Param, "%s draws no parameter", p.Kind)
				}
			}
		}
	}
	require.Equal(t, [2]int{200, 1500}, [2]int{specs[FaultLat].ParamMin, specs[FaultLat].ParamMax})
	require.Equal(t, [2]int{16, 128}, [2]int{specs[FaultBW].ParamMin, specs[FaultBW].ParamMax})
}

func TestPhaseTwoAssignmentCoverage(t *testing.T) {
	// PLAN §5.4's phase-2 claims, checked on the table itself.
	type cell struct{ id, version, proto string }
	cells := []cell{{"c41p4", "4.1", "4"}, {"c41p5", "4.1", "5"}, {"c50p4", "5.0", "4"}, {"c50p5", "5.0", "5"}}
	seen := map[Kind][]cell{}
	for _, c := range cells {
		a, err := PhaseTwoAssignment(c.id)
		require.NoError(t, err)
		require.Len(t, a.Short, 6)
		require.Contains(t, a.Short, FaultKill)
		require.Contains(t, a.Short, FaultPause)
		for _, k := range a.Short {
			seen[k] = append(seen[k], c)
		}
		seen[a.Long] = append(seen[a.Long], c)
	}
	for k, cs := range seen {
		if k == FaultKill || k == FaultPause {
			require.Len(t, cs, 4, "%s runs in every cell", k)
			continue
		}
		require.Len(t, cs, 2, "%s runs in exactly two cells", k)
		require.NotEqual(t, cs[0].version, cs[1].version, "%s covers both versions", k)
		require.NotEqual(t, cs[0].proto, cs[1].proto, "%s covers both protocols", k)
	}
	for _, k := range []Kind{FaultLat, FaultBW, FaultBlack, FaultHalfOpen, FaultReset, FaultTopo, FaultRoll} {
		require.Contains(t, seen, k)
	}
	_, err := PhaseTwoAssignment("c99p9")
	require.Error(t, err)
}

func TestPhaseTwoOptional(t *testing.T) {
	require.ElementsMatch(t, []Kind{FaultStop, FaultKill, FaultPause, FaultTwo, FaultFull,
		FaultLat, FaultBW, FaultBlack, FaultHalfOpen, FaultReset}, PhaseTwoOptional())
}

func TestSupported(t *testing.T) {
	buildable := func(k Kind) bool { return k == FaultStop || k == FaultLat }
	require.True(t, Supported(FaultStop, buildable))
	require.True(t, Supported(FaultRoll, buildable), "a composite runs as its F-stop sub-faults")
	require.False(t, Supported(FaultTopo, buildable))
	require.False(t, Supported(FaultRoll, func(Kind) bool { return false }))
	require.False(t, Supported("F-nope", func(Kind) bool { return true }), "an unknown kind")
}
