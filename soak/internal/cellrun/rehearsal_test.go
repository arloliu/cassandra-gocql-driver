package cellrun

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/toxic"
)

// refusal is one way a rehearsal's artifacts must not be judged green.
type refusal struct {
	edit func(t *testing.T, dir string)
	want string
}

func TestRehearsalGreen(t *testing.T) {
	r := judge(t, rehearsalFixture(t))
	require.True(t, r.Green, "%v", r.Reasons)
}

func TestRehearsalUncertainLWTOnlyIsGreen(t *testing.T) {
	// G10 fails on kLWTu alone; its oracle lists are empty, so the rehearsal stays green (PLAN §59.7).
	dir := rehearsalFixture(t)
	editVerdict(t, dir, func(t *testing.T, v map[string]any) { setGate(t, v, "G10", "fail") })
	r := judge(t, dir)
	require.True(t, r.Green, "%v", r.Reasons)
}

func TestRehearsalRefusesVerdicts(t *testing.T) {
	runRefusals(t, map[string]refusal{
		"incomplete evidence": {func(t *testing.T, dir string) {
			editVerdict(t, dir, func(_ *testing.T, v map[string]any) {
				v["evidence"] = map[string]any{"complete": false, "problems": []any{"x"}}
			})
		}, "evidence is incomplete"},
		"a required event missing despite complete evidence": {func(t *testing.T, dir string) {
			filterLines(t, filepath.Join(dir, "events.jsonl"), `"kind":"counters"`)
		}, "missing evidence"},
		"another cell": {func(t *testing.T, dir string) {
			editVerdict(t, dir, func(_ *testing.T, v map[string]any) { v["cell"] = "c41p5" })
		}, "want \"c50p5\""},
		"not final": {func(t *testing.T, dir string) {
			editVerdict(t, dir, func(_ *testing.T, v map[string]any) { v["final"] = false })
		}, "not final"},
		"G16 alone": {func(t *testing.T, dir string) {
			editVerdict(t, dir, func(t *testing.T, v map[string]any) {
				v["status"] = "fixture-invalid"
				setGate(t, v, "G16", "fail")
			})
		}, "G16 is fail"},
		"G16 with driver gates": {func(t *testing.T, dir string) {
			editVerdict(t, dir, func(_ *testing.T, v map[string]any) { v["annotations"] = []any{"fixture-suspect"} })
		}, "fixture-suspect"},
		"a fixed gate failing": {func(t *testing.T, dir string) {
			editVerdict(t, dir, func(t *testing.T, v map[string]any) { setGate(t, v, "G8", "fail") })
		}, "G8 is fail"},
	})
}

func TestRehearsalRefusesEvents(t *testing.T) {
	runRefusals(t, map[string]refusal{
		"a mandatory fault not done": {func(t *testing.T, dir string) {
			editOutcome(t, dir, "M3", "final", "failed")
		}, "outcome M3"},
		"a wrong-kind outcome with the right id": {func(t *testing.T, dir string) {
			editOutcome(t, dir, "M6", "kind", "F-two")
		}, "outcome M6 is F-two"},
		"an F-roll sub-fault missing": {func(t *testing.T, dir string) {
			editEvent(t, dir, "report", func(t *testing.T, r map[string]any) {
				outcomes, ok := r["outcomes"].([]any)
				require.True(t, ok)
				r["outcomes"] = slices.DeleteFunc(outcomes, func(o any) bool {
					m, ok := o.(map[string]any)
					return ok && m["id"] == "L/2"
				})
			})
		}, "no outcome for L/2"},
		"report counts unbound": {func(t *testing.T, dir string) {
			editEvent(t, dir, "report", func(_ *testing.T, r map[string]any) { r["mandatory_faults"], r["mandatory_executed"] = 7, 7 })
		}, "mandatory faults, 7 executed"},
		"a G7 counter": {func(t *testing.T, dir string) {
			editEvent(t, dir, "counters", func(t *testing.T, d map[string]any) {
				g7, ok := d["g7"].(map[string]any)
				require.True(t, ok)
				g7["NoHandler"] = 1
			})
		}, "G7 counters"},
		"a register violation": {func(t *testing.T, dir string) {
			editEvent(t, dir, "oracles", func(_ *testing.T, d map[string]any) { d["register"] = []any{"key 1: lost write"} })
		}, "oracle violations"},
		"a runtime violation": {func(t *testing.T, dir string) {
			f, err := os.OpenFile(filepath.Join(dir, "events.jsonl"), os.O_APPEND|os.O_WRONLY, 0)
			require.NoError(t, err)
			_, err = f.WriteString(`{"t":7000,"time":"2026-09-27T05:56:20+08:00","kind":"violation","data":"key 2: stale read"}` + "\n")
			require.NoError(t, err)
			require.NoError(t, f.Close())
		}, "runtime 1"},
	})
}

func TestRehearsalRefusesSchedules(t *testing.T) {
	editMandatory := func(edit func([]any) any) func(t *testing.T, dir string) {
		return func(t *testing.T, dir string) {
			editJSONFile(t, filepath.Join(dir, "schedule.json"), func(t *testing.T, s map[string]any) {
				m, ok := s["mandatory"].([]any)
				require.True(t, ok)
				s["mandatory"] = edit(m)
			})
		}
	}
	runRefusals(t, map[string]refusal{
		"mandatory null":     {editMandatory(func([]any) any { return nil }), "mandatory faults are not"},
		"a removed entry":    {editMandatory(func(m []any) any { return m[1:] }), "mandatory faults are not"},
		"a duplicated entry": {editMandatory(func(m []any) any { return append(m, m[0]) }), "mandatory faults are not"},
		"an empty parameter range": {func(t *testing.T, dir string) {
			// A configuration that schedules F-lat with an empty range is judged, not a panic (§65 BX03).
			editJSONFile(t, filepath.Join(dir, "config.json"), func(t *testing.T, c map[string]any) {
				shared := nested(t, c, "base", "shared")
				optional, ok := shared["optional"].([]any)
				require.True(t, ok)
				shared["optional"] = append(optional, "F-lat")
				lat := nested(t, shared, "faults", "F-lat")
				lat["ParamMin"], lat["ParamMax"] = 200, 199
			})
		}, "parameter range"},
	})
}

// Night A's schedule.json roundtrips strictly through chaos.Schedule after Planned gained Param (PLAN §59.4).
func TestNightAScheduleRoundtrips(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(calibrationFixture, "schedule.json"))
	require.NoError(t, err)
	var sch chaos.Schedule
	diff, err := strictDecode(raw, &sch)
	require.NoError(t, err)
	require.Empty(t, diff)
	require.Len(t, sch.Mandatory, 8)
}

// The toxic faults' events roundtrip strictly through their writers' types (PLAN §59.5).
func TestToxicEventsRoundtrip(t *testing.T) {
	round := toxic.Round{Fault: 3, Stage: "up", N: 2, Held: true, Detail: "silent 3s"}
	raw, err := json.Marshal(round)
	require.NoError(t, err)
	var r toxic.Round
	d, err := strictDecode(raw, &r)
	require.NoError(t, err)
	require.Empty(t, d)

	rec := toxic.Record{Fault: 3, Kind: chaos.FaultBlack, Proxy: "node2", Toxics: toxic.Toxics(chaos.FaultBlack, 3, 0)}
	raw, err = json.Marshal(rec)
	require.NoError(t, err)
	var back toxic.Record
	d, err = strictDecode(raw, &back)
	require.NoError(t, err)
	require.Empty(t, d)
}

// rehearsalFixture copies night A's c50p5 cell, with its own schedule.json, and makes its verdict that of a green
// rehearsal: a fail on provisional gates, with every fixed gate passing.
func rehearsalFixture(t *testing.T) string {
	t.Helper()
	dir := copyFixture(t)
	editVerdict(t, dir, func(t *testing.T, v map[string]any) {
		v["status"] = "fail"
		v["failing_gates"] = []any{"G3"}
		delete(v, "reasons")
		setGate(t, v, "G16", "pass")
	})
	return dir
}

func runRefusals(t *testing.T, cases map[string]refusal) {
	t.Helper()
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			dir := rehearsalFixture(t)
			tc.edit(t, dir)
			r := judge(t, dir)
			require.False(t, r.Green)
			require.True(t, slices.ContainsFunc(r.Reasons, func(s string) bool { return strings.Contains(s, tc.want) }), "%v", r.Reasons)
		})
	}
}

func judge(t *testing.T, dir string) Rehearsal {
	t.Helper()
	r, err := JudgeRehearsal(dir, "c50p5")
	require.NoError(t, err)
	return r
}

// setGate sets one gate's status in a verdict, dropping its details.
func setGate(t *testing.T, v map[string]any, gate, status string) {
	t.Helper()
	gates, ok := v["gates"].([]any)
	require.True(t, ok)
	for _, g := range gates {
		m, ok := g.(map[string]any)
		require.True(t, ok)
		if m["gate"] == gate {
			m["status"] = status
			delete(m, "details")
		}
	}
}

// nested walks a decoded JSON object by keys.
func nested(t *testing.T, m map[string]any, keys ...string) map[string]any {
	t.Helper()
	for _, k := range keys {
		next, ok := m[k].(map[string]any)
		require.True(t, ok, "key %s", k)
		m = next
	}
	return m
}

func writeJSONFile(t *testing.T, path string, v any) {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0o644))
}

func editVerdict(t *testing.T, dir string, edit func(*testing.T, map[string]any)) {
	t.Helper()
	editJSONFile(t, filepath.Join(dir, "verdict.json"), edit)
}

func editJSONFile(t *testing.T, path string, edit func(*testing.T, map[string]any)) {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var v map[string]any
	require.NoError(t, json.Unmarshal(raw, &v))
	edit(t, v)
	writeJSONFile(t, path, v)
}

// editOutcome sets one field of one outcome of the report event.
func editOutcome(t *testing.T, dir, id, field, value string) {
	t.Helper()
	editEvent(t, dir, "report", func(t *testing.T, r map[string]any) {
		outcomes, ok := r["outcomes"].([]any)
		require.True(t, ok)
		for _, o := range outcomes {
			m, ok := o.(map[string]any)
			require.True(t, ok)
			if m["id"] == id {
				m[field] = value
			}
		}
	})
}

// editEvent rewrites the data of every event of one kind.
func editEvent(t *testing.T, dir, kind string, edit func(*testing.T, map[string]any)) {
	t.Helper()
	path := filepath.Join(dir, "events.jsonl")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	for i, l := range lines {
		if !strings.Contains(l, `"kind":"`+kind+`"`) {
			continue
		}
		var e map[string]any
		require.NoError(t, json.Unmarshal([]byte(l), &e))
		data, ok := e["data"].(map[string]any)
		require.True(t, ok)
		edit(t, data)
		out, err := json.Marshal(e)
		require.NoError(t, err)
		lines[i] = string(out)
	}
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644))
}
