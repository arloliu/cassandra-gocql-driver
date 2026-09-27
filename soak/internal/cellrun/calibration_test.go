package cellrun

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
)

// calibrationFixture is a trimmed night-A cell (c50p5): every profile, the events the loader reads,
// the first three latency slices and six health rounds (PLAN §41.7).
const calibrationFixture = "../../testdata/calibration/c50p5"

// The expected values are read by hand from the fixture's JSON, and the critical values
// were computed from it by a separate script, not by the gate code (PLAN §41.7).
func TestLoadCalibrationFixture(t *testing.T) {
	cal, err := LoadCalibration(calibrationFixture)
	require.NoError(t, err)
	c := cal.Collected

	require.Equal(t, gate.ModeNight, c.Mode)
	require.Equal(t, 15*time.Minute, c.Timeline.Warmup)
	require.Equal(t, 105*time.Minute, c.Timeline.Cooldown)
	require.Equal(t, 120*time.Minute, c.Timeline.Workload)
	require.InDelta(t, 7200.000912135, c.WorkloadSeconds, 0)
	require.True(t, c.Ran)

	require.Len(t, c.Profiles, 39)
	require.InDelta(t, 0.004596438, c.Profiles[0].T, 0)
	require.InDelta(t, 3.3514938354492188, c.Profiles[0].HeapMiB, 0)
	require.Equal(t, 25, c.Profiles[0].FDs)
	require.Equal(t, 67, c.Profiles[0].Total)
	require.Equal(t, int64(5), c.Profiles[5].Balance["primary"])
	require.Equal(t, ReasonFinal, c.Profiles[38].Reason)

	require.Equal(t, 5, c.Baseline.Total)
	require.Equal(t, 15, c.Baseline.FDs)
	require.True(t, c.G12Checked)
	require.Equal(t, 15, c.G12.FD0)
	require.Equal(t, 15, c.G12.FDs)
	require.Equal(t, time.Duration(92639), c.G12.CloseElapsed)

	require.Len(t, c.Churns, 7)
	require.Equal(t, 2, c.Churns[2].Index)
	require.Equal(t, "C3", c.Churns[2].Slot)
	require.True(t, c.Churns[2].Measured)
	require.Equal(t, 7, c.Report.ChurnExecuted)
	require.Equal(t, 8, c.Report.MandatoryExecuted)

	require.Equal(t, int64(6820), c.Uncertain)
	require.Empty(t, c.Register)
	require.Empty(t, c.LWT)
	require.Empty(t, c.Runtime)
	require.Equal(t, gate.LogCounts{}, c.Logs)
	require.Equal(t, []string{"batch-logged", "batch-unlogged", "churn", "large-read", "lwt", "read", "scan", "short-deadline", "spec-read", "write"}, c.Classes)
	require.Equal(t, []string{"node1", "node2", "node3"}, c.Nodes)

	require.Len(t, c.Windows, 13)
	require.InDelta(t, 900.000468676, c.Windows[0].Start, 1e-6)
	require.InDelta(t, 931.360291154, c.Windows[0].End, 1e-6, "M0's end, 04:20:12.330745513, from the epoch 04:04:40.970454359")

	require.Len(t, c.TP, 97)
	require.Empty(t, cal.Missing)
	require.InDelta(t, 0.001746886, c.WarmupP99["batch-logged"], 0, "the recorded latency event")

	d, n, ok, ambiguous := cal.Latency.QuantileChecked("read", 0, 15, 1)
	require.True(t, ok)
	require.False(t, ambiguous)
	require.Equal(t, int64(6271), n, "slices 0–2 of read")
	require.Equal(t, int64(59305), d.Microseconds(), "the largest read key in slices 0–2")

	require.Equal(t, "c50p5", cal.Verdict.Cell)
	require.True(t, cal.Verdict.Final)
	require.True(t, cal.Verdict.Evidence.Complete)
	require.Equal(t, "20260926T200414Z-11a4", cal.Build.Attempt)
	require.Equal(t, "true", cal.Build.DriverDirty)
	require.Equal(t, 2, cal.NumConns)
	require.Equal(t, 3, cal.NodeCount)

	s := c.Series()
	require.InDelta(t, 900, s.Warm, 0)
	require.Len(t, s.Heap, 34)
	require.Len(t, s.QuietFDs, 14)

	crit := c.Critical()
	want := map[string]float64{gate.KG0: 48, gate.KF: 10, gate.KS: 14, gate.KC: 8, gate.KCh: -1, gate.KFs: 0, gate.KE: 0, gate.KLWTu: 6820}
	for k, v := range want {
		require.True(t, crit[k].OK, k)
		require.InDelta(t, v, crit[k].Value, 1e-9, k)
	}
	require.InDelta(t, 18.112289607127053, crit[gate.KH].Value, 1e-9)
}

func TestLoadCalibrationRefusesMissingPieces(t *testing.T) {
	for _, drop := range []string{"config.json", "build.json", "verdict.json", "samples.jsonl", "events.jsonl", "ccm/health.jsonl"} {
		t.Run(drop, func(t *testing.T) {
			dir := copyFixture(t)
			require.NoError(t, os.Remove(filepath.Join(dir, drop)))
			_, err := LoadCalibration(dir)
			require.Error(t, err)
		})
	}
	// A missing event or field is absent evidence, never a measured zero (Codex AK01).
	for _, tc := range []struct{ kind, want string }{
		{`"kind":"counters"`, "counters"},
		{`"kind":"oracles"`, "oracles"},
		{`"kind":"report"`, "report"},
		{`"kind":"latency"`, "latency"},
	} {
		t.Run("no "+tc.want+" event", func(t *testing.T) {
			dir := copyFixture(t)
			filterLines(t, filepath.Join(dir, "events.jsonl"), tc.kind)
			cal, err := LoadCalibration(dir)
			require.NoError(t, err)
			require.True(t, slices.ContainsFunc(cal.Missing, func(m string) bool { return strings.Contains(m, tc.want) }), "%v", cal.Missing)
		})
	}
	for _, tc := range []struct{ from, to, want string }{
		{`"Uncertain":6820`, `"UncertainX":6820`, "Uncertain"},
		{`"g7":{`, `"g7x":{`, "g7"},
		{`"warmup_p99_s":`, `"warmup_p99_x":`, "latency"},
	} {
		t.Run("no "+tc.want+" field", func(t *testing.T) {
			dir := copyFixture(t)
			replaceIn(t, filepath.Join(dir, "events.jsonl"), tc.from, tc.to)
			cal, err := LoadCalibration(dir)
			require.NoError(t, err)
			require.True(t, slices.ContainsFunc(cal.Missing, func(m string) bool { return strings.Contains(m, tc.want) }), "%v", cal.Missing)
		})
	}
	// Every consumed measurement and fixed-predicate field must be present; a nullable list may be null (Codex AL01).
	for _, tc := range []struct{ from, to, want string }{
		{`"EventBufferFull":0`, `"EventBufferFullX":0`, "EventBufferFull"},
		{`"Violations":null`, `"ViolationsX":null`, "Violations"},
		{`"register":null`, `"registerX":null`, "register"},
		{`"FDs":15`, `"FDsX":15`, "FDs"},
		{`"ProxySockets":0`, `"ProxySocketsX":0`, "ProxySockets"},
		{`"Leaks":0`, `"LeaksX":0`, "Leaks"},
		{`"churn_executed":7`, `"churn_executedX":7`, "churn_executed"},
		{`"live_entries":0`, `"live_entriesX":0`, "live_entries"},
		{`"failed":false`, `"failedX":false`, "failed"},
	} {
		t.Run("no "+tc.want+" field in an event", func(t *testing.T) {
			dir := copyFixture(t)
			replaceIn(t, filepath.Join(dir, "events.jsonl"), tc.from, tc.to)
			cal, err := LoadCalibration(dir)
			require.NoError(t, err)
			require.True(t, slices.ContainsFunc(cal.Missing, func(m string) bool { return strings.Contains(m, tc.want) }), "%v", cal.Missing)
		})
	}
	for _, key := range []string{`"heap_mib":`, `"fds":`, `"quiet":`, `"total":`} {
		t.Run("no "+key+" in a profile", func(t *testing.T) {
			dir := copyFixture(t)
			replaceIn(t, filepath.Join(dir, "samples.jsonl"), key, `"x`+key[1:])
			cal, err := LoadCalibration(dir)
			require.NoError(t, err)
			require.True(t, slices.ContainsFunc(cal.Missing, func(m string) bool { return strings.Contains(m, strings.Trim(key, `":`)) }), "%v", cal.Missing)
		})
	}

	// Every executed fault's outcome must have its window, and every window an outcome (Codex AL02):
	// a lost window would let fault-time readings count as steady state.
	t.Run("a lost window", func(t *testing.T) {
		dir := copyFixture(t)
		filterLines(t, filepath.Join(dir, "events.jsonl"), `"kind":"window","data":{"id":"M3"`)
		cal, err := LoadCalibration(dir)
		require.NoError(t, err)
		require.True(t, slices.ContainsFunc(cal.Missing, func(m string) bool { return strings.Contains(m, "outcome M3") }), "%v", cal.Missing)
	})
	t.Run("a lost repeated optional window", func(t *testing.T) {
		dir := copyFixture(t)
		dropNth(t, filepath.Join(dir, "events.jsonl"), `"kind":"window","data":{"id":"L+opt"`, 1)
		cal, err := LoadCalibration(dir)
		require.NoError(t, err)
		require.True(t, slices.ContainsFunc(cal.Missing, func(m string) bool { return strings.Contains(m, "outcome L+opt") }), "%v", cal.Missing)
	})
	t.Run("a window without an outcome", func(t *testing.T) {
		dir := copyFixture(t)
		replaceIn(t, filepath.Join(dir, "events.jsonl"), `"kind":"window","data":{"id":"M5"`, `"kind":"window","data":{"id":"M9"`)
		cal, err := LoadCalibration(dir)
		require.NoError(t, err)
		require.True(t, slices.ContainsFunc(cal.Missing, func(m string) bool { return strings.Contains(m, "window M9") }), "%v", cal.Missing)
	})
	// A window must place its exclusion correctly, not only match its outcome (Codex AM01);
	// a measured churn must carry both goroutine maps (AM02); a health reading its measurements (AM03).
	for _, tc := range []struct{ file, from, to, want string }{
		{"events.jsonl", `{"t":2550.000132769,"time":"2026-09-27T04:47:10.970587128+08:00","kind":"window"`, `{"t":2550.000132769,"kind":"window"`, "$.time: absent"},
		{"events.jsonl", `{"t":2550.000132769,"time":"2026-09-27T04:47:10.970587128+08:00","kind":"window"`, `{"time":"2026-09-27T04:47:10.970587128+08:00","kind":"window"`, "$.t: absent"},
		{"events.jsonl", `"end":"2026-09-27T04:48:36.221788074+08:00","failed":false`, `"end":"2026-09-27T04:40:00+08:00","failed":false`, "window M3"},
		{"events.jsonl", `"end":"2026-09-27T04:48:36.221788074+08:00","failed":false`, `"failed":false`, "window M3 ends at 0001-01-01T00:00:00Z"},
		{"events.jsonl", `"after":{`, `"after":null,"x":{`, "after"},
		{"events.jsonl", `"before":{`, `"before":null,"x":{`, "before"},
		{"ccm/health.jsonl", `"max_pause_ms":18,`, ``, "max_pause_ms"},
		{"ccm/health.jsonl", `"total_ms":273,`, `"total_ms":null,`, "total_ms"},
		{"ccm/health.jsonl", `"interval_ms":25011,`, ``, "interval_ms"},
		{"ccm/health.jsonl", `"pid":1157364,"dropped":0,`, `"dropped":0,`, "pid"},
	} {
		t.Run(tc.file+" "+tc.want+" "+tc.to, func(t *testing.T) {
			dir := copyFixture(t)
			replaceOnce(t, filepath.Join(dir, tc.file), tc.from, tc.to)
			cal, err := LoadCalibration(dir)
			require.NoError(t, err)
			require.True(t, slices.ContainsFunc(cal.Missing, func(m string) bool { return strings.Contains(m, tc.want) }), "%v", cal.Missing)
		})
	}
	// Round 27's sweep (Codex AN01–AN06): a record is accepted only if it is exactly what the writer emits for the value
	// it decodes to, plus the invariants the structure cannot express.
	for _, tc := range []struct {
		file, from, to, want string
		loadErr              bool
	}{
		{"samples.jsonl", `"groups":{"(no creator)":1,`, `"groups":{"(no creator)":null,`, "profile", false},
		{"samples.jsonl", `"balance":{"primary":0}`, `"balance":{"primary":null}`, "profile", false},
		{"events.jsonl", `"Baseline":{"(no creator)":1,`, `"Baseline":{"(no creator)":null,`, "g12", false},
		{"events.jsonl", `"before":{"(no creator)":1,`, `"before":{"(no creator)":null,`, "churn", false},
		{"samples.jsonl", `{"classes":{"batch-logged":{"start":0,`, `{"classesX":{"batch-logged":{"start":0,`, "latency", false},
		{"samples.jsonl", `{"classes":{"batch-logged":{"start":0,`, `{"classes":{"batch-logged":{`, "latency", false},
		{"samples.jsonl", `"n":505,"delayed":0},"batch-unlogged"`, `"n":0,"delayed":0},"batch-unlogged"`, "", true},
		// Structurally what the writer emits, but not on the slice grid, or empty: only the invariants catch these.
		{"samples.jsonl", `{"classes":{"batch-logged":{"start":0,`, `{"classes":{"batch-logged":{"start":1000,`, "start 1000", false},
		{"samples.jsonl", `{"classes":{"batch-logged":`, `{"classes":{"zz-empty":{"start":0,"seconds":5,"upper_us":{},"n":0,"delayed":0},"batch-logged":`, "class zz-empty: n 0", false},
		{"events.jsonl", `"recovered":"2026-09-27T04:48:16.207757167+08:00","end":"2026-09-27T04:48:26.221788074+08:00"`, `"recovered":"2026-09-27T04:48:16.207757167+08:00","end":"2026-09-27T04:40:00+08:00"`, "report outcome 3 (M3)", false},
		{"events.jsonl", `"kind":"window","data":{"id":"M3"`, `"kind":"","data":{"id":"M3"`, "no kind", false},
		{"samples.jsonl", `{"kind":"profile","t":0.004596438,`, `{"t":0.004596438,`, "kind", false},
		{"events.jsonl", `"kind":"window","data":{"id":"M3"`, `"data":{"id":"M3"`, "kind", false},
		{"config.json", `"cooldown": 6300000000000`, `"cooldown": null`, "config.json", false},
		{"config.json", `"cooldown": 6300000000000`, `"cooldown": 100`, "timeline", false},
		{"events.jsonl", `"end":"2026-09-27T04:48:26.221788074+08:00"`, `"endX":"2026-09-27T04:48:26.221788074+08:00"`, "report", false},
		{"ccm/health.jsonl", `"prime":true,`, ``, "prime", false},
		{"ccm/health.jsonl", `"prime":true,`, `"prime":null,`, "health", false},
	} {
		t.Run("strict "+tc.file+" "+tc.to, func(t *testing.T) {
			dir := copyFixture(t)
			replaceOnce(t, filepath.Join(dir, tc.file), tc.from, tc.to)
			cal, err := LoadCalibration(dir)
			if tc.loadErr {
				require.Error(t, err, "latency whose buckets do not add up to n cannot be rebuilt")
				return
			}
			require.NoError(t, err)
			require.True(t, slices.ContainsFunc(cal.Missing, func(m string) bool { return strings.Contains(m, tc.want) }), "%v", cal.Missing)
		})
	}
	// Round 28 (Codex AO01, AO03, AO04) and the inventories the writer guarantees (AO02, in part).
	for _, tc := range []struct{ file, from, to, want string }{
		{"samples.jsonl", `{"classes":{"batch-logged":`, `{"classesX":{},"x":{"batch-logged":`, "latency"},
		{"samples.jsonl", `"kind":"latency","slice":1}`, `"kind":"latency","slice":1,"classesY":null}`, "latency"},
		{"samples.jsonl", `{"kind":"profile","t":941.360415595,`, `{"kind":"cheap","t":941.360415595,`, "(cheap)"},
		{"events.jsonl", `"end":"2026-09-27T04:48:36.221788074+08:00","failed":false`, `"end":"2026-09-27T04:40:00+08:00","failed":true`, "window M3"},
		{"events.jsonl", `"end":"2026-09-27T04:48:36.221788074+08:00","failed":false`, `"end":"2026-09-28T04:48:36.221788074+08:00","failed":false`, "window M3 ends at"},
		{"samples.jsonl", `"total":67,`, `"total":68,`, "total 68"},
	} {
		t.Run("round 28 "+tc.file+" "+tc.to, func(t *testing.T) {
			dir := copyFixture(t)
			replaceOnce(t, filepath.Join(dir, tc.file), tc.from, tc.to)
			cal, err := LoadCalibration(dir)
			require.NoError(t, err)
			require.True(t, slices.ContainsFunc(cal.Missing, func(m string) bool { return strings.Contains(m, tc.want) }), "%v", cal.Missing)
		})
	}
	t.Run("an empty class map", func(t *testing.T) {
		dir := copyFixture(t)
		raw, err := os.ReadFile(filepath.Join(dir, "samples.jsonl"))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "samples.jsonl"), append(raw, []byte(`{"classes":{},"kind":"latency","slice":3}`+"\n")...), 0o600))
		cal, err := LoadCalibration(dir)
		require.NoError(t, err)
		require.True(t, slices.ContainsFunc(cal.Missing, func(m string) bool { return strings.Contains(m, "no class") }), "%v", cal.Missing)
	})
	// A periodic or recovered profile due at shutdown is not guaranteed (Codex AP03): losing one is not refused.
	t.Run("a missing periodic profile is not refused", func(t *testing.T) {
		dir := copyFixture(t)
		filterLines(t, filepath.Join(dir, "samples.jsonl"), `{"kind":"profile","t":7200.000913365,`)
		cal, err := LoadCalibration(dir)
		require.NoError(t, err)
		require.Empty(t, cal.Missing)
	})
	t.Run("no final profile", func(t *testing.T) {
		dir := copyFixture(t)
		filterLines(t, filepath.Join(dir, "samples.jsonl"), `"reason":"final"`)
		cal, err := LoadCalibration(dir)
		require.NoError(t, err)
		require.Contains(t, cal.Missing, "0 final profiles, not 1")
	})
	t.Run("no FD0", func(t *testing.T) {
		dir := copyFixture(t)
		replaceIn(t, filepath.Join(dir, "events.jsonl"), `"FD0":15,`, ``)
		cal, err := LoadCalibration(dir)
		require.NoError(t, err)
		require.True(t, slices.ContainsFunc(cal.Missing, func(m string) bool { return strings.Contains(m, "$.FD0: absent") }),
			"an absent FD0 is not a zero fd count: %v", cal.Missing)
	})
	t.Run("no g12 event", func(t *testing.T) {
		dir := copyFixture(t)
		filterLines(t, filepath.Join(dir, "events.jsonl"), `"kind":"g12"`)
		cal, err := LoadCalibration(dir)
		require.NoError(t, err, "a missing teardown is a suitability problem, not a load error")
		require.False(t, cal.Collected.G12Checked)
		require.False(t, cal.HasBaseline)
	})
}

// copyFixture copies the calibration fixture into a temporary directory.
func copyFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.CopyFS(dir, os.DirFS(calibrationFixture)))
	return dir
}

// filterLines drops every line of path that contains sub.
func filterLines(t *testing.T, path, sub string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var kept []string
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if !strings.Contains(line, sub) {
			kept = append(kept, line)
		}
	}
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(kept, "\n")+"\n"), 0o600))
}

// replaceIn replaces every from with to in path.
func replaceIn(t *testing.T, path, from, to string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(raw), from)
	require.NoError(t, os.WriteFile(path, []byte(strings.ReplaceAll(string(raw), from, to)), 0o600))
}

// dropNth drops the nth (from 0) line of path that contains sub.
func dropNth(t *testing.T, path, sub string, n int) {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var kept []string
	seen := 0
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if strings.Contains(line, sub) {
			seen++
			if seen-1 == n {
				continue
			}
		}
		kept = append(kept, line)
	}
	require.Greater(t, seen, n)
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(kept, "\n")+"\n"), 0o600))
}

// replaceOnce replaces the first from with to in path.
func replaceOnce(t *testing.T, path, from, to string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Contains(t, string(raw), from)
	require.NoError(t, os.WriteFile(path, []byte(strings.Replace(string(raw), from, to, 1)), 0o600))
}
