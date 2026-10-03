package derive

import (
	"encoding/json"
	"errors"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/canary"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cellrun"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/config"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
)

const (
	nightSHA   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	attemptSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

// The comparability table names exactly the canaries PLAN v7.16 §55.3 item 1 accepts, each with a reason.
func TestKLComparabilityTable(t *testing.T) {
	want := []string{"K1", "K10", "K15", "K17", "K2", "K3", "K5", "K6b", "K7", "K8", "control"}
	require.Equal(t, want, slices.Sorted(maps.Keys(klComparable)))
	for id, reason := range klComparable {
		require.NotEmpty(t, strings.TrimSpace(reason), id)
	}
	var registry []string
	for _, s := range canary.All() {
		registry = append(registry, s.ID)
	}
	for _, id := range []string{"K4", "K11", "K12", "K13", "K16"} {
		require.Contains(t, registry, id)
		require.NotContains(t, klComparable, id)
	}
}

func TestLedgerStage(t *testing.T) {
	op := &ExcludeAttempt{Reason: "host contention, IMPL-PROGRESS"}
	for name, tc := range map[string]struct {
		canary, state string
		exclude       *ExcludeAttempt
		want          string
	}{
		"control":          {"", "resolved", nil, ""},
		"K10":              {"K10", "resolved", nil, ""},
		"K4":               {"K4", "resolved", nil, "kind"},
		"K11":              {"K11", "resolved", nil, "kind"},
		"K12":              {"K12", "resolved", nil, "kind"},
		"K13":              {"K13", "resolved", nil, "kind"},
		"K16":              {"K16", "resolved", nil, "kind"},
		"absent canary":    {"K99", "resolved", nil, "kind"},
		"unresolved":       {"", "unresolved", nil, "state"},
		"submitted":        {"K1", "submitted", nil, "state"},
		"operator exclude": {"", "resolved", op, "operator: host contention"},
	} {
		t.Run(name, func(t *testing.T) {
			got := ledgerStage(tc.canary, tc.state, tc.exclude)
			if tc.want == "" {
				require.Empty(t, got)
				return
			}
			require.True(t, strings.HasPrefix(got, tc.want), got)
		})
	}
}

// validationCal is a validation attempt's execution directory, loaded: two classes at 2 ms, whose cool-down p99 is cool.
func validationCal(t *testing.T, cool time.Duration) cellrun.Calibration {
	t.Helper()
	col := cellrun.Collected{
		Mode:            gate.ModeValidate,
		Timeline:        config.Timeline{Warmup: config.ValidationWarmup, Cooldown: config.ValidationCooldown, Workload: config.ValidationWorkload},
		WorkloadSeconds: 2700, Ran: true,
		Classes: []string{"read", "write"},
	}
	lat := probe.NewLiveLatency(time.Unix(0, 0), 5*time.Second)
	for _, class := range col.Classes {
		for s := 0; s < 2700; s += 5 {
			d := 2 * time.Millisecond
			if s >= 2400 && class == "read" {
				d = cool
			}
			lat.Observe(class, time.Unix(int64(s), 0), d, false)
		}
	}
	lat = rebuilt(lat)
	col.WarmupP99, col.CooldownP99 = map[string]float64{}, map[string]float64{}
	for _, class := range col.Classes {
		w, _, _ := lat.Quantile(class, probe.Window{From: 0, To: 600}, 0.99)
		c, _, _ := lat.Quantile(class, probe.Window{From: 2400, To: 2700}, 0.99)
		col.WarmupP99[class], col.CooldownP99[class] = w.Seconds(), c.Seconds()
	}
	cal := cellrun.Calibration{Collected: col, Latency: lat, Late: map[string]int64{"read": 0, "write": 0}}
	cal.Build.DriverSHA, cal.Build.SourceClean = attemptSHA, "true"
	cal.Verdict.Final, cal.Verdict.Evidence.Complete = true, true
	cal.Verdict.Gates = []gate.Result{{Gate: "G16", Status: gate.StatusPass}}
	return cal
}

// attemptOf builds one ledger attempt entry.
func attemptOf(n int, state, execDir string) map[string]any {
	a := map[string]any{"n": n, "state": state, "root": "r"}
	if state == "resolved" {
		a["exec_dir"], a["result"], a["g16"] = execDir, "validated", "pass"
	}
	return a
}

// slotOf builds one ledger slot; effective is nil for a slot without an effective result.
func slotOf(slot, canaryID string, effective any, attempts ...map[string]any) map[string]any {
	return map[string]any{"slot": slot, "canary": canaryID, "attempts": attempts, "rerun_owed": false, "effective": effective}
}

var validated = map[string]any{"result": "validated", "reasons": []string{}}

// writeBatch writes a batch directory with its ledger and lock file and returns its path.
func writeBatch(t *testing.T, terminal any, slots ...map[string]any) string {
	t.Helper()
	dir := t.TempDir()
	raw, err := json.Marshal(map[string]any{"version": 1, "batch": map[string]any{"dir": dir, "terminal": terminal}, "slots": slots})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "validation-state.json"), raw, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".run-validation.lock"), nil, 0o600))
	return dir
}

// fakeLoads replaces the execution-directory loaders with a table and records every directory read.
func fakeLoads(t *testing.T, cals map[string]cellrun.Calibration) *[]string {
	t.Helper()
	var read []string
	oldCal, oldVerdict := loadCalibration, readVerdict
	t.Cleanup(func() { loadCalibration, readVerdict = oldCal, oldVerdict })
	loadCalibration = func(dir string) (cellrun.Calibration, error) {
		read = append(read, dir)
		c, ok := cals[dir]
		if !ok {
			return cellrun.Calibration{}, errors.New("no such directory")
		}
		return c, nil
	}
	readVerdict = func(dir string) (cellrun.RecordedVerdict, error) {
		read = append(read, dir)
		c, ok := cals[dir]
		if !ok {
			return cellrun.RecordedVerdict{}, errors.New("no verdict.json")
		}
		return c.Verdict, nil
	}
	return &read
}

func noPaths(string, string) ([]string, error) { return nil, nil }

func validationOptions(batches ...string) Options {
	return Options{Validation: batches, SourceUnchanged: unchanged, PathsChanged: noPaths}
}

// excludedOf returns the report entry of one attempt.
func attemptRef(t *testing.T, r *ValidationReport, slot string, n int) AttemptRef {
	t.Helper()
	require.NotNil(t, r)
	i := slices.IndexFunc(r.Attempts, func(a AttemptRef) bool { return a.Slot == slot && a.N == n })
	require.GreaterOrEqual(t, i, 0, "%s #%d", slot, n)
	return r.Attempts[i]
}

func TestLoadValidationVerificationShape(t *testing.T) {
	cals := map[string]cellrun.Calibration{"/open": validationCal(t, 2200*time.Microsecond), "/close": validationCal(t, 2*time.Millisecond)}
	read := fakeLoads(t, cals)
	b := writeBatch(t, nil,
		slotOf("control-open", "", validated, attemptOf(1, "resolved", "/open")),
		slotOf("k12", "K12", validated, attemptOf(1, "resolved", "/k12dir")),
		slotOf("control-close", "", validated, attemptOf(1, "resolved", "/close")),
	)
	src, rep, problems := loadValidation(validationOptions(b), nightSHA)
	require.Empty(t, problems)
	require.NotContains(t, *read, "/k12dir", "K12's artifacts are never read")
	require.True(t, strings.HasPrefix(attemptRef(t, rep, "k12", 1).Excluded, "kind"))
	open := attemptRef(t, rep, "control-open", 1)
	require.Empty(t, open.Excluded)
	require.Equal(t, "read", open.Class)
	require.InDelta(t, 0.1, *open.OKL, 0.02)
	require.Len(t, src, 4, "two classes of two eligible attempts")
	for _, s := range src {
		require.Equal(t, "validation", s.Kind)
		require.Equal(t, b, s.Batch)
	}
}

func TestLoadValidationUnresolvedArtifactsNeverRead(t *testing.T) {
	read := fakeLoads(t, map[string]cellrun.Calibration{"/close": validationCal(t, 2*time.Millisecond)})
	b := writeBatch(t, map[string]any{"state": "unresolved", "slot": "control-open", "reason": "stopped"},
		slotOf("control-open", "", nil, map[string]any{"n": 1, "state": "unresolved", "layer": "interrupted", "exec_dir": "/lost"}),
		slotOf("control-close", "", nil),
	)
	o := validationOptions(b)
	o.AcceptBatch = map[string]string{b: "stopped by the maintainer"}
	_, rep, problems := loadValidation(o, nightSHA)
	require.Empty(t, problems)
	require.NotContains(t, *read, "/lost")
	require.True(t, strings.HasPrefix(attemptRef(t, rep, "control-open", 1).Excluded, "state"))
	require.Equal(t, "unresolved", rep.Batches[0].Terminal)
	require.Equal(t, []string{"control-close"}, rep.Batches[0].Unattempted)
	require.Equal(t, []string{"control-open #1"}, rep.Batches[0].Unresolved)
	require.Equal(t, "stopped by the maintainer", rep.Batches[0].Accepted)
}

func TestLoadValidationBatchRefusals(t *testing.T) {
	cal := validationCal(t, 2*time.Millisecond)
	fakeLoads(t, map[string]cellrun.Calibration{"/open": cal})
	terminal := map[string]any{"state": "plan-amendment-required", "slot": "control-open", "reason": "g16-repeat"}
	owed := slotOf("control-open", "", nil, attemptOf(1, "resolved", "/open"))
	owed["rerun_owed"] = true
	for name, tc := range map[string]struct {
		dir    func(t *testing.T) string
		accept bool
		want   string
	}{
		"held lock": {func(t *testing.T) string {
			b := writeBatch(t, nil, slotOf("control-open", "", validated, attemptOf(1, "resolved", "/open")))
			f, err := os.Open(filepath.Join(b, ".run-validation.lock"))
			require.NoError(t, err)
			t.Cleanup(func() { _ = f.Close() })
			require.NoError(t, syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))
			return b
		}, false, "lock"},
		"terminal": {func(t *testing.T) string {
			return writeBatch(t, terminal, slotOf("control-open", "", map[string]any{"result": "not-validated"}, attemptOf(1, "resolved", "/open")))
		}, false, "terminal"},
		"missing ledger": {func(t *testing.T) string { return t.TempDir() }, false, "ledger"},
		"unattempted slot": {func(t *testing.T) string {
			return writeBatch(t, nil, slotOf("control-open", "", validated, attemptOf(1, "resolved", "/open")), slotOf("control-close", "", nil))
		}, true, "not finished"},
		"submitted attempt": {func(t *testing.T) string {
			return writeBatch(t, nil, slotOf("control-open", "", nil, attemptOf(1, "submitted", "")))
		}, true, "not finished"},
		"rerun owed": {func(t *testing.T) string { return writeBatch(t, nil, owed) }, true, "not finished"},
	} {
		t.Run(name, func(t *testing.T) {
			b := tc.dir(t)
			for _, accept := range []bool{false, tc.accept} {
				o := validationOptions(b)
				if accept {
					o.AcceptBatch = map[string]string{b: "r"}
				}
				src, _, problems := loadValidation(o, nightSHA)
				require.Empty(t, src)
				require.True(t, slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, tc.want) }), "%v", problems)
			}
		})
	}
}

func TestLoadValidationVerdictAndContamination(t *testing.T) {
	for name, tc := range map[string]struct {
		edit func(c *cellrun.Calibration)
		want string
	}{
		"not final":           {func(c *cellrun.Calibration) { c.Verdict.Final = false }, "verdict"},
		"evidence incomplete": {func(c *cellrun.Calibration) { c.Verdict.Evidence.Complete = false }, "verdict"},
		"G16 failed":          {func(c *cellrun.Calibration) { c.Verdict.Gates[0].Status = gate.StatusFail }, "verdict: G16"},
		"contaminated": {func(c *cellrun.Calibration) {
			c.Collected.Windows = []gate.Interval{{Start: 2350, End: 2450}}
		}, "contamination"},
	} {
		t.Run(name, func(t *testing.T) {
			cal := validationCal(t, 3*time.Millisecond)
			cal.Verdict.Gates = slices.Clone(cal.Verdict.Gates)
			tc.edit(&cal)
			fakeLoads(t, map[string]cellrun.Calibration{"/open": cal})
			b := writeBatch(t, nil, slotOf("control-open", "", validated, attemptOf(1, "resolved", "/open")))
			src, rep, problems := loadValidation(validationOptions(b), nightSHA)
			require.Empty(t, problems)
			require.Empty(t, src)
			got := attemptRef(t, rep, "control-open", 1).Excluded
			require.True(t, strings.HasPrefix(got, tc.want), got)
		})
	}

	t.Run("malformed verdict", func(t *testing.T) {
		fakeLoads(t, map[string]cellrun.Calibration{})
		b := writeBatch(t, nil, slotOf("control-open", "", validated, attemptOf(1, "resolved", "/open")))
		_, _, problems := loadValidation(validationOptions(b), nightSHA)
		require.True(t, slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, "verdict.json") }), "%v", problems)
	})
}

func TestLoadValidationSuitability(t *testing.T) {
	for name, tc := range map[string]struct {
		edit   func(c *cellrun.Calibration, o *Options)
		want   string // empty: suitable
		refuse bool
	}{
		"missing late":  {func(c *cellrun.Calibration, _ *Options) { c.Late = nil }, "late", true},
		"positive late": {func(c *cellrun.Calibration, _ *Options) { c.Late["read"] = 2 }, "late", true},
		"loader missing": {func(c *cellrun.Calibration, _ *Options) {
			c.Missing = []string{"events.jsonl: no latency event"}
		}, "no latency event", true},
		"source not clean": {func(c *cellrun.Calibration, _ *Options) { c.Build.SourceClean = "unverified" }, "source_clean", true},
		"source not clean, accepted": {func(c *cellrun.Calibration, o *Options) {
			c.Build.SourceClean, o.AcceptProvenance = "unverified", "override build reviewed"
		}, "", false},
		"driver changed": {func(_ *cellrun.Calibration, o *Options) {
			o.SourceUnchanged = func(string) (bool, error) { return false, nil }
		}, "outside soak/", true},
		"p99 mismatch": {func(c *cellrun.Calibration, _ *Options) { c.Collected.CooldownP99["read"] *= 1.02 }, "differs from the recorded", true},
		"class without warm-up p99": {func(c *cellrun.Calibration, _ *Options) {
			c.Collected.WarmupP99["write"] = 0
		}, "warm-up p99", true},
		"derive-only change": {func(_ *cellrun.Calibration, o *Options) {
			o.PathsChanged = func(string, string) ([]string, error) {
				return []string{"soak/internal/derive/validation.go", "soak/cmd/soak/derive.go", "soak/internal/cellrun/run_test.go", "soak/PLAN.md"}, nil
			}
		}, "", false},
		"workload change": {func(_ *cellrun.Calibration, o *Options) {
			o.PathsChanged = func(string, string) ([]string, error) { return []string{"soak/internal/workload/ops.go"}, nil }
		}, "soak/internal/workload/ops.go", true},
		"probe change, accepted": {func(_ *cellrun.Calibration, o *Options) {
			o.PathsChanged = func(string, string) ([]string, error) { return []string{"soak/internal/probe/latency.go"}, nil }
			o.AcceptCompat = map[string]string{"soak/internal/probe/latency.go": "a comment only"}
		}, "", false},
		"move out of the probe": {func(_ *cellrun.Calibration, o *Options) {
			o.PathsChanged = func(string, string) ([]string, error) {
				return []string{"soak/internal/derive/moved.go", "soak/internal/probe/moved.go"}, nil
			}
		}, "soak/internal/probe/moved.go", true},
	} {
		t.Run(name, func(t *testing.T) {
			cal := validationCal(t, 2*time.Millisecond)
			o := validationOptions()
			tc.edit(&cal, &o)
			fakeLoads(t, map[string]cellrun.Calibration{"/open": cal})
			b := writeBatch(t, nil, slotOf("control-open", "", validated, attemptOf(1, "resolved", "/open")))
			o.Validation = []string{b}
			src, _, problems := loadValidation(o, nightSHA)
			if !tc.refuse {
				require.Empty(t, problems)
				require.NotEmpty(t, src)
				return
			}
			require.True(t, slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, tc.want) }), "%v", problems)
		})
	}
}

func TestLoadValidationCompatibilityReport(t *testing.T) {
	fakeLoads(t, map[string]cellrun.Calibration{"/open": validationCal(t, 2*time.Millisecond)})
	b := writeBatch(t, nil, slotOf("control-open", "", validated, attemptOf(1, "resolved", "/open")))
	o := validationOptions(b)
	var asked [][2]string
	o.PathsChanged = func(from, to string) ([]string, error) {
		asked = append(asked, [2]string{from, to})
		return []string{"soak/internal/derive/x.go", "soak/run-night.sh"}, nil
	}
	o.AcceptCompat = map[string]string{"soak/run-night.sh": "log wording"}
	_, rep, problems := loadValidation(o, nightSHA)
	require.Empty(t, problems)
	require.Equal(t, [][2]string{{attemptSHA, nightSHA}}, asked)
	require.Equal(t, []PathDisposition{
		{Path: "soak/internal/derive/x.go", Disposition: "derivation-only"},
		{Path: "soak/run-night.sh", Disposition: "accepted: log wording"},
	}, attemptRef(t, rep, "control-open", 1).Paths)
}

func TestLoadValidationOperatorExclusion(t *testing.T) {
	fakeLoads(t, map[string]cellrun.Calibration{"/open": validationCal(t, 3*time.Millisecond), "/close": validationCal(t, 2*time.Millisecond)})
	b := writeBatch(t, nil,
		slotOf("control-open", "", validated, attemptOf(1, "resolved", "/open")),
		slotOf("control-close", "", validated, attemptOf(1, "resolved", "/close")),
	)
	o := validationOptions(b)
	o.Exclude = []ExcludeAttempt{{Batch: b, Slot: "control-open", N: 1, Reason: "fuzz job, IMPL-PROGRESS"}}
	src, rep, problems := loadValidation(o, nightSHA)
	require.Empty(t, problems)
	require.Equal(t, "operator: fuzz job, IMPL-PROGRESS", attemptRef(t, rep, "control-open", 1).Excluded)
	for _, s := range src {
		require.Equal(t, "control-close", s.Slot)
	}

	o.Exclude = []ExcludeAttempt{{Batch: b, Slot: "control-open", N: 2, Reason: "r"}}
	_, _, problems = loadValidation(o, nightSHA)
	require.True(t, slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, "no attempt") }), "%v", problems)
}

// The validation o_kL is the recorded comparison, G14's own, not the rebuilt one (PLAN v7.16 §55.2).
func TestValidationOKLUsesTheRecordedComparison(t *testing.T) {
	cal := validationCal(t, 2*time.Millisecond)
	cal.Collected.CooldownP99["read"] = cal.Collected.WarmupP99["read"] * 1.25
	got, problems := validationOKL(cal.Collected)
	require.Empty(t, problems)
	i := slices.IndexFunc(got, func(s KLSource) bool { return s.Class == "read" })
	require.GreaterOrEqual(t, i, 0)
	require.InDelta(t, 0.25, got[i].Value, 1e-12)
}

// klObs builds the kL observations of the given cells: one class, the night's four comparisons.
func klNight(id string, values ...float64) Observation {
	o := Observation{Full: crit(slices.Max(values))}
	cools := []probe.Window{{From: 6300, To: 6600}, {From: 6600, To: 6900}, {From: 6900, To: 7200}, {From: 6300, To: 7200}}
	for i, v := range values {
		warm := probe.Window{From: 0, To: 600}
		if i == 3 {
			warm = probe.Window{From: 0, To: 900}
		}
		o.Windows = append(o.Windows, v)
		o.Sources = append(o.Sources, KLSource{Kind: "night", Cell: id, Class: "read", Warm: &warm, Cool: &cools[i], Value: v})
	}
	return o
}

// combined runs Combine with every cell's kL set by night and then combineKL with the validation sources.
func combined(t *testing.T, night func(id string) Observation, val []KLSource) (gate.Thresholds, Derived) {
	t.Helper()
	obs := observations(Observation{Full: crit(1), Windows: []float64{1, 1, 1}}, func(id string, o map[string]Observation) {
		o[gate.KL] = night(id)
	})
	th, ds, problems := Combine(cellIDs, obs, nil)
	require.Empty(t, problems)
	combineKL(th, ds, cellIDs, obs, val)
	return th, derivedOf(t, ds, gate.KL)
}

func valSource(batch, slot string, idx, n int, class string, v float64) KLSource {
	return KLSource{Kind: "validation", Batch: batch, Slot: slot, slotIndex: idx, Attempt: n, Class: class, Value: v}
}

func TestCombineKLRuleAndAttribution(t *testing.T) {
	low := func(id string) Observation { return klNight(id, 0.01, 0.02, 0.03, 0.02) }

	t.Run("a validation attempt above every night observation", func(t *testing.T) {
		th, d := combined(t, low, []KLSource{valSource("/b", "control-open", 0, 1, "read", 0.3), valSource("/b", "control-open", 0, 1, "write", 0.1)})
		require.InDelta(t, 0.6, th[gate.KL], 1e-15)
		require.False(t, d.FloorApplied)
		require.Equal(t, []KLSource{valSource("/b", "control-open", 0, 1, "read", 0.3)}, d.Sources)
		require.InDelta(t, 0.3, *d.ValidationObserved, 0)
	})

	t.Run("everything below the floor", func(t *testing.T) {
		th, d := combined(t, low, []KLSource{valSource("/b", "control-open", 0, 1, "read", 0.1)})
		require.InDelta(t, Floors[gate.KL], th[gate.KL], 0)
		require.True(t, d.FloorApplied)
		require.Equal(t, []KLSource{{Kind: "floor", Value: Floors[gate.KL]}}, d.Sources)
	})

	t.Run("a night cell above every attempt", func(t *testing.T) {
		high := func(id string) Observation {
			if id == "c41p5" {
				return klNight(id, 0.01, 0.4, 0.03, 0.02)
			}
			return low(id)
		}
		th, d := combined(t, high, []KLSource{valSource("/b", "control-open", 0, 1, "read", 0.3)})
		require.InDelta(t, 0.8, th[gate.KL], 1e-15)
		require.Len(t, d.Sources, 1)
		require.Equal(t, "c41p5", d.Sources[0].Cell)
		require.Equal(t, probe.Window{From: 6600, To: 6900}, *d.Sources[0].Cool)
	})

	t.Run("a tie between comparisons with the same cool-down start", func(t *testing.T) {
		tie := func(id string) Observation {
			if id == "c50p4" {
				return klNight(id, 0.35, 0.01, 0.01, 0.35)
			}
			return low(id)
		}
		_, d := combined(t, tie, nil)
		require.Len(t, d.Sources, 2)
		require.Equal(t, probe.Window{From: 0, To: 600}, *d.Sources[0].Warm, "the shorter warm-up first")
		require.Equal(t, probe.Window{From: 0, To: 900}, *d.Sources[1].Warm)
	})

	t.Run("an observation whose doubled value is the floor", func(t *testing.T) {
		v := Floors[gate.KL] / Factor
		th, d := combined(t, low, []KLSource{valSource("/b", "control-close", 2, 1, "read", v)})
		require.InDelta(t, Floors[gate.KL], th[gate.KL], 0)
		require.Len(t, d.Sources, 2)
		require.Equal(t, "floor", d.Sources[0].Kind, "the floor is named first")
		require.Equal(t, "validation", d.Sources[1].Kind)
	})

	t.Run("validation ties in batch, slot, attempt and class order", func(t *testing.T) {
		val := []KLSource{
			valSource("/b2", "control-open", 0, 1, "read", 0.3),
			valSource("/b1", "control-close", 2, 1, "write", 0.3),
			valSource("/b1", "control-close", 2, 1, "read", 0.3),
			valSource("/b1", "control-open", 0, 2, "read", 0.3),
		}
		_, d := combined(t, low, val)
		var got []string
		for _, s := range d.Sources {
			got = append(got, s.Batch+" "+s.Slot+" "+s.Class)
		}
		require.Equal(t, []string{"/b1 control-open read", "/b1 control-close read", "/b1 control-close write", "/b2 control-open read"}, got)
	})
}

// Validation evidence changes kL only: every other threshold and the night's problems are the same without it.
func TestEvaluateValidationIsolation(t *testing.T) {
	s, cells := night(t)
	for i := range cells {
		cells[i] = withLatency(cells[i])
		cells[i].Cal.Build.DriverSHA = nightSHA
	}
	fakeLoads(t, map[string]cellrun.Calibration{"/open": validationCal(t, 2200*time.Microsecond)})
	b := writeBatch(t, nil, slotOf("control-open", "", validated, attemptOf(1, "resolved", "/open")))
	base := Options{SourceUnchanged: unchanged, AcceptZero: map[string]string{gate.KD: "no drops"}}
	th0, r0 := Evaluate("summary.json", s, cells, base)
	require.False(t, r0.Refused, "%v", r0.Problems)
	with := base
	with.Validation, with.PathsChanged = []string{b}, noPaths
	th1, r1 := Evaluate("summary.json", s, cells, with)
	require.False(t, r1.Refused, "%v", r1.Problems)
	require.Equal(t, th0, th1, "an attempt below the floor changes nothing")
	for _, d := range r0.Thresholds {
		if d.Name == gate.KL {
			continue
		}
		require.Equal(t, d, derivedOf(t, r1.Thresholds, d.Name), d.Name)
	}
	require.Nil(t, r0.Validation)
	require.NotNil(t, r1.Validation)
	require.Contains(t, r1.Markdown(), "Validation evidence")

	// A refused attempt refuses the derivation, without touching the night's own problems.
	broken := validationCal(t, 2*time.Millisecond)
	broken.Late["read"] = 1
	fakeLoads(t, map[string]cellrun.Calibration{"/open": broken})
	_, r2 := Evaluate("summary.json", s, cells, with)
	require.True(t, r2.Refused)
	for _, p := range r2.Problems {
		require.Contains(t, p, "control-open", p)
	}
}

// A raise above the derived kL is reported beside the derived source.
func TestEvaluateRaiseAboveTheFloor(t *testing.T) {
	s, cells := night(t)
	for i := range cells {
		cells[i] = withLatency(cells[i])
	}
	o := Options{SourceUnchanged: unchanged, AcceptZero: map[string]string{gate.KD: "no drops"}, Raise: []Raise{{Name: gate.KL, Value: 0.6, Reason: "r"}}}
	th, r := Evaluate("summary.json", s, cells, o)
	require.False(t, r.Refused, "%v", r.Problems)
	require.InDelta(t, 0.6, th[gate.KL], 0)
	d := derivedOf(t, r.Thresholds, gate.KL)
	require.Equal(t, &Raised{From: Floors[gate.KL], Given: 0.6, Reason: "r", Applied: true}, d.Raised)
	require.Equal(t, "floor", d.Sources[0].Kind)
	md := r.Markdown()
	require.Contains(t, md, "kL's source: floor")
	require.Contains(t, md, "raised from")
}

func TestFloorKL(t *testing.T) {
	require.InDelta(t, 0.48674869540553, Floors[gate.KL], 0, "PLAN v7.16 §55.2: twice batch 1 K8's 0.243374347702765")
	require.False(t, math.IsNaN(Floors[gate.KL]))
}

// A verdict.json whose judged fields are absent or null is malformed, never a quiet false (Codex BO01).
func TestReadVerdictFileStrict(t *testing.T) {
	good := `{"status":"pass","final":true,"evidence":{"complete":true},"gates":[{"gate":"G16","status":"pass"}]}`
	for name, tc := range map[string]struct {
		body string
		want string // empty: reads
	}{
		"valid":            {good, ""},
		"explicit false":   {`{"final":false,"evidence":{"complete":false},"gates":[]}`, ""},
		"syntax":           {`{"final":true,`, "does not parse"},
		"null":             {`null`, "no final"},
		"empty":            {`{}`, "no final"},
		"null evidence":    {`{"final":true,"evidence":null,"gates":[]}`, "no evidence.complete"},
		"no complete":      {`{"final":true,"evidence":{},"gates":[]}`, "no evidence.complete"},
		"no gates":         {`{"final":true,"evidence":{"complete":true}}`, "no gates"},
		"null final":       {`{"final":null,"evidence":{"complete":true},"gates":[]}`, "no final"},
		"final not a bool": {`{"final":"yes","evidence":{"complete":true},"gates":[]}`, "does not parse"},
		"null gate":        {`{"final":false,"evidence":{"complete":true},"gates":[null]}`, "gates[0]"},
		"gate after G16":   {`{"final":true,"evidence":{"complete":true},"gates":[{"gate":"G16","status":"fail"},{"gate":"G3"}]}`, "gates[1]"},
		"gate without id":  {`{"final":true,"evidence":{"complete":true},"gates":[{"status":"pass"}]}`, "gates[0]"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, "verdict.json"), []byte(tc.body), 0o600))
			_, err := readVerdictFile(dir)
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.want)
		})
	}
}

// A malformed verdict on disk refuses the derivation and is reported as refused, not eligible (Codex BO01, BO03).
func TestLoadValidationMalformedVerdictRefuses(t *testing.T) {
	exec := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(exec, "verdict.json"), []byte(`{"final":true,"evidence":null,"gates":[]}`), 0o600))
	read := fakeLoads(t, map[string]cellrun.Calibration{})
	readVerdict = readVerdictFile
	b := writeBatch(t, nil, slotOf("control-open", "", validated, attemptOf(1, "resolved", exec)))
	src, rep, problems := loadValidation(validationOptions(b), nightSHA)
	require.Empty(t, src)
	require.True(t, slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, "no evidence.complete") }), "%v", problems)
	require.Empty(t, *read, "the strict loader is never reached")
	a := attemptRef(t, rep, "control-open", 1)
	require.Equal(t, "refused", a.Outcome)
	require.Equal(t, "resolved", a.State)
	var md strings.Builder
	rep.markdown(&md)
	require.Contains(t, md.String(), "| resolved | refused: verdict.json: no evidence.complete |")
	require.NotContains(t, md.String(), "eligible")
}

// Every outcome is explicit: eligible, excluded with its stage, refused with its problems (Codex BO03).
func TestLoadValidationOutcomes(t *testing.T) {
	late := validationCal(t, 2*time.Millisecond)
	late.Late["read"] = 3
	fakeLoads(t, map[string]cellrun.Calibration{"/open": validationCal(t, 2*time.Millisecond), "/close": late})
	b := writeBatch(t, nil,
		slotOf("control-open", "", validated, attemptOf(1, "resolved", "/open")),
		slotOf("k12", "K12", validated, attemptOf(1, "resolved", "/k12dir")),
		slotOf("control-close", "", validated, attemptOf(1, "resolved", "/close")),
	)
	_, rep, problems := loadValidation(validationOptions(b), nightSHA)
	require.Len(t, problems, 1)
	require.Equal(t, "eligible", attemptRef(t, rep, "control-open", 1).Outcome)
	k12 := attemptRef(t, rep, "k12", 1)
	require.Equal(t, "excluded", k12.Outcome)
	require.True(t, strings.HasPrefix(k12.Excluded, "kind:"))
	closed := attemptRef(t, rep, "control-close", 1)
	require.Equal(t, "refused", closed.Outcome)
	require.Equal(t, []string{"3 late latency observations of class read"}, closed.Refused)
	require.Nil(t, closed.OKL)
}

// One batch has one identity: a second spelling neither duplicates it nor escapes an exclusion (Codex BO02).
func TestLoadValidationBatchAliases(t *testing.T) {
	fakeLoads(t, map[string]cellrun.Calibration{"/open": validationCal(t, 3*time.Millisecond), "/close": validationCal(t, 2*time.Millisecond)})
	b := writeBatch(t, nil,
		slotOf("control-open", "", validated, attemptOf(1, "resolved", "/open")),
		slotOf("control-close", "", validated, attemptOf(1, "resolved", "/close")),
	)
	link := filepath.Join(t.TempDir(), "alias")
	require.NoError(t, os.Symlink(b, link))
	t.Chdir(filepath.Dir(b))
	rel := filepath.Join(".", filepath.Base(b))
	canon, err := CanonicalBatch(b)
	require.NoError(t, err)

	for name, spelling := range map[string]string{"relative": rel, "symlink": link, "trailing slash": b + "/"} {
		t.Run(name, func(t *testing.T) {
			o := validationOptions(b, spelling)
			_, _, problems := loadValidation(o, nightSHA)
			require.True(t, slices.ContainsFunc(problems, func(p string) bool { return strings.Contains(p, "given twice") }), "%v", problems)

			o = validationOptions(spelling)
			o.Exclude = []ExcludeAttempt{{Batch: b, Slot: "control-open", N: 1, Reason: "fuzz job"}}
			src, rep, problems := loadValidation(o, nightSHA)
			require.Empty(t, problems)
			require.Equal(t, canon, rep.Batches[0].Dir)
			require.Equal(t, "excluded", attemptRef(t, rep, "control-open", 1).Outcome)
			for _, s := range src {
				require.Equal(t, "control-close", s.Slot, "the excluded attempt is not admitted through another spelling")
			}
		})
	}
}

func TestCombineKLMoreTies(t *testing.T) {
	low := func(id string) Observation { return klNight(id, 0.01, 0.02, 0.03, 0.02) }

	t.Run("night before validation", func(t *testing.T) {
		tie := func(id string) Observation {
			if id == "c50p5" {
				return klNight(id, 0.01, 0.3, 0.03, 0.02)
			}
			return low(id)
		}
		_, d := combined(t, tie, []KLSource{valSource("/a", "control-open", 0, 1, "read", 0.3)})
		require.Len(t, d.Sources, 2)
		require.Equal(t, "night", d.Sources[0].Kind, "the night first, though its batch path would sort after")
		require.Equal(t, "validation", d.Sources[1].Kind)
	})

	t.Run("attempt number within one slot", func(t *testing.T) {
		val := []KLSource{
			valSource("/b", "control-open", 0, 2, "read", 0.3),
			valSource("/b", "control-open", 0, 1, "write", 0.3),
		}
		_, d := combined(t, low, val)
		require.Len(t, d.Sources, 2)
		require.Equal(t, 1, d.Sources[0].Attempt, "attempt 1 before attempt 2, whatever the class")
		require.Equal(t, 2, d.Sources[1].Attempt)
	})
}

// Validation evidence leaves the rest of the report alone, an unsuitable night's problems included (Codex BO04).
func TestEvaluateValidationLeavesTheReport(t *testing.T) {
	fakeLoads(t, map[string]cellrun.Calibration{"/open": validationCal(t, 2200*time.Microsecond)})
	b := writeBatch(t, nil, slotOf("control-open", "", validated, attemptOf(1, "resolved", "/open")))
	base := Options{SourceUnchanged: unchanged, AcceptZero: map[string]string{gate.KD: "no drops"}}
	with := base
	with.Validation, with.PathsChanged = []string{b}, noPaths
	// strip removes the only permitted differences: the validation section, and kL's observed value,
	// which becomes the larger of the night's and the validation maximum.
	strip := func(r0, r Report) Report {
		r.Validation = nil
		r.Thresholds = slices.Clone(r.Thresholds)
		for i := range r.Thresholds {
			if d := &r.Thresholds[i]; d.Name == gate.KL {
				night := derivedOf(t, r0.Thresholds, gate.KL).Observed
				require.InDelta(t, math.Max(night, *d.ValidationObserved), d.Observed, 0)
				d.ValidationObserved, d.Observed = nil, night
			}
		}
		return r
	}
	for name, unsuitable := range map[string]bool{"suitable night": false, "unsuitable night": true} {
		t.Run(name, func(t *testing.T) {
			// Fresh cells per case, so neither case sees the other's edit (Codex BP03).
			s, cells := night(t)
			for i := range cells {
				cells[i] = withLatency(cells[i])
				cells[i].Cal.Build.DriverSHA = nightSHA
			}
			if unsuitable {
				cells[1].Cal.Verdict.Final = false
			}
			_, r0 := Evaluate("summary.json", s, cells, base)
			_, r1 := Evaluate("summary.json", s, cells, with)
			require.Equal(t, unsuitable, r0.Refused, "%v", r0.Problems)
			require.Equal(t, unsuitable, r1.Refused, "%v", r1.Problems)
			require.NotNil(t, r1.Validation)
			require.NotNil(t, derivedOf(t, r1.Thresholds, gate.KL).ValidationObserved)
			j0, err := json.Marshal(r0)
			require.NoError(t, err)
			j1, err := json.Marshal(strip(r0, r1))
			require.NoError(t, err)
			require.JSONEq(t, string(j0), string(j1))
		})
	}
}
