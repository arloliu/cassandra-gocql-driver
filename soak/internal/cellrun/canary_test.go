package cellrun

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/artifact"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/canary"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cell"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/view"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// No canary, no hook (PLAN §44.4); each canary installs only its own seam.
func TestCanaryHooks(t *testing.T) {
	apply := func(id string) (*workload.Env, *Churner) {
		spec, _ := canary.Lookup(id)
		env, ch := &workload.Env{}, &Churner{}
		applyEnvHooks(spec, env, ch)
		return env, ch
	}
	for _, id := range []string{"", "K1", "K6b", "K7", "K12"} {
		env, ch := apply(id)
		require.Nil(t, env.Excluded, id)
		require.Zero(t, env.Switches, id)
		require.Zero(t, ch.Switches, id)
	}
	env, _ := apply("K8")
	require.Equal(t, canary.PinnedKey(), *env.Excluded)
	for _, id := range []string{"K15", "K17"} {
		env, ch := apply(id)
		require.NotZero(t, env.Switches, id)
		require.Equal(t, env.Switches, ch.Switches, "%s reaches the aux sessions too", id)
	}
}

// K15 and K17 reach the aux Env the churner builds (PLAN §44.4).
func TestAuxEnvGetsTheSwitches(t *testing.T) {
	shortBudgets(t)
	chaos.ChurnBudget.Load = 10 * time.Millisecond
	for _, sw := range []workload.Switches{{}, {NoEarlyClose: true}, {NoSpeculation: true}} {
		c, _, _ := testChurner(t)
		c.Switches = sw
		c.Create = func(*gocql.ClusterConfig) (*gocql.Session, error) { return nil, nil }
		c.CloseSession = func(*cell.Session) {}
		var got atomic.Pointer[workload.Switches]
		c.StartLoad = func(_ context.Context, env *workload.Env, _ float64, _ int, _ workload.Mix, _ uint64) (Stopper, error) {
			s := env.Switches
			got.Store(&s)
			release := make(chan struct{})
			close(release)
			return blockingStop{release}, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		require.NoError(t, c.Churn(ctx, chaos.Slot{ID: "C1"}))
		cancel()
		require.Equal(t, sw, *got.Load())
	}
}

// judgeDir writes events.jsonl and errors.jsonl through the harness's own recorder, then judges.
type recorded struct {
	dir string
	rec *Recorder
	win *chaos.Windows
	ev  *artifact.JSONL
	er  *artifact.JSONL
}

func newRecorded(t *testing.T) *recorded {
	t.Helper()
	dir := t.TempDir()
	ev, err := artifact.OpenJSONL(filepath.Join(dir, "events.jsonl"))
	require.NoError(t, err)
	er, err := artifact.OpenJSONL(filepath.Join(dir, "errors.jsonl"))
	require.NoError(t, err)
	w := &chaos.Windows{}
	rec := NewRecorder(er, ev, w, 5)
	rec.SetEpoch(time.Now())
	return &recorded{dir: dir, rec: rec, win: w, ev: ev, er: er}
}

func (r *recorded) close(t *testing.T) {
	t.Helper()
	require.NoError(t, r.ev.Close())
	require.NoError(t, r.er.Close())
}

func (r *recorded) canaryEvent(e canary.Event) { r.rec.Event("canary", e.Time, e) }

func failing(gates ...string) gate.Verdict {
	v := gate.Verdict{Status: gate.VerdictPass}
	for i := range 17 {
		v.Gates = append(v.Gates, gate.Result{Gate: fmt.Sprintf("G%d", i), Status: gate.StatusPass})
	}
	for i, r := range v.Gates {
		for _, g := range gates {
			if r.Gate == g {
				v.Gates[i].Status, v.Gates[i].Details = gate.StatusFail, []string{"fired"}
				v.Status, v.FailingGates = gate.VerdictFail, append(v.FailingGates, g)
			}
		}
	}
	return v
}

// K7's assertion reads the recorder's own in_window and class from errors.jsonl, by the injected op ids.
func TestJudgeK7FromErrorsJSONL(t *testing.T) {
	r := newRecorded(t)
	env := &workload.Env{SessionID: "primary", OpIDs: &atomic.Uint64{}, Errors: r.rec.Error}
	op := env.RecordCanaryError(errors.New("canary"))
	r.canaryEvent(canary.Event{Canary: "K7", Kind: canary.KindInject, Time: time.Now(), OpID: op})
	r.close(t)
	got := judgeRun(r.dir, "K7", failing("G8"), Evidence{Complete: true})
	require.Equal(t, canary.Validated, got.Result, "%v", got.Reasons)

	// Inside a window only: the assertion fails.
	r = newRecorded(t)
	r.win.Open("V1", chaos.FaultStop, 1, time.Now().Add(-time.Second))
	env = &workload.Env{SessionID: "primary", OpIDs: &atomic.Uint64{}, Errors: r.rec.Error}
	op = env.RecordCanaryError(errors.New("canary"))
	r.canaryEvent(canary.Event{Canary: "K7", Kind: canary.KindInject, Time: time.Now(), OpID: op})
	r.close(t)
	got = judgeRun(r.dir, "K7", failing("G8"), Evidence{Complete: true})
	require.Equal(t, canary.NotValidated, got.Result)
	require.Contains(t, strings.Join(got.Reasons, "\n"), "class unknown")
}

// The recorder classifies K7's error as unknown and G8 does not admit it outside a window: the assertion's premise.
func TestK7ErrorIsUnexpected(t *testing.T) {
	class, _ := gate.ClassifyOp(errors.New("canary"), false, 5, 0)
	require.Equal(t, gate.ClassUnknown, class)
	require.False(t, gate.Admit(class, false, gate.Op{}))
	require.False(t, gate.Admit(class, true, gate.Op{}), "row 10 is unexpected in a window too")
}

func TestJudgeK15K17FromTheCoverageEvent(t *testing.T) {
	for _, tc := range []struct {
		id     string
		totals workload.SettlementTotals
		v      gate.Verdict
		ok     bool
	}{
		{"K15", workload.SettlementTotals{SpeculationProven: 30}, failing("G15"), true},
		{"K15", workload.SettlementTotals{PrefetchProven: 3}, failing("G15"), false},
		{"K17", workload.SettlementTotals{PrefetchProven: 90}, failing("G15"), true},
		{"K17", workload.SettlementTotals{PrefetchProven: 90}, failing(), true},
		{"K17", workload.SettlementTotals{SpeculationProven: 1}, failing("G15"), false},
	} {
		r := newRecorded(t)
		r.rec.Event("coverage", time.Now(), coverageEvent{Settlement: tc.totals, ErrorClasses: map[string]int64{}})
		r.close(t)
		got := judgeRun(r.dir, tc.id, tc.v, Evidence{Complete: true})
		require.Equal(t, tc.ok, got.Result == canary.Validated, "%s %+v: %v", tc.id, tc.totals, got.Reasons)
	}
}

// A missing, unreadable or altered assertion input is not-validated, with the reason (PLAN §44.8).
func TestJudgeMissingOrAlteredInputs(t *testing.T) {
	r := newRecorded(t)
	r.close(t)
	got := judgeRun(r.dir, "K15", failing("G15"), Evidence{Complete: true})
	require.Equal(t, canary.NotValidated, got.Result)
	require.Contains(t, strings.Join(got.Reasons, "\n"), "coverage")

	r = newRecorded(t)
	r.close(t)
	require.NoError(t, os.Remove(filepath.Join(r.dir, "events.jsonl")))
	got = judgeRun(r.dir, "K8", failing("G10"), Evidence{Complete: true})
	require.Equal(t, canary.NotValidated, got.Result)
	require.Contains(t, strings.Join(got.Reasons, "\n"), "events.jsonl")

	// A coverage event whose settlement lost a field is not what the harness writes.
	r = newRecorded(t)
	r.close(t)
	line := `{"t":1,"time":"2026-09-27T10:00:00Z","kind":"coverage","data":{"settlement":{"PrefetchUnproven":0},"short_deadline_timeouts":0,"error_classes":{}}}` + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(r.dir, "events.jsonl"), []byte(line), 0o644))
	got = judgeRun(r.dir, "K15", failing("G15"), Evidence{Complete: true})
	require.Equal(t, canary.NotValidated, got.Result)
	require.Contains(t, strings.Join(got.Reasons, "\n"), "not what the harness writes")

	// The control and the canaries without an assertion read nothing.
	r = newRecorded(t)
	r.close(t)
	require.NoError(t, os.RemoveAll(r.dir))
	require.Equal(t, canary.Validated, judgeRun(r.dir, "", failing(), Evidence{Complete: true}).Result)
	require.Equal(t, canary.Validated, judgeRun(r.dir, "K4", failing("G3"), Evidence{Complete: true}).Result)
}

// K12: a G0 failure is fixture-invalid from G0 alone with G16 not evaluated, which rule 2 validates.
func TestK12ThroughEvaluate(t *testing.T) {
	c := Collected{Mode: gate.ModeValidate, G0Checked: true, G0: []string{"127.0.1.1 release_version 5.0.3, want 0.0.0-k12"}}
	v := Evaluate(c)
	require.Equal(t, gate.VerdictFixtureInvalid, v.Status)
	got := judgeRun(t.TempDir(), "K12", v, EvidenceOf(v, c.Incomplete))
	require.Equal(t, canary.Validated, got.Result, "%v", got.Reasons)

	c.Incomplete = []string{"artifact events.jsonl: write failed"}
	v = Evaluate(c)
	require.Equal(t, canary.NotValidated, judgeRun(t.TempDir(), "K12", v, EvidenceOf(v, c.Incomplete)).Result)
}

// The validation object roundtrips through VerdictFile, and a night verdict has none (PLAN §44.8).
func TestVerdictFileValidationRoundtrip(t *testing.T) {
	vf := VerdictFile{Verdict: failing("G8"), Final: true, Phase: PhaseDone, Updated: time.Unix(1, 0).UTC(),
		Validation: &canary.Validation{Canary: "", Result: canary.Validated}}
	raw, err := json.Marshal(vf)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"validation":{"canary":"","result":"validated"}`, "a control's canary is written, empty")
	var back VerdictFile
	d, err := strictDecode(raw, &back)
	require.NoError(t, err)
	require.Empty(t, d)

	vf.Validation = nil
	raw, err = json.Marshal(vf)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "validation")
}

// A canary is refused in night mode; in validation mode its id reaches config.json.
func TestPrepareCanary(t *testing.T) {
	o := prepareOptions(t, "20260927T113426Z-1a4d")
	o.Canary, o.RunKind = "K7", "k7"
	r := newPrepareRun(o)
	require.NoError(t, r.prepare())
	defer r.closeStreams()
	require.Equal(t, "K7", r.conf.Overrides.Canary)
	require.Equal(t, "K7", r.canary.ID)

	o = prepareOptions(t, "20260927T113426Z-1a4e")
	o.Mode, o.Canary = gate.ModeNight, "K7"
	require.ErrorContains(t, newPrepareRun(o).prepare(), "validation mode")

	o = prepareOptions(t, "20260927T113426Z-1a4f")
	o.Canary = "K9"
	require.ErrorContains(t, newPrepareRun(o).prepare(), "K9")
}

// Every events.jsonl line is strict-decoded before it is dispatched: a corrupt line beside sufficient valid evidence
// is still a reason (Codex AT02).
func TestJudgeRefusesACorruptLineBesideValidEvidence(t *testing.T) {
	for name, bad := range map[string]string{
		"malformed JSON":        `{"t":1,"time":"2026-09-27T10:00:00Z","kind":"canary","data":{"canary":"K7",` + "\n",
		"no kind":               `{"t":1,"time":"2026-09-27T10:00:00Z","data":{"canary":"K7","kind":"inject","time":"2026-09-27T10:00:00Z","op_id":9}}` + "\n",
		"a blank line":          "\n",
		"an empty kind":         `{"t":1,"time":"2026-09-27T10:00:00Z","kind":"","data":{"canary":"K7","kind":"inject","time":"2026-09-27T10:00:00Z","op_id":9}}` + "\n",
		"an unknown field":      `{"t":1,"time":"2026-09-27T10:00:00Z","kind":"phase","data":"chaos","extra":1}` + "\n",
		"a lost envelope field": `{"time":"2026-09-27T10:00:00Z","kind":"canary","data":{"canary":"K7","kind":"inject","time":"2026-09-27T10:00:00Z","op_id":9}}` + "\n",
	} {
		r := newRecorded(t)
		env := &workload.Env{SessionID: "primary", OpIDs: &atomic.Uint64{}, Errors: r.rec.Error}
		op := env.RecordCanaryError(errors.New("canary"))
		r.canaryEvent(canary.Event{Canary: "K7", Kind: canary.KindInject, Time: time.Now(), OpID: op})
		r.close(t)
		require.Equal(t, canary.Validated, judgeRun(r.dir, "K7", failing("G8"), Evidence{Complete: true}).Result, name)
		f, err := os.OpenFile(filepath.Join(r.dir, "events.jsonl"), os.O_APPEND|os.O_WRONLY, 0)
		require.NoError(t, err)
		_, err = f.WriteString(bad)
		require.NoError(t, err)
		require.NoError(t, f.Close())
		got := judgeRun(r.dir, "K7", failing("G8"), Evidence{Complete: true})
		require.Equal(t, canary.NotValidated, got.Result, name)
		require.Contains(t, strings.Join(got.Reasons, "\n"), "events.jsonl line 2", name)
	}
}

// K11 through Evaluate: G11 fails on LWT progress, and G14's missing lwt cool-down p99 is exactly K11's declared entry.
func TestK11ThroughEvaluate(t *testing.T) {
	c := green()
	delete(c.CooldownP99, "lwt")
	for i := 1200; i < len(c.Progress.Completed["lwt"]); i++ {
		c.Progress.Completed["lwt"][i] = 0
	}
	v := Evaluate(c)
	require.Equal(t, gate.StatusFail, statusOf(v, "G11"))
	require.Equal(t, gate.StatusFail, statusOf(v, "G14"))
	ev := EvidenceOf(v, nil)
	spec, _ := canary.Lookup("K11")
	require.Equal(t, spec.Declared, ev.Problems)
	got := judgeRun(t.TempDir(), "K11", v, ev)
	require.Equal(t, canary.Validated, got.Result, "%v", got.Reasons)

	// Without the drop, G11 passes: not validated.
	c = green()
	delete(c.CooldownP99, "lwt")
	v = Evaluate(c)
	require.Equal(t, canary.NotValidated, judgeRun(t.TempDir(), "K11", v, EvidenceOf(v, nil)).Result)
}

// K13's F-stop is pinned to node2 after chaos.Plan, and schedule.json holds the pinned schedule (PLAN §44.5);
// a control keeps the drawn target.
func TestPrepareK13PinsTheStop(t *testing.T) {
	drawn := map[string]bool{}
	for i, seed := range []uint64{1, 2, 3, 4, 5, 6} {
		for _, id := range []string{"", "K13"} {
			o := prepareOptions(t, fmt.Sprintf("20260928T113426Z-%04x", 2*i+len(id)))
			o.Seed, o.Canary, o.RunKind = seed, id, canary.Kind(id)
			r := newPrepareRun(o)
			require.NoError(t, r.prepare())
			r.closeStreams()
			raw, err := os.ReadFile(filepath.Join(r.dir, "schedule.json"))
			require.NoError(t, err)
			var sch chaos.Schedule
			require.NoError(t, json.Unmarshal(raw, &sch))
			require.Equal(t, r.sch, sch, "schedule.json is the schedule that runs")
			var stops int
			for _, p := range sch.Mandatory {
				if p.Kind != chaos.FaultStop {
					continue
				}
				stops++
				if id == "K13" {
					require.Equal(t, []string{"node2"}, p.Targets, "seed %d", seed)
				} else {
					drawn[p.Targets[0]] = true
				}
			}
			require.Equal(t, 1, stops)
		}
	}
	require.Greater(t, len(drawn), 1, "a control's F-stop target is drawn")
}

// K13 through Evaluate: the rewritten :9042 dial after G0 leaves G5 as the only failing gate
// (whether the dial itself succeeds is the registry's concern, TestRewriteOnce; G5 reads every attempt).
func TestK13ThroughEvaluate(t *testing.T) {
	reg := view.NewRegistry()
	r := newRecorded(t)
	arm, ok := canary.ArmAfterG0("K13", canary.G0Deps{Registry: reg, Node: netip.MustParseAddr("127.0.1.2"), Now: time.Now(),
		Event: r.canaryEvent})
	require.True(t, ok)
	r.canaryEvent(arm)
	d := reg.Dialer("primary", 0, 200*time.Millisecond, 0)
	_, err := d.DialContext(t.Context(), "tcp", "127.0.1.2:19042")
	require.ErrorContains(t, err, "rewritten")
	r.close(t)
	c := green()
	for _, dl := range reg.DialsToPort("9042") {
		c.Dials9042 = append(c.Dials9042, fmt.Sprintf("dial to %s at %s", dl.Dest, dl.Time.Format(time.RFC3339)))
	}
	require.Len(t, c.Dials9042, 1)
	v := Evaluate(c)
	require.Equal(t, []string{"G5"}, v.FailingGates)
	got := judgeRun(r.dir, "K13", v, EvidenceOf(v, nil))
	require.Equal(t, canary.Validated, got.Result, "%v", got.Reasons)
	good := v

	// G5 failing on another :9042 dial is not K13's detection.
	c.Dials9042 = []string{"dial to 127.0.1.3:9042 at " + time.Now().Format(time.RFC3339)}
	v = Evaluate(c)
	require.Equal(t, []string{"G5"}, v.FailingGates)
	got = judgeRun(r.dir, "K13", v, EvidenceOf(v, nil))
	require.Equal(t, canary.NotValidated, got.Result)
	require.Contains(t, strings.Join(got.Reasons, "\n"), "127.0.1.2:9042")

	// Nor is G5's rewritten dial without K13's own records.
	empty := newRecorded(t)
	empty.close(t)
	got = judgeRun(empty.dir, "K13", good, EvidenceOf(good, nil))
	require.Equal(t, canary.NotValidated, got.Result)
}

// The churner's close hook (K10) runs after an aux session's Close, with its id, and a nil hook changes nothing.
func TestChurnerAfterCloseHook(t *testing.T) {
	shortBudgets(t)
	chaos.ChurnBudget.Load = 10 * time.Millisecond
	for _, withHook := range []bool{false, true} {
		c, _, _ := testChurner(t)
		c.Create = func(*gocql.ClusterConfig) (*gocql.Session, error) { return nil, nil }
		var closed atomic.Bool
		c.CloseSession = func(*cell.Session) { closed.Store(true) }
		var after []string
		if withHook {
			c.AfterClose = func(id string) { after = append(after, fmt.Sprintf("%s closed=%v", id, closed.Load())) }
		}
		c.StartLoad = func(context.Context, *workload.Env, float64, int, workload.Mix, uint64) (Stopper, error) {
			release := make(chan struct{})
			close(release)
			return blockingStop{release}, nil
		}
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		require.NoError(t, c.Churn(ctx, chaos.Slot{ID: "C1"}))
		cancel()
		require.True(t, closed.Load())
		if withHook {
			require.Equal(t, []string{"aux0 closed=true"}, after, "once, after Close")
		}
	}
}

// K10 through Evaluate: 50 goroutines per churn fail G13's group drift and G12's teardown drift; G2 is collateral.
func TestK10ThroughEvaluate(t *testing.T) {
	c := green()
	for i := range c.Churns {
		if i < 3 {
			c.Churns[i].Before = map[string]int{"main": 1, canary.K10Group: 50 * i}
			c.Churns[i].After = map[string]int{"main": 1, canary.K10Group: 50 * (i + 1)}
		}
	}
	c.G12.After = map[string]int{"main": 2, canary.K10Group: 150}
	v := Evaluate(c)
	require.ElementsMatch(t, []string{"G12", "G13"}, v.FailingGates)
	r := newRecorded(t)
	r.close(t)
	got := judgeRun(r.dir, "K10", v, EvidenceOf(v, nil))
	require.Equal(t, canary.Validated, got.Result, "%v", got.Reasons)

	// The same failing gates through another group's drift: not K10's detection.
	c = green()
	for i := range c.Churns {
		if i < 3 {
			c.Churns[i].Before = map[string]int{"main": 1, "main.other": 50 * i}
			c.Churns[i].After = map[string]int{"main": 1, "main.other": 50 * (i + 1)}
		}
	}
	c.G12.After = map[string]int{"main": 2, "main.other": 150}
	v = Evaluate(c)
	require.ElementsMatch(t, []string{"G12", "G13"}, v.FailingGates)
	got = judgeRun(r.dir, "K10", v, EvidenceOf(v, nil))
	require.Equal(t, canary.NotValidated, got.Result)
	require.Contains(t, strings.Join(got.Reasons, "\n"), canary.K10Group)
}

// K16 through Evaluate and the persisted records: G14 fails alone, and the final-counts event, strict-decoded from
// events.jsonl beside the selection event, satisfies the delayed-share assertion.
func TestK16ThroughEvaluate(t *testing.T) {
	r := newRecorded(t)
	sel := canary.Selection{N: 20, Occupancy: 9.5, LHS: 12.1, RHS: 24, Classes: map[string]canary.ClassDelay{"write": {WarmupP99: 0.002, T: 0.003, D: 0.006, Rate: 495}}}
	r.canaryEvent(canary.Event{Canary: "K16", Kind: canary.KindArm, Time: time.Now(), Detail: "armed"})
	r.canaryEvent(canary.Event{Canary: "K16", Kind: canary.KindSelect, Time: time.Now(), Selection: &sel})
	counts := map[string]canary.ClassCounts{}
	for _, s := range workload.DefaultMix() {
		counts[string(s.Class)] = canary.ClassCounts{Total: 4000, Delayed: 200}
	}
	r.canaryEvent(canary.Event{Canary: "K16", Kind: canary.KindFinalCounts, Time: time.Now(), Counts: counts})
	r.close(t)
	c := green()
	c.CooldownP99["read"] = c.WarmupP99["read"] * 3
	v := Evaluate(c)
	require.Equal(t, []string{"G14"}, v.FailingGates)
	got := judgeRun(r.dir, "K16", v, EvidenceOf(v, nil))
	require.Equal(t, canary.Validated, got.Result, "%v", got.Reasons)

	// Too few delayed completions in one class: not validated through the assertion, with G14 still failing.
	short := newRecorded(t)
	low := maps.Clone(counts)
	low["lwt"] = canary.ClassCounts{Total: 4000, Delayed: 59}
	short.canaryEvent(canary.Event{Canary: "K16", Kind: canary.KindFinalCounts, Time: time.Now(), Counts: low})
	short.close(t)
	got = judgeRun(short.dir, "K16", v, EvidenceOf(v, nil))
	require.Equal(t, canary.NotValidated, got.Result)
	require.Contains(t, strings.Join(got.Reasons, "\n"), "class lwt: 59 of 4000")

	// A final-counts line that lost a field is not what the harness writes.
	bad := newRecorded(t)
	bad.close(t)
	line := `{"t":1,"time":"2026-09-28T10:00:00Z","kind":"canary","data":{"canary":"K16","kind":"final-counts","time":"2026-09-28T10:00:00Z","counts":{"lwt":{"total":4000}}}}` + "\n"
	require.NoError(t, os.WriteFile(filepath.Join(bad.dir, "events.jsonl"), []byte(line), 0o644))
	got = judgeRun(bad.dir, "K16", v, EvidenceOf(v, nil))
	require.Equal(t, canary.NotValidated, got.Result)
	require.Contains(t, strings.Join(got.Reasons, "\n"), "not what the harness writes")

	// A contaminated cool-down leaves G14 not evaluated: not validated.
	c.Windows = append(c.Windows, gate.Interval{Start: 6400, End: 6500})
	v = Evaluate(c)
	require.Equal(t, gate.StatusNotEvaluated, statusOf(v, "G14"))
	require.Equal(t, canary.NotValidated, judgeRun(r.dir, "K16", v, EvidenceOf(v, nil)).Result)
}

// Every canary whose extra assertion rejects empty records is judged through it: with its must-fail gates fired and
// no persisted records, judgeRun is not-validated (Codex BA01).
func TestEveryAssertionIsJudged(t *testing.T) {
	var asserted []string
	for _, s := range canary.All() {
		if len(canary.Assert(s.ID, gate.Verdict{}, canary.Records{})) == 0 {
			continue
		}
		asserted = append(asserted, s.ID)
		r := newRecorded(t)
		r.close(t)
		got := judgeRun(r.dir, s.ID, failing(s.MustFail...), Evidence{Complete: true})
		require.Equal(t, canary.NotValidated, got.Result, s.ID)
		require.Contains(t, strings.Join(got.Reasons, "\n"), "extra assertion", s.ID)
	}
	require.Equal(t, []string{"K7", "K8", "K10", "K13", "K15", "K16", "K17"}, asserted)
}
