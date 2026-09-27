package cellrun

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	o.Canary = "K10"
	require.ErrorContains(t, newPrepareRun(o).prepare(), "K10")
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
