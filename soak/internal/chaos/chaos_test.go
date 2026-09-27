package chaos

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var nodes = []string{"node1", "node2", "node3"}

func TestOuterDeadlinesMatchThePlan(t *testing.T) {
	specs := Specs()
	want := map[Kind]time.Duration{
		FaultStop: 390 * time.Second, FaultKill: 390 * time.Second, FaultPause: 6 * time.Minute,
		FaultTwo: 450 * time.Second, FaultFull: 7 * time.Minute, FaultLat: 6 * time.Minute, FaultBW: 6 * time.Minute,
		FaultBlack: 390 * time.Second, FaultHalfOpen: 6 * time.Minute, FaultReset: 4 * time.Minute,
		FaultDDL: 210 * time.Second, FaultRoll: 1170 * time.Second, FaultTopo: 18 * time.Minute,
	}
	for k, w := range want {
		require.Equal(t, w, OuterDeadline(specs, k, specs[k].ActiveMax), k)
	}
}

func TestPreflightAcceptsEveryPlannedAssignment(t *testing.T) {
	specs := Specs()
	short := []Kind{FaultStop, FaultKill, FaultPause, FaultTwo, FaultFull, FaultLat, FaultBW, FaultBlack, FaultHalfOpen, FaultReset}
	for _, a := range []Assignment{
		PhaseOneAssignment(),
		{Short: []Kind{FaultKill, FaultPause, FaultStop, FaultLat, FaultBlack, FaultFull}, Long: FaultTopo},
		{Short: []Kind{FaultKill, FaultPause, FaultTwo, FaultBW, FaultHalfOpen, FaultReset}, Long: FaultRoll},
	} {
		sch, err := Plan(1, NightTimetable(), a, true, nodes, short, specs)
		require.NoError(t, err)
		require.NoError(t, Preflight(sch, NightChaosEnd, specs))
		require.Len(t, sch.Mandatory, 8)
	}
	val, err := Plan(1, ValidationTimetable(), Assignment{Short: []Kind{FaultStop, FaultPause}}, false, nodes, nil, specs)
	require.NoError(t, err)
	require.NoError(t, Preflight(val, ValidationChaosEnd, specs))
	require.Equal(t, []Kind{FaultStop, FaultPause}, []Kind{val.Mandatory[0].Kind, val.Mandatory[1].Kind})
	require.Empty(t, val.Optional)
}

func TestPreflightRejects(t *testing.T) {
	specs := Specs()
	sch, err := Plan(1, NightTimetable(), PhaseOneAssignment(), true, nodes, nil, specs)
	require.NoError(t, err)

	longInShort := sch
	longInShort.Mandatory = slices.Clone(sch.Mandatory)
	for i := range longInShort.Mandatory {
		if longInShort.Mandatory[i].Slot == "M1" {
			longInShort.Mandatory[i].Kind = FaultRoll
		}
	}
	require.ErrorContains(t, Preflight(longInShort, NightChaosEnd, specs), "exceeds the slot")

	overlap := sch
	overlap.Slots = slices.Clone(sch.Slots)
	overlap.Slots[1].Start -= time.Minute
	require.ErrorContains(t, Preflight(overlap, NightChaosEnd, specs), "overlaps")

	require.ErrorContains(t, Preflight(sch, 100*time.Minute, specs), "after")

	shortChurn := sch
	shortChurn.Slots = slices.Clone(sch.Slots)
	shortChurn.Slots[1].End = shortChurn.Slots[1].Start + time.Minute
	require.ErrorContains(t, Preflight(shortChurn, NightChaosEnd, specs), "shorter than")
}

func TestPlanIsSeededAndShaped(t *testing.T) {
	specs := Specs()
	a, err := Plan(7, NightTimetable(), PhaseOneAssignment(), true, nodes, []Kind{FaultStop, FaultPause}, specs)
	require.NoError(t, err)
	b, err := Plan(7, NightTimetable(), PhaseOneAssignment(), true, nodes, []Kind{FaultStop, FaultPause}, specs)
	require.NoError(t, err)
	require.Equal(t, a, b, "the same seed gives the same schedule")
	c, err := Plan(8, NightTimetable(), PhaseOneAssignment(), true, nodes, nil, specs)
	require.NoError(t, err)
	require.NotEqual(t, a.Mandatory, c.Mandatory)

	var shortKinds []Kind
	for _, p := range a.Mandatory {
		sp := specs[p.Kind]
		switch p.Slot {
		case "M0":
			require.Equal(t, FaultDDL, p.Kind)
		case "L":
			require.Equal(t, FaultRoll, p.Kind)
			require.Equal(t, nodes, p.Targets)
			require.Len(t, p.SubActive, 3)
		default:
			shortKinds = append(shortKinds, p.Kind)
			require.Len(t, p.Targets, min(sp.Width, 3))
			require.GreaterOrEqual(t, p.Active, sp.ActiveMin)
			require.LessOrEqual(t, p.Active, sp.ActiveMax)
		}
	}
	require.ElementsMatch(t, PhaseOneAssignment().Short, shortKinds, "M1–M6 are a permutation of the set")
	require.NotEmpty(t, a.Optional)
}

// F-two's worst case equals the 7.5 min short slot (PLAN §5.4, v7.4): equality passes, one second less does not.
func TestPreflightSlotBoundary(t *testing.T) {
	specs := Specs()
	worst := OuterDeadline(specs, FaultTwo, specs[FaultTwo].ActiveMax)
	require.Equal(t, 7*time.Minute+30*time.Second, worst)
	sch := func(length time.Duration) Schedule {
		return Schedule{
			Slots:     []Slot{{ID: "M1", Kind: SlotShort, Start: 0, End: length}},
			Mandatory: []Planned{{Slot: "M1", Kind: FaultTwo, Mandatory: true}},
		}
	}
	require.NoError(t, Preflight(sch(worst), NightChaosEnd, specs))
	require.Error(t, Preflight(sch(worst-time.Second), NightChaosEnd, specs))
}

func TestAdmitOptional(t *testing.T) {
	specs := Specs()
	slot := Slot{ID: "M1", Kind: SlotShort, Start: 0, End: 20 * time.Minute}
	p := Planned{Kind: FaultStop}
	require.True(t, AdmitOptional(12*time.Minute+30*time.Second, slot, p, specs), "12.5 min + 60 s + 6.5 min = 20 min fits")
	require.False(t, AdmitOptional(12*time.Minute+31*time.Second, slot, p, specs))
}

// fake is a Fault whose steps can fail or hang.
type fake struct {
	installErr, holdErr, removeErr error
	hangInstall, hangRemove        bool
	installs, removes              atomic.Int32
}

func (f *fake) Install(ctx context.Context) error {
	f.installs.Add(1)
	if f.hangInstall {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.installErr
}

func (f *fake) Hold(ctx context.Context, d time.Duration) error {
	if f.holdErr != nil {
		return f.holdErr
	}
	return Sleep(ctx, d)
}

func (f *fake) Remove(ctx context.Context) error {
	f.removes.Add(1)
	if f.hangRemove {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.removeErr
}

// recovery holds after `after` failing samples.
type recovery struct {
	mu      sync.Mutex
	after   int
	samples int
}

func (r *recovery) Sample(context.Context) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples++
	if r.samples > r.after {
		return nil
	}
	return []string{"not yet"}
}

func fastTiming() Timing {
	ms := time.Millisecond
	return Timing{Spec: Spec{Install: 50 * ms, Remove: 50 * ms, Recovery: 200 * ms}, Active: 20 * ms, Interval: 5 * ms, Consecutive: 3,
		Slack: 30 * ms}
}

func TestLifecycleDone(t *testing.T) {
	var w Windows
	var states []State
	rec := &recovery{after: 2}
	out := RunLifecycle(context.Background(), "M1", FaultStop, &fake{}, rec, fastTiming(), &w, 1,
		func(tr Transition) { states = append(states, tr.State) })
	require.Equal(t, StateDone, out.Final)
	require.Equal(t, []State{StateInstall, StateActive, StateRemove, StateRecover, StateDone}, states)
	require.Equal(t, 5, rec.samples, "two failures, then three in a row")
	require.False(t, out.Recovered.IsZero())
	win := w.All()[0]
	require.False(t, win.Failed)
	require.Equal(t, out.End.Add(windowTail), win.End)
}

func TestLifecycleFailures(t *testing.T) {
	tests := []struct {
		name  string
		f     *fake
		rec   *recovery
		t     Timing
		class FailClass
		undo  bool
	}{
		{"install error is fixture, then undone", &fake{installErr: errors.New("boom")}, &recovery{}, fastTiming(), FailFixture, true},
		{"install overrun is fixture", &fake{hangInstall: true}, &recovery{}, fastTiming(), FailFixture, true},
		{"remove error is fixture", &fake{removeErr: errors.New("boom")}, &recovery{}, fastTiming(), FailFixture, false},
		{"remove overrun is fixture", &fake{hangRemove: true}, &recovery{}, fastTiming(), FailFixture, false},
		{"hold error is fixture, then undone", &fake{holdErr: errors.New("ddl failed")}, &recovery{}, fastTiming(), FailFixture, true},
		{"no recovery is G9", &fake{}, &recovery{after: 1 << 30}, fastTiming(), FailG9, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var w Windows
			out := RunLifecycle(context.Background(), "M2", FaultKill, tt.f, tt.rec, tt.t, &w, 1, nil)
			require.Equal(t, StateFailed, out.Final)
			require.Equal(t, tt.class, out.Fail, out.Reason)
			if tt.undo {
				require.Equal(t, int32(1), tt.f.removes.Load(), "a failed install or hold is undone once")
			}
			win := w.All()[0]
			require.True(t, win.Failed)
			require.True(t, win.End.IsZero(), "a failed window stays open")
			if tt.class == FailG9 {
				require.Equal(t, []string{"not yet"}, out.LastProblems)
			}
		})
	}
}

// hangingHold is a fault whose active step ignores its duration, like a stuck F-ddl.
type hangingHold struct{ fake }

func (h *hangingHold) Hold(ctx context.Context, _ time.Duration) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestLifecycleOuterDeadlineIsG9(t *testing.T) {
	tm := fastTiming()
	tm.Slack = 20 * time.Millisecond
	var w Windows
	start := time.Now()
	out := RunLifecycle(context.Background(), "M3", FaultDDL, &hangingHold{}, &recovery{}, tm, &w, 0, nil)
	require.Equal(t, FailG9, out.Fail, out.Reason)
	require.Contains(t, out.Reason, "outer deadline")
	require.Less(t, time.Since(start), 2*time.Second)
}

func TestWindows(t *testing.T) {
	var w Windows
	t0 := time.Unix(1000, 0)
	w.Open("M1", FaultStop, 1, t0)
	w.Close("M1", t0.Add(time.Minute))
	w.Open("M2", FaultTwo, 2, t0.Add(10*time.Minute))
	w.Fail("M2")

	_, in := w.At(t0.Add(-time.Second))
	require.False(t, in)
	win, in := w.At(t0.Add(time.Minute + 5*time.Second))
	require.True(t, in, "done + 10 s is still inside")
	require.Equal(t, "M1", win.ID)
	_, in = w.At(t0.Add(time.Minute + 11*time.Second))
	require.False(t, in)
	win, in = w.At(t0.Add(5 * time.Hour))
	require.True(t, in, "a failed window never closes")
	require.Equal(t, 2, win.Width)

	iv := w.Intervals(t0, t0.Add(time.Hour))
	require.Len(t, iv, 2)
	require.InDelta(t, 70, iv[0].End, 0)
	require.InDelta(t, 3600, iv[1].End, 0)
}

type builder struct {
	mu    sync.Mutex
	built []Planned
	fail  map[string]*fake
}

func (b *builder) build(p Planned) (Fault, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.built = append(b.built, p)
	if f, ok := b.fail[p.Targets[0]+string(p.Kind)]; ok {
		return f, nil
	}
	return &fake{}, nil
}

func fastSpecs() map[Kind]Spec {
	ms := time.Millisecond
	sp := Spec{Install: 20 * ms, ActiveMin: 5 * ms, ActiveMax: 5 * ms, Remove: 20 * ms, Recovery: 100 * ms, Width: 1}
	two := sp
	two.Width = 2
	return map[Kind]Spec{FaultStop: sp, FaultKill: sp, FaultPause: sp, FaultTwo: two, FaultDDL: sp, FaultRoll: {SubFaults: 3, Width: 1}}
}

func fastSchedule(t *testing.T, a Assignment, optional []Kind) Schedule {
	ms := time.Millisecond
	slots := []Slot{
		{"M0", SlotDDL, 0, 400 * ms},
		{"C1", SlotChurn, 400 * ms, 450 * ms},
		{"M1", SlotShort, 450 * ms, 850 * ms},
		{"L", SlotLong, 850 * ms, 2 * time.Second},
		{"C2", SlotChurn, 2 * time.Second, 2050 * ms},
	}
	sch, err := Plan(3, slots, a, false, nodes, optional, fastSpecs())
	require.NoError(t, err)
	return sch
}

func newExecutor(sch Schedule, b *builder, churn func(context.Context, Slot) error) *Executor {
	return &Executor{
		Schedule: sch, Specs: fastSpecs(), Start: time.Now(), Build: b.build, Recovery: &recovery{},
		Churn: churn, Windows: &Windows{}, Interval: 5 * time.Millisecond,
	}
}

func TestExecutorRunsEverySlot(t *testing.T) {
	b := &builder{}
	var churns atomic.Int32
	e := newExecutor(fastSchedule(t, Assignment{Short: []Kind{FaultKill}, Long: FaultRoll}, nil), b,
		func(context.Context, Slot) error { churns.Add(1); return nil })
	rep := e.Run(context.Background())
	require.Empty(t, rep.Fixture)
	require.Empty(t, rep.FaultsStopped)
	require.Equal(t, 3, rep.MandatoryFaults)
	require.Equal(t, 3, rep.MandatoryExecuted)
	require.Equal(t, 2, rep.ChurnExecuted)
	require.Equal(t, int32(2), churns.Load())
	var kinds []Kind
	for _, p := range b.built {
		kinds = append(kinds, p.Kind)
	}
	require.Equal(t, []Kind{FaultDDL, FaultKill, FaultStop, FaultStop, FaultStop}, kinds, "the roll runs as three F-stops")
	require.Len(t, rep.Outcomes, 5)
}

func TestExecutorAdmitsOptionalFaultsThatFit(t *testing.T) {
	b := &builder{}
	sch := fastSchedule(t, Assignment{Short: []Kind{FaultKill}, Long: FaultRoll}, []Kind{FaultPause})
	e := newExecutor(sch, b, func(context.Context, Slot) error { return nil })
	rep := e.Run(context.Background())
	// The 60 s optional gap never fits a sub-second slot, so nothing optional runs.
	require.Len(t, rep.Outcomes, 5)

	// With a gap that fits, optional faults run after the mandatory one, inside its slot, and count apart.
	b = &builder{}
	e = newExecutor(fastSchedule(t, Assignment{Short: []Kind{FaultKill}, Long: FaultRoll}, []Kind{FaultPause}), b,
		func(context.Context, Slot) error { return nil })
	e.OptionalGap, e.Slack = 5*time.Millisecond, 5*time.Millisecond
	rep = e.Run(context.Background())
	var optional []Outcome
	for _, o := range rep.Outcomes {
		if strings.HasSuffix(o.ID, "+opt") {
			optional = append(optional, o)
		}
	}
	require.NotEmpty(t, optional, "an optional fault ran")
	for _, o := range optional {
		require.Equal(t, StateDone, o.Final)
		require.Equal(t, FaultPause, o.Kind)
	}
	require.Equal(t, 3, rep.MandatoryExecuted, "optional faults never count as mandatory")
	for _, sl := range e.Schedule.Slots {
		for _, o := range optional {
			if strings.HasPrefix(o.ID, sl.ID+"+") {
				require.False(t, o.End.After(e.Start.Add(sl.End)), "an optional fault ends inside its slot")
			}
		}
	}
}

func TestExecutorStopsFaultsAfterG9(t *testing.T) {
	b := &builder{fail: map[string]*fake{}}
	sch := fastSchedule(t, Assignment{Short: []Kind{FaultKill}, Long: FaultRoll}, nil)
	var target string
	for _, p := range sch.Mandatory {
		if p.Slot == "M1" {
			target = p.Targets[0]
		}
	}
	e := newExecutor(sch, b, func(context.Context, Slot) error { return nil })
	e.Recovery = &recoveryFor{bad: target}
	rep := e.Run(context.Background())
	require.NotEmpty(t, rep.FaultsStopped)
	require.Len(t, rep.G9Failures, 1)
	require.Equal(t, 2, rep.MandatoryExecuted, "the long fault after the G9 failure is not injected")
	require.Equal(t, 2, rep.ChurnExecuted, "churn still runs")
}

// recoveryFor never recovers while the fault on bad is the last one built.
type recoveryFor struct {
	bad   string
	calls atomic.Int32
}

func (r *recoveryFor) Sample(ctx context.Context) []string {
	if r.calls.Add(1) > 3 && r.calls.Load() < 1000 {
		return []string{"stuck on " + r.bad}
	}
	return nil
}

func TestExecutorAbortsOnFixtureFailure(t *testing.T) {
	sch := fastSchedule(t, Assignment{Short: []Kind{FaultKill}, Long: FaultRoll}, nil)
	var m1 Planned
	for _, p := range sch.Mandatory {
		if p.Slot == "M1" {
			m1 = p
		}
	}
	b := &builder{fail: map[string]*fake{m1.Targets[0] + string(FaultKill): {installErr: errors.New("node would not stop")}}}
	e := newExecutor(sch, b, func(context.Context, Slot) error { return nil })
	rep := e.Run(context.Background())
	require.Contains(t, rep.Fixture, "node would not stop")
	require.Equal(t, 1, rep.ChurnExecuted, "the cell aborts: nothing after M1 runs")
}

func TestExecutorChurnErrorFailsG13Only(t *testing.T) {
	b := &builder{}
	sch := fastSchedule(t, Assignment{Short: []Kind{FaultKill}, Long: FaultRoll}, nil)
	e := newExecutor(sch, b, func(ctx context.Context, sl Slot) error {
		if sl.ID == "C1" {
			return errors.New("aux session left residue")
		}
		return nil
	})
	rep := e.Run(context.Background())
	require.Len(t, rep.ChurnFailures, 1)
	require.Empty(t, rep.FaultsStopped, "a churn failure within its deadline fails G13 only")
}

func TestExecutorChurnOverrunStopsFaults(t *testing.T) {
	b := &builder{}
	sch := fastSchedule(t, Assignment{Short: []Kind{FaultKill}, Long: FaultRoll}, nil)
	e := newExecutor(sch, b, func(ctx context.Context, sl Slot) error {
		if sl.ID == "C1" {
			time.Sleep(30 * time.Millisecond) // ignores its deadline, like a hung CreateSession owner
		}
		return nil
	})
	e.ChurnDeadline = 10 * time.Millisecond
	rep := e.Run(context.Background())
	require.Len(t, rep.ChurnFailures, 1)
	require.Contains(t, rep.FaultsStopped, "C1")
	require.Equal(t, 1, rep.MandatoryExecuted, "no fault after the overrun")
	require.Equal(t, 2, rep.ChurnExecuted)
}

// A churn that reports unresolved work at its deadline stops faults even when it returns in time (Codex I03).
func TestExecutorChurnOverrunSentinelStopsFaults(t *testing.T) {
	b := &builder{}
	sch := fastSchedule(t, Assignment{Short: []Kind{FaultKill}, Long: FaultRoll}, nil)
	e := newExecutor(sch, b, func(ctx context.Context, sl Slot) error {
		if sl.ID == "C1" {
			return fmt.Errorf("aux0 create: %w", ErrChurnOverrun)
		}
		return nil
	})
	rep := e.Run(context.Background())
	require.Contains(t, rep.FaultsStopped, "C1")
	require.Equal(t, 1, rep.MandatoryExecuted)
}
