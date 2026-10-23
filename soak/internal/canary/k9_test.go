package canary

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/proxy"
)

var k9Planned = chaos.Planned{Slot: "V2", Kind: chaos.FaultPause, Targets: []string{"node2"}, Active: time.Millisecond, Mandatory: true}

// k9World is a fake toxiproxy, F-pause and recovery, sharing one call log.
type k9World struct {
	mu     sync.Mutex
	calls  []string
	toxics map[string][]string
	// addErr, removeErr and listErr fail those calls; keep leaves a deleted toxic in the listing.
	addErr, removeErr, listErr error
	keep                       bool
	// installErr, holdErr and innerRemoveErr fail the inner fault's steps.
	installErr, holdErr, innerRemoveErr error
	// withheld makes recovery fail while any toxic is on node2; never makes it fail regardless.
	withheld, never bool
	releases        int
	events          []Event
	transitions     []chaos.Transition
}

func newK9World() *k9World { return &k9World{toxics: map[string][]string{}, withheld: true} }

// The hidden toxic is added only after the inner removal succeeded in the remove step,
// and Release deletes it on every exit path, counting a 404 as removed (PLAN §67.4, §67.9).
func TestK9WrapperOnEveryPath(t *testing.T) {
	const add = "add node2 soak-k9-hidden timeout downstream map[timeout:0]"
	recovery := []chaos.State{chaos.StateInstall, chaos.StateActive, chaos.StateRemove, chaos.StateRecover, chaos.StateFailed}
	t.Run("recovery withheld: G9 in recovery, the toxic deleted after failed", func(t *testing.T) {
		w := newK9World()
		out := w.run(t.Context(), nil)
		require.Equal(t, chaos.FailG9, out.Fail)
		require.Contains(t, out.Reason, "recovery:")
		require.Equal(t, recovery, w.states())
		require.Equal(t, []string{"install", "hold", "remove", add, "delete node2 soak-k9-hidden", "list node2"}, w.calls)
		require.Equal(t, []string{"add-toxic ", "delete-toxic "}, w.kinds())
		require.Empty(t, w.toxics["node2"])
		require.Equal(t, 1, w.releases, "the inner fault is released too")

		// The events lie where the assertion looks for them.
		at := map[chaos.State]time.Time{}
		for _, tr := range w.transitions {
			at[tr.State] = tr.Time
		}
		require.False(t, w.events[0].Time.Before(at[chaos.StateRemove]))
		require.False(t, w.events[0].Time.After(at[chaos.StateRecover]))
		require.False(t, w.events[1].Time.Before(at[chaos.StateFailed]))
		require.Equal(t, "node2", w.events[0].Proxy)
		require.Equal(t, K9Toxic, w.events[1].Toxic)
	})
	t.Run("recovery despite the toxic: done, then deleted", func(t *testing.T) {
		w := newK9World()
		w.withheld = false
		out := w.run(t.Context(), nil)
		require.Equal(t, chaos.StateDone, out.Final)
		require.Equal(t, []string{"add-toxic ", "delete-toxic "}, w.kinds())
	})
	t.Run("a failed install: the undo adds nothing", func(t *testing.T) {
		w := newK9World()
		w.installErr = errors.New("boom")
		out := w.run(t.Context(), nil)
		require.Equal(t, chaos.FailFixture, out.Fail)
		require.Equal(t, []string{"install", "remove"}, w.calls)
		require.Empty(t, w.events)
		require.Equal(t, 1, w.releases)
	})
	t.Run("a failed hold: the undo adds nothing", func(t *testing.T) {
		w := newK9World()
		w.holdErr = errors.New("boom")
		out := w.run(t.Context(), nil)
		require.Equal(t, chaos.FailFixture, out.Fail)
		require.Equal(t, []string{"install", "hold", "remove"}, w.calls)
		require.Empty(t, w.events)
	})
	t.Run("a failed inner removal: a fixture failure, no toxic", func(t *testing.T) {
		w := newK9World()
		w.innerRemoveErr = errors.New("still paused")
		out := w.run(t.Context(), nil)
		require.Equal(t, chaos.FailFixture, out.Fail)
		require.Equal(t, []string{"install", "hold", "remove"}, w.calls)
		require.Empty(t, w.events)
	})
	t.Run("a context cancelled on entering recovery: G9, the toxic deleted without it", func(t *testing.T) {
		w := newK9World()
		w.never = true
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		out := w.run(ctx, func(tr chaos.Transition) {
			if tr.State == chaos.StateRecover {
				cancel()
			}
		})
		require.Equal(t, chaos.FailG9, out.Fail)
		require.Contains(t, out.Reason, "recovery:")
		require.Contains(t, out.Reason, context.Canceled.Error())
		require.Equal(t, recovery, w.states())
		require.Equal(t, []string{"add-toxic ", "delete-toxic "}, w.kinds())
		require.Empty(t, w.toxics["node2"])
	})
	t.Run("a failed addition is recorded, not returned; the deletion still runs and counts a 404 as removed", func(t *testing.T) {
		w := newK9World()
		w.addErr = errors.New("api down")
		out := w.run(t.Context(), nil)
		require.Equal(t, chaos.StateDone, out.Final, "nothing withholds the recovery")
		require.Equal(t, []string{"add-toxic api down", "delete-toxic "}, w.kinds())
	})
	t.Run("the listing still shows the toxic", func(t *testing.T) {
		w := newK9World()
		w.keep, w.withheld = true, false
		w.run(t.Context(), nil)
		require.Equal(t, []string{"add-toxic ", "delete-toxic toxic soak-k9-hidden still present on node2"}, w.kinds())
	})
	t.Run("a failed deletion or listing", func(t *testing.T) {
		w := newK9World()
		w.removeErr, w.withheld = errors.New("refused"), false
		w.run(t.Context(), nil)
		require.Equal(t, "delete-toxic delete soak-k9-hidden: refused", w.kinds()[1])
		w = newK9World()
		w.listErr, w.withheld = errors.New("refused"), false
		w.run(t.Context(), nil)
		require.Equal(t, "delete-toxic list toxics: refused", w.kinds()[1])
	})
	t.Run("Release is idempotent", func(t *testing.T) {
		w := newK9World()
		f := WrapFault("K9", k9Planned, w, FaultDeps{Ops: w, Event: func(e Event) { w.events = append(w.events, e) }})
		r, ok := f.(chaos.Releaser)
		require.True(t, ok)
		require.NoError(t, f.Hold(t.Context(), 0))
		require.NoError(t, f.Remove(t.Context()))
		r.Release()
		r.Release()
		require.Equal(t, []string{"add-toxic ", "delete-toxic "}, w.kinds())
		require.Equal(t, 2, w.releases)
	})
}

// Only K9's validation F-pause is wrapped: no other canary, slot, kind or target count gets a hook (PLAN §67.9).
func TestK9WrapsOnlyItsFault(t *testing.T) {
	w := newK9World()
	d := FaultDeps{Ops: w, Event: func(Event) { t.Fatal("no event") }}
	other := func(edit func(*chaos.Planned)) chaos.Planned {
		p := k9Planned
		p.Targets = slices.Clone(p.Targets)
		edit(&p)
		return p
	}
	for _, id := range []string{"", "K7", "K6a", "K13"} {
		require.Equal(t, chaos.Fault(w), WrapFault(id, k9Planned, w, d), id)
	}
	for _, p := range []chaos.Planned{
		other(func(p *chaos.Planned) { p.Slot = "V1" }),
		other(func(p *chaos.Planned) { p.Kind = chaos.FaultStop }),
		other(func(p *chaos.Planned) { p.Targets = []string{"node1", "node2"} }),
	} {
		require.Equal(t, chaos.Fault(w), WrapFault("K9", p, w, d), "%+v", p)
	}
	_, wrapped := WrapFault("K9", k9Planned, w, d).(*hiddenToxic)
	require.True(t, wrapped)
}

// K9's assertion over constructed records; cellrun's tests run the full matrix through the record reader.
func TestAssertK9(t *testing.T) {
	require.Empty(t, Assert("K9", gate.Verdict{}, k9Records()))
	rec := k9Records()
	rec.Events[0].Proxy = "node1"
	requireAssertion(t, Assert("K9", gate.Verdict{}, rec), "want soak-k9-hidden on node2")
	rec = k9Records()
	rec.Report.Outcomes[1].Reason = "remove: still paused"
	requireAssertion(t, Assert("K9", gate.Verdict{}, rec), "in recovery")
	rec = k9Records()
	rec.Transitions = rec.Transitions[:4]
	requireAssertion(t, Assert("K9", gate.Verdict{}, rec), "V2's transitions are [install active remove recover],")
	rec = k9Records()
	rec.Transitions[3].T = 1700
	requireAssertion(t, Assert("K9", gate.Verdict{}, rec), "recover transition precedes its remove transition")
	requireAssertion(t, Assert("K9", gate.Verdict{}, Records{}), "no schedule")
}

// k9Records is S8's shape (PLAN §67.2): V2 on node2 failed G9 in recovery;
// the toxic was added in its remove step and deleted after it failed.
func k9Records() Records {
	base := time.Date(2026, 10, 22, 14, 43, 0, 0, time.UTC)
	tr := func(sec int, s chaos.State) Transition {
		return Transition{T: float64(sec), Transition: chaos.Transition{Time: base.Add(time.Duration(sec) * time.Second), ID: "V2", Kind: chaos.FaultPause, State: s}}
	}
	ev := func(sec int, kind string) Event {
		return Event{Canary: "K9", Kind: kind, Time: base.Add(time.Duration(sec) * time.Second), Proxy: "node2", Toxic: K9Toxic}
	}
	return Records{
		Schedule: &chaos.Schedule{Mandatory: []chaos.Planned{
			{Slot: "V1", Kind: chaos.FaultStop, Targets: []string{"node3"}, Mandatory: true},
			{Slot: "V2", Kind: chaos.FaultPause, Targets: []string{"node2"}, Mandatory: true},
		}},
		Report: &chaos.Report{Outcomes: []chaos.Outcome{
			{ID: "V1", Kind: chaos.FaultStop, Final: chaos.StateDone},
			{ID: "V2", Kind: chaos.FaultPause, Final: chaos.StateFailed, Fail: chaos.FailG9, Reason: "recovery: not recovered within 2m0s: context deadline exceeded"},
		}},
		Transitions: []Transition{tr(1650, chaos.StateInstall), tr(1650, chaos.StateActive), tr(1736, chaos.StateRemove),
			tr(1736, chaos.StateRecover), tr(1856, chaos.StateFailed)},
		Events: []Event{ev(1736, KindAddToxic), ev(1857, KindDeleteToxic)},
	}
}

// AddToxic records the call and adds the toxic unless addErr is set.
func (w *k9World) AddToxic(_ context.Context, p string, t proxy.Toxic) error {
	w.log("add %s %s %s %s %v", p, t.Name, t.Type, t.Stream, t.Attributes)
	if w.addErr != nil {
		return w.addErr
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.toxics[p] = append(w.toxics[p], t.Name)
	return nil
}

// RemoveToxic records the call and deletes the toxic, answering a missing one with a wrapped proxy.ErrNotFound.
func (w *k9World) RemoveToxic(_ context.Context, p, name string) error {
	w.log("delete %s %s", p, name)
	if w.removeErr != nil {
		return w.removeErr
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	i := slices.Index(w.toxics[p], name)
	if i < 0 {
		return fmt.Errorf("toxic %s: %w", name, proxy.ErrNotFound)
	}
	if !w.keep {
		w.toxics[p] = slices.Delete(w.toxics[p], i, i+1)
	}
	return nil
}

// Toxics records the call and lists the proxy's toxics.
func (w *k9World) Toxics(_ context.Context, p string) ([]string, error) {
	w.log("list %s", p)
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.toxics[p]), w.listErr
}

// Install, Hold and Remove are the inner F-pause's steps; Release counts its releases.
func (w *k9World) Install(context.Context) error             { w.log("install"); return w.installErr }
func (w *k9World) Hold(context.Context, time.Duration) error { w.log("hold"); return w.holdErr }
func (w *k9World) Remove(context.Context) error              { w.log("remove"); return w.innerRemoveErr }
func (w *k9World) Release()                                  { w.releases++ }

// Sample is the recovery condition: it fails while the toxic withholds node2, or always when never is set.
func (w *k9World) Sample(context.Context) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.never || w.withheld && len(w.toxics["node2"]) > 0 {
		return []string{"R1: host 127.0.1.2 is not UP"}
	}
	return nil
}

func (w *k9World) log(format string, args ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls = append(w.calls, fmt.Sprintf(format, args...))
}

// run drives the wrapped F-pause through the real lifecycle with millisecond bounds; on, when set, sees each transition.
func (w *k9World) run(ctx context.Context, on func(chaos.Transition)) chaos.Outcome {
	f := WrapFault("K9", k9Planned, w, FaultDeps{Ops: w, Event: func(e Event) { w.events = append(w.events, e) }})
	t := chaos.Timing{Spec: chaos.Spec{Install: time.Second, Remove: time.Second, Recovery: 50 * time.Millisecond},
		Active: time.Millisecond, Interval: 5 * time.Millisecond, Consecutive: 2, Slack: time.Second}
	return chaos.RunLifecycle(ctx, "V2", chaos.FaultPause, f, w, t, &chaos.Windows{}, 1, func(tr chaos.Transition) {
		w.transitions = append(w.transitions, tr)
		if on != nil {
			on(tr)
		}
	})
}

func (w *k9World) states() []chaos.State {
	var out []chaos.State
	for _, tr := range w.transitions {
		out = append(out, tr.State)
	}
	return out
}

func (w *k9World) kinds() []string {
	var out []string
	for _, e := range w.events {
		if e.Canary != "K9" {
			out = append(out, "canary "+e.Canary)
		}
		out = append(out, e.Kind+" "+e.Err)
	}
	return out
}
