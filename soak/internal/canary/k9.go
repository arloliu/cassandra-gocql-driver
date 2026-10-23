package canary

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/proxy"
)

// K9Toxic is the name of K9's hidden toxic (PLAN §67.4).
const K9Toxic = "soak-k9-hidden"

// KindAddToxic and KindDeleteToxic are K9's events: the hidden toxic's addition and its deletion (PLAN §67.4).
const (
	KindAddToxic    = "add-toxic"
	KindDeleteToxic = "delete-toxic"
)

const (
	// k9Slot is the validation F-pause's slot, the one K9 wraps.
	k9Slot = "V2"
	// k9ReleaseBound bounds the deletion and the listing check in Release, which has no context of its own.
	k9ReleaseBound = 30 * time.Second
)

// ToxicOps is the part of toxiproxy's API K9 uses; *proxy.Client implements it.
type ToxicOps interface {
	// AddToxic adds a toxic to a proxy.
	//
	// Parameters:
	//   - ctx: bounds the call
	//   - proxyName: the proxy, nodeN
	//   - t: the toxic
	//
	// Returns:
	//   - error: on a transport failure or a refused addition
	AddToxic(ctx context.Context, proxyName string, t proxy.Toxic) error
	// RemoveToxic deletes a toxic from a proxy.
	//
	// Parameters:
	//   - ctx: bounds the call
	//   - proxyName: the proxy, nodeN
	//   - name: the toxic's name
	//
	// Returns:
	//   - error: proxy.ErrNotFound, wrapped, when the toxic does not exist; any other failure as it is
	RemoveToxic(ctx context.Context, proxyName, name string) error
	// Toxics lists a proxy's toxics.
	//
	// Parameters:
	//   - ctx: bounds the call
	//   - proxyName: the proxy, nodeN
	//
	// Returns:
	//   - []string: the toxics' names
	//   - error: on a transport failure
	Toxics(ctx context.Context, proxyName string) ([]string, error)
}

var _ ToxicOps = (*proxy.Client)(nil)

// FaultDeps is what a canary's fault wrapper acts through; cellrun supplies it.
type FaultDeps struct {
	// Ops reaches toxiproxy.
	Ops ToxicOps
	// Event records a canary event in events.jsonl.
	Event func(Event)
	// Now is the clock; nil uses time.Now.
	Now func() time.Time
}

// hiddenToxic is K9's F-pause: the inner fault's steps, a toxic added after its removal succeeded,
// and the toxic's deletion in Release.
// The lifecycle calls its steps and Release from one goroutine.
type hiddenToxic struct {
	inner chaos.Fault
	proxy string
	d     FaultDeps
	// held is set when the inner fault was installed and held, so the next Remove is the remove step, not an undo.
	held bool
	// added is set once the addition was attempted; released once the deletion was.
	added, released bool
}

var (
	_ chaos.Fault    = (*hiddenToxic)(nil)
	_ chaos.Releaser = (*hiddenToxic)(nil)
)

// K9HiddenToxic returns K9's toxic: a downstream timeout of 0, which holds every response on the proxy (PLAN §67.4).
//
// Returns:
//   - proxy.Toxic: the toxic
func K9HiddenToxic() proxy.Toxic {
	return proxy.Toxic{Name: K9Toxic, Type: "timeout", Stream: "downstream", Toxicity: 1, Attributes: map[string]any{"timeout": 0}}
}

// WrapFault returns a planned fault as a canary runs it (PLAN §67.4).
// K9 wraps the validation F-pause, slot V2,
// so that once its own removal succeeded a hidden toxic withholds the driver's recovery;
// the lifecycle's Release deletes the toxic on every exit path.
// Every other fault, and every other canary's, is returned unchanged.
//
// Parameters:
//   - id: the canary id, empty for none
//   - p: the planned fault
//   - f: the fault as built
//   - d: the dependencies
//
// Returns:
//   - chaos.Fault: the fault to run; it implements chaos.Releaser when wrapped
func WrapFault(id string, p chaos.Planned, f chaos.Fault, d FaultDeps) chaos.Fault {
	if id != "K9" || p.Slot != k9Slot || p.Kind != chaos.FaultPause || len(p.Targets) != 1 {
		return f
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	return &hiddenToxic{inner: f, proxy: p.Targets[0], d: d}
}

// assertK9 checks that V2 failed G9 in its recovery
// and that the hidden toxic sat on V2's node from inside its remove step until it failed (PLAN §67.4):
// the report's outcome, V2's transitions, its schedule entry and K9's own events.
func assertK9(rec Records) []string {
	if rec.Schedule == nil {
		return []string{"no schedule"}
	}
	var entries []chaos.Planned
	for _, p := range rec.Schedule.Mandatory {
		if p.Slot == k9Slot {
			entries = append(entries, p)
		}
	}
	if len(entries) != 1 {
		return []string{fmt.Sprintf("%d mandatory %s entries in the schedule, want 1", len(entries), k9Slot)}
	}
	if e := entries[0]; e.Kind != chaos.FaultPause || len(e.Targets) != 1 {
		return []string{fmt.Sprintf("%s holds %s with targets %v, want %s with one target", k9Slot, e.Kind, e.Targets, chaos.FaultPause)}
	}
	target := entries[0].Targets[0]

	if rec.Report == nil {
		return []string{"no report event"}
	}
	var outs []chaos.Outcome
	for _, o := range rec.Report.Outcomes {
		if o.ID == k9Slot {
			outs = append(outs, o)
		}
	}
	if len(outs) != 1 {
		return []string{fmt.Sprintf("%d outcomes of %s in the report, want 1", len(outs), k9Slot)}
	}
	o := outs[0]
	if o.Kind != chaos.FaultPause || o.Final != chaos.StateFailed || o.Fail != chaos.FailG9 || !strings.HasPrefix(o.Reason, "recovery:") {
		return []string{fmt.Sprintf("%s's outcome is %s %s %s %q, want F-pause failed G9 in recovery", k9Slot, o.Kind, o.Final, o.Fail, o.Reason)}
	}

	trs, problems := lifecycleOf(rec.Transitions, k9Slot,
		[]chaos.State{chaos.StateInstall, chaos.StateActive, chaos.StateRemove, chaos.StateRecover, chaos.StateFailed})
	if len(problems) > 0 {
		return problems
	}
	for _, tr := range trs {
		if tr.Kind != o.Kind {
			return []string{fmt.Sprintf("%s's %s transition is of kind %s, its outcome of %s", k9Slot, tr.State, tr.Kind, o.Kind)}
		}
	}
	remove, recover, failed := trs[2].Time, trs[3].Time, trs[4].Time

	var adds, deletes []Event
	for _, e := range rec.Events {
		switch {
		case e.Canary != "K9":
		case e.Kind == KindAddToxic:
			adds = append(adds, e)
		case e.Kind == KindDeleteToxic:
			deletes = append(deletes, e)
		}
	}
	if len(adds) != 1 || len(deletes) != 1 {
		return []string{fmt.Sprintf("%d additions and %d deletions of the hidden toxic are recorded, want 1 and 1", len(adds), len(deletes))}
	}
	var out []string
	for _, e := range []Event{adds[0], deletes[0]} {
		switch {
		case e.Proxy != target || e.Toxic != K9Toxic:
			out = append(out, fmt.Sprintf("the %s event names %s on %s, want %s on %s", e.Kind, e.Toxic, e.Proxy, K9Toxic, target))
		case e.Err != "":
			out = append(out, fmt.Sprintf("the %s event failed: %s", e.Kind, e.Err))
		}
	}
	if add := adds[0].Time; add.Before(remove) || add.After(recover) {
		out = append(out, fmt.Sprintf("the toxic was added at %s, outside %s's remove step (%s to %s)",
			add.Format(time.RFC3339Nano), k9Slot, remove.Format(time.RFC3339Nano), recover.Format(time.RFC3339Nano)))
	}
	if del := deletes[0].Time; del.Before(failed) {
		out = append(out, fmt.Sprintf("the toxic was deleted at %s, before %s failed at %s",
			del.Format(time.RFC3339Nano), k9Slot, failed.Format(time.RFC3339Nano)))
	}
	return out
}

// Install installs the inner fault.
//
// Parameters:
//   - ctx: the install bound
//
// Returns:
//   - error: the inner fault's
func (h *hiddenToxic) Install(ctx context.Context) error { return h.inner.Install(ctx) }

// Hold holds the inner fault and records whether it held.
//
// Parameters:
//   - ctx: the lifecycle's outer bound
//   - d: the active duration
//
// Returns:
//   - error: the inner fault's
func (h *hiddenToxic) Hold(ctx context.Context, d time.Duration) error {
	err := h.inner.Hold(ctx, d)
	h.held = err == nil
	return err
}

// Remove runs the inner removal; when that succeeded in the remove step, it adds the hidden toxic and records it.
// A failed addition is recorded and not returned:
// the fixture is sound, and the judgment rejects the attempt.
//
// Parameters:
//   - ctx: the remove bound
//
// Returns:
//   - error: the inner removal's
func (h *hiddenToxic) Remove(ctx context.Context) error {
	if err := h.inner.Remove(ctx); err != nil {
		return err
	}
	if !h.held || h.added {
		return nil
	}
	h.added = true
	err := h.d.Ops.AddToxic(ctx, h.proxy, K9HiddenToxic())
	h.event(KindAddToxic, err)
	return nil
}

// Release deletes the hidden toxic once, counting a 404 as removed, checks the listing and records the deletion;
// then it releases the inner fault.
func (h *hiddenToxic) Release() {
	if h.added && !h.released {
		h.released = true
		h.event(KindDeleteToxic, h.delete())
	}
	if r, ok := h.inner.(chaos.Releaser); ok {
		r.Release()
	}
}

// delete removes the hidden toxic and checks that the proxy no longer lists it.
func (h *hiddenToxic) delete() error {
	ctx, cancel := context.WithTimeout(context.Background(), k9ReleaseBound)
	defer cancel()
	if err := h.d.Ops.RemoveToxic(ctx, h.proxy, K9Toxic); err != nil && !errors.Is(err, proxy.ErrNotFound) {
		return fmt.Errorf("delete %s: %w", K9Toxic, err)
	}
	names, err := h.d.Ops.Toxics(ctx, h.proxy)
	switch {
	case err != nil:
		return fmt.Errorf("list toxics: %w", err)
	case slices.Contains(names, K9Toxic):
		return fmt.Errorf("toxic %s still present on %s", K9Toxic, h.proxy)
	}
	return nil
}

// event records an addition or a deletion, with its error if it failed.
func (h *hiddenToxic) event(kind string, err error) {
	e := Event{Canary: "K9", Kind: kind, Time: h.d.Now(), Proxy: h.proxy, Toxic: K9Toxic}
	if err != nil {
		e.Err = err.Error()
	}
	h.d.Event(e)
}
