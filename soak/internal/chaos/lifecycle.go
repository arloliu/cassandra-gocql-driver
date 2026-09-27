package chaos

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
)

// StateInstall and the other State values are a fault's lifecycle states (PLAN §5.2).
const (
	StateInstall State = "install"
	StateActive  State = "active"
	StateRemove  State = "remove"
	StateRecover State = "recover"
	StateDone    State = "done"
	StateFailed  State = "failed"
)

// FailFixture and FailG9 classify a failed fault.
const (
	// FailFixture is an install or remove step that failed or overran: the cell aborts as fixture-invalid.
	FailFixture FailClass = "fixture"
	// FailG9 is a recovery that did not happen in time, or an overrun outer deadline.
	FailG9 FailClass = "G9"
)

// Recovery sampling (PLAN §5.3).
const (
	// SampleInterval is the gap between recovery samples.
	SampleInterval = 5 * time.Second
	// Consecutive is how many consecutive samples must hold.
	Consecutive = 3
	// windowTail extends a done fault's window (PLAN §6.1).
	windowTail = 10 * time.Second
	// WindowTail is windowTail, for the calibration loader: a done window ends exactly this long after its fault (PLAN §41.2).
	WindowTail = windowTail
)

// State is a lifecycle state.
type State string

// FailClass says why a fault failed.
type FailClass string

// Fault is one injectable fault.
type Fault interface {
	// Install injects the fault and checks its effect; it must honour ctx.
	Install(ctx context.Context) error
	// Hold keeps the fault active for d; most faults sleep, F-ddl runs its statements.
	Hold(ctx context.Context, d time.Duration) error
	// Remove undoes the fault and checks the removal; it must honour ctx.
	Remove(ctx context.Context) error
}

// Recovery evaluates R1–R3 once.
type Recovery interface {
	// Sample returns the violations of R1–R3 now; empty means all hold.
	Sample(ctx context.Context) []string
}

// Timing is a lifecycle's bounds; production uses a Spec and the §5.3 sampling.
type Timing struct {
	// Spec bounds each step.
	Spec Spec
	// Active is the planned active duration.
	Active time.Duration
	// Interval and Consecutive shape the recovery condition.
	Interval    time.Duration
	Consecutive int
	// Slack is added to the outer deadline; production uses OuterSlack.
	Slack time.Duration
}

// Transition is one lifecycle step, for events.jsonl.
type Transition struct {
	// Time is when the fault entered State.
	Time time.Time `json:"time"`
	// ID names the fault instance, e.g. "M3" or "L/2".
	ID string `json:"id"`
	// Kind is the fault kind.
	Kind Kind `json:"kind"`
	// State is the state entered.
	State State `json:"state"`
	// Detail explains a failure.
	Detail string `json:"detail,omitempty"`
}

// Outcome is how one fault's lifecycle ended.
type Outcome struct {
	// ID and Kind identify the fault.
	ID   string `json:"id"`
	Kind Kind   `json:"kind"`
	// Final is StateDone or StateFailed.
	Final State `json:"final"`
	// Fail classifies a failure.
	Fail FailClass `json:"fail,omitempty"`
	// Reason explains a failure.
	Reason string `json:"reason,omitempty"`
	// Start is the install start; Recovered the first instant of the holding streak; End when it ended.
	Start     time.Time `json:"start"`
	Recovered time.Time `json:"recovered,omitzero"`
	End       time.Time `json:"end"`
	// LastProblems are the recovery violations at the last sample of a failed recovery.
	LastProblems []string `json:"last_problems,omitempty"`
}

// Windows tracks fault windows for G8: [install start, done + 10 s], or open to the end for a failed fault.
type Windows struct {
	mu   sync.Mutex
	list []Window
}

// Window is one fault window.
type Window struct {
	// ID and Kind identify the fault.
	ID   string `json:"id"`
	Kind Kind   `json:"kind"`
	// Width is how many nodes the fault takes down.
	Width int `json:"width"`
	// Start opens the window; End closes it, zero while open.
	Start time.Time `json:"start"`
	End   time.Time `json:"end,omitzero"`
	// Failed marks a failed window, which stays open to the end of the cell.
	Failed bool `json:"failed"`
}

// RunLifecycle drives one fault through install, active, remove and recover.
//
// The outer deadline is fixed before install: install + active + remove + recovery + slack (30 s).
// Overrunning it fails G9 whichever step is stuck.
// A failing or overrunning install or remove step is a fixture failure;
// after a failed install the removal is still attempted once, so a half-installed fault is undone.
//
// Parameters:
//   - ctx: the cell context
//   - id: names the fault instance
//   - kind: the fault kind
//   - f: the fault
//   - rec: the recovery condition
//   - t: the bounds
//   - windows: receives the fault's window
//   - width: nodes the fault takes down
//   - onTransition: receives every transition; may be nil
//
// Returns:
//   - Outcome: done or failed, with timings
func RunLifecycle(ctx context.Context, id string, kind Kind, f Fault, rec Recovery, t Timing,
	windows *Windows, width int, onTransition func(Transition)) Outcome {
	out := Outcome{ID: id, Kind: kind, Start: time.Now()}
	emit := func(s State, detail string) {
		if onTransition != nil {
			onTransition(Transition{Time: time.Now(), ID: id, Kind: kind, State: s, Detail: detail})
		}
	}
	outer := t.Spec.Install + t.Active + t.Spec.Remove + t.Spec.Recovery + t.Slack
	octx, cancel := context.WithTimeout(ctx, outer)
	defer cancel()
	windows.Open(id, kind, width, out.Start)

	fail := func(class FailClass, reason string) Outcome {
		out.Final, out.Fail, out.Reason, out.End = StateFailed, class, reason, time.Now()
		windows.Fail(id)
		emit(StateFailed, fmt.Sprintf("%s: %s", class, reason))
		return out
	}
	step := func(bound time.Duration, fn func(context.Context) error) (FailClass, error) {
		sctx, scancel := context.WithTimeout(octx, bound)
		defer scancel()
		err := fn(sctx)
		switch {
		case err == nil:
			return "", nil
		case octx.Err() != nil:
			return FailG9, fmt.Errorf("outer deadline %s: %w", outer, err)
		default:
			return FailFixture, err
		}
	}

	emit(StateInstall, "")
	if class, err := step(t.Spec.Install, f.Install); err != nil {
		_, rerr := step(t.Spec.Remove, f.Remove)
		return fail(class, fmt.Sprintf("install: %v (undo: %v)", err, rerr))
	}
	emit(StateActive, "")
	if err := f.Hold(octx, t.Active); err != nil {
		if octx.Err() != nil {
			return fail(FailG9, fmt.Sprintf("active: outer deadline %s: %v", outer, err))
		}
		_, rerr := step(t.Spec.Remove, f.Remove)
		return fail(FailFixture, fmt.Sprintf("active: %v (undo: %v)", err, rerr))
	}
	emit(StateRemove, "")
	if class, err := step(t.Spec.Remove, f.Remove); err != nil {
		return fail(class, fmt.Sprintf("remove: %v", err))
	}
	emit(StateRecover, "")
	recovered, last, err := awaitRecovery(octx, rec, t)
	if err != nil {
		out.LastProblems = last
		return fail(FailG9, fmt.Sprintf("recovery: %v", err))
	}
	out.Final, out.Recovered, out.End = StateDone, recovered, time.Now()
	windows.Close(id, out.End)
	emit(StateDone, "")
	return out
}

// awaitRecovery samples until Consecutive samples in a row hold, within the recovery budget.
func awaitRecovery(ctx context.Context, rec Recovery, t Timing) (time.Time, []string, error) {
	rctx, cancel := context.WithTimeout(ctx, t.Spec.Recovery)
	defer cancel()
	streak := 0
	var streakStart time.Time
	var last []string
	for {
		now := time.Now()
		last = rec.Sample(rctx)
		if len(last) == 0 {
			if streak == 0 {
				streakStart = now
			}
			streak++
			if streak >= t.Consecutive {
				return streakStart, nil, nil
			}
		} else {
			streak = 0
		}
		select {
		case <-rctx.Done():
			return time.Time{}, last, fmt.Errorf("not recovered within %s: %w", t.Spec.Recovery, rctx.Err())
		case <-time.After(t.Interval):
		}
	}
}

// Sleep is the Hold of a fault that only waits.
//
// Parameters:
//   - ctx: bounds the wait
//   - d: how long to hold
//
// Returns:
//   - error: ctx's error when it ends first
func Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Open starts a window.
//
// Parameters:
//   - id, kind: the fault
//   - width: nodes it takes down
//   - start: the install start
func (w *Windows) Open(id string, kind Kind, width int, start time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.list = append(w.list, Window{ID: id, Kind: kind, Width: width, Start: start})
}

// Close ends a done fault's window 10 s after done.
//
// Parameters:
//   - id: the fault
//   - done: when it reached done
func (w *Windows) Close(id string, done time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := range w.list {
		if w.list[i].ID == id && w.list[i].End.IsZero() && !w.list[i].Failed {
			w.list[i].End = done.Add(windowTail)
		}
	}
}

// Fail marks a window failed: it stays open to the end of the cell.
//
// Parameters:
//   - id: the fault
func (w *Windows) Fail(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := range w.list {
		if w.list[i].ID == id {
			w.list[i].Failed = true
			w.list[i].End = time.Time{}
		}
	}
}

// At returns the window covering t, if any; the widest one when several overlap.
//
// Parameters:
//   - t: the instant
//
// Returns:
//   - Window: the window
//   - bool: false when t is outside every window
func (w *Windows) At(t time.Time) (Window, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var best Window
	found := false
	for _, win := range w.list {
		if t.Before(win.Start) || (!win.End.IsZero() && t.After(win.End)) {
			continue
		}
		if !found || win.Width > best.Width {
			best, found = win, true
		}
	}
	return best, found
}

// Intervals returns every window as seconds since epoch, open ones ending at end, for G16's exclusion.
//
// Parameters:
//   - epoch: the cell epoch
//   - end: the end of the cell
//
// Returns:
//   - []gate.Interval: one per window
func (w *Windows) Intervals(epoch, end time.Time) []gate.Interval {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]gate.Interval, 0, len(w.list))
	for _, win := range w.list {
		stop := win.End
		if stop.IsZero() {
			stop = end
		}
		out = append(out, gate.Interval{Start: win.Start.Sub(epoch).Seconds(), End: stop.Sub(epoch).Seconds()})
	}
	return out
}

// All returns a copy of every window.
//
// Returns:
//   - []Window: the windows in opening order
func (w *Windows) All() []Window {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]Window, len(w.list))
	copy(out, w.list)
	return out
}
