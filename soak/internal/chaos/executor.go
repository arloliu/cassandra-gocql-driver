package chaos

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrChurnOverrun is returned by a Churn hook whose work is still unresolved at the slot deadline;
// the executor treats it as an overrun whatever the clock says (PLAN §5.4).
var ErrChurnOverrun = errors.New("chaos: churn work outlived its slot deadline")

// Executor runs a schedule's slots in order (PLAN §5.4).
type Executor struct {
	// Schedule is the seeded plan.
	Schedule Schedule
	// Specs is the fault catalog.
	Specs map[Kind]Spec
	// Start is the workload start; slot offsets are relative to it.
	Start time.Time
	// Build makes a runnable fault from a planned one; a composite (F-roll) is built per sub-fault,
	// with p.Kind FaultStop and a single target.
	Build func(p Planned) (Fault, error)
	// Recovery is the R1–R3 condition.
	Recovery Recovery
	// Churn runs one churn slot and must return by ctx's deadline; an error fails G13.
	Churn func(ctx context.Context, slot Slot) error
	// Windows receives every fault window.
	Windows *Windows
	// OnTransition receives every lifecycle transition; may be nil.
	OnTransition func(Transition)
	// ChurnDeadline overrides the churn slot deadline; zero means ChurnSlotDeadline.
	ChurnDeadline time.Duration
	// Interval and Consecutive override the §5.3 recovery sampling; zero means the defaults.
	Interval    time.Duration
	Consecutive int
	// Now is the clock; nil means time.Now.
	Now func() time.Time
	// OptionalGap overrides the 60 s gap before an optional fault, and Slack the 30 s outer-deadline slack;
	// zero keeps them.
	// Only tests set them, to run the timetable in milliseconds.
	OptionalGap time.Duration
	Slack       time.Duration
}

// Report is what an executor did.
type Report struct {
	// Outcomes holds every fault run, mandatory and optional, sub-faults of a composite included.
	Outcomes []Outcome `json:"outcomes"`
	// MandatoryFaults and MandatoryExecuted count the assignment and the mandatory faults that ran (G15).
	MandatoryFaults   int `json:"mandatory_faults"`
	MandatoryExecuted int `json:"mandatory_executed"`
	// ChurnSlots and ChurnExecuted count the churn reservations and those that ran.
	ChurnSlots    int `json:"churn_slots"`
	ChurnExecuted int `json:"churn_executed"`
	// ChurnFailures lists churn slots that failed or overran (G13).
	ChurnFailures []string `json:"churn_failures,omitempty"`
	// G9Failures lists faults that failed G9.
	G9Failures []string `json:"g9_failures,omitempty"`
	// FaultsStopped is set when a failure stopped further fault injection, with the reason.
	FaultsStopped string `json:"faults_stopped,omitempty"`
	// Fixture is set when a fixture failure aborted the cell.
	Fixture string `json:"fixture,omitempty"`
}

// Run walks the timetable until it ends, ctx ends, or a fixture failure aborts the cell.
//
// A mandatory fault starts at its slot's start and ends inside the slot by construction.
// After it is done, optional faults are admitted from the seeded sequence while they fit.
// After any fault failure or churn overrun, no further fault is injected; churn slots still run.
//
// Parameters:
//   - ctx: the cell context
//
// Returns:
//   - Report: what ran
func (e *Executor) Run(ctx context.Context) Report {
	var rep Report
	for _, sl := range e.Schedule.Slots {
		if sl.Kind == SlotChurn {
			rep.ChurnSlots++
		} else {
			rep.MandatoryFaults++
		}
	}
	mandatory := map[string]Planned{}
	for _, p := range e.Schedule.Mandatory {
		mandatory[p.Slot] = p
	}
	optional := e.Schedule.Optional
	for _, sl := range e.Schedule.Slots {
		if !e.waitUntil(ctx, sl.Start) {
			return rep
		}
		if sl.Kind == SlotChurn {
			e.runChurn(ctx, sl, &rep)
			continue
		}
		if rep.FaultsStopped != "" {
			continue
		}
		p, ok := mandatory[sl.ID]
		if !ok {
			rep.Fixture = fmt.Sprintf("slot %s has no planned fault", sl.ID)
			return rep
		}
		rep.MandatoryExecuted++
		if !e.runPlanned(ctx, sl.ID, p, &rep) {
			return rep
		}
		for rep.FaultsStopped == "" && len(optional) > 0 && e.admit(sl, optional[0]) {
			if !e.waitUntil(ctx, e.now().Sub(e.Start)+e.gap()) {
				return rep
			}
			p := optional[0]
			optional = optional[1:]
			if !e.runPlanned(ctx, sl.ID+"+opt", p, &rep) {
				return rep
			}
		}
	}
	return rep
}

// runPlanned runs one planned fault, a composite as its sub-faults in turn.
// It returns false when a fixture failure aborts the cell.
func (e *Executor) runPlanned(ctx context.Context, id string, p Planned, rep *Report) bool {
	sp := e.Specs[p.Kind]
	if sp.SubFaults > 0 {
		for i, target := range p.Targets {
			sub := Planned{Slot: p.Slot, Kind: FaultStop, Targets: []string{target}, Active: p.SubActive[i], Mandatory: p.Mandatory}
			if !e.runOne(ctx, fmt.Sprintf("%s/%d", id, i+1), sub, sp.Width, rep) || rep.FaultsStopped != "" {
				return rep.Fixture == ""
			}
		}
		return true
	}
	return e.runOne(ctx, id, p, sp.Width, rep)
}

func (e *Executor) runOne(ctx context.Context, id string, p Planned, width int, rep *Report) bool {
	f, err := e.Build(p)
	if err != nil {
		rep.Fixture = fmt.Sprintf("%s: build %s: %v", id, p.Kind, err)
		return false
	}
	t := Timing{Spec: e.Specs[p.Kind], Active: p.Active, Interval: e.Interval, Consecutive: e.Consecutive, Slack: e.slack()}
	if t.Interval == 0 {
		t.Interval = SampleInterval
	}
	if t.Consecutive == 0 {
		t.Consecutive = Consecutive
	}
	out := RunLifecycle(ctx, id, p.Kind, f, e.Recovery, t, e.Windows, width, e.OnTransition)
	rep.Outcomes = append(rep.Outcomes, out)
	switch out.Fail {
	case FailFixture:
		rep.Fixture = fmt.Sprintf("%s %s: %s", id, p.Kind, out.Reason)
		return false
	case FailG9:
		rep.G9Failures = append(rep.G9Failures, fmt.Sprintf("%s %s: %s", id, p.Kind, out.Reason))
		rep.FaultsStopped = fmt.Sprintf("%s failed G9", id)
	}
	return true
}

func (e *Executor) runChurn(ctx context.Context, sl Slot, rep *Report) {
	rep.ChurnExecuted++
	deadline := e.ChurnDeadline
	if deadline == 0 {
		deadline = ChurnSlotDeadline
	}
	cctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	start := e.now()
	err := e.Churn(cctx, sl)
	overran := e.now().Sub(start) > deadline || errors.Is(err, ErrChurnOverrun)
	switch {
	case overran:
		rep.ChurnFailures = append(rep.ChurnFailures, fmt.Sprintf("%s overran its %s deadline (err %v)", sl.ID, deadline, err))
		if rep.FaultsStopped == "" {
			rep.FaultsStopped = fmt.Sprintf("churn %s overran", sl.ID)
		}
	case err != nil:
		rep.ChurnFailures = append(rep.ChurnFailures, fmt.Sprintf("%s: %v", sl.ID, err))
	}
}

func (e *Executor) gap() time.Duration {
	if e.OptionalGap > 0 {
		return e.OptionalGap
	}
	return optionalGap
}

func (e *Executor) slack() time.Duration {
	if e.Slack > 0 {
		return e.Slack
	}
	return OuterSlack
}

// admit applies AdmitOptional with the executor's gap and slack.
func (e *Executor) admit(sl Slot, p Planned) bool {
	return e.now().Sub(e.Start)+e.gap()+outerDeadline(e.Specs, p.Kind, e.Specs[p.Kind].ActiveMax, e.slack()) <= sl.End
}

// waitUntil sleeps until offset past Start; false when ctx ends first.
func (e *Executor) waitUntil(ctx context.Context, offset time.Duration) bool {
	d := e.Start.Add(offset).Sub(e.now())
	if d <= 0 {
		return ctx.Err() == nil
	}
	return Sleep(ctx, d) == nil
}

func (e *Executor) now() time.Time {
	if e.Now != nil {
		return e.Now()
	}
	return time.Now()
}
