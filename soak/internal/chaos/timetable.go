// Package chaos schedules and runs the soak's faults (PLAN §5):
// a fixed timetable of reserved slots, a bounded lifecycle per fault, and the fault windows G8 reads.
package chaos

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"time"
)

// FaultStop and the other Kind values are the fault catalog (PLAN §5.1).
const (
	FaultStop     Kind = "F-stop"
	FaultKill     Kind = "F-kill"
	FaultPause    Kind = "F-pause"
	FaultRoll     Kind = "F-roll"
	FaultFull     Kind = "F-full"
	FaultTwo      Kind = "F-two"
	FaultDDL      Kind = "F-ddl"
	FaultTopo     Kind = "F-topo"
	FaultLat      Kind = "F-lat"
	FaultBW       Kind = "F-bw"
	FaultBlack    Kind = "F-black"
	FaultHalfOpen Kind = "F-halfopen"
	FaultReset    Kind = "F-reset"
)

// SlotFault and the other SlotKind values are what a reserved slot holds.
const (
	// SlotDDL is M0, always F-ddl.
	SlotDDL SlotKind = "ddl"
	// SlotShort is a mandatory short fault, M1–M6.
	SlotShort SlotKind = "short"
	// SlotLong is the mandatory long fault, L.
	SlotLong SlotKind = "long"
	// SlotChurn is a churn reservation.
	SlotChurn SlotKind = "churn"
)

// Timetable bounds.
const (
	// NightChaosEnd is when the night timetable must end: minute 105 of the workload.
	NightChaosEnd = 105 * time.Minute
	// ValidationChaosEnd is when the validation timetable must end: minute 40.
	ValidationChaosEnd = 40 * time.Minute
	// ChurnSlotDeadline is a churn slot's whole-slot deadline.
	ChurnSlotDeadline = 150 * time.Second
	// OuterSlack is the slack in every outer deadline (PLAN §5.2).
	OuterSlack = 30 * time.Second
	// optionalGap is the gap an optional fault needs before it (PLAN §5.4).
	optionalGap = 60 * time.Second
)

// ChurnBudget is the step budgets of a churn slot (PLAN §5.4); they sum to ChurnSlotDeadline.
var ChurnBudget = struct {
	Create, Load, Close, Grace, Residue, Margin time.Duration
}{20 * time.Second, 30 * time.Second, 30 * time.Second, 30 * time.Second, 20 * time.Second, 20 * time.Second}

// Kind is a fault kind.
type Kind string

// Spec is a fault kind's bounds (PLAN §5.1, §5.2).
type Spec struct {
	// Install bounds the install step, effect check included.
	Install time.Duration
	// ActiveMin and ActiveMax bound the active duration; the seed picks one in between.
	ActiveMin, ActiveMax time.Duration
	// Remove bounds the remove step, removal check included.
	Remove time.Duration
	// Recovery is the recovery budget.
	Recovery time.Duration
	// Width is how many nodes the fault takes down, for G8's unavailable evidence.
	Width int
	// SubFaults, when positive, makes the kind a composite of that many F-stop sub-faults (F-roll).
	SubFaults int
}

// SlotKind is what a reserved slot holds.
type SlotKind string

// Slot is one reserved slot of the timetable.
type Slot struct {
	// ID is the slot name, e.g. "M1", "C3", "L".
	ID string `json:"id"`
	// Kind is what the slot holds.
	Kind SlotKind `json:"kind"`
	// Start and End bound the slot, as offsets from the workload start.
	Start time.Duration `json:"start"`
	End   time.Duration `json:"end"`
}

// Assignment is the mandatory faults of a cell (PLAN §5.4 table).
type Assignment struct {
	// Short is the M1–M6 set, before the seed orders it.
	Short []Kind `json:"short"`
	// Long is the L fault.
	Long Kind `json:"long"`
}

// Planned is one fault placed in a slot, with its seeded parameters.
type Planned struct {
	// Slot is the slot id.
	Slot string `json:"slot"`
	// Kind is the fault kind.
	Kind Kind `json:"kind"`
	// Targets are the node names it acts on.
	Targets []string `json:"targets"`
	// Active is the seeded active duration.
	Active time.Duration `json:"active"`
	// SubActive is each sub-fault's active duration, for a composite (F-roll).
	SubActive []time.Duration `json:"sub_active,omitempty"`
	// Mandatory is false for an optional fault.
	Mandatory bool `json:"mandatory"`
}

// Schedule is the seeded plan of a cell, written to schedule.json.
type Schedule struct {
	// Seed is the cell seed.
	Seed uint64 `json:"seed"`
	// Slots is the timetable.
	Slots []Slot `json:"slots"`
	// Mandatory is one fault per fault slot, in slot order.
	Mandatory []Planned `json:"mandatory"`
	// Optional is the sequence optional faults are admitted from, in order.
	Optional []Planned `json:"optional"`
}

// Specs returns the bounds of every fault kind (PLAN §5.1, §5.2).
//
// Returns:
//   - map[Kind]Spec: the catalog
func Specs() map[Kind]Spec {
	m := time.Minute
	s := time.Second
	// A stop fault's remove step starts the node again, which gets the longer node-start bound (PLAN §5.2, v7.4).
	start := 90 * s
	return map[Kind]Spec{
		FaultStop:     {Install: m, ActiveMin: 30 * s, ActiveMax: 120 * s, Remove: start, Recovery: 90 * s, Width: 1},
		FaultKill:     {Install: m, ActiveMin: 30 * s, ActiveMax: 120 * s, Remove: start, Recovery: 90 * s, Width: 1},
		FaultPause:    {Install: m, ActiveMin: 10 * s, ActiveMax: 90 * s, Remove: m, Recovery: 120 * s, Width: 1},
		FaultRoll:     {SubFaults: 3, Width: 1},
		FaultFull:     {Install: m, ActiveMin: 30 * s, ActiveMax: 60 * s, Remove: start, Recovery: 180 * s, Width: 3},
		FaultTwo:      {Install: m, ActiveMin: 30 * s, ActiveMax: 90 * s, Remove: start, Recovery: 180 * s, Width: 2},
		FaultDDL:      {Install: 30 * s, ActiveMin: 60 * s, ActiveMax: 60 * s, Remove: 30 * s, Recovery: 60 * s},
		FaultTopo:     {Install: 8 * m, Remove: 8 * m, Recovery: 90 * s},
		FaultLat:      {Install: m, ActiveMin: 30 * s, ActiveMax: 120 * s, Remove: m, Recovery: 90 * s, Width: 1},
		FaultBW:       {Install: m, ActiveMin: 30 * s, ActiveMax: 120 * s, Remove: m, Recovery: 90 * s, Width: 1},
		FaultBlack:    {Install: m, ActiveMin: 30 * s, ActiveMax: 120 * s, Remove: m, Recovery: 120 * s, Width: 1},
		FaultHalfOpen: {Install: m, ActiveMin: 30 * s, ActiveMax: 90 * s, Remove: m, Recovery: 120 * s, Width: 1},
		FaultReset:    {Install: m, Remove: m, Recovery: 90 * s, Width: 1},
	}
}

// OuterDeadline returns a spec's worst-case outer deadline:
// install + active + remove + recovery + 30 s, or for a composite the sum of its F-stop sub-faults.
//
// Parameters:
//   - specs: the catalog
//   - k: the kind
//   - active: the active duration; ActiveMax gives the worst case
//
// Returns:
//   - time.Duration: the outer deadline
func OuterDeadline(specs map[Kind]Spec, k Kind, active time.Duration) time.Duration {
	return outerDeadline(specs, k, active, OuterSlack)
}

func outerDeadline(specs map[Kind]Spec, k Kind, active, slack time.Duration) time.Duration {
	sp := specs[k]
	if sp.SubFaults > 0 {
		stop := specs[FaultStop]
		return time.Duration(sp.SubFaults) * outerDeadline(specs, FaultStop, stop.ActiveMax, slack)
	}
	return sp.Install + active + sp.Remove + sp.Recovery + slack
}

// NightTimetable returns the §5.4 timetable.
//
// Returns:
//   - []Slot: M0, then churn and fault slots alternating, the long slot, ending at minute 102.5
func NightTimetable() []Slot {
	minutes := func(x float64) time.Duration { return time.Duration(x * float64(time.Minute)) }
	return []Slot{
		{"M0", SlotDDL, minutes(15), minutes(20)},
		{"C1", SlotChurn, minutes(20), minutes(22.5)},
		{"M1", SlotShort, minutes(22.5), minutes(30)},
		{"C2", SlotChurn, minutes(30), minutes(32.5)},
		{"M2", SlotShort, minutes(32.5), minutes(40)},
		{"C3", SlotChurn, minutes(40), minutes(42.5)},
		{"M3", SlotShort, minutes(42.5), minutes(50)},
		{"C4", SlotChurn, minutes(50), minutes(52.5)},
		{"M4", SlotShort, minutes(52.5), minutes(60)},
		{"C5", SlotChurn, minutes(60), minutes(62.5)},
		{"L", SlotLong, minutes(62.5), minutes(82.5)},
		{"M5", SlotShort, minutes(82.5), minutes(90)},
		{"C6", SlotChurn, minutes(90), minutes(92.5)},
		{"M6", SlotShort, minutes(92.5), minutes(100)},
		{"C7", SlotChurn, minutes(100), minutes(102.5)},
	}
}

// ValidationTimetable returns the §7 validation timetable: three churns and two fixed faults.
//
// Returns:
//   - []Slot: churns at 15, 25 and 35 min, F-stop at 17.5 and F-pause at 27.5 in 7.5 min slots
func ValidationTimetable() []Slot {
	minutes := func(x float64) time.Duration { return time.Duration(x * float64(time.Minute)) }
	return []Slot{
		{"C1", SlotChurn, minutes(15), minutes(17.5)},
		{"V1", SlotShort, minutes(17.5), minutes(25)},
		{"C2", SlotChurn, minutes(25), minutes(27.5)},
		{"V2", SlotShort, minutes(27.5), minutes(35)},
		{"C3", SlotChurn, minutes(35), minutes(37.5)},
	}
}

// PhaseOneOptional returns the kinds a phase-1 night draws optional faults from: the short kinds, F-ddl excluded (M0 only).
//
// Returns:
//   - []Kind: F-stop, F-kill, F-pause, F-two, F-full
func PhaseOneOptional() []Kind {
	return []Kind{FaultStop, FaultKill, FaultPause, FaultTwo, FaultFull}
}

// PhaseOneAssignment returns the phase-1 assignment, the same for every cell.
//
// Returns:
//   - Assignment: M1–M6 {F-kill, F-pause, F-stop, F-two, F-full, F-kill}, L F-roll
func PhaseOneAssignment() Assignment {
	return Assignment{Short: []Kind{FaultKill, FaultPause, FaultStop, FaultTwo, FaultFull, FaultKill}, Long: FaultRoll}
}

// Plan seeds a cell's schedule: the M1–M6 order, every fault's targets and active duration,
// and the optional sequence (shuffled rounds over the enabled short kinds).
//
// Parameters:
//   - seed: the cell seed
//   - slots: the timetable
//   - a: the mandatory assignment; for the validation timetable, Short holds the two fixed faults in order
//   - shuffle: whether to shuffle Short (night) or keep its order (validation)
//   - nodes: the node names
//   - optionalKinds: the kinds optional faults are drawn from; empty disables them
//   - specs: the catalog
//
// Returns:
//   - Schedule: the plan
//   - error: when the slots and the assignment do not match
func Plan(seed uint64, slots []Slot, a Assignment, shuffle bool, nodes []string, optionalKinds []Kind, specs map[Kind]Spec) (Schedule, error) {
	rng := rand.New(rand.NewPCG(seed, seed^0xc4a05))
	short := slices.Clone(a.Short)
	if shuffle {
		rng.Shuffle(len(short), func(i, j int) { short[i], short[j] = short[j], short[i] })
	}
	sch := Schedule{Seed: seed, Slots: slots}
	next := 0
	for _, sl := range slots {
		var k Kind
		switch sl.Kind {
		case SlotDDL:
			k = FaultDDL
		case SlotLong:
			k = a.Long
		case SlotShort:
			if next >= len(short) {
				return Schedule{}, fmt.Errorf("plan: slot %s has no assigned fault", sl.ID)
			}
			k = short[next]
			next++
		default:
			continue
		}
		sch.Mandatory = append(sch.Mandatory, plan(rng, sl.ID, k, nodes, specs, true))
	}
	if next != len(short) {
		return Schedule{}, fmt.Errorf("plan: %d short faults assigned, %d short slots", len(short), next)
	}
	// Enough optional faults for any leftover time: one per minute of timetable is more than can fit.
	for len(optionalKinds) > 0 && len(sch.Optional) < 2*len(slots) {
		round := slices.Clone(optionalKinds)
		rng.Shuffle(len(round), func(i, j int) { round[i], round[j] = round[j], round[i] })
		for _, k := range round {
			sch.Optional = append(sch.Optional, plan(rng, "", k, nodes, specs, false))
		}
	}
	return sch, nil
}

// Preflight verifies a schedule before the cluster is created (PLAN §5.4).
// Any violation makes the cell invalid-config.
//
// Parameters:
//   - sch: the planned schedule
//   - end: when the timetable must end
//   - specs: the catalog
//
// Returns:
//   - error: joining every violation, or nil
func Preflight(sch Schedule, end time.Duration, specs map[Kind]Spec) error {
	var errs []error
	slots := slices.Clone(sch.Slots)
	slices.SortFunc(slots, func(a, b Slot) int { return int(a.Start - b.Start) })
	for i, sl := range slots {
		if sl.End <= sl.Start {
			errs = append(errs, fmt.Errorf("slot %s is empty", sl.ID))
		}
		if i > 0 && sl.Start < slots[i-1].End {
			errs = append(errs, fmt.Errorf("slot %s overlaps %s", sl.ID, slots[i-1].ID))
		}
		if sl.End > end {
			errs = append(errs, fmt.Errorf("slot %s ends at %s, after %s", sl.ID, sl.End, end))
		}
		if sl.Kind == SlotChurn && sl.End-sl.Start < ChurnSlotDeadline {
			errs = append(errs, fmt.Errorf("churn slot %s is shorter than its %s deadline", sl.ID, ChurnSlotDeadline))
		}
	}
	b := ChurnBudget
	if sum := b.Create + b.Load + b.Close + b.Grace + b.Residue + b.Margin; sum > ChurnSlotDeadline {
		errs = append(errs, fmt.Errorf("churn budgets sum to %s > %s", sum, ChurnSlotDeadline))
	}
	byID := map[string]Slot{}
	for _, sl := range sch.Slots {
		byID[sl.ID] = sl
	}
	for _, p := range sch.Mandatory {
		sl, ok := byID[p.Slot]
		if !ok {
			errs = append(errs, fmt.Errorf("fault %s placed in unknown slot %s", p.Kind, p.Slot))
			continue
		}
		if _, known := specs[p.Kind]; !known {
			errs = append(errs, fmt.Errorf("slot %s: unknown fault kind %s", p.Slot, p.Kind))
			continue
		}
		worst := OuterDeadline(specs, p.Kind, specs[p.Kind].ActiveMax)
		if worst > sl.End-sl.Start {
			errs = append(errs, fmt.Errorf("slot %s: %s worst-case outer deadline %s exceeds the slot's %s",
				p.Slot, p.Kind, worst, sl.End-sl.Start))
		}
	}
	return errors.Join(errs...)
}

// AdmitOptional reports whether an optional fault fits in the rest of a slot:
// a 60 s gap plus its worst-case outer deadline must end by the slot's end.
//
// Parameters:
//   - now: the offset from the workload start
//   - slot: the current fault slot
//   - p: the candidate
//   - specs: the catalog
//
// Returns:
//   - bool: true when it fits
func AdmitOptional(now time.Duration, slot Slot, p Planned, specs map[Kind]Spec) bool {
	return now+optionalGap+OuterDeadline(specs, p.Kind, specs[p.Kind].ActiveMax) <= slot.End
}

func plan(rng *rand.Rand, slot string, k Kind, nodes []string, specs map[Kind]Spec, mandatory bool) Planned {
	sp := specs[k]
	p := Planned{Slot: slot, Kind: k, Mandatory: mandatory}
	if span := sp.ActiveMax - sp.ActiveMin; span > 0 {
		p.Active = sp.ActiveMin + time.Duration(rng.Int64N(int64(span)+1)).Round(time.Second)
	} else {
		p.Active = sp.ActiveMin
	}
	switch {
	case sp.SubFaults > 0:
		// A roll stops each node in turn, each for its own F-stop duration.
		p.Targets = slices.Clone(nodes)
		stop := specs[FaultStop]
		for range sp.SubFaults {
			p.SubActive = append(p.SubActive,
				stop.ActiveMin+time.Duration(rng.Int64N(int64(stop.ActiveMax-stop.ActiveMin)+1)).Round(time.Second))
		}
	case sp.Width >= len(nodes):
		p.Targets = slices.Clone(nodes)
	case sp.Width > 0:
		perm := rng.Perm(len(nodes))
		for _, i := range perm[:sp.Width] {
			p.Targets = append(p.Targets, nodes[i])
		}
		slices.Sort(p.Targets)
	}
	return p
}
