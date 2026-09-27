package workload

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"time"
)

// ClassWrite and the other Class values are the operation classes of the mix (PLAN §4.2).
const (
	ClassWrite         Class = "write"
	ClassRead          Class = "read"
	ClassBatchLogged   Class = "batch-logged"
	ClassBatchUnlogged Class = "batch-unlogged"
	ClassLWT           Class = "lwt"
	ClassScan          Class = "scan"
	ClassChurn         Class = "churn"
	ClassSpecRead      Class = "spec-read"
	ClassShortDeadline Class = "short-deadline"
	ClassLargeRead     Class = "large-read"
)

// schedulerTick is how often the open-loop scheduler releases operations.
const schedulerTick = 10 * time.Millisecond

// Class is an operation class.
type Class string

// Share is one class's weight in the mix, in percent.
type Share struct {
	// Class is the operation class.
	Class Class
	// Weight is its share of operations.
	Weight int
}

// Mix is the operation mix.
type Mix []Share

// Offer is one operation the scheduler released.
type Offer struct {
	// Seq numbers the offers of one scheduler.
	Seq uint64
	// Class is the operation class.
	Class Class
	// At is when the offer was released.
	At time.Time
}

// Chooser draws classes from a mix with a seeded generator.
type Chooser struct {
	rng     *rand.Rand
	classes []Class
	cum     []int
}

// Progress counts offered and completed operations per class per second, for G11.
type Progress struct {
	epoch     time.Time
	mu        sync.Mutex
	offered   map[Class][]float64
	completed map[Class][]float64
}

// DefaultMix returns the PLAN §4.2 mix.
//
// Returns:
//   - Mix: the ten classes, summing to 100
func DefaultMix() Mix {
	return Mix{
		{ClassWrite, 33}, {ClassRead, 28}, {ClassBatchLogged, 7}, {ClassBatchUnlogged, 4}, {ClassLWT, 3},
		{ClassScan, 7}, {ClassChurn, 6}, {ClassSpecRead, 5}, {ClassShortDeadline, 3}, {ClassLargeRead, 4},
	}
}

// NewChooser returns a chooser over a mix.
//
// Parameters:
//   - m: the mix; must be valid
//   - seed: the generator seed
//
// Returns:
//   - *Chooser: the chooser
//   - error: when the mix is invalid
func NewChooser(m Mix, seed uint64) (*Chooser, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	c := &Chooser{rng: rand.New(rand.NewPCG(seed, seed^0x5eed))}
	total := 0
	for _, s := range m {
		total += s.Weight
		c.classes = append(c.classes, s.Class)
		c.cum = append(c.cum, total)
	}
	return c, nil
}

// NewProgress returns an empty progress record.
//
// Parameters:
//   - epoch: the cell epoch
//
// Returns:
//   - *Progress: the record
func NewProgress(epoch time.Time) *Progress {
	return &Progress{epoch: epoch, offered: map[Class][]float64{}, completed: map[Class][]float64{}}
}

// RunScheduler releases operations at a constant rate until ctx ends (an open loop).
// An offer the workers cannot take at once is dropped, never queued:
// it still counts as offered, so saturation shows up in G11 instead of stretching the schedule.
//
// Parameters:
//   - ctx: stops the scheduler
//   - rate: operations per second
//   - ch: draws each offer's class
//   - out: the workers' channel
//   - progress: counts every offer
//   - dropped: called for each offer no worker took; may be nil
func RunScheduler(ctx context.Context, rate float64, ch *Chooser, out chan<- Offer, progress *Progress, dropped func(Offer)) {
	ticker := time.NewTicker(schedulerTick)
	defer ticker.Stop()
	last := time.Now()
	credit := 0.0
	var seq uint64
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			credit += rate * now.Sub(last).Seconds()
			last = now
			for ; credit >= 1; credit-- {
				seq++
				o := Offer{Seq: seq, Class: ch.Next(), At: now}
				progress.Offered(o.Class, now)
				select {
				case out <- o:
				default:
					if dropped != nil {
						dropped(o)
					}
				}
			}
		}
	}
}

// Validate checks that the weights are positive, the classes distinct, and the total 100.
//
// Returns:
//   - error: describing the first problem
func (m Mix) Validate() error {
	seen := map[Class]bool{}
	total := 0
	for _, s := range m {
		if s.Weight <= 0 {
			return fmt.Errorf("mix: class %s has weight %d", s.Class, s.Weight)
		}
		if seen[s.Class] {
			return fmt.Errorf("mix: class %s listed twice", s.Class)
		}
		seen[s.Class] = true
		total += s.Weight
	}
	if total != 100 {
		return fmt.Errorf("mix: weights sum to %d, want 100", total)
	}
	return nil
}

// Without returns the mix minus some classes, the rest rescaled to 100 by largest remainder.
// An aux session runs the mix without LWT (PLAN G13).
//
// Parameters:
//   - drop: the classes to remove
//
// Returns:
//   - Mix: the rescaled mix
//   - error: when nothing is left
func (m Mix) Without(drop ...Class) (Mix, error) {
	var kept Mix
	total := 0
	for _, s := range m {
		skip := false
		for _, d := range drop {
			skip = skip || s.Class == d
		}
		if !skip {
			kept = append(kept, s)
			total += s.Weight
		}
	}
	if total == 0 {
		return nil, errors.New("mix: no class left")
	}
	out := make(Mix, len(kept))
	type rem struct {
		i    int
		frac float64
	}
	var rems []rem
	sum := 0
	for i, s := range kept {
		exact := float64(s.Weight) * 100 / float64(total)
		out[i] = Share{Class: s.Class, Weight: int(exact)}
		sum += out[i].Weight
		rems = append(rems, rem{i, exact - float64(out[i].Weight)})
	}
	for sum < 100 {
		best := 0
		for j := range rems {
			if rems[j].frac > rems[best].frac {
				best = j
			}
		}
		out[rems[best].i].Weight++
		rems[best].frac = -1
		sum++
	}
	return out, nil
}

// Next draws a class.
//
// Returns:
//   - Class: the class
func (c *Chooser) Next() Class {
	x := c.rng.IntN(c.cum[len(c.cum)-1])
	for i, bound := range c.cum {
		if x < bound {
			return c.classes[i]
		}
	}
	return c.classes[len(c.classes)-1]
}

// Offered counts an offer.
//
// Parameters:
//   - class: its class
//   - at: when it was offered
func (p *Progress) Offered(class Class, at time.Time) { p.add(p.offered, class, at) }

// Completed counts a completed operation, successful or not.
//
// Parameters:
//   - class: its class
//   - at: when it completed
func (p *Progress) Completed(class Class, at time.Time) { p.add(p.completed, class, at) }

// Span sums offered and completed operations per class over [from, to) seconds since the epoch.
//
// Parameters:
//   - from, to: the span, in whole seconds
//
// Returns:
//   - offered, completed: class → count
func (p *Progress) Span(from, to int) (offered, completed map[string]float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	sum := func(m map[Class][]float64) map[string]float64 {
		out := map[string]float64{}
		for class, secs := range m {
			for s := max(from, 0); s < to && s < len(secs); s++ {
				out[string(class)] += secs[s]
			}
		}
		return out
	}
	return sum(p.offered), sum(p.completed)
}

// PerSecond returns copies of the per-second counts, for G11's rolling windows.
//
// Returns:
//   - offered, completed: class → count per second since the epoch
func (p *Progress) PerSecond() (offered, completed map[string][]float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := func(m map[Class][]float64) map[string][]float64 {
		out := make(map[string][]float64, len(m))
		for class, secs := range m {
			out[string(class)] = slices.Clone(secs)
		}
		return out
	}
	return cp(p.offered), cp(p.completed)
}

func (p *Progress) add(m map[Class][]float64, class Class, at time.Time) {
	sec := int(at.Sub(p.epoch).Seconds())
	if sec < 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	secs := m[class]
	if sec >= len(secs) {
		secs = append(secs, make([]float64, sec+1-len(secs))...)
	}
	secs[sec]++
	m[class] = secs
}
