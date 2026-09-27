package workload

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
)

// Runner drives one session's share of the load: an open-loop scheduler and W workers.
type Runner struct {
	stopSched context.CancelFunc
	schedDone chan struct{}
	offers    chan Offer
	wg        sync.WaitGroup
	dropped   atomic.Int64
	stopOnce  sync.Once
}

// Start starts the scheduler and the workers.
//
// Operations run on opCtx, which the caller cancels only for a hard stop (watchdog, shutdown);
// Stop itself never cancels an active operation, it stops offering and waits.
//
// Parameters:
//   - opCtx: the context operations derive from
//   - env: the session's environment
//   - rate: offered operations per second
//   - workers: W
//   - mix: the operation mix
//   - seed: seeds the class sequence and each worker's generator
//
// Returns:
//   - *Runner: the running load
//   - error: when the mix is invalid
func Start(opCtx context.Context, env *Env, rate float64, workers int, mix Mix, seed uint64) (*Runner, error) {
	ch, err := NewChooser(mix, seed)
	if err != nil {
		return nil, err
	}
	r := &Runner{offers: make(chan Offer), schedDone: make(chan struct{})}
	schedCtx, cancel := context.WithCancel(context.Background())
	r.stopSched = cancel
	for w := range workers {
		rng := rand.New(rand.NewPCG(seed, uint64(w)+1))
		r.wg.Go(func() {
			for o := range r.offers {
				env.Do(opCtx, w, rng, o)
			}
		})
	}
	go func() {
		defer close(r.schedDone)
		RunScheduler(schedCtx, rate, ch, r.offers, env.Progress, func(Offer) { r.dropped.Add(1) })
	}()
	return r, nil
}

// Stop stops offering, then waits for every worker to finish its current operation.
// Each operation is bounded by its own deadline, so Stop is bounded too.
func (r *Runner) Stop() {
	r.stopOnce.Do(func() {
		r.stopSched()
		<-r.schedDone
		close(r.offers)
		r.wg.Wait()
	})
}

// Dropped returns the offers no worker could take.
//
// Returns:
//   - int64: the count
func (r *Runner) Dropped() int64 { return r.dropped.Load() }
