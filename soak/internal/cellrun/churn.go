package cellrun

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cell"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/proxy"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/view"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// Churner runs the churn slots of a cell (PLAN §5.4, G13).
type Churner struct {
	// Spec is the aux session template; ID, Generation and Registry are set per churn.
	Spec cell.SessionSpec
	// Sampler receives each aux session while it lives.
	Sampler *Sampler
	// Recorder receives the aux session's errors and the churn events.
	Recorder *Recorder
	// Ledger, Settlement and OpIDs are shared with the primary.
	Ledger     *workload.Ledger
	Settlement *workload.Settlement
	OpIDs      *atomic.Uint64
	// Violations receives G10 violations seen while the aux load runs.
	Violations func(string)
	// Rate and Workers are the primary's; the aux load is a share of them.
	Rate    float64
	Workers int
	// Seed seeds the aux load.
	Seed uint64
	// Registry attributes the aux session's sockets.
	Registry *view.Registry
	// FDDir and NetDir are /proc/self/fd and /proc/net in production.
	FDDir, NetDir string
	// Create builds the driver session from its config; nil means cfg.CreateSession.
	Create func(*gocql.ClusterConfig) (*gocql.Session, error)
	// CloseSession closes an aux session; nil means Session.Close.
	CloseSession func(*cell.Session)
	// Measure takes an aux session's residue after its grace period; nil means the /proc-based probe.
	Measure func(id string) (measured bool, live, sockets int, groups map[string]int, err error)
	// AfterClose is called with an aux session's id once its Close returned (the K10 canary); may be nil.
	AfterClose func(id string)
	// Switches are the configuration canaries' Env flags, set on every aux Env too (K15, K17).
	Switches workload.Switches
	// StartLoad starts the aux load; nil means workload.Start.
	StartLoad func(ctx context.Context, env *workload.Env, rate float64, workers int, mix workload.Mix, seed uint64) (Stopper, error)

	mu      sync.Mutex
	next    int
	active  bool
	pending int
	residue []gate.ChurnResidue
	ranges  []workload.Range
	loggers []*probe.CountingLogger
}

// auxOwner is one aux session's lifetime: its instruments exist before construction starts,
// and whoever finishes the session — the slot or, after the slot deadline, the owner goroutine — measures its residue.
type auxOwner struct {
	c     *Churner
	id    string
	slot  string
	index int
	sess  *cell.Session
	// arrived is set once CreateSession returned a session.
	arrived bool
}

// Churn runs one churn slot; it is the chaos.Executor's Churn hook.
// Every step has its own budget inside the slot's 150 s deadline.
// CreateSession and Close run in owner goroutines: a step over its budget fails G13,
// and a step still unresolved at the slot deadline returns chaos.ErrChurnOverrun, which stops further faults.
// The owner then keeps the session's instruments, closes a late session, and measures its residue later (Codex J04).
//
// Parameters:
//   - ctx: carries the whole-slot deadline
//   - slot: the churn slot
//
// Returns:
//   - error: when a step failed or overran its budget, which fails G13
func (c *Churner) Churn(ctx context.Context, slot chaos.Slot) error {
	c.mu.Lock()
	index := c.next
	c.next++
	c.active = true
	c.residue = append(c.residue, gate.ChurnResidue{Slot: slot.ID, Index: index})
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.active = false
		c.mu.Unlock()
	}()
	rng, err := workload.AuxRange(index)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.ranges = append(c.ranges, rng)
	c.mu.Unlock()
	o := &auxOwner{c: c, id: fmt.Sprintf("aux%d", index), slot: slot.ID, index: index}
	b := chaos.ChurnBudget

	runtime.GC()
	before, _, err := probe.GoroutineGroups()
	if err != nil {
		return fmt.Errorf("baseline before %s: %w", o.id, err)
	}
	c.update(index, func(r *gate.ChurnResidue) { r.Before = before })
	o.event("create", nil)

	// The instruments exist, and are kept, before construction starts:
	// a failed or late constructor still leaves its log lines counted for G7.
	spec := c.Spec
	spec.ID, spec.Generation, spec.Registry = o.id, 0, c.Registry
	cfg, sess := cell.Configure(spec)
	o.sess = sess
	c.track(sess)
	stepErr := o.construct(ctx, cfg, b.Create)
	switch {
	case errors.Is(stepErr, chaos.ErrChurnOverrun):
		return fmt.Errorf("%s create: %w", o.id, stepErr)
	case !o.arrived:
		return fmt.Errorf("%s create: %w", o.id, stepErr)
	}
	if stepErr != nil {
		// Constructed over its budget but inside the slot: no load, but the same Close, grace and residue (Codex K02).
		stepErr = fmt.Errorf("%s create: %w", o.id, stepErr)
	} else if err := o.load(ctx, rng, index); err != nil {
		if errors.Is(err, chaos.ErrChurnOverrun) {
			return err
		}
		stepErr = err
	}

	o.event("close", nil)
	closeErr := o.close(ctx, b.Close)
	if errors.Is(closeErr, chaos.ErrChurnOverrun) {
		return errors.Join(stepErr, fmt.Errorf("%s: %w", o.id, closeErr))
	}
	if closeErr != nil {
		stepErr = errors.Join(stepErr, fmt.Errorf("%s: %w", o.id, closeErr))
	}
	return errors.Join(stepErr, o.settle(ctx))
}

// load runs the aux session's 30 s of the mix without LWT.
func (o *auxOwner) load(ctx context.Context, rng workload.Range, index int) error {
	c := o.c
	mix, err := workload.DefaultMix().Without(workload.ClassLWT)
	if err != nil {
		return err
	}
	env := &workload.Env{
		Session: o.sess.Session, SessionID: o.id, Range: rng, Workers: max(c.Workers/workload.AuxWorkersDivisor, 1),
		ChurnSpace: cell.ChurnSpace(), Ledger: c.Ledger, Settlement: c.Settlement,
		Progress: workload.NewProgress(time.Now()), Latency: probe.NewAggregateLatency(time.Now()), OpIDs: c.OpIDs,
		Errors: c.Recorder.Error, Violations: c.Violations, Switches: c.Switches,
	}
	start := c.StartLoad
	if start == nil {
		start = func(ctx context.Context, env *workload.Env, rate float64, workers int, mix workload.Mix, seed uint64) (Stopper, error) {
			return workload.Start(ctx, env, rate, workers, mix, seed)
		}
	}
	load, err := start(ctx, env, c.Rate*workload.AuxRateShare, env.Workers, mix, c.Seed+uint64(index))
	if err != nil {
		return err
	}
	o.event("load", nil)
	_ = chaos.Sleep(ctx, chaos.ChurnBudget.Load)
	// Stop waits for every worker's current operation; a driver call that ignores its deadline must not hold the slot
	// (Codex L01): at the slot deadline the rest of the cleanup goes to a pending owner.
	stopped := make(chan struct{})
	go func() {
		load.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
	}
	return fmt.Errorf("%s load drain: %w", o.id, o.handOff(func() { <-stopped }, closeNow, true))
}

// Stopper stops a running load and waits for its workers.
type Stopper interface {
	Stop()
}

// settle waits the grace period and measures the residue; if the slot deadline comes first,
// the rest is handed to a pending owner and the churn reports an overrun.
func (o *auxOwner) settle(ctx context.Context) error {
	deadline := time.Now().Add(chaos.ChurnBudget.Grace)
	if err := chaos.Sleep(ctx, chaos.ChurnBudget.Grace); err != nil {
		return fmt.Errorf("%s grace: %w", o.id, o.handOff(func() { time.Sleep(time.Until(deadline)) }, alreadyClosed, false))
	}
	// The residue probe reads /proc and forces a GC; it is bounded by the slot deadline too (Codex L01).
	measured := make(chan struct{})
	go func() {
		// Lateness is decided when the measurement is published: one that ends after the slot is late (Codex M04).
		o.measureResidue(func() bool { return ctx.Err() != nil })
		close(measured)
	}()
	select {
	case <-measured:
		return nil
	case <-ctx.Done():
	}
	o.c.addPending(1)
	go func() {
		defer o.c.addPending(-1)
		<-measured
	}()
	return fmt.Errorf("%s residue: %w", o.id, chaos.ErrChurnOverrun)
}

// lateNote marks a residue measured after its slot ended.
const lateNote = "residue measured after the slot ended"

// closeStep says what a pending owner still owes the session's Close.
type closeStep int

const (
	// closeNow: the owner closes the session and records the late Close.
	closeNow closeStep = iota
	// closedLate: Close was already running; the owner records its late return.
	closedLate
	// alreadyClosed: Close returned inside the slot.
	alreadyClosed
)

// handOff leaves the rest of the session's cleanup to one pending owner and returns chaos.ErrChurnOverrun:
// wait for the outstanding step, then finish Close, grace and residue as cleanUp does.
func (o *auxOwner) handOff(wait func(), step closeStep, grace bool) error {
	o.c.addPending(1)
	go func() {
		defer o.c.addPending(-1)
		wait()
		o.cleanUp(step, grace)
	}()
	return chaos.ErrChurnOverrun
}

// cleanUp is a pending owner's tail: Close as step says, the grace period if owed,
// and the residue into the slot's record, which also removes the session from the sampler.
func (o *auxOwner) cleanUp(step closeStep, grace bool) {
	switch step {
	case closeNow:
		o.doClose()
		o.event("late-close", map[string]any{"after_slot": true})
	case closedLate:
		o.event("late-close", map[string]any{"after_slot": true})
	}
	if grace {
		time.Sleep(chaos.ChurnBudget.Grace)
	}
	o.residue(true)
}

// construct runs CreateSession in an owner goroutine, bounded by budget and then by the slot deadline.
// A session arriving after the budget but within the slot returns an over-budget error with o.arrived set,
// so the caller still closes it and measures its residue; one still pending at the slot deadline is left to an owner.
func (o *auxOwner) construct(ctx context.Context, cfg *gocql.ClusterConfig, budget time.Duration) error {
	create := o.c.Create
	if create == nil {
		create = func(cfg *gocql.ClusterConfig) (*gocql.Session, error) { return cfg.CreateSession() }
	}
	type created struct {
		s   *gocql.Session
		err error
	}
	ch := make(chan created, 1)
	go func() {
		s, err := create(cfg)
		ch <- created{s, err}
	}()
	arrived := func(r created) error {
		if r.err != nil {
			return r.err
		}
		o.sess.Session, o.arrived = r.s, true
		o.c.Sampler.AddSession(o.sess)
		return nil
	}
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case r := <-ch:
		return arrived(r)
	case <-timer.C:
	case <-ctx.Done():
	}
	overBudget := fmt.Errorf("CreateSession did not return within %s", budget)
	select {
	case r := <-ch:
		o.event("late-session", map[string]any{"err": fmt.Sprint(r.err)})
		return errors.Join(overBudget, arrived(r))
	case <-ctx.Done():
	}
	o.c.addPending(1)
	go func() {
		defer o.c.addPending(-1)
		r := <-ch
		o.event("late-session", map[string]any{"err": fmt.Sprint(r.err), "after_slot": true})
		if arrived(r) != nil {
			return // nothing was built: no Close, no residue; the slot's record stays unmeasured
		}
		o.cleanUp(closeNow, true)
	}()
	return fmt.Errorf("%w: %w", chaos.ErrChurnOverrun, overBudget)
}

// close runs Close in an owner goroutine, bounded by budget and then by the slot deadline.
// A Close still pending at the slot deadline is left to an owner, which keeps the session sampled.
func (o *auxOwner) close(ctx context.Context, budget time.Duration) error {
	done := make(chan struct{})
	go func() {
		o.doClose()
		close(done)
	}()
	timer := time.NewTimer(budget)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
	case <-ctx.Done():
	}
	overBudget := errors.New("close did not return within " + budget.String())
	select {
	case <-done:
		o.event("late-close", nil)
		return overBudget
	case <-ctx.Done():
	}
	_ = o.handOff(func() { <-done }, closedLate, true)
	return fmt.Errorf("%w: %w", chaos.ErrChurnOverrun, overBudget)
}

func (o *auxOwner) doClose() {
	if o.c.CloseSession != nil {
		o.c.CloseSession(o.sess)
	} else {
		o.sess.Close()
	}
	if o.c.AfterClose != nil {
		o.c.AfterClose(o.id)
	}
}

// residue measures the session's residue into its slot's record and drops it from the sampler.
func (o *auxOwner) residue(late bool) {
	o.measureResidue(func() bool { return late })
}

// measureResidue is residue with lateness decided after the measurement returns.
func (o *auxOwner) measureResidue(lateFn func() bool) {
	defer o.c.Sampler.RemoveSession(o.id)
	start := time.Now()
	measure := o.c.Measure
	if measure == nil {
		measure = o.c.measure
	}
	measured, live, sockets, after, err := measure(o.id)
	late := lateFn()
	var res gate.ChurnResidue
	o.c.update(o.index, func(r *gate.ChurnResidue) {
		r.Measured, r.LiveEntries, r.AttributedSockets, r.After = measured, live, sockets, after
		r.Balance = o.sess.Streams.Balance()
		if err != nil {
			r.Failures = append(r.Failures, err.Error())
		}
		if took := time.Since(start); took > chaos.ChurnBudget.Residue {
			r.Failures = append(r.Failures, fmt.Sprintf("residue check took %s > %s", took.Round(time.Second), chaos.ChurnBudget.Residue))
		}
		if late {
			r.Failures = append(r.Failures, lateNote)
		}
		res = *r
	})
	o.event("residue", map[string]any{"residue": res, "late": late})
}

func (o *auxOwner) event(step string, extra map[string]any) {
	data := map[string]any{"slot": o.slot, "session": o.id, "step": step}
	for k, v := range extra {
		data[k] = v
	}
	o.c.Recorder.Event("churn", time.Now(), data)
}

// Busy reports whether a churn slot is running or an aux session's constructor or Close is still unresolved;
// no quiet checkpoint is taken while it is.
//
// Returns:
//   - bool: true while churn work is outstanding
func (c *Churner) Busy() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.active || c.pending > 0
}

// Snapshot returns every aux session's G7 counts and every churn's residue, read now,
// together with whether churn work is outstanding, under one lock,
// so an owner finishing in between cannot fall between them (Codex J04, K03).
//
// Returns:
//   - []gate.LogCounts: one per aux session, late and failed ones included
//   - []gate.ChurnResidue: one per churn, late measurements included
//   - bool: true while churn work is outstanding
func (c *Churner) Snapshot() ([]gate.LogCounts, []gate.ChurnResidue, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]gate.LogCounts, 0, len(c.loggers))
	for _, l := range c.loggers {
		out = append(out, l.Counts())
	}
	return out, append([]gate.ChurnResidue(nil), c.residue...), c.active || c.pending > 0
}

// Residue returns every churn's measurements, in index order.
//
// Returns:
//   - []gate.ChurnResidue: one per churn
func (c *Churner) Residue() []gate.ChurnResidue {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]gate.ChurnResidue(nil), c.residue...)
}

// Ranges returns the aux ranges written so far, for the register sample.
//
// Returns:
//   - []workload.Range: one per churn
func (c *Churner) Ranges() []workload.Range {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]workload.Range(nil), c.ranges...)
}

func (c *Churner) track(s *cell.Session) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loggers = append(c.loggers, s.Logger)
}

func (c *Churner) addPending(d int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pending += d
}

// update edits the residue record of churn index under the lock.
func (c *Churner) update(index int, f func(*gate.ChurnResidue)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.residue {
		if c.residue[i].Index == index {
			f(&c.residue[i])
			return
		}
	}
}

// measure takes the aux session's residue after its grace period.
func (c *Churner) measure(id string) (bool, int, int, map[string]int, error) {
	runtime.GC()
	groups, _, err := probe.GoroutineGroups()
	if err != nil {
		return false, 0, 0, nil, err
	}
	seq := c.Registry.Seq()
	owned, err := view.OwnedSocketInodes(c.FDDir)
	if err != nil {
		return false, 0, 0, groups, err
	}
	c.Registry.Prune(owned, seq)
	socks, err := view.ReadSockets(c.NetDir)
	if err != nil {
		return false, 0, 0, groups, err
	}
	rep := view.EvaluateR2(view.R2Input{Sockets: socks, Owned: owned, Registry: c.Registry, NumConns: cell.NumConns,
		Nodes: c.Sampler.Nodes, DriverAddrs: c.Sampler.DriverAddrs, ProxyPort: proxy.ProxyPort, NodePort: proxy.NodePort})
	return true, c.Registry.LiveBySession()[id], rep.BySession[id], groups, nil
}
