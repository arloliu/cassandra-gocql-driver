package workload

import (
	"sync"
	"time"
)

// SettleGrace is how long after an op's deadline its settlement entry is finalized (PLAN §4.2).
const SettleGrace = 500 * time.Millisecond

// ProofKind is what a settlement entry is trying to prove.
type ProofKind int

// ProofPrefetch and ProofSpeculation are the two G15 proofs.
const (
	// ProofPrefetch is a controlled early-close scan.
	ProofPrefetch ProofKind = iota + 1
	// ProofSpeculation is a spec-read.
	ProofSpeculation
)

// Settlement holds the G15 proofs of in-flight controlled scans and spec-reads (PLAN §4.2, v7.3).
//
// An entry is registered before its op executes, counts the op's observations,
// and is finalized exactly once at its deadline + SettleGrace, then folded into the totals and deleted.
// Ingestion and finalization are serialized.
// An observation for an op whose entry is gone is late: it is counted apart and never promotes coverage.
type Settlement struct {
	mu      sync.Mutex
	entries map[uint64]*entry
	totals  SettlementTotals
}

// SettlementTotals are the finalized G15 counts.
type SettlementTotals struct {
	// PrefetchProven counts scans with Close nil, ≥ pages+1 successful observations and no failed one.
	PrefetchProven int
	// PrefetchUnproven counts scans in the denominator that did not reach the proof.
	PrefetchUnproven int
	// PrefetchExcluded counts scans whose Close failed or that saw a failed observation.
	PrefetchExcluded int
	// PrefetchCloseMissing counts scans whose Close was never reported.
	PrefetchCloseMissing int
	// PrefetchOverlap counts proven scans whose last page observation arrived after Close (a diagnostic).
	PrefetchOverlap int
	// SpeculationProven counts spec-reads with ≥ 2 observations.
	SpeculationProven int
	// SpeculationTotal counts finalized spec-reads.
	SpeculationTotal int
	// LatePrefetch and LateSpeculation count observations that arrived after finalization.
	LatePrefetch, LateSpeculation int
}

type entry struct {
	kind         ProofKind
	pagesEntered int
	finalizeAt   time.Time
	ok, failed   int
	closeKnown   bool
	closeErr     bool
	okAtClose    int
}

// NewSettlement returns an empty table.
//
// Returns:
//   - *Settlement: the table
func NewSettlement() *Settlement {
	return &Settlement{entries: map[uint64]*entry{}}
}

// Register adds an entry before its op executes.
//
// Parameters:
//   - op: the op id
//   - kind: the proof it is after
//   - pagesEntered: for a controlled scan, the pages it will read rows from (2); ignored otherwise
//   - deadline: the op context's deadline
func (s *Settlement) Register(op uint64, kind ProofKind, pagesEntered int, deadline time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[op] = &entry{kind: kind, pagesEntered: pagesEntered, finalizeAt: deadline.Add(SettleGrace)}
}

// Observe ingests one ObserveQuery call.
//
// Parameters:
//   - op: the op id from the observation's context
//   - kind: the op's proof kind, so a late observation is counted under it
//   - failed: the observation's Err was non-nil
func (s *Settlement) Observe(op uint64, kind ProofKind, failed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[op]
	if !ok {
		switch kind {
		case ProofPrefetch:
			s.totals.LatePrefetch++
		case ProofSpeculation:
			s.totals.LateSpeculation++
		}
		return
	}
	if failed {
		e.failed++
	} else {
		e.ok++
	}
}

// Closed records a controlled scan's Close.
//
// Parameters:
//   - op: the op id
//   - closeErr: whether Close returned an error
func (s *Settlement) Closed(op uint64, closeErr bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[op]; ok {
		e.closeKnown, e.closeErr, e.okAtClose = true, closeErr, e.ok
	}
}

// Tick finalizes every entry due by now.
//
// Parameters:
//   - now: the current time
func (s *Settlement) Tick(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for op, e := range s.entries {
		if !now.Before(e.finalizeAt) {
			s.finalize(op, e)
		}
	}
}

// Drain finalizes every outstanding entry, before the G15 verdict.
func (s *Settlement) Drain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for op, e := range s.entries {
		s.finalize(op, e)
	}
}

// Totals returns the finalized counts.
//
// Returns:
//   - SettlementTotals: the counts so far
func (s *Settlement) Totals() SettlementTotals {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totals
}

// Pending returns the number of unfinalized entries.
//
// Returns:
//   - int: the entries still open
func (s *Settlement) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

// finalize folds one entry into the totals; s.mu is held.
func (s *Settlement) finalize(op uint64, e *entry) {
	delete(s.entries, op)
	switch e.kind {
	case ProofPrefetch:
		switch {
		case !e.closeKnown:
			s.totals.PrefetchCloseMissing++
		case e.closeErr || e.failed > 0:
			s.totals.PrefetchExcluded++
		case e.ok >= e.pagesEntered+1:
			s.totals.PrefetchProven++
			if e.okAtClose < e.pagesEntered+1 {
				s.totals.PrefetchOverlap++
			}
		default:
			s.totals.PrefetchUnproven++
		}
	case ProofSpeculation:
		s.totals.SpeculationTotal++
		if e.ok+e.failed >= 2 {
			s.totals.SpeculationProven++
		}
	}
}
