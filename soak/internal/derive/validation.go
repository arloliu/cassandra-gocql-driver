package derive

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cellrun"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
)

// klComparable is kL's comparability table (PLAN v7.16 §55.3 item 1):
// the attempt kinds whose injection leaves the warm-up and the cool-down comparable, each with the reason.
// A canary not named here is excluded, including one added later, until an amendment reviews it.
// It lives in this package, a derivation-only path, so reviewing it does not make earlier attempts incompatible (§55.3 item 4).
var klComparable = map[string]string{
	"control": "no injection",
	"K1":      "goroutines held outside the workload path",
	"K2":      "a dial outside the D8 registry and a blocked read, outside the workload path",
	"K3":      "file descriptors held outside the workload path",
	"K5":      "its query bypasses the workload's latency recording",
	"K6b":     "stream counters only",
	"K7":      "one synthetic error per minute through the error path, with no latency observation",
	"K8":      "one pinned key's acknowledged version",
	"K10":     "goroutines held by aux sessions, outside the workload path",
	"K15":     "a workload switch from the epoch, present in both windows",
	"K17":     "a workload switch from the epoch, present in both windows",
}

// The loaders of an execution directory; tests replace them.
var (
	loadCalibration = cellrun.LoadCalibration
	readVerdict     = readVerdictFile
)

// klKindRank orders the source kinds: the floor first, then the night, then validation (§55.2).
var klKindRank = map[string]int{"floor": 0, "night": 1, "validation": 2}

// ExcludeAttempt is the operator's exclusion of one validation attempt, for host contention recorded beforehand.
type ExcludeAttempt struct {
	Batch  string
	Slot   string
	N      int
	Reason string
}

// KLSource is one kL observation and where it came from (PLAN v7.16 §55.2).
type KLSource struct {
	// Kind is "floor", "night" or "validation".
	Kind string `json:"kind"`
	// Cell, Warm and Cool name a night comparison.
	Cell string        `json:"cell,omitempty"`
	Warm *probe.Window `json:"warm,omitempty"`
	Cool *probe.Window `json:"cool,omitempty"`
	// Batch, Slot and Attempt name a validation attempt; slotIndex is the slot's ledger order.
	Batch     string `json:"batch,omitempty"`
	Slot      string `json:"slot,omitempty"`
	Attempt   int    `json:"attempt,omitempty"`
	slotIndex int
	Class     string  `json:"class,omitempty"`
	Value     float64 `json:"value"`
}

// ValidationReport is the validation section of derivation.json (PLAN v7.16 §55.4).
type ValidationReport struct {
	// NightSHA is the night's driver commit, against which compatibility is judged.
	NightSHA string       `json:"night_sha"`
	Batches  []BatchRef   `json:"batches"`
	Attempts []AttemptRef `json:"attempts"`
}

// BatchRef is one validation batch as given.
type BatchRef struct {
	Dir string `json:"dir"`
	// Terminal is the ledger's terminal state, empty when the batch ended normally; Accepted the -accept-batch reason.
	Terminal    string   `json:"terminal,omitempty"`
	Accepted    string   `json:"accepted,omitempty"`
	Unattempted []string `json:"unattempted,omitempty"`
	Unresolved  []string `json:"unresolved,omitempty"`
}

// AttemptRef is one validation attempt and how it was judged.
type AttemptRef struct {
	Batch   string `json:"batch"`
	Slot    string `json:"slot"`
	N       int    `json:"n"`
	Kind    string `json:"kind"`
	ExecDir string `json:"exec_dir,omitempty"`
	// State is the attempt's ledger state.
	State string `json:"state"`
	// Outcome is "eligible", "excluded" or "refused"; Excluded is "<stage>: <reason>" for an excluded attempt,
	// and Refused the problems of a refused one, each of which refuses the derivation (Codex BO03).
	Outcome  string   `json:"outcome"`
	Excluded string   `json:"excluded,omitempty"`
	Refused  []string `json:"refused,omitempty"`
	// Class and OKL are the eligible attempt's largest class and its o_kL.
	Class string   `json:"class,omitempty"`
	OKL   *float64 `json:"o_kl,omitempty"`
	// Paths are the soak/ paths that differ from the night's commit, with their dispositions.
	Paths []PathDisposition `json:"paths,omitempty"`
}

// PathDisposition is one changed path's judgment: "derivation-only", "accepted: <reason>" or "refused".
type PathDisposition struct {
	Path        string `json:"path"`
	Disposition string `json:"disposition"`
}

// Ledger is the part of a batch's validation-state.json that §55.3 reads (soak/run-validation.sh).
type Ledger struct {
	Version int `json:"version"`
	Batch   struct {
		Terminal *struct {
			State  string `json:"state"`
			Slot   string `json:"slot"`
			Reason string `json:"reason"`
		} `json:"terminal"`
	} `json:"batch"`
	Slots []struct {
		Slot     string `json:"slot"`
		Canary   string `json:"canary"`
		Attempts []struct {
			N       int    `json:"n"`
			State   string `json:"state"`
			ExecDir string `json:"exec_dir"`
		} `json:"attempts"`
		RerunOwed bool `json:"rerun_owed"`
		Effective *struct {
			Result string `json:"result"`
		} `json:"effective"`
	} `json:"slots"`
}

// CanonicalBatch is a batch directory's one identity across -validation, -accept-batch and -exclude-attempt:
// absolute, with symbolic links resolved, so two spellings of one batch cannot be judged apart (Codex BO02).
//
// Parameters:
//   - dir: the batch directory as given
//
// Returns:
//   - string: the canonical path
//   - error: the directory cannot be resolved
func CanonicalBatch(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	return filepath.EvalSymlinks(abs)
}

// ReadLedger reads a batch's validation-state.json.
//
// Parameters:
//   - batch: the batch directory
//
// Returns:
//   - Ledger: the ledger
//   - error: unreadable, malformed, or of another version
func ReadLedger(batch string) (Ledger, error) {
	var l Ledger
	raw, err := os.ReadFile(filepath.Join(batch, "validation-state.json"))
	if err != nil {
		return l, fmt.Errorf("no readable ledger: %w", err)
	}
	if err := json.Unmarshal(raw, &l); err != nil {
		return l, fmt.Errorf("the ledger does not parse: %w", err)
	}
	if l.Version != 1 {
		return l, fmt.Errorf("ledger version %d, not 1", l.Version)
	}
	return l, nil
}

// derivationOnly reports whether a changed path under soak/ cannot change what a cell measures (PLAN v7.16 §55.3 item 4).
//
// Parameters:
//   - path: a repository-relative path
//
// Returns:
//   - bool: true for the derivation's own code, tests, test data and documents
func derivationOnly(path string) bool {
	switch {
	case strings.HasPrefix(path, "soak/internal/derive/"), path == "soak/cmd/soak/derive.go", path == "soak/cmd/soak/latencycheck.go":
		return true
	case strings.HasSuffix(path, "_test.go"), strings.HasPrefix(path, "soak/testdata/"), strings.HasSuffix(path, ".md"):
		return true
	}
	return false
}

// readVerdictFile reads an execution directory's verdict.json for §55.3 stage 2.
// The fields the stage judges must be present with their types: an absent or null final, evidence, complete or gates
// is malformed, never a false that would quietly exclude the attempt (Codex BO01).
//
// Parameters:
//   - dir: the execution directory
//
// Returns:
//   - cellrun.RecordedVerdict: the verdict
//   - error: unreadable or malformed
func readVerdictFile(dir string) (cellrun.RecordedVerdict, error) {
	var v cellrun.RecordedVerdict
	raw, err := os.ReadFile(filepath.Join(dir, "verdict.json"))
	if err != nil {
		return v, err
	}
	var shape struct {
		Final    *bool `json:"final"`
		Evidence *struct {
			Complete *bool `json:"complete"`
		} `json:"evidence"`
		Gates *[]*struct {
			Gate   *string `json:"gate"`
			Status *string `json:"status"`
		} `json:"gates"`
	}
	if err := json.Unmarshal(raw, &shape); err != nil {
		return v, fmt.Errorf("does not parse: %w", err)
	}
	switch {
	case shape.Final == nil:
		return v, errors.New("no final")
	case shape.Evidence == nil || shape.Evidence.Complete == nil:
		return v, errors.New("no evidence.complete")
	case shape.Gates == nil:
		return v, errors.New("no gates")
	}
	for i, g := range *shape.Gates {
		if g == nil || g.Gate == nil || g.Status == nil {
			return v, fmt.Errorf("gates[%d] has no gate or status", i)
		}
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return v, fmt.Errorf("does not parse: %w", err)
	}
	return v, nil
}

// compareKL is §55.2's total order of kL sources.
func compareKL(a, b KLSource) int {
	if c := cmp.Compare(klKindRank[a.Kind], klKindRank[b.Kind]); c != 0 {
		return c
	}
	window := func(w *probe.Window) [2]int {
		if w == nil {
			return [2]int{}
		}
		return [2]int{w.From, w.To}
	}
	aw, bw, ac, bc := window(a.Warm), window(b.Warm), window(a.Cool), window(b.Cool)
	return cmp.Or(
		cmp.Compare(a.Cell, b.Cell), cmp.Compare(a.Batch, b.Batch), cmp.Compare(a.slotIndex, b.slotIndex), cmp.Compare(a.Attempt, b.Attempt),
		cmp.Compare(a.Class, b.Class),
		cmp.Compare(aw[0], bw[0]), cmp.Compare(aw[1], bw[1]), cmp.Compare(ac[0], bc[0]), cmp.Compare(ac[1], bc[1]),
	)
}

// lockBatch takes the batch's runner lock shared and without blocking, so a running batch refuses (§55.3);
// a batch without a lock file never ran a runner that could still hold it.
func lockBatch(dir string) (func(), error) {
	f, err := os.Open(filepath.Join(dir, ".run-validation.lock"))
	if errors.Is(err, fs.ErrNotExist) {
		return func() {}, nil
	}
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("the batch's lock is held, a runner is active: %w", err)
	}
	return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, nil
}

// ledgerStage is §55.3 stage 1, from the ledger alone.
//
// Parameters:
//   - canaryID: the slot's canary, empty for a control
//   - state: the attempt's ledger state
//   - exclude: the operator's exclusion of this attempt, or nil
//
// Returns:
//   - string: "<stage>: <reason>" when excluded; empty when the attempt goes on
func ledgerStage(canaryID, state string, exclude *ExcludeAttempt) string {
	kind := canaryID
	if kind == "" {
		kind = "control"
	}
	switch {
	case klComparable[kind] == "":
		return fmt.Sprintf("kind: %s is not in kL's comparability table", kind)
	case state != "resolved":
		return fmt.Sprintf("state: %s, not resolved", state)
	case exclude != nil:
		return "operator: " + exclude.Reason
	}
	return ""
}

// verdictStage is §55.3 stage 2, over a loaded verdict.
func verdictStage(v cellrun.RecordedVerdict) string {
	switch {
	case !v.Final:
		return "verdict: not final"
	case !v.Evidence.Complete:
		return "verdict: evidence incomplete"
	}
	for _, r := range v.Gates {
		if r.Gate == "G16" && r.Status == gate.StatusFail {
			return "verdict: G16 failed"
		}
	}
	return ""
}

// validationOKL is an eligible attempt's o_kL per class, from its recorded latency event: G14's own comparison (§55.2).
//
// Parameters:
//   - col: the attempt's collected inputs
//
// Returns:
//   - []KLSource: one per class, without the attempt's identity
//   - []string: problems, e.g. a class without a positive warm-up p99
func validationOKL(col cellrun.Collected) ([]KLSource, []string) {
	var out []KLSource
	var problems []string
	for _, class := range slices.Sorted(slices.Values(col.Classes)) {
		warm, cool := col.WarmupP99[class], col.CooldownP99[class]
		if !(warm > 0) {
			problems = append(problems, fmt.Sprintf("class %s has no positive warm-up p99", class))
			continue
		}
		v := gate.G14Critical(map[string]float64{class: warm}, map[string]float64{class: cool})
		switch {
		case v.NonFinite:
			problems = append(problems, fmt.Sprintf("class %s's comparison is not finite", class))
		case v.OK:
			out = append(out, KLSource{Kind: "validation", Class: class, Value: v.Value})
		}
	}
	return out, problems
}

// attemptSuitability is §55.3 stage 4: any problem refuses the derivation.
func attemptSuitability(cal cellrun.Calibration, o Options, nightSHA string) ([]PathDisposition, []string) {
	var p []string
	add := func(format string, args ...any) { p = append(p, fmt.Sprintf(format, args...)) }
	p = append(p, cal.Missing...)
	b, col := cal.Build, cal.Collected
	if s, ok := b.SourceClean.(string); (!ok || s != "true") && strings.TrimSpace(o.AcceptProvenance) == "" {
		add(`source_clean is %v, not the string "true", and -accept-provenance was not given`, b.SourceClean)
	}
	var paths []PathDisposition
	switch {
	case b.DriverSHA == "":
		add("build.json names no driver commit")
	case o.SourceUnchanged == nil:
		add("no source check was given")
	default:
		if same, err := o.SourceUnchanged(b.DriverSHA); err != nil {
			add("the source check against %s failed: %v", b.DriverSHA, err)
		} else if !same {
			add("the tree outside soak/ changed between %s and HEAD", b.DriverSHA)
		}
		switch {
		case nightSHA == "":
			add("the night names no driver commit to judge compatibility against")
		case o.PathsChanged == nil:
			add("no compatibility check was given")
		default:
			changed, err := o.PathsChanged(b.DriverSHA, nightSHA)
			if err != nil {
				add("the compatibility check %s..%s failed: %v", b.DriverSHA, nightSHA, err)
			}
			for _, path := range slices.Sorted(slices.Values(changed)) {
				d := PathDisposition{Path: path, Disposition: "derivation-only"}
				if !derivationOnly(path) {
					if reason := o.AcceptCompat[path]; reason != "" {
						d.Disposition = "accepted: " + reason
					} else {
						d.Disposition = "refused"
						add("%s differs from the night's build (%s..%s) and is not derivation-only; -accept-compat was not given", path, b.DriverSHA, nightSHA)
					}
				}
				paths = append(paths, d)
			}
		}
	}
	switch {
	case cal.Late == nil:
		add("the latency event has no late counts (PLAN §51.2)")
	default:
		for _, class := range slices.Sorted(maps.Keys(cal.Late)) {
			if n := cal.Late[class]; n != 0 {
				add("%d late latency observations of class %s", n, class)
			}
		}
	}
	classes := slices.Sorted(slices.Values(col.Classes))
	if !slices.Equal(classes, slices.Sorted(maps.Keys(col.WarmupP99))) || !slices.Equal(classes, slices.Sorted(maps.Keys(col.CooldownP99))) ||
		(cal.Late != nil && !slices.Equal(classes, slices.Sorted(maps.Keys(cal.Late)))) {
		add("the mix's classes %v differ from the latency event's", classes)
	}
	if cal.Latency == nil {
		add("no latency slices")
	} else {
		_, _, cross := CrossCheckLatency("", &cal)
		for _, c := range cross {
			add("%s", strings.TrimPrefix(c, ": "))
		}
	}
	return paths, p
}

// loadValidation loads the given validation batches and judges every attempt (PLAN v7.16 §55.3).
//
// Parameters:
//   - o: the maintainer's inputs; o.Validation names the batches
//   - nightSHA: the night's driver commit
//
// Returns:
//   - []KLSource: the eligible attempts' observations
//   - *ValidationReport: the report section; nil when no batch was given
//   - []string: problems that refuse the derivation
func loadValidation(o Options, nightSHA string) ([]KLSource, *ValidationReport, []string) {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	canonical := func(flag, dir string) string {
		c, err := CanonicalBatch(dir)
		if err != nil {
			add("%s %s: %v", flag, dir, err)
			return ""
		}
		return c
	}
	var batches []string
	for _, b := range o.Validation {
		c := canonical("-validation", b)
		switch {
		case c == "":
		case slices.Contains(batches, c):
			add("-validation %s: the batch %s is given twice", b, c)
		default:
			batches = append(batches, c)
		}
	}
	accepted := map[string]string{}
	for b, reason := range o.AcceptBatch {
		c := canonical("-accept-batch", b)
		switch {
		case c == "":
		case !slices.Contains(batches, c) || strings.TrimSpace(reason) == "":
			add("-accept-batch %s: not a given batch, or no reason", b)
		case accepted[c] != "":
			add("-accept-batch %s: the batch %s is accepted twice", b, c)
		default:
			accepted[c] = reason
		}
	}
	for path, reason := range o.AcceptCompat {
		if strings.TrimSpace(reason) == "" {
			add("-accept-compat %s: no reason", path)
		}
	}
	excluded := map[string]*ExcludeAttempt{}
	for i, e := range o.Exclude {
		c := canonical("-exclude-attempt", e.Batch)
		key := attemptKey(c, e.Slot, e.N)
		switch {
		case c == "":
		case !slices.Contains(batches, c):
			add("-exclude-attempt %s/%s-%d: not in a given batch", e.Batch, e.Slot, e.N)
		case strings.TrimSpace(e.Reason) == "":
			add("-exclude-attempt %s/%s-%d: no reason", e.Batch, e.Slot, e.N)
		default:
			excluded[key] = &o.Exclude[i]
		}
	}
	if len(o.Validation) == 0 {
		return nil, nil, problems
	}
	rep := &ValidationReport{NightSHA: nightSHA, Batches: []BatchRef{}, Attempts: []AttemptRef{}}
	var sources []KLSource
	for _, batch := range batches {
		s, refs, ref, p := loadBatch(batch, accepted[batch], o, excluded, nightSHA)
		sources = append(sources, s...)
		rep.Attempts = append(rep.Attempts, refs...)
		rep.Batches = append(rep.Batches, ref)
		problems = append(problems, p...)
	}
	for key, e := range excluded {
		if !slices.ContainsFunc(rep.Attempts, func(a AttemptRef) bool { return attemptKey(a.Batch, a.Slot, a.N) == key }) {
			add("-exclude-attempt %s/%s-%d: the batch has no attempt %d of slot %s", e.Batch, e.Slot, e.N, e.N, e.Slot)
		}
	}
	return sources, rep, problems
}

// attemptKey identifies one attempt of a canonical batch.
func attemptKey(batch, slot string, n int) string {
	return fmt.Sprintf("%s\x00%s\x00%d", batch, slot, n)
}

// loadBatch judges one batch, by its canonical path: stability (§55.3 "Input and cohort"), then every attempt by the four stages.
func loadBatch(batch, accepted string, o Options, excluded map[string]*ExcludeAttempt, nightSHA string) ([]KLSource, []AttemptRef, BatchRef, []string) {
	ref := BatchRef{Dir: batch}
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(batch+": "+format, args...)) }
	release, err := lockBatch(batch)
	if err != nil {
		add("%v", err)
		return nil, nil, ref, problems
	}
	defer release()
	l, err := ReadLedger(batch)
	if err != nil {
		add("%v", err)
		return nil, nil, ref, problems
	}
	for _, s := range l.Slots {
		if len(s.Attempts) == 0 {
			ref.Unattempted = append(ref.Unattempted, s.Slot)
		}
		for _, a := range s.Attempts {
			if a.State != "resolved" {
				ref.Unresolved = append(ref.Unresolved, fmt.Sprintf("%s #%d", s.Slot, a.N))
			}
		}
	}
	if t := l.Batch.Terminal; t != nil {
		ref.Terminal = t.State
		if accepted == "" {
			add("the batch is terminal (%s at %s: %s) and -accept-batch was not given", t.State, t.Slot, t.Reason)
			return nil, nil, ref, problems
		}
		ref.Accepted = accepted
	} else {
		for _, s := range l.Slots {
			if s.Effective == nil {
				add("the batch is not finished: slot %s has no effective result (-accept-batch applies to terminal batches only)", s.Slot)
			}
		}
		if len(problems) > 0 {
			return nil, nil, ref, problems
		}
	}

	var sources []KLSource
	var refs []AttemptRef
	for si, s := range l.Slots {
		kind := s.Canary
		if kind == "" {
			kind = "control"
		}
		for _, a := range s.Attempts {
			ar := AttemptRef{Batch: batch, Slot: s.Slot, N: a.N, Kind: kind, State: a.State, ExecDir: a.ExecDir}
			id := fmt.Sprintf("%s/%s #%d", batch, s.Slot, a.N)
			if why := ledgerStage(s.Canary, a.State, excluded[attemptKey(batch, s.Slot, a.N)]); why != "" {
				ar.Outcome, ar.Excluded = "excluded", why
			} else {
				sources = judgeAttempt(&ar, o, nightSHA, sources, si)
				for _, r := range ar.Refused {
					problems = append(problems, id+": "+r)
				}
			}
			refs = append(refs, ar)
		}
	}
	return sources, refs, ref, problems
}

// judgeAttempt runs §55.3 stages 2–4 on an attempt that passed the ledger stage:
// it sets the attempt's outcome, and adds its observations when it is eligible.
func judgeAttempt(ar *AttemptRef, o Options, nightSHA string, sources []KLSource, slotIndex int) []KLSource {
	refuse := func(problems ...string) []KLSource {
		ar.Outcome, ar.Refused = "refused", append(ar.Refused, problems...)
		return sources
	}
	if !filepath.IsAbs(ar.ExecDir) {
		return refuse(fmt.Sprintf("the ledger's execution directory %q is not absolute", ar.ExecDir))
	}
	v, err := readVerdict(ar.ExecDir)
	if err != nil {
		return refuse(fmt.Sprintf("verdict.json: %v", err))
	}
	if why := verdictStage(v); why != "" {
		ar.Outcome, ar.Excluded = "excluded", why
		return sources
	}
	cal, err := loadCalibration(ar.ExecDir)
	if err != nil {
		return refuse(fmt.Sprintf("load %s: %v", ar.ExecDir, err))
	}
	if cal.Collected.CooldownContaminated() {
		ar.Outcome, ar.Excluded = "excluded", "contamination: a fault window reaches into the cool-down"
		return sources
	}
	paths, p := attemptSuitability(cal, o, nightSHA)
	ar.Paths = paths
	obs, op := validationOKL(cal.Collected)
	if p = append(p, op...); len(p) > 0 {
		return refuse(p...)
	}
	ar.Outcome = "eligible"
	for _, s := range obs {
		s.Batch, s.Slot, s.Attempt, s.slotIndex = ar.Batch, ar.Slot, ar.N, slotIndex
		sources = append(sources, s)
		if ar.OKL == nil || s.Value > *ar.OKL {
			v := s.Value
			ar.OKL, ar.Class = &v, s.Class
		}
	}
	return sources
}

// combineKL adds the validation observations to kL and attributes its value (PLAN v7.16 §55.2):
// observations, the max, the factor, the floor, in that order; -raise comes after, in ApplyRaises.
//
// Parameters:
//   - th: the thresholds, kL updated in place
//   - derived: the derivation records, kL's updated in place
//   - cells: the night's cell ids
//   - obs: the night's observations
//   - val: the eligible validation attempts' observations
func combineKL(th gate.Thresholds, derived []Derived, cells []string, obs map[string]map[string]Observation, val []KLSource) {
	i := slices.IndexFunc(derived, func(d Derived) bool { return d.Name == gate.KL })
	if i < 0 {
		return
	}
	d := &derived[i]
	var all []KLSource
	for _, id := range cells {
		if o := obs[id][gate.KL]; !o.Excluded {
			all = append(all, o.Sources...)
		}
	}
	for _, s := range val {
		if d.ValidationObserved == nil || s.Value > *d.ValidationObserved {
			v := s.Value
			d.ValidationObserved = &v
		}
	}
	if d.ValidationObserved != nil && *d.ValidationObserved > d.Observed {
		d.Observed = *d.ValidationObserved
	}
	d.Value, d.FloorApplied = Factor*d.Observed, false
	if d.Floor > d.Value {
		d.Value, d.FloorApplied = d.Floor, true
	}
	th[gate.KL] = d.Value
	all = append(all, val...)
	slices.SortStableFunc(all, compareKL)
	d.Sources = nil
	if d.Floor == d.Value {
		d.Sources = append(d.Sources, KLSource{Kind: "floor", Value: d.Floor})
	}
	for _, s := range all {
		if Factor*s.Value == d.Value {
			d.Sources = append(d.Sources, s)
		}
	}
}

// nightDriverSHA is the driver commit the night's cells share; provenance (§41.5 item 5) refuses when they differ.
func nightDriverSHA(cells []Cell) string {
	if len(cells) == 0 {
		return ""
	}
	return cells[0].Cal.Build.DriverSHA
}

// HasAttempt reports whether the ledger records attempt n of a slot.
//
// Parameters:
//   - slot: the slot name
//   - n: the attempt number
//
// Returns:
//   - bool: true when the attempt exists
func (l Ledger) HasAttempt(slot string, n int) bool {
	for _, s := range l.Slots {
		if s.Slot != slot {
			continue
		}
		for _, a := range s.Attempts {
			if a.N == n {
				return true
			}
		}
	}
	return false
}

// String renders a kL source for derivation.md.
//
// Returns:
//   - string: e.g. "night c50p5 read [0, 600) against [6600, 6900): 0.2"
func (s KLSource) String() string {
	switch s.Kind {
	case "floor":
		return fmt.Sprintf("floor %.6g", s.Value)
	case "night":
		return fmt.Sprintf("night %s %s [%d, %d) against [%d, %d): %.6g", s.Cell, s.Class, s.Warm.From, s.Warm.To, s.Cool.From, s.Cool.To, s.Value)
	}
	return fmt.Sprintf("validation %s %s #%d %s: %.6g", s.Batch, s.Slot, s.Attempt, s.Class, s.Value)
}

// markdown renders the validation section of derivation.md; nothing when no batch was given.
func (v *ValidationReport) markdown(b *strings.Builder) {
	if v == nil {
		return
	}
	fmt.Fprintf(b, "\n## Validation evidence (PLAN v7.16 §55.3)\n\nCompatibility is judged against the night's driver commit `%s`.\n\n", v.NightSHA)
	b.WriteString("| batch | terminal | accepted | unattempted | unresolved |\n|---|---|---|---|---|\n")
	for _, r := range v.Batches {
		fmt.Fprintf(b, "| `%s` | %s | %s | %s | %s |\n", r.Dir, mdCell(r.Terminal), mdCell(r.Accepted),
			mdCell(strings.Join(r.Unattempted, ", ")), mdCell(strings.Join(r.Unresolved, ", ")))
	}
	b.WriteString("\n| batch | slot | attempt | kind | state | outcome | largest class | o_kL | changed paths |\n|---|---|---|---|---|---|---|---|---|\n")
	for _, a := range v.Attempts {
		outcome, okl := a.Outcome, ""
		switch a.Outcome {
		case "excluded":
			outcome += ": " + a.Excluded
		case "refused":
			outcome += ": " + strings.Join(a.Refused, "; ")
		}
		if a.OKL != nil {
			okl = fmt.Sprintf("%.6g", *a.OKL)
		}
		var paths []string
		for _, p := range a.Paths {
			paths = append(paths, p.Path+" ("+p.Disposition+")")
		}
		fmt.Fprintf(b, "| `%s` | %s | %d | %s | %s | %s | %s | %s | %s |\n", a.Batch, a.Slot, a.N, a.Kind, a.State, mdCell(outcome), a.Class, okl, mdCell(strings.Join(paths, "<br>")))
	}
}
