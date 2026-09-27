package cellrun

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cell"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/config"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// cellNodes names a cell's ccm nodes.
var cellNodes = []string{"node1", "node2", "node3"}

// Calibration is one calibration cell loaded back from its execution directory (PLAN §41.2):
// the inputs of the calibrated gates, and what the suitability checks of §41.5 read.
// The non-calibrated gates are not replayed; their recorded results are in Verdict.
//
// The completeness rule (Codex rounds 24–27): a record is accepted only if encoding the value it decodes to,
// with the harness's own writer type, gives back the record exactly (strictDecode);
// then the invariants the structure cannot express are checked, and every violation is listed in Missing.
type Calibration struct {
	// Dir is the execution directory.
	Dir string
	// Collected holds the calibrated gates' inputs; Thresholds is nil.
	Collected Collected
	// Latency is the primary's latency histograms, rebuilt from the persisted slices.
	Latency *probe.Latency
	// Verdict is the recorded verdict.json.
	Verdict RecordedVerdict
	// Build is the recorded build.json.
	Build BuildFacts
	// NumConns and NodeCount come from config.json.
	NumConns, NodeCount int
	// HasBaseline is true when the g12 event carried baseline₀, the only place it is persisted.
	HasBaseline bool
	// Missing lists every record that is not exactly what the harness writes, every absent required event,
	// and every broken invariant: absent evidence is never a measured zero.
	Missing []string
	// WindowEvents counts the window events.
	WindowEvents int
}

// RecordedVerdict is the part of verdict.json the derivation reads.
type RecordedVerdict struct {
	Status          gate.VerdictStatus `json:"status"`
	Final           bool               `json:"final"`
	WorkloadSeconds float64            `json:"workload_seconds"`
	Cell            string             `json:"cell"`
	Evidence        struct {
		Complete bool     `json:"complete"`
		Problems []string `json:"problems"`
	} `json:"evidence"`
	Gates []gate.Result `json:"gates"`
}

// BuildFacts is the part of build.json the derivation reads.
// run-night.sh writes build.json with jq, not from a Go type, so its fields are checked one by one (buildMissing).
type BuildFacts struct {
	Attempt    string `json:"attempt"`
	Cell       string `json:"cell"`
	Mode       string `json:"mode"`
	Kind       string `json:"kind"`
	SoakSHA256 string `json:"soak_sha256"`
	DriverSHA  string `json:"driver_sha"`
	// DriverDirty is kept raw: run-night.sh writes a string, and PLAN §41.5 accepts only the exact string "false".
	DriverDirty any `json:"driver_dirty"`
}

// The writer types of the records the derivation reads that the harness writes as maps (sampler.go, run.go, churn.go).
type (
	// latencyRecord is a samples.jsonl record of kind latency: one slice, every class.
	latencyRecord struct {
		Kind    string                          `json:"kind"`
		Slice   int                             `json:"slice"`
		Classes map[string]probe.SliceHistogram `json:"classes"`
	}
	// envelope is an events.jsonl line (EventLine) with its payload kept raw.
	envelope struct {
		T    float64         `json:"t"`
		Time time.Time       `json:"time"`
		Kind string          `json:"kind"`
		Data json.RawMessage `json:"data,omitempty"`
	}
	residueEvent struct {
		Slot    string            `json:"slot"`
		Session string            `json:"session"`
		Step    string            `json:"step"`
		Residue gate.ChurnResidue `json:"residue"`
		Late    bool              `json:"late"`
	}
	countersEvent struct {
		G7            gate.LogCounts   `json:"g7"`
		Dials9042     []string         `json:"dials_9042"`
		AttemptErrors map[string]int64 `json:"attempt_errors"`
	}
	oraclesEvent struct {
		RegisterKeys int               `json:"register_keys"`
		Register     []string          `json:"register"`
		LWT          workload.LWTCheck `json:"lwt"`
	}
	latencyEvent struct {
		Warmup   map[string]float64 `json:"warmup_p99_s"`
		Cooldown map[string]float64 `json:"cooldown_p99_s"`
	}
)

// sampleKinds are the record kinds the sampler writes to samples.jsonl.
var sampleKinds = []string{"profile", "cheap", "latency"}

// requiredEvents are the events every completed cell writes once.
var requiredEvents = []string{"g12", "counters", "oracles", "report", "latency", "phase"}

var hexDigest = regexp.MustCompile(`^[0-9a-f]{64}$`)

// LoadCalibration loads a calibration cell's execution directory (PLAN §41.2).
// A record that does not decode, or a structural problem, is listed in Missing for §41.5 to refuse;
// only an unreadable file, or latency slices that cannot be rebuilt, is an error.
//
// Parameters:
//   - dir: the execution directory
//
// Returns:
//   - Calibration: the loaded cell
//   - error: a missing or unreadable artifact
func LoadCalibration(dir string) (Calibration, error) {
	cal := Calibration{Dir: dir}
	miss := func(format string, args ...any) { cal.Missing = append(cal.Missing, fmt.Sprintf(format, args...)) }
	check := func(what string, raw []byte, v any) bool {
		d, err := strictDecode(raw, v)
		switch {
		case err != nil:
			miss("%s: %v", what, err)
			return false
		case d != "":
			miss("%s is not what the harness writes: %s", what, d)
		}
		return true
	}

	var conf config.Config
	var vfile VerdictFile
	for _, f := range []struct {
		name string
		v    any
	}{{"config.json", &conf}, {"verdict.json", &vfile}} {
		raw, err := os.ReadFile(filepath.Join(dir, f.name))
		if err != nil {
			return cal, err
		}
		check(f.name, raw, f.v)
	}
	if err := readJSON(filepath.Join(dir, "verdict.json"), &cal.Verdict); err != nil {
		return cal, err
	}
	if err := readJSON(filepath.Join(dir, "build.json"), &cal.Build); err != nil {
		return cal, err
	}
	cal.Missing = append(cal.Missing, buildMissing(filepath.Join(dir, "build.json"))...)
	tl, _, _, _ := conf.Effective()
	if !(tl.Warmup > 0 && tl.Warmup < tl.Cooldown && tl.Cooldown <= tl.Workload) {
		miss("config.json: the effective timeline %v is not 0 < warm-up < cool-down ≤ workload", tl)
	}
	cal.NumConns, cal.NodeCount = conf.Base.Shared.Driver.NumConns, conf.Base.Shared.Cluster.Nodes
	c := &cal.Collected
	c.Mode, c.Timeline, c.WorkloadSeconds, c.Ran = conf.Overrides.Mode, tl, cal.Verdict.WorkloadSeconds, true
	for _, sh := range conf.Base.Shared.Mix {
		c.Classes = append(c.Classes, string(sh.Class))
	}
	c.Nodes = slices.Clone(cellNodes) // §41.5 checks NodeCount against it

	byClass := map[string][]probe.SliceHistogram{}
	sliceSeconds := int(CheapInterval / time.Second)
	n := 0
	err := eachLine(filepath.Join(dir, "samples.jsonl"), func(line []byte) error {
		n++
		var head struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(line, &head); err != nil || !slices.Contains(sampleKinds, head.Kind) {
			miss("samples.jsonl line %d: kind %q is not one the sampler writes", n, head.Kind)
			return nil
		}
		switch head.Kind {
		case "cheap":
			// Not consumed, but validated: a record relabelled cheap must not hide another kind's payload (Codex AO03).
			var ch Cheap
			check(fmt.Sprintf("samples.jsonl line %d (cheap)", n), line, &ch)
		case "profile":
			var p Profile
			if check(fmt.Sprintf("samples.jsonl line %d (profile)", n), line, &p) {
				c.Profiles = append(c.Profiles, p)
			}
		case "latency":
			var l latencyRecord
			if !check(fmt.Sprintf("samples.jsonl line %d (latency)", n), line, &l) {
				return nil
			}
			// The sampler writes a slice only when some class has observations (Codex AO01).
			if len(l.Classes) == 0 {
				miss("samples.jsonl line %d (latency): no class", n)
			}
			for class, h := range l.Classes {
				// The sampler writes a class only when it has observations, on the slice grid (Codex AN02).
				if h.N <= 0 || h.Seconds != sliceSeconds || h.Start != l.Slice*sliceSeconds {
					miss("samples.jsonl line %d (latency) class %s: n %d, slice %d, start %d, seconds %d", n, class, h.N, l.Slice, h.Start, h.Seconds)
				}
				byClass[class] = append(byClass[class], h)
			}
		}
		return nil
	})
	if err != nil {
		return cal, err
	}
	if cal.Latency, err = probe.RebuildLatency(sliceSeconds, byClass); err != nil {
		return cal, fmt.Errorf("%s: %w", dir, err)
	}

	var windows []chaos.Window
	var epochs []time.Time
	var phaseEpoch *time.Time
	seen := map[string]bool{}
	n = 0
	err = eachLine(filepath.Join(dir, "events.jsonl"), func(line []byte) error {
		n++
		var e envelope
		what := fmt.Sprintf("events.jsonl line %d", n)
		if !check(what, line, &e) {
			return nil
		}
		if e.Kind == "" {
			miss("%s: no kind", what)
			return nil
		}
		seen[e.Kind] = true
		if e.Kind == "phase" && phaseEpoch == nil {
			epoch := epochOf(e)
			phaseEpoch = &epoch
		}
		return cal.event(e, what+" ("+e.Kind+")", check, &windows, &epochs)
	})
	if err != nil {
		return cal, err
	}
	for _, req := range requiredEvents {
		if !seen[req] {
			miss("no %s event in events.jsonl", req)
		}
	}
	for i, o := range c.Report.Outcomes {
		if o.Start.IsZero() || o.End.IsZero() || o.End.Before(o.Start) {
			miss("report outcome %d (%s): start %s, end %s", i, o.ID, o.Start.Format(time.RFC3339Nano), o.End.Format(time.RFC3339Nano))
		}
	}
	cal.Missing = append(cal.Missing, profileInventory(c.Profiles)...)
	cal.WindowEvents = len(windows)
	cal.Missing = append(cal.Missing, matchWindows(c.Report.Outcomes, windows)...)
	// Every event carries the same workload epoch; a window whose own epoch differs would move its exclusion (Codex AM01).
	if phaseEpoch != nil {
		for i, w := range windows {
			if d := epochs[i].Sub(*phaseEpoch); d > maxEpochSkew || d < -maxEpochSkew {
				miss("window %s: its epoch is %s off the workload epoch", w.ID, d)
			}
		}
	}
	for i, w := range windows {
		// The window event is recorded at the window's start, so its own time − t is the workload epoch.
		epoch := epochs[i]
		end := c.WorkloadSeconds // an open window extends to the stop, as Windows.Intervals does
		if !w.End.IsZero() {
			end = w.End.Sub(epoch).Seconds()
		}
		c.Windows = append(c.Windows, gate.Interval{Start: w.Start.Sub(epoch).Seconds(), End: end})
	}

	n = 0
	err = eachLine(filepath.Join(dir, "ccm", "health.jsonl"), func(line []byte) error {
		n++
		var l RoundLine
		if !check(fmt.Sprintf("health.jsonl round %d", n), line, &l) {
			return nil
		}
		// The first round is the prime, which only resets gcstats; no other is (Codex AN06).
		if l.Prime != (n == 1) {
			miss("health.jsonl round %d: prime is %v", n, l.Prime)
		}
		tp, gc := l.Samples()
		c.TP, c.GC = append(c.TP, tp), append(c.GC, gc...)
		return nil
	})
	if err != nil {
		return cal, fmt.Errorf("%s: %w", dir, err)
	}
	return cal, nil
}

// event folds one events.jsonl line into the calibration; check is LoadCalibration's strict decoder.
func (cal *Calibration) event(e envelope, what string, check func(string, []byte, any) bool, windows *[]chaos.Window, epochs *[]time.Time) error {
	c := &cal.Collected
	switch e.Kind {
	case "g12":
		if !check(what, e.Data, &c.G12) {
			return nil
		}
		c.G12Checked = true
		if c.G12.Baseline != nil && c.G12.FD0 >= 0 {
			cal.HasBaseline = true
			c.Baseline = Baseline{Groups: c.G12.Baseline, Total: groupTotal(c.G12.Baseline), FDs: c.G12.FD0}
		}
	case "churn":
		var head struct {
			Step string `json:"step"`
		}
		if err := json.Unmarshal(e.Data, &head); err != nil || head.Step != "residue" {
			return nil
		}
		var d residueEvent
		if !check(what, e.Data, &d) {
			return nil
		}
		if d.Residue.Measured && (d.Residue.Before == nil || d.Residue.After == nil) {
			// A measured churn's goroutine maps are its measurement (Codex AM02).
			cal.Missing = append(cal.Missing, fmt.Sprintf("%s: churn %s is measured but has no before or after", what, d.Residue.Slot))
		}
		c.Churns = append(c.Churns, d.Residue)
	case "counters":
		var d countersEvent
		if check(what, e.Data, &d) {
			c.Logs = d.G7
		}
	case "oracles":
		var d oraclesEvent
		if check(what, e.Data, &d) {
			c.Register, c.LWT, c.Uncertain = d.Register, d.LWT.Violations, d.LWT.Uncertain
		}
	case "violation":
		var v string
		if check(what, e.Data, &v) {
			c.Runtime = append(c.Runtime, v)
		}
	case "window":
		var w chaos.Window
		if check(what, e.Data, &w) {
			*windows = append(*windows, w)
			*epochs = append(*epochs, epochOf(e))
		}
	case "report":
		check(what, e.Data, &c.Report)
	case "latency":
		var d latencyEvent
		if check(what, e.Data, &d) {
			c.WarmupP99, c.CooldownP99 = d.Warmup, d.Cooldown
			if d.Warmup == nil || d.Cooldown == nil {
				cal.Missing = append(cal.Missing, what+": a null p99 map")
			}
		}
	}
	return nil
}

// profileInventory checks what the sampler guarantees of a completed cell's profiles (Codex AO02, AP03):
// exactly one final profile, and each dump's total equal to its groups' sum.
// Periodic and recovered profiles are not counted: a timer due at shutdown, or a checkpoint 20 s after a late fault,
// is not guaranteed to run, so their absence is not evidence of loss.
func profileInventory(profiles []Profile) []string {
	var missing []string
	final := 0
	for _, p := range profiles {
		if p.Reason == ReasonFinal {
			final++
		}
		if p.Groups != nil && groupTotal(p.Groups) != p.Total {
			missing = append(missing, fmt.Sprintf("profile at t=%.0fs: total %d, its groups sum to %d", p.T, p.Total, groupTotal(p.Groups)))
		}
	}
	if final != 1 {
		missing = append(missing, fmt.Sprintf("%d final profiles, not 1", final))
	}
	return missing
}

// buildMissing checks build.json, which run-night.sh writes with jq: every identity the derivation binds must be named.
func buildMissing(path string) []string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return []string{err.Error()}
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return []string{"build.json: " + err.Error()}
	}
	var missing []string
	for _, k := range []string{"attempt", "cell", "mode", "kind", "soak_sha256", "driver_sha"} {
		if s, ok := m[k].(string); !ok || s == "" {
			missing = append(missing, "build.json: no "+k)
		}
	}
	if s, _ := m["soak_sha256"].(string); s != "" && !hexDigest.MatchString(s) {
		missing = append(missing, "build.json: soak_sha256 is not a sha256")
	}
	return missing
}

// Critical returns every threshold's critical value on this cell's data (PLAN §41.1).
// Each comes from the same inputs, built the same way, as the gate that reads the threshold.
// kF and kC are each read by two gates, so theirs is the larger of the two.
//
// Returns:
//   - map[string]gate.Critical: by threshold name; a value that is not OK had no inputs
func (c Collected) Critical() map[string]gate.Critical {
	s := c.Series()
	out := map[string]gate.Critical{}
	out[gate.KG], out[gate.KG0] = gate.G2Critical(c.g2Input(s))
	out[gate.KH], out[gate.KHr] = gate.G3Critical(s.Heap, s.Warm, s.End)
	kF4, kFs := gate.G4Critical(s.QuietFDs, float64(c.Baseline.FDs))
	out[gate.KFs] = kFs
	out[gate.KS], out[gate.KSs] = gate.G6Critical(s.QuietBalance)
	out[gate.KE] = gate.G7Critical(c.Logs)
	out[gate.KLWTu] = gate.G10Critical(c.Uncertain)
	var kC12, kF12 gate.Critical
	if c.G12Checked {
		kC12, kF12 = gate.G12Critical(c.G12)
	}
	kC13, kCh := gate.G13Critical(c.Churns)
	out[gate.KCh] = kCh
	out[gate.KF] = gate.MaxCritical(kF4, kF12)
	out[gate.KC] = gate.MaxCritical(kC12, kC13)
	if !c.cooldownContaminated() {
		out[gate.KL] = gate.G14Critical(c.WarmupP99, c.CooldownP99)
	} else {
		out[gate.KL] = gate.Critical{}
	}
	tp, gc := healthWithin(c.TP, c.GC, c.WorkloadSeconds)
	out[gate.KD], out[gate.KGp], out[gate.KGt] = gate.G16Critical(tp, gc, c.Windows)
	return out
}

// g2Input is what G2 evaluates, from the trend series.
func (c Collected) g2Input(s TrendSeries) gate.G2Input {
	return gate.G2Input{Groups: s.Groups, QuietTotals: s.QuietTotals, Base: float64(c.Baseline.Total), Hosts: clusterHosts, NumConns: cell.NumConns}
}

// CooldownContaminated reports whether a fault window reaches into the cool-down, which excludes G14.
//
// Returns:
//   - bool: true when G14 is not evaluated on this cell
func (c Collected) CooldownContaminated() bool {
	return c.cooldownContaminated()
}

// maxEpochSkew bounds how far an event's own epoch, time − t, may sit from the workload epoch: float rounding of t only.
const maxEpochSkew = time.Millisecond

// epochOf is the workload epoch an event implies: its wall clock minus its seconds since the epoch.
func epochOf(e envelope) time.Time {
	return e.Time.Add(-time.Duration(e.T * float64(time.Second)))
}

// matchWindows pairs every fault outcome with the window its lifecycle opened, by id and start (Codex AL02):
// RunLifecycle opens the window at the outcome's own start, so a lost window, whose readings would count as steady state,
// or a window without an outcome is found.
func matchWindows(outcomes []chaos.Outcome, windows []chaos.Window) []string {
	used := make([]bool, len(windows))
	var missing []string
	for _, o := range outcomes {
		i := slices.IndexFunc(windows, func(w chaos.Window) bool { return w.ID == o.ID && w.Start.Equal(o.Start) })
		for i >= 0 && used[i] {
			j := slices.IndexFunc(windows[i+1:], func(w chaos.Window) bool { return w.ID == o.ID && w.Start.Equal(o.Start) })
			if j < 0 {
				i = -1
				break
			}
			i += 1 + j
		}
		if i < 0 {
			missing = append(missing, fmt.Sprintf("outcome %s at %s has no window event", o.ID, o.Start.Format(time.RFC3339Nano)))
			continue
		}
		used[i] = true
		// Windows.Close ends a done fault's window exactly WindowTail after done, the outcome's end;
		// Windows.Fail clears the end of every window with the failed fault's id, earlier ones included (Codex AM01, AO04).
		switch w := windows[i]; {
		case w.Failed:
			if !w.End.IsZero() || !slices.ContainsFunc(outcomes, func(f chaos.Outcome) bool { return f.ID == w.ID && f.Final == chaos.StateFailed }) {
				missing = append(missing, fmt.Sprintf("window %s at %s is failed with end %s, but no fault %s failed or its end is set",
					w.ID, w.Start.Format(time.RFC3339Nano), w.End.Format(time.RFC3339Nano), w.ID))
			}
		case o.Final != chaos.StateDone:
			missing = append(missing, fmt.Sprintf("window %s at %s did not fail, but its fault ended %s", w.ID, w.Start.Format(time.RFC3339Nano), o.Final))
		case !w.End.Equal(o.End.Add(chaos.WindowTail)):
			missing = append(missing, fmt.Sprintf("window %s ends at %s, not %s after its fault ended at %s",
				w.ID, w.End.Format(time.RFC3339Nano), chaos.WindowTail, o.End.Format(time.RFC3339Nano)))
		}
	}
	for i, w := range windows {
		if !used[i] {
			missing = append(missing, fmt.Sprintf("window %s at %s has no outcome", w.ID, w.Start.Format(time.RFC3339Nano)))
		}
	}
	return missing
}

func groupTotal(groups map[string]int) int {
	n := 0
	for _, v := range groups {
		n += v
	}
	return n
}

func readJSON(path string, v any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	return nil
}

// eachLine calls fn for every non-empty line of a JSON-lines file.
func eachLine(path string, fn func([]byte) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	n := 0
	for sc.Scan() {
		n++
		if len(sc.Bytes()) == 0 {
			continue
		}
		if err := fn(sc.Bytes()); err != nil {
			return fmt.Errorf("%s line %d: %w", path, n, err)
		}
	}
	if err := sc.Err(); err != nil {
		return errors.Join(fmt.Errorf("read %s", path), err)
	}
	return nil
}
