package derive

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cell"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cellrun"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/config"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
)

var hexDigest = regexp.MustCompile(`^[0-9a-f]{64}$`)

// MinWorkloadSeconds is the workload a calibration cell must have run (PLAN §41.5 item 3).
const MinWorkloadSeconds = 7200.0

// nonCalibratedGates are the gates whose recorded results must pass (PLAN §41.5 item 4).
var nonCalibratedGates = []string{"G0", "G1", "G5", "G8", "G9", "G11", "G15"}

// Summary is the part of a night summary, night-<start>-<token>.json (PLAN §25.3), that §41.5 reads.
type Summary struct {
	Mode           string        `json:"mode"`
	Kind           string        `json:"kind"`
	State          string        `json:"state"`
	RunnerToken    string        `json:"runner_token"`
	RequestedCells []string      `json:"requested_cells"`
	Cells          []SummaryCell `json:"cells"`
}

// SummaryCell is one cell of a night summary.
type SummaryCell struct {
	Cell       string  `json:"cell"`
	Completion string  `json:"completion"`
	Receipt    Receipt `json:"receipt"`
	// Report is the runner's validated cleanup report; the runner writes null when it could not validate it.
	Report *CleanupReport `json:"report"`
	// Problems are the runner's own problems with this cell, e.g. an unreadable report; nil when the list is absent,
	// which is not a clean result (Codex AN08).
	Problems *[]string `json:"problems"`
}

// CleanupReport is the part of a cell's validated cleanup report that §41.5 reads.
type CleanupReport struct {
	// Unresolved is nil when the list was absent, which is not a clean result.
	Unresolved *[]any `json:"unresolved"`
}

// Receipt is a cell's launcher receipt, as the summary carries it.
type Receipt struct {
	State        string `json:"state"`
	Cell         string `json:"cell"`
	Mode         string `json:"mode"`
	Kind         string `json:"kind"`
	Attempt      string `json:"attempt"`
	ExecDir      string `json:"exec_dir"`
	Digest       string `json:"digest"`
	LauncherExit *int   `json:"launcher_exit"`
	Proven       bool   `json:"proven"`
}

// Accept is the maintainer's acceptance of one failing non-calibrated gate on one cell.
type Accept struct {
	Cell   string `json:"cell"`
	Gate   string `json:"gate"`
	Reason string `json:"reason"`
}

// Options are the maintainer's inputs.
type Options struct {
	// Accept admits failing non-calibrated gates (§41.5 item 4).
	Accept []Accept
	// AcceptZero admits degenerate thresholds, by name (§41.3).
	AcceptZero map[string]string
	// AcceptProvenance admits a build whose source_clean is not the string "true" (§41.5 item 5, PLAN §51.4).
	AcceptProvenance string
	// Raise sets minimums applied after the rule and the floors (PLAN v7.14 §48.5).
	Raise []Raise
	// SourceUnchanged reports whether the tree outside soak/ is unchanged between a driver commit and HEAD.
	SourceUnchanged func(driverSHA string) (bool, error)
}

// Suitability checks PLAN §41.5 and the loader's own consistency checks (§41.2).
//
// Parameters:
//   - s: the night summary
//   - cells: the cells loaded from the summary's receipts, in summary order
//   - o: the maintainer's inputs
//
// Returns:
//   - []string: every reason to refuse; empty when the night is suitable
func Suitability(s Summary, cells []Cell, o Options) []string {
	var p []string
	add := func(format string, args ...any) { p = append(p, fmt.Sprintf(format, args...)) }

	if s.Mode != "night" || s.Kind != "calib" {
		add("the night ran -mode %s -kind %s, not -mode night -kind calib", s.Mode, s.Kind)
	}
	var want []string
	for _, c := range config.Cells() {
		want = append(want, c.ID)
	}
	var got []string
	for _, c := range s.Cells {
		got = append(got, c.Cell)
	}
	if sorted := slices.Sorted(slices.Values(got)); !slices.Equal(sorted, want) {
		add("the night's cells are %v, not exactly %v", got, want)
	}

	for i, sc := range s.Cells {
		r := sc.Receipt
		if sc.Completion != "done" {
			add("%s: completion %q, not done", sc.Cell, sc.Completion)
		}
		if r.State != "done" || !r.Proven {
			add("%s: receipt state %q, proven %v", sc.Cell, r.State, r.Proven)
		}
		if r.Cell != sc.Cell || r.Mode != "night" || r.Kind != "calib" {
			add("%s: receipt is for %s -mode %s -kind %s", sc.Cell, r.Cell, r.Mode, r.Kind)
		}
		if r.LauncherExit == nil || (*r.LauncherExit != 0 && *r.LauncherExit != 1) {
			add("%s: launcher exit %v, not 0 or 1", sc.Cell, r.LauncherExit)
		}
		switch {
		case sc.Report == nil || sc.Report.Unresolved == nil:
			add("%s: no validated cleanup report with an unresolved list (Codex AK02)", sc.Cell)
		case len(*sc.Report.Unresolved) > 0:
			add("%s: the cleanup report has unresolved resources: %v", sc.Cell, *sc.Report.Unresolved)
		}
		switch {
		case sc.Problems == nil:
			add("%s: the summary has no problems list for the cell", sc.Cell)
		case len(*sc.Problems) > 0:
			add("%s: the runner recorded problems: %v", sc.Cell, *sc.Problems)
		}
		// Equal identities prove nothing when they are empty (Codex AN07).
		if r.Attempt == "" {
			add("%s: the receipt names no attempt", sc.Cell)
		}
		if r.ExecDir == "" {
			add("%s: the receipt names no execution directory", sc.Cell)
		}
		if !hexDigest.MatchString(r.Digest) {
			add("%s: the receipt's digest %q is not a sha256", sc.Cell, r.Digest)
		}
		if i >= len(cells) {
			continue
		}
		cal := cells[i].Cal
		b := cal.Build
		if b.Attempt != r.Attempt || b.SoakSHA256 != r.Digest || b.Cell != sc.Cell {
			add("%s: build.json (attempt %s, cell %s, digest %s) does not match the receipt (attempt %s, digest %s)",
				sc.Cell, b.Attempt, b.Cell, b.SoakSHA256, r.Attempt, r.Digest)
		}
		p = append(p, cellChecks(sc.Cell, cal, o.Accept)...)
	}
	for _, a := range o.Accept {
		if !slices.Contains(got, a.Cell) || !slices.Contains(nonCalibratedGates, a.Gate) {
			add("-accept %s:%s names no cell of the night or no non-calibrated gate", a.Cell, a.Gate)
		}
	}
	p = append(p, provenance(cells, o)...)
	return p
}

// cellChecks are §41.5 items 3, 4, 6, 7 and 8, and the loader's consistency checks, for one cell.
func cellChecks(id string, cal cellrun.Calibration, accepts []Accept) []string {
	var p []string
	add := func(format string, args ...any) { p = append(p, fmt.Sprintf(id+": "+format, args...)) }
	v, c := cal.Verdict, cal.Collected
	for _, m := range cal.Missing {
		add("%s", m)
	}
	if v.Cell != id {
		add("verdict.json is for cell %q", v.Cell)
	}
	if !v.Final {
		add("verdict.json is not final")
	}
	if !v.Evidence.Complete {
		add("evidence is incomplete: %v", v.Evidence.Problems)
	}
	if v.WorkloadSeconds < MinWorkloadSeconds {
		add("the workload ran %.0f s, less than %.0f s", v.WorkloadSeconds, MinWorkloadSeconds)
	}
	recorded := map[string]gate.Result{}
	for _, r := range v.Gates {
		recorded[r.Gate] = r
	}
	for _, g := range nonCalibratedGates {
		r, ok := recorded[g]
		if ok && r.Status == gate.StatusPass {
			continue
		}
		if slices.ContainsFunc(accepts, func(a Accept) bool { return a.Cell == id && a.Gate == g }) {
			continue
		}
		add("%s is %s in verdict.json and not accepted: %v", g, r.Status, r.Details)
	}
	if c.Report.FaultsStopped != "" {
		add("faults stopped after %s", c.Report.FaultsStopped)
	}
	if c.Report.MandatoryExecuted != c.Report.MandatoryFaults {
		add("mandatory faults executed %d of %d", c.Report.MandatoryExecuted, c.Report.MandatoryFaults)
	}
	if !cal.HasBaseline {
		add("no g12 event with baseline₀")
	}
	if q := c.Series().Quiet; q < cellrun.MinQuietCheckpoints {
		add("%d quiet trend checkpoints, need %d", q, cellrun.MinQuietCheckpoints)
	}
	if cal.NumConns != cell.NumConns {
		add("config.json num_conns %d, the cell definition has %d", cal.NumConns, cell.NumConns)
	}
	if cal.NodeCount != len(c.Nodes) {
		add("config.json nodes %d, the harness names %d", cal.NodeCount, len(c.Nodes))
	}
	if len(c.Churns) != c.Report.ChurnExecuted {
		add("%d churn residues for %d executed churns", len(c.Churns), c.Report.ChurnExecuted)
	}
	// A late observation is missing from samples.jsonl, so the cell's persisted latency is incomplete; no flag accepts it (PLAN §51.2).
	for _, class := range slices.Sorted(maps.Keys(cal.Late)) {
		if n := cal.Late[class]; n > 0 {
			add("%d late latency observations of class %s: samples.jsonl does not hold them", n, class)
		}
	}
	classes := slices.Sorted(slices.Values(c.Classes))
	if !slices.Equal(classes, slices.Sorted(maps.Keys(c.WarmupP99))) || !slices.Equal(classes, slices.Sorted(maps.Keys(c.CooldownP99))) {
		add("the mix's classes %v differ from the latency event's", classes)
	}
	return p
}

// provenance is §41.5 item 5.
func provenance(cells []Cell, o Options) []string {
	var p []string
	shas, digests := map[string]bool{}, map[string]bool{}
	dirty := false
	for _, c := range cells {
		shas[c.Cal.Build.DriverSHA], digests[c.Cal.Build.SoakSHA256] = true, true
		if s, ok := c.Cal.Build.SourceClean.(string); !ok || s != "true" {
			dirty = true
		}
	}
	if len(digests) != 1 {
		p = append(p, fmt.Sprintf("the cells ran different binaries: %v", slices.Sorted(maps.Keys(digests))))
	}
	if len(shas) != 1 {
		p = append(p, fmt.Sprintf("the cells recorded different driver commits: %v", slices.Sorted(maps.Keys(shas))))
	}
	for sha := range shas {
		switch {
		case sha == "":
			p = append(p, "a cell recorded no driver commit")
		case o.SourceUnchanged == nil:
			p = append(p, "no source check was given")
		default:
			same, err := o.SourceUnchanged(sha)
			if err != nil {
				p = append(p, fmt.Sprintf("the source check against %s failed: %v", sha, err))
			} else if !same {
				p = append(p, fmt.Sprintf("the tree outside soak/ changed between %s and HEAD", sha))
			}
		}
	}
	if dirty && strings.TrimSpace(o.AcceptProvenance) == "" {
		p = append(p, `a cell's source_clean is not the string "true", and -accept-provenance was not given`)
	}
	return p
}
