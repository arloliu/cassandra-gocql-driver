package derive

import (
	"fmt"
	"slices"
	"strings"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cellrun"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
)

// Report is derivation.json (PLAN §41.9): what was derived, how, and every reason to refuse.
type Report struct {
	// Summary is the night summary's path; Night its runner token.
	Summary string `json:"summary"`
	Night   string `json:"night"`
	// Cells are the execution directories used, in summary order.
	Cells []CellRef `json:"cells"`
	// Refused is true when gates.json was not written; Problems says why.
	Refused  bool     `json:"refused"`
	Problems []string `json:"problems"`
	// Accepted, AcceptedZero and Provenance are the maintainer's written acceptances.
	Accepted     []Accept          `json:"accepted,omitempty"`
	AcceptedZero map[string]string `json:"accepted_zero,omitempty"`
	Provenance   string            `json:"provenance,omitempty"`
	// Thresholds is each threshold's derivation, in PLAN §41.1's order.
	Thresholds []Derived `json:"thresholds"`
}

// CellRef names one cell's execution.
type CellRef struct {
	ID      string `json:"id"`
	Attempt string `json:"attempt"`
	ExecDir string `json:"exec_dir"`
}

// Run loads a calibration night from its summary and derives gates.json (PLAN §41).
//
// Parameters:
//   - summaryPath: the night summary, recorded in the report
//   - s: the parsed summary
//   - o: the maintainer's inputs
//
// Returns:
//   - gate.Thresholds: the derived thresholds; nil when refused
//   - Report: the derivation report, written in every case
func Run(summaryPath string, s Summary, o Options) (gate.Thresholds, Report) {
	var cells []Cell
	var loadProblems []string
	for _, sc := range s.Cells {
		cal, err := cellrun.LoadCalibration(sc.Receipt.ExecDir)
		if err != nil {
			loadProblems = append(loadProblems, fmt.Sprintf("%s: load %s: %v", sc.Cell, sc.Receipt.ExecDir, err))
			continue
		}
		cells = append(cells, Cell{ID: sc.Cell, Cal: cal})
	}
	if len(loadProblems) > 0 {
		r := newReport(summaryPath, s, o)
		r.Refused, r.Problems = true, loadProblems
		return nil, r
	}
	return Evaluate(summaryPath, s, cells, o)
}

// Evaluate derives gates.json from loaded cells: suitability, observation, the rule and the self-check (PLAN §41).
//
// Parameters:
//   - summaryPath: the night summary's path, recorded in the report
//   - s: the parsed summary
//   - cells: the loaded cells, in summary order
//   - o: the maintainer's inputs
//
// Returns:
//   - gate.Thresholds: the derived thresholds; nil when refused
//   - Report: the derivation report
func Evaluate(summaryPath string, s Summary, cells []Cell, o Options) (gate.Thresholds, Report) {
	r := newReport(summaryPath, s, o)
	problems := Suitability(s, cells, o)
	obs := map[string]map[string]Observation{}
	var ids []string
	for _, c := range cells {
		o, p := Observe(c)
		obs[c.ID] = o
		ids = append(ids, c.ID)
		problems = append(problems, p...)
	}
	th, derived, p := Combine(ids, obs, o.AcceptZero)
	r.Thresholds = derived
	problems = append(problems, p...)
	if len(problems) == 0 {
		problems = SelfCheck(cells, th)
	}
	r.Problems = problems
	if len(problems) > 0 {
		r.Refused = true
		return nil, r
	}
	return th, r
}

func newReport(summaryPath string, s Summary, o Options) Report {
	r := Report{Summary: summaryPath, Night: s.RunnerToken, Accepted: o.Accept, AcceptedZero: o.AcceptZero, Provenance: o.AcceptProvenance, Problems: []string{}}
	for _, sc := range s.Cells {
		r.Cells = append(r.Cells, CellRef{ID: sc.Cell, Attempt: sc.Receipt.Attempt, ExecDir: sc.Receipt.ExecDir})
	}
	return r
}

// Markdown renders the report as derivation.md.
//
// Returns:
//   - string: the Markdown text
func (r Report) Markdown() string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Threshold derivation (PLAN §41)\n\nNight summary `%s`, runner token `%s`.\n\n", r.Summary, r.Night)
	if r.Refused {
		b.WriteString("**Refused: no gates.json was written.**\n\n")
		for _, p := range r.Problems {
			fmt.Fprintf(&b, "- %s\n", p)
		}
		b.WriteString("\n")
	} else {
		b.WriteString("gates.json was written.\n\n")
	}
	b.WriteString("| cell | attempt | execution directory |\n|---|---|---|\n")
	for _, c := range r.Cells {
		fmt.Fprintf(&b, "| %s | %s | `%s` |\n", c.ID, c.Attempt, c.ExecDir)
	}
	b.WriteString("\n| k | rule | per cell (whole cell) | windows used / skipped | p95 | observed | floor | value | notes |\n|---|---|---|---|---|---|---|---|---|\n")
	for _, d := range r.Thresholds {
		var cells, windows []string
		for _, c := range r.Cells {
			v := "none"
			if p, ok := d.PerCell[c.ID]; ok && p != nil {
				v = fmt.Sprintf("%.6g", *p)
			} else if slices.Contains(d.Excluded, c.ID) {
				v = "excluded"
			}
			cells = append(cells, c.ID+" "+v)
			if d.WindowsUsed != nil {
				windows = append(windows, fmt.Sprintf("%d/%d", d.WindowsUsed[c.ID], d.WindowsSkipped[c.ID]))
			}
		}
		p95 := ""
		if d.P95 != nil {
			p95 = fmt.Sprintf("%.6g", *d.P95)
		}
		var notes []string
		if d.FloorApplied {
			notes = append(notes, "floor applied")
		}
		if d.Degenerate {
			notes = append(notes, "degenerate; accepted: "+d.AcceptedZero)
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %.6g | %.6g | %.6g | %s |\n",
			d.Name, d.Rule, strings.Join(cells, "<br>"), strings.Join(windows, " "), p95, d.Observed, d.Floor, d.Value, strings.Join(notes, "; "))
	}
	if len(r.Accepted) > 0 || r.Provenance != "" {
		b.WriteString("\n## Accepted by the maintainer\n\n")
		for _, a := range r.Accepted {
			fmt.Fprintf(&b, "- %s %s: %s\n", a.Cell, a.Gate, a.Reason)
		}
		if r.Provenance != "" {
			fmt.Fprintf(&b, "- provenance: %s\n", r.Provenance)
		}
	}
	return b.String()
}
