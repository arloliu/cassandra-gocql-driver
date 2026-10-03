package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/artifact"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/derive"
)

// repeated collects a repeatable flag's values.
type repeated []string

func (r *repeated) String() string     { return strings.Join(*r, ", ") }
func (r *repeated) Set(v string) error { *r = append(*r, v); return nil }

// sourceUnchanged reports whether the tree outside soak/ is the same at a driver commit and at HEAD (PLAN §41.5 item 5).
// It is a variable so the command's tests can run without the repository's history.
var sourceUnchanged = func(repo, sha string) (bool, error) {
	err := exec.Command("git", "-C", repo, "diff", "--quiet", sha, "HEAD", "--", ".", ":!soak").Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return true, nil
	case errors.As(err, &exit) && exit.ExitCode() == 1:
		return false, nil
	default:
		return false, err
	}
}

// pathsChanged lists the paths under soak/ that differ between two driver commits (PLAN v7.16 §55.3 item 4).
// --no-renames lists both sides of a rename, so a file moved into a derivation-only path still names its origin.
// It is a variable so the command's tests can run without the repository's history.
var pathsChanged = func(repo, from, to string) ([]string, error) {
	out, err := exec.Command("git", "-C", repo, "diff", "--no-renames", "--name-only", "-z", from, to, "--", "soak/").Output()
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, p := range strings.Split(string(out), "\x00") {
		if p != "" {
			paths = append(paths, p)
		}
	}
	return paths, nil
}

// parseExclude parses -exclude-attempt's <batch dir>/<slot>-<n>:<reason>.
func parseExclude(v string) (derive.ExcludeAttempt, bool) {
	target, reason, ok := strings.Cut(v, ":")
	if !ok || strings.TrimSpace(reason) == "" {
		return derive.ExcludeAttempt{}, false
	}
	base := filepath.Base(target)
	i := strings.LastIndex(base, "-")
	if i <= 0 {
		return derive.ExcludeAttempt{}, false
	}
	n, err := strconv.Atoi(base[i+1:])
	if err != nil || n < 1 {
		return derive.ExcludeAttempt{}, false
	}
	return derive.ExcludeAttempt{Batch: filepath.Clean(filepath.Dir(target)), Slot: base[:i], N: n, Reason: strings.TrimSpace(reason)}, true
}

// reasons parses repeated <key>:<reason> values into a map; ok is false on a malformed or repeated key.
func reasons(values []string) (map[string]string, bool) {
	m := map[string]string{}
	for _, v := range values {
		k, reason, ok := strings.Cut(v, ":")
		if _, dup := m[k]; !ok || k == "" || strings.TrimSpace(reason) == "" || dup {
			return nil, false
		}
		m[k] = strings.TrimSpace(reason)
	}
	return m, true
}

// deriveMain derives gates.json from a calibration night's summary (PLAN §41).
// Exit status: 0 written, 1 refused, 2 bad usage or an unreadable summary.
func deriveMain(args []string) int {
	fs := flag.NewFlagSet("derive", flag.ContinueOnError)
	summary := fs.String("summary", "", "the calibration night's summary, night-<start>-<token>.json")
	out := fs.String("out", "gates.json", "the gates.json to write; derivation.json and derivation.md go beside it")
	repo := fs.String("repo", "..", "the driver repository, for the source check")
	var accepts, zeros, raises, batches, excludes, acceptBatches, acceptCompats repeated
	fs.Var(&accepts, "accept", "cell:gate:reason — admit a failing non-calibrated gate (repeatable)")
	fs.Var(&zeros, "accept-zero", "k:reason — admit a degenerate threshold of 0 (repeatable)")
	provenance := fs.String("accept-provenance", "", `reason — admit builds whose source_clean is not "true"`)
	fs.Var(&raises, "raise", "k=value:reason — raise a threshold to at least value after the rule and floors (repeatable)")
	fs.Var(&batches, "validation", "batch dir — a validation batch whose attempts add to kL's evidence (repeatable, PLAN v7.16 §55.3)")
	fs.Var(&excludes, "exclude-attempt", "batch dir/slot-n:reason — exclude one attempt for recorded host contention (repeatable)")
	fs.Var(&acceptBatches, "accept-batch", "batch dir:reason — admit a terminal batch (repeatable)")
	fs.Var(&acceptCompats, "accept-compat", "path:reason — admit a changed soak/ path that is not derivation-only (repeatable)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *summary == "" || fs.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "usage: soak derive -summary <night json> [-out gates.json] [-accept cell:gate:reason]… [-accept-zero k:reason]… [-accept-provenance reason] [-raise k=value:reason]…"+
			" [-validation <batch dir>]… [-exclude-attempt <batch dir>/<slot>-<n>:reason]… [-accept-batch <batch dir>:reason]… [-accept-compat <path>:reason]…")
		return 2
	}
	o := derive.Options{AcceptZero: map[string]string{}, AcceptProvenance: strings.TrimSpace(*provenance)}
	for _, a := range accepts {
		parts := strings.SplitN(a, ":", 3)
		if len(parts) != 3 || strings.TrimSpace(parts[2]) == "" {
			fmt.Fprintf(os.Stderr, "-accept %q: want cell:gate:reason\n", a)
			return 2
		}
		o.Accept = append(o.Accept, derive.Accept{Cell: parts[0], Gate: parts[1], Reason: strings.TrimSpace(parts[2])})
	}
	for _, z := range zeros {
		k, reason, ok := strings.Cut(z, ":")
		if !ok || strings.TrimSpace(reason) == "" {
			fmt.Fprintf(os.Stderr, "-accept-zero %q: want k:reason\n", z)
			return 2
		}
		o.AcceptZero[k] = strings.TrimSpace(reason)
	}
	for _, r := range raises {
		k, rest, okK := strings.Cut(r, "=")
		num, reason, okR := strings.Cut(rest, ":")
		v, err := strconv.ParseFloat(num, 64)
		if !okK || !okR || err != nil || strings.TrimSpace(reason) == "" {
			fmt.Fprintf(os.Stderr, "-raise %q: want k=value:reason\n", r)
			return 2
		}
		o.Raise = append(o.Raise, derive.Raise{Name: k, Value: v, Reason: strings.TrimSpace(reason)})
	}
	o.SourceUnchanged = func(sha string) (bool, error) { return sourceUnchanged(*repo, sha) }
	o.PathsChanged = func(from, to string) ([]string, error) { return pathsChanged(*repo, from, to) }
	// Batches are compared by their canonical paths, the identity derive itself uses (Codex BO02).
	given := map[string]bool{}
	canonical := func(flag, dir string) (string, bool) {
		c, err := derive.CanonicalBatch(dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s %s: %v\n", flag, dir, err)
			return "", false
		}
		return c, true
	}
	for _, b := range batches {
		c, err := derive.CanonicalBatch(b)
		if err != nil {
			// An unresolvable batch is derive's refusal, with a report, not a usage error (Codex BP02).
			o.Validation = append(o.Validation, b)
			continue
		}
		if given[c] {
			fmt.Fprintf(os.Stderr, "-validation %s: the batch %s is given twice\n", b, c)
			return 2
		}
		given[c] = true
		o.Validation = append(o.Validation, c)
	}
	accepted, ok := reasons(acceptBatches)
	if !ok {
		fmt.Fprintln(os.Stderr, "-accept-batch: want <batch dir>:reason, each batch once")
		return 2
	}
	o.AcceptBatch = map[string]string{}
	for b, reason := range accepted {
		c, ok := canonical("-accept-batch", b)
		if !ok {
			return 2
		}
		if _, dup := o.AcceptBatch[c]; dup {
			fmt.Fprintln(os.Stderr, "-accept-batch: want <batch dir>:reason, each batch once")
			return 2
		}
		o.AcceptBatch[c] = reason
	}
	if o.AcceptCompat, ok = reasons(acceptCompats); !ok {
		fmt.Fprintln(os.Stderr, "-accept-compat: want <path>:reason, each path once")
		return 2
	}
	for b := range o.AcceptBatch {
		if !given[b] {
			fmt.Fprintf(os.Stderr, "-accept-batch %s: not a -validation batch\n", b)
			return 2
		}
	}
	for _, v := range excludes {
		e, ok := parseExclude(v)
		if !ok {
			fmt.Fprintf(os.Stderr, "-exclude-attempt %q: want <batch dir>/<slot>-<n>:reason\n", v)
			return 2
		}
		c, ok := canonical("-exclude-attempt", e.Batch)
		if !ok {
			return 2
		}
		if !given[c] {
			fmt.Fprintf(os.Stderr, "-exclude-attempt %q: %s is not a -validation batch\n", v, e.Batch)
			return 2
		}
		e.Batch = c
		l, err := derive.ReadLedger(e.Batch)
		if err != nil || !l.HasAttempt(e.Slot, e.N) {
			fmt.Fprintf(os.Stderr, "-exclude-attempt %q: the batch has no attempt %d of slot %s\n", v, e.N, e.Slot)
			return 2
		}
		o.Exclude = append(o.Exclude, e)
	}

	raw, err := os.ReadFile(*summary)
	if err != nil {
		fmt.Fprintln(os.Stderr, "derive:", err)
		return 2
	}
	var s derive.Summary
	if err := json.Unmarshal(raw, &s); err != nil {
		fmt.Fprintf(os.Stderr, "derive: parse %s: %v\n", *summary, err)
		return 2
	}
	th, report := derive.Run(*summary, s, o)

	dir := filepath.Dir(*out)
	if err := artifact.WriteJSON(filepath.Join(dir, "derivation.json"), report); err != nil {
		fmt.Fprintln(os.Stderr, "derive:", err)
		return 2
	}
	if err := os.WriteFile(filepath.Join(dir, "derivation.md"), []byte(report.Markdown()), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "derive:", err)
		return 2
	}
	if report.Refused {
		fmt.Fprintf(os.Stderr, "derive: refused, no gates.json written (%d problems; see %s)\n", len(report.Problems), filepath.Join(dir, "derivation.md"))
		for _, p := range report.Problems {
			fmt.Fprintln(os.Stderr, "  -", p)
		}
		return 1
	}
	if err := artifact.WriteJSON(*out, th); err != nil {
		fmt.Fprintln(os.Stderr, "derive:", err)
		return 2
	}
	fmt.Printf("wrote %s, derivation.json and derivation.md\n", *out)
	return 0
}
