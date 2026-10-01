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

// deriveMain derives gates.json from a calibration night's summary (PLAN §41).
// Exit status: 0 written, 1 refused, 2 bad usage or an unreadable summary.
func deriveMain(args []string) int {
	fs := flag.NewFlagSet("derive", flag.ContinueOnError)
	summary := fs.String("summary", "", "the calibration night's summary, night-<start>-<token>.json")
	out := fs.String("out", "gates.json", "the gates.json to write; derivation.json and derivation.md go beside it")
	repo := fs.String("repo", "..", "the driver repository, for the source check")
	var accepts, zeros, raises repeated
	fs.Var(&accepts, "accept", "cell:gate:reason — admit a failing non-calibrated gate (repeatable)")
	fs.Var(&zeros, "accept-zero", "k:reason — admit a degenerate threshold of 0 (repeatable)")
	provenance := fs.String("accept-provenance", "", `reason — admit builds whose source_clean is not "true"`)
	fs.Var(&raises, "raise", "k=value:reason — raise a threshold to at least value after the rule and floors (repeatable)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *summary == "" || fs.NArg() > 0 {
		fmt.Fprintln(os.Stderr, "usage: soak derive -summary <night json> [-out gates.json] [-accept cell:gate:reason]… [-accept-zero k:reason]… [-accept-provenance reason] [-raise k=value:reason]…")
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
