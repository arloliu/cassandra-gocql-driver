package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// git runs git in dir with a fixed identity.
func git(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false"}, args...)...)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
	return strings.TrimSpace(string(out))
}

// A file moved from a measurement path into a derivation-only one still names its origin (PLAN v7.16 §55.3, Codex BM02).
func TestPathsChangedListsBothSidesOfARename(t *testing.T) {
	repo := t.TempDir()
	git(t, repo, "init", "-q")
	probe := filepath.Join(repo, "soak", "internal", "probe")
	require.NoError(t, os.MkdirAll(probe, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(probe, "moved.go"), []byte("package probe\n\n// A file long enough for git to see the move as a rename.\nvar x = 1\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(repo, "outside.go"), []byte("package x\n"), 0o600))
	git(t, repo, "add", ".")
	git(t, repo, "commit", "-q", "-m", "a")
	from := git(t, repo, "rev-parse", "HEAD")
	require.NoError(t, os.MkdirAll(filepath.Join(repo, "soak", "internal", "derive"), 0o755))
	git(t, repo, "mv", "soak/internal/probe/moved.go", "soak/internal/derive/moved.go")
	require.NoError(t, os.WriteFile(filepath.Join(repo, "outside.go"), []byte("package x\n\nvar y = 2\n"), 0o600))
	git(t, repo, "commit", "-q", "-am", "b")
	to := git(t, repo, "rev-parse", "HEAD")
	require.Contains(t, git(t, repo, "diff", "--name-status", "-M", from, to), "R100", "the fixture is a rename to git's default diff")

	paths, err := pathsChanged(repo, from, to)
	require.NoError(t, err)
	require.Equal(t, []string{"soak/internal/derive/moved.go", "soak/internal/probe/moved.go"}, paths, "outside soak/ is not listed")
}

// writeLedger writes a minimal finished batch with one control attempt.
func writeLedger(t *testing.T) string {
	t.Helper()
	b := t.TempDir()
	raw, err := json.Marshal(map[string]any{"version": 1, "batch": map[string]any{"terminal": nil}, "slots": []any{
		map[string]any{"slot": "control-open", "canary": "", "rerun_owed": false, "effective": map[string]any{"result": "validated"},
			"attempts": []any{map[string]any{"n": 1, "state": "resolved", "exec_dir": "x"}}},
	}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(b, "validation-state.json"), raw, 0o600))
	return b
}

// A malformed validation flag is a usage error before anything is derived or written.
func TestDeriveCommandValidationUsage(t *testing.T) {
	withSourceCheck(t, true)
	summary := nightDirs(t)
	b := writeLedger(t)
	other := t.TempDir()
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"exclude without reason":   {[]string{"-validation", b, "-exclude-attempt", b + "/control-open-1"}, "-exclude-attempt"},
		"exclude without attempt":  {[]string{"-validation", b, "-exclude-attempt", b + "/control-open:r"}, "-exclude-attempt"},
		"exclude of another batch": {[]string{"-validation", b, "-exclude-attempt", other + "/control-open-1:r"}, "not a -validation batch"},
		"exclude of no attempt":    {[]string{"-validation", b, "-exclude-attempt", b + "/control-open-2:r"}, "no attempt 2"},
		"accept another batch":     {[]string{"-validation", b, "-accept-batch", other + ":r"}, "not a -validation batch"},
		"accept batch twice":       {[]string{"-validation", b, "-accept-batch", b + ":r", "-accept-batch", b + "/:s"}, "each batch once"},
		"accept compat no reason":  {[]string{"-validation", b, "-accept-compat", "soak/run-night.sh: "}, "-accept-compat"},
	} {
		t.Run(name, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "gates.json")
			var code int
			stderr := captureStderr(t, func() {
				code = deriveMain(append([]string{"-summary", summary, "-out", out, "-accept-zero", "kD:r"}, tc.args...))
			})
			require.Equal(t, 2, code)
			require.Contains(t, stderr, tc.want)
			require.NoFileExists(t, filepath.Join(filepath.Dir(out), "derivation.json"))
		})
	}

	got, ok := parseExclude("/x/batch/control-close-12:host contention: fuzz job")
	require.True(t, ok)
	require.Equal(t, "/x/batch", got.Batch)
	require.Equal(t, "control-close", got.Slot)
	require.Equal(t, 12, got.N)
	require.Equal(t, "host contention: fuzz job", got.Reason)
}

// The §55.4 live check, artifact-only: a fixture night with a real validation batch as -validation.
// It runs only when SOAK_LIVE_VALIDATION_BATCH names a batch directory built on a late-carrying commit.
func TestDeriveCommandLiveValidationBatch(t *testing.T) {
	batch := os.Getenv("SOAK_LIVE_VALIDATION_BATCH")
	if batch == "" {
		t.Skip("SOAK_LIVE_VALIDATION_BATCH is not set")
	}
	builds, err := filepath.Glob(filepath.Join(batch, "attempts", "control-open-*", "*", "c50p5", "*", "build.json"))
	require.NoError(t, err)
	require.NotEmpty(t, builds)
	var b struct {
		DriverSHA string `json:"driver_sha"`
	}
	raw, err := os.ReadFile(builds[0])
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &b))

	withSourceCheck(t, true)
	summary := nightDirs(t)
	for _, id := range []string{"c41p4", "c41p5", "c50p4", "c50p5"} {
		editJSON(t, filepath.Join(filepath.Dir(summary), id, "build.json"), func(m map[string]any) { m["driver_sha"] = b.DriverSHA })
	}
	out := filepath.Join(t.TempDir(), "gates.json")
	require.Equal(t, 0, deriveMain([]string{"-summary", summary, "-out", out, "-repo", "../../..", "-accept-zero", "kD:r", "-validation", batch}))
	md, err := os.ReadFile(filepath.Join(filepath.Dir(out), "derivation.md"))
	require.NoError(t, err)
	t.Log("\n" + string(md))
	var r struct {
		Thresholds []struct {
			Name    string  `json:"name"`
			Value   float64 `json:"value"`
			Sources []struct {
				Kind string `json:"kind"`
			} `json:"sources"`
		} `json:"thresholds"`
		Validation struct {
			Attempts []struct {
				Slot    string   `json:"slot"`
				Outcome string   `json:"outcome"`
				OKL     *float64 `json:"o_kl"`
			} `json:"attempts"`
		} `json:"validation"`
	}
	raw, err = os.ReadFile(filepath.Join(filepath.Dir(out), "derivation.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &r))
	got := map[string]string{}
	for _, a := range r.Validation.Attempts {
		got[a.Slot] = a.Outcome
		if a.Outcome == "eligible" {
			require.NotNil(t, a.OKL, a.Slot)
			require.InDelta(t, 0.0824, *a.OKL, 0.0001, a.Slot)
		}
	}
	require.Equal(t, map[string]string{"control-open": "eligible", "k12": "excluded", "control-close": "eligible"}, got)
	for _, d := range r.Thresholds {
		if d.Name == "kL" {
			require.InDelta(t, 0.48674869540553, d.Value, 0)
			require.Equal(t, "floor", d.Sources[0].Kind)
		}
	}
}

// A batch that cannot be resolved is a refusal with a report, not a usage error (Codex BP02).
func TestDeriveCommandUnresolvableBatchRefuses(t *testing.T) {
	withSourceCheck(t, true)
	summary := nightDirs(t)
	dangling := filepath.Join(t.TempDir(), "dangling")
	require.NoError(t, os.Symlink(filepath.Join(t.TempDir(), "gone"), dangling))
	for name, batch := range map[string]string{"nonexistent": filepath.Join(t.TempDir(), "missing"), "dangling symlink": dangling} {
		t.Run(name, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "gates.json")
			require.Equal(t, 1, deriveMain([]string{"-summary", summary, "-out", out, "-accept-zero", "kD:r", "-validation", batch}))
			require.NoFileExists(t, out)
			var r struct {
				Refused  bool     `json:"refused"`
				Problems []string `json:"problems"`
			}
			raw, err := os.ReadFile(filepath.Join(filepath.Dir(out), "derivation.json"))
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(raw, &r))
			require.True(t, r.Refused)
			require.True(t, strings.Contains(strings.Join(r.Problems, "\n"), batch), "%v", r.Problems)
		})
	}
}
