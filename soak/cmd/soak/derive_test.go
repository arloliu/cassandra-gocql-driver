package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
)

const calibrationFixture = "../../testdata/calibration/c50p5"

// nightDirs writes four calibration cells, copies of the fixture, whose latency is one 1.7 ms bucket over every range kL reads,
// and a summary naming them; it returns the summary's path.
func nightDirs(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	var classes []string
	var conf struct {
		Base struct {
			Shared struct {
				Mix []struct{ Class string } `json:"mix"`
			} `json:"shared"`
		} `json:"base"`
	}
	raw, err := os.ReadFile(filepath.Join(calibrationFixture, "config.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &conf))
	for _, m := range conf.Base.Shared.Mix {
		classes = append(classes, m.Class)
	}
	// One bucket per slice; the recorded p99 is whatever the rebuild gives for it.
	key := int64(1712) // a real bucket key, about 1.7 ms
	var slices []string
	byClass := map[string][]probe.SliceHistogram{}
	for s := 0; s < 7200; s += 5 {
		cls := classes
		if s >= 900 && s < 6300 {
			cls = classes[:1] // kL reads only the warm-up and cool-down, but every slice must be there
		}
		h := map[string]probe.SliceHistogram{}
		for _, c := range cls {
			sh := probe.SliceHistogram{Start: s, Seconds: 5, UpperMicros: map[int64]int64{key: 10}, N: 10}
			h[c] = sh
			byClass[c] = append(byClass[c], sh)
		}
		line, err := json.Marshal(map[string]any{"kind": "latency", "slice": s / 5, "classes": h})
		require.NoError(t, err)
		slices = append(slices, string(line))
	}
	lat, err := probe.RebuildLatency(5, byClass)
	require.NoError(t, err)
	p99, _, _ := lat.Quantile(classes[0], probe.Window{From: 0, To: 900}, 0.99)
	p99s, late := map[string]float64{}, map[string]int64{}
	for _, c := range classes {
		p99s[c], late[c] = p99.Seconds(), 0
	}
	latencyEvent, err := json.Marshal(map[string]any{"t": 7200.3, "time": time.Now(), "kind": "latency",
		"data": map[string]any{"warmup_p99_s": p99s, "cooldown_p99_s": p99s, "late": late}})
	require.NoError(t, err)

	type cellEntry map[string]any
	var cells []cellEntry
	for _, id := range []string{"c41p4", "c41p5", "c50p4", "c50p5"} {
		dir := filepath.Join(root, id)
		require.NoError(t, os.CopyFS(dir, os.DirFS(calibrationFixture)))
		samples := keepLines(t, filepath.Join(dir, "samples.jsonl"), func(l string) bool { return !strings.Contains(l, `"kind":"latency"`) })
		write(t, filepath.Join(dir, "samples.jsonl"), append(samples, slices...))
		events := keepLines(t, filepath.Join(dir, "events.jsonl"), func(l string) bool { return !strings.Contains(l, `"kind":"latency"`) })
		write(t, filepath.Join(dir, "events.jsonl"), append(events, string(latencyEvent)))
		editJSON(t, filepath.Join(dir, "verdict.json"), func(m map[string]any) { m["cell"] = id })
		var digest string
		editJSON(t, filepath.Join(dir, "build.json"), func(m map[string]any) {
			m["cell"], m["attempt"], m["source_clean"] = id, "att-"+id, "true"
			digest = m["soak_sha256"].(string)
		})
		cells = append(cells, cellEntry{"cell": id, "completion": "done", "problems": []string{},
			"report":  map[string]any{"exit": 1, "unresolved": []any{}, "retained": []any{}},
			"receipt": map[string]any{"state": "done", "cell": id, "mode": "night", "kind": "calib", "attempt": "att-" + id, "exec_dir": dir, "digest": digest, "launcher_exit": 1, "proven": true}})
	}
	summary := filepath.Join(root, "night.json")
	raw, err = json.Marshal(map[string]any{"mode": "night", "kind": "calib", "state": "completed", "runner_token": "tok", "cells": cells})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(summary, raw, 0o600))
	return summary
}

func keepLines(t *testing.T, path string, keep func(string) bool) []string {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if keep(l) {
			out = append(out, l)
		}
	}
	return out
}

func write(t *testing.T, path string, lines []string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600))
}

func editJSON(t *testing.T, path string, edit func(map[string]any)) {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, json.Unmarshal(raw, &m))
	edit(m)
	raw, err = json.Marshal(m)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0o600))
}

func withSourceCheck(t *testing.T, same bool) {
	t.Helper()
	saved := sourceUnchanged
	sourceUnchanged = func(string, string) (bool, error) { return same, nil }
	t.Cleanup(func() { sourceUnchanged = saved })
}

func TestDeriveCommandWritesGates(t *testing.T) {
	withSourceCheck(t, true)
	summary := nightDirs(t)
	out := filepath.Join(t.TempDir(), "gates.json")
	require.Equal(t, 0, deriveMain([]string{"-summary", summary, "-out", out, "-accept-zero", "kD:no drops in steady state"}))

	raw, err := os.ReadFile(out)
	require.NoError(t, err)
	var th map[string]float64
	require.NoError(t, json.Unmarshal(raw, &th))
	require.Len(t, th, 16)
	require.InDelta(t, 16, th["kC"], 0)
	require.FileExists(t, filepath.Join(filepath.Dir(out), "derivation.json"))
	md, err := os.ReadFile(filepath.Join(filepath.Dir(out), "derivation.md"))
	require.NoError(t, err)
	require.Contains(t, string(md), "gates.json was written")
	require.Contains(t, string(md), "degenerate; accepted: no drops in steady state")
}

// -raise sets a minimum after the rule and the floors, recorded in the report (PLAN v7.14 §48.5).
func TestDeriveCommandRaise(t *testing.T) {
	withSourceCheck(t, true)
	summary := nightDirs(t)
	out := filepath.Join(t.TempDir(), "gates.json")
	require.Equal(t, 0, deriveMain([]string{"-summary", summary, "-out", out, "-accept-zero", "kD:r", "-raise", "kL=0.6:§48.1: batch-1 evidence"}))
	raw, err := os.ReadFile(out)
	require.NoError(t, err)
	var th map[string]float64
	require.NoError(t, json.Unmarshal(raw, &th))
	require.InDelta(t, 0.6, th["kL"], 0)
	md, err := os.ReadFile(filepath.Join(filepath.Dir(out), "derivation.md"))
	require.NoError(t, err)
	require.Contains(t, string(md), "§48.1: batch-1 evidence")

	out = filepath.Join(t.TempDir(), "gates.json")
	require.Equal(t, 1, deriveMain([]string{"-summary", summary, "-out", out, "-accept-zero", "kD:r", "-raise", "kX=1:r"}))
	require.NoFileExists(t, out)
}

// captureStderr runs fn with os.Stderr redirected and returns what it wrote.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	saved := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = saved }()
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	fn()
	require.NoError(t, w.Close())
	return <-done
}

// A malformed -raise is a usage error before anything is derived or written, on a readable night (Codex AY02).
func TestDeriveCommandRaiseUsage(t *testing.T) {
	withSourceCheck(t, true)
	summary := nightDirs(t)
	for name, flag := range map[string]string{
		"no value":     "kL",
		"no reason":    "kL=0.4",
		"not a number": "kL=abc:r",
		"blank reason": "kL=0.4: ",
	} {
		t.Run(name, func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "gates.json")
			var code int
			stderr := captureStderr(t, func() {
				code = deriveMain([]string{"-summary", summary, "-out", out, "-accept-zero", "kD:r", "-raise", flag})
			})
			require.Equal(t, 2, code)
			require.Contains(t, stderr, fmt.Sprintf("-raise %q: want k=value:reason", flag))
			require.NoFileExists(t, out)
			require.NoFileExists(t, filepath.Join(filepath.Dir(out), "derivation.json"))
		})
	}
}

func TestDeriveCommandRefusalWritesNoGates(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		same bool
	}{
		"degenerate kD":  {nil, true},
		"source changed": {[]string{"-accept-zero", "kD:r"}, false},
	} {
		t.Run(name, func(t *testing.T) {
			withSourceCheck(t, tc.same)
			summary := nightDirs(t)
			out := filepath.Join(t.TempDir(), "gates.json")
			require.Equal(t, 1, deriveMain(append([]string{"-summary", summary, "-out", out}, tc.args...)))
			require.NoFileExists(t, out)
			var r struct {
				Refused  bool     `json:"refused"`
				Problems []string `json:"problems"`
			}
			raw, err := os.ReadFile(filepath.Join(filepath.Dir(out), "derivation.json"))
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(raw, &r))
			require.True(t, r.Refused)
			require.NotEmpty(t, r.Problems, fmt.Sprint(r))
		})
	}
}

// A field lost from one cell's artifacts refuses at the command's boundary (Codex AL01, AL02).
func TestDeriveCommandRefusesLostEvidence(t *testing.T) {
	for name, edit := range map[string][2]string{
		"lost counter":     {`"EventBufferFull":0`, `"EventBufferFullX":0`},
		"lost window":      {`"kind":"window","data":{"id":"M3"`, `"kind":"window-lost","data":{"id":"M3"`},
		"lost teardown":    {`"FDs":15`, `"FDsX":15`},
		"lost gc pause":    {`"max_pause_ms":5,`, ``},
		"lost window time": {`"time":"2026-09-27T04:47:10.970587128+08:00","kind":"window"`, `"kind":"window"`},
		"lost churn map":   {`"after":{`, `"after":null,"x":{`},
	} {
		t.Run(name, func(t *testing.T) {
			withSourceCheck(t, true)
			summary := nightDirs(t)
			events := filepath.Join(filepath.Dir(summary), "c50p4", "events.jsonl")
			if strings.Contains(edit[0], "pause") {
				events = filepath.Join(filepath.Dir(summary), "c50p4", "ccm", "health.jsonl")
			}
			raw, err := os.ReadFile(events)
			require.NoError(t, err)
			require.Contains(t, string(raw), edit[0])
			require.NoError(t, os.WriteFile(events, []byte(strings.Replace(string(raw), edit[0], edit[1], 1)), 0o600))
			out := filepath.Join(t.TempDir(), "gates.json")
			require.Equal(t, 1, deriveMain([]string{"-summary", summary, "-out", out, "-accept-zero", "kD:r"}))
			require.NoFileExists(t, out)
		})
	}
}

func TestDeriveCommandUsage(t *testing.T) {
	require.Equal(t, 2, deriveMain(nil))
	require.Equal(t, 2, deriveMain([]string{"-summary", "x.json", "-accept", "c41p4:G8"}), "an acceptance needs a reason")
	require.Equal(t, 2, deriveMain([]string{"-summary", "x.json", "-accept-zero", "kD"}))
	require.Equal(t, 2, deriveMain([]string{"-summary", filepath.Join(t.TempDir(), "missing.json")}))
}
