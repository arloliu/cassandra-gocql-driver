package main

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// latencyCheckDir is one cell of nightDirs: persisted slices that rebuild the recorded p99s, and an all-zero late count.
func latencyCheckDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(filepath.Dir(nightDirs(t)), "c50p5")
}

func TestLatencyCheckPasses(t *testing.T) {
	dir := latencyCheckDir(t)
	problems, err := latencyCheck(dir)
	require.NoError(t, err)
	require.Empty(t, problems)
	out := captureStdout(t, func() { require.Equal(t, 0, latencyCheckMain([]string{dir})) })
	require.Contains(t, out, ": ok")
}

// A p99 the slices do not rebuild, a positive late count and an absent one each fail (PLAN §51.5).
func TestLatencyCheckFails(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(string)
		want string
	}{
		{"a p99 mismatch", func(dir string) {
			editLatencyEvent(t, dir, func(d map[string]any) { d["warmup_p99_s"].(map[string]any)["read"] = 0.5 })
		}, "rebuilt p99"},
		{"a late observation", func(dir string) {
			editLatencyEvent(t, dir, func(d map[string]any) { d["late"].(map[string]any)["lwt"] = 2 })
		}, "2 late latency observations of class lwt"},
		{"no late count", func(dir string) {
			editLatencyEvent(t, dir, func(d map[string]any) { delete(d, "late") })
		}, "the late-observation count is absent"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := latencyCheckDir(t)
			tc.edit(dir)
			problems, err := latencyCheck(dir)
			require.NoError(t, err)
			require.True(t, strings.Contains(strings.Join(problems, "\n"), tc.want), "%v", problems)
			captureStdout(t, func() { require.Equal(t, 1, latencyCheckMain([]string{dir})) })
		})
	}
}

// An unreadable directory does not stop the check of the others; the exit status is the worst (Codex BG02).
func TestLatencyCheckContinuesAfterAnUnreadableDirectory(t *testing.T) {
	good, bad := latencyCheckDir(t), latencyCheckDir(t)
	editLatencyEvent(t, bad, func(d map[string]any) { d["warmup_p99_s"].(map[string]any)["read"] = 0.5 })
	var code int
	out := captureStdout(t, func() {
		captureStderr(t, func() { code = latencyCheckMain([]string{t.TempDir(), good, bad}) })
	})
	require.Equal(t, 2, code)
	require.Contains(t, out, good+": ok")
	require.Contains(t, out, bad+": 1 problems")
}

func TestLatencyCheckUsage(t *testing.T) {
	require.Equal(t, 2, latencyCheckMain(nil))
	captureStderr(t, func() { require.Equal(t, 2, latencyCheckMain([]string{t.TempDir()})) })
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	saved := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = saved }()
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	fn()
	require.NoError(t, w.Close())
	return <-done
}

// editLatencyEvent rewrites the data of the execution directory's latency event.
func editLatencyEvent(t *testing.T, dir string, edit func(map[string]any)) {
	t.Helper()
	path := filepath.Join(dir, "events.jsonl")
	lines := keepLines(t, path, func(string) bool { return true })
	for i, l := range lines {
		if !strings.Contains(l, `"kind":"latency"`) {
			continue
		}
		var e map[string]any
		require.NoError(t, json.Unmarshal([]byte(l), &e))
		edit(e["data"].(map[string]any))
		raw, err := json.Marshal(e)
		require.NoError(t, err)
		lines[i] = string(raw)
	}
	write(t, path, lines)
}
