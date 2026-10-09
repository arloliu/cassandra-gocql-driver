package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRehearsalMainUsage(t *testing.T) {
	require.Equal(t, 2, rehearsalMain(nil), "both flags are required")
	require.Equal(t, 2, rehearsalMain([]string{"-dir", t.TempDir(), "-cell", "c41p5"}), "an unreadable directory")
}

// The documented invocation, `soak rehearsal <exec dir> -cell <id>`, judges night A's c50p5 cell made green (PLAN §59.7).
func TestRehearsalMainDocumentedSyntax(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.CopyFS(dir, os.DirFS("../../testdata/calibration/c50p5")))
	path := filepath.Join(dir, "verdict.json")
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	var v map[string]any
	require.NoError(t, json.Unmarshal(raw, &v))
	v["status"] = "fail"
	delete(v, "reasons")
	gates, ok := v["gates"].([]any)
	require.True(t, ok)
	for _, g := range gates {
		g, ok := g.(map[string]any)
		require.True(t, ok)
		if g["gate"] == "G16" {
			g["status"] = "pass"
			delete(g, "details")
		}
	}
	raw, err = json.Marshal(v)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, raw, 0o644))

	require.Equal(t, 0, rehearsalMain([]string{dir, "-cell", "c50p5"}))
	require.Equal(t, 1, rehearsalMain([]string{dir, "-cell", "c41p5"}), "another cell is not green")
}
