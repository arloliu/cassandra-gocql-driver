package cellrun

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/artifact"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/view"
)

func prepareOptions(t *testing.T, attempt string) Options {
	t.Helper()
	return Options{CellID: "c50p5", Mode: gate.ModeValidate, RunKind: artifact.RunControl, Seed: 1, Rate: 1500, Workers: 32,
		OutRoot: t.TempDir(), Date: "2026-09-25", AttemptID: attempt, LaunchToken: "tok-1",
		GatesPath: filepath.Join(t.TempDir(), "absent.json")}
}

func newPrepareRun(o Options) *cellRun {
	return &cellRun{o: o, logf: func(string, ...any) {}, windows: &chaos.Windows{}, reg: view.NewRegistry(), diskLow: make(chan struct{})}
}

// The launcher chooses the attempt id so it knows the execution directory before the harness starts.
func TestPrepareUsesTheGivenAttemptID(t *testing.T) {
	o := prepareOptions(t, "20260925T113426Z-1a4d")
	r := newPrepareRun(o)
	require.NoError(t, r.prepare())
	defer r.closeStreams()
	want := filepath.Join(o.OutRoot, "soak-2026-09-25", "c50p5", "control-20260925T113426Z-1a4d")
	require.Equal(t, want, r.dir)
	for _, f := range []string{"resources.json", "verdict.json", "config.json"} {
		_, err := os.Stat(filepath.Join(want, f))
		require.NoError(t, err, f)
	}
	raw, err := os.ReadFile(filepath.Join(want, "resources.json"))
	require.NoError(t, err)
	var res artifact.Resources
	require.NoError(t, json.Unmarshal(raw, &res))
	require.Equal(t, "tok-1", res.LaunchToken, "resources.json carries the launcher's token from the first write")
	require.Equal(t, os.Getpid(), res.HarnessPID)

	again := newPrepareRun(o)
	require.ErrorIs(t, again.prepare(), artifact.ErrExists, "a reused attempt id never reuses the directory")
}

func TestPrepareRejectsAMalformedAttemptID(t *testing.T) {
	o := prepareOptions(t, "../escape")
	r := newPrepareRun(o)
	require.ErrorContains(t, r.prepare(), "attempt id")
	entries, err := os.ReadDir(o.OutRoot)
	require.NoError(t, err)
	require.Empty(t, entries, "nothing is created for a rejected id")
}
