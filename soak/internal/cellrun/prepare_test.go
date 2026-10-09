package cellrun

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/artifact"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/ccmctl"
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

// With batch 2 every kind of every cell runs, so prepare refuses none (PLAN §59.4; the gate's own cases are
// chaos.Supported's tests); a validation run is checked on its own F-stop and F-pause.
func TestPrepareAcceptsEveryPhaseTwoCell(t *testing.T) {
	for _, tc := range []struct {
		cell string
		mode gate.Mode
	}{
		{"c41p4", gate.ModeNight}, {"c41p5", gate.ModeNight}, {"c50p4", gate.ModeNight}, {"c50p5", gate.ModeNight},
		{"c50p5", gate.ModeValidate},
	} {
		o := prepareOptions(t, "")
		o.CellID, o.Mode = tc.cell, tc.mode
		if tc.mode == gate.ModeNight {
			o.RunKind = artifact.RunNight
		}
		r := newPrepareRun(o)
		require.NoError(t, r.prepare(), tc.cell)
		require.Empty(t, r.col.InvalidConfig, "%s %s", tc.cell, tc.mode)
		r.closeStreams()
	}
}

// node4's pid is recorded while F-topo has it, so the launcher's cleanup knows its process (PLAN §59.6).
func TestRecordPIDsIncludesTheTopologyNode(t *testing.T) {
	config := t.TempDir()
	for node, pid := range map[string]string{"node1": "101", "node2": "102", "node3": "103", "node4": "104"} {
		dir := filepath.Join(config, "gocql_soak_c50p5", node)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "cassandra.pid"), []byte(pid+"\n"), 0o644))
	}
	r := newPrepareRun(prepareOptions(t, ""))
	r.dir, r.nodes = t.TempDir(), slices.Clone(cellNodes)
	r.cl = ccmctl.New(ccmctl.Config{Binary: "ccm", ConfigDir: config}).Cluster("gocql_soak_c50p5")
	r.recordPIDs()
	require.Equal(t, map[string]int{"node1": 101, "node2": 102, "node3": 103, "node4": 104}, r.res.NodePIDs)

	require.NoError(t, os.RemoveAll(filepath.Join(config, "gocql_soak_c50p5", "node4")))
	r.recordPIDs()
	require.NotContains(t, r.res.NodePIDs, "node4", "a removed node4 leaves the record")
}

// A cell that ends while F-topo has node4 collects node4's logs with the others (PLAN §59.6).
func TestCollectNodeLogsIncludesAPresentTopologyNode(t *testing.T) {
	for _, present := range []bool{true, false} {
		config := t.TempDir()
		nodes := slices.Clone(cellNodes)
		if present {
			nodes = append(nodes, "node4")
		}
		for _, n := range nodes {
			logs := filepath.Join(config, "gocql_soak_c50p5", n, "logs")
			require.NoError(t, os.MkdirAll(logs, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(logs, "system.log"), []byte(n), 0o644))
		}
		r := newPrepareRun(prepareOptions(t, ""))
		r.dir, r.nodes = t.TempDir(), slices.Clone(cellNodes)
		r.cl = ccmctl.New(ccmctl.Config{Binary: "/nonexistent/ccm", ConfigDir: config}).Cluster("gocql_soak_c50p5")
		require.NoError(t, r.collectNodeLogs(t.Context()))
		_, err := os.Stat(filepath.Join(r.dir, "ccm", "node4", "system.log"))
		require.Equal(t, present, err == nil, "node4 present=%v", present)
		_, err = os.Stat(filepath.Join(r.dir, "ccm", "node3", "system.log"))
		require.NoError(t, err)
	}
}
