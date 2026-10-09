package cell

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/ccmctl"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
)

// node4's logs are saved under the cell's ccm artifacts before its directory is deleted, once per removal (PLAN §59.6).
func TestTopoOpsSaveLogs(t *testing.T) {
	config, out := t.TempDir(), t.TempDir()
	logs := filepath.Join(config, "gocql_soak_c50p5", "node4", "logs")
	require.NoError(t, os.MkdirAll(logs, 0o755))
	for _, f := range []string{"system.log", "gc.log.0.current", "debug.log"} {
		require.NoError(t, os.WriteFile(filepath.Join(logs, f), []byte(f), 0o644))
	}
	cl := ccmctl.New(ccmctl.Config{ConfigDir: config}).Cluster("gocql_soak_c50p5")
	b := &Builder{Cluster: cl, DC: "datacenter1", LogDir: out}
	// Two separately built F-topo faults save to their own directories (§67 BZ01).
	var ops []*topoOps
	for range 2 {
		f, err := b.Build(chaos.Planned{Kind: chaos.FaultTopo})
		require.NoError(t, err)
		_, ok := f.(*chaos.TopoFault)
		require.True(t, ok)
		ops = append(ops, &topoOps{c: cl, logDir: out, seq: int(b.topoSeq.Load())})
	}
	require.Equal(t, 2, ops[1].seq)
	o := &topoOps{c: cl, logDir: out, seq: 1}
	require.NoError(t, o.SaveLogs(t.Context(), "node4"))
	o.seq = 2
	require.NoError(t, os.WriteFile(filepath.Join(logs, "system.log"), []byte("second"), 0o644))
	require.NoError(t, o.SaveLogs(t.Context(), "node4"))
	first, err := os.ReadFile(filepath.Join(out, "node4-1", "system.log"))
	require.NoError(t, err)
	require.Equal(t, "system.log", string(first), "the first fault's logs are not overwritten")
	for _, d := range []string{"node4-1", "node4-2"} {
		got, err := os.ReadDir(filepath.Join(out, d))
		require.NoError(t, err)
		var names []string
		for _, e := range got {
			names = append(names, e.Name())
		}
		require.ElementsMatch(t, []string{"system.log", "gc.log.0.current"}, names, "system and gc logs only")
	}
	require.NoError(t, os.RemoveAll(logs))
	require.Error(t, o.SaveLogs(t.Context(), "node4"), "a node without logs is a failed save, not a silent one")
}
