package probe

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func readTestdata(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile("testdata/" + name)
	require.NoError(t, err)
	return string(raw)
}

// The fixtures are real output under load, captured on 2026-09-25 (tmp/soak-smoke/nodetool-capture/).
func TestTPStatsDroppedRealOutput(t *testing.T) {
	for _, v := range []string{"4.1.6", "5.0.3"} {
		total, perType, err := TPStatsDropped(readTestdata(t, "tpstats-"+v+".txt"))
		require.NoError(t, err, v)
		require.Zero(t, total, v)
		require.Empty(t, perType, v)
	}
}

func TestTPStatsDroppedSums(t *testing.T) {
	out := readTestdata(t, "tpstats-5.0.3.txt")
	out = strings.Replace(out, "MUTATION_REQ                      0 ", "MUTATION_REQ                      7 ", 1)
	out = strings.Replace(out, "READ_REQ                          0 ", "READ_REQ                          5 ", 1)
	total, perType, err := TPStatsDropped(out)
	require.NoError(t, err)
	require.EqualValues(t, 12, total)
	require.Equal(t, map[string]int64{"MUTATION_REQ": 7, "READ_REQ": 5}, perType)
}

func TestTPStatsDroppedMissing(t *testing.T) {
	for name, out := range map[string]string{
		"empty":     "",
		"no table":  "Pool Name Active Pending\nReadStage 0 0\n",
		"bad count": "Message type Dropped 50%\nREAD_REQ x 1.0\n",
		"no rows":   "Message type Dropped 50%\n\n",
	} {
		_, _, err := TPStatsDropped(out)
		require.Error(t, err, name)
	}
}

func TestGCStatsRealOutput(t *testing.T) {
	iv, maxMs, totalMs, err := GCStats(readTestdata(t, "gcstats-4.1.6.txt"))
	require.NoError(t, err)
	require.Positive(t, iv)
	require.GreaterOrEqual(t, totalMs, maxMs)
	iv, maxMs, totalMs, err = GCStats(readTestdata(t, "gcstats-5.0.3.txt"))
	require.NoError(t, err)
	require.InDelta(t, 48441, iv, 0)
	require.InDelta(t, 26, maxMs, 0)
	require.InDelta(t, 324, totalMs, 0)
}

func TestGCStatsMissing(t *testing.T) {
	for name, out := range map[string]string{
		"empty":     "",
		"no header": "  33839  3  132\n",
		"short":     "Interval (ms) Max GC Elapsed (ms)Total\n 33839 3\n",
		"garbage":   "Interval (ms) Max GC Elapsed (ms)Total\n a b c\n",
	} {
		_, _, _, err := GCStats(out)
		require.Error(t, err, name)
	}
}

func TestInfoHostID(t *testing.T) {
	out := "ID                     : 3F2E1A9C-0B1D-4E6F-8A7B-1C2D3E4F5A6B\nGossip active          : true\n"
	id, err := InfoHostID(out)
	require.NoError(t, err)
	require.Equal(t, "3f2e1a9c-0b1d-4e6f-8a7b-1c2d3e4f5a6b", id)
	_, err = InfoHostID("Gossip active          : true\n")
	require.Error(t, err)
}
