package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cell"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/view"
)

// Smoke's record answers its whole-load p99 through its own constructor and report (PLAN §51.3, §51.5).
func TestSmokeLatencyReport(t *testing.T) {
	epoch := time.Unix(0, 0)
	l := newSmokeLatency(epoch)
	l.Observe("read", epoch.Add(30*time.Second), 3*time.Millisecond, false)
	l.Observe("read", epoch.Add(3*time.Hour), 4*time.Millisecond, false)
	var lines []string
	logf := func(f string, a ...any) { lines = append(lines, fmt.Sprintf(f, a...)) }
	err := smokeLatencyReport(l, map[string]float64{"read": 2}, map[string]float64{"read": 2}, logf)
	require.NoError(t, err)
	require.Len(t, lines, 1)
	require.Contains(t, lines[0], "p99 4.0")
}

// A record that cannot answer smoke's query makes the smoke command fail instead of printing a zero p99.
func TestSmokeLatencyReportUnregistered(t *testing.T) {
	l := probe.NewAggregateLatency(time.Unix(0, 0))
	err := smokeLatencyReport(l, map[string]float64{"read": 1}, map[string]float64{"read": 1}, func(string, ...any) {})
	require.ErrorContains(t, err, "is not registered")
}

func TestParseSmokeFaults(t *testing.T) {
	specs := chaos.Specs()
	got, err := parseSmokeFaults("F-lat:200,F-lat:1500,F-bw:16,F-bw:128,F-black,F-halfopen,F-reset,F-kill", specs)
	require.NoError(t, err)
	require.Len(t, got, 8)
	require.Equal(t, chaos.Planned{Kind: chaos.FaultLat, Param: 200}, got[0])
	require.Equal(t, 128, got[3].Param)
	require.Zero(t, got[4].Param)
	for _, bad := range []string{"F-lat", "F-bw:8", "F-bw:x", "F-kill:3", "F-nope", "F-lat:1501"} {
		_, err := parseSmokeFaults(bad, specs)
		require.Error(t, err, bad)
	}
}

// A bad -faults ends the smoke command before any fixture exists (§64 BW08).
func TestRunSmokeRefusesBadFaultsFirst(t *testing.T) {
	err := runSmoke(t.Context(), smokeConfig{faults: "F-lat:200,F-bw:8", outDir: t.TempDir(), ccmBin: "/nonexistent/ccm"})
	require.ErrorContains(t, err, "-faults")
	require.NotContains(t, err.Error(), "ccm")
}

// Smoke's faults reach the builder with their parameter, so §59.7 step 2 runs the production faults (§65 BX05).
func TestSmokeScheduleReachesTheBuilder(t *testing.T) {
	specs := chaos.Specs()
	parsed, err := parseSmokeFaults("F-lat:1500,F-bw:16,F-black,F-reset", specs)
	require.NoError(t, err)
	planned := smokeSchedule(parsed, specs, 30*time.Second)
	require.Equal(t, 1500, planned[0].Param)
	require.Equal(t, 16, planned[1].Param)
	require.Equal(t, 30*time.Second, planned[2].Active)
	require.Zero(t, planned[3].Active, "F-reset has no active range")
	b := &cell.Builder{Registry: view.NewRegistry()}
	for _, p := range planned {
		require.Len(t, p.Targets, 1)
		_, err := b.Build(p)
		require.NoError(t, err, "%s", p.Kind)
	}
	parsed[0].Param = 0
	_, err = b.Build(smokeSchedule(parsed[:1], specs, time.Second)[0])
	require.Error(t, err, "a lost parameter is refused by the builder")
}
