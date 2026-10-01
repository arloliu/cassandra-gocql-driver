package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/probe"
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
