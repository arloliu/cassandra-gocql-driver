package main

import (
	"fmt"
	"maps"
	"os"
	"slices"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cellrun"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/derive"
)

// latencyCheckMain checks that each execution directory's persisted latency is complete and rebuilds
// the p99s its run recorded (PLAN §51.5): no loader problem, an all-zero late count, and the derivation's cross-check.
// Every directory is checked. Exit status: 0 every directory passes, 1 a problem, 2 bad usage or an unreadable directory.
func latencyCheckMain(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: soak latency-check <exec dir>…")
		return 2
	}
	status := 0
	for _, dir := range args {
		problems, err := latencyCheck(dir)
		if err != nil {
			// An unreadable directory does not stop the others from being checked (Codex BG02).
			fmt.Fprintf(os.Stderr, "latency-check: %s: %v\n", dir, err)
			status = 2
			continue
		}
		if len(problems) == 0 {
			fmt.Printf("%s: ok\n", dir)
			continue
		}
		status = max(status, 1)
		fmt.Printf("%s: %d problems\n", dir, len(problems))
		for _, p := range problems {
			fmt.Printf("  - %s\n", p)
		}
	}
	return status
}

// latencyCheck returns one execution directory's latency problems.
func latencyCheck(dir string) ([]string, error) {
	cal, err := cellrun.LoadCalibration(dir)
	if err != nil {
		return nil, err
	}
	problems := slices.Clone(cal.Missing)
	for _, class := range slices.Sorted(maps.Keys(cal.Late)) {
		if n := cal.Late[class]; n > 0 {
			problems = append(problems, fmt.Sprintf("%d late latency observations of class %s", n, class))
		}
	}
	_, _, p := derive.CrossCheckLatency(dir, &cal)
	return append(problems, p...), nil
}
