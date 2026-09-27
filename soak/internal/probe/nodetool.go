package probe

import (
	"bufio"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// TPStatsDropped parses `nodetool tpstats` output and sums the Dropped column of its "Message type" table (G16).
// The counters are cumulative since the node started, so a caller differences them.
// The layout is the same in 4.1.6 and 5.0.3 (soak/internal/probe/testdata).
//
// Parameters:
//   - out: the command's output; lines before the table, such as ccm's own log lines, are ignored
//
// Returns:
//   - int64: the total dropped messages
//   - map[string]int64: the non-zero counts per message type, for diagnosis
//   - error: when the table is missing or a row does not parse; the sample is then missing, never zero
func TPStatsDropped(out string) (int64, map[string]int64, error) {
	sc := bufio.NewScanner(strings.NewReader(out))
	inTable := false
	rows := 0
	var total int64
	perType := map[string]int64{}
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if !inTable {
			if len(fields) >= 3 && fields[0] == "Message" && fields[1] == "type" && fields[2] == "Dropped" {
				inTable = true
			}
			continue
		}
		if len(fields) == 0 {
			break
		}
		if len(fields) < 2 {
			return 0, nil, fmt.Errorf("tpstats: short row %q", sc.Text())
		}
		n, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, nil, fmt.Errorf("tpstats: row %q: %w", sc.Text(), err)
		}
		rows++
		total += n
		if n != 0 {
			perType[fields[0]] = n
		}
	}
	if !inTable || rows == 0 {
		return 0, nil, errors.New("tpstats: no dropped-message table")
	}
	return total, perType, nil
}

// GCStats parses `nodetool gcstats` output (G16).
// gcstats reports the interval since its previous call and resets it (getAndResetGCStats),
// so exactly one caller may read it, and the values are never differenced.
//
// Parameters:
//   - out: the command's output
//
// Returns:
//   - intervalMs: the interval the reading covers
//   - maxPauseMs: the longest GC pause in it
//   - totalMs: the total GC time in it
//   - err: when the header or the value row is missing or malformed
func GCStats(out string) (intervalMs, maxPauseMs, totalMs float64, err error) {
	sc := bufio.NewScanner(strings.NewReader(out))
	header := false
	for sc.Scan() {
		line := sc.Text()
		if !header {
			header = strings.Contains(line, "Interval (ms)") && strings.Contains(line, "Max GC Elapsed (ms)")
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		if len(fields) < 3 {
			return 0, 0, 0, fmt.Errorf("gcstats: short row %q", line)
		}
		var v [3]float64
		for i := range v {
			if v[i], err = strconv.ParseFloat(fields[i], 64); err != nil {
				return 0, 0, 0, fmt.Errorf("gcstats: row %q: %w", line, err)
			}
		}
		return v[0], v[1], v[2], nil
	}
	return 0, 0, 0, errors.New("gcstats: no value row")
}

// hostIDLine matches nodetool info's "ID" row, printed as "%-23s: %s" (NodeTool Info).
var hostIDLine = regexp.MustCompile(`(?m)^ID\s*:\s*([0-9a-fA-F-]{36})\s*$`)

// InfoHostID parses a node's host id from `nodetool info` output: the fixture's own view of its identity (G0, R1).
//
// Parameters:
//   - out: the command's output
//
// Returns:
//   - string: the host id, lower case
//   - error: when no ID row is present
func InfoHostID(out string) (string, error) {
	m := hostIDLine.FindStringSubmatch(out)
	if m == nil {
		return "", errors.New("nodetool info: no ID row")
	}
	return strings.ToLower(m[1]), nil
}
