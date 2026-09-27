// Package probe samples the harness process and the driver:
// goroutines, leaks, heap, fds, streams, log lines and latency (PLAN §6 series).
package probe

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
)

// NoCreator is the group of goroutines whose stack has no "created by" line, such as main.
const NoCreator = "(no creator)"

const bytesPerMiB = 1 << 20

var leakHeader = regexp.MustCompile(`^goroutineleak profile: total (\d+)`)

// GoroutineGroups counts live goroutines by the function that created them (G2).
//
// Returns:
//   - map[string]int: creator function → goroutines, NoCreator for those without one
//   - int: the total
//   - error: when the profile cannot be written
func GoroutineGroups() (map[string]int, int, error) {
	var b bytes.Buffer
	if err := WriteGoroutineDump(&b); err != nil {
		return nil, 0, err
	}
	groups, total := ParseGoroutineGroups(&b)
	return groups, total, nil
}

// WriteGoroutineDump writes the goroutine profile with full stacks (debug=2).
//
// Parameters:
//   - w: the destination
//
// Returns:
//   - error: from the profile writer
func WriteGoroutineDump(w io.Writer) error {
	if err := pprof.Lookup("goroutine").WriteTo(w, 2); err != nil {
		return fmt.Errorf("goroutine profile: %w", err)
	}
	return nil
}

// ParseGoroutineGroups groups a debug=2 goroutine dump by creator.
//
// Parameters:
//   - r: the dump
//
// Returns:
//   - map[string]int: creator function → goroutines
//   - int: the total
func ParseGoroutineGroups(r io.Reader) (map[string]int, int) {
	groups := map[string]int{}
	total := 0
	creator := ""
	inGoroutine := false
	flush := func() {
		if !inGoroutine {
			return
		}
		if creator == "" {
			creator = NoCreator
		}
		groups[creator]++
		total++
		creator, inGoroutine = "", false
	}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "goroutine "):
			flush()
			inGoroutine = true
		case strings.HasPrefix(line, "created by "):
			fn := strings.TrimPrefix(line, "created by ")
			if i := strings.Index(fn, " in goroutine "); i >= 0 {
				fn = fn[:i]
			}
			creator = fn
		}
	}
	flush()
	return groups, total
}

// GoroutineLeaks runs the Go 1.27 goroutineleak detector (G1).
// It forces a GC; the detector sees only goroutines blocked on unreachable primitives,
// not readers parked in netpoll (tmp/soak-review-2026-09-24/RESEARCH.md §3).
//
// Parameters:
//   - w: receives the debug=1 profile; may be nil
//
// Returns:
//   - int: the leaked goroutines reported
//   - bool: false when the runtime has no goroutineleak profile
//   - error: when the profile cannot be written or parsed
func GoroutineLeaks(w io.Writer) (int, bool, error) {
	p := pprof.Lookup("goroutineleak")
	if p == nil {
		return 0, false, nil
	}
	var b bytes.Buffer
	if err := p.WriteTo(&b, 1); err != nil {
		return 0, true, fmt.Errorf("goroutineleak profile: %w", err)
	}
	if w != nil {
		if _, err := w.Write(b.Bytes()); err != nil {
			return 0, true, fmt.Errorf("goroutineleak profile: %w", err)
		}
	}
	first, _, _ := strings.Cut(b.String(), "\n")
	m := leakHeader.FindStringSubmatch(first)
	if m == nil {
		return 0, true, fmt.Errorf("goroutineleak profile: unexpected header %q", first)
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, true, fmt.Errorf("goroutineleak profile: %w", err)
	}
	return n, true, nil
}

// HeapAfterGC forces a GC and returns the live heap (G3).
//
// Returns:
//   - float64: HeapAlloc in MiB
func HeapAfterGC() float64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return float64(ms.HeapAlloc) / bytesPerMiB
}

// WriteHeapProfile writes the heap profile, after a GC.
//
// Parameters:
//   - w: the destination
//
// Returns:
//   - error: from the profile writer
func WriteHeapProfile(w io.Writer) error {
	runtime.GC()
	if err := pprof.Lookup("heap").WriteTo(w, 0); err != nil {
		return fmt.Errorf("heap profile: %w", err)
	}
	return nil
}

// FDCount counts the open fds of a process (G4).
//
// Parameters:
//   - fdDir: /proc/self/fd in production
//
// Returns:
//   - int: the number of entries
//   - error: when the directory cannot be read
func FDCount(fdDir string) (int, error) {
	ents, err := os.ReadDir(fdDir)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", fdDir, err)
	}
	return len(ents), nil
}

// ProcessState returns the one-letter state of a process from /proc/<pid>/stat.
// F-pause's effect check expects "T" (stopped); F-stop's expects the process to be gone.
//
// Parameters:
//   - procDir: /proc in production
//   - pid: the process
//
// Returns:
//   - string: the state letter, or "" when the process does not exist
//   - error: when stat exists but cannot be parsed
func ProcessState(procDir string, pid int) (string, error) {
	raw, err := os.ReadFile(fmt.Sprintf("%s/%d/stat", procDir, pid))
	if err != nil {
		return "", nil //nolint:nilerr // a missing stat file means the process is gone
	}
	// The command name is in parentheses and may contain spaces; the state follows the last ')'.
	s := string(raw)
	i := strings.LastIndexByte(s, ')')
	if i < 0 || i+2 >= len(s) {
		return "", fmt.Errorf("parse %d/stat: %q", pid, s)
	}
	return string(s[i+2]), nil
}
