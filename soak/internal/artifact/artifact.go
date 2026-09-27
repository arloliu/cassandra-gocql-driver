// Package artifact writes a soak run's execution directory (PLAN §8.2):
// a unique directory per run, append-only JSONL streams, and JSON files replaced atomically.
package artifact

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

// RunNight and the other run kinds name what an execution directory holds (PLAN §8.2).
const (
	// RunNight is a cell of a nightly run.
	RunNight = "night"
	// RunCalib is a calibration cell.
	RunCalib = "calib"
	// RunControl is a validation run with no canary.
	RunControl = "control"
)

// attemptRandomBytes is how many random bytes follow the timestamp in an attempt id: 4 hex digits.
const attemptRandomBytes = 2

// ErrExists is returned when an execution directory already exists; it is never reused.
var ErrExists = errors.New("artifact: execution directory already exists")

// JSONL appends one JSON value per line to a file; it is safe for concurrent use.
type JSONL struct {
	mu      sync.Mutex
	f       *os.File
	err     error
	skipped int
}

// AttemptID returns a new attempt id: the UTC time to the second, then 4 random hex digits.
//
// Parameters:
//   - now: the attempt's start
//   - rnd: the random source; crypto/rand.Reader in production
//
// Returns:
//   - string: e.g. 20260925T021530Z-3fa9
//   - error: when rnd fails
func AttemptID(now time.Time, rnd io.Reader) (string, error) {
	b := make([]byte, attemptRandomBytes)
	if _, err := io.ReadFull(rnd, b); err != nil {
		return "", fmt.Errorf("attempt id: %w", err)
	}
	return now.UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(b), nil
}

// attemptIDPattern is the shape AttemptID produces.
var attemptIDPattern = regexp.MustCompile(`^[0-9]{8}T[0-9]{6}Z-[0-9a-f]{4}$`)

// ValidAttemptID reports whether id has the shape AttemptID produces.
// A caller-chosen id that passes cannot escape the cell directory.
//
// Parameters:
//   - id: the candidate, e.g. 20260925T021530Z-3fa9
//
// Returns:
//   - bool: true when id matches
func ValidAttemptID(id string) bool {
	return attemptIDPattern.MatchString(id)
}

// NewExecDir creates a run's execution directory, root/soak-<date>/<cell>/<run-kind>-<attempt-id>.
// The directory must not exist: an existing one is never reused or overwritten.
//
// Parameters:
//   - root: the directory holding every night, e.g. the repository's tmp/
//   - date: the night's date, e.g. 2026-09-25
//   - cell: the cell id, e.g. c50p5
//   - runKind: RunNight, RunCalib, RunControl or a canary id
//   - attemptID: from AttemptID
//
// Returns:
//   - string: the new directory, with its pprof/ and ccm/ subdirectories
//   - error: wrapping ErrExists when it exists, or the filesystem error
func NewExecDir(root, date, cell, runKind, attemptID string) (string, error) {
	parent := filepath.Join(root, "soak-"+date, cell)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return "", fmt.Errorf("execution directory: %w", err)
	}
	dir := filepath.Join(parent, runKind+"-"+attemptID)
	if err := os.Mkdir(dir, 0o755); err != nil {
		if errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("%w: %s", ErrExists, dir)
		}
		return "", fmt.Errorf("execution directory: %w", err)
	}
	for _, sub := range []string{"pprof", "ccm"} {
		if err := os.Mkdir(filepath.Join(dir, sub), 0o755); err != nil {
			return "", fmt.Errorf("execution directory: %w", err)
		}
	}
	return dir, nil
}

// WriteJSON replaces a JSON file atomically: it writes a temporary file in the same directory,
// syncs it, and renames it over path, so a reader (or a killed writer) never leaves a partial file.
//
// Parameters:
//   - path: the destination
//   - v: the value, encoded indented
//
// Returns:
//   - error: from encoding or the filesystem
func WriteJSON(path string, v any) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("encode %s: %w", filepath.Base(path), err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(append(raw, '\n')); err != nil {
		tmp.Close()
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	if err := tmp.Chmod(0o644); err != nil {
		tmp.Close()
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("write %s: %w", filepath.Base(path), err)
	}
	return nil
}

// CopyFile copies a file, e.g. gates.json into the execution directory.
//
// Parameters:
//   - dst, src: the paths
//
// Returns:
//   - error: from the filesystem
func CopyFile(dst, src string) error {
	raw, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("copy %s: %w", src, err)
	}
	if err := os.WriteFile(dst, raw, 0o644); err != nil {
		return fmt.Errorf("copy %s: %w", src, err)
	}
	return nil
}

// OpenJSONL creates a JSONL file; it must not exist.
//
// Parameters:
//   - path: the file
//
// Returns:
//   - *JSONL: the appender
//   - error: from the filesystem
func OpenJSONL(path string) (*JSONL, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", filepath.Base(path), err)
	}
	return &JSONL{f: f}, nil
}

// Write appends one value as a line.
// A value that cannot be encoded is skipped and counted, and the stream goes on.
// After the first write failure every later write is dropped and Err reports that failure,
// so a full disk cannot stall the workload.
//
// Parameters:
//   - v: the value
func (j *JSONL) Write(v any) {
	raw, err := json.Marshal(v)
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.err != nil || j.f == nil {
		return
	}
	if err != nil {
		j.skipped++
		return
	}
	if _, err := j.f.Write(append(raw, '\n')); err != nil {
		j.err = err
	}
}

// Skipped returns how many values could not be encoded.
//
// Returns:
//   - int: the count
func (j *JSONL) Skipped() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.skipped
}

// Err returns the first write failure.
//
// Returns:
//   - error: nil when every write succeeded
func (j *JSONL) Err() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.err
}

// Close closes the file; later writes are dropped.
//
// Returns:
//   - error: the first write failure, or the close error
func (j *JSONL) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.f == nil {
		return j.err
	}
	err := j.f.Close()
	j.f = nil
	if j.err != nil {
		return j.err
	}
	return err
}

// NewAttemptID returns an attempt id for now from crypto/rand.
//
// Returns:
//   - string: the id
//   - error: when the random source fails
func NewAttemptID() (string, error) {
	return AttemptID(time.Now(), rand.Reader)
}

// Resources is what the harness created for its cell, written to resources.json as each resource appears,
// so the launcher's cleanup touches only these (PLAN §8.1).
type Resources struct {
	// Cluster is the ccm cluster name; empty until it is created.
	Cluster string `json:"cluster,omitempty"`
	// CCMConfigDir is the private CCM_CONFIG_DIR the cluster lives in.
	CCMConfigDir string `json:"ccm_config_dir,omitempty"`
	// NodePIDs maps node name to its Cassandra pid, as last seen.
	NodePIDs map[string]int `json:"node_pids,omitempty"`
	// ToxiproxyPID is the toxiproxy-server pid; zero until it runs.
	ToxiproxyPID int `json:"toxiproxy_pid,omitempty"`
	// Proxies are the proxy names created.
	Proxies []string `json:"proxies,omitempty"`
	// HarnessPID is the harness process.
	HarnessPID int `json:"harness_pid"`
	// LaunchToken is the launcher's per-launch token.
	// It lets the launcher tell its own execution directory from one another launch created at the same path.
	LaunchToken string `json:"launch_token,omitempty"`
	// Cleanup is empty until the harness's own cleanup starts, then "running", "done", or "failed: …";
	// the launcher's cleanup must finish whatever is not "done".
	Cleanup string `json:"cleanup,omitempty"`
}
