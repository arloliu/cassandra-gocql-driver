package gate

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// Threshold names in gates.json (PLAN §6, values marked "cal").
const (
	// KG is the per-group goroutine slope bound, goroutines per hour (G2).
	KG = "kG"
	// KG0 is the slack on the total goroutine bound at quiet checkpoints (G2).
	KG0 = "kG0"
	// KH is the heap slope bound, MiB per hour (G3).
	KH = "kH"
	// KHr is the allowed relative rise of the late heap median over the early one (G3).
	KHr = "kHr"
	// KF is the slack on the fd bound at quiet checkpoints (G4, G12).
	KF = "kF"
	// KFs is the fd slope bound, fds per hour (G4).
	KFs = "kFs"
	// KS is the stream balance bound at quiet checkpoints (G6).
	KS = "kS"
	// KSs is the stream balance slope bound, streams per hour (G6).
	KSs = "kSs"
	// KE is the allowed count of dropped event frames (G7).
	KE = "kE"
	// KC is the goroutine group tolerance after Close (G12, G13).
	KC = "kC"
	// KCh is the per-churn residue slope bound, goroutines per churn (G13).
	KCh = "kCh"
	// KL is the allowed relative rise of the cool-down p99 over the warm-up p99 (G14).
	KL = "kL"
	// KLWTu is the allowed count of uncertain LWT operations (G10b).
	KLWTu = "kLWTu"
	// KD is the allowed dropped-message count per steady-state interval (G16).
	KD = "kD"
	// KGp is the allowed maximum GC pause per interval, milliseconds (G16).
	KGp = "kGp"
	// KGt is the allowed GC time fraction per interval (G16).
	KGt = "kGt"
)

// ErrMissingThreshold is returned for a threshold that gates.json lacks or holds as a non-finite number.
var ErrMissingThreshold = errors.New("gate: threshold missing from gates.json")

// Thresholds holds the frozen calibration values, by name.
type Thresholds map[string]float64

// LoadThresholds reads gates.json.
//
// Parameters:
//   - path: the gates.json file, a JSON object of name to number
//
// Returns:
//   - Thresholds: the values by name
//   - error: when the file cannot be read or is not such an object
func LoadThresholds(path string) (Thresholds, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read thresholds: %w", err)
	}
	var th Thresholds
	if err := json.Unmarshal(raw, &th); err != nil {
		return nil, fmt.Errorf("parse thresholds %s: %w", path, err)
	}
	return th, nil
}

// Get returns one threshold.
//
// Parameters:
//   - name: one of the K* names
//
// Returns:
//   - float64: the value
//   - error: wrapping ErrMissingThreshold when the value is absent or not finite
func (t Thresholds) Get(name string) (float64, error) {
	v, ok := t[name]
	if !ok || !isFinite(v) {
		return 0, fmt.Errorf("%w: %s", ErrMissingThreshold, name)
	}
	return v, nil
}

// getAll returns the named thresholds in order, or the first missing one's error.
func (t Thresholds) getAll(names ...string) ([]float64, error) {
	out := make([]float64, len(names))
	for i, n := range names {
		v, err := t.Get(n)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}
