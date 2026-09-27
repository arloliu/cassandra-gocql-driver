// Package cellrun runs one soak cell end to end (PLAN §8.3):
// setup, warm-up, the chaos timetable with churn slots, cool-down, teardown,
// the 5 s sampler and profile checkpoints, and the gates that decide verdict.json.
package cellrun

import (
	"errors"
	"fmt"
	"sync"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/artifact"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// maxG8Details bounds how many unexpected errors G8 names in its details; errors.jsonl keeps them all.
const maxG8Details = 20

// ErrorLine is one terminal operation error in errors.jsonl (PLAN §6.1).
type ErrorLine struct {
	// T is seconds since the workload epoch; Time the wall clock.
	T    float64   `json:"t"`
	Time time.Time `json:"time"`
	// Session, OpClass and OpID identify the operation.
	Session string `json:"session"`
	OpClass string `json:"op_class"`
	OpID    uint64 `json:"op_id"`
	// Class is the §6.1 class; Window the fault window covering the error, if any.
	Class    string `json:"class"`
	Window   string `json:"window,omitempty"`
	InWindow bool   `json:"in_window"`
	// Admitted is false for an unexpected error, which fails G8.
	Admitted bool `json:"admitted"`
	// Evidence flags an unavailable error that a one-node fault cannot explain.
	Evidence bool `json:"evidence,omitempty"`
	// Err, Type and Code describe the error; Code is -1 for a non-server error.
	Err  string `json:"err"`
	Type string `json:"type"`
	Code int    `json:"code"`
	// Host is the coordinator; Consistency and Serial the operation's consistencies;
	// ServerConsistency the consistency the server's error reports; Step the failing statement of an LWT.
	Host              string `json:"host,omitempty"`
	Consistency       string `json:"consistency,omitempty"`
	Serial            string `json:"serial,omitempty"`
	ServerConsistency string `json:"server_consistency,omitempty"`
	Step              string `json:"step,omitempty"`
	// Required, Alive, Received and BlockFor are the server error's fields, when it has them.
	Required int `json:"required,omitempty"`
	Alive    int `json:"alive,omitempty"`
	Received int `json:"received,omitempty"`
	BlockFor int `json:"block_for,omitempty"`
	// WriteType is a write timeout's write type, e.g. CAS.
	WriteType string `json:"write_type,omitempty"`
	// ElapsedMS is the failed attempt's elapsed in milliseconds, when the query observer saw it (LWT only).
	ElapsedMS float64 `json:"elapsed_ms,omitempty"`
	// P4CASUnknown is true when row 4's protocol-4 member classified the error (PLAN §36.4).
	P4CASUnknown bool `json:"p4_cas_unknown,omitempty"`
}

// EventLine is one line of events.jsonl.
type EventLine struct {
	// T is seconds since the workload epoch; zero before it; Time the wall clock.
	T    float64   `json:"t"`
	Time time.Time `json:"time"`
	// Kind names the event, e.g. fault, window, host, churn, dial, phase.
	Kind string `json:"kind"`
	// Data is the event's payload.
	Data any `json:"data,omitempty"`
}

// Recorder classifies terminal errors into errors.jsonl and appends events to events.jsonl.
// G8 is decided as each error is recorded: the window it falls in is already open,
// and a failed window never closes, so the live count and the final count agree.
type Recorder struct {
	errs     *artifact.JSONL
	events   *artifact.JSONL
	attempts *artifact.JSONL
	windows  *chaos.Windows
	proto    int

	mu         sync.Mutex
	epoch      time.Time
	classes    map[string]int64
	unexpected int64
	details    []string
}

// NewRecorder returns a recorder writing to the two streams.
//
// Parameters:
//   - errs: errors.jsonl
//   - events: events.jsonl
//   - windows: the cell's fault windows
//   - proto: the cell's protocol version, for row 4's protocol-4 member (PLAN §36.4)
//
// Returns:
//   - *Recorder: the recorder; its epoch is unset until SetEpoch
func NewRecorder(errs, events *artifact.JSONL, windows *chaos.Windows, proto int) *Recorder {
	return &Recorder{errs: errs, events: events, windows: windows, proto: proto, classes: map[string]int64{}}
}

// SetEpoch sets the workload epoch that line times are relative to.
//
// Parameters:
//   - epoch: the workload start
func (r *Recorder) SetEpoch(epoch time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.epoch = epoch
}

// Error classifies one terminal error, writes it to errors.jsonl and counts it for G8.
//
// Parameters:
//   - e: the error as the workload reported it
func (r *Recorder) Error(e workload.ErrorRecord) {
	class, p4CAS := gate.ClassifyOp(e.Err, e.Class == workload.ClassLWT, r.proto, e.Elapsed)
	win, in := r.windows.At(e.Time)
	admitted := gate.Admit(class, in, gate.Op{ShortDeadline: e.Class == workload.ClassShortDeadline, LWT: e.Class == workload.ClassLWT})
	line := ErrorLine{
		Time: e.Time, Session: e.Session, OpClass: string(e.Class), OpID: e.OpID,
		Class: class.String(), InWindow: in, Admitted: admitted,
		Err: e.Err.Error(), Type: fmt.Sprintf("%T", e.Err), Code: -1, Host: e.Host, Consistency: e.Consistency,
		Serial: e.Serial, Step: e.Step, P4CASUnknown: p4CAS,
	}
	if e.Elapsed > 0 {
		line.ElapsedMS = float64(e.Elapsed) / float64(time.Millisecond)
	}
	if in {
		line.Window = win.ID
		line.Evidence = gate.UnavailableEvidence(e.Err, win.Width)
	}
	serverFields(e.Err, &line)

	r.mu.Lock()
	line.T = r.since(e.Time)
	r.classes[line.Class]++
	if !admitted {
		r.unexpected++
		if len(r.details) < maxG8Details {
			r.details = append(r.details, fmt.Sprintf("t=%.0fs %s %s %s window=%q: %s", line.T, e.Session, e.Class, line.Class, line.Window, line.Err))
		}
	}
	r.mu.Unlock()
	r.errs.Write(line)
}

// SetAttempts sets the attempts.jsonl stream.
//
// Parameters:
//   - j: the stream
func (r *Recorder) SetAttempts(j *artifact.JSONL) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts = j
}

// Attempt appends one failed attempt to attempts.jsonl.
//
// Parameters:
//   - a: the attempt
func (r *Recorder) Attempt(a workload.AttemptRecord) {
	r.mu.Lock()
	t, out := r.since(a.Time), r.attempts
	r.mu.Unlock()
	if out != nil {
		out.Write(struct {
			T float64 `json:"t"`
			workload.AttemptRecord
		}{t, a})
	}
}

// Event appends one event to events.jsonl.
//
// Parameters:
//   - kind: the event kind
//   - at: when it happened
//   - data: its payload
func (r *Recorder) Event(kind string, at time.Time, data any) {
	r.mu.Lock()
	t := r.since(at)
	r.mu.Unlock()
	r.events.Write(EventLine{T: t, Time: at, Kind: kind, Data: data})
}

// ClassCounts returns the terminal errors counted per §6.1 class.
//
// Returns:
//   - map[string]int64: class name → count
func (r *Recorder) ClassCounts() map[string]int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]int64, len(r.classes))
	for k, v := range r.classes {
		out[k] = v
	}
	return out
}

// Unexpected returns the number of unexpected errors so far.
//
// Returns:
//   - int64: the count
func (r *Recorder) Unexpected() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.unexpected
}

// G8 returns the G8 result: fail on any unexpected error, naming the first ones.
//
// Returns:
//   - gate.Result: the result
func (r *Recorder) G8() gate.Result {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.unexpected == 0 {
		return gate.Bool("G8", nil)
	}
	details := append([]string{fmt.Sprintf("%d unexpected errors (errors.jsonl admitted=false)", r.unexpected)}, r.details...)
	return gate.Bool("G8", details)
}

func (r *Recorder) since(t time.Time) float64 {
	if r.epoch.IsZero() {
		return 0
	}
	return t.Sub(r.epoch).Seconds()
}

// serverFields copies a server error's code and replica counts into the line.
func serverFields(err error, line *ErrorLine) {
	var re gocql.RequestError
	if errors.As(err, &re) {
		line.Code = re.Code()
	}
	var un *gocql.RequestErrUnavailable
	var rt *gocql.RequestErrReadTimeout
	var wt *gocql.RequestErrWriteTimeout
	var cas *gocql.RequestErrCASWriteUnknown
	switch {
	case errors.As(err, &un):
		line.Required, line.Alive, line.ServerConsistency = un.Required, un.Alive, un.Consistency.String()
	case errors.As(err, &rt):
		line.Received, line.BlockFor, line.ServerConsistency = rt.Received, rt.BlockFor, rt.Consistency.String()
	case errors.As(err, &wt):
		line.Received, line.BlockFor, line.ServerConsistency = wt.Received, wt.BlockFor, wt.Consistency.String()
		line.WriteType = wt.WriteType
	case errors.As(err, &cas):
		line.Received, line.BlockFor, line.ServerConsistency = cas.Received, cas.BlockFor, cas.Consistency.String()
	}
}
