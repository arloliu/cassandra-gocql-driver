package probe

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
)

// Driver log messages G7 counts (PLAN §6).
const (
	msgGoroutinePanicked = "Goroutine panicked."
	msgNoHandler         = "Received response for stream which has no handler."
	msgSchemaAgreement   = "Error while awaiting for schema agreement after a schema change event."
	msgEventBufferFull   = "Event buffer full, dropping event frame."
	// msgIterNotClosed is a substring: the driver's line is longer and prefixed (session.go:2915).
	msgIterNotClosed = "Iter was garbage-collected without Close()"
)

// StreamCounters counts one session's streams for G6: started, finished, abandoned.
type StreamCounters struct {
	started, finished, abandoned atomic.Int64
}

var _ gocql.StreamObserver = (*StreamCounters)(nil)

// streamContext forwards one stream's transitions to its session's counters.
type streamContext struct {
	c *StreamCounters
}

var _ gocql.StreamObserverContext = streamContext{}

// CountingLogger is the driver's StructuredLogger.
// It counts the G7 lines and writes Info and above to driver.log as JSON lines.
type CountingLogger struct {
	session string
	mu      sync.Mutex
	out     io.Writer
	// Counters, one per G7 line.
	panicked, noHandler, iterNotClosed, schemaAgreement, eventBufferFull atomic.Int64
}

var _ gocql.StructuredLogger = (*CountingLogger)(nil)

// logLine is one driver.log entry.
type logLine struct {
	Time    time.Time      `json:"time"`
	Session string         `json:"session"`
	Level   string         `json:"level"`
	Msg     string         `json:"msg"`
	Fields  map[string]any `json:"fields,omitempty"`
}

// NewCountingLogger returns a logger for one session.
//
// Parameters:
//   - session: the harness's session id, written on every line
//   - out: driver.log; shared by the sessions, so writes are serialized per logger
//
// Returns:
//   - *CountingLogger: the logger
func NewCountingLogger(session string, out io.Writer) *CountingLogger {
	return &CountingLogger{session: session, out: out}
}

// StreamContext returns the per-stream observer.
//
// Parameters:
//   - ctx: the request context, unused
//
// Returns:
//   - gocql.StreamObserverContext: forwards to these counters
func (c *StreamCounters) StreamContext(context.Context) gocql.StreamObserverContext {
	return streamContext{c: c}
}

// Snapshot returns the three counters.
//
// Returns:
//   - started, finished, abandoned: the counts so far
func (c *StreamCounters) Snapshot() (started, finished, abandoned int64) {
	return c.started.Load(), c.finished.Load(), c.abandoned.Load()
}

// Balance returns started − finished − abandoned, the streams still outstanding.
//
// Returns:
//   - int64: the balance
func (c *StreamCounters) Balance() int64 {
	s, f, a := c.Snapshot()
	return s - f - a
}

// StreamStarted counts a started stream.
func (s streamContext) StreamStarted(gocql.ObservedStream) { s.c.started.Add(1) }

// StreamFinished counts a stream that received its response.
func (s streamContext) StreamFinished(gocql.ObservedStream) { s.c.finished.Add(1) }

// StreamAbandoned counts a stream whose connection closed first.
func (s streamContext) StreamAbandoned(gocql.ObservedStream) { s.c.abandoned.Add(1) }

// Counts returns the G7 counters.
//
// Returns:
//   - gate.LogCounts: the counts so far
func (l *CountingLogger) Counts() gate.LogCounts {
	return gate.LogCounts{
		GoroutinePanicked: int(l.panicked.Load()),
		NoHandler:         int(l.noHandler.Load()),
		IterNotClosed:     int(l.iterNotClosed.Load()),
		SchemaAgreement:   int(l.schemaAgreement.Load()),
		EventBufferFull:   int(l.eventBufferFull.Load()),
	}
}

// Error counts and writes an error line.
func (l *CountingLogger) Error(msg string, fields ...gocql.LogField) { l.log("error", msg, fields) }

// Warning counts and writes a warning line.
func (l *CountingLogger) Warning(msg string, fields ...gocql.LogField) {
	l.log("warning", msg, fields)
}

// Info counts and writes an info line.
func (l *CountingLogger) Info(msg string, fields ...gocql.LogField) { l.log("info", msg, fields) }

// Debug counts a debug line; debug lines are not written.
func (l *CountingLogger) Debug(msg string, fields ...gocql.LogField) { l.count(msg) }

func (l *CountingLogger) log(level, msg string, fields []gocql.LogField) {
	l.count(msg)
	if l.out == nil {
		return
	}
	line := logLine{Time: time.Now(), Session: l.session, Level: level, Msg: msg}
	if len(fields) > 0 {
		line.Fields = make(map[string]any, len(fields))
		for _, f := range fields {
			line.Fields[f.Name] = f.Value.String()
		}
	}
	raw, err := json.Marshal(line)
	if err != nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.out.Write(append(raw, '\n'))
}

func (l *CountingLogger) count(msg string) {
	switch msg {
	case msgGoroutinePanicked:
		l.panicked.Add(1)
	case msgNoHandler:
		l.noHandler.Add(1)
	case msgSchemaAgreement:
		l.schemaAgreement.Add(1)
	case msgEventBufferFull:
		l.eventBufferFull.Add(1)
	default:
		if strings.Contains(msg, msgIterNotClosed) {
			l.iterNotClosed.Add(1)
		}
	}
}
