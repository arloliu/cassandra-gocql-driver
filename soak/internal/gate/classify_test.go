package gate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// unprepared is held as an error, as the driver returns it, so %w wraps it as it is wrapped in the wild.
var unprepared error = &gocql.RequestErrUnprepared{}

func TestClassify(t *testing.T) {
	heartbeat := fmt.Errorf("gocql: heartbeat failed")
	// The driver returns the pointer (frame.go); held as an error so %w wraps it as it would be wrapped in the wild.
	var overloaded error = &gocql.RequestErrOverloaded{}
	tests := []struct {
		name string
		err  error
		want ErrorClass
	}{
		{"nil", nil, ClassNone},
		{"no streams", gocql.ErrNoStreams, ClassStream},
		{"wrapped no streams", fmt.Errorf("exec: %w", gocql.ErrNoStreams), ClassStream},
		{"context deadline", context.DeadlineExceeded, ClassDeadline},
		{"wrapped context deadline", fmt.Errorf("op: %w", context.DeadlineExceeded), ClassDeadline},
		{"no response", gocql.ErrTimeoutNoResponse, ClassDeadline},
		{"read timeout", &gocql.RequestErrReadTimeout{}, ClassServerTimeout},
		{"write timeout", &gocql.RequestErrWriteTimeout{}, ClassServerTimeout},
		{"cas write unknown", &gocql.RequestErrCASWriteUnknown{}, ClassCASUnknown},
		{"unavailable", &gocql.RequestErrUnavailable{Required: 2, Alive: 1}, ClassUnavailable},
		{"overloaded", &gocql.RequestErrOverloaded{}, ClassOverloaded},
		{"wrapped overloaded", fmt.Errorf("exec: %w", overloaded), ClassOverloaded},
		{"unprepared", &gocql.RequestErrUnprepared{}, ClassUnprepared},
		// The driver's re-prepare cap wraps the server's error with %w on both paths (conn.go).
		{"query re-prepare cap", fmt.Errorf("gocql: failed to execute prepared statement after 6 re-prepare attempts: %w", unprepared), ClassUnprepared},
		{"batch re-prepare cap", fmt.Errorf("gocql: failed to execute batch after 6 re-prepare attempts: %w", unprepared), ClassUnprepared},
		{"bootstrapping stays unknown", &gocql.RequestErrBootstrapping{}, ClassUnknown},
		{"eof", io.EOF, ClassTransport},
		{"unexpected eof", io.ErrUnexpectedEOF, ClassTransport},
		{"net op error", &net.OpError{Op: "read", Err: errors.New("boom")}, ClassTransport},
		{"econnreset", fmt.Errorf("write: %w", syscall.ECONNRESET), ClassTransport},
		{"epipe", syscall.EPIPE, ClassTransport},
		{"connection closed", gocql.ErrConnectionClosed, ClassTransport},
		{"heartbeat", heartbeat, ClassTransport},
		{"wrapped heartbeat", fmt.Errorf("conn: %w", heartbeat), ClassTransport},
		{"joined heartbeat", errors.Join(errors.New("other"), heartbeat), ClassTransport},
		{"heartbeat prefix is not enough", errors.New("gocql: heartbeat failed: extra"), ClassUnknown},
		{"no connections", gocql.ErrNoConnections, ClassNoConn},
		{"canceled", context.Canceled, ClassUnknown},
		{"not found", gocql.ErrNotFound, ClassUnknown},
		{"other", errors.New("canary"), ClassUnknown},
		{"syntax", &gocql.RequestErrSyntax{}, ClassUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, Classify(tt.err))
		})
	}
}

// The first matching row wins (PLAN §6.1).
func TestClassifyPrecedence(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want ErrorClass
	}{
		// context.DeadlineExceeded also satisfies net.Error.
		{"deadline before transport", context.DeadlineExceeded, ClassDeadline},
		{"stream before deadline", errors.Join(context.DeadlineExceeded, gocql.ErrNoStreams), ClassStream},
		{"deadline before server timeout", errors.Join(&gocql.RequestErrReadTimeout{}, gocql.ErrTimeoutNoResponse), ClassDeadline},
		{"stream before cas unknown", errors.Join(&gocql.RequestErrCASWriteUnknown{}, gocql.ErrNoStreams), ClassStream},
		{"server timeout before cas unknown", errors.Join(&gocql.RequestErrCASWriteUnknown{}, &gocql.RequestErrWriteTimeout{WriteType: "CAS"}), ClassServerTimeout},
		{"server timeout before unavailable", errors.Join(&gocql.RequestErrUnavailable{}, &gocql.RequestErrWriteTimeout{}), ClassServerTimeout},
		{"cas unknown before unavailable", errors.Join(&gocql.RequestErrUnavailable{}, &gocql.RequestErrCASWriteUnknown{}), ClassCASUnknown},
		{"unavailable before overloaded", errors.Join(&gocql.RequestErrOverloaded{}, &gocql.RequestErrUnavailable{}), ClassUnavailable},
		{"overloaded before transport", errors.Join(io.EOF, &gocql.RequestErrOverloaded{}), ClassOverloaded},
		{"overloaded before unprepared", errors.Join(unprepared, &gocql.RequestErrOverloaded{}), ClassOverloaded},
		{"unavailable before unprepared", errors.Join(unprepared, &gocql.RequestErrUnavailable{}), ClassUnavailable},
		{"stream before unprepared", errors.Join(unprepared, gocql.ErrNoStreams), ClassStream},
		{"server timeout before unprepared", errors.Join(unprepared, &gocql.RequestErrReadTimeout{}), ClassServerTimeout},
		{"unprepared before transport", errors.Join(io.EOF, unprepared), ClassUnprepared},
		{"unprepared before no-conn", errors.Join(gocql.ErrNoConnections, unprepared), ClassUnprepared},
		{"overloaded inside a net error", &net.OpError{Op: "read", Err: &gocql.RequestErrOverloaded{}}, ClassOverloaded},
		{"unavailable before transport", errors.Join(io.EOF, &gocql.RequestErrUnavailable{}), ClassUnavailable},
		{"transport before no-conn", errors.Join(gocql.ErrNoConnections, io.EOF), ClassTransport},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, Classify(tt.err))
		})
	}
}

func TestAdmit(t *testing.T) {
	none, short, lwt := Op{}, Op{ShortDeadline: true}, Op{LWT: true}
	tests := []struct {
		class    ErrorClass
		inWindow bool
		op       Op
		want     bool
	}{
		{ClassStream, false, none, false},
		{ClassStream, true, none, false},
		{ClassStream, true, short, false},
		{ClassStream, true, lwt, false},
		{ClassDeadline, false, none, false},
		{ClassDeadline, false, short, true},
		{ClassDeadline, false, lwt, false},
		{ClassDeadline, true, none, true},
		{ClassServerTimeout, false, none, false},
		{ClassServerTimeout, false, short, false},
		{ClassServerTimeout, false, lwt, false},
		{ClassServerTimeout, true, none, true},
		{ClassCASUnknown, false, none, false},
		{ClassCASUnknown, false, short, false},
		{ClassCASUnknown, false, lwt, true},
		{ClassCASUnknown, true, none, true},
		{ClassUnavailable, false, none, false},
		{ClassUnavailable, false, lwt, false},
		{ClassUnavailable, true, none, true},
		{ClassOverloaded, false, none, false},
		{ClassOverloaded, false, short, false},
		{ClassOverloaded, false, lwt, false},
		{ClassOverloaded, true, none, true},
		{ClassUnprepared, false, none, false},
		{ClassUnprepared, false, short, false},
		{ClassUnprepared, false, lwt, false},
		{ClassUnprepared, true, none, true},
		{ClassUnprepared, true, short, true},
		{ClassUnprepared, true, lwt, true},
		{ClassTransport, false, none, false},
		{ClassTransport, false, lwt, false},
		{ClassTransport, true, none, true},
		{ClassNoConn, false, none, false},
		{ClassNoConn, true, none, true},
		{ClassUnknown, false, none, false},
		{ClassUnknown, true, short, false},
		{ClassUnknown, true, lwt, false},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("%s window=%v %+v", tt.class, tt.inWindow, tt.op)
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tt.want, Admit(tt.class, tt.inWindow, tt.op))
		})
	}
}

func TestUnavailableEvidence(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		faultWidth int
		want       bool
	}{
		{"one-node fault, alive short by two", &gocql.RequestErrUnavailable{Required: 3, Alive: 1}, 1, true},
		{"one-node fault, alive short by one", &gocql.RequestErrUnavailable{Required: 3, Alive: 2}, 1, false},
		{"two-node fault is never evidence", &gocql.RequestErrUnavailable{Required: 3, Alive: 0}, 2, false},
		{"wrapped", fmt.Errorf("op: %w", asError(&gocql.RequestErrUnavailable{Required: 2, Alive: 0})), 1, true},
		{"not unavailable", io.EOF, 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, UnavailableEvidence(tt.err, tt.faultWidth))
		})
	}
}

// asError widens a driver error to the error interface,
// so wrapping it with %w does not trip vet's pointer-receiver check.
func asError(err error) error { return err }

func TestClassifyOp(t *testing.T) {
	partial := func(received, blockFor int, writeType string) error {
		return &gocql.RequestErrWriteTimeout{WriteType: writeType, Received: received, BlockFor: blockFor, Consistency: gocql.Serial}
	}
	withCL := func(cl gocql.Consistency) error {
		return &gocql.RequestErrWriteTimeout{WriteType: "CAS", Received: 1, BlockFor: 2, Consistency: cl}
	}
	casP4 := partial(1, 2, "CAS")
	ms := time.Millisecond
	tests := []struct {
		name     string
		err      error
		lwt      bool
		proto    int
		elapsed  time.Duration
		want     ErrorClass
		wantRule bool
	}{
		{"p4 partial accept, 3 ms", casP4, true, 4, 3 * ms, ClassCASUnknown, true},
		{"p4 partial accept, just under 1 s", casP4, true, 4, time.Second - time.Nanosecond, ClassCASUnknown, true},
		{"p4 partial accept, 999 ms", casP4, true, 4, 999 * ms, ClassCASUnknown, true},
		{"p4 at 1 s stays row 3", casP4, true, 4, time.Second, ClassServerTimeout, false},
		{"p4 commit-timeout shape at 2 s stays row 3", casP4, true, 4, 2 * time.Second, ClassServerTimeout, false},
		{"p4 elapsed unknown stays row 3", casP4, true, 4, 0, ClassServerTimeout, false},
		{"p4 negative elapsed stays row 3", casP4, true, 4, -ms, ClassServerTimeout, false},
		{"p4 received 0 stays row 3", partial(0, 2, "CAS"), true, 4, 3 * ms, ClassServerTimeout, false},
		{"p4 received equal to block_for stays row 3", partial(2, 2, "CAS"), true, 4, 3 * ms, ClassServerTimeout, false},
		{"p4 received 1 of 3", partial(1, 3, "CAS"), true, 4, 3 * ms, ClassCASUnknown, true},
		{"p4 received 2 of 3", partial(2, 3, "CAS"), true, 4, 3 * ms, ClassCASUnknown, true},
		{"p4 write type SIMPLE stays row 3", partial(1, 2, "SIMPLE"), true, 4, 3 * ms, ClassServerTimeout, false},
		{"p4 non-LWT stays row 3", casP4, false, 4, 3 * ms, ClassServerTimeout, false},
		{"p5 CAS write timeout stays row 3", casP4, true, 5, 3 * ms, ClassServerTimeout, false},
		{"p3 partial accept", casP4, true, 3, 3 * ms, ClassCASUnknown, true},
		{"p4 read timeout stays row 3", &gocql.RequestErrReadTimeout{Received: 1, BlockFor: 2}, true, 4, 3 * ms, ClassServerTimeout, false},
		{"p4 wrapped partial accept", fmt.Errorf("lwt: %w", casP4), true, 4, 3 * ms, ClassCASUnknown, true},
		{"p5 cas unknown is row 4 without the rule", &gocql.RequestErrCASWriteUnknown{Received: 1, BlockFor: 2}, true, 5, 3 * ms, ClassCASUnknown, false},
		{"combined cas unknown and p4 partial accept", errors.Join(&gocql.RequestErrCASWriteUnknown{}, casP4), true, 4, 3 * ms, ClassCASUnknown, true},
		{"combined cas unknown and p4 write timeout at 2 s", errors.Join(&gocql.RequestErrCASWriteUnknown{}, casP4), true, 4, 2 * time.Second, ClassServerTimeout, false},
		{"no streams wins over the rule", errors.Join(casP4, gocql.ErrNoStreams), true, 4, 3 * ms, ClassStream, false},
		{"deadline wins over the rule", errors.Join(casP4, context.DeadlineExceeded), true, 4, 3 * ms, ClassDeadline, false},
		{"p4 LOCAL_SERIAL partial accept", withCL(gocql.LocalSerial), true, 4, 3 * ms, ClassCASUnknown, true},
		{"p4 QUORUM commit failure stays row 3", withCL(gocql.Quorum), true, 4, 3 * ms, ClassServerTimeout, false},
		{"p4 consistency unset stays row 3", withCL(gocql.Any), true, 4, 3 * ms, ClassServerTimeout, false},
		{"p4 partial accept joined with a read timeout stays row 3",
			errors.Join(casP4, &gocql.RequestErrReadTimeout{Received: 1, BlockFor: 2}), true, 4, 3 * ms, ClassServerTimeout, false},
		{"p4 partial accept joined with a QUORUM write timeout stays row 3",
			errors.Join(casP4, withCL(gocql.Quorum)), true, 4, 3 * ms, ClassServerTimeout, false},
		{"p4 QUORUM write timeout joined with a partial accept stays row 3",
			errors.Join(withCL(gocql.Quorum), casP4), true, 4, 3 * ms, ClassServerTimeout, false},
		{"p4 partial accept joined with a value read timeout stays row 3",
			errors.Join(casP4, gocql.RequestErrReadTimeout{}), true, 4, 3 * ms, ClassServerTimeout, false},
		{"p4 value read timeout joined with a partial accept stays row 3",
			errors.Join(gocql.RequestErrReadTimeout{}, casP4), true, 4, 3 * ms, ClassServerTimeout, false},
		{"p4 partial accept joined with a wrapped value QUORUM write timeout stays row 3",
			errors.Join(casP4, fmt.Errorf("w: %w", gocql.RequestErrWriteTimeout{WriteType: "CAS", Received: 1, BlockFor: 2, Consistency: gocql.Quorum})),
			true, 4, 3 * ms, ClassServerTimeout, false},
		{"p4 value partial accept joined with a pointer one",
			errors.Join(casP4, gocql.RequestErrWriteTimeout{WriteType: "CAS", Received: 1, BlockFor: 2, Consistency: gocql.Serial}),
			true, 4, 3 * ms, ClassCASUnknown, true},
		{"p4 two qualifying write timeouts", errors.Join(casP4, partial(1, 3, "CAS")), true, 4, 3 * ms, ClassCASUnknown, true},
		{"nil", nil, true, 4, 3 * ms, ClassNone, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, rule := ClassifyOp(tt.err, tt.lwt, tt.proto, tt.elapsed)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.wantRule, rule)
		})
	}
}

func TestClassifyOpAdmission(t *testing.T) {
	casP4 := &gocql.RequestErrWriteTimeout{WriteType: "CAS", Received: 1, BlockFor: 2, Consistency: gocql.Serial}
	tests := []struct {
		name     string
		elapsed  time.Duration
		inWindow bool
		want     bool
	}{
		{"under 1 s outside a window", 3 * time.Millisecond, false, true},
		{"under 1 s inside a window", 3 * time.Millisecond, true, true},
		{"1 s outside a window", time.Second, false, false},
		{"1 s inside a window", time.Second, true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			class, _ := ClassifyOp(casP4, true, 4, tt.elapsed)
			require.Equal(t, tt.want, Admit(class, tt.inWindow, Op{LWT: true}))
		})
	}
}
