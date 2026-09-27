// Package gate evaluates the soak harness's invariants (PLAN §6)
// as pure functions over recorded series, classified errors and component outcomes.
package gate

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// ClassNone and the other ErrorClass values are the rows of PLAN §6.1.
const (
	// ClassNone is the class of a nil error.
	ClassNone ErrorClass = iota
	// ClassStream is ErrNoStreams: never allowed.
	ClassStream
	// ClassDeadline is a caller or driver timeout.
	ClassDeadline
	// ClassServerTimeout is a coordinator read or write timeout.
	ClassServerTimeout
	// ClassCASUnknown is an LWT write whose outcome is unknown.
	ClassCASUnknown
	// ClassUnavailable is a coordinator reporting too few live replicas.
	ClassUnavailable
	// ClassOverloaded is a coordinator answering overloaded, which 5.0 also sends while shutting down.
	ClassOverloaded
	// ClassUnprepared is RequestErrUnprepared anywhere in the chain (PLAN §39).
	// Seen as the driver's capped re-prepare error, but a PREPARE's error passes through bare too,
	// so the class proves neither cap exhaustion nor server eviction.
	ClassUnprepared
	// ClassTransport is a broken or closed connection.
	ClassTransport
	// ClassNoConn is ErrNoConnections: no pooled host was available.
	ClassNoConn
	// ClassUnknown is anything else: never allowed.
	ClassUnknown
)

// heartbeatFailedMsg is the message of the heartbeat failure error,
// which the driver builds with fmt.Errorf rather than exporting a sentinel (conn.go:1006).
const heartbeatFailedMsg = "gocql: heartbeat failed"

// ErrorClass is the §6.1 class of a terminal operation error.
type ErrorClass int

// Classify maps a terminal operation error to its §6.1 class.
//
// Rows are tried in table order and the first match wins,
// so an error chain that matches several rows gets the earliest one.
// In particular context.DeadlineExceeded, which also satisfies net.Error,
// is a deadline and not a transport error.
//
// Parameters:
//   - err: the error returned to the harness caller; may be nil
//
// Returns:
//   - ErrorClass: ClassNone for nil, otherwise the first matching row
func Classify(err error) ErrorClass {
	switch {
	case err == nil:
		return ClassNone
	case errors.Is(err, gocql.ErrNoStreams):
		return ClassStream
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, gocql.ErrTimeoutNoResponse):
		return ClassDeadline
	case isAs[*gocql.RequestErrReadTimeout](err), isAs[*gocql.RequestErrWriteTimeout](err):
		return ClassServerTimeout
	case isAs[*gocql.RequestErrCASWriteUnknown](err):
		return ClassCASUnknown
	case isAs[*gocql.RequestErrUnavailable](err):
		return ClassUnavailable
	case isAs[*gocql.RequestErrOverloaded](err):
		return ClassOverloaded
	case isAs[*gocql.RequestErrUnprepared](err):
		return ClassUnprepared
	case isTransport(err):
		return ClassTransport
	case errors.Is(err, gocql.ErrNoConnections):
		return ClassNoConn
	default:
		return ClassUnknown
	}
}

// CASPartialAcceptMax bounds the failed attempt's elapsed for a protocol-4 CAS write timeout to be a partial accept (PLAN §36.4).
// Under §36.2's premises every other SERIAL or LOCAL_SERIAL CAS write timeout arrives after
// cas_contention_timeout (1000 ms) or write_request_timeout (2000 ms);
// a COMMIT failure can arrive sooner on 5.0.3 but carries the commit consistency, which partialAcceptShape rejects.
const CASPartialAcceptMax = time.Second

// ClassifyOp classifies a terminal operation error like Classify,
// then applies row 4's protocol-4 member (PLAN §36.4):
// below protocol 5 the server rewrites a Paxos partial accept, which is cas-unknown on protocol 5,
// as a write timeout with write type CAS and the Paxos consistency,
// so an LWT's SERIAL CAS write timeout with 1 ≤ received < block_for whose attempt took under CASPartialAcceptMax is cas-unknown.
// It applies only to a server-timeout result whose every read or write timeout has that shape, so earlier rows still win.
//
// Parameters:
//   - err: the error returned to the harness caller; may be nil
//   - lwt: the operation is of the LWT class
//   - proto: the cell's protocol version
//   - elapsed: the failed attempt's elapsed from the query observer; zero or negative when unknown
//
// Returns:
//   - ErrorClass: ClassCASUnknown when the rule fired, otherwise Classify's class
//   - bool: true when the rule fired
func ClassifyOp(err error, lwt bool, proto int, elapsed time.Duration) (ErrorClass, bool) {
	class := Classify(err)
	if class != ClassServerTimeout || !lwt || proto >= 5 || elapsed <= 0 || elapsed >= CASPartialAcceptMax {
		return class, false
	}
	qualifying := false
	other := anyInChain(err, func(e error) bool {
		// The driver returns pointers (frame.go); the value forms are errors too, so a joined tree can hold either.
		switch t := e.(type) {
		case *gocql.RequestErrReadTimeout, gocql.RequestErrReadTimeout:
			return true
		case *gocql.RequestErrWriteTimeout:
			if !partialAcceptShape(*t) {
				return true
			}
			qualifying = true
		case gocql.RequestErrWriteTimeout:
			if !partialAcceptShape(t) {
				return true
			}
			qualifying = true
		}
		return false
	})
	if other || !qualifying {
		return class, false
	}
	return ClassCASUnknown, true
}

// partialAcceptShape reports whether a write timeout has the protocol-4 partial accept's fields (PLAN §36.4 condition 4).
// A Paxos COMMIT failure has the commit consistency, never a serial one.
func partialAcceptShape(wt gocql.RequestErrWriteTimeout) bool {
	serial := wt.Consistency == gocql.Serial || wt.Consistency == gocql.LocalSerial
	return wt.WriteType == "CAS" && serial && wt.Received >= 1 && wt.Received < wt.BlockFor
}

// Op names the operation-type allowances of PLAN §6.1: the only rows an operation's type can relax.
type Op struct {
	// ShortDeadline is the short-deadline class: its timeouts are allowed anywhere (row 2).
	ShortDeadline bool
	// LWT is the LWT class: its cas-unknown outcomes, from Paxos contention, are allowed anywhere (row 4, v7.6).
	LWT bool
}

// Admit reports whether an error of class is allowed where it happened.
//
// Parameters:
//   - class: the error's §6.1 class
//   - inWindow: the error happened inside a fault window or a failed window
//   - op: the operation's type allowances
//
// Returns:
//   - bool: true when G8 allows the error, false when it is unexpected
func Admit(class ErrorClass, inWindow bool, op Op) bool {
	switch class {
	case ClassDeadline:
		return inWindow || op.ShortDeadline
	case ClassCASUnknown:
		return inWindow || op.LWT
	case ClassServerTimeout, ClassUnavailable, ClassOverloaded, ClassUnprepared, ClassTransport, ClassNoConn:
		return inWindow
	default:
		return false
	}
}

// UnavailableEvidence reports whether an unavailable error is evidence of a wrong liveness view.
// A one-node fault cannot leave more than one replica missing,
// so Alive < Required-1 means the coordinator saw extra nodes down.
//
// Parameters:
//   - err: the terminal error
//   - faultWidth: how many nodes the active fault takes down
//
// Returns:
//   - bool: true when err is RequestErrUnavailable and the counts are evidence
func UnavailableEvidence(err error, faultWidth int) bool {
	var u *gocql.RequestErrUnavailable
	if !errors.As(err, &u) {
		return false
	}
	return faultWidth == 1 && u.Alive < u.Required-1
}

// String returns the §6.1 name of the class.
func (c ErrorClass) String() string {
	switch c {
	case ClassNone:
		return "none"
	case ClassStream:
		return "stream"
	case ClassDeadline:
		return "deadline"
	case ClassServerTimeout:
		return "server-timeout"
	case ClassCASUnknown:
		return "cas-unknown"
	case ClassUnavailable:
		return "unavailable"
	case ClassOverloaded:
		return "overloaded"
	case ClassUnprepared:
		return "unprepared"
	case ClassTransport:
		return "transport"
	case ClassNoConn:
		return "no-conn"
	default:
		return "unknown"
	}
}

func isAs[T error](err error) bool {
	var target T
	return errors.As(err, &target)
}

func isTransport(err error) bool {
	for _, sentinel := range []error{io.EOF, io.ErrUnexpectedEOF, syscall.ECONNRESET, syscall.EPIPE, gocql.ErrConnectionClosed} {
		if errors.Is(err, sentinel) {
			return true
		}
	}
	if isAs[net.Error](err) {
		return true
	}
	return anyInChain(err, func(e error) bool { return e.Error() == heartbeatFailedMsg })
}

// anyInChain walks err's whole tree, both Unwrap() error and Unwrap() []error.
func anyInChain(err error, match func(error) bool) bool {
	if err == nil {
		return false
	}
	if match(err) {
		return true
	}
	switch u := err.(type) {
	case interface{ Unwrap() error }:
		return anyInChain(u.Unwrap(), match)
	case interface{ Unwrap() []error }:
		for _, e := range u.Unwrap() {
			if anyInChain(e, match) {
				return true
			}
		}
	}
	return false
}
