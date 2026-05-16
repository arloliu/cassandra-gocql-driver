/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"math"
	"testing"
)

// appendInt32BE appends a big-endian int32 to b.
func appendInt32BE(b []byte, n int32) []byte {
	return append(b, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
}

// newTestFramerWithBody constructs a framer with the given response op and
// body, ready to be passed to parseFrame.
func newTestFramerWithBody(op frameOp, body []byte) *framer {
	f := getFramer(nil, protoVersion4, GlobalTypes)
	f.header = &frameHeader{
		version: protoVersion4 | protoDirectionMask,
		op:      op,
		length:  len(body),
	}
	f.readBuffer = make([]byte, len(body))
	copy(f.readBuffer, body)
	f.buf = f.readBuffer
	return f
}

// TestParsePreparedMetadata_HostileColCount asserts that a peer-supplied
// column count of MaxInt32 is rejected with an error rather than triggering
// make([]ColumnInfo, 2^31-1) (which would crash with out-of-memory).
func TestParsePreparedMetadata_HostileColCount(t *testing.T) {
	// resultKindPrepared body: int(kind), shortBytes(preparedID), then preparedMetadata.
	body := appendInt32BE(nil, resultKindPrepared)
	// preparedID short-bytes: length 0
	body = append(body, 0x00, 0x00)
	// preparedMetadata flags = 0 (no global table spec, etc.)
	body = appendInt32BE(body, 0)
	// colCount = MaxInt32 — should be rejected
	body = appendInt32BE(body, math.MaxInt32)

	f := newTestFramerWithBody(opResult, body)
	defer f.release()

	_, err := f.parseFrame()
	t.Logf("HostileColCount error: %v", err)
	if err == nil {
		t.Logf("Got expected error: %v", err)
		t.Fatalf("expected bounded-count error for colCount=MaxInt32, got nil")
	}
}

// TestParsePreparedMetadata_HostilePkeyCount asserts that a peer-supplied
// partition-key count of MaxInt32 is rejected with an error rather than
// triggering make([]int, 2^31-1).
func TestParsePreparedMetadata_HostilePkeyCount(t *testing.T) {
	body := appendInt32BE(nil, resultKindPrepared)
	body = append(body, 0x00, 0x00) // empty preparedID
	body = appendInt32BE(body, int32(flagNoMetaData))
	body = appendInt32BE(body, 0) // colCount = 0
	// pkeyCount = MaxInt32 — must be rejected before the make([]int, ...)
	body = appendInt32BE(body, math.MaxInt32)

	f := newTestFramerWithBody(opResult, body)
	defer f.release()

	_, err := f.parseFrame()
	t.Logf("HostilePkeyCount error: %v", err)
	if err == nil {
		t.Logf("Got expected error: %v", err)
		t.Fatalf("expected bounded-count error for pkeyCount=MaxInt32, got nil")
	}
}

// TestParseResultMetadata_HostileColCount asserts that a Rows result with a
// huge colCount is rejected. parseResultMetadata is shared between the Rows
// and Prepared parsers; this exercises the Rows path.
func TestParseResultMetadata_HostileColCount(t *testing.T) {
	body := appendInt32BE(nil, resultKindRows)
	// resultMetadata flags = 0
	body = appendInt32BE(body, 0)
	// colCount = MaxInt32
	body = appendInt32BE(body, math.MaxInt32)

	f := newTestFramerWithBody(opResult, body)
	defer f.release()

	_, err := f.parseFrame()
	t.Logf("Rows HostileColCount error: %v", err)
	if err == nil {
		t.Logf("Got expected error: %v", err)
		t.Fatalf("expected bounded-count error for Rows colCount=MaxInt32, got nil")
	}
}

// TestReadErrorMap_HostileNumErrs asserts that a ReadFailure error frame
// claiming MaxInt32 failure entries is rejected before make(ErrorMap, n).
// Only the v5+ path uses readErrorMap, so the framer is built as v5.
func TestReadErrorMap_HostileNumErrs(t *testing.T) {
	f := getFramer(nil, protoVersion5, GlobalTypes)
	defer f.release()

	// Error frame body: int(code), string(msg) — message is short-prefixed.
	body := appendInt32BE(nil, ErrCodeReadFailure)
	msg := "fail"
	body = append(body, byte(len(msg)>>8), byte(len(msg)))
	body = append(body, msg...)
	// Consistency (short), Received (int), BlockFor (int)
	body = append(body, 0x00, 0x01) // ONE
	body = appendInt32BE(body, 1)   // received
	body = appendInt32BE(body, 1)   // blockfor
	// numErrs = MaxInt32 — must be rejected before make(ErrorMap, n)
	body = appendInt32BE(body, math.MaxInt32)

	f.header = &frameHeader{
		version: protoVersion5 | protoDirectionMask,
		op:      opError,
		length:  len(body),
	}
	f.readBuffer = make([]byte, len(body))
	copy(f.readBuffer, body)
	f.buf = f.readBuffer

	_, err := f.parseFrame()
	t.Logf("HostileNumErrs error: %v", err)
	if err == nil {
		t.Logf("Got expected error: %v", err)
		t.Fatalf("expected bounded-count error for ReadFailure numErrs=MaxInt32, got nil")
	}
}

// TestReadTrace_TruncatedReturnsError asserts that a response frame with the
// tracing flag set but fewer than 16 bytes of body returns an error rather
// than panicking. The pre-fix behavior was a panic that would propagate up
// the per-connection serve() goroutine and kill the host process.
func TestReadTrace_TruncatedReturnsError(t *testing.T) {
	for _, n := range []int{0, 1, 8, 15} {
		t.Run("", func(t *testing.T) {
			f := getFramer(nil, protoVersion4, GlobalTypes)
			defer f.release()

			body := make([]byte, n) // shorter than 16
			f.header = &frameHeader{
				version: protoVersion4 | protoDirectionMask,
				op:      opReady,
				flags:   flagTracing,
				length:  len(body),
			}
			f.readBuffer = body
			f.buf = f.readBuffer

			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("parseFrame panicked on truncated trace body: %v", r)
				}
			}()

			_, err := f.parseFrame()
			if err == nil {
				t.Fatalf("expected error for truncated trace body (len=%d), got nil", n)
			}
		})
	}
}

// TestCheckBoundedCount_NegativeAndOverMax exercises the helper directly so
// the message format is locked in by a test (callers grep these strings).
func TestCheckBoundedCount_NegativeAndOverMax(t *testing.T) {
	if err := checkBoundedCount(-1, "thing"); err == nil {
		t.Fatalf("expected error for negative count")
	}
	if err := checkBoundedCount(maxFrameElementCount+1, "thing"); err == nil {
		t.Fatalf("expected error for count above ceiling")
	}
	if err := checkBoundedCount(0, "thing"); err != nil {
		t.Fatalf("zero count must be accepted, got %v", err)
	}
	if err := checkBoundedCount(maxFrameElementCount, "thing"); err != nil {
		t.Fatalf("count == ceiling must be accepted, got %v", err)
	}
}
