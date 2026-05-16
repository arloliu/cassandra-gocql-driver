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
	"errors"
	"strings"
	"sync"
	"testing"
)

// captureLogger is a minimal StructuredLogger that records the most recent
// log call. Used by recover_test to assert the helper logged with the
// expected fields.
type captureLogger struct {
	mu     sync.Mutex
	level  LogLevel
	msg    string
	fields []LogField
}

func (c *captureLogger) record(level LogLevel, msg string, fields ...LogField) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.level = level
	c.msg = msg
	c.fields = append([]LogField(nil), fields...)
}

func (c *captureLogger) Debug(msg string, fields ...LogField) {
	c.record(LogLevelDebug, msg, fields...)
}
func (c *captureLogger) Info(msg string, fields ...LogField) { c.record(LogLevelInfo, msg, fields...) }
func (c *captureLogger) Warning(msg string, fields ...LogField) {
	c.record(LogLevelWarn, msg, fields...)
}
func (c *captureLogger) Error(msg string, fields ...LogField) {
	c.record(LogLevelError, msg, fields...)
}

func (c *captureLogger) snapshot() (LogLevel, string, []LogField) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.level, c.msg, append([]LogField(nil), c.fields...)
}

// panickingLogger satisfies StructuredLogger but panics on Error. It
// exercises the helper's secondary-recover path around the logger call.
type panickingLogger struct{}

func (panickingLogger) Trace(msg string, fields ...LogField)   {}
func (panickingLogger) Debug(msg string, fields ...LogField)   {}
func (panickingLogger) Info(msg string, fields ...LogField)    {}
func (panickingLogger) Warning(msg string, fields ...LogField) {}
func (panickingLogger) Error(msg string, fields ...LogField) {
	panic("logger Error panicked")
}

// runWithRecover invokes f via a goroutine + recoverGoroutine, and waits
// for that goroutine to exit. Returns whether recoverGoroutine itself
// allowed any panic to escape (it must not, ever).
func runWithRecover(logger StructuredLogger, name string, teardown func(error), f func()) (escaped bool) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			// If recoverGoroutine ever lets a panic escape, this recover
			// catches it and we flag it.
			if r := recover(); r != nil {
				escaped = true
			}
		}()
		defer recoverGoroutine(logger, name, teardown)
		f()
	}()
	<-done
	return escaped
}

func TestRecoverGoroutine_NoPanicNoAction(t *testing.T) {
	logger := &captureLogger{}
	teardownCalled := false
	escaped := runWithRecover(logger, "test", func(err error) { teardownCalled = true }, func() {
		// no panic
	})
	if escaped {
		t.Fatalf("recoverGoroutine let a panic escape on a non-panicking call")
	}
	if teardownCalled {
		t.Fatalf("teardown invoked despite no panic")
	}
	if _, msg, _ := logger.snapshot(); msg != "" {
		t.Fatalf("logger was called despite no panic; msg=%q", msg)
	}
}

func TestRecoverGoroutine_RecoversAndLogs(t *testing.T) {
	logger := &captureLogger{}
	escaped := runWithRecover(logger, "test.goroutine", nil, func() {
		panic("boom")
	})
	if escaped {
		t.Fatalf("recoverGoroutine let panic escape")
	}
	level, msg, fields := logger.snapshot()
	if level != LogLevelError {
		t.Fatalf("expected Error level, got %v", level)
	}
	if !strings.Contains(msg, "panicked") {
		t.Fatalf("expected message about panic, got %q", msg)
	}
	// Verify field names present
	wantFields := map[string]bool{"goroutine": false, "panic": false, "stack": false}
	for _, f := range fields {
		if _, ok := wantFields[f.Name]; ok {
			wantFields[f.Name] = true
		}
	}
	for name, found := range wantFields {
		if !found {
			t.Errorf("expected log field %q, not found", name)
		}
	}
}

func TestRecoverGoroutine_InvokesTeardown(t *testing.T) {
	logger := &captureLogger{}
	var teardownErr error
	escaped := runWithRecover(logger, "test", func(err error) { teardownErr = err }, func() {
		panic(errors.New("boom"))
	})
	if escaped {
		t.Fatalf("recoverGoroutine let panic escape")
	}
	if teardownErr == nil {
		t.Fatalf("teardown was not invoked")
	}
	if !strings.Contains(teardownErr.Error(), "boom") {
		t.Fatalf("teardown error should contain panic value, got %v", teardownErr)
	}
}

func TestRecoverGoroutine_NilTeardownNoop(t *testing.T) {
	logger := &captureLogger{}
	escaped := runWithRecover(logger, "test", nil, func() {
		panic("boom")
	})
	if escaped {
		t.Fatalf("recoverGoroutine let panic escape with nil teardown")
	}
	if _, msg, _ := logger.snapshot(); !strings.Contains(msg, "panicked") {
		t.Fatalf("expected logger to be called despite nil teardown")
	}
}

func TestRecoverGoroutine_NilLoggerFallsBackToStderr(t *testing.T) {
	// We can't easily redirect os.Stderr in a portable way; just verify
	// the helper does not panic with a nil logger.
	escaped := runWithRecover(nil, "test", nil, func() {
		panic("boom-no-logger")
	})
	if escaped {
		t.Fatalf("recoverGoroutine let panic escape with nil logger")
	}
}

func TestRecoverGoroutine_LoggerPanicAbsorbed(t *testing.T) {
	teardownCalled := false
	escaped := runWithRecover(panickingLogger{}, "test", func(err error) { teardownCalled = true }, func() {
		panic("primary boom")
	})
	if escaped {
		t.Fatalf("logger panic escaped through recoverGoroutine")
	}
	if !teardownCalled {
		t.Fatalf("teardown must run even if logger panicked")
	}
}

func TestRecoverGoroutine_TeardownPanicAbsorbed(t *testing.T) {
	logger := &captureLogger{}
	escaped := runWithRecover(logger, "test", func(err error) {
		panic("teardown boom")
	}, func() {
		panic("primary boom")
	})
	if escaped {
		t.Fatalf("teardown panic escaped through recoverGoroutine")
	}
	// Primary panic should still have been logged.
	if _, msg, _ := logger.snapshot(); !strings.Contains(msg, "panicked") {
		t.Fatalf("primary panic should still be logged even if teardown panics; got msg=%q", msg)
	}
}

func TestRecoverGoroutine_BothLoggerAndTeardownPanic(t *testing.T) {
	// Worst-case path: logger panics AND teardown panics. Helper must
	// still not propagate.
	teardownCalled := false
	escaped := runWithRecover(panickingLogger{}, "test", func(err error) {
		teardownCalled = true
		panic("teardown boom")
	}, func() {
		panic("primary boom")
	})
	if escaped {
		t.Fatalf("recoverGoroutine let a panic escape when both logger and teardown panicked")
	}
	if !teardownCalled {
		t.Fatalf("teardown should have been invoked at least once")
	}
}

func TestRecoverGoroutine_PanicWithStackPreserved(t *testing.T) {
	// Simulate an inner re-panic site (refreshDebouncer.flusher pattern).
	logger := &captureLogger{}
	customStack := []byte("CUSTOM_INNER_STACK_FRAME")
	escaped := runWithRecover(logger, "test", nil, func() {
		panic(panicWithStack{value: errors.New("original cause"), stack: customStack})
	})
	if escaped {
		t.Fatalf("recoverGoroutine let panicWithStack escape")
	}
	_, _, fields := logger.snapshot()
	stackLogged := ""
	panicLogged := ""
	for _, f := range fields {
		switch f.Name {
		case "stack":
			stackLogged = f.Value.Any().(string)
		case "panic":
			panicLogged = f.Value.Any().(string)
		}
	}
	if !strings.Contains(stackLogged, "CUSTOM_INNER_STACK_FRAME") {
		t.Fatalf("expected wrapped stack to be logged, got %q", stackLogged)
	}
	if !strings.Contains(panicLogged, "original cause") {
		t.Fatalf("expected unwrapped panic value, got %q", panicLogged)
	}
}
