//go:build all || unit
// +build all unit

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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// logRecord is one call made to a historyLogger.
type logRecord struct {
	level  LogLevel
	msg    string
	fields []LogField
}

// historyLogger records every log call, unlike captureLogger which keeps only the last.
type historyLogger struct {
	mu      sync.Mutex
	records []logRecord
}

var _ StructuredLogger = (*historyLogger)(nil)

func (l *historyLogger) record(level LogLevel, msg string, fields ...LogField) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, logRecord{level: level, msg: msg, fields: append([]LogField(nil), fields...)})
}

// Debug records a debug-level call.
func (l *historyLogger) Debug(msg string, fields ...LogField) {
	l.record(LogLevelDebug, msg, fields...)
}

// Info records an info-level call.
func (l *historyLogger) Info(msg string, fields ...LogField) { l.record(LogLevelInfo, msg, fields...) }

// Warning records a warning-level call.
func (l *historyLogger) Warning(msg string, fields ...LogField) {
	l.record(LogLevelWarn, msg, fields...)
}

// Error records an error-level call.
func (l *historyLogger) Error(msg string, fields ...LogField) {
	l.record(LogLevelError, msg, fields...)
}

// withMessage returns every record whose message equals msg.
//
// Returns:
//   - []logRecord: the matching records in call order
func (l *historyLogger) withMessage(msg string) []logRecord {
	l.mu.Lock()
	defer l.mu.Unlock()

	var out []logRecord
	for _, r := range l.records {
		if r.msg == msg {
			out = append(out, r)
		}
	}
	return out
}

// fieldString returns the string form of the named field, or "" when absent.
//
// Returns:
//   - string: the field's String(), or "" when the record has no such field
func (r logRecord) fieldString(name string) string {
	for _, f := range r.fields {
		if f.Name == name {
			return f.Value.String()
		}
	}
	return ""
}

// awaitRefreshDone receives one refresh result or fails the test.
//
// Returns:
//   - error: the result the refresh reported
func awaitRefreshDone(t *testing.T, done <-chan error, what string) error {
	t.Helper()

	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

// TestRingRefreshFailure_IsLogged proves a ring refresh that nobody waits for
// still logs its failure: the fixture has no control connection,
// so a triggered refresh fails with errNoControl and must say so.
func TestRingRefreshFailure_IsLogged(t *testing.T) {
	logger := &historyLogger{}
	done := make(chan error, 8)
	session, _, _ := newPauseRecoverySession(t, func(cluster *ClusterConfig) {
		cluster.Logger = logger
		cluster.testRingRefreshDone = func(err error) { done <- err }
	})

	session.ringRefresher.trigger()

	err := awaitRefreshDone(t, done, "the triggered refresh to finish")
	require.ErrorIs(t, err, errNoControl, "without a control connection the refresh must fail with errNoControl")

	warnings := logger.withMessage("Ring refresh failed.")
	require.Len(t, warnings, 1, "exactly one warning for the failed refresh")
	require.Equal(t, LogLevelWarn, warnings[0].level)
	require.Equal(t, errNoControl.Error(), warnings[0].fieldString("err"))
}
