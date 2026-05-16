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
	"errors"
	"strconv"
	"testing"
)

// These tests pin down that errors produced at wrapped call sites can be
// unwrapped by callers via errors.Is / errors.As / errors.Unwrap. The
// behavior is purely additive — previously these errors were
// fmt.Errorf(..., %v) which produced a flat string; the conversion to
// %w means callers can now match the inner cause programmatically.
//
// We don't try to cover every wrap site; we cover representative ones
// across host_source.go (version-parse), so a future regression that
// reverts the %w would be caught.

// TestErrorWrap_CassVersion_PreservesStrconvErr asserts that
// cassVersion.unmarshal's wrapped error preserves the underlying
// strconv.ErrSyntax via errors.Is — i.e. callers can detect "this was
// a parse failure" without string matching.
func TestErrorWrap_CassVersion_PreservesStrconvErr(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantMsg string
	}{
		{"bad major", "x.2.3", "invalid major version"},
		{"bad minor", "1.x.3", "invalid minor version"},
		{"bad patch", "1.2.x", "invalid patch version"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var v cassVersion
			err := v.unmarshal([]byte(tc.input))
			if err == nil {
				t.Fatalf("expected parse error for %q, got nil", tc.input)
			}
			// (1) message is preserved
			if !contains(err.Error(), tc.wantMsg) {
				t.Errorf("error message lost: want %q in %q", tc.wantMsg, err.Error())
			}
			// (2) the wrapped cause is errors.Is-detectable
			if !errors.Is(err, strconv.ErrSyntax) {
				t.Errorf("errors.Is(err, strconv.ErrSyntax) = false; wrapping broken")
			}
			// (3) errors.Unwrap returns non-nil
			if errors.Unwrap(err) == nil {
				t.Errorf("errors.Unwrap returned nil; %%w not in effect")
			}
		})
	}
}

// TestErrorWrap_CassVersion_NumError asserts errors.As works to recover
// the concrete *strconv.NumError, demonstrating the chain is structurally
// intact (not just message-preserved).
func TestErrorWrap_CassVersion_NumError(t *testing.T) {
	var v cassVersion
	err := v.unmarshal([]byte("x.2.3"))
	if err == nil {
		t.Fatalf("expected parse error, got nil")
	}
	var numErr *strconv.NumError
	if !errors.As(err, &numErr) {
		t.Fatalf("errors.As(err, *strconv.NumError) = false; wrapping broken")
	}
	if numErr.Func != "Atoi" {
		t.Errorf("unexpected NumError.Func = %q", numErr.Func)
	}
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
