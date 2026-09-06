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
	"fmt"
	"os"
	"runtime/debug"
)

// panicWithStack wraps a recovered panic value with the stack captured at
// the original panic site. It is used at inner recover-and-rethrow sites
// (e.g. refreshDebouncer.flusher) so that the outer recoverGoroutine logs
// the stack of the original panic, not the re-panic site.
//
// Unexported on purpose: only the driver's own re-panic sites should use
// this wrapping.
type panicWithStack struct {
	value any
	stack []byte
}

// recoverGoroutine recovers a panic in a driver-owned goroutine, logs it
// with a stack trace, and runs an optional teardown callback. Call via
// `defer` at the top of every driver-spawned goroutine.
//
// teardown is invoked only when a panic is recovered. Pass nil if the
// goroutine has no cleanup needs.
//
// recoverGoroutine NEVER propagates a panic. Panics during logging or
// teardown are absorbed by nested recovers; an outermost barrier swallows
// anything that escapes (e.g. a panic from writing to a closed os.Stderr).
//
// For goroutines whose teardown needs to read mutating local state at
// the moment of panic (e.g. writeCoalescer.flusher's pending result
// chans), use an inline closure with recover() and call
// handleRecoveredPanic. recover() only works when called *directly* by a
// deferred function, so capturing-by-closure cannot be done through
// recoverGoroutine.
func recoverGoroutine(logger StructuredLogger, name string, teardown func(panicErr error)) {
	if r := recover(); r != nil {
		handleRecoveredPanic(logger, name, r, teardown)
	}
}

// safely runs fn and absorbs a panic out of it, logging it like any other
// recovered panic.
//
// It is for the short stretches of a deferred handler that call user code after the
// handler has already recovered: handleRecoveredPanic's barrier protects only its own
// call, so anything that runs after it is unguarded and would propagate.
//
// Parameters:
//   - logger: the logger to report a recovered panic on
//   - name: the name reported for the panic
//   - fn: the call to isolate
func safely(logger StructuredLogger, name string, fn func()) {
	defer func() { handleRecoveredPanic(logger, name, recover(), nil) }()
	fn()
}

// handleRecoveredPanic is the shared post-recover handler for both
// recoverGoroutine and call sites that need to use their own inline
// `if r := recover(); r != nil` (because they need to capture mutating
// local state by closure — recover() must be called directly by the
// deferred function, not by a function it calls).
//
// Like recoverGoroutine, this function NEVER propagates a panic.
func handleRecoveredPanic(logger StructuredLogger, name string, r any, teardown func(panicErr error)) {
	if r == nil {
		return
	}

	// Outermost barrier: nothing in the recovery body should escape, even
	// a panic from fmt.Fprintf into a closed os.Stderr. This is the last
	// line of defense for the "library never crashes the host" guarantee.
	defer func() { _ = recover() }()

	// If the recovered value was wrapped at an inner re-panic site,
	// prefer the captured stack over our own debug.Stack() (which would
	// show the re-panic frame, not the original site).
	var (
		panicVal any
		stack    []byte
	)
	if pw, ok := r.(panicWithStack); ok {
		panicVal = pw.value
		stack = pw.stack
	} else {
		panicVal = r
		stack = debug.Stack()
	}
	err := fmt.Errorf("gocql: panic in goroutine %s: %v", name, panicVal)

	// Phase 1: log. Logger is user-supplied; isolate from teardown.
	func() {
		defer func() {
			// Secondary recover MUST NOT call the logger — that's what
			// failed. Use stderr only.
			if r2 := recover(); r2 != nil {
				fmt.Fprintf(os.Stderr,
					"gocql: panic in logger during recovery of %s: %v\nprimary panic: %v\n%s\n",
					name, r2, panicVal, stack)
			}
		}()
		if logger != nil {
			logger.Error("Goroutine panicked.",
				NewLogFieldString("goroutine", name),
				NewLogFieldString("panic", fmt.Sprint(panicVal)),
				NewLogFieldString("stack", string(stack)),
			)
		} else {
			fmt.Fprintf(os.Stderr,
				"gocql: panic in goroutine %s: %v\n%s\n", name, panicVal, stack)
		}
	}()

	if teardown != nil {
		// Phase 2: teardown. Same discipline — stderr only on secondary panic.
		func() {
			defer func() {
				if r2 := recover(); r2 != nil {
					fmt.Fprintf(os.Stderr,
						"gocql: panic during recovery teardown in %s: %v\n", name, r2)
				}
			}()
			teardown(err)
		}()
	}
}
