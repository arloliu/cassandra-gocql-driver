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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gocql

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// armedPanicPolicy wraps a policy and, once armed, panics in HostUp the way a
// misbehaving application policy would.
type armedPanicPolicy struct {
	HostSelectionPolicy
	armed atomic.Bool
	calls atomic.Int32
}

var _ HostSelectionPolicy = (*armedPanicPolicy)(nil)

// HostUp forwards to the wrapped policy, then panics when armed.
func (p *armedPanicPolicy) HostUp(host *HostInfo) {
	p.HostSelectionPolicy.HostUp(host)
	if p.armed.Load() {
		p.calls.Add(1)
		panic("gocql: HostUp panic injected by the refill test")
	}
}

// panicLogger records Error calls and signals the ones that report a recovered
// goroutine panic.
type panicLogger struct {
	StructuredLogger
	recovered chan string
}

var _ StructuredLogger = (*panicLogger)(nil)

// Error forwards to the wrapped logger and publishes the goroutine name of a
// recovered panic.
func (l *panicLogger) Error(msg string, fields ...LogField) {
	l.StructuredLogger.Error(msg, fields...)
	if msg != "Goroutine panicked." {
		return
	}
	for _, f := range fields {
		if f.Name == "goroutine" {
			select {
			case l.recovered <- f.Value.String():
			default:
			}
		}
	}
}

// TestFill_RefillHostUpPanicIsRecovered proves a panic in the application's HostUp
// callback on the asynchronous refill path is recovered and logged like every other
// driver goroutine instead of crashing the process.
//
// The initial fill notifies from a recovered goroutine;
// the refill of a pool that still holds a connection used to notify from a bare one.
// The pool is grown after the fixture settles so the fill takes the refill branch.
func TestFill_RefillHostUpPanicIsRecovered(t *testing.T) {
	logger := &panicLogger{StructuredLogger: newTestLogger(LogLevelDebug), recovered: make(chan string, 4)}
	policy := &armedPanicPolicy{}
	harness := newFillHarness(t, 1, func(cluster *ClusterConfig) {
		policy.HostSelectionPolicy = cluster.PoolConfig.HostSelectionPolicy
		cluster.PoolConfig.HostSelectionPolicy = policy
		cluster.Logger = logger
	})
	host := harness.hosts[0]
	pool := harness.pool(t, host)

	pool.mu.Lock()
	pool.size = 2
	pool.mu.Unlock()
	policy.armed.Store(true)

	// scheduleFill publishes the claim that fill's asynchronous branch releases,
	// the same way Pick and HandleError drive a refill.
	pool.scheduleFill()

	select {
	case name := <-logger.recovered:
		require.Equal(t, "Session.handleNodeConnected", name, "the recovered goroutine must be the refill notification")
	case <-time.After(fillEventBudget):
		t.Fatalf("timed out after %v waiting for the refill HostUp panic to be recovered and logged", fillEventBudget)
	}
	require.Equal(t, int32(1), policy.calls.Load(), "HostUp must have been invoked exactly once by the refill")

	harness.events.awaitEachHost(t, poolFillDone, []*HostInfo{host}, "the refill to release its claim")
	require.Zero(t, pendingFills(pool), "the refill must release exactly the claim it was given")
	require.Equal(t, 2, pool.Size(), "the refill itself must have completed")
	require.NotNil(t, pool.Pick(), "the pool must still serve connections after the recovered panic")
}
