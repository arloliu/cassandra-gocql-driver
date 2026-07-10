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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package streams

import (
	"math"
	"strconv"
	"sync/atomic"
	"testing"
)

func TestUsesAllStreams(t *testing.T) {
	streams := New(1, 0)

	got := make(map[int]struct{})

	for i := 1; i < streams.NumStreams; i++ {
		stream, ok := streams.GetStream()
		if !ok {
			t.Fatalf("unable to get stream %d", i)
		}

		if _, ok = got[stream]; ok {
			t.Fatalf("got an already allocated stream: %d", stream)
		}
		got[stream] = struct{}{}

		if !streams.isSet(stream) {
			bucket := atomic.LoadUint64(&streams.streams[bucketOffset(stream)])
			t.Logf("bucket=%d: %s\n", bucket, strconv.FormatUint(bucket, 2))
			t.Fatalf("stream not set: %d", stream)
		}
	}

	for i := 1; i < streams.NumStreams; i++ {
		if _, ok := got[i]; !ok {
			t.Errorf("did not use stream %d", i)
		}
	}
	if _, ok := got[0]; ok {
		t.Fatal("expected to not use stream 0")
	}

	for i, bucket := range streams.streams {
		if bucket != math.MaxUint64 {
			t.Errorf("did not use all streams in offset=%d bucket=%s", i, bitfmt(bucket))
		}
	}
}

func TestFullStreams(t *testing.T) {
	streams := New(1, 0)
	for i := range streams.streams {
		streams.streams[i] = math.MaxUint64
	}

	stream, ok := streams.GetStream()
	if ok {
		t.Fatalf("should not get stream when all in use: stream=%d", stream)
	}
}

func TestClearStreams(t *testing.T) {
	streams := New(1, 0)
	for i := range streams.streams {
		streams.streams[i] = math.MaxUint64
	}
	streams.inuseStreams = int32(streams.NumStreams)

	for i := 0; i < streams.NumStreams; i++ {
		streams.Clear(i)
	}

	for i, bucket := range streams.streams {
		if bucket != 0 {
			t.Errorf("did not clear streams in offset=%d bucket=%s", i, bitfmt(bucket))
		}
	}
}

func TestDoubleClear(t *testing.T) {
	streams := New(1, 0)
	stream, ok := streams.GetStream()
	if !ok {
		t.Fatal("did not get stream")
	}

	if !streams.Clear(stream) {
		t.Fatalf("stream not indicated as in use: %d", stream)
	}
	if streams.Clear(stream) {
		t.Fatalf("stream not as in use after clear: %d", stream)
	}
}

func TestNewMaxStreams(t *testing.T) {
	tests := []struct {
		name             string
		proto, maxStream int
		want             int
	}{
		{"v5 default caps at 2048", 5, 0, 2048},
		{"v5 negative restores proto max", 5, -1, 32768},
		{"v4 default caps at 2048", 4, 0, 2048},
		{"v3 default caps at 2048", 3, 0, 2048},
		{"v5 explicit value", 5, 4096, 4096},
		{"v5 rounds up to bucket multiple", 5, 100, 128},
		{"v5 floors at bucketBits", 5, 10, 64},
		{"v5 caps positive at proto max", 5, 100000, 32768},
		{"v2 default capped at 128", 2, 0, 128},
		{"v2 negative capped at 128", 2, -1, 128},
		{"v1 explicit capped at 128", 1, 2048, 128},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := New(tt.proto, tt.maxStream)
			if g.NumStreams != tt.want {
				t.Errorf("New(%d, %d).NumStreams = %d, want %d", tt.proto, tt.maxStream, g.NumStreams, tt.want)
			}
			if g.NumStreams%bucketBits != 0 {
				t.Errorf("NumStreams %d is not a multiple of %d", g.NumStreams, bucketBits)
			}
			if int(g.numBuckets) != g.NumStreams/bucketBits {
				t.Errorf("numBuckets %d inconsistent with NumStreams %d", g.numBuckets, g.NumStreams)
			}
			if g.streams[0] != 1<<63 {
				t.Errorf("stream 0 must be reserved, got bucket[0]=%x", g.streams[0])
			}
		})
	}
}

// TestNewMaxStreamsAllocation verifies a capped table hands out exactly
// NumStreams-1 streams (stream 0 is reserved) and no duplicates.
func TestNewMaxStreamsAllocation(t *testing.T) {
	g := New(5, 128)

	seen := make(map[int]struct{})
	for {
		stream, ok := g.GetStream()
		if !ok {
			break
		}
		if stream <= 0 || stream >= g.NumStreams {
			t.Fatalf("stream %d out of range [1,%d)", stream, g.NumStreams)
		}
		if _, dup := seen[stream]; dup {
			t.Fatalf("duplicate stream handed out: %d", stream)
		}
		seen[stream] = struct{}{}
	}

	if len(seen) != g.NumStreams-1 {
		t.Errorf("allocated %d streams, want %d", len(seen), g.NumStreams-1)
	}
}

// TestNewDefaultExhaustClearReuse exercises the v3+ default (2048) table end to
// end: it must expose exactly NumStreams-1 usable streams, refuse allocation once
// saturated, and hand a freed stream back out after Clear.
func TestNewDefaultExhaustClearReuse(t *testing.T) {
	g := New(5, 0) // default -> 2048 for proto v3+
	if g.NumStreams != 2048 {
		t.Fatalf("default NumStreams = %d, want 2048", g.NumStreams)
	}

	allocated := 0
	for {
		if _, ok := g.GetStream(); !ok {
			break
		}
		allocated++
	}
	if allocated != g.NumStreams-1 {
		t.Fatalf("exhausted %d streams, want %d (stream 0 reserved)", allocated, g.NumStreams-1)
	}

	// Saturated: the next allocation must fail.
	if _, ok := g.GetStream(); ok {
		t.Fatal("expected GetStream to fail on a saturated table")
	}

	// Free a stream and confirm it becomes allocatable again.
	const freed = 100
	if !g.Clear(freed) {
		t.Fatalf("Clear(%d) reported the stream was not in use", freed)
	}
	got, ok := g.GetStream()
	if !ok {
		t.Fatal("expected GetStream to succeed after Clear")
	}
	if got != freed {
		t.Fatalf("reacquired stream %d, want the freed stream %d", got, freed)
	}
}

func BenchmarkConcurrentUse(b *testing.B) {
	streams := New(2, 0)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			stream, ok := streams.GetStream()
			if !ok {
				b.Error("unable to get stream")
				return
			}

			if !streams.Clear(stream) {
				b.Errorf("stream was already cleared: %d", stream)
				return
			}
		}
	})
}

func TestStreamOffset(t *testing.T) {
	tests := [...]struct {
		n   int
		off uint64
	}{
		{0, 63},
		{1, 62},
		{2, 61},
		{3, 60},
		{63, 0},
		{64, 63},

		{128, 63},
	}

	for _, test := range tests {
		if off := streamOffset(test.n); off != test.off {
			t.Errorf("n=%d expected %d got %d", test.n, off, test.off)
		}
	}
}

func TestIsSet(t *testing.T) {
	tests := [...]struct {
		stream int
		bucket uint64
		set    bool
	}{
		{0, 0, false},
		{0, 1 << 63, true},
		{1, 0, false},
		{1, 1 << 62, true},
		{63, 1, true},
		{64, 1 << 63, true},
		{0, 0x8000000000000000, true},
	}

	for i, test := range tests {
		if set := isSet(test.bucket, test.stream); set != test.set {
			t.Errorf("[%d] stream=%d expected %v got %v", i, test.stream, test.set, set)
		}
	}

	for i := 0; i < bucketBits; i++ {
		if !isSet(math.MaxUint64, i) {
			var shift uint64 = math.MaxUint64 >> streamOffset(i)
			t.Errorf("expected isSet for all i=%d got=%d", i, shift)
		}
	}
}

func TestBucketOfset(t *testing.T) {
	tests := [...]struct {
		n      int
		bucket int
	}{
		{0, 0},
		{1, 0},
		{63, 0},
		{64, 1},
	}

	for _, test := range tests {
		if bucket := bucketOffset(test.n); bucket != test.bucket {
			t.Errorf("n=%d expected %v got %v", test.n, test.bucket, bucket)
		}
	}
}

func TestStreamFromBucket(t *testing.T) {
	tests := [...]struct {
		bucket int
		pos    int
		stream int
	}{
		{0, 0, 0},
		{0, 1, 1},
		{0, 2, 2},
		{0, 63, 63},
		{1, 0, 64},
		{1, 1, 65},
	}

	for _, test := range tests {
		if stream := streamFromBucket(test.bucket, test.pos); stream != test.stream {
			t.Errorf("bucket=%d pos=%d expected %v got %v", test.bucket, test.pos, test.stream, stream)
		}
	}
}
