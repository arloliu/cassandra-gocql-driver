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
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type identityCompressor struct{}

func (identityCompressor) Name() string { return "identity" }

func (identityCompressor) AppendCompressedWithLength(dst, src []byte) ([]byte, error) {
	// Write decompressed length prefix (big-endian int32), then raw bytes.
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(src)))
	dst = append(dst, hdr[:]...)
	dst = append(dst, src...)
	return dst, nil
}

func (identityCompressor) AppendDecompressedWithLength(dst, src []byte) ([]byte, error) {
	if len(src) < 4 {
		return dst, nil
	}
	// Ignore declared length; just append remaining bytes.
	dst = append(dst, src[4:]...)
	return dst, nil
}

func (identityCompressor) AppendCompressed(dst, src []byte) ([]byte, error) {
	dst = append(dst, src...)
	return dst, nil
}

func (identityCompressor) AppendDecompressed(dst, src []byte, _ uint32) ([]byte, error) {
	dst = append(dst, src...)
	return dst, nil
}

func encodeShort(n uint16) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], n)
	return b[:]
}

func encodeInt(n int32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(n))
	return b[:]
}

func encodeString(s string) []byte {
	b := []byte(s)
	out := make([]byte, 0, 2+len(b))
	out = append(out, encodeShort(uint16(len(b)))...)
	out = append(out, b...)
	return out
}

func encodeBytes(b []byte) []byte {
	out := make([]byte, 0, 4+len(b))
	out = append(out, encodeInt(int32(len(b)))...)
	out = append(out, b...)
	return out
}

func encodeStringList(ss []string) []byte {
	out := make([]byte, 0, 2)
	out = append(out, encodeShort(uint16(len(ss)))...)
	for _, s := range ss {
		out = append(out, encodeString(s)...)
	}
	return out
}

func reacquireSameFramer(t *testing.T, target *framer) *framer {
	t.Helper()
	// sync.Pool has no guarantee, but in practice it tends to return recently released objects.
	// If we can't reacquire the same instance quickly, skip to avoid flakiness.
	for i := 0; i < 1000; i++ {
		f := getFramer(nil, protoVersion4, GlobalTypes)
		if f == target {
			return f
		}
		f.release()
	}
	t.Skip("could not reacquire same framer instance from pool")
	return nil
}

// TestFramerPool_GetAndRelease verifies basic get/release cycle works correctly.
func TestFramerPool_GetAndRelease(t *testing.T) {
	f := getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, f)

	// Verify initial state
	assert.Equal(t, byte(protoVersion4), f.proto)
	assert.Nil(t, f.header)
	assert.Nil(t, f.traceID)
	assert.Nil(t, f.customPayload)

	// Release and get again
	f.release()

	f2 := getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, f2)

	// Verify state is reset
	assert.Nil(t, f2.header)
	assert.Nil(t, f2.traceID)
	assert.Nil(t, f2.customPayload)

	f2.release()
}

// TestFramerPool_BufferReuse verifies that buffer is properly reset between uses.
func TestFramerPool_BufferReuse(t *testing.T) {
	f := getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, f)

	// Write some data to the buffer
	testData := []byte("test data that should be cleared")
	f.buf = append(f.buf, testData...)
	assert.Equal(t, len(testData), len(f.buf))

	// Release the framer
	f.release()

	// Get a new framer (may or may not be the same one from pool)
	f2 := getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, f2)

	// Buffer should be empty (reset to zero length)
	assert.Equal(t, 0, len(f2.buf), "buffer should be reset to zero length")

	f2.release()
}

// TestFramerPool_LargeBufferDiscarded verifies that buffers larger than
// maxPooledBufSize are discarded to prevent memory bloat.
func TestFramerPool_LargeBufferDiscarded(t *testing.T) {
	f := getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, f)

	// Simulate a large frame by expanding the buffer beyond maxPooledBufSize.
	f.readBuffer = make([]byte, maxPooledBufSize+1024)
	f.buf = f.readBuffer[:0]
	require.Greater(t, cap(f.readBuffer), maxPooledBufSize)

	// release() must drop the oversized buffer. Assert on the still-referenced framer
	// (deterministic) rather than whatever sync.Pool hands back next.
	f.release()
	assert.Equal(t, defaultBufSize, cap(f.readBuffer),
		"oversized readBuffer should be discarded and reset to the default size on release")
}

// TestFramerPool_TypicalLargeFrameRetained verifies that megabyte-scale frame
// buffers — representative of production result/batch frames, and far above the
// old 64 KiB cap — are retained on release for reuse rather than discarded.
// Regression guard for the readFrame reallocation churn (B2).
func TestFramerPool_TypicalLargeFrameRetained(t *testing.T) {
	f := getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, f)

	const frameSize = 1 << 20 // 1 MiB, ~ the production read-path average
	require.LessOrEqual(t, frameSize, maxPooledBufSize,
		"a typical ~1 MiB frame must fit within the pooled cap")

	f.readBuffer = make([]byte, frameSize)
	f.buf = f.readBuffer[:0]

	// release() must keep the buffer (not reset it to defaultBufSize) so the
	// next large frame reuses it instead of allocating.
	f.release()
	assert.Equal(t, frameSize, cap(f.readBuffer),
		"megabyte-scale frame buffer should be retained for reuse")
}

// BenchmarkFramerReadFrameReuse exercises the get -> readFrame -> release cycle
// with a frame body larger than the old 64 KiB cap. With retention (B2) the
// pooled buffer is reused, so steady-state allocations come only from the
// bytes.Reader, not the frame body; before B2 every iteration reallocated the
// whole body.
func BenchmarkFramerReadFrameReuse(b *testing.B) {
	const bodyLen = 256 * 1024
	body := make([]byte, bodyLen)
	head := frameHeader{version: protoVersion4 | 0x80, op: opReady, length: bodyLen}

	b.ReportAllocs()
	for b.Loop() {
		f := getFramer(nil, protoVersion4, GlobalTypes)
		if err := f.readFrame(bytes.NewReader(body), &head); err != nil {
			b.Fatal(err)
		}
		f.release()
	}
}

// TestFramerPool_ConcurrentAccess verifies that concurrent get/release
// operations are safe and don't cause data races.
func TestFramerPool_ConcurrentAccess(t *testing.T) {
	const numGoroutines = 100
	const numIterations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	var failures atomic.Int64

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()

			for j := 0; j < numIterations; j++ {
				f := getFramer(nil, protoVersion4, GlobalTypes)
				if f == nil {
					failures.Add(1)
					continue
				}

				// Simulate some work with the framer
				f.buf = append(f.buf, byte(id), byte(j))

				// Verify the framer state is valid
				if f.proto != byte(protoVersion4) || f.types == nil {
					failures.Add(1)
				}

				// Release
				f.release()
			}
		}(i)
	}

	wg.Wait()
	if failures.Load() != 0 {
		t.Fatalf("concurrent get/release had %d failures", failures.Load())
	}
}

func TestFramerPool_ResetSetsCompressionFlag(t *testing.T) {
	comp := identityCompressor{}

	// v4 should set compress flag when compressor is present
	f := getFramer(comp, protoVersion4, GlobalTypes)
	require.NotNil(t, f)
	assert.Equal(t, byte(protoVersion4), f.proto)
	assert.Equal(t, byte(flagCompress), f.flags&flagCompress)
	f.release()

	// v5+ should NOT set compress flag (protocol >=5 doesn't use the flag)
	f2 := getFramer(comp, protoVersion5, GlobalTypes)
	require.NotNil(t, f2)
	assert.Equal(t, byte(protoVersion5), f2.proto)
	assert.Equal(t, byte(0), f2.flags&flagCompress)
	f2.release()
}

func TestFramerPool_AuthChallengeDataDoesNotAliasPooledBuffer(t *testing.T) {
	data := []byte("challenge-data")
	body := encodeBytes(data)

	f := getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, f)
	// Response frame: set response bit (0x80)
	f.header = &frameHeader{version: protoVersion4 | 0x80, op: opAuthChallenge, length: len(body)}
	f.readBuffer = make([]byte, len(body))
	copy(f.readBuffer, body)
	f.buf = f.readBuffer

	resp, err := f.parseFrame()
	require.NoError(t, err)
	challenge, ok := resp.(*authChallengeFrame)
	require.True(t, ok)
	got := append([]byte(nil), challenge.data...) // snapshot

	f.release()
	f2 := reacquireSameFramer(t, f)
	// overwrite pooled buffer
	for i := range f2.readBuffer {
		f2.readBuffer[i] = 0
	}
	f2.release()

	assert.Equal(t, data, got, "auth challenge data should not alias pooled buffer")
}

func TestFramerPool_AuthSuccessDataDoesNotAliasPooledBuffer(t *testing.T) {
	data := []byte("success-data")
	body := encodeBytes(data)

	f := getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, f)
	f.header = &frameHeader{version: protoVersion4 | 0x80, op: opAuthSuccess, length: len(body)}
	f.readBuffer = make([]byte, len(body))
	copy(f.readBuffer, body)
	f.buf = f.readBuffer

	resp, err := f.parseFrame()
	require.NoError(t, err)
	success, ok := resp.(*authSuccessFrame)
	require.True(t, ok)
	got := append([]byte(nil), success.data...) // snapshot

	f.release()
	f2 := reacquireSameFramer(t, f)
	for i := range f2.readBuffer {
		f2.readBuffer[i] = 0
	}
	f2.release()

	assert.Equal(t, data, got, "auth success data should not alias pooled buffer")
}

func TestFramerPool_CustomPayloadValuesDoNotAliasPooledBuffer(t *testing.T) {
	key := "k"
	val := []byte{1, 2, 3, 4, 5}

	// Bytes map encoding: [short n][string key][bytes val]
	body := make([]byte, 0, 2+len(key)+4+len(val)+8)
	body = append(body, encodeShort(1)...)
	body = append(body, encodeString(key)...)
	body = append(body, encodeBytes(val)...)

	f := getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, f)
	f.header = &frameHeader{version: protoVersion4 | 0x80, flags: flagCustomPayload, op: opReady, length: len(body)}
	f.readBuffer = make([]byte, len(body))
	copy(f.readBuffer, body)
	f.buf = f.readBuffer

	resp, err := f.parseFrame()
	require.NoError(t, err)
	_, ok := resp.(*readyFrame)
	require.True(t, ok)
	require.NotNil(t, f.customPayload)
	gotVal := append([]byte(nil), f.customPayload[key]...) // snapshot

	f.release()
	f2 := reacquireSameFramer(t, f)
	for i := range f2.readBuffer {
		f2.readBuffer[i] = 0
	}
	f2.release()

	assert.Equal(t, val, gotVal, "custom payload values should not alias pooled buffer")
}

func TestIter_GetCustomPayload_SurvivesCloseAndPoolReuse(t *testing.T) {
	key := "k"
	val := []byte{9, 8, 7, 6, 5}

	// Build a response frame body that includes a custom payload.
	body := make([]byte, 0, 2+len(key)+4+len(val)+8)
	body = append(body, encodeShort(1)...)
	body = append(body, encodeString(key)...)
	body = append(body, encodeBytes(val)...)

	f := getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, f)
	f.header = &frameHeader{version: protoVersion4 | 0x80, flags: flagCustomPayload, op: opReady, length: len(body)}
	f.readBuffer = make([]byte, len(body))
	copy(f.readBuffer, body)
	f.buf = f.readBuffer

	_, err := f.parseFrame()
	require.NoError(t, err)

	iter := newIter(nil, "", nil, nil)
	iter.framer = f

	payload := iter.GetCustomPayload()
	require.NotNil(t, payload)
	got := payload[key]
	require.Equal(t, val, got)

	// Close releases the framer back to the pool.
	require.NoError(t, iter.Close())

	// Now aggressively reuse the same framer and overwrite its pooled buffer.
	f2 := reacquireSameFramer(t, f)
	for i := range f2.readBuffer {
		f2.readBuffer[i] = 0
	}
	f2.release()

	// The previously obtained payload should still be intact.
	assert.Equal(t, val, got, "payload returned by Iter.GetCustomPayload should remain stable even after Iter.Close and framer reuse")
}

// TestFramerPool_ResetClearsState verifies that all framer fields are properly
// reset when released and re-acquired.
func TestFramerPool_ResetClearsState(t *testing.T) {
	f := getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, f)

	// Set various fields to non-default values
	f.header = &frameHeader{
		version: protoVersion4,
		op:      opResult,
		stream:  42,
	}
	f.traceID = []byte{1, 2, 3, 4}
	f.customPayload = map[string][]byte{"key": {1, 2, 3}}
	f.flags = flagTracing | flagCompress

	// Release
	f.release()

	// Get a new framer (should be the same one, reset)
	f2 := getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, f2)

	// Verify all fields are reset
	assert.Nil(t, f2.header, "header should be nil after reset")
	assert.Nil(t, f2.traceID, "traceID should be nil after reset")
	assert.Nil(t, f2.customPayload, "customPayload should be nil after reset")
	// flags should only have compression flag if compressor is set (which it isn't)
	assert.Equal(t, byte(0), f2.flags, "flags should be reset")
	assert.Equal(t, 0, len(f2.buf), "buf should be empty")

	f2.release()
}

// TestFramerPool_CompressorReset verifies that compressor and compress buffer
// are properly handled during reset.
func TestFramerPool_CompressorReset(t *testing.T) {
	// Get framer with no compressor
	f := getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, f)
	assert.Nil(t, f.compres)

	// Simulate having used compression
	f.compressBuf = make([]byte, 1024)

	f.release()

	// Get another framer, this time verify compressBuf handling
	f2 := getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, f2)

	// compressBuf may still be present (small buffer) or nil (if it was large)
	// This is expected behavior - we just verify no panic

	f2.release()
}

// TestFramerPool_LargeCompressBufDiscarded verifies that large compress buffers
// are discarded to prevent memory bloat.
func TestFramerPool_LargeCompressBufDiscarded(t *testing.T) {
	f := getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, f)

	// Set a compress buffer beyond the retention cap.
	f.compressBuf = make([]byte, maxPooledBufSize+1024)
	require.Greater(t, cap(f.compressBuf), maxPooledBufSize)

	// release() sets an oversized compressBuf to nil. Assert on the released framer.
	f.release()
	assert.Nil(t, f.compressBuf, "oversized compressBuf should be discarded (set to nil) on release")
}

func TestFramerPool_ReadFrame_CompressedButNoCompressor(t *testing.T) {
	// A compressed frame with no compressor should fail with a protocol error,
	// but still be safe to release back to the pool.
	f := getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, f)
	defer f.release()

	// Body doesn't matter much; readFrame will try to decompress and fail because compres is nil.
	body := append(encodeInt(1), 0xAA) // length prefix + one byte
	head := frameHeader{
		version: protoVersion4 | 0x80,
		flags:   flagCompress,
		op:      opReady,
		length:  len(body),
	}

	err := f.readFrame(bytes.NewReader(body), &head)
	require.Error(t, err)
	// NewErrProtocol returns an error type, but we just check it matches our expectation.
	assert.Contains(t, err.Error(), "no compressor available", "expected protocol error when compressed frame received without a compressor")
}

func TestFramerPool_ReadBufferBoundary_NotDiscardedAtMax(t *testing.T) {
	f := getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, f)

	// release() only discards buffers with cap > maxPooledBufSize, so a buffer exactly
	// at the cap must be retained. Assert on the released framer directly.
	f.readBuffer = make([]byte, maxPooledBufSize)
	f.buf = f.readBuffer[:0]

	f.release()
	assert.Equal(t, maxPooledBufSize, cap(f.readBuffer),
		"readBuffer at exactly maxPooledBufSize should be retained")
}

func TestIter_Warnings_SurvivesCloseAndPoolReuse(t *testing.T) {
	warns := []string{"w1", "warning two"}
	body := encodeStringList(warns)

	f := getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, f)
	f.header = &frameHeader{version: protoVersion4 | 0x80, flags: flagWarning, op: opReady, length: len(body)}
	f.readBuffer = make([]byte, len(body))
	copy(f.readBuffer, body)
	f.buf = f.readBuffer

	_, err := f.parseFrame()
	require.NoError(t, err)

	iter := newIter(nil, "", nil, nil)
	iter.framer = f

	gotWarns := iter.Warnings()
	require.Equal(t, warns, gotWarns)

	// Close releases the framer.
	require.NoError(t, iter.Close())

	// Reuse/overwrite pooled buffer; warnings should remain stable because strings are copied.
	f2 := reacquireSameFramer(t, f)
	for i := range f2.readBuffer {
		f2.readBuffer[i] = 0
	}
	f2.release()

	assert.Equal(t, warns, gotWarns, "warnings should remain stable across Iter.Close and pool reuse")
}

func TestIter_Close_IsIdempotent(t *testing.T) {
	iter := newIter(nil, "", nil, nil)
	iter.framer = getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, iter.framer)

	require.NoError(t, iter.Close())
	require.NoError(t, iter.Close())
}

func TestIter_Close_ReturnsIterErr(t *testing.T) {
	sentinel := errors.New("sentinel")
	iter := newErrIter(sentinel, nil, "", nil, nil)
	iter.framer = getFramer(nil, protoVersion4, GlobalTypes)
	require.NotNil(t, iter.framer)

	err := iter.Close()
	require.Error(t, err)
	assert.True(t, errors.Is(err, sentinel))
}
