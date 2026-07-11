//go:build all || unit
// +build all unit

package gocql

import (
	"bytes"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildBigBatchReq builds a single-statement batch whose one column value is
// valueBytes long, so a frame of a chosen body size can be produced cheaply (no
// per-row marshalling) to exercise segment boundaries and buffer-retention bounds.
func buildBigBatchReq(valueBytes int) *writeBatchFrame {
	preparedID := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	val := make([]byte, valueBytes)
	for i := range val {
		val[i] = byte(i*31 + 7)
	}
	return &writeBatchFrame{
		typ:         UnloggedBatch,
		statements:  []batchStatment{{preparedID: preparedID, values: []queryValues{{value: val}}}},
		consistency: Quorum,
	}
}

// backing returns the identity (address) of a slice's underlying array, for
// distinct-buffer assertions. Compared as uintptr because testify's Equal on *byte
// would dereference and compare byte values, not pointer identity.
func backing(b []byte) uintptr {
	return uintptr(unsafe.Pointer(unsafe.SliceData(b)))
}

// reassembleUncompressedSegments reads every uncompressed segment from buf and
// concatenates the payloads back into the original pre-segmentation frame body.
func reassembleUncompressedSegments(t *testing.T, buf []byte) (body []byte, segments int) {
	t.Helper()
	r := bytes.NewReader(buf)
	for r.Len() > 0 {
		payload, bufPtr, _, err := readUncompressedSegment(r)
		require.NoError(t, err)
		body = append(body, payload...)
		releaseSegmentBuffer(bufPtr)
		segments++
	}
	return body, segments
}

// TestAppendUncompressedSegment proves the single uncompressed encoder produces a
// wire-valid segment (round-trips back through the independent readUncompressedSegment
// verifier, which recomputes CRC24 + CRC32) across payload sizes and both
// self-contained flags. It exercises the warm-reuse reslice branch with a POISONED
// destination whose capacity exceeds the output — including large→small reuse — so a
// stale tail or wrong length would surface.
func TestAppendUncompressedSegment(t *testing.T) {
	// Includes a large→small transition (maxSegmentPayloadSize then 200, 0) so a stale
	// tail from the previous larger payload would surface in the reused buffer.
	sizes := []int{0, 1, 15, 100, 1000, maxSegmentPayloadSize, 200, 0}
	// A poisoned buffer big enough for the largest segment; reused across iterations
	// so a later smaller payload must not leak the previous (larger) tail.
	warm := make([]byte, 6+maxSegmentPayloadSize+crc32Size)
	for i := range warm {
		warm[i] = 0xEE
	}

	for _, n := range sizes {
		payload := make([]byte, n)
		for i := range payload {
			payload[i] = byte(i*7 + 3)
		}
		for _, sc := range []bool{true, false} {
			segSize := 6 + n + crc32Size

			// Cold path: nil dst allocates exactly the segment size.
			cold, err := appendUncompressedSegment(nil, payload, sc)
			require.NoError(t, err)
			require.Len(t, cold, segSize, "cold n=%d sc=%v", n, sc)

			// Warm-reuse path: capacity >= segSize, so the reslice branch runs.
			require.GreaterOrEqual(t, cap(warm), segSize, "test setup: warm must force the reslice branch")
			got, err := appendUncompressedSegment(warm[:0], payload, sc)
			require.NoError(t, err)
			require.Len(t, got, segSize, "warm len must be exact, no stale tail: n=%d sc=%v", n, sc)
			require.Equal(t, cold, got, "warm output must match cold: n=%d sc=%v", n, sc)

			// Round-trip through the independent reader (verifies header bit + both CRCs).
			if sc {
				readback, bufPtr, isSelfContained, err := readUncompressedSegment(bytes.NewReader(got))
				require.NoError(t, err)
				assert.True(t, isSelfContained)
				assert.Equal(t, payload, readback, "round-trip n=%d", n)
				releaseSegmentBuffer(bufPtr)
			}
		}
	}
}

// TestAppendUncompressedSegmentOverMax verifies the size guard: a payload larger than
// one segment is rejected, not silently truncated (the caller's multi-segment path
// handles oversize frames instead).
func TestAppendUncompressedSegmentOverMax(t *testing.T) {
	_, err := appendUncompressedSegment(nil, make([]byte, maxSegmentPayloadSize+1), true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum size")
}

// TestFramerFinishRetainsGrownBuffer proves finish() extends the framer's pooled
// buffer to the grown request body (so a reused framer no longer re-grows from
// defaultBufSize), and that it does NOT shrink an already-larger buffer nor retain an
// over-cap buffer. Capacity assertions only — no pool-identity (sync.Pool drops puts
// under -race), matching the B1/B4 convention.
func TestFramerFinishRetainsGrownBuffer(t *testing.T) {
	// Grown body within the bound: retained.
	f := newFramer(nil, protoVersion5, GlobalTypes)
	require.Equal(t, defaultBufSize, cap(f.readBuffer))
	require.NoError(t, f.writeBatchFrame(0, buildTChartBatchReq(100), nil))
	require.Greater(t, len(f.buf), defaultBufSize, "sanity: body must outgrow the initial buffer")
	assert.GreaterOrEqual(t, cap(f.readBuffer), len(f.buf),
		"finish() must retain the grown request buffer so the pool keeps the capacity")

	// A tiny frame must not shrink an already-large readBuffer (guard: only grow).
	prevCap := cap(f.readBuffer)
	f.reset(nil, protoVersion5, GlobalTypes)
	require.NoError(t, f.writeBatchFrame(0, buildTChartBatchReq(1), nil))
	assert.Equal(t, prevCap, cap(f.readBuffer), "finish() must not shrink a larger retained buffer")

	// Over-cap body must NOT be retained (bound = maxPooledBufSize).
	big := newFramer(nil, protoVersion5, GlobalTypes)
	require.NoError(t, big.writeBatchFrame(0, buildBigBatchReq(maxPooledBufSize+4096), nil))
	require.Greater(t, cap(big.buf), maxPooledBufSize, "sanity: body must exceed the retention bound")
	assert.LessOrEqual(t, cap(big.readBuffer), maxPooledBufSize,
		"finish() must not retain an over-cap buffer")
}

// TestPrepareModernLayoutSelfContainedRoundTrip drives the new self-contained write
// path end to end: build a batch frame, capture the pre-segmentation body, apply the
// modern layout, and confirm the emitted segment reads back to the identical body.
func TestPrepareModernLayoutSelfContainedRoundTrip(t *testing.T) {
	f := newFramer(nil, protoVersion5, GlobalTypes)
	require.NoError(t, f.writeBatchFrame(0, buildTChartBatchReq(50), nil))

	body := append([]byte(nil), f.buf...) // copy: prepareModernLayout reassigns f.buf
	require.LessOrEqual(t, len(body), maxSegmentPayloadSize, "test frame must be self-contained")

	require.NoError(t, f.prepareModernLayout())

	payload, bufPtr, isSelfContained, err := readUncompressedSegment(bytes.NewReader(f.buf))
	require.NoError(t, err)
	assert.True(t, isSelfContained)
	assert.Equal(t, body, payload)
	releaseSegmentBuffer(bufPtr)
}

// TestPrepareModernLayoutReuseDeterministic is the deterministic alias/recovery guard.
// On ONE framer object (reset between frames, no sync.Pool round-trip) it runs
// large→small→large and asserts: (1) the emitted bytes exactly match independent fresh
// framers, so no stale tail from a prior larger frame leaks; (2) after layout the body
// buffer (readBuffer) and the segment buffer (segBuf) have distinct backing arrays;
// (3) after reset, f.buf recovers the body backing, never the segment backing.
func TestPrepareModernLayoutReuseDeterministic(t *testing.T) {
	reqs := []*writeBatchFrame{buildTChartBatchReq(90), buildTChartBatchReq(2), buildTChartBatchReq(120)}

	want := make([][]byte, len(reqs))
	for i, req := range reqs {
		ref := newFramer(nil, protoVersion5, GlobalTypes)
		require.NoError(t, ref.writeBatchFrame(0, req, nil))
		require.NoError(t, ref.prepareModernLayout())
		want[i] = append([]byte(nil), ref.buf...)
	}

	f := newFramer(nil, protoVersion5, GlobalTypes)
	for i, req := range reqs {
		f.reset(nil, protoVersion5, GlobalTypes)
		require.NoError(t, f.writeBatchFrame(0, req, nil))
		bodyBacking := backing(f.readBuffer)

		require.NoError(t, f.prepareModernLayout())
		assert.Equal(t, want[i], f.buf, "reused framer diverged on frame %d (stale tail / bad reuse)", i)

		// The emitted self-contained segment lives in segBuf, a buffer distinct from
		// the body — and layout must not disturb the retained body buffer.
		require.Equal(t, backing(f.segBuf), backing(f.buf),
			"emitted segment must be segBuf (frame %d)", i)
		require.NotEqual(t, bodyBacking, backing(f.segBuf),
			"segBuf must be distinct from the body buffer (frame %d)", i)
		require.Equal(t, bodyBacking, backing(f.readBuffer),
			"prepareModernLayout must not disturb the retained body buffer (frame %d)", i)

		f.reset(nil, protoVersion5, GlobalTypes)
		require.Equal(t, bodyBacking, backing(f.buf),
			"reset must recover the body backing, not the segment backing (frame %d)", i)
	}
}

// TestPrepareModernLayoutMultiSegment covers the >128 KiB write path (untouched by the
// fast path but now sharing finish() retention): at the segment boundary and beyond,
// every emitted segment must parse and reassemble to the original body, with the
// expected segment count.
func TestPrepareModernLayoutMultiSegment(t *testing.T) {
	// Value sizes chosen so the resulting body straddles 1, 2 and 3 segments.
	for _, valueBytes := range []int{maxSegmentPayloadSize - 64, maxSegmentPayloadSize, 2 * maxSegmentPayloadSize} {
		f := newFramer(nil, protoVersion5, GlobalTypes)
		require.NoError(t, f.writeBatchFrame(0, buildBigBatchReq(valueBytes), nil))
		body := append([]byte(nil), f.buf...)
		wantSegs := (len(body) + maxSegmentPayloadSize - 1) / maxSegmentPayloadSize

		require.NoError(t, f.prepareModernLayout())

		got, segs := reassembleUncompressedSegments(t, f.buf)
		assert.Equal(t, wantSegs, segs, "segment count for body len %d", len(body))
		assert.Equal(t, body, got, "reassembled body must equal pre-layout body (valueBytes=%d)", valueBytes)
	}
}

// TestPrepareModernLayoutCompressedSelfContained covers the changed compressed
// self-contained branch (which now uses newCompressedSegment's buffer directly instead
// of copying it). With a deterministic compressor, the emitted segment must equal the
// reference build and round-trip back to the original body.
func TestPrepareModernLayoutCompressedSelfContained(t *testing.T) {
	comp := identityCompressor{}

	// Reference: legacy behavior (segment built by newCompressedSegment then used).
	ref := newFramer(comp, protoVersion5, GlobalTypes)
	require.NoError(t, ref.writeBatchFrame(0, buildTChartBatchReq(20), nil))
	body := append([]byte(nil), ref.buf...)
	wantSeg, err := newCompressedSegment(body, true, comp)
	require.NoError(t, err)

	f := newFramer(comp, protoVersion5, GlobalTypes)
	require.NoError(t, f.writeBatchFrame(0, buildTChartBatchReq(20), nil))
	require.NoError(t, f.prepareModernLayout())
	assert.Equal(t, wantSeg, f.buf, "compressed self-contained segment must match reference")

	readback, isSelfContained, err := readCompressedSegment(bytes.NewReader(f.buf), comp)
	require.NoError(t, err)
	assert.True(t, isSelfContained)
	assert.Equal(t, body, readback)
}

// TestProtoV4FinishRetentionReuse covers proto-v4 writes, which skip prepareModernLayout
// but now share finish()'s buffer retention. A grown frame built twice on the same
// framer must retain capacity and produce byte-identical output on reuse.
func TestProtoV4FinishRetentionReuse(t *testing.T) {
	f := newFramer(nil, protoVersion4, GlobalTypes)
	require.NoError(t, f.writeBatchFrame(0, buildTChartBatchReq(100), nil))
	first := append([]byte(nil), f.buf...)
	require.Greater(t, len(first), defaultBufSize)
	assert.GreaterOrEqual(t, cap(f.readBuffer), len(f.buf), "proto-v4 finish() must retain the grown buffer")

	f.reset(nil, protoVersion4, GlobalTypes)
	require.NoError(t, f.writeBatchFrame(0, buildTChartBatchReq(100), nil))
	assert.Equal(t, first, f.buf, "proto-v4 reuse must be byte-identical")
}
