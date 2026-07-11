package gocql

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAppendUncompressedSegment proves the allocation-free write-side segment builder
// is byte-identical to newUncompressedSegment for every payload size and both
// self-contained flags, across the cold (nil dst) and warm-reuse (dst[:0]) paths, and
// that the produced segment round-trips back through readUncompressedSegment. A slip
// in header/CRC/length handling or a stale-capacity reslice would corrupt the frame
// sent to Cassandra, so this is the core correctness gate for B5.
func TestAppendUncompressedSegment(t *testing.T) {
	sizes := []int{0, 1, 15, 100, 1000, maxSegmentPayloadSize}
	for _, n := range sizes {
		payload := make([]byte, n)
		for i := range payload {
			payload[i] = byte(i*7 + 3)
		}
		for _, sc := range []bool{true, false} {
			want, err := newUncompressedSegment(payload, sc)
			require.NoError(t, err)

			// Cold path: nil dst allocates exactly what's needed.
			got, err := appendUncompressedSegment(nil, payload, sc)
			require.NoError(t, err)
			require.Equal(t, want, got, "cold n=%d sc=%v", n, sc)

			// Warm-reuse path: a small pre-existing buffer must be resliced/grown and
			// must not leak stale bytes past the segment.
			warm := make([]byte, 8)
			for i := range warm {
				warm[i] = 0xEE // sentinel: must not survive into the output
			}
			got2, err := appendUncompressedSegment(warm[:0], payload, sc)
			require.NoError(t, err)
			require.Equal(t, want, got2, "warm n=%d sc=%v", n, sc)

			// Round-trip: a self-contained segment must read back to the exact payload.
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

// TestAppendUncompressedSegmentOverMax verifies the size guard matches
// newUncompressedSegment: a payload larger than one segment is rejected, not silently
// truncated (the caller's multi-segment path handles oversize frames instead).
func TestAppendUncompressedSegmentOverMax(t *testing.T) {
	payload := make([]byte, maxSegmentPayloadSize+1)
	_, err := appendUncompressedSegment(nil, payload, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds maximum size")
}

// TestFramerFinishRetainsGrownBuffer proves finish() extends the framer's pooled
// buffer to the grown request body, so a reused framer no longer re-grows from
// defaultBufSize on every write. It asserts capacity growth (deterministic), not pool
// identity (sync.Pool drops puts under -race), matching the B1/B4 test convention.
func TestFramerFinishRetainsGrownBuffer(t *testing.T) {
	f := newFramer(nil, protoVersion5, GlobalTypes)
	require.Equal(t, defaultBufSize, cap(f.readBuffer))

	// A ~12 KB batch body forces the write buffer well past defaultBufSize.
	req := buildTChartBatchReq(100)
	require.NoError(t, f.writeBatchFrame(0, req, nil))

	assert.Greater(t, len(f.buf), defaultBufSize, "sanity: body must outgrow the initial buffer")
	assert.GreaterOrEqual(t, cap(f.readBuffer), len(f.buf),
		"finish() must retain the grown request buffer so the pool keeps the capacity")
}

// TestPrepareModernLayoutSelfContainedRoundTrip drives the new self-contained write
// path end to end: build a batch frame, capture the pre-segmentation body, apply the
// modern layout, and confirm the emitted segment reads back to the identical body.
func TestPrepareModernLayoutSelfContainedRoundTrip(t *testing.T) {
	f := newFramer(nil, protoVersion5, GlobalTypes)
	req := buildTChartBatchReq(50)
	require.NoError(t, f.writeBatchFrame(0, req, nil))

	body := append([]byte(nil), f.buf...) // copy: prepareModernLayout reassigns f.buf
	require.LessOrEqual(t, len(body), maxSegmentPayloadSize, "test frame must be self-contained")

	require.NoError(t, f.prepareModernLayout())

	payload, bufPtr, isSelfContained, err := readUncompressedSegment(bytes.NewReader(f.buf))
	require.NoError(t, err)
	assert.True(t, isSelfContained)
	assert.Equal(t, body, payload)
	releaseSegmentBuffer(bufPtr)
}

// TestPrepareModernLayoutWarmReuseIdentical guards the segBuf reuse: running two
// frames of different sizes through one pooled framer must not leak bytes from the
// first frame's segment into the second. Build small-then-large and large-then-small,
// comparing each pooled result against a fresh framer's output for the same request.
func TestPrepareModernLayoutWarmReuseIdentical(t *testing.T) {
	reqs := []*writeBatchFrame{buildTChartBatchReq(3), buildTChartBatchReq(80), buildTChartBatchReq(1)}

	// Reference outputs from independent fresh framers.
	want := make([][]byte, len(reqs))
	for i, req := range reqs {
		ref := newFramer(nil, protoVersion5, GlobalTypes)
		require.NoError(t, ref.writeBatchFrame(0, req, nil))
		require.NoError(t, ref.prepareModernLayout())
		want[i] = append([]byte(nil), ref.buf...)
	}

	// Same reused framer (via the pool) must produce identical bytes each time.
	for i, req := range reqs {
		f := getFramer(nil, protoVersion5, GlobalTypes)
		require.NoError(t, f.writeBatchFrame(0, req, nil))
		require.NoError(t, f.prepareModernLayout())
		assert.Equal(t, want[i], f.buf, "reused framer diverged on frame %d", i)
		f.release()
	}
}
