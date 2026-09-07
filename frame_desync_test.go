//go:build unit
// +build unit

package gocql

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// errCompressor is a Compressor whose Decode always fails, standing in for a
// body that arrived whole but could not be decompressed.
type errCompressor struct{ err error }

func (c errCompressor) Name() string { return "err" }

func (c errCompressor) AppendCompressed(dst, src []byte) ([]byte, error) {
	return append(dst, src...), nil
}

func (c errCompressor) AppendDecompressed(dst, src []byte, _ uint32) ([]byte, error) {
	return nil, c.err
}

func (c errCompressor) AppendCompressedWithLength(dst, src []byte) ([]byte, error) {
	return append(dst, src...), nil
}

func (c errCompressor) AppendDecompressedWithLength(dst, src []byte) ([]byte, error) {
	return nil, c.err
}

// timeoutError is a net.Error reporting a timeout, so the tests can show that
// classification does not key on the error's dynamic type.
type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// zeroReader is an endless source of zero bytes, so an oversized-frame discard
// can be exercised without allocating the frame.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// failingReader yields n bytes and then fails with err.
type failingReader struct {
	data []byte
	err  error
}

func (r *failingReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

// TestFramer_ReadFrameMarksOnlyBoundaryLossAsTerminal pins the classification the
// connection relies on: a readFrame error is terminal exactly when part of the
// frame is still on the wire, never because of the error's type.
//
// This is the acceptance gate for F-frame-1's classification table. Every exit of
// readFrame appears here; adding an exit without deciding its side is a gap.
func TestFramer_ReadFrameMarksOnlyBoundaryLossAsTerminal(t *testing.T) {
	tests := []struct {
		name       string
		head       frameHeader
		reader     io.Reader
		compressor Compressor
		terminal   bool
	}{
		{
			name:     "negative body length",
			head:     frameHeader{length: -1},
			reader:   bytes.NewReader(nil),
			terminal: true,
		},
		{
			name:     "oversized body cannot be discarded",
			head:     frameHeader{length: maxFrameSize + 1},
			reader:   &failingReader{data: []byte("partial"), err: io.ErrUnexpectedEOF},
			terminal: true,
		},
		{
			name:     "body read times out mid-frame",
			head:     frameHeader{length: 8},
			reader:   &failingReader{data: []byte("half"), err: timeoutError{}},
			terminal: true,
		},
		{
			name:     "body truncated by a closed peer",
			head:     frameHeader{length: 8},
			reader:   bytes.NewReader([]byte("half")),
			terminal: true,
		},
		{
			name:     "zero bytes of the body arrive",
			head:     frameHeader{length: 8},
			reader:   &failingReader{err: timeoutError{}},
			terminal: true,
		},
		{
			name:     "oversized body discarded whole",
			head:     frameHeader{length: maxFrameSize + 1},
			reader:   io.LimitReader(zeroReader{}, maxFrameSize+1),
			terminal: false,
		},
		{
			name:     "complete body, no compressor for a compressed frame",
			head:     frameHeader{length: 4, flags: flagCompress},
			reader:   bytes.NewReader([]byte("body")),
			terminal: false,
		},
		{
			name:       "complete body, decompression fails with a plain error",
			head:       frameHeader{length: 4, flags: flagCompress},
			reader:     bytes.NewReader([]byte("body")),
			compressor: errCompressor{err: errors.New("corrupt")},
			terminal:   false,
		},
		{
			// The discriminator is wire consumption, not error type: this one is a
			// net.Error reporting a timeout and must still leave the link usable.
			name:       "complete body, decompression fails with a net.Error",
			head:       frameHeader{length: 4, flags: flagCompress},
			reader:     bytes.NewReader([]byte("body")),
			compressor: errCompressor{err: timeoutError{}},
			terminal:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFramer(tt.compressor, protoVersion4, GlobalTypes)
			head := tt.head

			err := f.readFrame(tt.reader, &head)
			require.Error(t, err, "every case here is an error case")

			var desync *frameReadError
			require.Equal(t, tt.terminal, errors.As(err, &desync),
				"terminal classification must follow how much of the frame was consumed: got %v", err)
		})
	}
}

// TestFramer_ReadFrameKeepsTheUnderlyingError proves the marker wraps rather than
// replaces, so callers can still inspect the cause.
func TestFramer_ReadFrameKeepsTheUnderlyingError(t *testing.T) {
	f := newFramer(nil, protoVersion4, GlobalTypes)
	head := frameHeader{length: 8}

	err := f.readFrame(&failingReader{data: []byte("half"), err: timeoutError{}}, &head)

	var desync *frameReadError
	require.True(t, errors.As(err, &desync))

	var netErr interface{ Timeout() bool }
	require.True(t, errors.As(err, &netErr), "the cause must stay reachable through the marker")
	require.True(t, netErr.Timeout())
}
