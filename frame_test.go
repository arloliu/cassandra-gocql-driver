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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"bytes"
	"errors"
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/lz4"
	"github.com/apache/cassandra-gocql-driver/v2/snappy"
)

func TestFuzzBugs(t *testing.T) {
	// these inputs are found using go-fuzz (https://github.com/dvyukov/go-fuzz)
	// and should cause a panic unless fixed.
	tests := [][]byte{
		[]byte("00000\xa0000"),
		[]byte("\x8000\x0e\x00\x00\x00\x000"),
		[]byte("\x8000\x00\x00\x00\x00\t0000000000"),
		[]byte("\xa0\xff\x01\xae\xefqE\xf2\x1a"),
		[]byte("\x8200\b\x00\x00\x00c\x00\x00\x00\x02000\x01\x00\x00\x00\x03" +
			"\x00\n0000000000\x00\x14000000" +
			"00000000000000\x00\x020000" +
			"\x00\a000000000\x00\x050000000" +
			"\xff0000000000000000000" +
			"0000000"),
		[]byte("\x82\xe600\x00\x00\x00\x000"),
		[]byte("\x8200\b\x00\x00\x00\b0\x00\x00\x00\x040000"),
		[]byte("\x8200\x00\x00\x00\x00\x100\x00\x00\x12\x00\x00\x0000000" +
			"00000"),
		[]byte("\x83000\b\x00\x00\x00\x14\x00\x00\x00\x020000000" +
			"000000000"),
		[]byte("\x83000\b\x00\x00\x000\x00\x00\x00\x04\x00\x1000000" +
			"00000000000000e00000" +
			"000\x800000000000000000" +
			"0000000000000"),
	}

	for i, test := range tests {
		r := bytes.NewReader(test)
		head, err := readHeader(r, make([]byte, 9))
		if err != nil {
			continue
		}

		framer := newFramer(nil, byte(head.version), GlobalTypes)
		err = framer.readFrame(r, &head)
		if err != nil {
			continue
		}

		frame, err := framer.parseFrame()
		if err != nil {
			continue
		}

		t.Errorf("(%d) expected to fail for input % X", i, test)
		t.Errorf("(%d) frame=%+#v", i, frame)
	}
}

func TestFrameWriteTooLong(t *testing.T) {
	if os.Getenv("TRAVIS") == "true" {
		t.Skip("skipping test in travis due to memory pressure with the race detecor")
	}

	framer := newFramer(nil, 2, GlobalTypes)

	framer.writeHeader(0, opStartup, 1)
	framer.writeBytes(make([]byte, maxFrameSize+1))
	err := framer.finish()
	if err != ErrFrameTooBig {
		t.Fatalf("expected to get %v got %v", ErrFrameTooBig, err)
	}
}

func TestFrameReadTooLong(t *testing.T) {
	if os.Getenv("TRAVIS") == "true" {
		t.Skip("skipping test in travis due to memory pressure with the race detecor")
	}

	r := &bytes.Buffer{}
	r.Write(make([]byte, maxFrameSize+1))
	// write a new header right after this frame to verify that we can read it
	r.Write([]byte{protoVersionMask & protoVersion3, 0x00, 0x00, 0x00, byte(opReady), 0x00, 0x00, 0x00, 0x00})

	framer := newFramer(nil, 3, GlobalTypes)

	head := frameHeader{
		version: protoVersion3,
		op:      opReady,
		length:  r.Len() - frameHeadSize,
	}

	err := framer.readFrame(r, &head)
	if err != ErrFrameTooBig {
		t.Fatalf("expected to get %v got %v", ErrFrameTooBig, err)
	}

	head, err = readHeader(r, make([]byte, frameHeadSize))
	if err != nil {
		t.Fatal(err)
	}
	if head.op != opReady {
		t.Fatalf("expected to get header %v got %v", opReady, head.op)
	}
}

func Test_framer_writeExecuteFrame(t *testing.T) {
	framer := newFramer(nil, protoVersion5, GlobalTypes)
	nowInSeconds := 123
	frame := writeExecuteFrame{
		preparedID:       []byte{1, 2, 3},
		resultMetadataID: []byte{4, 5, 6},
		customPayload: map[string][]byte{
			"key1": []byte("value1"),
		},
		params: queryParams{
			nowInSeconds: &nowInSeconds,
			keyspace:     "test_keyspace",
		},
	}

	err := framer.writeExecuteFrame(123, frame.preparedID, frame.resultMetadataID, &frame.params, &frame.customPayload)
	if err != nil {
		t.Fatal(err)
	}

	// skipping header
	framer.buf = framer.buf[9:]

	bm, err := framer.readBytesMap()
	if err != nil {
		t.Fatal(err)
	}
	assertDeepEqual(t, "customPayload", frame.customPayload, bm)
	b, err := framer.readShortBytes()
	if err != nil {
		t.Fatal(err)
	}
	assertDeepEqual(t, "preparedID", frame.preparedID, b)
	b, err = framer.readShortBytes()
	if err != nil {
		t.Fatal(err)
	}
	assertDeepEqual(t, "resultMetadataID", frame.resultMetadataID, b)
	c, err := framer.readConsistency()
	if err != nil {
		t.Fatal(err)
	}
	assertDeepEqual(t, "constistency", frame.params.consistency, c)

	flags, err := framer.readInt()
	if err != nil {
		t.Fatal(err)
	}
	if flags&int(flagWithNowInSeconds) != int(flagWithNowInSeconds) {
		t.Fatal("expected flagNowInSeconds to be set, but it is not")
	}

	if flags&int(flagWithKeyspace) != int(flagWithKeyspace) {
		t.Fatal("expected flagWithKeyspace to be set, but it is not")
	}

	k, err := framer.readString()
	if err != nil {
		t.Fatal(err)
	}
	assertDeepEqual(t, "keyspace", frame.params.keyspace, k)
	secs, err := framer.readInt()
	if err != nil {
		t.Fatal(err)
	}
	assertDeepEqual(t, "nowInSeconds", nowInSeconds, secs)
}

func Test_framer_writeBatchFrame(t *testing.T) {
	framer := newFramer(nil, protoVersion5, GlobalTypes)
	nowInSeconds := 123
	frame := writeBatchFrame{
		customPayload: map[string][]byte{
			"key1": []byte("value1"),
		},
		nowInSeconds: &nowInSeconds,
	}

	err := framer.writeBatchFrame(123, &frame, frame.customPayload)
	if err != nil {
		t.Fatal(err)
	}

	// skipping header
	framer.buf = framer.buf[9:]

	bm, err := framer.readBytesMap()
	if err != nil {
		t.Fatal(err)
	}
	assertDeepEqual(t, "customPayload", frame.customPayload, bm)
	b, err := framer.readByte()
	if err != nil {
		t.Fatal(err)
	}
	assertDeepEqual(t, "typ", frame.typ, BatchType(b))
	l, err := framer.readShort()
	if err != nil {
		t.Fatal(err)
	}
	assertDeepEqual(t, "len(statements)", len(frame.statements), int(l))
	c, err := framer.readConsistency()
	if err != nil {
		t.Fatal(err)
	}
	assertDeepEqual(t, "consistency", frame.consistency, c)

	flags, err := framer.readInt()
	if err != nil {
		t.Fatal(err)
	}
	if flags&int(flagWithNowInSeconds) != int(flagWithNowInSeconds) {
		t.Fatal("expected flagNowInSeconds to be set, but it is not")
	}

	secs, err := framer.readInt()
	if err != nil {
		t.Fatal(err)
	}
	assertDeepEqual(t, "nowInSeconds", nowInSeconds, secs)
}

type testMockedCompressor struct {
	// this is an error its methods should return
	expectedError error

	// invalidateDecodedDataLength allows to simulate data decoding invalidation
	invalidateDecodedDataLength bool
}

func (m testMockedCompressor) Name() string {
	return "testMockedCompressor"
}

func (m testMockedCompressor) AppendCompressed(_, src []byte) ([]byte, error) {
	if m.expectedError != nil {
		return nil, m.expectedError
	}
	return src, nil
}

func (m testMockedCompressor) AppendDecompressed(_, src []byte, decompressedLength uint32) ([]byte, error) {
	if m.expectedError != nil {
		return nil, m.expectedError
	}

	// simulating invalid size of decoded data
	if m.invalidateDecodedDataLength {
		return src[:decompressedLength-1], nil
	}

	return src, nil
}

func (m testMockedCompressor) AppendCompressedWithLength(dst, src []byte) ([]byte, error) {
	panic("testMockedCompressor.AppendCompressedWithLength is not implemented")
}

func (m testMockedCompressor) AppendDecompressedWithLength(dst, src []byte) ([]byte, error) {
	panic("testMockedCompressor.AppendDecompressedWithLength is not implemented")
}

func Test_readUncompressedFrame(t *testing.T) {
	tests := []struct {
		name        string
		modifyFrame func([]byte) []byte
		expectedErr string
	}{
		{
			name: "header crc24 mismatch",
			modifyFrame: func(frame []byte) []byte {
				// simulating some crc invalidation
				frame[0] = 255
				return frame
			},
			expectedErr: "gocql: crc24 mismatch in frame header",
		},
		{
			name: "body crc32 mismatch",
			modifyFrame: func(frame []byte) []byte {
				// simulating body crc32 mismatch
				frame[len(frame)-1] = 255
				return frame
			},
			expectedErr: "gocql: payload crc32 mismatch",
		},
		{
			name: "invalid frame length",
			modifyFrame: func(frame []byte) []byte {
				// simulating body length invalidation
				frame = frame[:7]
				return frame
			},
			expectedErr: "gocql: failed to read uncompressed frame payload",
		},
		{
			name: "cannot read body checksum",
			modifyFrame: func(frame []byte) []byte {
				// simulating body length invalidation
				frame = frame[:len(frame)-4]
				return frame
			},
			expectedErr: "gocql: failed to read payload crc32",
		},
		{
			name:        "success",
			modifyFrame: nil,
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			framer := newFramer(nil, protoVersion5, GlobalTypes)
			req := writeQueryFrame{
				statement: "SELECT * FROM system.local",
				params: queryParams{
					consistency: Quorum,
					keyspace:    "gocql_test",
				},
			}

			err := req.buildFrame(framer, 128)
			require.NoError(t, err)

			frame, err := newUncompressedSegment(framer.buf, true)
			require.NoError(t, err)

			if tt.modifyFrame != nil {
				frame = tt.modifyFrame(frame)
			}

			readFrame, bufPtr, isSelfContained, err := readUncompressedSegment(bytes.NewReader(frame), nil)

			if tt.expectedErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErr)
				// On error the pooled buffer is released internally and no
				// ownership escapes to the caller.
				assert.Nil(t, bufPtr)
			} else {
				require.NoError(t, err)
				assert.True(t, isSelfContained)
				assert.Equal(t, framer.buf, readFrame)
				releaseSegmentBuffer(bufPtr)
			}
		})
	}
}

// TestSegmentBuffer covers getSegmentBuffer sizing/growth: a buffer is resliced
// to exactly the requested length and the backing array grows when the request
// exceeds current capacity.
func TestSegmentBuffer(t *testing.T) {
	// getSegmentBuffer returns a buffer of exactly the requested length.
	bp := getSegmentBuffer(100)
	require.NotNil(t, bp)
	assert.Len(t, *bp, 100)
	assert.GreaterOrEqual(t, cap(*bp), 100)
	releaseSegmentBuffer(bp)

	// A larger request grows the backing array to fit.
	big := getSegmentBuffer(maxSegmentPayloadSize)
	assert.Len(t, *big, maxSegmentPayloadSize)
	assert.GreaterOrEqual(t, cap(*big), maxSegmentPayloadSize)
	releaseSegmentBuffer(big)

	// The production wrapper must not panic on nil (end-to-end smoke over the real
	// pool); the deterministic guard is TestReleaseSegmentBufferFiltersNil.
	assert.NotPanics(t, func() { releaseSegmentBuffer(nil) })
}

// TestReleaseSegmentBufferFiltersNil deterministically verifies the nil guard:
// a nil buffer must NOT reach the pool (a typed-nil *[]byte would panic a later
// getSegmentBuffer dereference), and a real buffer must be forwarded exactly
// once. It drives releaseSegmentBufferInto with a counting sink, so the check is
// independent of non-deterministic sync.Pool.Get behavior under -race and races
// on no shared state. Removing the nil guard makes the first assertion fail.
func TestReleaseSegmentBufferFiltersNil(t *testing.T) {
	var puts []*[]byte
	sink := func(bp *[]byte) { puts = append(puts, bp) }

	releaseSegmentBufferInto(sink, nil)
	require.Empty(t, puts, "nil must be filtered before reaching the pool")

	b := make([]byte, 8)
	releaseSegmentBufferInto(sink, &b)
	require.Len(t, puts, 1)
	assert.Same(t, &b, puts[0])
}

// BenchmarkReadUncompressedSegment proves the payload buffer is pooled: with a
// reused bytes.Reader (so the reader itself is not measured), a steady-state
// read+release should not allocate a fresh payload per segment. Run outside the
// -race gate (sync.Pool drops puts under -race), so this is the allocation
// regression proof, not a -race assertion.
func BenchmarkReadUncompressedSegment(b *testing.B) {
	framer := newFramer(nil, protoVersion5, GlobalTypes)
	req := writeQueryFrame{
		statement: "SELECT * FROM system.local",
		params:    queryParams{consistency: Quorum, keyspace: "gocql_test"},
	}
	if err := req.buildFrame(framer, 128); err != nil {
		b.Fatal(err)
	}
	seg, err := newUncompressedSegment(framer.buf, true)
	if err != nil {
		b.Fatal(err)
	}

	r := bytes.NewReader(nil)
	b.ReportAllocs()
	for b.Loop() {
		r.Reset(seg)
		_, bufPtr, _, err := readUncompressedSegment(r, nil)
		if err != nil {
			b.Fatal(err)
		}
		releaseSegmentBuffer(bufPtr)
	}
}

func Test_readCompressedFrame(t *testing.T) {
	tests := []struct {
		name string
		// modifyFrameFn is useful for simulating frame data invalidation
		modifyFrameFn func([]byte) []byte
		compressor    testMockedCompressor

		// expectedErrorMsg is an error message that should be returned by Error() method.
		// We need this to understand which of fmt.Errorf() is returned
		expectedErrorMsg string
	}{
		{
			name: "header crc24 mismatch",
			modifyFrameFn: func(frame []byte) []byte {
				// simulating some crc invalidation
				frame[0] = 255
				return frame
			},
			expectedErrorMsg: "gocql: crc24 mismatch in frame header",
		},
		{
			name: "body crc32 mismatch",
			modifyFrameFn: func(frame []byte) []byte {
				// simulating body crc32 mismatch
				frame[len(frame)-1] = 255
				return frame
			},
			expectedErrorMsg: "gocql: crc32 mismatch in payload",
		},
		{
			name: "invalid frame length",
			modifyFrameFn: func(frame []byte) []byte {
				// simulating body length invalidation
				return frame[:12]
			},
			expectedErrorMsg: "gocql: failed to read compressed frame payload",
		},
		{
			name: "cannot read body checksum",
			modifyFrameFn: func(frame []byte) []byte {
				// simulating body length invalidation
				return frame[:len(frame)-4]
			},
			expectedErrorMsg: "gocql: failed to read payload crc32",
		},
		{
			name:          "failed to encode payload",
			modifyFrameFn: nil,
			compressor: testMockedCompressor{
				expectedError: errors.New("failed to encode payload"),
			},
			expectedErrorMsg: "failed to encode payload",
		},
		{
			name:          "failed to decode payload",
			modifyFrameFn: nil,
			compressor: testMockedCompressor{
				expectedError: errors.New("failed to decode payload"),
			},
			expectedErrorMsg: "failed to decode payload",
		},
		{
			name:          "length mismatch after decoding",
			modifyFrameFn: nil,
			compressor: testMockedCompressor{
				invalidateDecodedDataLength: true,
			},
			expectedErrorMsg: "gocql: length mismatch after payload decoding",
		},
		{
			name:             "success",
			modifyFrameFn:    nil,
			expectedErrorMsg: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			framer := newFramer(nil, protoVersion5, GlobalTypes)
			req := writeQueryFrame{
				statement: "SELECT * FROM system.local",
				params: queryParams{
					consistency: Quorum,
					keyspace:    "gocql_test",
				},
			}

			err := req.buildFrame(framer, 128)
			require.NoError(t, err)

			frame, err := newCompressedSegment(framer.buf, true, testMockedCompressor{})
			require.NoError(t, err)

			if tt.modifyFrameFn != nil {
				frame = tt.modifyFrameFn(frame)
			}

			readFrame, selfContained, err := readCompressedSegment(bytes.NewReader(frame), tt.compressor, nil)

			switch {
			case tt.expectedErrorMsg != "":
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErrorMsg)
			case tt.compressor.expectedError != nil:
				require.ErrorIs(t, err, tt.compressor.expectedError)
			default:
				require.NoError(t, err)
				assert.True(t, selfContained)
				assert.Equal(t, framer.buf, readFrame)
			}
		})
	}
}

func TestFrameReadParam(t *testing.T) {
	testCases := []struct {
		Write func(*framer)
		Param interface{}
		Exp   interface{}
	}{
		{
			Write: func(f *framer) {
				f.writeString("foo")
			},
			Param: "",
			Exp:   "foo",
		},
		{
			Write: func(f *framer) {
				f.writeShort(2)
				f.writeString("foo")
				f.writeString("bar")
			},
			Param: []interface{}{"", ""},
			Exp:   []interface{}{"foo", "bar"},
		},
		{
			Write: func(f *framer) {
				f.writeShort(uint16(TypeBoolean))
			},
			Param: (*TypeInfo)(nil),
			Exp:   booleanTypeInfo{},
		},
		{
			Write: func(f *framer) {
				f.writeInt(5)
			},
			Param: int(0),
			Exp:   int(5),
		},
		{
			Write: func(f *framer) {
				f.writeInt(5)
			},
			Param: new(int),
			Exp:   int(5),
		},
		{
			Write: func(f *framer) {
				f.writeShort(5)
			},
			Param: uint16(0),
			Exp:   uint16(5),
		},
		{
			Write: func(f *framer) {
				f.writeByte(10)
			},
			Param: byte(0),
			Exp:   byte(10),
		},
		{
			Write: func(f *framer) {
				f.writeShort(uint16(TypeBoolean))
			},
			Param: Type(0),
			Exp:   TypeBoolean,
		},
		{
			Write: func(f *framer) {
				f.writeShort(2)
				f.writeShort(uint16(TypeBoolean))
				f.writeShort(uint16(TypeBoolean))
			},
			Param: []TypeInfo{},
			Exp:   []TypeInfo{booleanTypeInfo{}, booleanTypeInfo{}},
		},
		{
			Write: func(f *framer) {
				f.writeShort(2)
				f.writeShort(uint16(TypeBoolean))
				f.writeShort(uint16(TypeBoolean))
			},
			Param: []TypeInfo{nil, nil},
			Exp:   []TypeInfo{booleanTypeInfo{}, booleanTypeInfo{}},
		},
		{
			Write: func(f *framer) {
				f.writeInt(5)
			},
			Param: func() *interface{} {
				var i interface{}
				i = int(0)
				return &i
			}(),
			Exp: int(5),
		},
	}
	for i := range testCases {
		framer := newFramer(nil, 4, GlobalTypes)
		testCases[i].Write(framer)
		res, err := framer.readParam(testCases[i].Param)
		if err != nil {
			t.Errorf("[%d] unexpected error: %v", i, err)
		} else if !reflect.DeepEqual(res, testCases[i].Exp) {
			t.Errorf("[%d] expected %+v, got %+v", i, testCases[i].Exp, res)
		}
	}
}

func TestFrameReadTypeInfo(t *testing.T) {
	tests := []struct {
		name     string
		typ      Type
		more     func(f *framer)
		custom   string
		expected TypeInfo
	}{
		{
			name: "text",
			typ:  TypeVarchar,
			expected: varcharLikeTypeInfo{
				typ: TypeVarchar,
			},
		},
		{
			name:     "boolean",
			typ:      TypeBoolean,
			expected: booleanTypeInfo{},
		},
		{
			name: "set_int",
			typ:  TypeSet,
			more: func(f *framer) {
				f.writeShort(uint16(TypeInt))
			},
			expected: CollectionType{
				typ:  TypeSet,
				Elem: intTypeInfo{},
			},
		},
		{
			name: "list_int",
			typ:  TypeList,
			more: func(f *framer) {
				f.writeShort(uint16(TypeInt))
			},
			expected: CollectionType{
				typ:  TypeList,
				Elem: intTypeInfo{},
			},
		},
		{
			name: "list_list_int",
			typ:  TypeList,
			more: func(f *framer) {
				f.writeShort(uint16(TypeList))
				f.writeShort(uint16(TypeInt))
			},
			expected: CollectionType{
				typ: TypeList,
				Elem: CollectionType{
					typ:  TypeList,
					Elem: intTypeInfo{},
				},
			},
		},
		{
			name: "map_int_int",
			typ:  TypeMap,
			more: func(f *framer) {
				f.writeShort(uint16(TypeInt))
				f.writeShort(uint16(TypeInt))
			},
			expected: CollectionType{
				typ:  TypeMap,
				Key:  intTypeInfo{},
				Elem: intTypeInfo{},
			},
		},
		{
			name: "list_list_int",
			typ:  TypeUDT,
			more: func(f *framer) {
				f.writeString("gocql_test")
				f.writeString("person")
				f.writeShort(3)
				f.writeString("first_name")
				f.writeShort(uint16(TypeVarchar))
				f.writeString("last_name")
				f.writeShort(uint16(TypeVarchar))
				f.writeString("age")
				f.writeShort(uint16(TypeInt))
			},
			expected: UDTTypeInfo{
				Keyspace: "gocql_test",
				Name:     "person",
				Elements: []UDTField{
					{Name: "first_name", Type: varcharLikeTypeInfo{typ: TypeVarchar}},
					{Name: "last_name", Type: varcharLikeTypeInfo{typ: TypeVarchar}},
					{Name: "age", Type: intTypeInfo{}},
				},
			},
		},
		{
			name: "tuple_int_int",
			typ:  TypeTuple,
			more: func(f *framer) {
				f.writeShort(2)
				f.writeShort(uint16(TypeInt))
				f.writeShort(uint16(TypeInt))
			},
			expected: TupleTypeInfo{
				Elems: []TypeInfo{
					intTypeInfo{},
					intTypeInfo{},
				},
			},
		},

		// these abuse the custom type to test some cases of typeInfoFromString
		{
			name:   "vector_text",
			typ:    TypeCustom,
			custom: "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.UTF8Type, 3)",
			expected: VectorType{
				SubType: varcharLikeTypeInfo{
					typ: TypeVarchar,
				},
				Dimensions: 3,
			},
		},
		{
			name:   "vector_set_int",
			typ:    TypeCustom,
			custom: "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.Int32Type), 2)",
			expected: VectorType{
				SubType: CollectionType{
					typ:  TypeSet,
					Elem: intTypeInfo{},
				},
				Dimensions: 2,
			},
		},
		{
			name:   "vector_udt",
			typ:    TypeCustom,
			custom: "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.UserType(gocql_test,706572736f6e,66697273745f6e616d65:org.apache.cassandra.db.marshal.UTF8Type,6c6173745f6e616d65:org.apache.cassandra.db.marshal.UTF8Type,616765:org.apache.cassandra.db.marshal.Int32Type), 2)",
			expected: VectorType{
				SubType: UDTTypeInfo{
					Keyspace: "gocql_test",
					Name:     "person",
					Elements: []UDTField{
						{Name: "first_name", Type: varcharLikeTypeInfo{typ: TypeVarchar}},
						{Name: "last_name", Type: varcharLikeTypeInfo{typ: TypeVarchar}},
						{Name: "age", Type: intTypeInfo{}},
					},
				},
				Dimensions: 2,
			},
		},
		{
			name:   "vector_tuple",
			typ:    TypeCustom,
			custom: "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.TupleType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type,org.apache.cassandra.db.marshal.UTF8Type), 2)",
			expected: VectorType{
				SubType: TupleTypeInfo{
					Elems: []TypeInfo{
						varcharLikeTypeInfo{typ: TypeVarchar},
						intTypeInfo{},
						varcharLikeTypeInfo{typ: TypeVarchar},
					},
				},
				Dimensions: 2,
			},
		},
		{
			name:   "vector_vector_inet",
			typ:    TypeCustom,
			custom: "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.InetAddressType, 2), 3)",
			expected: VectorType{
				SubType: VectorType{
					SubType:    inetType{},
					Dimensions: 2,
				},
				Dimensions: 3,
			},
		},
	}

	// org.apache.cassandra.db.marshal.VectorType(%s, 2)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newFramer(nil, 4, GlobalTypes)
			f.writeShort(uint16(test.typ))
			if test.typ == TypeCustom {
				f.writeString(test.custom)
			} else if test.more != nil {
				test.more(f)
			}
			parsedType, err := f.readTypeInfo()
			require.NoError(t, err)
			if len(f.buf) != 0 {
				t.Errorf("frame's buffer was not empty after readTypeInfo: %d left", len(f.buf))
			}
			if !reflect.DeepEqual(test.expected, parsedType) {
				t.Errorf("expected (%#v) but was (%#v) instead", test.expected, parsedType)
			}
		})
	}
}

// BenchmarkFramerReadCol_Tuple measures the per-column decode cost of a tuple
// type.
// meta must be a real *resultMetadata: readCol's tuple branch writes
// meta.actualColCount, so passing nil dereferences a nil pointer.
// actualColCount is a running counter that affects neither decoding nor
// allocation nor loop bounds, so it is deliberately not reset per iteration.
func BenchmarkFramerReadCol_Tuple(b *testing.B) {
	b.ReportAllocs()
	framer := newFramer(nil, 4, GlobalTypes)
	framer.writeString("foo")
	framer.writeShort(uint16(TypeTuple))
	framer.writeShort(uint16(2))
	framer.writeShort(uint16(TypeVarchar))
	framer.writeShort(uint16(TypeVarchar))
	buf := framer.buf
	var col ColumnInfo
	meta := &resultMetadata{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		framer.buf = buf
		if err := framer.readCol(&col, meta, true, "", ""); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFramerReadCol_Set measures the per-column decode cost of a set
// type.
// It shares the tuple benchmark's contract: a real *resultMetadata, and a
// checked error so that an early return cannot masquerade as a speed-up.
func BenchmarkFramerReadCol_Set(b *testing.B) {
	b.ReportAllocs()

	framer := newFramer(nil, 4, GlobalTypes)
	framer.writeString("foo")
	framer.writeShort(uint16(TypeSet))
	framer.writeShort(uint16(TypeInt))
	buf := framer.buf
	var col ColumnInfo
	meta := &resultMetadata{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		framer.buf = buf
		if err := framer.readCol(&col, meta, true, "", ""); err != nil {
			b.Fatal(err)
		}
	}
}

func Test_newFrame_compressionFlag(t *testing.T) {
	tests := []struct {
		name          string
		protoVersion  protoVersion
		compressor    Compressor
		expectedFlags byte
	}{
		{
			name:          "proto3-nil-compressor",
			protoVersion:  protoVersion3,
			compressor:    nil,
			expectedFlags: 0, // no flags
		},
		{
			name:          "proto3-snappy-compressor",
			protoVersion:  protoVersion3,
			compressor:    snappy.SnappyCompressor{},
			expectedFlags: 0b1, // compressions is enabled
		},
		{
			name:          "proto4-nil-compressor",
			protoVersion:  protoVersion4,
			compressor:    nil,
			expectedFlags: 0,
		},
		{
			name:          "proto4-snappy-compressor",
			protoVersion:  protoVersion4,
			compressor:    snappy.SnappyCompressor{},
			expectedFlags: 0b1,
		},
		{
			name:          "proto5-nil-compressor",
			protoVersion:  protoVersion5,
			compressor:    nil,
			expectedFlags: 0,
		},
		{
			// In protocol v5 compression happens on the segment level (v5 new frame format). The body of the frame (envelope)
			// is not compressed, so we don't have to set compression flag in the frame header
			name:          "proto5-lz4-compressor-no-compression-flag",
			protoVersion:  protoVersion5,
			compressor:    lz4.LZ4Compressor{},
			expectedFlags: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newFramer(test.compressor, byte(test.protoVersion), GlobalTypes)
			require.Equal(t, test.expectedFlags, f.flags)
		})
	}
}

func newErrorFrameForTest(code int, msg string) *framer {
	f := newFramer(nil, protoVersion4, GlobalTypes)
	f.header = &frameHeader{
		version: protoVersion4 | protoDirectionMask,
		stream:  1,
		op:      opError,
	}
	f.writeInt(int32(code))
	f.writeString(msg)
	return f
}

func TestParseErrorFrameDedicatedTypes(t *testing.T) {
	tests := []struct {
		name    string
		code    int
		msg     string
		assertT func(*testing.T, frame)
	}{
		{
			name: "overloaded",
			code: ErrCodeOverloaded,
			msg:  "coordinator overloaded",
			assertT: func(t *testing.T, got frame) {
				reqErr, ok := got.(*RequestErrOverloaded)
				if !ok {
					t.Fatalf("expected *RequestErrOverloaded, got %T", got)
				}
				if reqErr.Code() != ErrCodeOverloaded {
					t.Fatalf("expected code %x, got %x", ErrCodeOverloaded, reqErr.Code())
				}
				if reqErr.Message() != "coordinator overloaded" {
					t.Fatalf("expected message %q, got %q", "coordinator overloaded", reqErr.Message())
				}
				if reqErr.Error() != "coordinator overloaded" {
					t.Fatalf("expected error string %q, got %q", "coordinator overloaded", reqErr.Error())
				}
				if reqErr.Header().op != opError {
					t.Fatalf("expected op %v, got %v", opError, reqErr.Header().op)
				}
			},
		},
		{
			name: "bootstrapping",
			code: ErrCodeBootstrapping,
			msg:  "node is bootstrapping",
			assertT: func(t *testing.T, got frame) {
				reqErr, ok := got.(*RequestErrBootstrapping)
				if !ok {
					t.Fatalf("expected *RequestErrBootstrapping, got %T", got)
				}
				if reqErr.Code() != ErrCodeBootstrapping {
					t.Fatalf("expected code %x, got %x", ErrCodeBootstrapping, reqErr.Code())
				}
				if reqErr.Message() != "node is bootstrapping" {
					t.Fatalf("expected message %q, got %q", "node is bootstrapping", reqErr.Message())
				}
				if reqErr.Error() != "node is bootstrapping" {
					t.Fatalf("expected error string %q, got %q", "node is bootstrapping", reqErr.Error())
				}
				if reqErr.Header().op != opError {
					t.Fatalf("expected op %v, got %v", opError, reqErr.Header().op)
				}
			},
		},
		{
			name: "invalid",
			code: ErrCodeInvalid,
			msg:  "invalid query",
			assertT: func(t *testing.T, got frame) {
				reqErr, ok := got.(*RequestErrInvalid)
				if !ok {
					t.Fatalf("expected *RequestErrInvalid, got %T", got)
				}
				if reqErr.Code() != ErrCodeInvalid {
					t.Fatalf("expected code %x, got %x", ErrCodeInvalid, reqErr.Code())
				}
				if reqErr.Message() != "invalid query" {
					t.Fatalf("expected message %q, got %q", "invalid query", reqErr.Message())
				}
				if reqErr.Error() != "invalid query" {
					t.Fatalf("expected error string %q, got %q", "invalid query", reqErr.Error())
				}
				if reqErr.Header().op != opError {
					t.Fatalf("expected op %v, got %v", opError, reqErr.Header().op)
				}
			},
		},
		{
			name: "config",
			code: ErrCodeConfig,
			msg:  "configuration error",
			assertT: func(t *testing.T, got frame) {
				reqErr, ok := got.(*RequestErrConfig)
				if !ok {
					t.Fatalf("expected *RequestErrConfig, got %T", got)
				}
				if reqErr.Code() != ErrCodeConfig {
					t.Fatalf("expected code %x, got %x", ErrCodeConfig, reqErr.Code())
				}
				if reqErr.Message() != "configuration error" {
					t.Fatalf("expected message %q, got %q", "configuration error", reqErr.Message())
				}
				if reqErr.Error() != "configuration error" {
					t.Fatalf("expected error string %q, got %q", "configuration error", reqErr.Error())
				}
				if reqErr.Header().op != opError {
					t.Fatalf("expected op %v, got %v", opError, reqErr.Header().op)
				}
			},
		},
		{
			name: "credentials",
			code: ErrCodeCredentials,
			msg:  "bad credentials",
			assertT: func(t *testing.T, got frame) {
				reqErr, ok := got.(*RequestErrCredentials)
				if !ok {
					t.Fatalf("expected *RequestErrCredentials, got %T", got)
				}
				if reqErr.Code() != ErrCodeCredentials {
					t.Fatalf("expected code %x, got %x", ErrCodeCredentials, reqErr.Code())
				}
				if reqErr.Message() != "bad credentials" {
					t.Fatalf("expected message %q, got %q", "bad credentials", reqErr.Message())
				}
				if reqErr.Error() != "bad credentials" {
					t.Fatalf("expected error string %q, got %q", "bad credentials", reqErr.Error())
				}
				if reqErr.Header().op != opError {
					t.Fatalf("expected op %v, got %v", opError, reqErr.Header().op)
				}
			},
		},
		{
			name: "syntax",
			code: ErrCodeSyntax,
			msg:  "syntax error",
			assertT: func(t *testing.T, got frame) {
				reqErr, ok := got.(*RequestErrSyntax)
				if !ok {
					t.Fatalf("expected *RequestErrSyntax, got %T", got)
				}
				if reqErr.Code() != ErrCodeSyntax {
					t.Fatalf("expected code %x, got %x", ErrCodeSyntax, reqErr.Code())
				}
				if reqErr.Message() != "syntax error" {
					t.Fatalf("expected message %q, got %q", "syntax error", reqErr.Message())
				}
				if reqErr.Error() != "syntax error" {
					t.Fatalf("expected error string %q, got %q", "syntax error", reqErr.Error())
				}
				if reqErr.Header().op != opError {
					t.Fatalf("expected op %v, got %v", opError, reqErr.Header().op)
				}
			},
		},
		{
			name: "truncate",
			code: ErrCodeTruncate,
			msg:  "truncation error",
			assertT: func(t *testing.T, got frame) {
				reqErr, ok := got.(*RequestErrTruncate)
				if !ok {
					t.Fatalf("expected *RequestErrTruncate, got %T", got)
				}
				if reqErr.Code() != ErrCodeTruncate {
					t.Fatalf("expected code %x, got %x", ErrCodeTruncate, reqErr.Code())
				}
				if reqErr.Message() != "truncation error" {
					t.Fatalf("expected message %q, got %q", "truncation error", reqErr.Message())
				}
				if reqErr.Error() != "truncation error" {
					t.Fatalf("expected error string %q, got %q", "truncation error", reqErr.Error())
				}
				if reqErr.Header().op != opError {
					t.Fatalf("expected op %v, got %v", opError, reqErr.Header().op)
				}
			},
		},
		{
			name: "unauthorized",
			code: ErrCodeUnauthorized,
			msg:  "unauthorized",
			assertT: func(t *testing.T, got frame) {
				reqErr, ok := got.(*RequestErrUnauthorized)
				if !ok {
					t.Fatalf("expected *RequestErrUnauthorized, got %T", got)
				}
				if reqErr.Code() != ErrCodeUnauthorized {
					t.Fatalf("expected code %x, got %x", ErrCodeUnauthorized, reqErr.Code())
				}
				if reqErr.Message() != "unauthorized" {
					t.Fatalf("expected message %q, got %q", "unauthorized", reqErr.Message())
				}
				if reqErr.Error() != "unauthorized" {
					t.Fatalf("expected error string %q, got %q", "unauthorized", reqErr.Error())
				}
				if reqErr.Header().op != opError {
					t.Fatalf("expected op %v, got %v", opError, reqErr.Header().op)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newErrorFrameForTest(tt.code, tt.msg)

			got, err := f.parseErrorFrame()
			if err != nil {
				t.Fatalf("parseErrorFrame returned error: %v", err)
			}

			tt.assertT(t, got)
		})
	}
}

func TestParseErrorFrameAllGenericCodes(t *testing.T) {
	codes := []struct {
		name string
		code int
	}{
		{"overloaded", ErrCodeOverloaded},
		{"bootstrapping", ErrCodeBootstrapping},
		{"invalid", ErrCodeInvalid},
		{"config", ErrCodeConfig},
		{"credentials", ErrCodeCredentials},
		{"syntax", ErrCodeSyntax},
		{"truncate", ErrCodeTruncate},
		{"unauthorized", ErrCodeUnauthorized},
	}

	for _, tc := range codes {
		t.Run(tc.name, func(t *testing.T) {
			f := newErrorFrameForTest(tc.code, "test message")

			got, err := f.parseErrorFrame()
			if err != nil {
				t.Fatalf("parseErrorFrame returned error: %v", err)
			}

			reqErr, ok := got.(RequestError)
			if !ok {
				t.Fatalf("expected RequestError, got %T", got)
			}
			if reqErr.Code() != tc.code {
				t.Fatalf("expected code %x, got %x", tc.code, reqErr.Code())
			}
		})
	}
}

// TestResetBatchCollections is the linchpin B4 correctness guard: pooled batch
// collections must be fully zeroed on reuse, because marshalQueryValue and the
// batch build loop only conditionally set fields (a stale isUnset/name/preparedID
// would corrupt the wire frame). Driven on deliberately dirtied backing slices —
// no sync.Pool round-trip — so it is deterministic under -race.
func TestResetBatchCollections(t *testing.T) {
	// resetQueryValues zeroes the reused region (cap >= n path).
	dirtyQV := make([]queryValues, 4)
	for i := range dirtyQV {
		dirtyQV[i] = queryValues{value: []byte{1, 2, 3}, name: "stale", isUnset: true}
	}
	gotQV := resetQueryValues(dirtyQV, 3)
	require.Len(t, gotQV, 3)
	for i := range gotQV {
		assert.Equal(t, queryValues{}, gotQV[i], "queryValues[%d] not zeroed", i)
	}

	// resetBatchStmts likewise.
	dirtyBS := make([]batchStatment, 4)
	for i := range dirtyBS {
		dirtyBS[i] = batchStatment{preparedID: []byte{9}, statement: "stale", values: []queryValues{{value: []byte{1}}}}
	}
	gotBS := resetBatchStmts(dirtyBS, 3)
	require.Len(t, gotBS, 3)
	for i := range gotBS {
		assert.Equal(t, batchStatment{}, gotBS[i], "batchStatment[%d] not zeroed", i)
	}

	// Growth beyond cap allocates a fresh, zeroed slice.
	grown := resetQueryValues(make([]queryValues, 0, 1), 4)
	require.Len(t, grown, 4)
	for i := range grown {
		assert.Equal(t, queryValues{}, grown[i])
	}
}

// TestReleaseBatchCollectionsCap verifies the retention cap and nil-safety of the
// release helpers via an injected counting sink (deterministic; no sync.Pool
// round-trip): nil and oversized backing arrays are dropped, only within-cap
// slices are returned to the pool.
func TestReleaseBatchCollectionsCap(t *testing.T) {
	// queryValues: nil and over-cap are dropped; the within-cap slice is pooled and
	// must be cleared first so it does not pin the marshaled value payloads.
	var (
		qvPuts int
		qvGot  *[]queryValues
	)
	qvSink := func(sp *[]queryValues) { qvPuts++; qvGot = sp }
	releaseQueryValuesInto(qvSink, nil) // dropped
	within := make([]queryValues, 3, maxPooledQueryValues)
	within[0] = queryValues{value: []byte{1}, name: "x", isUnset: true}
	within[1] = queryValues{value: []byte{2}}
	within[2] = queryValues{value: []byte{3}}
	releaseQueryValuesInto(qvSink, &within)             // cap == ceiling -> retained
	over := make([]queryValues, maxPooledQueryValues+1) // cap > ceiling -> dropped
	releaseQueryValuesInto(qvSink, &over)
	require.Equal(t, 1, qvPuts, "only the within-cap queryValues slice should be pooled")
	require.NotNil(t, qvGot)
	for i := range *qvGot {
		assert.Equal(t, queryValues{}, (*qvGot)[i], "pooled queryValues[%d] must be cleared", i)
	}

	// batchStatment: the pooled slice must be cleared so its values sub-slices no
	// longer reference (pin) the flat []queryValues backing.
	var (
		bsPuts int
		bsGot  *[]batchStatment
	)
	bsSink := func(sp *[]batchStatment) { bsPuts++; bsGot = sp }
	releaseBatchStmtsInto(bsSink, nil)
	flat := make([]queryValues, 6)
	withinBS := make([]batchStatment, 3, maxPooledBatchStmts)
	for i := range withinBS {
		withinBS[i] = batchStatment{preparedID: []byte{byte(i)}, values: flat[i*2 : i*2+2]}
	}
	releaseBatchStmtsInto(bsSink, &withinBS)
	overBS := make([]batchStatment, maxPooledBatchStmts+1)
	releaseBatchStmtsInto(bsSink, &overBS)
	require.Equal(t, 1, bsPuts, "only the within-cap batchStatment slice should be pooled")
	require.NotNil(t, bsGot)
	for i := range *bsGot {
		assert.Equal(t, batchStatment{}, (*bsGot)[i], "pooled batchStatment[%d] must be cleared (no flat/prepared pin)", i)
	}
}

// TestBatchFastPathByteParity proves the fast-path collections (dirty backing ->
// reset -> flat sub-slices) serialize to byte-identical wire output as the
// make-based path. If clear-on-acquire regressed, a stale isUnset/name would
// change the bytes.
func TestBatchFastPathByteParity(t *testing.T) {
	const n, k = 3, 2
	rows := [][][]byte{
		{{0x01}, {0x02, 0x03}},
		{{0x04}, {0x05}},
		{{0x06, 0x07}, {0x08}},
	}
	preparedID := []byte{0xaa, 0xbb}

	// Reference: make-based build.
	ref := &writeBatchFrame{typ: LoggedBatch, consistency: Quorum, statements: make([]batchStatment, n)}
	for i := range ref.statements {
		bs := &ref.statements[i]
		bs.preparedID = preparedID
		bs.values = make([]queryValues, k)
		for j := 0; j < k; j++ {
			bs.values[j].value = rows[i][j]
		}
	}

	// Fast-path style from deliberately dirty backing (cap >= size so reset takes
	// the clear path, not the realloc path).
	dirtyStmts := make([]batchStatment, n)
	for i := range dirtyStmts {
		dirtyStmts[i] = batchStatment{preparedID: []byte{0xff}, statement: "junk", values: []queryValues{{isUnset: true}}}
	}
	dirtyFlat := make([]queryValues, n*k)
	for i := range dirtyFlat {
		dirtyFlat[i] = queryValues{value: []byte{0x99}, name: "junk", isUnset: true}
	}
	stmtsBuf := resetBatchStmts(dirtyStmts, n)
	flat := resetQueryValues(dirtyFlat, n*k)
	fast := &writeBatchFrame{typ: LoggedBatch, consistency: Quorum, statements: stmtsBuf}
	for i := 0; i < n; i++ {
		bs := &fast.statements[i]
		bs.preparedID = preparedID
		bs.values = flat[i*k : i*k+k]
		for j := 0; j < k; j++ {
			bs.values[j].value = rows[i][j]
		}
	}

	assert.Equal(t, serializeBatchFrame(t, ref), serializeBatchFrame(t, fast))
}

func serializeBatchFrame(t *testing.T, req *writeBatchFrame) []byte {
	t.Helper()
	f := newFramer(nil, protoVersion5, GlobalTypes)
	require.NoError(t, f.writeBatchFrame(0, req, nil))
	return append([]byte(nil), f.buf...)
}

// BenchmarkBatchCollections compares the per-batch collection allocations the B4
// fast path replaces (make([]batchStatment) + make([]queryValues) per statement)
// against the pooled flat buffer. Run outside -race (sync.Pool drops puts under
// -race). Expect the pooled variant to steady-state near zero allocs/op.
func BenchmarkBatchCollections(b *testing.B) {
	const n, k = 100, 3
	b.Run("make", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			stmts := make([]batchStatment, n)
			for i := range stmts {
				stmts[i].values = make([]queryValues, k)
			}
			_ = stmts
		}
	})
	b.Run("pooled", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			sp := getBatchStmts(n)
			flat := getQueryValues(n * k)
			stmts := *sp
			for i := range stmts {
				stmts[i].values = (*flat)[i*k : i*k+k]
			}
			releaseQueryValues(flat)
			releaseBatchStmts(sp)
		}
	})
	// general approximates the mixed/general path's per-batch collections (the
	// statement slice, per-statement values, and the stmts/localCache maps) that
	// the fast path avoids. It is the baseline the fast path is measured against
	// and guards against a regression that pushes the general path's allocations
	// higher; the general path itself is unchanged by B4.
	b.Run("general", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			stmts := make([]batchStatment, n)
			stmtsMap := make(map[string]string, n)
			localCache := make(map[string]*preparedStatment, 16)
			for i := range stmts {
				stmts[i].values = make([]queryValues, k)
				id := string(rune(i))
				stmtsMap[id] = "stmt"
				localCache[id] = nil
			}
			_, _, _ = stmts, stmtsMap, localCache
		}
	})
}
