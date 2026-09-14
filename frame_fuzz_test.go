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
	"testing"
)

// goFuzzCrashers are inputs found with go-fuzz (github.com/dvyukov/go-fuzz)
// that once panicked somewhere in the decode path. They are the seed corpus for
// FuzzFrameDecode and the fixture for TestFuzzBugs, which asserts the narrower
// property that none of them decodes into a frame.
//
// Keep them here, in one copy: they are a regression corpus, not test data
// belonging to either test on its own.
var goFuzzCrashers = [][]byte{
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

// decodeFrameBytes drives the server-to-client decode path the way conn.go
// does: read the header, size a framer from the version it declares, read the
// body, then parse it. Every step is allowed to fail - arbitrary bytes are not
// a frame - and a failure at any of them is reported as not-decoded rather than
// as a test failure.
func decodeFrameBytes(data []byte) (frame, bool) {
	r := bytes.NewReader(data)

	head, err := readHeader(r, make([]byte, 9))
	if err != nil {
		return nil, false
	}

	f := newFramer(nil, byte(head.version), GlobalTypes)
	if err := f.readFrame(r, &head); err != nil {
		return nil, false
	}

	fr, err := f.parseFrame()
	if err != nil {
		return nil, false
	}

	return fr, true
}

// wellFormedResponseFrames returns one encodable response frame per protocol
// version the driver speaks, for every body shape cheap enough to build here.
//
// The direction bit is set on the version byte by hand. writeHeader emits
// f.proto verbatim, which makes a REQUEST frame, and parseFrame rejects those
// outright with "got a request frame from server" - so a seed built the obvious
// way never reaches a body parser at all. TestFrameDecodeSeeds is what keeps
// that mistake from going unnoticed again.
func wellFormedResponseFrames(tb testing.TB) [][]byte {
	tb.Helper()

	build := func(version byte, op frameOp, body func(f *framer)) []byte {
		f := newFramer(nil, version, GlobalTypes)
		f.writeHeader(0, op, 0)
		if body != nil {
			body(f)
		}
		if err := f.finish(); err != nil {
			tb.Fatalf("building a proto v%d frame: %v", version, err)
		}
		buf := append([]byte(nil), f.buf...)
		buf[0] = version | protoDirectionMask
		return buf
	}

	var frames [][]byte
	for _, version := range []byte{protoVersion3, protoVersion4, protoVersion5} {
		frames = append(frames,
			build(version, opReady, nil),
			build(version, opSupported, func(f *framer) {
				f.writeStringMultiMap(map[string][]string{"CQL_VERSION": {"3.0.0"}})
			}),
			build(version, opError, func(f *framer) {
				f.writeInt(0x0000)
				f.writeString("boom")
			}),
		)
	}
	return frames
}

// TestFrameDecodeSeeds pins that every well-formed seed really does decode.
//
// Without it the seeds can rot into bytes that die in readHeader, and
// FuzzFrameDecode would still pass - it treats a rejected input as the correct
// outcome - while fuzzing only ever explored the neighbourhood of garbage. That
// is the failure mode that left fuzz.go calling a newFramer signature that had
// not existed for years.
func TestFrameDecodeSeeds(t *testing.T) {
	for i, seed := range wellFormedResponseFrames(t) {
		fr, ok := decodeFrameBytes(seed)
		if !ok {
			t.Errorf("seed %d did not decode: % X", i, seed)
			continue
		}
		if fr == nil {
			t.Errorf("seed %d decoded to a nil frame: % X", i, seed)
		}
	}
}

// FuzzFrameDecode fuzzes envelope decoding: the 9-byte header, the body read
// sized from it, and parseFrame's dispatch over the body. Every byte it sees is
// attacker-supplied and untrusted at the point it is parsed.
//
// It does NOT cover the proto-v5 segment layer. After the v5 startup handshake,
// socket bytes reach this path only through segment decoding - segment header,
// CRC24 over that header, CRC32 over the payload, decompression and reassembly
// (conn.go) - and none of that is exercised here, however many executions pass.
// A second target over the segment decoder would be the way to cover it. The v5
// seeds below are legitimate inner frames, and legitimate whole frames on the
// unsegmented startup path; they are not post-startup socket bytes.
//
// It replaces the go-fuzz harness that used to live in fuzz.go under the
// `gofuzz` build tag. That file sat outside every lane - check-test-selection
// walks only *_test.go, and `gofuzz` is not one of the tag sets check-vet-lanes
// sweeps - so nothing compiled it, and its newFramer and readFrame calls had
// drifted out of date with the signatures they were calling.
//
// The assertion is deliberately weak, because it has to hold for arbitrary
// input: decoding must not panic, and must not report success without handing
// back a frame. Most generated inputs are rejected, which is the correct
// outcome, not a skip. The stronger "these must never decode" property belongs
// to TestFuzzBugs, which can assert it because its corpus is fixed.
//
// The seeds run as ordinary subtests whenever the unit lane selects this target
// - `make test-unit` does, a narrower -run filter may not. Generating new input
// happens only under -fuzz:
//
//	go test -tags unit -run FuzzFrameDecode -fuzz FuzzFrameDecode .
func FuzzFrameDecode(f *testing.F) {
	for _, crasher := range goFuzzCrashers {
		f.Add(crasher)
	}
	for _, seed := range wellFormedResponseFrames(f) {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		fr, ok := decodeFrameBytes(data)
		if !ok {
			return
		}

		// A decode that reports success must hand back something to act on.
		// Returning (nil, nil) would make every caller's type switch fall
		// through silently on attacker-supplied bytes.
		if fr == nil {
			t.Fatalf("parseFrame reported success but returned a nil frame for % X", data)
		}
	})
}
