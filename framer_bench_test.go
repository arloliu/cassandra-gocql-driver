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
	"compress/gzip"
	"io/ioutil"
	"os"
	"testing"
)

var benchSinkInt int

func readGzipData(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return ioutil.ReadAll(r)
}

func BenchmarkParseRowsFrame(b *testing.B) {
	data, err := readGzipData("testdata/frames/bench_parse_result.gz")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for b.Loop() {
		framer := &framer{
			header: &frameHeader{
				version: protoVersion4 | 0x80,
				op:      opResult,
				length:  len(data),
			},
			buf: data,
		}

		_, err = framer.parseFrame()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetFramerPooled(b *testing.B) {
	for b.Loop() {
		f := getFramer(nil, protoVersion4, GlobalTypes)
		f.release()
	}
}

func BenchmarkNewFramerNotPooled(b *testing.B) {
	for b.Loop() {
		_ = newFramer(nil, protoVersion4, GlobalTypes)
	}
}

func BenchmarkFramerHotPath_Pooled(b *testing.B) {
	data, err := readGzipData("testdata/frames/bench_parse_result.gz")
	if err != nil {
		b.Fatal(err)
	}

	head := frameHeader{
		version: protoVersion4 | 0x80,
		op:      opResult,
		length:  len(data),
	}

	reader := bytes.NewReader(data)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		reader.Reset(data)
		f := getFramer(nil, protoVersion4, GlobalTypes)
		if err := f.readFrame(reader, &head); err != nil {
			b.Fatal(err)
		}
		f.release()
	}
}

func BenchmarkFramerHotPath_New(b *testing.B) {
	data, err := readGzipData("testdata/frames/bench_parse_result.gz")
	if err != nil {
		b.Fatal(err)
	}

	head := frameHeader{
		version: protoVersion4 | 0x80,
		op:      opResult,
		length:  len(data),
	}

	reader := bytes.NewReader(data)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		reader.Reset(data)
		f := newFramer(nil, protoVersion4, GlobalTypes)
		if err := f.readFrame(reader, &head); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFramerReadAndParse_Pooled(b *testing.B) {
	data, err := readGzipData("testdata/frames/bench_parse_result.gz")
	if err != nil {
		b.Fatal(err)
	}

	head := frameHeader{
		version: protoVersion4 | 0x80,
		op:      opResult,
		length:  len(data),
	}

	reader := bytes.NewReader(data)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		reader.Reset(data)
		f := getFramer(nil, protoVersion4, GlobalTypes)
		if err := f.readFrame(reader, &head); err != nil {
			f.release()
			b.Fatal(err)
		}

		resp, err := f.parseFrame()
		if err != nil {
			f.release()
			b.Fatal(err)
		}

		// Touch only scalar values so we don't retain references to pooled buffers.
		switch x := resp.(type) {
		case *resultRowsFrame:
			benchSinkInt = x.numRows + len(x.meta.columns) + len(x.meta.pagingState)
		case *resultVoidFrame:
			benchSinkInt = 0
		case error:
			benchSinkInt = 1
		default:
			benchSinkInt = 2
		}

		f.release()
	}
}

func BenchmarkFramerReadAndParse_New(b *testing.B) {
	data, err := readGzipData("testdata/frames/bench_parse_result.gz")
	if err != nil {
		b.Fatal(err)
	}

	head := frameHeader{
		version: protoVersion4 | 0x80,
		op:      opResult,
		length:  len(data),
	}

	reader := bytes.NewReader(data)

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		reader.Reset(data)
		f := newFramer(nil, protoVersion4, GlobalTypes)
		if err := f.readFrame(reader, &head); err != nil {
			b.Fatal(err)
		}

		resp, err := f.parseFrame()
		if err != nil {
			b.Fatal(err)
		}

		switch x := resp.(type) {
		case *resultRowsFrame:
			benchSinkInt = x.numRows + len(x.meta.columns) + len(x.meta.pagingState)
		case *resultVoidFrame:
			benchSinkInt = 0
		case error:
			benchSinkInt = 1
		default:
			benchSinkInt = 2
		}
	}
}
