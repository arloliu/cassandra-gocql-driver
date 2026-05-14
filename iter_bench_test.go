//go:build unit
// +build unit

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
 */

package gocql

import (
	"math/big"
	"testing"
)

// End-to-end Iter.Scan / Iter.MapScan benchmarks. The Iter is constructed
// directly from in-memory serialized row bytes — no network, no goroutines.
// This isolates the per-row scan cost so allocation and CPU changes in the
// scan/MapScan/Unmarshal hot path can be measured.

func benchMakeRow(cols []ColumnInfo, vals []interface{}) []byte {
	var buf []byte
	for i, col := range cols {
		b, err := Marshal(col.TypeInfo, vals[i])
		if err != nil {
			panic(err)
		}
		n := int32(len(b))
		buf = append(buf, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
		buf = append(buf, b...)
	}
	return buf
}

func benchRepeat(b []byte, n int) []byte {
	out := make([]byte, 0, len(b)*n)
	for i := 0; i < n; i++ {
		out = append(out, b...)
	}
	return out
}

// Mixed 10-column schema representative of a typical wide row.
func benchMixedCols() ([]ColumnInfo, []interface{}) {
	cols := []ColumnInfo{
		{Name: "id", TypeInfo: GlobalTypes.fastTypeInfoLookup(TypeBigInt)},
		{Name: "name", TypeInfo: GlobalTypes.fastTypeInfoLookup(TypeVarchar)},
		{Name: "age", TypeInfo: GlobalTypes.fastTypeInfoLookup(TypeInt)},
		{Name: "active", TypeInfo: GlobalTypes.fastTypeInfoLookup(TypeBoolean)},
		{Name: "score", TypeInfo: GlobalTypes.fastTypeInfoLookup(TypeDouble)},
		{Name: "email", TypeInfo: GlobalTypes.fastTypeInfoLookup(TypeVarchar)},
		{Name: "balance", TypeInfo: GlobalTypes.fastTypeInfoLookup(TypeBigInt)},
		{Name: "rank", TypeInfo: GlobalTypes.fastTypeInfoLookup(TypeInt)},
		{Name: "verified", TypeInfo: GlobalTypes.fastTypeInfoLookup(TypeBoolean)},
		{Name: "country", TypeInfo: GlobalTypes.fastTypeInfoLookup(TypeVarchar)},
	}
	vals := []interface{}{
		int64(12345),
		"alice doe",
		int(28),
		true,
		float64(3.14159),
		"alice@example.com",
		int64(99999),
		int(7),
		false,
		"US",
	}
	return cols, vals
}

func benchNewIter(numRows int, cols []ColumnInfo, rowBytes []byte) (*Iter, *framer, []byte) {
	pageBuf := benchRepeat(rowBytes, numRows)
	fr := &framer{
		header: &frameHeader{
			version: protoVersion4 | 0x80,
			op:      opResult,
			length:  len(pageBuf),
		},
		buf: pageBuf,
	}
	iter := &Iter{
		meta: resultMetadata{
			colCount:       len(cols),
			actualColCount: len(cols),
			columns:        cols,
		},
		numRows: numRows,
		framer:  fr,
	}
	return iter, fr, pageBuf
}

// MapScan: 1 row per b.Loop iteration, fresh map per row.
// Reports the cost of a single MapScan call including map allocation.
func BenchmarkIterMapScan_Mixed10_PerRow(b *testing.B) {
	cols, vals := benchMixedCols()
	rowBytes := benchMakeRow(cols, vals)
	iter, fr, pageBuf := benchNewIter(1, cols, rowBytes)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		fr.buf = pageBuf
		iter.pos = 0
		m := make(map[string]interface{}, len(cols))
		iter.MapScan(m)
	}
}

// MapScan: full 100-row page per b.Loop iteration, fresh map per row.
// Closest to typical paged iteration with a fresh map every row.
func BenchmarkIterMapScan_Mixed10_Page100(b *testing.B) {
	const rowsPerPage = 100
	cols, vals := benchMixedCols()
	rowBytes := benchMakeRow(cols, vals)
	iter, fr, pageBuf := benchNewIter(rowsPerPage, cols, rowBytes)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		fr.buf = pageBuf
		iter.pos = 0
		for {
			m := make(map[string]interface{}, len(cols))
			if !iter.MapScan(m) {
				break
			}
			_ = m
		}
	}
	b.ReportMetric(float64(rowsPerPage), "rows/op")
}

// MapScan: 1 row per b.Loop iteration, reusing one map (cleared between rows).
// Isolates the MapScan-internal allocation cost from the per-row map alloc.
func BenchmarkIterMapScan_Mixed10_ReusedMap(b *testing.B) {
	cols, vals := benchMixedCols()
	rowBytes := benchMakeRow(cols, vals)
	iter, fr, pageBuf := benchNewIter(1, cols, rowBytes)

	m := make(map[string]interface{}, len(cols))

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		fr.buf = pageBuf
		iter.pos = 0
		for k := range m {
			delete(m, k)
		}
		iter.MapScan(m)
	}
}

// Iter.Scan with typed destinations: 1 row per b.Loop iteration.
// Comparison baseline — no map plumbing, only Unmarshal.
func BenchmarkIterScan_Mixed10_PerRow(b *testing.B) {
	cols, vals := benchMixedCols()
	rowBytes := benchMakeRow(cols, vals)
	iter, fr, pageBuf := benchNewIter(1, cols, rowBytes)

	var (
		id       int64
		name     string
		age      int
		active   bool
		score    float64
		email    string
		balance  int64
		rank     int
		verified bool
		country  string
	)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		fr.buf = pageBuf
		iter.pos = 0
		iter.Scan(&id, &name, &age, &active, &score, &email, &balance, &rank, &verified, &country)
	}
}

// All-varchar 10-column schema, to validate the Unmarshal fast-path
// against a text-heavy workload where *string is the only destination
// type. The Varchar microbench had shown a slight regression when the
// fast-path switched on many types together; this measures the
// real-world impact.
func benchAllVarcharCols() ([]ColumnInfo, []interface{}) {
	cols := make([]ColumnInfo, 10)
	vals := make([]interface{}, 10)
	for i := range cols {
		cols[i] = ColumnInfo{
			Name:     "c" + string(rune('0'+i)),
			TypeInfo: GlobalTypes.fastTypeInfoLookup(TypeVarchar),
		}
		vals[i] = "hello world"
	}
	return cols, vals
}

func BenchmarkIterScan_AllVarchar10_PerRow(b *testing.B) {
	cols, vals := benchAllVarcharCols()
	rowBytes := benchMakeRow(cols, vals)
	iter, fr, pageBuf := benchNewIter(1, cols, rowBytes)

	var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9 string

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		fr.buf = pageBuf
		iter.pos = 0
		iter.Scan(&s0, &s1, &s2, &s3, &s4, &s5, &s6, &s7, &s8, &s9)
	}
}

func BenchmarkIterMapScan_AllVarchar10_PerRow(b *testing.B) {
	cols, vals := benchAllVarcharCols()
	rowBytes := benchMakeRow(cols, vals)
	iter, fr, pageBuf := benchNewIter(1, cols, rowBytes)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		fr.buf = pageBuf
		iter.pos = 0
		m := make(map[string]interface{}, len(cols))
		iter.MapScan(m)
	}
}

// Varint MapScan: measures the per-row Zero()-reset cost on a
// pointer-returning Zero() type. This is the bug-fix path — expected to
// allocate one *big.Int per row per column.
func BenchmarkIterMapScan_Varint5_PerRow(b *testing.B) {
	cols := make([]ColumnInfo, 5)
	vals := make([]interface{}, 5)
	for i := range cols {
		cols[i] = ColumnInfo{
			Name:     "v" + string(rune('0'+i)),
			TypeInfo: GlobalTypes.fastTypeInfoLookup(TypeVarint),
		}
		vals[i] = big.NewInt(int64(i + 1))
	}
	rowBytes := benchMakeRow(cols, vals)
	iter, fr, pageBuf := benchNewIter(1, cols, rowBytes)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		fr.buf = pageBuf
		iter.pos = 0
		m := make(map[string]interface{}, len(cols))
		iter.MapScan(m)
	}
}

// Focused microbench for queryRoutingInfo read path. Reads dominate
// (every Iter.Keyspace / Iter.Table / observer hop) and used to take
// an RWMutex on every call.
func BenchmarkRoutingInfo_GetKeyspace(b *testing.B) {
	ri := &queryRoutingInfo{}
	ri.set("ks", "tbl")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = ri.getKeyspace()
	}
}

// Writer-side: every set() now allocates a fresh *routingKsTable.
// Verifies the per-write cost we trade for the lock-free read path.
func BenchmarkRoutingInfo_Set(b *testing.B) {
	ri := &queryRoutingInfo{}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		ri.set("ks", "tbl")
	}
}

func BenchmarkRoutingInfo_GetKeyspace_Parallel(b *testing.B) {
	ri := &queryRoutingInfo{}
	ri.set("ks", "tbl")
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = ri.getKeyspace()
		}
	})
}

// Iter.Scan with typed destinations: 100 rows per b.Loop iteration.
func BenchmarkIterScan_Mixed10_Page100(b *testing.B) {
	const rowsPerPage = 100
	cols, vals := benchMixedCols()
	rowBytes := benchMakeRow(cols, vals)
	iter, fr, pageBuf := benchNewIter(rowsPerPage, cols, rowBytes)

	var (
		id       int64
		name     string
		age      int
		active   bool
		score    float64
		email    string
		balance  int64
		rank     int
		verified bool
		country  string
	)

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		fr.buf = pageBuf
		iter.pos = 0
		for iter.Scan(&id, &name, &age, &active, &score, &email, &balance, &rank, &verified, &country) {
		}
	}
	b.ReportMetric(float64(rowsPerPage), "rows/op")
}
