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

	"gopkg.in/inf.v0"
)

// Unit tests for Iter.MapScan. Cover:
//   - default-fill path (map empty, MapScan populates from each column's Zero())
//   - pre-populated typed-pointer path (Scan writes through user's pointers,
//     map ends up with dereferenced values)
//   - cross-row reuse of the lazily-built mapScanCache
//   - pointer-returning Zero() types (varint -> *big.Int, decimal -> *inf.Dec):
//     each row must produce a distinct pointer so previously-returned maps
//     are not mutated by later scans
//   - NULL-then-non-NULL transitions on the same pointer-Zero types
//     (a buggy cache would either stay nil or panic on the second row)

func TestMapScan_DefaultFill_NonTuple(t *testing.T) {
	cols, vals := benchMixedCols()
	rowBytes := benchMakeRow(cols, vals)
	iter, _, _ := benchNewIter(1, cols, rowBytes)

	m := make(map[string]interface{}, len(cols))
	if !iter.MapScan(m) {
		t.Fatalf("MapScan returned false, err=%v", iter.err)
	}

	if got, want := m["id"], int64(12345); got != want {
		t.Errorf("id: got %v (%T), want %v", got, got, want)
	}
	if got, want := m["name"], "alice doe"; got != want {
		t.Errorf("name: got %q, want %q", got, want)
	}
	if got, want := m["age"], 28; got != want {
		t.Errorf("age: got %v (%T), want %v", got, got, want)
	}
	if got, want := m["active"], true; got != want {
		t.Errorf("active: got %v, want %v", got, want)
	}
	if got, want := m["score"], 3.14159; got != want {
		t.Errorf("score: got %v, want %v", got, want)
	}
}

func TestMapScan_PrePopulatedPointers(t *testing.T) {
	cols, vals := benchMixedCols()
	rowBytes := benchMakeRow(cols, vals)
	iter, _, _ := benchNewIter(1, cols, rowBytes)

	var name string
	var age int
	m := map[string]interface{}{
		"name": &name,
		"age":  &age,
	}
	if !iter.MapScan(m) {
		t.Fatalf("MapScan returned false, err=%v", iter.err)
	}

	// Underlying user variables should be populated (Scan wrote through their pointers).
	if name != "alice doe" {
		t.Errorf("name var: got %q, want %q", name, "alice doe")
	}
	if age != 28 {
		t.Errorf("age var: got %d, want %d", age, 28)
	}
	// And the map should now hold the dereferenced values, not the pointers
	// (mirrors the legacy MapScan contract — see helpers.go documentation).
	if got, want := m["name"], "alice doe"; got != want {
		t.Errorf("m[name]: got %v (%T), want %v", got, got, want)
	}
	if got, want := m["age"], 28; got != want {
		t.Errorf("m[age]: got %v (%T), want %v", got, got, want)
	}
}

// Verifies that calling MapScan more than once on the same Iter reuses the
// cache and produces correct values for each row.
func TestMapScan_CacheReuse_AcrossRows(t *testing.T) {
	cols, vals := benchMixedCols()
	rowBytes := benchMakeRow(cols, vals)
	iter, _, _ := benchNewIter(3, cols, rowBytes)

	for r := 0; r < 3; r++ {
		m := make(map[string]interface{}, len(cols))
		if !iter.MapScan(m) {
			t.Fatalf("row %d: MapScan returned false, err=%v", r, iter.err)
		}
		if got := m["id"]; got != int64(12345) {
			t.Errorf("row %d id: got %v, want 12345", r, got)
		}
		if got := m["name"]; got != "alice doe" {
			t.Errorf("row %d name: got %v, want alice doe", r, got)
		}
	}
	if iter.mapScanCache == nil {
		t.Fatal("expected mapScanCache to be populated after first call")
	}
	if got := len(iter.mapScanCache.names); got != len(cols) {
		t.Errorf("cache.names: got len=%d, want %d", got, len(cols))
	}
}

// makeIterFromRows builds an Iter where each row's columns can be set
// to specific byte payloads (or nil for CQL NULL, encoded as length=-1).
// Used by tests that need explicit per-row data such as NULL transitions.
func makeIterFromRows(cols []ColumnInfo, rows [][][]byte) *Iter {
	var buf []byte
	for _, row := range rows {
		for _, v := range row {
			if v == nil {
				// CQL NULL: length = -1 (0xFFFFFFFF)
				buf = append(buf, 0xFF, 0xFF, 0xFF, 0xFF)
				continue
			}
			n := int32(len(v))
			buf = append(buf, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
			buf = append(buf, v...)
		}
	}
	fr := &framer{
		header: &frameHeader{version: protoVersion4 | 0x80, op: opResult, length: len(buf)},
		buf:    buf,
	}
	return &Iter{
		meta:    resultMetadata{colCount: len(cols), actualColCount: len(cols), columns: cols},
		numRows: len(rows),
		framer:  fr,
	}
}

// Regression: varint scanning across rows must return distinct *big.Int
// pointers, otherwise mutating one row's value mutates previously-returned
// row maps. Triggered when MapScan reuses a single cache.zeros[i] slot
// holding a *big.Int across rows: the Unmarshal reflect path unwraps the
// pointer and calls SetBytes in place.
func TestMapScan_Varint_DistinctPointersAcrossRows(t *testing.T) {
	cols := []ColumnInfo{{Name: "v", TypeInfo: GlobalTypes.fastTypeInfoLookup(TypeVarint)}}
	row1Bytes, _ := Marshal(cols[0].TypeInfo, big.NewInt(1))
	row2Bytes, _ := Marshal(cols[0].TypeInfo, big.NewInt(2))
	iter := makeIterFromRows(cols, [][][]byte{{row1Bytes}, {row2Bytes}})

	m1 := make(map[string]interface{})
	if !iter.MapScan(m1) {
		t.Fatalf("row 1 MapScan failed: %v", iter.err)
	}
	v1 := m1["v"].(*big.Int)
	if v1.Cmp(big.NewInt(1)) != 0 {
		t.Fatalf("row 1 value: got %v, want 1", v1)
	}

	m2 := make(map[string]interface{})
	if !iter.MapScan(m2) {
		t.Fatalf("row 2 MapScan failed: %v", iter.err)
	}
	v2 := m2["v"].(*big.Int)

	if v1 == v2 {
		t.Errorf("row 1 and row 2 share the same *big.Int pointer (%p) — row 1 map would mutate when row 2 is scanned", v1)
	}
	if v1.Cmp(big.NewInt(1)) != 0 {
		t.Errorf("row 1 was corrupted by row 2's scan: got %v, want 1", v1)
	}
	if v2.Cmp(big.NewInt(2)) != 0 {
		t.Errorf("row 2 value: got %v, want 2", v2)
	}
}

// Regression: decimal scanning across rows must return distinct *inf.Dec
// pointers, mirroring the varint case.
func TestMapScan_Decimal_DistinctPointersAcrossRows(t *testing.T) {
	cols := []ColumnInfo{{Name: "d", TypeInfo: GlobalTypes.fastTypeInfoLookup(TypeDecimal)}}
	d1 := inf.NewDec(10, 2) // 0.10
	d2 := inf.NewDec(20, 2) // 0.20
	row1Bytes, err := Marshal(cols[0].TypeInfo, *d1)
	if err != nil {
		t.Fatal(err)
	}
	row2Bytes, err := Marshal(cols[0].TypeInfo, *d2)
	if err != nil {
		t.Fatal(err)
	}
	iter := makeIterFromRows(cols, [][][]byte{{row1Bytes}, {row2Bytes}})

	m1 := make(map[string]interface{})
	if !iter.MapScan(m1) {
		t.Fatalf("row 1 MapScan failed: %v", iter.err)
	}
	v1 := m1["d"].(*inf.Dec)

	m2 := make(map[string]interface{})
	if !iter.MapScan(m2) {
		t.Fatalf("row 2 MapScan failed: %v", iter.err)
	}
	v2 := m2["d"].(*inf.Dec)

	if v1 == v2 {
		t.Errorf("row 1 and row 2 share the same *inf.Dec pointer (%p)", v1)
	}
	if v1.String() != d1.String() {
		t.Errorf("row 1 was corrupted by row 2's scan: got %v, want %v", v1, d1)
	}
	if v2.String() != d2.String() {
		t.Errorf("row 2 value: got %v, want %v", v2, d2)
	}
}

// Regression: NULL followed by a non-NULL varint must yield the non-NULL
// value on the second row. A buggy cache that stores a typed-nil *big.Int
// after the NULL would leave the second row's result stuck at nil because
// the reflect path recurses with the nil pointer and the type handler
// discards the fresh internal big.Int.
func TestMapScan_Varint_NullThenNonNull(t *testing.T) {
	cols := []ColumnInfo{{Name: "v", TypeInfo: GlobalTypes.fastTypeInfoLookup(TypeVarint)}}
	row2Bytes, _ := Marshal(cols[0].TypeInfo, big.NewInt(42))
	iter := makeIterFromRows(cols, [][][]byte{{nil}, {row2Bytes}})

	m1 := make(map[string]interface{})
	if !iter.MapScan(m1) {
		t.Fatalf("row 1 (NULL) MapScan failed: %v", iter.err)
	}

	m2 := make(map[string]interface{})
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("row 2 panicked scanning non-NULL after NULL: %v", r)
		}
	}()
	if !iter.MapScan(m2) {
		t.Fatalf("row 2 MapScan failed: %v", iter.err)
	}

	v, ok := m2["v"].(*big.Int)
	if !ok || v == nil {
		t.Fatalf("row 2 should be non-nil 42; got %v (type %T)", m2["v"], m2["v"])
	}
	if v.Cmp(big.NewInt(42)) != 0 {
		t.Errorf("row 2 value: got %v, want 42", v)
	}
}

// Regression: NULL followed by a non-NULL decimal must not panic. A buggy
// cache that stores a typed-nil *inf.Dec after the NULL would cause the
// type handler to dereference a nil pointer (*v = *inf.NewDecBig(...)).
func TestMapScan_Decimal_NullThenNonNull(t *testing.T) {
	cols := []ColumnInfo{{Name: "d", TypeInfo: GlobalTypes.fastTypeInfoLookup(TypeDecimal)}}
	row2Bytes, _ := Marshal(cols[0].TypeInfo, *inf.NewDec(5, 1))
	iter := makeIterFromRows(cols, [][][]byte{{nil}, {row2Bytes}})

	m1 := make(map[string]interface{})
	if !iter.MapScan(m1) {
		t.Fatalf("row 1 (NULL) MapScan failed: %v", iter.err)
	}

	m2 := make(map[string]interface{})
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("row 2 panicked scanning non-NULL after NULL: %v", r)
		}
	}()
	if !iter.MapScan(m2) {
		t.Fatalf("row 2 MapScan failed: %v", iter.err)
	}

	v, ok := m2["d"].(*inf.Dec)
	if !ok || v == nil {
		t.Fatalf("row 2 should be non-nil 0.5; got %v (type %T)", m2["d"], m2["d"])
	}
	if v.String() != "0.5" {
		t.Errorf("row 2 value: got %v, want 0.5", v)
	}
}
