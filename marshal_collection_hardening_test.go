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
	"strings"
	"testing"
)

// These tests verify that the list/set/map unmarshal paths reject peer-
// supplied length prefixes that would otherwise drive reflect.MakeSlice
// or reflect.MakeMapWithSize into a panic or OOM-class allocation, and
// reject negative non-NULL element-length sentinels instead of silently
// treating them as NULL.

func TestList_Unmarshal_NegativeSize_ReturnsError(t *testing.T) {
	typ := CollectionType{typ: TypeList, Elem: intTypeInfo{}}
	// Outer collection size = -2 (int32 BE).
	data := []byte{0xFF, 0xFF, 0xFF, 0xFE}

	var dst []int
	err := Unmarshal(typ, data, &dst)
	if err == nil {
		t.Fatalf("expected error for negative list size, got nil; dst=%v", dst)
	}
	if !strings.Contains(err.Error(), "negative collection size") {
		t.Errorf("error %q does not mention negative collection size", err)
	}
}

func TestList_Unmarshal_OversizeCount_ReturnsError(t *testing.T) {
	typ := CollectionType{typ: TypeList, Elem: intTypeInfo{}}
	// size = 1_000_000, no element payload at all.
	data := []byte{0x00, 0x0F, 0x42, 0x40}

	var dst []int
	err := Unmarshal(typ, data, &dst)
	if err == nil {
		t.Fatalf("expected error for oversize list count, got nil; dst=%v", dst)
	}
	if !strings.Contains(err.Error(), "exceeds remaining buffer") {
		t.Errorf("error %q does not mention buffer-size bound", err)
	}
}

func TestList_Unmarshal_NegativeElementLength_ReturnsError(t *testing.T) {
	typ := CollectionType{typ: TypeList, Elem: intTypeInfo{}}
	// size = 1, element length = -2 (invalid; only -1 is NULL).
	data := []byte{0x00, 0x00, 0x00, 0x01, 0xFF, 0xFF, 0xFF, 0xFE}

	var dst []int
	err := Unmarshal(typ, data, &dst)
	if err == nil {
		t.Fatalf("expected error for negative element length, got nil; dst=%v", dst)
	}
	if !strings.Contains(err.Error(), "only -1 is valid for NULL") {
		t.Errorf("error %q does not mention the -1 NULL convention", err)
	}
}

func TestMap_Unmarshal_NegativeSize_ReturnsError(t *testing.T) {
	typ := CollectionType{
		typ:  TypeMap,
		Key:  varcharLikeTypeInfo{typ: TypeVarchar},
		Elem: intTypeInfo{},
	}
	// Outer map size = -2.
	data := []byte{0xFF, 0xFF, 0xFF, 0xFE}

	var dst map[string]int
	err := Unmarshal(typ, data, &dst)
	if err == nil {
		t.Fatalf("expected error for negative map size, got nil; dst=%v", dst)
	}
	if !strings.Contains(err.Error(), "negative map size") {
		t.Errorf("error %q does not mention negative map size", err)
	}
}

func TestMap_Unmarshal_OversizeCount_ReturnsError(t *testing.T) {
	typ := CollectionType{
		typ:  TypeMap,
		Key:  varcharLikeTypeInfo{typ: TypeVarchar},
		Elem: intTypeInfo{},
	}
	// size = 1_000_000, no entry payload.
	data := []byte{0x00, 0x0F, 0x42, 0x40}

	var dst map[string]int
	err := Unmarshal(typ, data, &dst)
	if err == nil {
		t.Fatalf("expected error for oversize map count, got nil; dst=%v", dst)
	}
	if !strings.Contains(err.Error(), "exceeds remaining buffer") {
		t.Errorf("error %q does not mention buffer-size bound", err)
	}
}

func TestMap_Unmarshal_NegativeElementLength_ReturnsError(t *testing.T) {
	typ := CollectionType{
		typ:  TypeMap,
		Key:  varcharLikeTypeInfo{typ: TypeVarchar},
		Elem: intTypeInfo{},
	}

	t.Run("key", func(t *testing.T) {
		// size=1, key length=-2 (invalid), then 8 trailing bytes so the
		// upper-bound check passes and execution reaches the element loop.
		data := []byte{
			0x00, 0x00, 0x00, 0x01,
			0xFF, 0xFF, 0xFF, 0xFE,
			0x00, 0x00, 0x00, 0x00,
		}
		var dst map[string]int
		err := Unmarshal(typ, data, &dst)
		if err == nil {
			t.Fatalf("expected error for negative key length, got nil; dst=%v", dst)
		}
		if !strings.Contains(err.Error(), "negative key length prefix") {
			t.Errorf("error %q does not mention negative key length", err)
		}
	})

	t.Run("value", func(t *testing.T) {
		// size=1, key length=-1 (NULL, valid), value length=-2 (invalid).
		data := []byte{
			0x00, 0x00, 0x00, 0x01,
			0xFF, 0xFF, 0xFF, 0xFF,
			0xFF, 0xFF, 0xFF, 0xFE,
		}
		var dst map[string]int
		err := Unmarshal(typ, data, &dst)
		if err == nil {
			t.Fatalf("expected error for negative value length, got nil; dst=%v", dst)
		}
		if !strings.Contains(err.Error(), "negative value length prefix") {
			t.Errorf("error %q does not mention negative value length", err)
		}
	})
}

// TestList_Unmarshal_PlausibleCountTruncated verifies that a count which passes
// the n > len(data)/4 bound check still produces an eof error when the data is
// truncated mid-element. This distinguishes the early count-bound rejection path
// from the per-element eof path.
func TestList_Unmarshal_PlausibleCountTruncated_ReturnsEof(t *testing.T) {
	typ := CollectionType{typ: TypeList, Elem: intTypeInfo{}}
	// n=2 with 8 bytes of element data: 2 > 8/4 = 2 is FALSE so bound check passes.
	// Element 1: length=4 (0x00000004) + 4 bytes payload => complete.
	// Element 2: no bytes remaining => unexpected eof on the second length read.
	data := []byte{
		0x00, 0x00, 0x00, 0x02, // count = 2
		0x00, 0x00, 0x00, 0x04, // elem[0] length = 4
		0x00, 0x00, 0x00, 0x01, // elem[0] payload = int(1)
		// elem[1] length prefix missing -> eof
	}

	var dst []int
	err := Unmarshal(typ, data, &dst)
	if err == nil {
		t.Fatalf("expected error for truncated list, got nil; dst=%v", dst)
	}
	if !strings.Contains(err.Error(), "unexpected eof") {
		t.Errorf("error %q does not mention unexpected eof", err)
	}
}

// TestMap_Unmarshal_PlausibleCountTruncated verifies that a count which passes
// the n > len(data)/8 bound check still produces an eof error when the data is
// truncated mid-entry. This distinguishes the early count-bound rejection path
// from the per-entry eof path.
func TestMap_Unmarshal_PlausibleCountTruncated_ReturnsEof(t *testing.T) {
	typ := CollectionType{
		typ:  TypeMap,
		Key:  varcharLikeTypeInfo{typ: TypeVarchar},
		Elem: intTypeInfo{},
	}
	// n=2 with 16 bytes of entry data: 2 > 16/8 = 2 is FALSE so bound check passes.
	// Entry 1: key-len=0 (4 bytes) + key="" (0 bytes) + val-len=4 (4 bytes) + val=1 (4 bytes) = 12 bytes.
	// Entry 2: key-len=4 (4 bytes) but no key payload -> unexpected eof.
	data := []byte{
		0x00, 0x00, 0x00, 0x02, // count = 2
		0x00, 0x00, 0x00, 0x00, // key[0] length = 0 (empty string)
		0x00, 0x00, 0x00, 0x04, // val[0] length = 4
		0x00, 0x00, 0x00, 0x01, // val[0] payload = int(1)
		0x00, 0x00, 0x00, 0x04, // key[1] length = 4, but payload missing -> eof
	}

	var dst map[string]int
	err := Unmarshal(typ, data, &dst)
	if err == nil {
		t.Fatalf("expected error for truncated map, got nil; dst=%v", dst)
	}
	if !strings.Contains(err.Error(), "unexpected eof") {
		t.Errorf("error %q does not mention unexpected eof", err)
	}
}
