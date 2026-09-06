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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gocql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// nilZeroCustomType is a registered custom CQL type whose TypeInfo reports a
// nil zero value, the shape that lets MapScan hand Unmarshal a nil interface
// for a NULL column without the caller doing anything unusual.
type nilZeroCustomType struct{}

var _ CQLType = nilZeroCustomType{}

func (nilZeroCustomType) Params(int) []interface{} { return nil }

func (nilZeroCustomType) TypeInfoFromParams(int, []interface{}) (TypeInfo, error) {
	return nilZeroTypeInfo{}, nil
}

func (nilZeroCustomType) TypeInfoFromString(int, string) (TypeInfo, error) {
	return nilZeroTypeInfo{}, nil
}

// nilZeroTypeInfo decodes its payload as a string and has a nil zero value.
type nilZeroTypeInfo struct{}

var _ TypeInfo = nilZeroTypeInfo{}

func (nilZeroTypeInfo) Type() Type        { return TypeCustom }
func (nilZeroTypeInfo) Zero() interface{} { return nil }
func (nilZeroTypeInfo) Marshal(v interface{}) ([]byte, error) {
	return []byte(v.(string)), nil
}
func (nilZeroTypeInfo) Unmarshal(data []byte, value interface{}) error {
	*(value.(*interface{})) = string(data)
	return nil
}

// TestUnmarshal_NullIntoNilInterface proves a CQL NULL decoded into a nil
// interface{} leaves it nil instead of panicking.
//
// The reflect.Interface branch of Unmarshal used to test IsValid on the
// interface slot (always valid for a *interface{} destination) instead of on
// the value inside it, and then called Type() on the zero Value that a nil
// interface's Elem() returns.
func TestUnmarshal_NullIntoNilInterface(t *testing.T) {
	var v interface{}
	require.NotPanics(t, func() {
		require.NoError(t, Unmarshal(GlobalTypes.fastTypeInfoLookup(TypeInt), nil, &v))
	})
	require.Nil(t, v, "a NULL must leave the interface nil")
}

// TestIterScan_NullIntoNilInterface proves the same through Iter.Scan.
func TestIterScan_NullIntoNilInterface(t *testing.T) {
	cols := []ColumnInfo{{Name: "v", TypeInfo: GlobalTypes.fastTypeInfoLookup(TypeInt)}}
	iter := makeIterFromRows(cols, [][][]byte{{nil}})

	var v interface{}
	require.NotPanics(t, func() {
		require.True(t, iter.Scan(&v), "Scan must report the row")
	})
	require.NoError(t, iter.err)
	require.Nil(t, v, "a NULL must leave the interface nil")
}

// TestIterScan_NonNullIntoNilInterface proves the same destination decodes a
// non-NULL value, so *interface{} is a supported destination shape and only the
// NULL path was broken.
func TestIterScan_NonNullIntoNilInterface(t *testing.T) {
	cols := []ColumnInfo{{Name: "v", TypeInfo: GlobalTypes.fastTypeInfoLookup(TypeInt)}}
	b, err := Marshal(cols[0].TypeInfo, 7)
	require.NoError(t, err)
	iter := makeIterFromRows(cols, [][][]byte{{b}})

	var v interface{}
	require.True(t, iter.Scan(&v))
	require.NoError(t, iter.err)
	require.Equal(t, 7, v)
}

// TestIterMapScan_NullCustomColumnWithNilZero proves MapScan with an ordinary
// empty map reaches the NULL-into-nil-interface path when the column's
// registered custom type has a nil zero value: MapScan fills the slot with
// TypeInfo.Zero() and hands Unmarshal a pointer to it.
//
// The TypeInfo is obtained through the registry, the way readTypeInfo obtains
// it from result metadata, so the column is one a live server can produce.
func TestIterMapScan_NullCustomColumnWithNilZero(t *testing.T) {
	const name = "org.example.NilZero"
	types := GlobalTypes.Copy()
	require.NoError(t, types.RegisterCustom(name, nilZeroCustomType{}))

	ti, err := types.typeInfoFromString(int(protoVersion4), name)
	require.NoError(t, err)
	require.Nil(t, ti.Zero(), "the fixture type must have a nil zero value")

	cols := []ColumnInfo{{Name: "v", TypeInfo: ti}}
	iter := makeIterFromRows(cols, [][][]byte{{nil}, {[]byte("x")}})

	m := map[string]interface{}{}
	require.NotPanics(t, func() {
		require.True(t, iter.MapScan(m), "MapScan must report the NULL row")
	})
	require.NoError(t, iter.err)
	require.Contains(t, m, "v")
	require.Nil(t, m["v"], "a NULL custom column must map to nil")

	m = map[string]interface{}{}
	require.True(t, iter.MapScan(m), "MapScan must report the non-NULL row")
	require.Equal(t, "x", m["v"])
}
