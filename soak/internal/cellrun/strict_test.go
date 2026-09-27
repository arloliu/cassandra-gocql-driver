package cellrun

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// strictRecord has one field of each shape the writer types use.
type strictRecord struct {
	N     int            `json:"n"`
	F     float64        `json:"f"`
	M     map[string]int `json:"m"`
	L     []string       `json:"l"`
	P     *float64       `json:"p"`
	Opt   string         `json:"opt,omitempty"`
	Inner struct {
		B bool `json:"b"`
	} `json:"inner"`
}

// The completeness rule (PLAN §41.2): a record is accepted only if it is exactly what the writer emits for its value.
func TestStrictDecode(t *testing.T) {
	for name, tc := range map[string]struct {
		raw, want string
	}{
		"exact":                       {`{"n":1,"f":0.5,"m":{"a":2},"l":["x"],"p":1.5,"opt":"o","inner":{"b":true}}`, ""},
		"nil map, slice and pointer":  {`{"n":0,"f":0,"m":null,"l":null,"p":null,"inner":{"b":false}}`, ""},
		"omitempty omitted":           {`{"n":1,"f":2,"m":{},"l":[],"p":null,"inner":{"b":false}}`, ""},
		"key order and spacing":       {`{ "inner": {"b": false}, "p": null, "l": [], "m": {}, "f": 2, "n": 1 }`, ""},
		"absent field":                {`{"f":2,"m":{},"l":[],"p":null,"inner":{"b":false}}`, "$.n: absent"},
		"absent nested field":         {`{"n":1,"f":2,"m":{},"l":[],"p":null,"inner":{}}`, "$.inner.b: absent"},
		"null scalar":                 {`{"n":null,"f":2,"m":{},"l":[],"p":null,"inner":{"b":false}}`, "$.n: null"},
		"null map value":              {`{"n":1,"f":2,"m":{"a":null},"l":[],"p":null,"inner":{"b":false}}`, "$.m.a"},
		"null slice element":          {`{"n":1,"f":2,"m":{},"l":[null],"p":null,"inner":{"b":false}}`, "$.l[0]"},
		"field the writer never uses": {`{"n":1,"f":2,"m":{},"l":[],"p":null,"inner":{"b":false},"x":1}`, "$.x: not a field"},
	} {
		t.Run(name, func(t *testing.T) {
			var v strictRecord
			d, err := strictDecode([]byte(tc.raw), &v)
			require.NoError(t, err)
			if tc.want == "" {
				require.Empty(t, d)
				return
			}
			require.Contains(t, d, tc.want)
		})
	}
	_, err := strictDecode([]byte(`{"n":"one"}`), new(strictRecord))
	require.Error(t, err, "a wrong type does not decode")
}
