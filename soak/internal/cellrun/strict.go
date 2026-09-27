package cellrun

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
)

// strictDecode decodes raw into v and reports how encoding v differs from raw (PLAN §41.2, Codex AN01–AN06).
// Every record the derivation reads was written by encoding/json from v's type, so the two agree exactly
// unless the record lost something: a field the writer always emits that is absent, or a null where it writes a value,
// re-encodes as a zero and differs. A nil map, slice or pointer the writer wrote as null, and an omitempty field, agree.
//
// Parameters:
//   - raw: one record
//   - v: a pointer to the writer's type
//
// Returns:
//   - string: the first difference, by path; empty when the record is exactly what the writer emits for v
//   - error: when raw does not decode into v
func strictDecode(raw []byte, v any) (string, error) {
	if err := json.Unmarshal(raw, v); err != nil {
		return "", err
	}
	back, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	want, err := canonical(raw)
	if err != nil {
		return "", err
	}
	got, err := canonical(back)
	if err != nil {
		return "", err
	}
	return jsonDiff("$", want, got), nil
}

// canonical parses JSON into maps, slices and json.Numbers, keeping every number's text.
func canonical(raw []byte) (any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	err := d.Decode(&v)
	return v, err
}

// jsonDiff returns the first difference between the record and its re-encoding, by path.
func jsonDiff(path string, record, written any) string {
	switch r := record.(type) {
	case map[string]any:
		w, ok := written.(map[string]any)
		if !ok {
			return fmt.Sprintf("%s: an object, written as %s", path, show(written))
		}
		keys := slices.Sorted(maps.Keys(r))
		for k := range w {
			if !slices.Contains(keys, k) {
				keys = append(keys, k)
			}
		}
		slices.Sort(keys)
		for _, k := range keys {
			rv, inR := r[k]
			wv, inW := w[k]
			switch {
			case !inR:
				return fmt.Sprintf("%s.%s: absent, the harness writes %s", path, k, show(wv))
			case !inW:
				return fmt.Sprintf("%s.%s: not a field the harness writes", path, k)
			}
			if d := jsonDiff(path+"."+k, rv, wv); d != "" {
				return d
			}
		}
		return ""
	case []any:
		w, ok := written.([]any)
		if !ok || len(w) != len(r) {
			return fmt.Sprintf("%s: %s, written as %s", path, show(record), show(written))
		}
		for i := range r {
			if d := jsonDiff(fmt.Sprintf("%s[%d]", path, i), r[i], w[i]); d != "" {
				return d
			}
		}
		return ""
	default:
		if !reflect.DeepEqual(record, written) {
			return fmt.Sprintf("%s: %s, the harness writes %s for the value it decodes to", path, show(record), show(written))
		}
		return ""
	}
}

// show renders a value for a difference message, shortened.
func show(v any) string {
	raw, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	if len(raw) > 60 {
		return string(raw[:57]) + "..."
	}
	return string(raw)
}
