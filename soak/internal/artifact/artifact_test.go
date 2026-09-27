package artifact

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAttemptID(t *testing.T) {
	now := time.Date(2026, 9, 25, 2, 15, 30, 0, time.FixedZone("x", 8*3600))
	id, err := AttemptID(now, bytes.NewReader([]byte{0x3f, 0xa9}))
	require.NoError(t, err)
	require.Equal(t, "20260924T181530Z-3fa9", id, "UTC, then 4 hex digits")
	_, err = AttemptID(now, bytes.NewReader(nil))
	require.Error(t, err)
	require.True(t, ValidAttemptID(id), "AttemptID output is valid")
}

func TestValidAttemptIDRejectsOtherShapes(t *testing.T) {
	for _, id := range []string{"", "20260924T181530Z-3FA9", "20260924T181530Z-3fa", "20260924T181530Z-3fa9x",
		"../20260924T181530Z-3fa9", "20260924T181530Z-3fa9/..", "2026092T181530Z-3fa9"} {
		require.False(t, ValidAttemptID(id), "%q", id)
	}
}

func TestNewExecDirNeverReuses(t *testing.T) {
	root := t.TempDir()
	dir, err := NewExecDir(root, "2026-09-25", "c50p5", RunNight, "20260925T000000Z-0001")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(root, "soak-2026-09-25", "c50p5", "night-20260925T000000Z-0001"), dir)
	for _, sub := range []string{"pprof", "ccm"} {
		st, err := os.Stat(filepath.Join(dir, sub))
		require.NoError(t, err)
		require.True(t, st.IsDir())
	}
	_, err = NewExecDir(root, "2026-09-25", "c50p5", RunNight, "20260925T000000Z-0001")
	require.ErrorIs(t, err, ErrExists)
}

func TestWriteJSONReplacesAtomically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "verdict.json")
	require.NoError(t, WriteJSON(path, map[string]string{"status": "running"}))
	require.NoError(t, WriteJSON(path, map[string]string{"status": "pass"}))
	var got map[string]string
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Equal(t, "pass", got["status"])
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "no temporary file is left behind")
	require.Error(t, WriteJSON(path, func() {}), "an unencodable value fails without touching the file")
	raw2, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, raw, raw2)
}

func TestJSONLConcurrentAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	j, err := OpenJSONL(path)
	require.NoError(t, err)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 100 {
				j.Write(map[string]int{"g": g, "i": i})
			}
		})
	}
	wg.Wait()
	require.NoError(t, j.Close())
	j.Write(map[string]int{"after": 1})

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	lines := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var v map[string]int
		require.NoError(t, json.Unmarshal(sc.Bytes(), &v), "every line is whole")
		lines++
	}
	require.Equal(t, 800, lines, "a write after Close is dropped")

	_, err = OpenJSONL(path)
	require.True(t, errors.Is(err, os.ErrExist), "an existing stream is never reopened")
}

func TestJSONLSkipsUnencodableValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.jsonl")
	j, err := OpenJSONL(path)
	require.NoError(t, err)
	j.Write(func() {})
	j.Write(math.NaN())
	j.Write(1)
	require.NoError(t, j.Err())
	require.Equal(t, 2, j.Skipped())
	require.NoError(t, j.Close())
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "1\n", string(raw), "the stream goes on after a bad value")
}

func TestJSONLKeepsFirstWriteError(t *testing.T) {
	j, err := OpenJSONL(filepath.Join(t.TempDir(), "x.jsonl"))
	require.NoError(t, err)
	require.NoError(t, j.f.Close())
	j.Write(1)
	require.Error(t, j.Err())
	require.Error(t, j.Close())
}
