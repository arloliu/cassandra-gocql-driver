package config

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/canary"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

var testDriver = Driver{
	NumConns: 2, Consistency: "QUORUM", SerialConsistency: "SERIAL", Timeout: 2 * time.Second,
	ConnectTimeout: 5 * time.Second, ReconnectInterval: 10 * time.Second, HostPolicy: "token-aware(round-robin)",
	OpDeadline: 2 * time.Second, ShortDeadlineMin: 20 * time.Millisecond, ShortDeadlineMax: 300 * time.Millisecond, Retries: 2,
}

func base(t *testing.T, id string) Base {
	t.Helper()
	c, err := CellByID(id)
	require.NoError(t, err)
	return NightBase(c, testDriver, 1500, 32)
}

func hashes(t *testing.T, b Base) (string, string) {
	t.Helper()
	ch, sh, err := Hashes(b)
	require.NoError(t, err)
	return ch, sh
}

// Every permitted override leaves both hashes unchanged (PLAN §9 step 8).
func TestOverridesLeaveHashesUnchanged(t *testing.T) {
	b := base(t, "c50p5")
	night, err := New(b, Overrides{Mode: "night"}, 1)
	require.NoError(t, err)
	for name, o := range map[string]Overrides{
		"validation control": ValidationOverrides(""),
		"canary":             ValidationOverrides("K7"),
		"canary params": func() Overrides {
			o := ValidationOverrides("K16")
			o.CanaryParams = map[string]float64{"n": 20}
			return o
		}(),
		"k12 expected version": ValidationOverrides("K12"),
		"duration":             {Mode: "night", Workload: time.Hour},
		"g15 scale":            {Mode: "night", G15Scale: 0.5},
	} {
		c, err := New(b, o, 99)
		require.NoError(t, err, name)
		require.Equal(t, night.CellHash, c.CellHash, name)
		require.Equal(t, night.SharedHash, c.SharedHash, name)
	}
}

// A change to a shared field changes both hashes; a change to a cell field changes only the cell hash.
func TestChangesMoveTheRightHash(t *testing.T) {
	b := base(t, "c50p5")
	ch0, sh0 := hashes(t, b)

	shared := map[string]func(*Base){
		"rate":           func(b *Base) { b.Shared.Rate = 1600 },
		"workers":        func(b *Base) { b.Shared.Workers = 33 },
		"mix":            func(b *Base) { b.Shared.Mix[0].Weight++; b.Shared.Mix[1].Weight-- },
		"driver setting": func(b *Base) { b.Shared.Driver.NumConns = 3 },
		"fault param": func(b *Base) {
			sp := b.Shared.Faults[chaos.FaultStop]
			sp.Remove = time.Minute
			b.Shared.Faults[chaos.FaultStop] = sp
		},
		"timetable":       func(b *Base) { b.Shared.Timetable[1].End += time.Second },
		"aux share":       func(b *Base) { b.Shared.Workload.AuxRateShare = 0.3 },
		"scan variant":    func(b *Base) { b.Shared.Workload.ControlledShare = 30 },
		"spec delay":      func(b *Base) { b.Shared.Workload.SpecMaxDelay = time.Second },
		"churn budget":    func(b *Base) { b.Shared.Churn.Create = time.Minute },
		"optional faults": func(b *Base) { b.Shared.Optional = b.Shared.Optional[1:] },
	}
	for name, mutate := range shared {
		c := base(t, "c50p5")
		mutate(&c)
		ch, sh := hashes(t, c)
		require.NotEqual(t, ch0, ch, name)
		require.NotEqual(t, sh0, sh, name)
	}

	cell := map[string]func(*Base){
		"version":     func(b *Base) { b.Cell.Version = "4.1.12" },
		"protocol":    func(b *Base) { b.Cell.Proto = 4 },
		"compression": func(b *Base) { b.Cell.Compression = "lz4-frame" },
		"assignment":  func(b *Base) { b.Cell.Short[0] = chaos.FaultLat },
		"long fault":  func(b *Base) { b.Cell.Long = chaos.FaultTopo },
	}
	for name, mutate := range cell {
		c := base(t, "c50p5")
		mutate(&c)
		ch, sh := hashes(t, c)
		require.NotEqual(t, ch0, ch, name)
		require.Equal(t, sh0, sh, name)
	}
}

// The four cells share one shared hash, so canary records can transfer between them.
func TestCellsShareTheSharedHash(t *testing.T) {
	var cellHashes []string
	_, want := hashes(t, base(t, "c50p5"))
	for _, c := range Cells() {
		ch, sh := hashes(t, NightBase(c, testDriver, 1500, 32))
		require.Equal(t, want, sh, c.ID)
		cellHashes = append(cellHashes, ch)
	}
	slices.Sort(cellHashes)
	require.Len(t, slices.Compact(cellHashes), 4)
}

func TestNormalizeIsCanonical(t *testing.T) {
	b := base(t, "c41p4")
	shuffled := b
	shuffled.Cell.Short = slices.Clone(b.Cell.Short)
	slices.Reverse(shuffled.Cell.Short)
	shuffled.Shared.Mix = slices.Clone(b.Shared.Mix)
	slices.Reverse(shuffled.Shared.Mix)
	shuffled.Shared.Timetable = slices.Clone(b.Shared.Timetable)
	slices.Reverse(shuffled.Shared.Timetable)
	ch, sh := hashes(t, b)
	ch2, sh2 := hashes(t, shuffled)
	require.Equal(t, ch, ch2)
	require.Equal(t, sh, sh2)
	require.Equal(t, Normalize(b), Normalize(Normalize(b)))
	require.Equal(t, b.Shared.Mix, Normalize(shuffled).Shared.Mix)
	require.NoError(t, Normalize(b).Shared.Mix.Validate())
	require.ElementsMatch(t, workload.DefaultMix(), Normalize(b).Shared.Mix)
}

func TestEffective(t *testing.T) {
	b := base(t, "c50p5")
	night, err := New(b, Overrides{Mode: "night"}, 1)
	require.NoError(t, err)
	tl, slots, a, fixed := night.Effective()
	require.Equal(t, Timeline{Warmup: NightWarmup, Cooldown: NightCooldown, Workload: NightWorkload}, tl)
	require.Equal(t, chaos.NightTimetable(), slots)
	require.False(t, fixed)
	require.Equal(t, chaos.FaultRoll, a.Long)
	require.ElementsMatch(t, chaos.PhaseOneAssignment().Short, a.Short)
	require.InDelta(t, 1.0, night.G15Scale(), 0)

	val, err := New(b, ValidationOverrides(""), 1)
	require.NoError(t, err)
	tl, slots, a, fixed = val.Effective()
	require.Equal(t, Timeline{Warmup: ValidationWarmup, Cooldown: ValidationCooldown, Workload: ValidationWorkload}, tl)
	require.Equal(t, chaos.ValidationTimetable(), slots)
	require.True(t, fixed)
	require.Equal(t, []chaos.Kind{chaos.FaultStop, chaos.FaultPause}, a.Short)
	require.InDelta(t, ValidationG15Scale, val.G15Scale(), 0)
}

// Every canary's overrides leave both hashes unchanged (PLAN §44.4, hash neutrality).
func TestEveryCanaryIsHashNeutral(t *testing.T) {
	b := base(t, "c50p5")
	control, err := New(b, ValidationOverrides(""), 1)
	require.NoError(t, err)
	for _, s := range canary.All() {
		o := ValidationOverrides(s.ID)
		require.Equal(t, s.ID, o.Canary)
		c, err := New(b, o, 2)
		require.NoError(t, err, s.ID)
		require.Equal(t, control.CellHash, c.CellHash, s.ID)
		require.Equal(t, control.SharedHash, c.SharedHash, s.ID)
		require.Equal(t, b.Shared.Workload, c.Base.Shared.Workload, "%s: workload.Params is untouched", s.ID)
	}
}

// Only K12 changes G0's expected version; the ccm install directory keeps using the cell's (Base.Cell.Version).
func TestExpectedVersion(t *testing.T) {
	b := base(t, "c50p5")
	for _, id := range []string{"", "K7"} {
		c, err := New(b, ValidationOverrides(id), 1)
		require.NoError(t, err)
		require.Empty(t, c.Overrides.ExpectedVersion, id)
		require.Equal(t, "5.0.3", c.ExpectedVersion(), id)
	}
	k12, _ := canary.Lookup("K12")
	c, err := New(b, ValidationOverrides("K12"), 1)
	require.NoError(t, err)
	require.Equal(t, k12.ExpectedVersion, c.ExpectedVersion())
	require.Equal(t, "5.0.3", c.Base.Cell.Version)
}

// No canary, no override field: a control's and a night's overrides are what they were before the canaries.
func TestNoCanaryNoOverrideField(t *testing.T) {
	o := ValidationOverrides("")
	require.Empty(t, o.Canary)
	require.Empty(t, o.ExpectedVersion)
	require.Nil(t, o.CanaryParams)
	raw, err := json.Marshal(o)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "canary")
	require.NotContains(t, string(raw), "expected_version")
}
