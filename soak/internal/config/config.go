// Package config describes a soak cell's effective configuration
// and computes its two configuration identities (PLAN §9 step 8).
//
// A configuration is a canonical base, exactly what the night runs for a cell,
// plus an override object for validation and canary runs.
// Both hashes are computed over the normalized base only, so a permitted override never changes them.
package config

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/canary"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

// Night timeline (PLAN §8.3), as offsets from the workload start.
const (
	// NightWarmup ends the warm-up: no faults, no churn, excluded from trends.
	NightWarmup = 15 * time.Minute
	// NightWorkload is how long the workload runs, cool-down included.
	NightWorkload = 120 * time.Minute
	// NightCooldown starts the cool-down.
	NightCooldown = 105 * time.Minute
)

// Validation timeline (PLAN §7), as offsets from the workload start.
const (
	// ValidationWarmup ends the warm-up.
	ValidationWarmup = 10 * time.Minute
	// ValidationCooldown starts the cool-down.
	ValidationCooldown = 40 * time.Minute
	// ValidationWorkload is how long the workload runs.
	ValidationWorkload = 45 * time.Minute
	// ValidationG15Scale scales the G15 minimums: the 30 min measured window of the 90 min chaos phase.
	ValidationG15Scale = 30.0 / 90.0
)

// Cell is the cell fields of a configuration: what may differ between cells (PLAN §9 step 8).
type Cell struct {
	// ID is the cell id, e.g. c50p5.
	ID string `json:"id"`
	// Version is the Cassandra version.
	Version string `json:"version"`
	// Proto is the protocol version.
	Proto int `json:"proto"`
	// Compression is the compression mode, lz4 framed (v4) or segmented (v5).
	Compression string `json:"compression"`
	// Short is the mandatory M1–M6 set, sorted; the seed orders it.
	Short []chaos.Kind `json:"short"`
	// Long is the L fault.
	Long chaos.Kind `json:"long"`
}

// Driver is the driver configuration of the primary and aux sessions (PLAN §3.4).
type Driver struct {
	NumConns          int           `json:"num_conns"`
	Consistency       string        `json:"consistency"`
	SerialConsistency string        `json:"serial_consistency"`
	Timeout           time.Duration `json:"timeout"`
	ConnectTimeout    time.Duration `json:"connect_timeout"`
	ReconnectInterval time.Duration `json:"reconnect_interval"`
	HostPolicy        string        `json:"host_policy"`
	OpDeadline        time.Duration `json:"op_deadline"`
	ShortDeadlineMin  time.Duration `json:"short_deadline_min"`
	ShortDeadlineMax  time.Duration `json:"short_deadline_max"`
	Retries           int           `json:"retries"`
}

// Cluster is the cluster profile (PLAN §3.2).
type Cluster struct {
	Nodes    int    `json:"nodes"`
	HeapMax  string `json:"heap_max"`
	HeapNew  string `json:"heap_new"`
	IPPrefix string `json:"ip_prefix"`
}

// Dataset is the key space and payload sizes (PLAN §4.1).
type Dataset struct {
	PrimaryPartitions      int `json:"primary_partitions"`
	ClusteringPerPartition int `json:"clustering_per_partition"`
	AuxRanges              int `json:"aux_ranges"`
	AuxPartitions          int `json:"aux_partitions"`
	BlobKeys               int `json:"blob_keys"`
	ScanPartitions         int `json:"scan_partitions"`
	ScanRowsPerPartition   int `json:"scan_rows_per_partition"`
	LWTRows                int `json:"lwt_rows"`
	KVPayloadMin           int `json:"kv_payload_min"`
	KVPayloadMax           int `json:"kv_payload_max"`
	RegisterSample         int `json:"register_sample"`
}

// Timeline is the night timeline (PLAN §8.3).
type Timeline struct {
	Warmup   time.Duration `json:"warmup"`
	Cooldown time.Duration `json:"cooldown"`
	Workload time.Duration `json:"workload"`
}

// Shared is every normalized field that is not a cell field.
type Shared struct {
	// Rate is the offered operations per second; Workers is W.
	Rate    float64 `json:"rate"`
	Workers int     `json:"workers"`
	// Mix is the operation mix, sorted by class.
	Mix workload.Mix `json:"mix"`
	// Driver is the session configuration.
	Driver Driver `json:"driver"`
	// Faults is the fault catalog: every kind's bounds.
	Faults map[chaos.Kind]chaos.Spec `json:"faults"`
	// Timetable is the night timetable.
	Timetable []chaos.Slot `json:"timetable"`
	// Timeline is the night timeline.
	Timeline Timeline `json:"timeline"`
	// Cluster is the cluster profile.
	Cluster Cluster `json:"cluster"`
	// Dataset is the key space.
	Dataset Dataset `json:"dataset"`
	// Workload is the workload constants: batch sizes, scan variants, speculation, aux load shares.
	Workload workload.Params `json:"workload"`
	// Churn is the churn slot's step budgets and whole-slot deadline.
	Churn Churn `json:"churn"`
	// Optional is the kinds optional faults are drawn from in a night.
	Optional []chaos.Kind `json:"optional"`
}

// Churn is a churn slot's budgets (PLAN §5.4).
type Churn struct {
	Deadline time.Duration `json:"deadline"`
	Create   time.Duration `json:"create"`
	Load     time.Duration `json:"load"`
	Close    time.Duration `json:"close"`
	Grace    time.Duration `json:"grace"`
	Residue  time.Duration `json:"residue"`
	Margin   time.Duration `json:"margin"`
}

// Base is the canonical configuration of one night cell.
type Base struct {
	Cell   Cell   `json:"cell"`
	Shared Shared `json:"shared"`
}

// Overrides is what a validation or canary run changes, outside both hashes (PLAN §9 step 8).
type Overrides struct {
	// Mode is night or validate.
	Mode gate.Mode `json:"mode"`
	// Workload, Warmup and Cooldown replace the night timeline; zero keeps it.
	Workload time.Duration `json:"workload,omitempty"`
	Warmup   time.Duration `json:"warmup,omitempty"`
	Cooldown time.Duration `json:"cooldown,omitempty"`
	// Timetable replaces the night timetable; nil keeps it.
	Timetable []chaos.Slot `json:"timetable,omitempty"`
	// Faults replaces the mandatory assignment with fixed faults in slot order; nil keeps it.
	Faults []chaos.Kind `json:"faults,omitempty"`
	// Canary is the canary id, empty for none.
	Canary string `json:"canary,omitempty"`
	// CanaryParams are the canary's parameters, e.g. K16's n.
	CanaryParams map[string]float64 `json:"canary_params,omitempty"`
	// ExpectedVersion replaces the Cassandra version G0 expects (K12); empty keeps Base.Cell.Version,
	// which also names the ccm install directory.
	ExpectedVersion string `json:"expected_version,omitempty"`
	// G15Scale scales the G15 minimums; zero means 1.
	G15Scale float64 `json:"g15_scale,omitempty"`
}

// Config is a run's effective configuration, written to config.json.
type Config struct {
	// Base is the hashed configuration.
	Base Base `json:"base"`
	// Overrides are the applied overrides, stored but not hashed.
	Overrides Overrides `json:"overrides"`
	// Seed seeds the schedule; like the attempt id it is a run parameter, not configuration.
	Seed uint64 `json:"seed"`
	// CellHash and SharedHash are the two configuration identities.
	CellHash   string `json:"cell_hash"`
	SharedHash string `json:"shared_hash"`
}

// Cells returns the four cells of the matrix (PLAN §3.1) with their mandatory assignment (PLAN §5.4, phase 1).
//
// Returns:
//   - []Cell: c41p4, c41p5, c50p4, c50p5
func Cells() []Cell {
	a := chaos.PhaseOneAssignment()
	mk := func(id, version string, proto int) Cell {
		compression := "lz4-frame"
		if proto >= 5 {
			compression = "lz4-segment"
		}
		return Cell{ID: id, Version: version, Proto: proto, Compression: compression, Short: slices.Clone(a.Short), Long: a.Long}
	}
	return []Cell{mk("c41p4", "4.1.12", 4), mk("c41p5", "4.1.12", 5), mk("c50p4", "5.0.3", 4), mk("c50p5", "5.0.3", 5)}
}

// CellByID returns one cell of the matrix.
//
// Parameters:
//   - id: e.g. c50p5
//
// Returns:
//   - Cell: the cell
//   - error: for an unknown id
func CellByID(id string) (Cell, error) {
	for _, c := range Cells() {
		if c.ID == id {
			return c, nil
		}
	}
	return Cell{}, fmt.Errorf("unknown cell %q", id)
}

// NightBase returns the base configuration of a cell, from the harness's constants.
//
// Parameters:
//   - c: the cell
//   - driver: the session configuration, from package cell
//   - rate, workers: the offered rate and W
//
// Returns:
//   - Base: the base, normalized
func NightBase(c Cell, driver Driver, rate float64, workers int) Base {
	return Normalize(Base{
		Cell: c,
		Shared: Shared{
			Rate: rate, Workers: workers, Mix: workload.DefaultMix(), Driver: driver,
			Faults: chaos.Specs(), Timetable: chaos.NightTimetable(),
			Timeline: Timeline{Warmup: NightWarmup, Cooldown: NightCooldown, Workload: NightWorkload},
			Cluster:  Cluster{Nodes: 3, HeapMax: "1G", HeapNew: "256M", IPPrefix: "127.0.1."},
			Dataset: Dataset{
				PrimaryPartitions: workload.PrimaryPartitions, ClusteringPerPartition: workload.ClusteringPerPartition,
				AuxRanges: workload.AuxRanges, AuxPartitions: workload.AuxPartitions, BlobKeys: workload.BlobKeys,
				ScanPartitions: workload.ScanPartitions, ScanRowsPerPartition: workload.ScanRowsPerPartition,
				LWTRows: workload.LWTRows, KVPayloadMin: workload.KVPayloadMin, KVPayloadMax: workload.KVPayloadMax,
				RegisterSample: workload.RegisterSample,
			},
			Workload: workload.DefaultParams(),
			Churn: Churn{Deadline: chaos.ChurnSlotDeadline, Create: chaos.ChurnBudget.Create, Load: chaos.ChurnBudget.Load,
				Close: chaos.ChurnBudget.Close, Grace: chaos.ChurnBudget.Grace, Residue: chaos.ChurnBudget.Residue, Margin: chaos.ChurnBudget.Margin},
			Optional: chaos.PhaseOneOptional(),
		},
	})
}

// ValidationOverrides returns the overrides of a validation run (PLAN §7): its own timeline and timetable,
// F-stop then F-pause as the fixed faults, and the scaled G15 minimums.
//
// A canary's overrides are its id and, for K12, the expected version (PLAN §44.4); every other hook follows from the id.
//
// Parameters:
//   - id: the canary id, empty for the control run
//
// Returns:
//   - Overrides: the overrides
func ValidationOverrides(id string) Overrides {
	o := Overrides{
		Mode: gate.ModeValidate, Workload: ValidationWorkload, Warmup: ValidationWarmup, Cooldown: ValidationCooldown,
		Timetable: chaos.ValidationTimetable(), Faults: []chaos.Kind{chaos.FaultStop, chaos.FaultPause},
		Canary: id, G15Scale: ValidationG15Scale,
	}
	if s, ok := canary.Lookup(id); ok {
		o.ExpectedVersion = s.ExpectedVersion
	}
	return o
}

// Normalize returns a base in canonical form: the mandatory set and the mix sorted, the timetable by start.
// Normalizing twice changes nothing.
//
// Parameters:
//   - b: the base
//
// Returns:
//   - Base: the canonical base
func Normalize(b Base) Base {
	b.Cell.Short = slices.Clone(b.Cell.Short)
	slices.Sort(b.Cell.Short)
	b.Shared.Mix = slices.Clone(b.Shared.Mix)
	slices.SortFunc(b.Shared.Mix, func(x, y workload.Share) int { return cmp.Compare(x.Class, y.Class) })
	b.Shared.Timetable = slices.Clone(b.Shared.Timetable)
	slices.SortFunc(b.Shared.Timetable, func(x, y chaos.Slot) int { return cmp.Compare(x.Start, y.Start) })
	b.Shared.Optional = slices.Clone(b.Shared.Optional)
	slices.Sort(b.Shared.Optional)
	b.Shared.Workload.ScanPageSizes = slices.Clone(b.Shared.Workload.ScanPageSizes)
	slices.Sort(b.Shared.Workload.ScanPageSizes)
	return b
}

// Hashes returns the two configuration identities of a base.
// The cell hash covers every normalized field; the shared hash covers them minus the cell fields.
//
// Parameters:
//   - b: the base; it is normalized first
//
// Returns:
//   - cellHash: sha256 of the canonical base, hex
//   - sharedHash: sha256 of the canonical shared fields, hex
//   - err: when the base cannot be encoded
func Hashes(b Base) (cellHash, sharedHash string, err error) {
	b = Normalize(b)
	cellHash, err = hashJSON(b)
	if err != nil {
		return "", "", err
	}
	sharedHash, err = hashJSON(b.Shared)
	if err != nil {
		return "", "", err
	}
	return cellHash, sharedHash, nil
}

// New builds a run's configuration and its identities.
//
// Parameters:
//   - b: the base
//   - o: the overrides
//   - seed: the schedule seed
//
// Returns:
//   - Config: the configuration with both hashes
//   - error: when the base cannot be encoded
func New(b Base, o Overrides, seed uint64) (Config, error) {
	b = Normalize(b)
	ch, sh, err := Hashes(b)
	if err != nil {
		return Config{}, err
	}
	return Config{Base: b, Overrides: o, Seed: seed, CellHash: ch, SharedHash: sh}, nil
}

// Effective returns the timeline, timetable and assignment a run uses after its overrides.
//
// Returns:
//   - Timeline: the timeline
//   - []chaos.Slot: the timetable
//   - chaos.Assignment: the mandatory assignment
//   - bool: true when the short faults keep their order (fixed validation faults), false when the seed shuffles them
func (c Config) Effective() (Timeline, []chaos.Slot, chaos.Assignment, bool) {
	tl := c.Base.Shared.Timeline
	if c.Overrides.Workload > 0 {
		tl.Workload = c.Overrides.Workload
	}
	if c.Overrides.Warmup > 0 {
		tl.Warmup = c.Overrides.Warmup
	}
	if c.Overrides.Cooldown > 0 {
		tl.Cooldown = c.Overrides.Cooldown
	}
	slots := c.Base.Shared.Timetable
	if c.Overrides.Timetable != nil {
		slots = c.Overrides.Timetable
	}
	if c.Overrides.Faults != nil {
		return tl, slices.Clone(slots), chaos.Assignment{Short: slices.Clone(c.Overrides.Faults)}, true
	}
	return tl, slices.Clone(slots), chaos.Assignment{Short: slices.Clone(c.Base.Cell.Short), Long: c.Base.Cell.Long}, false
}

// ExpectedVersion returns the Cassandra version G0 expects: K12's override, or the cell's.
//
// Returns:
//   - string: the version
func (c Config) ExpectedVersion() string {
	if c.Overrides.ExpectedVersion != "" {
		return c.Overrides.ExpectedVersion
	}
	return c.Base.Cell.Version
}

// G15Scale returns the scale of the G15 minimums.
//
// Returns:
//   - float64: 1 unless overridden
func (c Config) G15Scale() float64 {
	if c.Overrides.G15Scale > 0 {
		return c.Overrides.G15Scale
	}
	return 1
}

func hashJSON(v any) (string, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("config hash: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}
