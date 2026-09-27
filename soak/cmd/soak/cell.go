package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/artifact"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/canary"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cellrun"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
)

// cellMain runs one cell (PLAN §8.3). It exits 0 on pass, 1 on any other verdict, 2 when it could not run;
// in validation mode, 0 on validated and 1 on not-validated (PLAN §44.3).
func cellMain(ctx context.Context, args []string) int {
	o, err := parseCellArgs(args, flag.ExitOnError)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cell:", err)
		return 2
	}
	if o.Seed == 0 {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			fmt.Fprintln(os.Stderr, "seed:", err)
			return 2
		}
		o.Seed = binary.LittleEndian.Uint64(b[:]) | 1
	}
	o.Logf = func(format string, args ...any) {
		fmt.Printf("%s "+format+"\n", append([]any{time.Now().Format("15:04:05")}, args...)...)
	}
	o.Logf("cell %s mode %s kind %s canary %q seed %d", o.CellID, o.Mode, o.RunKind, o.Canary, o.Seed)
	dir, v, err := cellrun.Run(ctx, o)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cell:", err)
		return 2
	}
	o.Logf("artifacts in %s", dir)
	return cellExit(o.Mode, v)
}

// parseCellArgs parses the cell flags and checks the mode, the canary and the run kind (PLAN §44.5):
// a canary runs only in validation mode, only when implemented, and its run kind is its lowercase id.
//
// Parameters:
//   - args: the flags
//   - handling: flag.ExitOnError in production
//
// Returns:
//   - cellrun.Options: the run; Seed is 0 when it is to be drawn
//   - error: a refused combination
func parseCellArgs(args []string, handling flag.ErrorHandling) (cellrun.Options, error) {
	home, _ := os.UserHomeDir()
	fs := flag.NewFlagSet("cell", handling)
	var o cellrun.Options
	var mode string
	fs.StringVar(&o.CellID, "cell", "c50p5", "cell id: c41p4, c41p5, c50p4 or c50p5")
	fs.StringVar(&mode, "mode", string(gate.ModeNight), "night (2 h) or validate (48 min, PLAN §7)")
	fs.StringVar(&o.RunKind, "kind", "", "execution directory kind: night or calib; control or the canary's lowercase id in validate mode (default from mode and canary)")
	fs.StringVar(&o.Canary, "canary", "", "validate mode: the canary id, e.g. K7 (PLAN §44.2); empty for a control run")
	fs.Uint64Var(&o.Seed, "seed", 0, "schedule and load seed; 0 draws one")
	fs.Float64Var(&o.Rate, "rate", 1500, "offered operations per second")
	fs.IntVar(&o.Workers, "workers", 32, "workers, W")
	fs.StringVar(&o.OutRoot, "out", filepath.Join("..", "tmp"), "directory holding soak-<date>/")
	fs.StringVar(&o.Date, "date", time.Now().Format("2006-01-02"), "night date (local) for the execution directory")
	fs.StringVar(&o.AttemptID, "attempt", "", "attempt id, YYYYMMDDTHHMMSSZ-xxxx (default: drawn); set by run-night.sh")
	fs.StringVar(&o.LaunchToken, "launch-token", "", "recorded in resources.json; set by run-night.sh")
	fs.StringVar(&o.GatesPath, "gates", "gates.json", "frozen thresholds; missing makes every calibrated gate invalid-config")
	fs.BoolVar(&o.KeepFailed, "keep-failed", false, "keep the cluster of a cell that did not pass")
	fs.StringVar(&o.Repository, "repository", filepath.Join(home, ".ccm", "repository"), "unpacked Cassandra versions")
	fs.StringVar(&o.CCMBin, "ccm", "ccm", "ccm executable")
	fs.StringVar(&o.CCMConfig, "ccm-config", filepath.Join(home, ".ccm-soak"), "private CCM_CONFIG_DIR")
	fs.StringVar(&o.JavaHome, "java-home", "/usr/lib/jvm/java-11-openjdk-amd64", "JAVA_HOME for Cassandra")
	fs.StringVar(&o.Toxiproxy, "toxiproxy", "toxiproxy-server", "toxiproxy-server executable")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	o.Mode = gate.Mode(mode)
	switch o.Mode {
	case gate.ModeNight:
		if o.Canary != "" {
			return o, fmt.Errorf("-canary %s needs -mode validate", o.Canary)
		}
		if o.RunKind == "" {
			o.RunKind = artifact.RunNight
		}
	case gate.ModeValidate:
		if o.Canary != "" {
			if s, ok := canary.Lookup(o.Canary); !ok || !s.Implemented {
				return o, fmt.Errorf("-canary %s is not an implemented phase-1 canary", o.Canary)
			}
		}
		want := canary.Kind(o.Canary)
		if o.RunKind != "" && o.RunKind != want {
			return o, fmt.Errorf("-kind %s disagrees with the run kind %s of canary %q", o.RunKind, want, o.Canary)
		}
		o.RunKind = want
	default:
		return o, fmt.Errorf("unknown mode %q", mode)
	}
	return o, nil
}

// cellExit maps a final verdict.json to the exit status: night mode 0 only on pass;
// validation mode 0 on validated, 1 on not-validated, 2 without a judgment.
func cellExit(mode gate.Mode, v cellrun.VerdictFile) int {
	if mode == gate.ModeValidate {
		switch {
		case v.Validation == nil:
			return 2
		case v.Validation.Result == canary.Validated:
			return 0
		default:
			return 1
		}
	}
	if v.Status != gate.VerdictPass {
		return 1
	}
	return 0
}
