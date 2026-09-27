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
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cellrun"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/gate"
)

// cellMain runs one cell (PLAN §8.3) and exits 0 on pass, 1 on any other verdict, 2 when it could not run.
func cellMain(ctx context.Context, args []string) int {
	home, _ := os.UserHomeDir()
	fs := flag.NewFlagSet("cell", flag.ExitOnError)
	var o cellrun.Options
	var mode string
	var seed uint64
	fs.StringVar(&o.CellID, "cell", "c50p5", "cell id: c41p4, c41p5, c50p4 or c50p5")
	fs.StringVar(&mode, "mode", string(gate.ModeNight), "night (2 h) or validate (48 min, PLAN §7)")
	fs.StringVar(&o.RunKind, "kind", "", "execution directory kind: night, calib or control (default from mode)")
	fs.Uint64Var(&seed, "seed", 0, "schedule and load seed; 0 draws one")
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
		return 2
	}
	o.Mode = gate.Mode(mode)
	switch {
	case o.Mode == gate.ModeNight && o.RunKind == "":
		o.RunKind = artifact.RunNight
	case o.Mode == gate.ModeValidate && o.RunKind == "":
		o.RunKind = artifact.RunControl
	case o.Mode != gate.ModeNight && o.Mode != gate.ModeValidate:
		fmt.Fprintf(os.Stderr, "unknown mode %q\n", mode)
		return 2
	}
	if seed == 0 {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			fmt.Fprintln(os.Stderr, "seed:", err)
			return 2
		}
		seed = binary.LittleEndian.Uint64(b[:]) | 1
	}
	o.Seed = seed
	o.Logf = func(format string, args ...any) {
		fmt.Printf("%s "+format+"\n", append([]any{time.Now().Format("15:04:05")}, args...)...)
	}
	o.Logf("cell %s mode %s kind %s seed %d", o.CellID, o.Mode, o.RunKind, o.Seed)
	dir, v, err := cellrun.Run(ctx, o)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cell:", err)
		return 2
	}
	o.Logf("artifacts in %s", dir)
	if v.Status != gate.VerdictPass {
		return 1
	}
	return 0
}
