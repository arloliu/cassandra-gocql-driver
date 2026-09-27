// Command soak runs the gocql overnight soak harness (tmp/soak-review-2026-09-24/PLAN.md).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: soak cell|smoke|derive [flags]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch os.Args[1] {
	case "cell":
		os.Exit(cellMain(ctx, os.Args[2:]))
	case "smoke":
		os.Exit(smokeMain(ctx, os.Args[2:]))
	case "derive":
		os.Exit(deriveMain(os.Args[2:]))
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q\n", os.Args[1])
		os.Exit(2)
	}
}

func smokeMain(ctx context.Context, args []string) int {
	home, _ := os.UserHomeDir()
	fs := flag.NewFlagSet("smoke", flag.ExitOnError)
	var cfg smokeConfig
	fs.StringVar(&cfg.version, "version", "5.0.3", "Cassandra version under the ccm repository")
	fs.IntVar(&cfg.proto, "proto", 5, "protocol version")
	fs.DurationVar(&cfg.duration, "duration", time.Minute, "load duration")
	fs.Float64Var(&cfg.rate, "rate", 1500, "offered operations per second")
	fs.IntVar(&cfg.workers, "workers", 32, "workers")
	fs.BoolVar(&cfg.keep, "keep", false, "keep the cluster after the run")
	fs.StringVar(&cfg.faults, "faults", "", "comma-separated fault kinds to run in turn, e.g. F-ddl,F-kill,F-pause")
	fs.DurationVar(&cfg.faultActive, "fault-active", 10*time.Second, "active duration of each smoke fault")
	fs.DurationVar(&cfg.removeBound, "remove-bound", 0, "override the remove bound of node-stop faults, for measurement")
	fs.BoolVar(&cfg.keepGoing, "keep-going", false, "run the next fault after a failed one")
	fs.DurationVar(&cfg.g11Warmup, "g11-warmup", 0, "fault-free runs only: evaluate G11 after this warm-up and write progress.json (0: off)")
	fs.StringVar(&cfg.outDir, "out", filepath.Join(os.TempDir(), "soak-smoke"), "artifact directory")
	fs.StringVar(&cfg.repoDir, "repository", filepath.Join(home, ".ccm", "repository"), "unpacked Cassandra versions")
	fs.StringVar(&cfg.ccmBin, "ccm", "ccm", "ccm executable")
	fs.StringVar(&cfg.toxiproxy, "toxiproxy", "toxiproxy-server", "toxiproxy-server executable")
	fs.StringVar(&cfg.javaHome, "java-home", "/usr/lib/jvm/java-11-openjdk-amd64", "JAVA_HOME for Cassandra")
	fs.StringVar(&cfg.ccmConfig, "ccm-config", filepath.Join(home, ".ccm-soak"), "private CCM_CONFIG_DIR")
	fs.StringVar(&cfg.clusterName, "cluster", "gocql_soak_smoke", "cluster name")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if err := runSmoke(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "smoke:", err)
		return 1
	}
	return 0
}
