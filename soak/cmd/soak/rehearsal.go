package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/cellrun"
)

// rehearsalMain judges a rehearsal cell from its artifacts (PLAN §59.7): exit 0 green, 1 not green, 2 unreadable.
// The execution directory is the first argument, as `soak rehearsal <exec dir> -cell <id>`, or -dir.
func rehearsalMain(args []string) int {
	fs := flag.NewFlagSet("rehearsal", flag.ContinueOnError)
	dir := fs.String("dir", "", "the rehearsal's execution directory, also accepted as the first argument")
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		*dir, args = args[0], args[1:]
	}
	cell := fs.String("cell", "", "the rehearsal's cell id, e.g. c41p5")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *dir == "" || *cell == "" {
		fmt.Fprintln(os.Stderr, "rehearsal: -dir and -cell are required")
		return 2
	}
	r, err := cellrun.JudgeRehearsal(*dir, *cell)
	if err != nil {
		fmt.Fprintln(os.Stderr, "rehearsal:", err)
		return 2
	}
	out, _ := json.MarshalIndent(r, "", "  ")
	fmt.Println(string(out))
	if !r.Green {
		return 1
	}
	return 0
}
