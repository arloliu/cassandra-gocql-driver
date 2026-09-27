package cell

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/view"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/workload"
)

func TestHeapFlagProblem(t *testing.T) {
	nul := func(args ...string) []byte {
		var b []byte
		for _, a := range args {
			b = append(append(b, a...), 0)
		}
		return b
	}
	require.Empty(t, HeapFlagProblem(nul("java", "-Xms1G", "-Xmx1G", "-cp", "x"), "1G"))
	require.Equal(t, "-Xmx500M", HeapFlagProblem(nul("java", "-Xms1G", "-Xmx500M"), "1G"))
	require.Contains(t, HeapFlagProblem(nul("java", "-Xmx1G"), "1G"), "0 -Xms")
	require.Contains(t, HeapFlagProblem(nul("java", "-Xmx1G", "-Xms1G", "-Xmx1G"), "1G"), "2 -Xmx")
	require.Equal(t, "-Xmx1g", HeapFlagProblem(nul("java", "-Xms1G", "-Xmx1g"), "1G"), "the flag is compared exactly")
}

func TestConfigureInstallsConnectObserver(t *testing.T) {
	var got []ConnectEvent
	cfg, _ := Configure(SessionSpec{ID: "primary", Registry: view.NewRegistry(), DriverLog: io.Discard,
		Settlement: workload.NewSettlement(), ConnectEvents: func(e ConnectEvent) { got = append(got, e) }})
	require.NotNil(t, cfg.ConnectObserver)
	cfg.ConnectObserver.ObserveConnect(gocql.ObservedConnect{Err: errors.New("refused")})
	require.Equal(t, []ConnectEvent{{Session: "primary", Err: "refused"}}, got)

	cfg, _ = Configure(SessionSpec{ID: "aux0", Registry: view.NewRegistry(), DriverLog: io.Discard, Settlement: workload.NewSettlement()})
	require.Nil(t, cfg.ConnectObserver)
}
