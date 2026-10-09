package cell

import (
	"context"
	"net/netip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/chaos"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/toxic"
	"github.com/apache/cassandra-gocql-driver/v2/soak/internal/view"
)

func TestKernelSocketsSocket(t *testing.T) {
	local, remote := netip.MustParseAddrPort("127.0.1.2:9042"), netip.MustParseAddrPort("127.0.0.1:50808")
	one := "0 0 127.0.1.2:9042 127.0.0.1:50808\n\t cubic bytes_acked:10 bytes_received:20\n"
	k := &KernelSockets{SS: fakeSS(t, one)}
	s, err := k.Socket(t.Context(), local, remote)
	require.NoError(t, err)
	require.Equal(t, int64(20), s.BytesReceived)
	require.Equal(t, int64(10), s.BytesAcked)
	args, err := os.ReadFile(filepath.Join(filepath.Dir(k.SS), "args"))
	require.NoError(t, err)
	require.Equal(t, "-tinH state established src 127.0.1.2:9042 dst 127.0.0.1:50808\n", string(args))

	for name, out := range map[string]string{"none": "", "two": one + one, "another socket": "0 0 127.0.1.2:9042 127.0.0.1:1\n\t cubic\n"} {
		k := &KernelSockets{SS: fakeSS(t, out)}
		_, err := k.Socket(t.Context(), local, remote)
		require.ErrorContains(t, err, "matching sockets", name)
	}
}

func TestKernelSocketsUpstream(t *testing.T) {
	var got []string
	k := &KernelSockets{Node: "node2", Nodetool: func(_ context.Context, node string, args ...string) (string, error) {
		got = append([]string{node}, args...)
		return "Address          Requests Driver-Name     \n/127.0.0.1:50808 4        soak-probe-t1-a \n", nil
	}}
	row, err := k.Upstream(t.Context(), "soak-probe-t1-a")
	require.NoError(t, err)
	require.Equal(t, netip.MustParseAddrPort("127.0.0.1:50808"), row.Address)
	require.Equal(t, []string{"node2", "--", "clientstats", "--all"}, got)
}

func TestBuilderToxic(t *testing.T) {
	b := &Builder{Registry: view.NewRegistry()}
	for _, k := range []chaos.Kind{chaos.FaultLat, chaos.FaultBW, chaos.FaultBlack, chaos.FaultHalfOpen, chaos.FaultReset} {
		require.True(t, Buildable(k), "%s", k)
		f, err := b.Build(chaos.Planned{Kind: k, Targets: []string{"node2"}, Param: 200})
		require.NoError(t, err, "%s", k)
		require.IsType(t, &toxic.Fault{}, f)
	}
	require.False(t, Buildable(chaos.FaultRoll), "F-roll is built as its F-stop sub-faults")
	_, err := b.Build(chaos.Planned{Kind: chaos.FaultRoll})
	require.ErrorContains(t, err, "not implemented")
	require.True(t, Buildable(chaos.FaultTopo))
	_, err = b.Build(chaos.Planned{Kind: chaos.FaultTopo})
	require.ErrorContains(t, err, "datacenter", "F-topo needs the datacenter its node joins")
	b.DC = "datacenter1"
	f, err := b.Build(chaos.Planned{Kind: chaos.FaultTopo})
	require.NoError(t, err)
	require.IsType(t, &chaos.TopoFault{}, f)
	_, err = b.Build(chaos.Planned{Kind: chaos.FaultLat, Targets: []string{"proxy2"}, Param: 200})
	require.ErrorContains(t, err, "not a node name")
	_, err = b.Build(chaos.Planned{Kind: chaos.FaultLat, Targets: []string{"node2"}})
	require.ErrorContains(t, err, "needs a parameter")
}

// fakeSS writes a script that prints out, standing in for ss.
func fakeSS(t *testing.T, out string) string {
	t.Helper()
	dir := t.TempDir()
	data := filepath.Join(dir, "out")
	require.NoError(t, os.WriteFile(data, []byte(out), 0o644))
	path := filepath.Join(dir, "ss")
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\necho \"$@\" > "+dir+"/args\ncat "+data+"\n"), 0o755))
	return path
}
