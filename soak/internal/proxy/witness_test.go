package proxy

import (
	"net/netip"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var header = []string{"Address", "SSL", "Cipher", "Protocol", "Version", "User", "Keyspace", "Requests", "Driver-Name", "Driver-Version"}

func TestClientStatsRowsCaptured(t *testing.T) {
	raw, err := os.ReadFile("testdata/clientstats-4.1.12.txt")
	require.NoError(t, err)
	rows, err := ClientStatsRows(string(raw))
	require.NoError(t, err)
	require.Equal(t, []ClientRow{
		{Address: netip.MustParseAddrPort("127.0.0.1:60956"), Requests: 3712921, DriverName: ".."},
		{Address: netip.MustParseAddrPort("127.0.0.1:60942"), Requests: 1501, DriverName: ".."},
		{Address: netip.MustParseAddrPort("127.0.0.1:60948"), Requests: 3709858, DriverName: ".."},
	}, rows, "an empty Keyspace does not shift the columns")
	_, err = ParseClientStats(string(raw), "..")
	require.Error(t, err, "three rows share the driver's name")

	raw, err = os.ReadFile("testdata/clientstats-5.0.3.txt")
	require.NoError(t, err)
	rows, err = ClientStatsRows(string(raw))
	require.NoError(t, err)
	require.Equal(t, []ClientRow{
		{Address: netip.MustParseAddrPort("127.0.0.1:59696"), Requests: 1139037, DriverName: ".."},
		{Address: netip.MustParseAddrPort("127.0.0.1:59702"), Requests: 1139145, DriverName: ".."},
	}, rows, "5.0.3's table has the same columns")
}

func TestParseClientStats(t *testing.T) {
	out := table(header,
		[]string{"/127.0.0.1:41000", "false", "undefined", "undefined", "4", "anonymous", "", "7", "soak-probe-t1-a", "0"},
		[]string{"/127.0.0.1:41002", "false", "undefined", "undefined", "4", "anonymous", "soak", "12", "soak-probe-t2-a", "0"})
	row, err := ParseClientStats(out, "soak-probe-t1-a")
	require.NoError(t, err)
	require.Equal(t, ClientRow{Address: netip.MustParseAddrPort("127.0.0.1:41000"), Requests: 7, DriverName: "soak-probe-t1-a"}, row)

	row, err = ParseClientStats(out, "soak-probe-t2-a")
	require.NoError(t, err)
	require.Equal(t, int64(12), row.Requests)
	require.Equal(t, netip.MustParseAddrPort("127.0.0.1:41002"), row.Address)

	_, err = ParseClientStats(out, "soak-probe-t9-a")
	require.ErrorIs(t, err, ErrNoRow)
}

func TestParseClientStatsColumnOrder(t *testing.T) {
	// The columns are found by the header, not by position (PLAN §59.8).
	out := table([]string{"Address", "Driver-Name", "Requests", "User"}, []string{"/127.0.0.1:41000", "soak-probe-t1-b", "3", "anonymous"})
	row, err := ParseClientStats(out, "soak-probe-t1-b")
	require.NoError(t, err)
	require.Equal(t, int64(3), row.Requests)
}

func TestParseClientStatsRefusals(t *testing.T) {
	h := []string{"Address", "SSL", "Requests", "Driver-Name"}
	for name, out := range map[string]string{
		"no header":      "Total connected clients: 0\n",
		"duplicate row":  table(h, []string{"/127.0.0.1:1", "false", "1", "soak-probe-t1-a"}, []string{"/127.0.0.1:2", "false", "1", "soak-probe-t1-a"}),
		"IPv6 address":   table(h, []string{"/[::1]:41000", "false", "1", "soak-probe-t1-a"}),
		"bad requests":   table(h, []string{"/127.0.0.1:1", "false", "x", "soak-probe-t1-a"}),
		"no port":        table(h, []string{"/127.0.0.1", "false", "1", "soak-probe-t1-a"}),
		"missing column": table([]string{"Address", "SSL", "Driver-Name"}, []string{"/127.0.0.1:1", "false", "soak-probe-t1-a"}),
	} {
		_, err := ParseClientStats(out, "soak-probe-t1-a")
		require.Error(t, err, name)
		require.NotErrorIs(t, err, ErrNoRow, name)
	}
}

func TestParseClientStatsHostPrefix(t *testing.T) {
	out := table([]string{"Address", "Requests", "Driver-Name"}, []string{"localhost/127.0.0.1:41000", "5", "soak-probe-t1-a"})
	row, err := ParseClientStats(out, "soak-probe-t1-a")
	require.NoError(t, err)
	require.Equal(t, netip.MustParseAddrPort("127.0.0.1:41000"), row.Address)
}

func TestParseSSCaptured(t *testing.T) {
	raw, err := os.ReadFile("testdata/ss-proxy-client.txt")
	require.NoError(t, err)
	socks, err := ParseSS(string(raw))
	require.NoError(t, err)
	require.Len(t, socks, 2)
	require.Equal(t, netip.MustParseAddrPort("127.0.1.1:19042"), socks[0].Local)
	require.Equal(t, netip.MustParseAddrPort("127.0.0.1:49974"), socks[0].Remote)
	require.Positive(t, socks[0].BytesAcked)
	require.Positive(t, socks[0].BytesReceived)

	raw, err = os.ReadFile("testdata/ss-toxiproxy-server.txt")
	require.NoError(t, err)
	socks, err = ParseSS(string(raw))
	require.NoError(t, err)
	require.Len(t, socks, 2)
	require.Equal(t, uint16(9042), socks[0].Remote.Port())
}

func TestParseSSCounters(t *testing.T) {
	out := "17     0      127.0.1.2:19042 127.0.0.1:41000\n" +
		"\t cubic wscale:10,10 rto:201 bytes_sent:40 bytes_acked:40 segs_out:3\n"
	socks, err := ParseSS(out)
	require.NoError(t, err)
	require.Equal(t, []SocketInfo{{Local: netip.MustParseAddrPort("127.0.1.2:19042"), Remote: netip.MustParseAddrPort("127.0.0.1:41000"),
		RecvQ: 17, BytesAcked: 40}}, socks, "an omitted bytes_received is zero")

	socks, err = ParseSS("")
	require.NoError(t, err)
	require.Empty(t, socks)

	_, err = ParseSS("0 0 127.0.1.2:9042 127.0.0.1:1\n")
	require.Error(t, err, "a socket without its info line")
	_, err = ParseSS("0 0 127.0.1.2:9042\n\t cubic\n")
	require.Error(t, err, "a short socket line")
	_, err = ParseSS("\t cubic bytes_acked:1\n")
	require.Error(t, err, "an info line without a socket")
	_, err = ParseSS("0 0 127.0.1.2:9042 127.0.0.1:1\n\t bytes_acked:x\n")
	require.Error(t, err, "a counter that does not parse")
}

// table formats rows as nodetool's TableBuilder does: each column left-aligned and padded to its widest cell plus one space.
func table(rows ...[]string) string {
	widths := make([]int, len(rows[0]))
	for _, r := range rows {
		for i, c := range r {
			widths[i] = max(widths[i], len(c))
		}
	}
	var b strings.Builder
	for _, r := range rows {
		for i, c := range r {
			b.WriteString(c + strings.Repeat(" ", widths[i]-len(c)+1))
		}
		b.WriteString("\n")
	}
	return b.String() + "\nTotal connected clients: 2\n"
}
