package proxy

import (
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
)

// ErrNoRow is returned when nodetool clientstats lists no connection with the wanted driver name.
var ErrNoRow = errors.New("proxy: no clientstats row for the driver name")

// ClientRow is one connection of `nodetool clientstats --all`.
type ClientRow struct {
	// Address is the connection's remote endpoint as Cassandra sees it: toxiproxy's end of the upstream link.
	Address netip.AddrPort
	// Requests is Cassandra's dispatch count for the connection, for diagnosis only (PLAN §59.3).
	Requests int64
	// DriverName is the DRIVER_NAME the client sent in STARTUP.
	DriverName string
}

// SocketInfo is one established socket of `ss -tinH`, with the TCP_INFO counters the effect checks read.
type SocketInfo struct {
	// Local and Remote are the socket's endpoints.
	Local, Remote netip.AddrPort
	// RecvQ is the bytes received by the kernel and not yet read by the owning process.
	RecvQ int64
	// BytesReceived is tcpi_bytes_received; ss omits a zero counter, which reads as 0.
	BytesReceived int64
	// BytesAcked is tcpi_bytes_acked; ss omits a zero counter, which reads as 0.
	BytesAcked int64
}

// ParseClientStats finds the row of one driver name in `nodetool clientstats --all` output.
//
// Parameters:
//   - out: the command's output
//   - driverName: the DRIVER_NAME to find
//
// Returns:
//   - ClientRow: the row
//   - error: ErrNoRow when no row has the name; an error for a duplicate row or any error of ClientStatsRows
func ParseClientStats(out, driverName string) (ClientRow, error) {
	rows, err := ClientStatsRows(out)
	if err != nil {
		return ClientRow{}, err
	}
	var found []ClientRow
	for _, r := range rows {
		if r.DriverName == driverName {
			found = append(found, r)
		}
	}
	switch len(found) {
	case 0:
		return ClientRow{}, fmt.Errorf("%w %q", ErrNoRow, driverName)
	case 1:
		return found[0], nil
	default:
		return ClientRow{}, fmt.Errorf("clientstats: %d rows for %q", len(found), driverName)
	}
}

// ClientStatsRows parses every connection row of `nodetool clientstats --all` output.
//
// The columns are located by the header's offsets, since nodetool pads them left-aligned and an empty Keyspace
// leaves a gap that splitting on spaces would misread.
// The Address is Java's InetSocketAddress.toString(), e.g. "/127.0.0.1:50808" or "host/127.0.0.1:50808";
// what follows the last '/' must be an IPv4 literal and a port.
//
// Parameters:
//   - out: the command's output
//
// Returns:
//   - []ClientRow: the rows, in order
//   - error: for a missing header or column, or a row whose Address or Requests does not parse
func ClientStatsRows(out string) ([]ClientRow, error) {
	lines := strings.Split(out, "\n")
	head := -1
	for i, l := range lines {
		if strings.HasPrefix(l, "Address") && strings.Contains(l, "Driver-Name") {
			head = i
			break
		}
	}
	if head < 0 {
		return nil, errors.New("clientstats: no header line")
	}
	cols := columnOffsets(lines[head])
	for _, name := range []string{"Address", "Requests", "Driver-Name"} {
		if _, ok := cols[name]; !ok {
			return nil, fmt.Errorf("clientstats: no %s column", name)
		}
	}
	var rows []ClientRow
	for _, l := range lines[head+1:] {
		if strings.TrimSpace(l) == "" {
			break
		}
		row, err := clientRow(l, cols)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// ParseSS parses `ss -tinH state established ...` output: each socket's first line, then its indented info line.
//
// Parameters:
//   - out: the command's output
//
// Returns:
//   - []SocketInfo: the sockets, in order
//   - error: for a socket line that does not parse or a socket without an info line
func ParseSS(out string) ([]SocketInfo, error) {
	var socks []SocketInfo
	open := false
	for _, l := range strings.Split(out, "\n") {
		if strings.TrimSpace(l) == "" {
			continue
		}
		if l[0] != '\t' && l[0] != ' ' {
			if open {
				return nil, fmt.Errorf("ss: socket %s has no info line", socks[len(socks)-1].Local)
			}
			s, err := socketLine(l)
			if err != nil {
				return nil, err
			}
			socks = append(socks, s)
			open = true
			continue
		}
		if !open {
			return nil, fmt.Errorf("ss: info line without a socket: %q", strings.TrimSpace(l))
		}
		if err := infoLine(l, &socks[len(socks)-1]); err != nil {
			return nil, err
		}
		open = false
	}
	if open {
		return nil, fmt.Errorf("ss: socket %s has no info line", socks[len(socks)-1].Local)
	}
	return socks, nil
}

// columnOffsets maps each header word to its start offset.
func columnOffsets(header string) map[string][2]int {
	cols := map[string][2]int{}
	var names []string
	var starts []int
	for i := 0; i < len(header); {
		if header[i] == ' ' {
			i++
			continue
		}
		j := i
		for j < len(header) && header[j] != ' ' {
			j++
		}
		names = append(names, header[i:j])
		starts = append(starts, i)
		i = j
	}
	for k, n := range names {
		end := -1
		if k+1 < len(starts) {
			end = starts[k+1]
		}
		cols[n] = [2]int{starts[k], end}
	}
	return cols
}

// cell returns a row's trimmed text between a column's offsets.
func cell(row string, span [2]int) string {
	if span[0] >= len(row) {
		return ""
	}
	end := span[1]
	if end < 0 || end > len(row) {
		end = len(row)
	}
	return strings.TrimSpace(row[span[0]:end])
}

func clientRow(l string, cols map[string][2]int) (ClientRow, error) {
	raw := cell(l, cols["Address"])
	addr, err := javaAddress(raw)
	if err != nil {
		return ClientRow{}, err
	}
	req, err := strconv.ParseInt(cell(l, cols["Requests"]), 10, 64)
	if err != nil {
		return ClientRow{}, fmt.Errorf("clientstats: Requests of %s: %w", raw, err)
	}
	return ClientRow{Address: addr, Requests: req, DriverName: cell(l, cols["Driver-Name"])}, nil
}

// javaAddress parses InetSocketAddress.toString() for an IPv4 literal: "/127.0.0.1:50808" or "host/127.0.0.1:50808".
func javaAddress(raw string) (netip.AddrPort, error) {
	s := raw
	if i := strings.LastIndexByte(s, '/'); i >= 0 {
		s = s[i+1:]
	}
	ap, err := netip.ParseAddrPort(s)
	if err != nil || !ap.Addr().Is4() {
		return netip.AddrPort{}, fmt.Errorf("clientstats: address %q is not an IPv4 literal and port", raw)
	}
	return ap, nil
}

func socketLine(l string) (SocketInfo, error) {
	f := strings.Fields(l)
	if len(f) != 4 {
		return SocketInfo{}, fmt.Errorf("ss: socket line %q has %d fields, want Recv-Q Send-Q Local Peer", l, len(f))
	}
	recvQ, err := strconv.ParseInt(f[0], 10, 64)
	if err != nil {
		return SocketInfo{}, fmt.Errorf("ss: Recv-Q of %q: %w", l, err)
	}
	local, err := netip.ParseAddrPort(f[2])
	if err != nil {
		return SocketInfo{}, fmt.Errorf("ss: local address of %q: %w", l, err)
	}
	remote, err := netip.ParseAddrPort(f[3])
	if err != nil {
		return SocketInfo{}, fmt.Errorf("ss: peer address of %q: %w", l, err)
	}
	return SocketInfo{Local: local, Remote: remote, RecvQ: recvQ}, nil
}

func infoLine(l string, s *SocketInfo) error {
	for _, tok := range strings.Fields(l) {
		key, val, ok := strings.Cut(tok, ":")
		if !ok {
			continue
		}
		var dst *int64
		switch key {
		case "bytes_received":
			dst = &s.BytesReceived
		case "bytes_acked":
			dst = &s.BytesAcked
		default:
			continue
		}
		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return fmt.Errorf("ss: %s of %s: %w", key, s.Local, err)
		}
		*dst = n
	}
	return nil
}
