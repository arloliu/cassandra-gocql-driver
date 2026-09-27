package view

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// TCPEstablished is the /proc/net/tcp state code of an established connection.
const TCPEstablished = 0x01

// Socket is one row of /proc/net/tcp or tcp6.
type Socket struct {
	// Local and Remote are the endpoints.
	Local, Remote netip.AddrPort
	// State is the kernel TCP state code, e.g. TCPEstablished.
	State uint8
	// Inode is the socket inode.
	Inode uint64
}

// OwnedSocketInodes lists the socket inodes open in a process's fd table.
//
// Parameters:
//   - fdDir: the fd directory, /proc/self/fd in production
//
// Returns:
//   - map[uint64]bool: the socket inodes
//   - error: when the directory cannot be read
func OwnedSocketInodes(fdDir string) (map[uint64]bool, error) {
	ents, err := os.ReadDir(fdDir)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", fdDir, err)
	}
	out := map[uint64]bool{}
	for _, e := range ents {
		link, err := os.Readlink(filepath.Join(fdDir, e.Name()))
		if err != nil {
			// The fd closed between ReadDir and Readlink.
			continue
		}
		num, ok := strings.CutPrefix(link, "socket:[")
		if !ok {
			continue
		}
		if ino, err := strconv.ParseUint(strings.TrimSuffix(num, "]"), 10, 64); err == nil {
			out[ino] = true
		}
	}
	return out, nil
}

// ReadSockets reads tcp and tcp6 from a /proc/net directory.
// The tables are namespace-wide: they include other processes' sockets,
// so callers must filter by OwnedSocketInodes (soak/SPIKES.md, S1).
//
// Parameters:
//   - netDir: /proc/net in production
//
// Returns:
//   - []Socket: every row of both tables
//   - error: when a table exists but cannot be parsed
func ReadSockets(netDir string) ([]Socket, error) {
	var out []Socket
	for _, name := range []string{"tcp", "tcp6"} {
		f, err := os.Open(filepath.Join(netDir, name))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", name, err)
		}
		socks, err := ParseProcNetTCP(f)
		f.Close()
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", name, err)
		}
		out = append(out, socks...)
	}
	return out, nil
}

// ParseProcNetTCP parses one /proc/net/tcp-format table.
//
// Parameters:
//   - r: the table, header line included
//
// Returns:
//   - []Socket: one per row
//   - error: on a malformed row
func ParseProcNetTCP(r io.Reader) ([]Socket, error) {
	sc := bufio.NewScanner(r)
	sc.Scan() // header
	var out []Socket
	for sc.Scan() {
		fs := strings.Fields(sc.Text())
		if len(fs) < 10 {
			continue
		}
		local, err := parseHexAddrPort(fs[1])
		if err != nil {
			return nil, err
		}
		remote, err := parseHexAddrPort(fs[2])
		if err != nil {
			return nil, err
		}
		state, err := strconv.ParseUint(fs[3], 16, 8)
		if err != nil {
			return nil, fmt.Errorf("state %q: %w", fs[3], err)
		}
		ino, err := strconv.ParseUint(fs[9], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("inode %q: %w", fs[9], err)
		}
		out = append(out, Socket{Local: local, Remote: remote, State: uint8(state), Inode: ino})
	}
	return out, sc.Err()
}

// parseHexAddrPort parses "0101007F:4A62": an address in host byte order per 32-bit word, then a hex port.
func parseHexAddrPort(s string) (netip.AddrPort, error) {
	addrHex, portHex, ok := strings.Cut(s, ":")
	if !ok {
		return netip.AddrPort{}, fmt.Errorf("address %q: no port", s)
	}
	b, err := hex.DecodeString(addrHex)
	if err != nil || (len(b) != 4 && len(b) != 16) {
		return netip.AddrPort{}, fmt.Errorf("address %q: bad hex", s)
	}
	port, err := strconv.ParseUint(portHex, 16, 16)
	if err != nil {
		return netip.AddrPort{}, fmt.Errorf("address %q: %w", s, err)
	}
	// Each 32-bit word is little-endian on the hosts this runs on.
	for i := 0; i < len(b); i += 4 {
		b[i], b[i+1], b[i+2], b[i+3] = b[i+3], b[i+2], b[i+1], b[i]
	}
	addr, _ := netip.AddrFromSlice(b)
	return netip.AddrPortFrom(addr.Unmap(), uint16(port)), nil
}
