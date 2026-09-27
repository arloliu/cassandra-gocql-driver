// Package proxy runs toxiproxy between the driver and the nodes (PLAN §3.3, D3).
//
// The server runs in its own process group and is stopped with it.
// The client speaks the toxiproxy HTTP API directly, with a context on every call,
// because the upstream Go client has no way to bound a hung request.
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"syscall"
	"time"
)

// NodePort is the Cassandra native port, ProxyPort the port each node's proxy listens on.
const (
	// NodePort is the native protocol port on each node.
	NodePort = 9042
	// ProxyPort is the port each node's proxy listens on, on the node's own address.
	ProxyPort = 19042
)

const (
	// readyTimeout bounds how long StartServer waits for the API to answer.
	readyTimeout = 10 * time.Second
	// stopGrace is how long Stop waits after SIGTERM before SIGKILL.
	stopGrace = 5 * time.Second
	// requestTimeout bounds one API request when the caller's context has no earlier deadline.
	requestTimeout = 10 * time.Second
)

// ErrAPIPortBusy is returned when something already answers on the API port: a foreign toxiproxy-server.
var ErrAPIPortBusy = errors.New("proxy: toxiproxy API port already in use")

// Spec is one proxy.
type Spec struct {
	// Name is the proxy name, nodeN.
	Name string `json:"name"`
	// Listen is the address the proxy listens on.
	Listen string `json:"listen"`
	// Upstream is the node address the proxy forwards to.
	Upstream string `json:"upstream"`
}

// Toxic is one toxic on a proxy.
type Toxic struct {
	// Name identifies the toxic on its proxy.
	Name string `json:"name"`
	// Type is the toxic type, e.g. latency, bandwidth, timeout, reset_peer.
	Type string `json:"type"`
	// Stream is upstream or downstream.
	Stream string `json:"stream"`
	// Toxicity is the probability the toxic applies to a connection, 1 for always.
	Toxicity float64 `json:"toxicity"`
	// Attributes are the type's parameters.
	Attributes map[string]any `json:"attributes"`
}

// ServerConfig describes the toxiproxy server to start.
type ServerConfig struct {
	// Binary is the toxiproxy-server executable.
	Binary string
	// Host and Port are the API address.
	Host string
	Port int
	// LogPath receives the server's stdout and stderr.
	LogPath string
	// OnStart is called with the server's pid as soon as the process exists, before its API answers,
	// so a resource manifest never misses it; may be nil.
	OnStart func(pid int)
}

// Server is a running toxiproxy-server.
type Server struct {
	cmd    *exec.Cmd
	log    *os.File
	client *Client
	done   chan struct{}
}

// Client is a toxiproxy HTTP API client.
type Client struct {
	base string
	http *http.Client
}

// NewClient returns a client for the API at addr.
//
// Parameters:
//   - addr: host:port of the API
//
// Returns:
//   - *Client: the client
func NewClient(addr string) *Client {
	return &Client{base: "http://" + addr, http: &http.Client{Timeout: requestTimeout}}
}

// StartServer starts toxiproxy-server in its own process group and waits for its API.
//
// Parameters:
//   - ctx: bounds the start
//   - cfg: the server to start
//
// Returns:
//   - *Server: the running server
//   - error: ErrAPIPortBusy when the port already answers, or a start or readiness failure
func StartServer(ctx context.Context, cfg ServerConfig) (*Server, error) {
	addr := net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	if conn, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
		conn.Close()
		return nil, fmt.Errorf("%w: %s", ErrAPIPortBusy, addr)
	}
	logFile, err := os.OpenFile(cfg.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("toxiproxy log: %w", err)
	}
	cmd := exec.Command(cfg.Binary, "-host", cfg.Host, "-port", strconv.Itoa(cfg.Port))
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return nil, fmt.Errorf("start toxiproxy-server: %w", err)
	}
	if cfg.OnStart != nil {
		cfg.OnStart(cmd.Process.Pid)
	}
	s := &Server{cmd: cmd, log: logFile, client: NewClient(addr), done: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		close(s.done)
	}()

	readyCtx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	for {
		if _, err := s.client.do(readyCtx, http.MethodGet, "/version", nil); err == nil {
			return s, nil
		}
		select {
		case <-s.done:
			logFile.Close()
			return nil, errors.New("toxiproxy-server exited before its API answered")
		case <-readyCtx.Done():
			_ = s.Stop(context.Background())
			return nil, fmt.Errorf("toxiproxy API at %s not ready: %w", addr, readyCtx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// NodeSpecs returns one proxy per node, nodeN listening on prefixN:19042 and forwarding to prefixN:9042.
//
// Parameters:
//   - prefix: the loopback prefix, e.g. "127.0.1."
//   - nodes: the node count
//
// Returns:
//   - []Spec: node1 … nodeN
func NodeSpecs(prefix string, nodes int) []Spec {
	specs := make([]Spec, nodes)
	for i := range nodes {
		ip := prefix + strconv.Itoa(i+1)
		specs[i] = Spec{
			Name:     "node" + strconv.Itoa(i+1),
			Listen:   net.JoinHostPort(ip, strconv.Itoa(ProxyPort)),
			Upstream: net.JoinHostPort(ip, strconv.Itoa(NodePort)),
		}
	}
	return specs
}

// PID returns the server's process id, which is also its process group id.
//
// Returns:
//   - int: the pid
func (s *Server) PID() int { return s.cmd.Process.Pid }

// Client returns a client for this server's API.
//
// Returns:
//   - *Client: the client
func (s *Server) Client() *Client { return s.client }

// CloseIdleConnections drops the client's idle keep-alive connections and their goroutines,
// so a goroutine or fd baseline does not depend on how recently the API was called.
func (c *Client) CloseIdleConnections() { c.http.CloseIdleConnections() }

// Stop terminates the server's process group: SIGTERM, then SIGKILL after a grace period.
//
// Parameters:
//   - ctx: bounds the wait; on expiry the group is killed
//
// Returns:
//   - error: when the group could not be signalled
func (s *Server) Stop(ctx context.Context) error {
	defer s.log.Close()
	pgid := s.cmd.Process.Pid
	if err := syscall.Kill(-pgid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("stop toxiproxy-server: %w", err)
	}
	timer := time.NewTimer(stopGrace)
	defer timer.Stop()
	select {
	case <-s.done:
		return nil
	case <-timer.C:
	case <-ctx.Done():
	}
	if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return fmt.Errorf("kill toxiproxy-server: %w", err)
	}
	<-s.done
	return nil
}

// CreateProxy creates and enables one proxy.
//
// Parameters:
//   - ctx: bounds the request
//   - spec: the proxy
//
// Returns:
//   - error: on a transport failure or a non-2xx status
func (c *Client) CreateProxy(ctx context.Context, spec Spec) error {
	body := struct {
		Spec
		Enabled bool `json:"enabled"`
	}{spec, true}
	_, err := c.do(ctx, http.MethodPost, "/proxies", body)
	return err
}

// DeleteProxy deletes one proxy and closes its connections.
//
// Parameters:
//   - ctx: bounds the request
//   - name: the proxy name
//
// Returns:
//   - error: on a transport failure or a non-2xx status
func (c *Client) DeleteProxy(ctx context.Context, name string) error {
	_, err := c.do(ctx, http.MethodDelete, "/proxies/"+name, nil)
	return err
}

// ProxyNames lists the proxies the server holds.
//
// Parameters:
//   - ctx: bounds the request
//
// Returns:
//   - []string: the names, sorted
//   - error: on a transport failure, a non-2xx status or a malformed reply
func (c *Client) ProxyNames(ctx context.Context) ([]string, error) {
	raw, err := c.do(ctx, http.MethodGet, "/proxies", nil)
	if err != nil {
		return nil, err
	}
	var proxies map[string]json.RawMessage
	if err := json.Unmarshal(raw, &proxies); err != nil {
		return nil, fmt.Errorf("parse proxies: %w", err)
	}
	names := make([]string, 0, len(proxies))
	for n := range proxies {
		names = append(names, n)
	}
	slices.Sort(names)
	return names, nil
}

// AddToxic adds a toxic to a proxy.
//
// Parameters:
//   - ctx: bounds the request
//   - proxy: the proxy name
//   - t: the toxic
//
// Returns:
//   - error: on a transport failure or a non-2xx status
func (c *Client) AddToxic(ctx context.Context, proxy string, t Toxic) error {
	_, err := c.do(ctx, http.MethodPost, "/proxies/"+proxy+"/toxics", t)
	return err
}

// RemoveToxic removes a toxic from a proxy.
//
// Parameters:
//   - ctx: bounds the request
//   - proxy: the proxy name
//   - name: the toxic name
//
// Returns:
//   - error: on a transport failure or a non-2xx status
func (c *Client) RemoveToxic(ctx context.Context, proxy, name string) error {
	_, err := c.do(ctx, http.MethodDelete, "/proxies/"+proxy+"/toxics/"+name, nil)
	return err
}

// Reset enables every proxy and removes every toxic.
//
// Parameters:
//   - ctx: bounds the request
//
// Returns:
//   - error: on a transport failure or a non-2xx status
func (c *Client) Reset(ctx context.Context) error {
	_, err := c.do(ctx, http.MethodPost, "/reset", nil)
	return err
}

func (c *Client) do(ctx context.Context, method, path string, body any) ([]byte, error) {
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encode %s %s: %w", method, path, err)
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return nil, fmt.Errorf("build %s %s: %w", method, path, err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("toxiproxy %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("toxiproxy %s %s: read reply: %w", method, path, err)
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("toxiproxy %s %s: status %d: %s", method, path, resp.StatusCode, bytes.TrimSpace(raw))
	}
	return raw, nil
}
