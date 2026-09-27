package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type recorded struct {
	method, path, body string
}

func fakeAPI(t *testing.T, status int, reply string) (*Client, *[]recorded) {
	t.Helper()
	var mu sync.Mutex
	var got []recorded
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		got = append(got, recorded{r.Method, r.URL.Path, string(body)})
		mu.Unlock()
		w.WriteHeader(status)
		_, _ = io.WriteString(w, reply)
	}))
	t.Cleanup(srv.Close)
	return NewClient(srv.Listener.Addr().String()), &got
}

func TestClientRequests(t *testing.T) {
	c, got := fakeAPI(t, http.StatusOK, `{}`)
	ctx := context.Background()
	require.NoError(t, c.CreateProxy(ctx, Spec{Name: "node1", Listen: "127.0.1.1:19042", Upstream: "127.0.1.1:9042"}))
	require.NoError(t, c.AddToxic(ctx, "node1", Toxic{Name: "lat", Type: "latency", Stream: "downstream",
		Toxicity: 1, Attributes: map[string]any{"latency": 200}}))
	require.NoError(t, c.RemoveToxic(ctx, "node1", "lat"))
	require.NoError(t, c.DeleteProxy(ctx, "node1"))
	require.NoError(t, c.Reset(ctx))

	require.Len(t, *got, 5)
	require.Equal(t, recorded{"POST", "/proxies", `{"name":"node1","listen":"127.0.1.1:19042","upstream":"127.0.1.1:9042","enabled":true}`}, (*got)[0])
	require.Equal(t, "POST", (*got)[1].method)
	require.Equal(t, "/proxies/node1/toxics", (*got)[1].path)
	var toxic map[string]any
	require.NoError(t, json.Unmarshal([]byte((*got)[1].body), &toxic))
	require.Equal(t, map[string]any{"name": "lat", "type": "latency", "stream": "downstream", "toxicity": 1.0,
		"attributes": map[string]any{"latency": 200.0}}, toxic)
	require.Equal(t, recorded{"DELETE", "/proxies/node1/toxics/lat", ""}, (*got)[2])
	require.Equal(t, recorded{"DELETE", "/proxies/node1", ""}, (*got)[3])
	require.Equal(t, recorded{"POST", "/reset", ""}, (*got)[4])
}

func TestClientErrorStatus(t *testing.T) {
	c, _ := fakeAPI(t, http.StatusConflict, `{"error":"proxy already exists"}`)
	err := c.CreateProxy(context.Background(), Spec{Name: "node1"})
	require.ErrorContains(t, err, "409")
	require.ErrorContains(t, err, "proxy already exists")
}

func TestClientHonoursContext(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-block }))
	t.Cleanup(func() { close(block); srv.Close() })
	c := NewClient(srv.Listener.Addr().String())
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	require.Error(t, c.Reset(ctx))
	require.Less(t, time.Since(start), 5*time.Second)
}

func TestNodeSpecs(t *testing.T) {
	require.Equal(t, []Spec{
		{Name: "node1", Listen: "127.0.1.1:19042", Upstream: "127.0.1.1:9042"},
		{Name: "node2", Listen: "127.0.1.2:19042", Upstream: "127.0.1.2:9042"},
	}, NodeSpecs("127.0.1.", 2))
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	addr, ok := l.Addr().(*net.TCPAddr)
	require.True(t, ok)
	return addr.Port
}

// TestServerLifecycle runs the real toxiproxy-server when it is on PATH (soak/mise.toml installs it).
func TestServerLifecycle(t *testing.T) {
	bin, err := exec.LookPath("toxiproxy-server")
	if err != nil {
		t.Skip("toxiproxy-server not on PATH")
	}
	port := freePort(t)
	logPath := filepath.Join(t.TempDir(), "toxiproxy.log")
	ctx := context.Background()

	srv, err := StartServer(ctx, ServerConfig{Binary: bin, Host: "127.0.0.1", Port: port, LogPath: logPath})
	require.NoError(t, err)
	require.Positive(t, srv.PID())

	// A second server on the same port is refused before it starts.
	_, err = StartServer(ctx, ServerConfig{Binary: bin, Host: "127.0.0.1", Port: port, LogPath: logPath})
	require.ErrorIs(t, err, ErrAPIPortBusy)

	echo, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer echo.Close()
	go func() {
		for {
			conn, err := echo.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(conn, conn); conn.Close() }()
		}
	}()

	listen := "127.0.0.1:" + strconv.Itoa(freePort(t))
	c := srv.Client()
	require.NoError(t, c.CreateProxy(ctx, Spec{Name: "echo", Listen: listen, Upstream: echo.Addr().String()}))
	conn, err := net.Dial("tcp", listen)
	require.NoError(t, err)
	_, err = conn.Write([]byte("ping"))
	require.NoError(t, err)
	buf := make([]byte, 4)
	_, err = io.ReadFull(conn, buf)
	require.NoError(t, err)
	require.Equal(t, "ping", string(buf))
	conn.Close()

	names, err := c.ProxyNames(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"echo"}, names)

	require.NoError(t, srv.Stop(ctx))
	require.Eventually(t, func() bool {
		_, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(port), 100*time.Millisecond)
		return err != nil
	}, 5*time.Second, 50*time.Millisecond)
	_, err = os.Stat(logPath)
	require.NoError(t, err)
}
