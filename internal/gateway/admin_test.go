package gateway

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mrrobertkent/hookgate/internal/config"
)

func TestAdminAndMetrics(t *testing.T) {
	up := &captured{}
	srv := httptest.NewServer(up.handler(200))
	defer srv.Close()
	t.Setenv("T", "token-value")
	g := newGateway(t, `
sources:
  - name: s
    path: /s
    checks: [{token: {header: X-T, secrets: [{env: T}]}}]
    forward: {urls: [`+srv.URL+`]}
  - name: broken
    path: /b
    checks: [{token: {header: X-T, secrets: [{env: MISSING_FOR_TEST}]}}]
    forward: {urls: [`+srv.URL+`]}
`)
	post(g, "/s", "{}", map[string]string{"X-T": "token-value"})
	post(g, "/s", "{}", map[string]string{"X-T": "wrong"})

	admin := g.Admin()
	get := func(path string) (int, string) {
		w := httptest.NewRecorder()
		admin.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		return w.Code, w.Body.String()
	}
	if code, _ := get("/healthz"); code != 200 {
		t.Errorf("healthz %d", code)
	}
	if code, _ := get("/readyz"); code != 200 {
		t.Errorf("readyz %d", code)
	}
	_, m := get("/metrics")
	for _, want := range []string{
		`hookgate_requests_total{source="s",outcome="forwarded"} 1`,
		`hookgate_requests_total{source="s",outcome="rejected"} 1`,
		`hookgate_upstream_attempts_total{source="s",result="2xx"} 1`,
		`hookgate_source_ready{source="broken"} 0`,
		`hookgate_source_ready{source="s"} 1`,
		`hookgate_draining 0`,
	} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics missing %q:\n%s", want, m)
		}
	}
	g.m.draining.Store(true)
	if code, _ := get("/readyz"); code != 503 {
		t.Errorf("readyz while draining %d", code)
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// A shutdown signal keeps the listener serving for drain_delay before closing.
func TestRunDrains(t *testing.T) {
	up := &captured{}
	srv := httptest.NewServer(up.handler(200))
	defer srv.Close()
	t.Setenv("T", "token-value")
	addr, adminAddr := freePort(t), freePort(t)
	cfg, err := config.Parse([]byte(`
listen: "` + addr + `"
admin_listen: "` + adminAddr + `"
drain_delay: 300ms
shutdown_timeout: 2s
sources:
  - name: s
    path: /s
    checks: [{token: {header: X-T, secrets: [{env: T}]}}]
    forward: {urls: [` + srv.URL + `]}
`))
	if err != nil {
		t.Fatal(err)
	}
	g := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()

	send := func() (int, error) {
		req, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/s", strings.NewReader("{}"))
		req.Header.Set("X-T", "token-value")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0, err
		}
		resp.Body.Close()
		return resp.StatusCode, nil
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		if code, err := send(); err == nil && code == 200 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server did not start")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	time.Sleep(100 * time.Millisecond)
	if code, err := send(); err != nil || code != 200 {
		t.Fatalf("request during drain: %d %v", code, err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after drain")
	}
}
