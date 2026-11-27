package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mrrobertkent/hookgate/internal/output"
)

type unreadyProbeOutput struct {
	output.Output
	readyCalls int
}

func (o *unreadyProbeOutput) Ready() error {
	o.readyCalls++
	return errors.New("output unavailable")
}

func TestSourceProbes(t *testing.T) {
	up := &captured{}
	srv := httptest.NewServer(up.handler(http.StatusOK))
	defer srv.Close()
	t.Setenv("PROBE_TOKEN", "probe-token-value-0123")
	t.Setenv("PROBE_MISSING_TOKEN", "")
	yaml := `
max_concurrent: 1
sources:
  - name: off
    path: /events
    checks: [{token: {header: X-T, secrets: [{env: PROBE_MISSING_TOKEN}]}}]
    output: {http: {urls: [` + srv.URL + `]}}
  - name: on
    path: /events
    hosts: [probe.example.com]
    answer_probes: true
    checks: [{token: {header: X-T, secrets: [{env: PROBE_TOKEN}]}}]
    output: {http: {urls: [` + srv.URL + `]}}
  - name: missing
    path: /missing
    answer_probes: true
    checks: [{token: {header: X-T, secrets: [{env: PROBE_MISSING_TOKEN}]}}]
    output: {http: {urls: [` + srv.URL + `]}}
  - name: query
    path: /query
    answer_probes: true
    allow_query: true
    checks: [{token: {header: X-T, secrets: [{env: PROBE_TOKEN}]}}]
    output: {http: {urls: [` + srv.URL + `]}}
  - name: query_host
    path: /query
    hosts: [probe.example.com]
    checks: [{token: {header: X-T, secrets: [{env: PROBE_TOKEN}]}}]
    output: {http: {urls: [` + srv.URL + `]}}
`
	for _, c := range []struct {
		name, method, path, host string
		status                   int
		allow, source, outcome   string
		unready                  bool
	}{
		{"off GET with missing secret", http.MethodGet, "/events", "other.example.com", 405, "POST", "off", "bad_request", false},
		{"off HEAD", http.MethodHead, "/events", "other.example.com", 405, "POST", "off", "bad_request", false},
		{"on GET", http.MethodGet, "/events", "probe.example.com", 200, "", "on", "probe", false},
		{"on HEAD", http.MethodHead, "/events", "PROBE.Example.COM.:443", 200, "", "on", "probe", false},
		{"query denied", http.MethodGet, "/events?x=1", "probe.example.com", 400, "", "on", "bad_request", false},
		{"query allowed", http.MethodHead, "/query?x=1", "other.example.com", 200, "", "query", "probe", false},
		{"host overrides enabled fallback", http.MethodHead, "/query?x=1", "probe.example.com", 405, "POST", "query_host", "bad_request", false},
		{"missing secret", http.MethodGet, "/missing", "other.example.com", 200, "", "missing", "probe", false},
		{"unready output", http.MethodHead, "/events", "probe.example.com", 200, "", "on", "probe", true},
		{"on PUT", http.MethodPut, "/events", "probe.example.com", 405, "GET, HEAD, POST", "on", "bad_request", false},
		{"unknown HEAD", http.MethodHead, "/unknown", "probe.example.com", 404, "", "", "", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			g := newGateway(t, yaml)
			closeGateway(t, g)
			var logs bytes.Buffer
			g.log = slog.New(slog.NewJSONHandler(&logs, nil))
			g.sem <- struct{}{}

			body := strings.NewReader("unread")
			req := httptest.NewRequest(c.method, c.path, body)
			req.Host = c.host
			req.Header.Set("Content-Encoding", "gzip")
			var unready *unreadyProbeOutput
			if c.unready {
				src := g.match(req)
				unready = &unreadyProbeOutput{Output: src.out}
				src.out = unready
			}
			w := httptest.NewRecorder()
			g.ServeHTTP(w, req)
			res := w.Result()
			defer res.Body.Close()

			if res.StatusCode != c.status || res.Header.Get("Allow") != c.allow {
				t.Fatalf("got %d with Allow %q, want %d with Allow %q", res.StatusCode, res.Header.Get("Allow"), c.status, c.allow)
			}
			if body.Len() != len("unread") {
				t.Error("gateway read the request body")
			}
			if unready != nil && unready.readyCalls != 0 {
				t.Errorf("gateway checked output readiness %d times", unready.readyCalls)
			}
			if c.outcome == "probe" {
				assertProbeResponse(t, res)
				var line struct {
					Level   string `json:"level"`
					Message string `json:"msg"`
					Source  string `json:"source"`
					Outcome string `json:"outcome"`
					Status  int    `json:"status"`
				}
				if err := json.Unmarshal(logs.Bytes(), &line); err != nil {
					t.Fatalf("expected one JSON log entry, got %q: %v", logs.String(), err)
				}
				if line.Level != "INFO" || line.Message != "webhook" || line.Source != c.source ||
					line.Outcome != "probe" || line.Status != http.StatusOK {
					t.Errorf("unexpected probe log: %+v", line)
				}
			}
			m := httptest.NewRecorder()
			g.Admin().ServeHTTP(m, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			if c.outcome == "" {
				if strings.Contains(m.Body.String(), "hookgate_requests_total{") {
					t.Errorf("unknown route counted a request: %s", m.Body.String())
				}
				return
			}
			want := fmt.Sprintf("hookgate_requests_total{source=%q,outcome=%q} 1\n", c.source, c.outcome)
			if !strings.Contains(m.Body.String(), want) {
				t.Errorf("metrics missing %q:\n%s", want, m.Body.String())
			}
		})
	}
	up.mu.Lock()
	defer up.mu.Unlock()
	if len(up.reqs) != 0 {
		t.Errorf("upstream got %d requests", len(up.reqs))
	}
}

func TestSourceProbesOverHTTP(t *testing.T) {
	t.Setenv("PROBE_TOKEN", "probe-token-value-0123")
	g := newGateway(t, `
sources:
  - name: s
    path: /s
    answer_probes: true
    checks: [{token: {header: X-T, secrets: [{env: PROBE_TOKEN}]}}]
    output: {http: {urls: [http://upstream.invalid]}}
`)
	closeGateway(t, g)
	for _, c := range []struct {
		protocol string
		http2    bool
	}{
		{"HTTP/1.1", false},
		{"HTTP/2.0", true},
	} {
		t.Run(c.protocol, func(t *testing.T) {
			srv := httptest.NewUnstartedServer(g)
			if c.http2 {
				srv.EnableHTTP2 = true
				srv.StartTLS()
			} else {
				srv.Start()
			}
			defer srv.Close()
			for _, method := range []string{http.MethodGet, http.MethodHead} {
				t.Run(method, func(t *testing.T) {
					req, err := http.NewRequestWithContext(t.Context(), method, srv.URL+"/s", nil)
					if err != nil {
						t.Fatal(err)
					}
					res, err := srv.Client().Do(req)
					if err != nil {
						t.Fatal(err)
					}
					defer res.Body.Close()
					if res.Proto != c.protocol {
						t.Errorf("negotiated %s, want %s", res.Proto, c.protocol)
					}
					assertProbeResponse(t, res)
				})
			}
		})
	}
}

func assertProbeResponse(t *testing.T, res *http.Response) {
	t.Helper()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK || len(body) != 0 || res.Header.Get("Cache-Control") != "no-store" ||
		res.Header.Get("Content-Length") != "0" || res.ContentLength != 0 || len(res.Header.Values("Content-Type")) != 0 {
		t.Errorf("unexpected probe response: status=%d, headers=%v, ContentLength=%d, body=%q",
			res.StatusCode, res.Header, res.ContentLength, body)
	}
}
