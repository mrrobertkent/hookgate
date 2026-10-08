package gateway

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/mrrobertkent/hookgate/internal/config"
)

type captured struct {
	mu   sync.Mutex
	reqs []*http.Request
	body [][]byte
}

func (c *captured) handler(status int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		c.mu.Lock()
		c.reqs = append(c.reqs, r)
		c.body = append(c.body, b)
		c.mu.Unlock()
		w.WriteHeader(status)
	}
}

func newGateway(t *testing.T, yaml string) *Gateway {
	t.Helper()
	cfg, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	return New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func sign(key, body string) string {
	m := hmac.New(sha256.New, []byte(key))
	m.Write([]byte(body))
	return base64.StdEncoding.EncodeToString(m.Sum(nil))
}

func post(g http.Handler, path, body string, h map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	for k, v := range h {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	return w
}

func TestForwardsOnlyVerifiedBytes(t *testing.T) {
	up := &captured{}
	srv := httptest.NewServer(up.handler(200))
	defer srv.Close()
	t.Setenv("E2_TOKEN", "e2-token-value-0123")
	t.Setenv("E2_SECRET", "e2-signing-secret")
	t.Setenv("INTERNAL", "internal-bearer")
	g := newGateway(t, `
sources:
  - name: e2
    path: /e2/events
    max_body_bytes: 64
    checks:
      - token: {header: X-E2-Token, secrets: [{env: E2_TOKEN}]}
      - hmac: {header: X-Sig, algorithm: sha256, encoding: base64, secrets: [{env: E2_SECRET}]}
    forward:
      urls: [`+srv.URL+`/in]
      pass_headers: [Content-Type, X-Keep-*]
      set_headers: {Authorization: {env: INTERNAL}}
`)
	body := "[{\"a\":1}]\r\n"
	good := map[string]string{"X-E2-Token": "e2-token-value-0123", "X-Sig": sign("e2-signing-secret", body),
		"Content-Type": "application/json", "X-Keep-Me": "1", "X-Drop-Me": "1", "Authorization": "client-sent"}

	if w := post(g, "/e2/events", body, good); w.Code != 200 {
		t.Fatalf("valid request: %d", w.Code)
	}
	if len(up.reqs) != 1 {
		t.Fatalf("upstream got %d requests", len(up.reqs))
	}
	r := up.reqs[0]
	if !bytes.Equal(up.body[0], []byte(body)) {
		t.Errorf("forwarded body %q, want %q", up.body[0], body)
	}
	if r.URL.Path != "/in" || r.Header.Get("Authorization") != "internal-bearer" || r.Header.Get("X-Keep-Me") != "1" ||
		r.Header.Get("X-Drop-Me") != "" || r.Header.Get("X-E2-Token") != "" || r.Header.Get("X-Sig") != "" {
		t.Errorf("forwarded request path %s headers %v", r.URL.Path, r.Header)
	}

	bad := []struct {
		name   string
		path   string
		body   string
		mutate func(map[string]string)
		code   int
	}{
		{"bad signature", "/e2/events", body, func(h map[string]string) { h["X-Sig"] = sign("other", body) }, 401},
		{"bad token", "/e2/events", body, func(h map[string]string) { h["X-E2-Token"] = "nope" }, 401},
		{"no signature", "/e2/events", body, func(h map[string]string) { delete(h, "X-Sig") }, 401},
		{"tampered body", "/e2/events", body + "x", func(map[string]string) {}, 401},
		{"query string", "/e2/events?service.name=x", body, func(map[string]string) {}, 400},
		{"gzip", "/e2/events", body, func(h map[string]string) { h["Content-Encoding"] = "gzip" }, 415},
		{"too large", "/e2/events", strings.Repeat("x", 65), func(map[string]string) {}, 413},
		{"unknown path", "/e2/events/", body, func(map[string]string) {}, 404},
	}
	for _, c := range bad {
		h := map[string]string{}
		for k, v := range good {
			h[k] = v
		}
		c.mutate(h)
		if w := post(g, c.path, c.body, h); w.Code != c.code {
			t.Errorf("%s: got %d, want %d", c.name, w.Code, c.code)
		}
	}
	if len(up.reqs) != 1 {
		t.Fatalf("invalid requests reached upstream: %d requests", len(up.reqs))
	}
}

func TestSourceWithMissingSecretIsIsolated(t *testing.T) {
	up := &captured{}
	srv := httptest.NewServer(up.handler(200))
	defer srv.Close()
	t.Setenv("OK_TOKEN", "ok-token-value-0123")
	g := newGateway(t, `
sources:
  - name: broken
    path: /broken
    checks: [{token: {header: X-T, secrets: [{env: MISSING_FOR_TEST}]}}]
    forward: {urls: [`+srv.URL+`]}
  - name: ok
    path: /ok
    checks: [{token: {header: X-T, secrets: [{env: OK_TOKEN}]}}]
    forward: {urls: [`+srv.URL+`/ok]}
`)
	if w := post(g, "/broken", "{}", map[string]string{"X-T": ""}); w.Code != 503 {
		t.Errorf("broken source: got %d, want 503", w.Code)
	}
	if w := post(g, "/ok", "{}", map[string]string{"X-T": "ok-token-value-0123"}); w.Code != 200 {
		t.Errorf("healthy source: got %d, want 200", w.Code)
	}
	if len(up.reqs) != 1 || up.reqs[0].URL.Path != "/ok" {
		t.Errorf("upstream requests: %d", len(up.reqs))
	}
}

func TestRetriesNextTargetOn5xxAndTransportError(t *testing.T) {
	failing := &captured{}
	f := httptest.NewServer(failing.handler(503))
	defer f.Close()
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	good := &captured{}
	ok := httptest.NewServer(good.handler(202))
	defer ok.Close()
	t.Setenv("T", "token-value-0123")
	g := newGateway(t, `
sources:
  - name: s
    path: /s
    checks: [{token: {header: X-T, secrets: [{env: T}]}}]
    forward: {urls: [`+f.URL+`, `+dead.URL+`, `+ok.URL+`], max_attempts: 3}
`)
	if w := post(g, "/s", "{}", map[string]string{"X-T": "token-value-0123"}); w.Code != 202 {
		t.Fatalf("got %d, want 202 from the third target", w.Code)
	}
	if len(failing.reqs) != 1 || len(good.reqs) != 1 {
		t.Errorf("attempts: failing=%d good=%d", len(failing.reqs), len(good.reqs))
	}
}

func TestUpstream4xxIsFinal(t *testing.T) {
	up := &captured{}
	srv := httptest.NewServer(up.handler(401))
	defer srv.Close()
	t.Setenv("T", "token-value-0123")
	g := newGateway(t, `
sources:
  - name: s
    path: /s
    checks: [{token: {header: X-T, secrets: [{env: T}]}}]
    forward: {urls: [`+srv.URL+`, `+srv.URL+`]}
`)
	if w := post(g, "/s", "{}", map[string]string{"X-T": "token-value-0123"}); w.Code != 401 {
		t.Fatalf("got %d", w.Code)
	}
	if len(up.reqs) != 1 {
		t.Errorf("4xx was retried: %d attempts", len(up.reqs))
	}
}

func TestConfigRejectsUnknownKeysAndUncheckedSources(t *testing.T) {
	for _, y := range []string{
		"sources: [{name: s, path: /s, checks: [{token: {header: X, secrets: [{env: A}]}}], forward: {urls: [http://u]}, typo: 1}]",
		"sources: [{name: s, path: /s, checks: [], forward: {urls: [http://u]}}]",
		"sources: [{name: s, path: /s, checks: [{hmac: {header: X, algorithm: md5, secrets: [{env: A}]}}], forward: {urls: [http://u]}}]",
		"sources: [{name: s, path: /s, checks: [{token: {header: X, secrets: [{env: A}]}}], forward: {urls: [ftp://u]}}]",
	} {
		if _, err := config.Parse([]byte(y)); err == nil {
			t.Errorf("accepted invalid config: %s", y)
		}
	}
}
