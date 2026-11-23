package gateway

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mrrobertkent/hookgate/internal/config"
)

func TestRoutesByHostAndPath(t *testing.T) {
	up := &captured{}
	srv := httptest.NewServer(up.handler(200))
	defer srv.Close()
	t.Setenv("ROUTE_TOKEN", "route-token-value-0123")
	check := `checks: [{token: {header: X-T, secrets: [{env: ROUTE_TOKEN}]}}]`
	g := newGateway(t, `
sources:
  - name: named
    hosts: [Hooks.Example.COM., other.example.com]
    path: /events
    `+check+`
    forward: {urls: [`+srv.URL+`/named]}
  - name: v6
    hosts: ["[::1]", 192.0.2.1]
    path: /events
    `+check+`
    forward: {urls: [`+srv.URL+`/v6]}
  - name: fallback
    path: /events
    `+check+`
    forward: {urls: [`+srv.URL+`/fallback]}
  - name: hosted-only
    hosts: [hooks.example.com]
    path: /only
    `+check+`
    forward: {urls: [`+srv.URL+`/hosted-only]}
`)
	cases := []struct {
		host, path string
		header     map[string]string
		want       string // upstream path; "" means 404
	}{
		{"hooks.example.com", "/events", nil, "/named"},
		{"other.example.com", "/events", nil, "/named"},
		{"HOOKS.Example.Com", "/events", nil, "/named"},
		{"hooks.example.com:8443", "/events", nil, "/named"},
		{"hooks.example.com.", "/events", nil, "/named"},
		{"Hooks.Example.Com.:443", "/events", nil, "/named"},
		{"[::1]:8080", "/events", nil, "/v6"},
		{"[::1]", "/events", nil, "/v6"},
		{"[0:0::0001]", "/events", nil, "/v6"},
		{"192.0.2.1:80", "/events", nil, "/v6"},
		{"unknown.example.com", "/events", nil, "/fallback"},
		{"", "/events", nil, "/fallback"},
		{"unknown.example.com", "/events", map[string]string{"X-Forwarded-Host": "hooks.example.com"}, "/fallback"},
		{"hooks.example.com", "/only", nil, "/hosted-only"},
		{"other.example.com", "/only", nil, ""},
		{"hooks.example.com", "/elsewhere", nil, ""},
	}
	for _, c := range cases {
		up.reqs = nil
		r := httptest.NewRequest(http.MethodPost, c.path, strings.NewReader("{}"))
		r.Host = c.host
		r.Header.Set("X-T", "route-token-value-0123")
		for k, v := range c.header {
			r.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		g.ServeHTTP(w, r)
		switch {
		case c.want == "" && w.Code != http.StatusNotFound:
			t.Errorf("%s %s: got %d, want 404", c.host, c.path, w.Code)
		case c.want == "":
		case w.Code != http.StatusOK || len(up.reqs) != 1:
			t.Errorf("%s %s: got %d with %d upstream requests", c.host, c.path, w.Code, len(up.reqs))
		case up.reqs[0].URL.Path != c.want:
			t.Errorf("%s %s: routed to %s, want %s", c.host, c.path, up.reqs[0].URL.Path, c.want)
		}
	}
	if len(up.reqs) != 0 {
		t.Errorf("unmatched request reached upstream")
	}
}

func TestMethodNotAllowed(t *testing.T) {
	g := newGateway(t, `
sources:
  - name: s
    path: /s
    checks: [{token: {header: X-T, secrets: [{env: UNSET_FOR_TEST}]}}]
    forward: {urls: [http://upstream.invalid]}
`)
	r := httptest.NewRequest(http.MethodGet, "/s", nil)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != http.MethodPost {
		t.Errorf("GET: got %d, Allow %q", w.Code, w.Header().Get("Allow"))
	}
}

// FuzzHost checks that every spelling of an accepted configured host that a client may send (any case, a
// port, IPv6 brackets, a trailing dot) normalises to the configured form, and that normalising never panics.
func FuzzHost(f *testing.F) {
	for _, s := range []string{"example.com", "Example.COM.", "[::1]", "::1", "192.0.2.1", "[::1]:80", "a:b", "*.x", ""} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, h string) {
		_ = requestHost(h)
		n, err := config.NormalizeHost(h)
		if err != nil {
			return
		}
		if again, err := config.NormalizeHost(n); err != nil || again != n {
			t.Fatalf("NormalizeHost(%q) = %q, then %q, %v", h, n, again, err)
		}
		for _, sent := range []string{n, strings.ToUpper(n), net.JoinHostPort(n, "443"), net.JoinHostPort(strings.ToUpper(n), "1"), n + "."} {
			if got := requestHost(sent); got != n {
				t.Fatalf("configured %q (normalised %q): request host %q normalises to %q", h, n, sent, got)
			}
		}
	})
}
