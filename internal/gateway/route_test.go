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
    output: {http: {urls: [`+srv.URL+`/named]}}
  - name: v6
    hosts: ["[::1]", 192.0.2.1]
    path: /events
    `+check+`
    output: {http: {urls: [`+srv.URL+`/v6]}}
  - name: fallback
    path: /events
    `+check+`
    output: {http: {urls: [`+srv.URL+`/fallback]}}
  - name: hosted-only
    hosts: [hooks.example.com, id.example.com]
    path: /only
    `+check+`
    output: {http: {urls: [`+srv.URL+`/hosted-only]}}
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
		{"hooks.example.com:", "/only", nil, "/hosted-only"},
		// Malformed authorities match no listed host: 404 where the path has no source without hosts.
		{"hooks.example.com:abc", "/only", nil, ""},
		{"hooks.example.com:443:443", "/only", nil, ""},
		{"hooks.example.com:123456", "/only", nil, ""},
		{"[hooks.example.com]:443", "/only", nil, ""},
		{"[hooks.example.com]", "/only", nil, ""},
		{"hooks.example.com]", "/only", nil, ""},
		{"[::1", "/only", nil, ""},
		// ...and fall back to the source without hosts where the path has one.
		{"hooks.example.com:abc", "/events", nil, "/fallback"},
		{"[::1", "/events", nil, "/fallback"},
		{"[::1]]", "/events", nil, "/fallback"},
		{"[::1]x", "/events", nil, "/fallback"},
		{"::1", "/events", nil, "/fallback"},
		{"[192.0.2.1]", "/events", nil, "/fallback"},
		{"[fe80::1%25eth0]", "/events", nil, "/fallback"},
		// strings.ToLower maps U+0130 to "i" and U+212A (Kelvin) to "k": non-ASCII never matches a listed host.
		{"HOOKS.EXAMPLE.COM", "/only", nil, "/hosted-only"},
		{"id.example.com", "/only", nil, "/hosted-only"},
		{"\u0130d.example.com", "/only", nil, ""},
		{"hoo\u212as.example.com", "/only", nil, ""},
		{"hooks.example.com\x80", "/only", nil, ""},
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
		case c.want == "" && (w.Code != http.StatusNotFound || len(up.reqs) != 0):
			t.Errorf("%s %s: got %d with %d upstream requests, want 404 and none", c.host, c.path, w.Code, len(up.reqs))
		case c.want == "":
		case w.Code != http.StatusOK || len(up.reqs) != 1:
			t.Errorf("%s %s: got %d with %d upstream requests", c.host, c.path, w.Code, len(up.reqs))
		case up.reqs[0].URL.Path != c.want:
			t.Errorf("%s %s: routed to %s, want %s", c.host, c.path, up.reqs[0].URL.Path, c.want)
		}
	}
}

func TestMethodNotAllowed(t *testing.T) {
	g := newGateway(t, `
sources:
  - name: s
    path: /s
    checks: [{token: {header: X-T, secrets: [{env: UNSET_FOR_TEST}]}}]
    output: {http: {urls: [http://upstream.invalid]}}
`)
	r := httptest.NewRequest(http.MethodGet, "/s", nil)
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != http.MethodPost {
		t.Errorf("GET: got %d, Allow %q", w.Code, w.Header().Get("Allow"))
	}
}

// FuzzHost checks that every spelling of an accepted configured host that a client may send (any case, a
// port, IPv6 brackets, a trailing dot on a name) normalises to the configured form, and that normalising never
// panics.
func FuzzHost(f *testing.F) {
	for _, s := range []string{"example.com", "Example.COM.", "[::1]", "::1", "192.0.2.1", "[::1]:80", "a:b", "*.x", "", "[x]:1", "[::1"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, h string) {
		if got, ok := requestHost(h); !ok && got != "" {
			t.Fatalf("requestHost(%q) = %q, false", h, got)
		}
		n, err := config.NormalizeHost(h)
		if err != nil {
			return
		}
		if again, err := config.NormalizeHost(n); err != nil || again != n {
			t.Fatalf("NormalizeHost(%q) = %q, then %q, %v", h, n, again, err)
		}
		lit := n
		if strings.Contains(n, ":") {
			lit = "[" + n + "]"
		} else {
			checkSent(t, n, n+".")
		}
		for _, sent := range []string{lit, strings.ToUpper(lit), net.JoinHostPort(n, "443"), net.JoinHostPort(strings.ToUpper(n), "1"), lit + ":"} {
			checkSent(t, n, sent)
		}
	})
}

func checkSent(t *testing.T, want, sent string) {
	t.Helper()
	if got, ok := requestHost(sent); !ok || got != want {
		t.Fatalf("configured %q: request host %q normalises to %q, %v", want, sent, got, ok)
	}
}
