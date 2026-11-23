package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const minimal = `
sources:
  - name: s
    path: /s
    checks: [{hmac: {header: X-Sig, algorithm: sha256, secrets: [{env: K}]}}]
    forward: {urls: [http://upstream:8080/in]}
`

func TestDefaults(t *testing.T) {
	c, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	s := c.Sources[0]
	switch {
	case c.Listen != ":8080", c.AdminListen != ":9090", c.MaxConcurrent != 64, c.IdleTimeout != 120*time.Second:
		t.Errorf("server defaults: %+v", c)
	case s.MaxBodyBytes != 1<<20, s.RejectStatus != 401, s.Checks[0].HMAC.Encoding != "hex":
		t.Errorf("source defaults: %+v", s)
	case s.Forward.MaxAttempts != 3, s.Forward.AttemptTimeout != 5*time.Second, s.Forward.Timeout != 20*time.Second:
		t.Errorf("forward defaults: %+v", s.Forward)
	}
}

func TestValidationErrors(t *testing.T) {
	cases := map[string]string{
		"no sources":     `listen: ":1"`,
		"duplicate path": strings.Replace(minimal, "  - name: s", "  - name: a\n    path: /s\n    checks: [{token: {header: X, secrets: [{env: K}]}}]\n    forward: {urls: [http://u]}\n  - name: s", 1),
		"relative path":  strings.Replace(minimal, "path: /s", "path: s", 1),
		"unclean path":   strings.Replace(minimal, "path: /s", "path: /s/../t", 1),
		"both checks":    strings.Replace(minimal, "{hmac:", "{token: {header: X, secrets: [{env: K}]}, hmac:", 1),
		"bad encoding":   strings.Replace(minimal, "algorithm: sha256,", "algorithm: sha256, encoding: b32,", 1),
		"no secrets":     strings.Replace(minimal, "secrets: [{env: K}]", "secrets: []", 1),
		"env and file":   strings.Replace(minimal, "{env: K}", "{env: K, file: /f}", 1),
		"timeouts":       strings.Replace(minimal, "forward: {", "forward: {attempt_timeout: 10s, timeout: 1s, ", 1),
		"unknown key":    minimal + "    colour: blue\n",
	}
	for name, y := range cases {
		if _, err := Parse([]byte(y)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

const standardWebhooks = `
sources:
  - name: s
    path: /s
    checks: [{standard_webhooks: {secrets: [{env: K}]}}]
    forward: {urls: [http://upstream:8080/in]}
`

func TestStandardWebhooks(t *testing.T) {
	c, err := Parse([]byte(standardWebhooks))
	if err != nil {
		t.Fatal(err)
	}
	if sw := c.Sources[0].Checks[0].StandardWebhooks; sw.Headers != "standard" || *sw.Tolerance != 5*time.Minute {
		t.Errorf("standard_webhooks defaults: %+v", sw)
	}
	c, err = Parse([]byte(strings.Replace(standardWebhooks, "{secrets:", "{headers: svix, tolerance: 30s, secrets:", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if sw := c.Sources[0].Checks[0].StandardWebhooks; sw.Headers != "svix" || *sw.Tolerance != 30*time.Second {
		t.Errorf("standard_webhooks settings: %+v", sw)
	}

	cases := map[string]struct{ yaml, want string }{
		"unknown headers":       {strings.Replace(standardWebhooks, "{secrets:", "{headers: custom, secrets:", 1), "standard_webhooks.headers must be standard or svix"},
		"negative tolerance":    {strings.Replace(standardWebhooks, "{secrets:", "{tolerance: -1s, secrets:", 1), "standard_webhooks.tolerance must be > 0 and <= 1h"},
		"zero tolerance":        {strings.Replace(standardWebhooks, "{secrets:", "{tolerance: 0s, secrets:", 1), "standard_webhooks.tolerance must be > 0 and <= 1h"},
		"long tolerance":        {strings.Replace(standardWebhooks, "{secrets:", "{tolerance: 61m, secrets:", 1), "standard_webhooks.tolerance must be > 0 and <= 1h"},
		"no secrets":            {strings.Replace(standardWebhooks, "secrets: [{env: K}]", "secrets: []", 1), "standard_webhooks: at least one secret is required"},
		"with hmac":             {strings.Replace(standardWebhooks, "{standard_webhooks:", "{hmac: {header: X, algorithm: sha256, secrets: [{env: K}]}, standard_webhooks:", 1), "set exactly one of token, hmac, standard_webhooks"},
		"empty check":           {strings.Replace(standardWebhooks, "checks: [{standard_webhooks: {secrets: [{env: K}]}}]", "checks: [{}]", 1), "set exactly one of token, hmac, standard_webhooks"},
		"two standard_webhooks": {strings.Replace(standardWebhooks, "checks: [{standard_webhooks: {secrets: [{env: K}]}}]", "checks: [{standard_webhooks: {secrets: [{env: K}]}}, {standard_webhooks: {headers: svix, secrets: [{env: K}]}}]", 1), "at most one standard_webhooks check is allowed"},
		"unknown key":           {strings.Replace(standardWebhooks, "{secrets:", "{header: X, secrets:", 1), "field header not found"},
	}
	for name, c := range cases {
		_, err := Parse([]byte(c.yaml))
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want an error containing %q", name, err, c.want)
		}
	}
}

func TestSecretRef(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "s")
	if err := os.WriteFile(f, []byte("from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SET", "from-env")
	cases := []struct {
		ref     SecretRef
		want    string
		wantErr bool
	}{
		{SecretRef{Env: "SET"}, "from-env", false},
		{SecretRef{Env: "UNSET_FOR_TEST"}, "", true},
		{SecretRef{Env: "UNSET_FOR_TEST", Optional: true}, "", false},
		{SecretRef{File: f}, "from-file", false},
		{SecretRef{File: empty}, "", true},
		{SecretRef{File: filepath.Join(dir, "missing")}, "", true},
		{SecretRef{File: filepath.Join(dir, "missing"), Optional: true}, "", false},
	}
	for _, c := range cases {
		got, err := c.ref.Resolve()
		if got != c.want || (err != nil) != c.wantErr {
			t.Errorf("%s: got %q, %v", c.ref, got, err)
		}
	}
}

func TestLoad(t *testing.T) {
	file := filepath.Join(t.TempDir(), "hookgate.yaml")
	if err := os.WriteFile(file, []byte(minimal), 0o600); err != nil {
		t.Fatal(err)
	}
	if c, err := Load(file); err != nil || len(c.Sources) != 1 {
		t.Fatalf("Load = %v, %v", c, err)
	}
	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Error("Load of a missing file succeeded")
	}
}

// source renders one source; hosts "" omits the key.
func source(name, hosts, path string) string {
	if hosts != "" {
		hosts = "\n    hosts: " + hosts
	}
	return "  - name: " + name + hosts + "\n    path: " + path +
		"\n    checks: [{token: {header: X, secrets: [{env: K}]}}]\n    forward: {urls: [http://u]}\n"
}

func TestRouteUniqueness(t *testing.T) {
	cases := []struct {
		name    string
		sources string
		wantErr string
	}{
		{"host-less and hosted share a path", source("a", "", "/e") + source("b", "[x.example]", "/e"), ""},
		{"same host on different paths", source("a", "[x.example]", "/e") + source("b", "[x.example]", "/f"), ""},
		{"different hosts on one path", source("a", "[x.example]", "/e") + source("b", "[y.example]", "/e"), ""},
		{"two host-less sources on one path", source("a", "", "/e") + source("b", "", "/e"),
			`source "b": path /e without hosts is already used by source "a"`},
		{"shared host on one path", source("a", "[x.example, y.example]", "/e") + source("b", "[z.example, Y.Example.]", "/e"),
			`source "b": host y.example on path /e is already used by source "a"`},
		{"host listed twice", source("a", "[x.example, X.EXAMPLE]", "/e"),
			`source "a": host x.example on path /e is already used by source "a"`},
	}
	for _, c := range cases {
		_, err := Parse([]byte("sources:\n" + c.sources))
		switch {
		case c.wantErr == "" && err != nil:
			t.Errorf("%s: %v", c.name, err)
		case c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)):
			t.Errorf("%s: got %v, want %q", c.name, err, c.wantErr)
		}
	}
}

func TestNormalizeHost(t *testing.T) {
	ok := map[string]string{
		"example.com":                  "example.com",
		"Hooks.Example.COM":            "hooks.example.com",
		"example.com.":                 "example.com",
		"xn--bcher-kva.example":        "xn--bcher-kva.example",
		"localhost":                    "localhost",
		"a-b.c1":                       "a-b.c1",
		"192.0.2.1":                    "192.0.2.1",
		"::1":                          "::1",
		"[::1]":                        "::1",
		"[2001:DB8:0:0:0:0:0:1]":       "2001:db8::1",
		"2001:0db8::0001":              "2001:db8::1",
		strings.Repeat("a", 63) + ".x": strings.Repeat("a", 63) + ".x",
	}
	for in, want := range ok {
		if got, err := NormalizeHost(in); err != nil || got != want {
			t.Errorf("NormalizeHost(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{
		"", ".", "*", "*.example.com", "example.*", "example.com:443", "[::1]:443", "192.0.2.1:80", "[::1", "[192.0.2.1]",
		"fe80::1%eth0", "[fe80::1%25eth0]", "exa mple.com", "under_score.example", "-lead.example", "trail-.example",
		"a..b", "example.com..", "bücher.example", "İ.example", "\u212a.example", "example.com\x80", "http://example.com", "example.com/path",
		strings.Repeat("a", 64) + ".x", strings.Repeat("a.", 127) + "aa",
	} {
		if got, err := NormalizeHost(in); err == nil {
			t.Errorf("NormalizeHost(%q) = %q, want an error", in, got)
		}
	}
}

func TestHostsAreNormalisedInPlace(t *testing.T) {
	c, err := Parse([]byte("sources:\n" + source("a", "[Hooks.Example.COM., \"[::1]\"]", "/e")))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Sources[0].Hosts; len(got) != 2 || got[0] != "hooks.example.com" || got[1] != "::1" {
		t.Errorf("hosts %q", got)
	}
}

func TestSourceNames(t *testing.T) {
	for _, name := range []string{"a", "0", "resend", "GitHub", "github-app_2", "B_2-x", strings.Repeat("a", 63)} {
		if _, err := Parse([]byte("sources:\n" + source(name, "", "/e"))); err != nil {
			t.Errorf("name %q: %v", name, err)
		}
	}
	for _, name := range []string{`""`, "-a", "_a", "a.b", "a:b", "a/b", `"a b"`, `"a*"`, "a>", "ä", strings.Repeat("a", 64)} {
		if _, err := Parse([]byte("sources:\n" + source(name, "", "/e"))); err == nil {
			t.Errorf("name %q: accepted", name)
		}
	}
	if _, err := Parse([]byte("sources:\n" + source("a", "", "/e") + source("a", "", "/f"))); err == nil ||
		!strings.Contains(err.Error(), "duplicate name") {
		t.Errorf("duplicate name: %v", err)
	}
}

func TestHostsMustBeNonEmptyStrings(t *testing.T) {
	for _, hosts := range []string{"[null]", "[~]", "[x.example, null]", "[[x.example]]", "[{a: b}]", "[]", "x.example", "null", "~", `""`} {
		if c, err := Parse([]byte("sources:\n" + source("a", hosts, "/e"))); err == nil {
			t.Errorf("hosts: %s accepted as %q", hosts, c.Sources[0].Hosts)
		}
	}
	// An empty hosts: key (no value) is null too.
	if _, err := Parse([]byte("sources:\n" + strings.Replace(source("a", "", "/e"), "    path:", "    hosts:\n    path:", 1))); err == nil {
		t.Error("empty hosts key accepted")
	}
}
