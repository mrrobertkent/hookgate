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
