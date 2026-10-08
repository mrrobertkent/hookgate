package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestHealthcheck(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer ok.Close()
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer down.Close()
	unreachable := httptest.NewServer(http.NotFoundHandler())
	unreachable.Close()

	for url, want := range map[string]int{ok.URL: 0, down.URL: 1, unreachable.URL: 1} {
		var stdout, stderr bytes.Buffer
		if got := run([]string{"healthcheck", "-url", url}, &stdout, &stderr); got != want {
			t.Errorf("healthcheck %s = %d, want %d (stderr %q)", url, got, want, stderr.String())
		}
	}
}

func TestValidate(t *testing.T) {
	file := filepath.Join(t.TempDir(), "hookgate.yaml")
	cfg := `
sources:
  - name: s
    path: /s
    checks: [{token: {header: X-T, secrets: [{env: HOOKGATE_TEST_TOKEN}]}}]
    forward: {urls: ["http://upstream/"]}
`
	if err := os.WriteFile(file, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	for secret, want := range map[string]int{"sixteen-bytes!!!": 0, "too-short": 1, "": 1} {
		t.Setenv("HOOKGATE_TEST_TOKEN", secret)
		var stdout, stderr bytes.Buffer
		if got := run([]string{"validate", "-config", file}, &stdout, &stderr); got != want {
			t.Errorf("validate with a %d-byte secret = %d, want %d (stderr %q)", len(secret), got, want, stderr.String())
		}
		if secret != "" && strings.Contains(stderr.String(), secret) {
			t.Errorf("validate printed the secret: %q", stderr.String())
		}
	}
}

func TestVersionAndUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if run([]string{"version"}, &stdout, &stderr) != 0 || strings.TrimSpace(stdout.String()) != version {
		t.Errorf("version printed %q", stdout.String())
	}
	if run([]string{"bogus"}, &stdout, &stderr) != 2 {
		t.Error("unknown command did not exit 2")
	}
}
