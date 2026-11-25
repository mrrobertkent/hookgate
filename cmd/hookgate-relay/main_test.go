package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func runCmd(args ...string) (int, string, string) {
	var out, errOut bytes.Buffer
	code := run(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

func writeConfig(t *testing.T, handlerPath string) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "relay.yaml")
	cfg := `
connection: {nats: {urls: ["nats://127.0.0.1:4222"], none: true}}
stream: WEBHOOK_EVENTS
consumer: app
filter_subjects: [webhooks.events.resend, webhooks.replay.app]
dead_letter: {stream: WEBHOOK_DEAD}
handler:
  command:
    path: ` + handlerPath + `
    set_env: {TOKEN: {env: RELAY_MAIN_TEST_TOKEN}}
`
	if err := os.WriteFile(file, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	return file
}

func TestVersionAndUsage(t *testing.T) {
	if code, out, _ := runCmd("version"); code != 0 || out != "dev\n" {
		t.Errorf("version %d %q", code, out)
	}
	if code, _, errOut := runCmd("bogus"); code != 2 || !strings.Contains(errOut, "Usage:") {
		t.Errorf("unknown command %d", code)
	}
}

func TestValidate(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	file := writeConfig(t, exe)
	t.Setenv("RELAY_MAIN_TEST_TOKEN", "x")
	if code, out, errOut := runCmd("validate", "-config", file); code != 0 || !strings.Contains(out, "ok: consumer app") {
		t.Errorf("validate %d %q %q", code, out, errOut)
	}
	t.Setenv("RELAY_MAIN_TEST_TOKEN", "")
	if code, _, errOut := runCmd("validate", "-config", file); code != 1 || !strings.Contains(errOut, "RELAY_MAIN_TEST_TOKEN") {
		t.Errorf("missing secret %d %q", code, errOut)
	}
	t.Setenv("RELAY_MAIN_TEST_TOKEN", "x")
	missing := writeConfig(t, "/nonexistent/handler")
	if code, _, errOut := runCmd("validate", "-config", missing); code != 1 || !strings.Contains(errOut, "handler.command.path") {
		t.Errorf("missing handler %d %q", code, errOut)
	}
	if runtime.GOOS == "linux" {
		t.Setenv("RELAY_MAIN_TEST_TOKEN", strings.Repeat("x", 4<<20))
		if code, _, errOut := runCmd("validate", "-config", file); code != 1 || !strings.Contains(errOut, "environment variable TOKEN") {
			t.Errorf("oversized secret %d %q", code, errOut)
		}
	}
}

func TestReplayFlags(t *testing.T) {
	if code, _, errOut := runCmd("replay"); code != 2 || !strings.Contains(errOut, "-seq") {
		t.Errorf("no seq %d %q", code, errOut)
	}
	if code, _, _ := runCmd("replay", "-seq", "0"); code != 2 {
		t.Errorf("seq 0 accepted")
	}
	var s seqList
	_ = s.Set("3")
	_ = s.Set("9")
	if s.String() != "3,9" {
		t.Errorf("%q", s.String())
	}
}

func TestHealthcheck(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer ok.Close()
	if code, _, _ := runCmd("healthcheck", "-url", ok.URL); code != 0 {
		t.Errorf("healthy %d", code)
	}
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }))
	defer bad.Close()
	if code, _, _ := runCmd("healthcheck", "-url", bad.URL); code != 1 {
		t.Errorf("unhealthy %d", code)
	}
}
