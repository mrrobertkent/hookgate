package relay

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/mrrobertkent/hookgate/internal/claimcheck"
	"github.com/mrrobertkent/hookgate/internal/config"
	"github.com/mrrobertkent/hookgate/internal/contract"
	"github.com/mrrobertkent/hookgate/internal/testutil"
)

func testEvent() *event {
	h := nats.Header{}
	h.Set(contract.HeaderContract, "1")
	h.Set(contract.HeaderSource, "resend")
	h.Set(contract.HeaderEventID, "msg_1")
	h.Set(contract.HeaderEventIDAuthenticated, "true")
	h.Set(contract.HeaderReceivedAt, "2026-11-23T10:00:00.123Z")
	h["webhook-id"] = []string{"msg_1"}
	return &event{subject: eventSubj, msgID: "resend:msg_1", streamSeq: 7, attempt: 2, body: []byte(`{"a":1}`), headers: h}
}

func command(t *testing.T, timeout time.Duration, args ...string) *commandDeliverer {
	t.Helper()
	h := helperHandler(t, args...).Command
	h.Timeout = timeout
	d, err := newCommandDeliverer(*h, slog.New(slog.NewTextHandler(t.Output(), nil)))
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestCommandExitMapping(t *testing.T) {
	cases := []struct {
		args []string
		want verdict
	}{
		{[]string{"exit", "0"}, verdictAck},
		{[]string{"exit", "1"}, verdictRetry},
		{[]string{"exit", "2"}, verdictRetry},
		{[]string{"exit", "3"}, verdictRetry},
		{[]string{"exit", "64"}, verdictRetry},
		{[]string{"exit", "65"}, verdictReject},
		{[]string{"exit", "66"}, verdictRetry},
		{[]string{"exit", "69"}, verdictRetry},
		{[]string{"exit", "71"}, verdictRetry},
		{[]string{"exit", "74"}, verdictRetry},
		{[]string{"exit", "75"}, verdictRetry},
		{[]string{"exit", "77"}, verdictRetry},
		{[]string{"exit", "78"}, verdictPause},
		{[]string{"exit", "125"}, verdictRetry},
		{[]string{"exit", "126"}, verdictPause},
		{[]string{"exit", "127"}, verdictPause},
		{[]string{"exit", "128"}, verdictRetry},
		{[]string{"exit", "255"}, verdictRetry},
		{[]string{"kill"}, verdictRetry},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			res := command(t, 10*time.Second, tc.args...).deliver(context.Background(), testEvent())
			if res.verdict != tc.want {
				t.Errorf("verdict %s (%s), want %s", res.verdict, res.detail, tc.want)
			}
		})
	}
}

func TestCommandRejectDetail(t *testing.T) {
	res := command(t, 10*time.Second, "stderr", "65", "unknown event type").deliver(context.Background(), testEvent())
	if res.verdict != verdictReject || res.detail != "exit status 65" {
		t.Errorf("%s %q", res.verdict, res.detail)
	}
}

func TestCommandTimeoutKillsProcessGroup(t *testing.T) {
	dir := t.TempDir()
	start := time.Now()
	res := command(t, 500*time.Millisecond, "spawn", dir).deliver(context.Background(), testEvent())
	if res.verdict != verdictRetry || !strings.Contains(res.detail, "timed out") {
		t.Errorf("%s %q", res.verdict, res.detail)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("took %s", d)
	}
	waitChildGone(t, dir, 5*time.Second)
}

// waitChildGone waits until the child whose pid the helper wrote to dir has exited.
func waitChildGone(t *testing.T, dir string, within time.Duration) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "child.pid")) //nolint:gosec // G304: a test temporary directory.
	if err != nil {
		t.Fatal(err)
	}
	pid, _ := strconv.Atoi(string(b))
	testutil.Eventually(t, within, func() bool {
		return syscall.Kill(pid, 0) == syscall.ESRCH
	}, "grandchild still running")
}

// TestCommandChildHoldingStderr covers a leader that exits while a child keeps its stderr open: the timeout still
// kills the whole group, without waiting for the pipe.
func TestCommandChildHoldingStderr(t *testing.T) {
	dir := t.TempDir()
	start := time.Now()
	res := command(t, 3*time.Second, "spawn-exit", dir).deliver(context.Background(), testEvent())
	if res.verdict != verdictAck {
		t.Errorf("%s %q", res.verdict, res.detail)
	}
	if d := time.Since(start); d >= waitDelay {
		t.Errorf("took %s; the group was not killed at the timeout", d)
	}
	waitChildGone(t, dir, 2*time.Second)
}

func TestCommandCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)
	res := command(t, time.Minute, "sleep", "1m").deliver(ctx, testEvent())
	if res.verdict != verdictCancelled {
		t.Errorf("%s %q", res.verdict, res.detail)
	}
}

func TestCommandStartErrors(t *testing.T) {
	dir := t.TempDir()
	noExec := filepath.Join(dir, "noexec")
	garbage := filepath.Join(dir, "garbage")
	if err := os.WriteFile(noExec, []byte("#!/bin/sh\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(garbage, []byte("\x00\x01\x02 not a program"), 0o700); err != nil { //nolint:gosec // G306: the test needs an executable file.
		t.Fatal(err)
	}
	loop := filepath.Join(dir, "loop")
	if err := os.Symlink(loop, loop); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(dir, "missing"), noExec, garbage, dir, loop, filepath.Join(noExec, "x")} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			d, err := newCommandDeliverer(CommandHandler{Path: path, Timeout: time.Second}, slog.New(slog.DiscardHandler))
			if err != nil {
				t.Fatal(err)
			}
			if res := d.deliver(context.Background(), testEvent()); res.verdict != verdictPause {
				t.Errorf("%s %q", res.verdict, res.detail)
			}
		})
	}
}

func TestCommandInputs(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("RELAY_TEST_ALLOWED", "yes")
	t.Setenv("RELAY_TEST_SECRET_SRC", "s3cret")
	t.Setenv("RELAY_TEST_NOT_ALLOWED", "no")
	h := helperHandler(t, "record", dir).Command
	h.EnvAllow = []string{"RELAY_TEST_ALLOWED", "RELAY_TEST_UNSET"}
	h.SetEnv = map[string]config.SecretRef{"TOKEN": {Env: "RELAY_TEST_SECRET_SRC"}, "OPTIONAL": {Env: "RELAY_TEST_UNSET", Optional: true}}
	d, err := newCommandDeliverer(*h, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	ev := testEvent()
	ev.headers.Set(contract.HeaderReplay, "WEBHOOK_DEAD:4")
	if res := d.deliver(context.Background(), ev); res.verdict != verdictAck {
		t.Fatalf("%s %q", res.verdict, res.detail)
	}
	recs := readRecords(t, dir)
	if len(recs) != 1 {
		t.Fatalf("%d records", len(recs))
	}
	r := recs[0]
	if string(r.Stdin) != `{"a":1}` {
		t.Errorf("stdin %q", r.Stdin)
	}
	want := map[string]string{
		"HOOKGATE_CONTRACT": "1", "HOOKGATE_SOURCE": "resend", "HOOKGATE_SUBJECT": eventSubj,
		"HOOKGATE_MESSAGE_ID": "resend:msg_1", "HOOKGATE_EVENT_ID": "msg_1", "HOOKGATE_EVENT_ID_AUTHENTICATED": "true",
		"HOOKGATE_ATTEMPT": "2", "HOOKGATE_STREAM_SEQ": "7", "HOOKGATE_REPLAY": "WEBHOOK_DEAD:4",
		"HOOKGATE_RECEIVED_AT": "2026-11-23T10:00:00.123Z", "RELAY_TEST_ALLOWED": "yes", "TOKEN": "s3cret",
	}
	for k, v := range want {
		if r.Env[k] != v {
			t.Errorf("%s = %q, want %q", k, r.Env[k], v)
		}
	}
	for _, k := range []string{"RELAY_TEST_NOT_ALLOWED", "RELAY_TEST_UNSET", "OPTIONAL", "PATH", "HOME"} {
		if _, ok := r.Env[k]; ok {
			t.Errorf("%s leaked into the handler environment", k)
		}
	}
	var headers map[string][]string
	if err := json.Unmarshal([]byte(r.Env["HOOKGATE_HEADERS"]), &headers); err != nil {
		t.Fatal(err)
	}
	if headers["webhook-id"][0] != "msg_1" || headers[contract.HeaderSource][0] != "resend" {
		t.Errorf("headers %v", headers)
	}
}

func httpDeliver(t *testing.T, h http.HandlerFunc, timeout time.Duration) (result, *http.Request) {
	t.Helper()
	var got atomic.Pointer[http.Request]
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Store(r.Clone(context.Background()))
		h(w, r)
	}))
	t.Cleanup(srv.Close)
	t.Setenv("RELAY_TEST_AUTH", "Bearer abc")
	d, err := newHTTPDeliverer(HTTPHandler{URL: srv.URL + "/events", Timeout: timeout,
		SetHeaders: map[string]config.SecretRef{"Authorization": {Env: "RELAY_TEST_AUTH"}}})
	if err != nil {
		t.Fatal(err)
	}
	return d.deliver(context.Background(), testEvent()), got.Load()
}

func TestHTTPStatusMapping(t *testing.T) {
	cases := []struct {
		status int
		want   verdict
	}{
		{200, verdictAck}, {202, verdictAck}, {204, verdictAck},
		{300, verdictPause}, {301, verdictPause}, {302, verdictPause}, {304, verdictPause}, {307, verdictPause}, {308, verdictPause},
		{400, verdictReject}, {413, verdictReject}, {415, verdictReject}, {422, verdictReject},
		{401, verdictPause}, {403, verdictPause},
		{402, verdictRetry}, {404, verdictRetry}, {405, verdictRetry}, {406, verdictRetry}, {408, verdictRetry},
		{409, verdictRetry}, {410, verdictRetry}, {418, verdictRetry}, {425, verdictRetry}, {429, verdictRetry},
		{451, verdictRetry}, {499, verdictRetry},
		{500, verdictRetry}, {501, verdictRetry}, {502, verdictRetry}, {503, verdictRetry}, {504, verdictRetry}, {599, verdictRetry},
	}
	for _, tc := range cases {
		t.Run(strconv.Itoa(tc.status), func(t *testing.T) {
			res, _ := httpDeliver(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/events" {
					t.Error("redirect followed")
				}
				w.Header().Set("Location", "/elsewhere")
				w.WriteHeader(tc.status)
			}, 5*time.Second)
			if res.verdict != tc.want {
				t.Errorf("verdict %s (%s), want %s", res.verdict, res.detail, tc.want)
			}
		})
	}
}

func TestHTTPRequest(t *testing.T) {
	res, req := httpDeliver(t, func(w http.ResponseWriter, r *http.Request) {
		if b, _ := io.ReadAll(r.Body); string(b) != `{"a":1}` {
			t.Errorf("body %q", b)
		}
	}, 5*time.Second)
	if res.verdict != verdictAck {
		t.Fatalf("%s %q", res.verdict, res.detail)
	}
	want := map[string]string{
		"Hookgate-Source": "resend", "Hookgate-Event-Id": "msg_1", "Hookgate-Attempt": "2",
		"Webhook-Id": "msg_1", "Authorization": "Bearer abc",
	}
	if req.Method != http.MethodPost {
		t.Errorf("method %s", req.Method)
	}
	for k, v := range want {
		if got := req.Header.Get(k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
}

func TestHTTPTimeoutAndTransport(t *testing.T) {
	res, _ := httpDeliver(t, func(http.ResponseWriter, *http.Request) { time.Sleep(time.Second) }, 200*time.Millisecond)
	if res.verdict != verdictRetry || !strings.Contains(res.detail, "timed out") {
		t.Errorf("timeout: %s %q", res.verdict, res.detail)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	d, err := newHTTPDeliverer(HTTPHandler{URL: "http://" + addr + "/", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if res := d.deliver(context.Background(), testEvent()); res.verdict != verdictRetry {
		t.Errorf("refused: %s %q", res.verdict, res.detail)
	}
}

func TestHTTPProtocolFaults(t *testing.T) {
	cases := map[string]struct {
		h    http.HandlerFunc
		want verdict
	}{
		"101": {func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Connection", "Upgrade")
			w.Header().Set("Upgrade", "x")
			w.WriteHeader(http.StatusSwitchingProtocols)
			w.(http.Flusher).Flush()
			time.Sleep(3 * time.Second)
		}, verdictPause},
		"302 malformed location": {func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Location", "http://[::1")
			w.WriteHeader(http.StatusFound)
		}, verdictPause},
		"400 then stall": {func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("partial"))
			w.(http.Flusher).Flush()
			time.Sleep(3 * time.Second)
		}, verdictRetry},
		"credential in a malformed header": {func(w http.ResponseWriter, _ *http.Request) {
			conn, buf, err := http.NewResponseController(w).Hijack()
			if err != nil {
				return
			}
			_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Length: Bearer secret-token\r\n\r\n")
			_ = buf.Flush()
			conn.Close()
		}, verdictRetry},
		"400 truncated": {func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Length", "100")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("short"))
			w.(http.Flusher).Flush()
			conn, _, err := http.NewResponseController(w).Hijack()
			if err == nil {
				conn.Close()
			}
		}, verdictRetry},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			start := time.Now()
			res, _ := httpDeliver(t, tc.h, 500*time.Millisecond)
			if res.verdict != tc.want {
				t.Errorf("verdict %s (%s), want %s", res.verdict, res.detail, tc.want)
			}
			if d := time.Since(start); d > 2*time.Second {
				t.Errorf("took %s", d)
			}
			if strings.Contains(res.detail, "secret") {
				t.Errorf("detail leaks the response: %q", res.detail)
			}
		})
	}
}

func TestHTTPConfiguredHeaderWins(t *testing.T) {
	var got http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got = r.Header.Clone() }))
	t.Cleanup(srv.Close)
	t.Setenv("RELAY_TEST_AUTH", "Bearer configured")
	d, err := newHTTPDeliverer(HTTPHandler{URL: srv.URL, Timeout: time.Second,
		SetHeaders: map[string]config.SecretRef{"Authorization": {Env: "RELAY_TEST_AUTH"}}})
	if err != nil {
		t.Fatal(err)
	}
	ev := testEvent()
	ev.headers["authorization"] = []string{"Bearer from-sender"}
	if res := d.deliver(context.Background(), ev); res.verdict != verdictAck {
		t.Fatalf("%s %q", res.verdict, res.detail)
	}
	if v := got.Values("Authorization"); len(v) != 1 || v[0] != "Bearer configured" {
		t.Errorf("authorization %q", v)
	}
}

// blockingStore is a claim store whose reads never finish.
type blockingStore struct{ claimcheck.Store }

func (blockingStore) Get(ctx context.Context, _ claimcheck.Ref) ([]byte, error) {
	time.Sleep(time.Hour)
	return nil, ctx.Err()
}

func TestClaimFetchAbandonedOnShutdown(t *testing.T) {
	r := &Relay{cfg: &Config{DrainTimeout: time.Minute, ClaimCheck: &ClaimCheck{}}, claims: blockingStore{}}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	start := time.Now()
	if _, err := r.fetchClaim(ctx, "k"); err == nil {
		t.Fatal("no error")
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("took %s", d)
	}
	r.cfg.DrainTimeout = 200 * time.Millisecond
	start = time.Now()
	if _, err := r.fetchClaim(context.Background(), "k"); err == nil || time.Since(start) > time.Second {
		t.Errorf("fetch not bounded by drain_timeout: %v after %s", err, time.Since(start))
	}
}

func TestHTTPConfiguredHost(t *testing.T) {
	var host string
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { host = r.Host }))
	t.Cleanup(srv.Close)
	t.Setenv("RELAY_TEST_HOST", "app.internal")
	d, err := newHTTPDeliverer(HTTPHandler{URL: srv.URL, Timeout: time.Second,
		SetHeaders: map[string]config.SecretRef{"Host": {Env: "RELAY_TEST_HOST"}}})
	if err != nil {
		t.Fatal(err)
	}
	ev := testEvent()
	ev.headers["host"] = []string{"sender.example"}
	if res := d.deliver(context.Background(), ev); res.verdict != verdictAck {
		t.Fatalf("%s %q", res.verdict, res.detail)
	}
	if host != "app.internal" {
		t.Errorf("host %q", host)
	}
}
