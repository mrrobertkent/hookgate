package relay

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/mrrobertkent/hookgate/internal/config"
	"github.com/mrrobertkent/hookgate/internal/contract"
	"github.com/mrrobertkent/hookgate/internal/testutil"
)

func TestRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		values []string
		want   time.Duration
	}{
		{nil, 0},
		{[]string{""}, 0},
		{[]string{"0"}, 0},
		{[]string{"120"}, 2 * time.Minute},
		{[]string{" 30 "}, 30 * time.Second},
		{[]string{"007"}, 7 * time.Second},
		{[]string{"99999999999999999999"}, time.Duration(maxRetryAfter) * time.Second},
		{[]string{"9223372036"}, time.Duration(maxRetryAfter) * time.Second},
		{[]string{"-5"}, 0},
		{[]string{"1.5"}, 0},
		{[]string{"5s"}, 0},
		{[]string{"30", "60"}, 0},
		{[]string{"Mon, 23 Nov 2026 10:01:30 GMT"}, 90 * time.Second},  // IMF-fixdate; the weekday is not checked
		{[]string{"Monday, 23-Nov-26 10:00:10 GMT"}, 10 * time.Second}, // obsolete RFC 850
		{[]string{"Mon Nov 23 10:00:05 2026"}, 5 * time.Second},        // obsolete asctime
		{[]string{"Mon, 23 Nov 2026 09:59:00 GMT"}, 0},                 // in the past
		{[]string{"Mon, 23 Nov 2026 10:00:00 GMT"}, 0},
		{[]string{"23 Nov 2026 10:01:00"}, 0},
		{[]string{"tomorrow"}, 0},
	}
	for _, tc := range cases {
		h := http.Header{}
		for _, v := range tc.values {
			h.Add("Retry-After", v)
		}
		if got := retryAfter(h, now); got != tc.want {
			t.Errorf("retryAfter(%q) = %s, want %s", tc.values, got, tc.want)
		}
	}
}

// FuzzRetryAfter checks that Retry-After parsing never panics or yields a negative delay.
func FuzzRetryAfter(f *testing.F) {
	for _, s := range []string{"120", "0", "Mon, 23 Nov 2026 10:01:30 GMT", "Monday, 23-Nov-26 10:00:10 GMT", "Mon Nov 23 10:00:05 2026", "99999999999999999999", "-1", ""} {
		f.Add(s)
	}
	now := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	f.Fuzz(func(t *testing.T, v string) {
		if d := retryAfter(http.Header{"Retry-After": {v}}, now); d < 0 {
			t.Fatalf("retryAfter(%q) = %s", v, d)
		}
	})
}

func TestHTTPRetryAfterResult(t *testing.T) {
	cases := []struct {
		status int
		header string
		want   time.Duration
	}{
		{http.StatusTooManyRequests, "30", 30 * time.Second},
		{http.StatusServiceUnavailable, "45", 45 * time.Second},
		{http.StatusServiceUnavailable, time.Now().Add(time.Hour).UTC().Format(http.TimeFormat), time.Hour},
		{http.StatusTooManyRequests, "soon", 0},
		{http.StatusInternalServerError, "30", 0},
		{http.StatusBadGateway, "30", 0},
	}
	for _, tc := range cases {
		res, _ := httpDeliver(t, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", tc.header)
			w.WriteHeader(tc.status)
		}, 5*time.Second)
		if res.verdict != verdictRetry || res.after > tc.want || res.after < tc.want-5*time.Second {
			t.Errorf("%d Retry-After %q: %s after %s (%s), want %s", tc.status, tc.header, res.verdict, res.after, res.detail, tc.want)
		}
		if tc.want > 0 && !strings.Contains(res.detail, "Retry-After") {
			t.Errorf("detail %q", res.detail)
		}
	}
}

func TestRetryDelay(t *testing.T) {
	r := &Relay{cfg: &Config{Backoff: []time.Duration{10 * time.Second, time.Minute, 5 * time.Minute}}}
	cases := []struct {
		attempt uint64
		after   time.Duration
		want    time.Duration
	}{
		{1, 0, 10 * time.Second},
		{1, 3 * time.Second, 10 * time.Second},
		{1, 30 * time.Second, 30 * time.Second},
		{1, time.Hour, 5 * time.Minute},
		{2, 30 * time.Second, time.Minute},
		{2, 2 * time.Minute, 2 * time.Minute},
		{7, time.Hour, 5 * time.Minute},
	}
	for _, tc := range cases {
		meta := &jetstream.MsgMetadata{NumDelivered: tc.attempt}
		if got := r.retryDelay(meta, tc.after); got != tc.want {
			t.Errorf("attempt %d, Retry-After %s: %s, want %s", tc.attempt, tc.after, got, tc.want)
		}
	}
	// A schedule that falls never shortens the backoff step.
	r.cfg.Backoff = []time.Duration{time.Minute, 10 * time.Second}
	if got := r.retryDelay(&jetstream.MsgMetadata{NumDelivered: 1}, time.Hour); got != time.Minute {
		t.Errorf("falling schedule: %s", got)
	}
}

// TestRetryAfterThroughBroker: a 503 with Retry-After delays the redelivery by the requested time, not the shorter
// backoff step.
func TestRetryAfterThroughBroker(t *testing.T) {
	var mu sync.Mutex
	var at []time.Time
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		at = append(at, time.Now())
		first := len(at) == 1
		mu.Unlock()
		if first {
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	t.Cleanup(srv.Close)
	e := newEnv(t)
	e.consumer(t, "rex")
	cfg := e.config(t, "rex", Handler{HTTP: &HTTPHandler{URL: srv.URL, Timeout: 5 * time.Second}})
	cfg.Backoff = []time.Duration{100 * time.Millisecond, 10 * time.Second}
	start(t, cfg, true)
	seq := e.publish(t, pub{body: []byte("{}")})
	e.waitSettled(t, "rex", seq)
	mu.Lock()
	defer mu.Unlock()
	if len(at) != 2 {
		t.Fatalf("%d requests", len(at))
	}
	if gap := at[1].Sub(at[0]); gap < 2*time.Second || gap > 4*time.Second {
		t.Errorf("redelivered after %s, want about 2s", gap)
	}
}

// TestCommandRejectRetriesUnknownExit: an exit status without a meaning of its own is retried up to max_attempts,
// then dead-lettered as max_attempts; 65 is dead-lettered at once.
func TestCommandRejectRetriesUnknownExit(t *testing.T) {
	for code, want := range map[string]struct {
		reason string
		runs   int
	}{"1": {contract.DeadMaxAttempts, 3}, "65": {contract.DeadHandlerRejected, 1}} {
		t.Run(code, func(t *testing.T) {
			e := newEnv(t)
			e.consumer(t, "rex")
			dir := t.TempDir()
			cfg := e.config(t, "rex", helperHandler(t, "record", dir, code))
			cfg.MaxAttempts = 3
			start(t, cfg, true)
			seq := e.publish(t, pub{body: []byte("{}")})
			recs := e.waitDead(t, 1)
			e.waitSettled(t, "rex", seq)
			if got := recs[0].Header.Get(contract.HeaderDeadReason); got != want.reason {
				t.Errorf("reason %q, want %q", got, want.reason)
			}
			if n := len(readRecords(t, dir)); n != want.runs {
				t.Errorf("%d runs, want %d", n, want.runs)
			}
		})
	}
}

func TestCheckExecSize(t *testing.T) {
	lim := execLimits{maxString: 1024, total: eventEnvReserve() + execSlack + 4096}
	cases := []struct {
		name string
		args []string
		env  []string
		want string
	}{
		{"fits", []string{"ingest"}, []string{"TOKEN=abc"}, ""},
		{"long argument", []string{"ok", strings.Repeat("a", 1024)}, nil, "handler.command.args[1]: 1024 bytes, above the 1023-byte limit"},
		{"longest argument", []string{strings.Repeat("a", 1023)}, nil, ""},
		{"long variable", nil, []string{"TOKEN=" + strings.Repeat("s", 1020)}, "environment variable TOKEN: 1026 bytes"},
		{"total", nil, []string{"A=" + strings.Repeat("x", 1000), "B=" + strings.Repeat("x", 1000), "C=" + strings.Repeat("x", 1000), "D=" + strings.Repeat("x", 1000)}, "exceed the"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkExecSize("/usr/local/bin/handler", tc.args, tc.env, lim)
			switch {
			case tc.want == "" && err != nil:
				t.Errorf("error %v", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Errorf("error %v, want %q", err, tc.want)
			}
			if err != nil && strings.Contains(err.Error(), "xxx") {
				t.Errorf("error quotes a value: %v", err)
			}
		})
	}
	if err := checkExecSize("/x", []string{strings.Repeat("a", 1<<20)}, nil, execLimits{}); err != nil {
		t.Errorf("no limits: %v", err)
	}
}

// TestCommandStaticEnvTooLarge: a set_env secret that cannot fit an execve stops the relay at start instead of
// dead-lettering every event.
func TestCommandStaticEnvTooLarge(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("execve limits are checked on Linux")
	}
	lim := currentExecLimits()
	if lim.maxString == 0 || lim.total < eventEnvReserve() {
		t.Fatalf("limits %+v", lim)
	}
	secret := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(secret, []byte(strings.Repeat("s", lim.maxString)), 0o600); err != nil {
		t.Fatal(err)
	}
	h := CommandHandler{Path: "/usr/local/bin/handler", Timeout: time.Second, SetEnv: map[string]config.SecretRef{"BIG": {File: secret}}}
	if _, err := newCommandDeliverer(h, slog.New(slog.DiscardHandler)); err == nil || !strings.Contains(err.Error(), "environment variable BIG") {
		t.Errorf("error %v", err)
	}
	h.SetEnv = nil
	h.Args = slices.Repeat([]string{strings.Repeat("a", lim.maxString/2)}, lim.total/(lim.maxString/2))
	if _, err := newCommandDeliverer(h, slog.New(slog.DiscardHandler)); err == nil || !strings.Contains(err.Error(), "exceed the") {
		t.Errorf("error %v", err)
	}
}

func TestReadinessDeadLetterKeepsRecords(t *testing.T) {
	cases := map[string]struct {
		events func(*jetstream.StreamConfig)
		dead   func(*jetstream.StreamConfig)
		want   string
	}{
		"duplicate window": {nil, func(c *jetstream.StreamConfig) { c.Duplicates = 10 * time.Second }, "duplicate_window 10s is below"},
		"shorter max_age": {func(c *jetstream.StreamConfig) { c.MaxAge = 48 * time.Hour },
			func(c *jetstream.StreamConfig) { c.MaxAge = 24 * time.Hour }, "max_age 24h0m0s is below"},
		"max_age against unlimited": {nil, func(c *jetstream.StreamConfig) { c.MaxAge = 24 * time.Hour }, "max_age 24h0m0s is below"},
		"longer max_age":            {func(c *jetstream.StreamConfig) { c.MaxAge = 24 * time.Hour }, func(c *jetstream.StreamConfig) { c.MaxAge = 48 * time.Hour }, ""},
		"unlimited max_age":         {func(c *jetstream.StreamConfig) { c.MaxAge = 24 * time.Hour }, nil, ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			e.consumer(t, "rex")
			update := func(c jetstream.StreamConfig, modify func(*jetstream.StreamConfig)) {
				if modify == nil {
					return
				}
				modify(&c)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if _, err := e.js.UpdateStream(ctx, c); err != nil {
					t.Fatal(err)
				}
			}
			update(jetstream.StreamConfig{Name: eventStream, Subjects: []string{"webhooks.events.>", "webhooks.replay.>"}, Duplicates: time.Minute}, tc.events)
			update(jetstream.StreamConfig{Name: deadStream, Subjects: []string{"webhooks.dead.>"}, AllowDirect: true, Discard: jetstream.DiscardNew}, tc.dead)
			r := start(t, e.config(t, "rex", helperHandler(t, "exit", "0")), tc.want == "")
			if tc.want == "" {
				return
			}
			testutil.Eventually(t, 10*time.Second, func() bool {
				err := r.notReady()
				return err != nil && strings.Contains(err.Error(), tc.want)
			}, "relay ready with a dead-letter stream that can lose records")
		})
	}
}

// TestExecSizeCheckIsSufficient: fixed arguments that just pass the start check still leave room for an event with
// 60 KiB of headers.
func TestExecSizeCheckIsSufficient(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("execve limits are checked on Linux")
	}
	lim := currentExecLimits()
	h := helperHandler(t, "exit", "0").Command
	h.Timeout = 10 * time.Second
	chunk := lim.maxString/2 - 1
	for {
		next := append(slices.Clone(h.Args), strings.Repeat("a", chunk))
		if checkExecSize(h.Path, next, nil, lim) != nil {
			break
		}
		h.Args = next
	}
	for n := chunk; n > 0; n /= 2 {
		if next := append(slices.Clone(h.Args), strings.Repeat("b", n)); checkExecSize(h.Path, next, nil, lim) == nil {
			h.Args = next
		}
	}
	d, err := newCommandDeliverer(*h, slog.New(slog.NewTextHandler(t.Output(), nil)))
	if err != nil {
		t.Fatal(err)
	}
	ev := testEvent()
	ev.headers["webhook-big"] = []string{strings.Repeat("v", 60<<10)}
	if res := d.deliver(context.Background(), ev); res.verdict != verdictAck {
		t.Errorf("%s %q", res.verdict, res.detail)
	}
}
