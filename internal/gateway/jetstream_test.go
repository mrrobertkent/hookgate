package gateway

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mrrobertkent/hookgate/internal/config"
	"github.com/mrrobertkent/hookgate/internal/contract"
	"github.com/mrrobertkent/hookgate/internal/output"
	"github.com/mrrobertkent/hookgate/internal/testutil"
)

const tokenValue = "token-value-0123456"

func jetStreamGateway(t *testing.T, urls []string, server, output string) *Gateway {
	t.Helper()
	t.Setenv("T", tokenValue)
	cfg, err := config.Parse([]byte(server + `
connections:
  ev:
    nats: {urls: ["` + strings.Join(urls, `", "`) + `"], none: true}
sources:
  - name: src
    path: /e
    max_body_bytes: 65536
    checks: [{token: {header: X-T, secrets: [{env: T}]}}]
    output:
      jetstream:
        connection: ev
        stream: EVENTS
        message_id: {header: X-Event-Id, unauthenticated: true}
` + output))
	if err != nil {
		t.Fatal(err)
	}
	return New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func closeGateway(t *testing.T, g *Gateway) {
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = g.Close(ctx)
	})
}

func waitReady(t *testing.T, g *Gateway) {
	t.Helper()
	s := g.routes["/e"].any
	testutil.Eventually(t, 15*time.Second, func() bool { return s.out.Ready() == nil }, "source not ready")
}

func sendEvent(g http.Handler, id string) int {
	return post(g, "/e", `{"id":"`+id+`"}`, map[string]string{"X-T": tokenValue, "X-Event-Id": id}).Code
}

func storedIDs(t *testing.T, js jetstream.JetStream) map[string]int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	st, err := js.Stream(ctx, "EVENTS")
	if err != nil {
		t.Fatal(err)
	}
	info, err := st.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]int{}
	for seq := info.State.FirstSeq; info.State.Msgs > 0 && seq <= info.State.LastSeq; seq++ {
		m, err := st.GetMsg(ctx, seq)
		if err != nil {
			t.Fatalf("seq %d: %v", seq, err)
		}
		ids[m.Header.Get(contract.HeaderEventID)]++
	}
	return ids
}

func TestJetStreamSourceAndMetrics(t *testing.T) {
	s := testutil.RunServer(t)
	_, js := testutil.Connect(t, s.ClientURL())
	testutil.CreateStream(t, js, jetstream.StreamConfig{Name: "EVENTS", Subjects: []string{"webhooks.events.>"}})
	g := jetStreamGateway(t, []string{s.ClientURL()}, "", "")
	closeGateway(t, g)
	waitReady(t, g)

	w := post(g, "/e", "{}", map[string]string{"X-T": tokenValue, "X-Event-Id": "a"})
	if w.Code != 200 || w.Header().Get("Content-Type") != "application/json" || w.Body.String() != `{"status":"accepted","duplicate":false}`+"\n" {
		t.Fatalf("%d %q %q", w.Code, w.Header(), w.Body.String())
	}
	if code := sendEvent(g, "a"); code != 200 {
		t.Fatalf("duplicate: %d", code)
	}
	if code := post(g, "/e", "{}", map[string]string{"X-T": "wrong"}).Code; code != 401 {
		t.Fatalf("forged: %d", code)
	}

	rec := httptest.NewRecorder()
	g.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, want := range []string{
		`hookgate_requests_total{source="src",outcome="published"} 1`,
		`hookgate_requests_total{source="src",outcome="duplicate"} 1`,
		`hookgate_publish_attempts_total{source="src",result="ok"} 1`,
		`hookgate_publish_attempts_total{source="src",result="duplicate"} 1`,
		`hookgate_publish_duration_seconds_count{source="src"} 2`,
		`hookgate_nats_connected{connection="ev"} 1`,
		`hookgate_source_ready{source="src"} 1`,
	} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("metrics missing %q:\n%s", want, rec.Body.String())
		}
	}
}

// While the connection is down the source answers 503 at once, and its readiness gauge reports the check
// that failed.
func TestJetStreamSourceUnavailable(t *testing.T) {
	g := jetStreamGateway(t, []string{fmt.Sprintf("nats://127.0.0.1:%d", testutil.FreePort(t))}, "", "")
	closeGateway(t, g)
	w := post(g, "/e", "{}", map[string]string{"X-T": tokenValue})
	if w.Code != 503 || w.Body.String() != "unavailable\n" {
		t.Errorf("%d %q", w.Code, w.Body.String())
	}

	s := testutil.RunServer(t)
	g2 := jetStreamGateway(t, []string{s.ClientURL()}, "", "")
	closeGateway(t, g2)
	src := g2.routes["/e"].any
	testutil.Eventually(t, 5*time.Second, func() bool {
		err := src.out.Ready()
		return err != nil && strings.Contains(err.Error(), "stream EVENTS not found")
	}, "missing stream not reported")
	if code := sendEvent(g2, "x"); code != 503 {
		t.Errorf("not-ready source: %d", code)
	}
	rec := httptest.NewRecorder()
	g2.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, want := range []string{`hookgate_source_ready{source="src"} 0`, `hookgate_requests_total{source="src",outcome="not_ready"} 1`} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

// A shutdown signal that arrives while a publish waits for its acknowledgement lets the publish finish; the
// connection is drained only after the HTTP server has shut down.
func TestShutdownDuringPendingPubAck(t *testing.T) {
	s := testutil.RunServer(t)
	_, js := testutil.Connect(t, s.ClientURL())
	testutil.CreateStream(t, js, jetstream.StreamConfig{Name: "EVENTS", Subjects: []string{"webhooks.events.>"}})
	proxy := testutil.StartProxy(t, strings.TrimPrefix(s.ClientURL(), "nats://"))
	addr, adminAddr := freePort(t), freePort(t)
	g := jetStreamGateway(t, []string{"nats://" + proxy.Addr()},
		"listen: \""+addr+"\"\nadmin_listen: \""+adminAddr+"\"\nread_timeout: 2s\nshutdown_timeout: 6s\n",
		"        attempt_timeout: 3s\n        timeout: 3s\n")
	waitReady(t, g)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx) }()
	testutil.Eventually(t, 5*time.Second, func() bool {
		resp, err := http.Get("http://" + adminAddr + "/healthz")
		if err == nil {
			resp.Body.Close()
		}
		return err == nil
	}, "gateway did not start")

	proxy.HoldNext([]byte(`"stream":"EVENTS"`), time.Second)
	published := proxy.Seen([]byte("webhooks.events.src"))
	status := make(chan int, 1)
	go func() {
		req, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/e", strings.NewReader("{}"))
		req.Header.Set("X-T", tokenValue)
		req.Header.Set("X-Event-Id", "pending")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			status <- 0
			return
		}
		resp.Body.Close()
		status <- resp.StatusCode
	}()
	select {
	case <-published:
	case <-time.After(5 * time.Second):
		t.Fatal("publish not seen")
	}
	cancel()
	if code := <-status; code != 200 {
		t.Fatalf("pending publish answered %d", code)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return")
	}
	if !g.conns["ev"].IsClosed() {
		t.Error("connection still open after Run returned")
	}
	if ids := storedIDs(t, js); ids["pending"] != 1 {
		t.Errorf("stored %v", ids)
	}
}

// eventLoad sends events with unique IDs from several workers and records which were acknowledged.
type eventLoad struct {
	mu                     sync.Mutex
	acked                  []string
	failed, ackedAfterKill int
	killed                 bool
}

// start runs workers until the returned function is called; that function waits for them.
func (l *eventLoad) start(g http.Handler, workers int) func() {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				case <-time.After(2 * time.Millisecond):
				}
				id := fmt.Sprintf("w%d-%d", w, i)
				l.record(id, sendEvent(g, id))
			}
		})
	}
	return func() { close(stop); wg.Wait() }
}

func (l *eventLoad) record(id string, code int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if code != http.StatusOK {
		l.failed++
		return
	}
	l.acked = append(l.acked, id)
	if l.killed {
		l.ackedAfterKill++
	}
}

func (l *eventLoad) afterKill() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ackedAfterKill
}

func (l *eventLoad) markKilled() {
	l.mu.Lock()
	l.killed = true
	l.mu.Unlock()
}

// moveLeaderTo makes the named server the stream's leader and returns it.
func moveLeaderTo(t *testing.T, c *testutil.Cluster, name string) *server.Server {
	t.Helper()
	nc, _ := testutil.Connect(t, c.URLs()...)
	testutil.Eventually(t, 15*time.Second, func() bool {
		if l := c.StreamLeader("EVENTS"); l != nil && l.Name() == name {
			return true
		}
		_, _ = nc.Request("$JS.API.STREAM.LEADER.STEPDOWN.EVENTS", []byte(`{"placement":{"preferred":"`+name+`"}}`), 2*time.Second)
		time.Sleep(200 * time.Millisecond)
		return false
	}, "stream leader did not move to "+name)
	return c.StreamLeader("EVENTS")
}

// Losing the node that is both the stream leader and the gateway's server may cost a few 503s while publishes
// keep flowing, but never an acknowledged event.
func TestFailoverLosesNoAcknowledgedEvent(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster failover")
	}
	c := testutil.RunCluster(t, 3)
	_, js := testutil.Connect(t, c.URLs()...)
	testutil.CreateStream(t, js, jetstream.StreamConfig{Name: "EVENTS", Subjects: []string{"webhooks.events.>"}, Replicas: 3, Duplicates: time.Minute})
	g := jetStreamGateway(t, c.URLs(), "", "        attempt_timeout: 1s\n        timeout: 4s\n")
	closeGateway(t, g)
	waitReady(t, g)

	load := &eventLoad{}
	stop := load.start(g, 4)
	time.Sleep(time.Second)
	leader := moveLeaderTo(t, c, g.conns["ev"].ConnectedServerName())
	load.markKilled()
	leader.Shutdown()
	// The gateway reconnects (ReconnectWait plus jitter), re-checks the stream, and the stream elects a leader.
	deadline := time.Now().Add(30 * time.Second)
	for load.afterKill() < 100 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	stop()

	if load.ackedAfterKill == 0 {
		t.Fatal("no event acknowledged after the leader was lost")
	}
	stored := storedIDs(t, js)
	for _, id := range load.acked {
		if stored[id] != 1 {
			t.Errorf("acknowledged event %s stored %d times", id, stored[id])
		}
	}
	acked, ackedAfterKill, failed := len(load.acked), load.ackedAfterKill, load.failed
	t.Logf("%d acknowledged (%d after the leader loss), %d answered 503", acked, ackedAfterKill, failed)
}

// The event ID and signing time a Standard Webhooks check authenticates become the message's ID and metadata;
// the signature header is never published.
func TestStandardWebhooksEventToJetStream(t *testing.T) {
	s := testutil.RunServer(t)
	_, js := testutil.Connect(t, s.ClientURL())
	testutil.CreateStream(t, js, jetstream.StreamConfig{Name: "EVENTS", Subjects: []string{"webhooks.events.>"}})
	key := []byte("0123456789abcdef0123456789abcdef")
	t.Setenv("SW_SECRET", "whsec_"+base64.StdEncoding.EncodeToString(key))
	cfg, err := config.Parse([]byte(`
connections:
  ev:
    nats: {urls: ["` + s.ClientURL() + `"], none: true}
sources:
  - name: resend
    path: /e
    max_body_bytes: 65536
    checks: [{standard_webhooks: {headers: svix, secrets: [{env: SW_SECRET}]}}]
    output:
      jetstream:
        connection: ev
        stream: EVENTS
        pass_headers: [Content-Type, Svix-*]
`))
	if err != nil {
		t.Fatal(err)
	}
	g := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	closeGateway(t, g)
	waitReady(t, g)

	body, id, ts := `{"type":"email.sent"}`, "msg_2Kx", time.Now().Unix()
	mac := hmac.New(sha256.New, key)
	fmt.Fprintf(mac, "%s.%d.%s", id, ts, body)
	h := map[string]string{"svix-id": id, "svix-timestamp": strconv.FormatInt(ts, 10),
		"svix-signature": "v1," + base64.StdEncoding.EncodeToString(mac.Sum(nil)), "Content-Type": "application/json"}
	for i, want := range []string{`"duplicate":false`, `"duplicate":true`} {
		if w := post(g, "/e", body, h); w.Code != 200 || !strings.Contains(w.Body.String(), want) {
			t.Fatalf("delivery %d: %d %s", i, w.Code, w.Body.String())
		}
	}
	st, _ := js.Stream(context.Background(), "EVENTS")
	m, err := st.GetMsg(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		contract.HeaderMsgID:                "resend:" + id,
		contract.HeaderEventID:              id,
		contract.HeaderEventIDAuthenticated: "true",
		contract.HeaderSignedAt:             time.Unix(ts, 0).UTC().Format(time.RFC3339),
		"svix-id":                           id,
		"svix-timestamp":                    strconv.FormatInt(ts, 10),
	}
	for k, v := range want {
		if got := m.Header.Get(k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	if _, ok := m.Header["svix-signature"]; ok {
		t.Error("signature header published")
	}
}

// A reconnect invalidates the previous check: after the server comes back without the stream, the source is not
// ready until a check on the new connection passes, with no manual nudge.
func TestReconnectRechecksDestination(t *testing.T) {
	port := testutil.FreePort(t)
	first := testutil.RunServer(t, func(o *server.Options) { o.Port = port })
	_, js := testutil.Connect(t, first.ClientURL())
	testutil.CreateStream(t, js, jetstream.StreamConfig{Name: "EVENTS", Subjects: []string{"webhooks.events.>"}})
	g := jetStreamGateway(t, []string{first.ClientURL()}, "", "")
	closeGateway(t, g)
	waitReady(t, g)
	src := g.routes["/e"].any
	gauge := func() string {
		rec := httptest.NewRecorder()
		g.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		return rec.Body.String()
	}

	first.Shutdown()
	first.WaitForShutdown()
	testutil.Eventually(t, 5*time.Second, func() bool {
		return errors.Is(src.out.Ready(), output.ErrNotConnected) && strings.Contains(gauge(), `hookgate_source_ready{source="src"} 0`)
	}, "source still ready after the server stopped")

	testutil.RunServer(t, func(o *server.Options) { o.Port = port }) // fresh store: no stream
	testutil.Eventually(t, 15*time.Second, func() bool {
		err := src.out.Ready()
		return err != nil && strings.Contains(err.Error(), "stream EVENTS not found")
	}, "reconnect did not re-run the checks")
	if code := sendEvent(g, "after-restart"); code != 503 {
		t.Errorf("request after reconnect: %d", code)
	}
}

// A malformed event ID that a Standard Webhooks check rejects gets 400, not reject_status.
func TestRepeatedSignedEventIDIs400(t *testing.T) {
	t.Setenv("SW_SECRET", "whsec_"+base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")))
	cfg, err := config.Parse([]byte(`
sources:
  - name: s
    path: /e
    checks: [{standard_webhooks: {secrets: [{env: SW_SECRET}]}}]
    output: {http: {urls: ["http://127.0.0.1:1/"]}}
`))
	if err != nil {
		t.Fatal(err)
	}
	g := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	closeGateway(t, g)
	r := httptest.NewRequest(http.MethodPost, "/e", strings.NewReader("{}"))
	r.Header["Webhook-Id"] = []string{"a", "b"}
	r.Header.Set("Webhook-Timestamp", strconv.FormatInt(time.Now().Unix(), 10))
	r.Header.Set("Webhook-Signature", "v1,x")
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 400 {
		t.Errorf("repeated webhook-id: %d", w.Code)
	}
}

// Run that cannot bind its listener still releases the connections New opened.
func TestRunBindFailureClosesConnections(t *testing.T) {
	s := testutil.RunServer(t)
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busy.Close()
	g := jetStreamGateway(t, []string{s.ClientURL()}, "listen: \""+busy.Addr().String()+"\"\nshutdown_timeout: 30s\n", "")
	if err := g.Run(context.Background()); err == nil {
		t.Fatal("Run bound a busy address")
	}
	if !g.conns["ev"].IsClosed() {
		t.Error("connection left open")
	}
}
