package gateway

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"

	"github.com/mrrobertkent/hookgate/internal/config"
	"github.com/mrrobertkent/hookgate/internal/testutil"
)

// instance is one gateway with its webhook and admin listeners. Once dead it drops every request without an
// answer, like a process that is gone, and its health check fails.
type instance struct {
	id    string
	g     *Gateway
	front *httptest.Server
	admin *httptest.Server
	dead  atomic.Bool
	// dieAfter, when it returns true for a request, kills the instance after the request was delivered but
	// before its answer leaves, so the sender never learns the outcome.
	dieAfter func(*http.Request) bool
}

func startInstance(t *testing.T, yaml string, dieAfter func(*http.Request) bool) *instance {
	t.Helper()
	cfg, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	in := &instance{dieAfter: dieAfter}
	in.g = New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	closeGateway(t, in.g)
	in.front = httptest.NewServer(http.HandlerFunc(in.serve))
	t.Cleanup(in.front.Close)
	in.admin = httptest.NewServer(in.g.Admin())
	t.Cleanup(in.admin.Close)
	return in
}

func (in *instance) serve(w http.ResponseWriter, r *http.Request) {
	if in.dead.Load() {
		panic(http.ErrAbortHandler)
	}
	rec := httptest.NewRecorder()
	in.g.ServeHTTP(rec, r)
	if in.dieAfter != nil && in.dieAfter(r) {
		in.dead.Store(true)
	}
	if in.dead.Load() {
		panic(http.ErrAbortHandler)
	}
	for k, v := range rec.Header() {
		w.Header()[k] = v
	}
	w.Header().Set("X-Test-Instance", in.id)
	w.WriteHeader(rec.Code)
	_, _ = w.Write(rec.Body.Bytes())
}

// readyz returns the status /readyz answers, or 0 when the instance is dead or does not answer.
func (in *instance) readyz() int {
	if in.dead.Load() {
		return 0
	}
	resp, err := http.Get(in.admin.URL + "/readyz")
	if err != nil {
		return 0
	}
	resp.Body.Close()
	return resp.StatusCode
}

// metric returns the value of one sample of the instance's /metrics; a missing sample fails the test.
func (in *instance) metric(t *testing.T, sample string) float64 {
	t.Helper()
	resp, err := http.Get(in.admin.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), sample+" "); ok {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				t.Fatal(err)
			}
			return f
		}
	}
	t.Fatalf("metric sample %s missing", sample)
	return 0
}

// balancer spreads requests round robin over the instances whose /readyz answers 200, as a load balancer
// with health checks does. A request that fails in transit gets 502; it is not retried.
type balancer struct {
	instances []*instance
	next      atomic.Uint64
}

func (b *balancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	n := uint64(len(b.instances))
	start := b.next.Add(1)
	for i := range n {
		in := b.instances[(start+i)%n]
		if in.readyz() != http.StatusOK {
			continue
		}
		target := in.front.URL + r.URL.RequestURI()
		req, _ := http.NewRequestWithContext(r.Context(), r.Method, target, bytes.NewReader(body)) //nolint:gosec // G704: targets are this test's own servers.
		req.Header = r.Header.Clone()
		resp, err := http.DefaultTransport.RoundTrip(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		return
	}
	http.Error(w, "no ready instance", http.StatusServiceUnavailable)
}

// sender posts events and retries every answer other than 2xx, as webhook senders do.
type sender struct {
	url string
	mu  sync.Mutex
	// attempts counts the requests each event took until it was acknowledged, and by names the instance
	// that acknowledged it.
	attempts map[string]int
	by       map[string]string
}

func newSender(t *testing.T, instances ...*instance) *sender {
	srv := httptest.NewServer(&balancer{instances: instances})
	t.Cleanup(srv.Close)
	return &sender{url: srv.URL, attempts: map[string]int{}, by: map[string]string{}}
}

// send delivers one event and returns the body of the answer that acknowledged it. It may run on any
// goroutine, so it reports failure with t.Errorf.
func (s *sender) send(t *testing.T, id string) string {
	for n := 1; n <= 20; n++ {
		req, _ := http.NewRequest(http.MethodPost, s.url+"/e", strings.NewReader(`{"id":"`+id+`"}`))
		req.Header.Set("X-T", tokenValue)
		req.Header.Set("X-Event-Id", id)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Errorf("event %s: %v", id, err)
			return ""
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode/100 == 2 {
			s.mu.Lock()
			s.attempts[id] = n
			s.by[id] = resp.Header.Get("X-Test-Instance")
			s.mu.Unlock()
			return string(body)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("event %s was never acknowledged", id)
	return ""
}

// sendAll sends count events named prefix-<n> from four workers and returns their IDs.
func (s *sender) sendAll(t *testing.T, prefix string, count int) []string {
	ids := make([]string, count)
	for i := range ids {
		ids[i] = fmt.Sprintf("%s-%d", prefix, i)
	}
	next := make(chan string)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for id := range next {
				s.send(t, id)
			}
		})
	}
	for _, id := range ids {
		next <- id
	}
	close(next)
	wg.Wait()
	return ids
}

// destination is where a fleet delivers: an HTTP upstream or a JetStream stream.
type destination struct {
	name   string
	output string // the source's output block
	stored func(t *testing.T) map[string]int
}

func httpDestination(t *testing.T) destination {
	up := &captured{}
	srv := httptest.NewServer(up.handler(http.StatusOK))
	t.Cleanup(srv.Close)
	return destination{
		name:   "http",
		output: "      http: {urls: [" + srv.URL + "], pass_headers: [X-Event-Id]}\n",
		stored: func(*testing.T) map[string]int {
			up.mu.Lock()
			defer up.mu.Unlock()
			ids := map[string]int{}
			for _, r := range up.reqs {
				ids[r.Header.Get("X-Event-Id")]++
			}
			return ids
		},
	}
}

func jetStreamDestination(t *testing.T) destination {
	s := testutil.RunServer(t)
	_, js := testutil.Connect(t, s.ClientURL())
	testutil.CreateStream(t, js, jetstream.StreamConfig{Name: "EVENTS", Subjects: []string{"webhooks.events.>"}, Discard: jetstream.DiscardNew, Duplicates: time.Minute})
	return destination{
		name:   "jetstream",
		output: jetStreamOutputBlock(s.ClientURL()),
		stored: func(t *testing.T) map[string]int { return storedIDs(t, js) },
	}
}

// jetStreamOutputBlock is a source output publishing through connection ev, plus that connection to urls.
func jetStreamOutputBlock(urls ...string) string {
	return `      jetstream:
        connection: ev
        stream: EVENTS
        message_id: {header: X-Event-Id, unauthenticated: true}
connections:
  ev:
    nats: {urls: ["` + strings.Join(urls, `", "`) + `"], none: true}
`
}

// fleetConfig is a configuration with one source, /e, delivering to output.
func fleetConfig(server, output string) string {
	return server + `
sources:
  - name: src
    path: /e
    max_body_bytes: 65536
    checks: [{token: {header: X-T, secrets: [{env: T}]}}]
    output:
` + output
}

func startFleet(t *testing.T, n int, yaml string, dieAfter func(*http.Request) bool) []*instance {
	t.Helper()
	t.Setenv("T", tokenValue)
	fleet := make([]*instance, n)
	for i := range fleet {
		fleet[i] = startInstance(t, yaml, dieAfter)
		fleet[i].id = strconv.Itoa(i)
		waitReady(t, fleet[i].g)
	}
	return fleet
}

// One instance or several behind a balancer deliver every event exactly once, and the per-instance request
// counters add up to the fleet's total.
func TestFleetDeliversEveryEvent(t *testing.T) {
	for _, dest := range []func(*testing.T) destination{httpDestination, jetStreamDestination} {
		for _, n := range []int{1, 3} {
			d := dest(t)
			t.Run(fmt.Sprintf("%s/%d", d.name, n), func(t *testing.T) {
				fleet := startFleet(t, n, fleetConfig("", d.output), nil)
				ids := newSender(t, fleet...).sendAll(t, "ev", 30)
				stored := d.stored(t)
				for _, id := range ids {
					if stored[id] != 1 {
						t.Errorf("event %s stored %d times", id, stored[id])
					}
				}
				checkRequestCounts(t, fleet, d.name, len(ids))
			})
		}
	}
}

// checkRequestCounts checks that every instance served requests and that their counters add up to want.
func checkRequestCounts(t *testing.T, fleet []*instance, dest string, want int) {
	t.Helper()
	outcome := map[string]string{"http": "forwarded", "jetstream": "published"}[dest]
	var total float64
	for i, in := range fleet {
		v := in.metric(t, `hookgate_requests_total{source="src",outcome="`+outcome+`"}`)
		if v == 0 {
			t.Errorf("instance %d served nothing", i)
		}
		total += v
	}
	if total != float64(want) {
		t.Errorf("instances counted %v requests, want %d", total, want)
	}
}

// An instance that dies after delivering an event but before answering loses no acknowledged event: the
// sender's retry reaches another instance. An HTTP upstream receives that event twice; a stream keeps one
// copy and the retry is answered as a duplicate.
func TestInstanceLostMidDelivery(t *testing.T) {
	for _, dest := range []func(*testing.T) destination{httpDestination, jetStreamDestination} {
		d := dest(t)
		t.Run(d.name, func(t *testing.T) {
			var killed atomic.Bool
			dieAfter := func(r *http.Request) bool {
				return r.Header.Get("X-Event-Id") == "victim" && killed.CompareAndSwap(false, true)
			}
			fleet := startFleet(t, 3, fleetConfig("", d.output), dieAfter)
			s := newSender(t, fleet...)
			var ids []string
			var wg sync.WaitGroup
			wg.Go(func() { ids = s.sendAll(t, "load", 60) })
			answer := s.send(t, "victim")
			wg.Wait()

			checkVictimRetried(t, fleet, s)
			stored := d.stored(t)
			for _, id := range ids {
				if stored[id] < 1 || (d.name == "jetstream" && stored[id] != 1) {
					t.Errorf("acknowledged event %s stored %d times", id, stored[id])
				}
			}
			checkVictimStored(t, d.name, stored["victim"], answer)
		})
	}
}

// checkVictimRetried checks that exactly one instance died and that the victim was acknowledged by a live one on
// its second attempt.
func checkVictimRetried(t *testing.T, fleet []*instance, s *sender) {
	t.Helper()
	var dead []string
	for _, in := range fleet {
		if in.dead.Load() {
			dead = append(dead, in.id)
		}
	}
	if len(dead) != 1 || s.attempts["victim"] != 2 || s.by["victim"] == dead[0] {
		t.Fatalf("dead instances %v; victim acknowledged by instance %s after %d attempts, want a live one after 2",
			dead, s.by["victim"], s.attempts["victim"])
	}
}

// checkVictimStored checks the victim's copies: two at an HTTP upstream, one in a stream with the retry answered
// as a duplicate.
func checkVictimStored(t *testing.T, dest string, copies int, answer string) {
	t.Helper()
	switch dest {
	case "http":
		if copies != 2 {
			t.Errorf("upstream received the victim %d times, want 2", copies)
		}
	case "jetstream":
		if copies != 1 || !strings.Contains(answer, `"duplicate":true`) {
			t.Errorf("stream holds the victim %d times; retry answered %q", copies, answer)
		}
	}
}

// Configuration faults fail their own source, never readiness, even with require_output_connections: a source
// without its secret and a source whose stream is missing answer 503 while /readyz stays 200 and the other
// source serves.
func TestInstanceWithUnreadySource(t *testing.T) {
	s := testutil.RunServer(t)
	up := &captured{}
	srv := httptest.NewServer(up.handler(http.StatusOK))
	t.Cleanup(srv.Close)
	t.Setenv("T", tokenValue)
	in := startInstance(t, `
readiness: {require_output_connections: true, disconnect_grace: 0s}
connections:
  ev:
    nats: {urls: ["`+s.ClientURL()+`"], none: true}
sources:
  - name: ok
    path: /ok
    checks: [{token: {header: X-T, secrets: [{env: T}]}}]
    output: {http: {urls: [`+srv.URL+`]}}
  - name: nosecret
    path: /nosecret
    checks: [{token: {header: X-T, secrets: [{env: MISSING_FOR_TEST}]}}]
    output: {http: {urls: [`+srv.URL+`]}}
  - name: nostream
    path: /nostream
    max_body_bytes: 65536
    checks: [{token: {header: X-T, secrets: [{env: T}]}}]
    output: {jetstream: {connection: ev, stream: MISSING}}
`, nil)
	nostream := in.g.routes["/nostream"].any
	testutil.Eventually(t, 10*time.Second, func() bool {
		err := nostream.out.Ready()
		return err != nil && strings.Contains(err.Error(), "stream MISSING not found")
	}, "missing stream not reported")

	if code := in.readyz(); code != http.StatusOK {
		t.Errorf("readyz %d", code)
	}
	for path, want := range map[string]int{"/ok": 200, "/nosecret": 503, "/nostream": 503} {
		if code := post(in.g, path, "{}", map[string]string{"X-T": tokenValue}).Code; code != want {
			t.Errorf("%s answered %d, want %d", path, code, want)
		}
	}
	if v := in.metric(t, "hookgate_ready"); v != 1 {
		t.Errorf("hookgate_ready %v", v)
	}
}

// With require_output_connections, /readyz fails once an output connection has been down longer than
// disconnect_grace, so a balancer routes around the instance; a shorter drop leaves it ready, and it is ready
// again once the connection is back.
func TestReadinessRequiresOutputConnections(t *testing.T) {
	const grace = time.Second
	s := testutil.RunServer(t)
	_, js := testutil.Connect(t, s.ClientURL())
	testutil.CreateStream(t, js, jetstream.StreamConfig{Name: "EVENTS", Subjects: []string{"webhooks.events.>"}, Discard: jetstream.DiscardNew})
	// Two routes to the server, as with a cluster: the client retries the other at once and waits its
	// reconnect delay only after trying every URL.
	target := strings.TrimPrefix(s.ClientURL(), "nats://")
	proxies := []*testutil.Proxy{testutil.StartProxy(t, target), testutil.StartProxy(t, target)}
	each := func(f func(*testutil.Proxy)) {
		for _, p := range proxies {
			f(p)
		}
	}
	t.Setenv("T", tokenValue)
	readiness := "readiness: {require_output_connections: true, disconnect_grace: " + grace.String() + "}"
	cut := startInstance(t, fleetConfig(readiness, jetStreamOutputBlock("nats://"+proxies[0].Addr(), "nats://"+proxies[1].Addr())), nil)
	healthy := startInstance(t, fleetConfig(readiness, jetStreamOutputBlock(s.ClientURL())), nil)
	waitReady(t, cut.g)
	waitReady(t, healthy.g)
	conn := cut.g.conns["ev"]

	// Probe readiness continuously across the whole flap and well past the grace.
	before := conn.Stats().Reconnects
	probes, failures := probeReadiness(cut, func() {
		each((*testutil.Proxy).DropConnections)
		time.Sleep(2 * grace)
	})
	if failures > 0 {
		t.Fatalf("%d of %d readiness probes failed during a drop shorter than the grace", failures, probes)
	}
	if conn.Stats().Reconnects == before || !conn.IsConnected() {
		t.Fatal("connection did not drop and come back")
	}

	// Nothing can observe the outage before the cut starts, so the grace is measured from there.
	lost := time.Now()
	each(func(p *testutil.Proxy) { p.SetDown(true) })
	testutil.Eventually(t, 10*time.Second, func() bool { return cut.readyz() == http.StatusServiceUnavailable }, "readyz stayed 200")
	if elapsed := time.Since(lost); elapsed < grace {
		t.Errorf("readyz failed %v after the connection was lost, before the %v grace", elapsed, grace)
	}
	if v := cut.metric(t, "hookgate_ready"); v != 0 {
		t.Errorf("hookgate_ready %v while the connection is lost", v)
	}
	snd := newSender(t, cut, healthy)
	for _, id := range snd.sendAll(t, "routed", 10) {
		if snd.attempts[id] != 1 {
			t.Errorf("event %s took %d attempts", id, snd.attempts[id])
		}
	}

	each(func(p *testutil.Proxy) { p.SetDown(false) })
	testutil.Eventually(t, 10*time.Second, func() bool { return cut.readyz() == http.StatusOK }, "readyz did not recover")
}

// probeReadiness probes in's readiness continuously while during runs and counts the probes and the failures.
func probeReadiness(in *instance, during func()) (probes, failures int64) {
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			probes++
			if in.g.notReady() != "" {
				failures++
			}
		}
	})
	during()
	close(stop)
	wg.Wait()
	return probes, failures
}

// Credentials the server rejects are a configuration fault every instance shares: with
// require_output_connections the instance stays ready, the connection's source answers 503, the rejection is
// reported, and the other source serves. Later failures in the same outage (here the broker becoming
// unreachable) leave it a configuration fault until the connection succeeds.
func TestRejectedCredentialsKeepInstanceReady(t *testing.T) {
	const grace = 500 * time.Millisecond
	allowed, _ := nkeys.CreateUser()
	allowedPub, _ := allowed.PublicKey()
	s := testutil.RunServer(t, func(o *server.Options) { o.Nkeys = []*server.NkeyUser{{Nkey: allowedPub}} })
	proxy := testutil.StartProxy(t, strings.TrimPrefix(s.ClientURL(), "nats://"))
	user, _ := nkeys.CreateUser()
	seed, _ := user.Seed()
	// A JWT with three parts passes the local check; only the server can reject it.
	creds := "-----BEGIN NATS USER JWT-----\neyJ0eXAiOiJKV1QifQ.eyJzdWIiOiJub3QtYS11c2VyIn0.c2lnbmF0dXJl\n------END NATS USER JWT------\n\n" +
		"-----BEGIN USER NKEY SEED-----\n" + string(seed) + "\n------END USER NKEY SEED------\n"
	file := filepath.Join(t.TempDir(), "user.creds")
	if err := os.WriteFile(file, []byte(creds), 0o600); err != nil {
		t.Fatal(err)
	}
	up := &captured{}
	srv := httptest.NewServer(up.handler(http.StatusOK))
	t.Cleanup(srv.Close)
	t.Setenv("T", tokenValue)
	in := startInstance(t, `
readiness: {require_output_connections: true, disconnect_grace: `+grace.String()+`}
connections:
  ev:
    nats: {urls: ["nats://`+proxy.Addr()+`"], creds: {file: `+file+`}}
sources:
  - name: ok
    path: /ok
    checks: [{token: {header: X-T, secrets: [{env: T}]}}]
    output: {http: {urls: [`+srv.URL+`]}}
  - name: events
    path: /events
    max_body_bytes: 65536
    checks: [{token: {header: X-T, secrets: [{env: T}]}}]
    output: {jetstream: {connection: ev, stream: EVENTS}}
`, nil)
	rejected := `hookgate_nats_credentials_rejected{connection="ev"}`
	testutil.Eventually(t, 10*time.Second, func() bool { return in.metric(t, rejected) == 1 }, "rejected credentials not reported")
	for deadline := time.Now().Add(4 * grace); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if reason := in.g.notReady(); reason != "" {
			t.Fatalf("not ready (%s) because of rejected credentials", reason)
		}
	}
	for path, want := range map[string]int{"/ok": 200, "/events": 503} {
		if code := post(in.g, path, "{}", map[string]string{"X-T": tokenValue}).Code; code != want {
			t.Errorf("%s answered %d, want %d", path, code, want)
		}
	}
	if v := in.metric(t, "hookgate_ready"); v != 1 {
		t.Errorf("hookgate_ready %v", v)
	}

	proxy.SetDown(true)
	for deadline := time.Now().Add(6 * grace); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if reason := in.g.notReady(); reason != "" {
			t.Fatalf("not ready (%s) after later failures in a rejected outage", reason)
		}
	}
	if in.metric(t, rejected) != 1 {
		t.Errorf("rejection cleared by a later failure in the same outage")
	}
}

// /readyz and request admission answer at once while a reconnect attempt is stuck in a handshake with a server
// that accepts and never answers.
func TestReadyzDoesNotWaitForReconnect(t *testing.T) {
	s := testutil.RunServer(t)
	_, js := testutil.Connect(t, s.ClientURL())
	testutil.CreateStream(t, js, jetstream.StreamConfig{Name: "EVENTS", Subjects: []string{"webhooks.events.>"}, Discard: jetstream.DiscardNew})
	proxy := testutil.StartProxy(t, strings.TrimPrefix(s.ClientURL(), "nats://"))
	stall := startStallServer(t)
	t.Setenv("T", tokenValue)
	block := jetStreamOutputBlock("nats://"+proxy.Addr(), "nats://"+stall.Addr().String())
	block = strings.Replace(block, "none: true}", "none: true, connect_timeout: 2s}", 1)
	in := startInstance(t, fleetConfig("readiness: {require_output_connections: true, disconnect_grace: 10s}", block), nil)
	waitReady(t, in.g)
	conn := in.g.conns["ev"]

	// Measure how long the client's own status call waits, to show that a reconnect attempt did stall.
	stalled := measureLongest(func() { conn.IsConnected() })
	proxy.SetDown(true)
	// A request admitted before the loss was sampled runs its output timeout; measure from the sample.
	testutil.Eventually(t, 2*time.Second, func() bool { return !conn.Status().Connected }, "loss not sampled")
	var slowest, slowestRequest time.Duration
	for deadline := time.Now().Add(4 * time.Second); time.Now().Before(deadline); {
		start := time.Now()
		if code := in.readyz(); code != http.StatusOK {
			t.Fatalf("readyz answered %d within the grace", code)
		}
		slowest = max(slowest, time.Since(start))
		start = time.Now()
		if code := sendEvent(in.g, "stalled"); code != http.StatusServiceUnavailable {
			t.Fatalf("request while disconnected answered %d", code)
		}
		slowestRequest = max(slowestRequest, time.Since(start))
	}
	if s := stalled(); s < time.Second {
		t.Fatalf("no reconnect attempt stalled (longest status call %v)", s)
	}
	if slowest > 500*time.Millisecond {
		t.Errorf("readyz took %v while a reconnect attempt stalled", slowest)
	}
	if slowestRequest > 500*time.Millisecond {
		t.Errorf("a request took %v to be refused while a reconnect attempt stalled", slowestRequest)
	}
}

// startStallServer listens on a local port and accepts connections it never answers, holding them until the test
// ends.
func startStallServer(t *testing.T) net.Listener {
	t.Helper()
	stall, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var held sync.WaitGroup
	t.Cleanup(func() { stall.Close(); held.Wait() })
	held.Go(func() {
		var conns []net.Conn
		defer func() {
			for _, c := range conns {
				c.Close()
			}
		}()
		for {
			c, err := stall.Accept()
			if err != nil {
				return
			}
			conns = append(conns, c)
		}
	})
	return stall
}

// measureLongest calls f continuously until the returned function is called, which returns the longest call.
func measureLongest(f func()) func() time.Duration {
	var longest time.Duration
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			start := time.Now()
			f()
			longest = max(longest, time.Since(start))
		}
	})
	return func() time.Duration {
		close(stop)
		wg.Wait()
		return longest
	}
}
