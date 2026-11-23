package relay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mrrobertkent/hookgate/internal/claimcheck"
	"github.com/mrrobertkent/hookgate/internal/config"
	"github.com/mrrobertkent/hookgate/internal/contract"
	"github.com/mrrobertkent/hookgate/internal/testutil"
)

const (
	eventStream = "WEBHOOK_EVENTS"
	deadStream  = "WEBHOOK_DEAD"
	claimBucket = "WEBHOOK_CLAIMS"
	eventSubj   = "webhooks.events.resend"
	helperArg   = "relay-test-handler"
)

// TestMain doubles as the command handler used by the tests: run with helperArg it acts as a handler.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == helperArg {
		os.Exit(helper(os.Args[2:]))
	}
	os.Exit(m.Run())
}

// helperBehaviours are the test handler's modes, selected by its first argument. "-until N" variants behave so
// on attempts before N and exit 0 from attempt N on.
var helperBehaviours = map[string]func(args []string, stdin []byte, attempt int) int{
	"exit": func(a []string, _ []byte, _ int) int { return atoi(a[0]) },
	"exit-until": func(a []string, _ []byte, n int) int {
		return until(n, a[0], func() int { return atoi(a[1]) })
	},
	"record": func(a []string, stdin []byte, _ int) int {
		writeRecord(a[0], stdin)
		if len(a) > 1 {
			return atoi(a[1])
		}
		return 0
	},
	"record-until": func(a []string, stdin []byte, n int) int {
		writeRecord(a[0], stdin)
		return until(n, a[1], func() int { return atoi(a[2]) })
	},
	"kill-until": func(a []string, _ []byte, n int) int {
		return until(n, a[0], func() int { _ = syscall.Kill(os.Getpid(), syscall.SIGKILL); select {} })
	},
	"sleep": func(a []string, stdin []byte, _ int) int {
		if len(a) > 1 {
			writeRecord(a[1], stdin)
		}
		sleepFor(a[0])
		return 0
	},
	"sleep-until": func(a []string, _ []byte, n int) int {
		return until(n, a[0], func() int { sleepFor(a[1]); return 0 })
	},
	"spawn":      func(a []string, _ []byte, _ int) int { spawnChild(a[0]); time.Sleep(time.Hour); return 0 },
	"spawn-exit": func(a []string, _ []byte, _ int) int { spawnChild(a[0]); return 0 },
	"stderr": func(a []string, _ []byte, _ int) int {
		fmt.Fprintln(os.Stderr, "earlier line")
		fmt.Fprintln(os.Stderr, a[1])
		return atoi(a[0])
	},
}

func helper(args []string) int {
	if args[0] == "kill" {
		_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
		select {}
	}
	stdin, _ := io.ReadAll(os.Stdin)
	attempt, _ := strconv.Atoi(os.Getenv("HOOKGATE_ATTEMPT"))
	return helperBehaviours[args[0]](args[1:], stdin, attempt)
}

func until(attempt int, n string, before func() int) int {
	if attempt < atoi(n) {
		return before()
	}
	return 0
}

func sleepFor(d string) {
	dur, _ := time.ParseDuration(d)
	time.Sleep(dur)
}

// spawnChild starts a child that shares the handler's stderr and outlives it, and writes its pid to dir.
func spawnChild(dir string) {
	self, _ := os.Executable()
	child := exec.Command(self, helperArg, "sleep", "1h") //nolint:gosec // G204: re-executes the test binary.
	child.Stderr = os.Stderr
	_ = child.Start()
	_ = os.WriteFile(filepath.Join(dir, "child.pid"), //nolint:gosec // G703: the test passes a temporary directory.
		[]byte(strconv.Itoa(child.Process.Pid)), 0o600)
}

func atoi(s string) int { n, _ := strconv.Atoi(s); return n }

// record is what the helper handler saw.
type record struct {
	Stdin []byte            `json:"stdin"`
	Env   map[string]string `json:"env"`
	At    time.Time         `json:"at"`
}

func writeRecord(dir string, stdin []byte) {
	env := map[string]string{}
	for _, kv := range os.Environ() {
		k, v, _ := strings.Cut(kv, "=")
		env[k] = v
	}
	b, _ := json.Marshal(record{Stdin: stdin, Env: env, At: time.Now()})
	name := os.Getenv("HOOKGATE_STREAM_SEQ") + "-" + os.Getenv("HOOKGATE_ATTEMPT") + ".json"
	_ = os.WriteFile(filepath.Join(dir, name), b, 0o600) //nolint:gosec // G703: the test passes a temporary directory.
}

func readRecords(t *testing.T, dir string) []record {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	var out []record
	for _, f := range files {
		b, err := os.ReadFile(f) //nolint:gosec // G304: files in a test temporary directory.
		if err != nil {
			t.Fatal(err)
		}
		var r record
		if err := json.Unmarshal(b, &r); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

func helperPath(t testing.TB) string {
	t.Helper()
	p, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func helperHandler(t testing.TB, args ...string) Handler {
	return Handler{Command: &CommandHandler{Path: helperPath(t), Args: append([]string{helperArg}, args...), Timeout: 20 * time.Second}}
}

// env is an embedded NATS server with the event stream, dead-letter stream and claim bucket.
type env struct {
	srv *server.Server
	nc  *nats.Conn
	js  jetstream.JetStream
}

func newEnv(t *testing.T) *env {
	t.Helper()
	return setupEnv(t, testutil.RunServer(t))
}

func setupEnv(t *testing.T, srv *server.Server, opts ...nats.Option) *env {
	t.Helper()
	nc, err := nats.Connect(srv.ClientURL(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	testutil.CreateStream(t, js, jetstream.StreamConfig{
		Name: eventStream, Subjects: []string{"webhooks.events.>", "webhooks.replay.>"}, Duplicates: time.Minute,
	})
	testutil.CreateStream(t, js, jetstream.StreamConfig{Name: deadStream, Subjects: []string{"webhooks.dead.>"}, AllowDirect: true, Discard: jetstream.DiscardNew})
	testutil.CreateBucket(t, js, jetstream.ObjectStoreConfig{Bucket: claimBucket})
	return &env{srv: srv, nc: nc, js: js}
}

func (e *env) consumer(t *testing.T, name string, modify ...func(*jetstream.ConsumerConfig)) {
	t.Helper()
	cfg := jetstream.ConsumerConfig{
		Durable:        name,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        3 * time.Second,
		MaxDeliver:     -1,
		FilterSubjects: []string{eventSubj, "webhooks.replay." + name},
	}
	for _, m := range modify {
		m(&cfg)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := e.js.CreateOrUpdateConsumer(ctx, eventStream, cfg); err != nil {
		t.Fatal(err)
	}
}

// config is a valid relay configuration for consumer with fast timings.
func (e *env) config(t *testing.T, consumer string, h Handler) *Config {
	t.Helper()
	c := &Config{
		AdminListen:      "127.0.0.1:0",
		Connection:       Connection{NATS: config.NATSConnection{URLs: []string{e.srv.ClientURL()}, None: true}},
		Stream:           eventStream,
		Consumer:         consumer,
		FilterSubjects:   []string{eventSubj, "webhooks.replay." + consumer},
		PassHeaders:      []string{"content-type", "webhook-*"},
		MaxAttempts:      5,
		Backoff:          []time.Duration{100 * time.Millisecond},
		ClaimCheck:       &ClaimCheck{ObjectStore: config.ObjectStoreRef{Bucket: claimBucket}},
		DeadLetter:       DeadLetter{Stream: deadStream},
		DrainTimeout:     5 * time.Second,
		ShutdownNakDelay: 100 * time.Millisecond,
		Handler:          h,
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
	return c
}

// running is a relay started by start.
type running struct {
	*Relay
	stop func()
}

// start runs a relay until the test ends or stop is called; waitReady waits until it is ready.
func start(t *testing.T, cfg *Config, waitReady bool) *running {
	t.Helper()
	r, err := New(cfg, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				t.Errorf("run: %v", err)
			}
		})
	}
	t.Cleanup(stop)
	if waitReady {
		testutil.Eventually(t, 10*time.Second, func() bool { return r.notReady() == nil }, "relay not ready")
	}
	return &running{Relay: r, stop: stop}
}

// pub describes a published test event.
type pub struct {
	body        []byte
	eventID     string
	subject     string
	claim       bool              // store the body in the claim bucket
	claimKey    string            // claim key header without storing anything
	claimBucket string            // bucket header value; default claimBucket
	sha         string            // overrides the SHA-256 header
	omit        string            // header to leave out
	extra       map[string]string // more headers
}

func (e *env) publish(t *testing.T, p pub) uint64 {
	t.Helper()
	if p.eventID == "" {
		p.eventID = "msg_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	if p.subject == "" {
		p.subject = eventSubj
	}
	sum := sha256.Sum256(p.body)
	h := nats.Header{}
	h.Set(contract.HeaderContract, contract.Version)
	h.Set(contract.HeaderSource, "resend")
	h.Set(contract.HeaderEventID, p.eventID)
	h.Set(contract.HeaderEventIDAuthenticated, "true")
	h.Set(contract.HeaderSignedAt, "2026-11-23T10:00:00Z")
	h.Set(contract.HeaderReceivedAt, "2026-11-23T10:00:00.123Z")
	h.Set(contract.HeaderBodySHA256, hex.EncodeToString(sum[:]))
	h.Set(contract.HeaderBodySize, strconv.Itoa(len(p.body)))
	h["content-type"] = []string{"application/json"}
	h["webhook-id"] = []string{p.eventID}
	h["x-not-passed"] = []string{"kept for dead letters"}
	for k, v := range p.extra {
		h[k] = []string{v}
	}
	data := p.body
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	switch {
	case p.claim:
		store := claimcheck.NewObjectStore(e.js, claimBucket, claimcheck.ObjectStoreOptions{ReuseWithin: time.Hour})
		ref, err := store.Put(ctx, contract.ClaimKey("resend", p.eventID, hex.EncodeToString(sum[:])), p.body, hex.EncodeToString(sum[:]))
		if err != nil {
			t.Fatal(err)
		}
		p.claimKey, data = ref.Key, nil
	case p.claimKey != "":
		data = nil
	}
	if p.claimKey != "" {
		if p.claimBucket == "" {
			p.claimBucket = claimBucket
		}
		h.Set(contract.HeaderClaimBucket, p.claimBucket)
		h.Set(contract.HeaderClaimKey, p.claimKey)
	}
	if p.sha != "" {
		h.Set(contract.HeaderBodySHA256, p.sha)
	}
	if p.omit != "" {
		h.Del(p.omit)
	}
	ack, err := e.js.PublishMsg(ctx, &nats.Msg{Subject: p.subject, Header: h, Data: data},
		jetstream.WithExpectStream(eventStream), jetstream.WithMsgID(contract.MessageID("resend", p.eventID)))
	if err != nil {
		t.Fatal(err)
	}
	return ack.Sequence
}

func (e *env) consumerInfo(t *testing.T, name string) *jetstream.ConsumerInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := e.js.Consumer(ctx, eventStream, name)
	if err != nil {
		t.Fatal(err)
	}
	info, err := c.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return info
}

// waitSettled waits until consumer has acknowledged or terminated everything up to seq.
func (e *env) waitSettled(t *testing.T, name string, seq uint64) {
	t.Helper()
	testutil.Eventually(t, 20*time.Second, func() bool {
		info := e.consumerInfo(t, name)
		return info.AckFloor.Stream >= seq && info.NumAckPending == 0
	}, "consumer "+name+" did not settle")
}

func (e *env) deadRecords(t *testing.T) []*jetstream.RawStreamMsg {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := e.js.Stream(ctx, deadStream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var out []*jetstream.RawStreamMsg
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq && info.State.Msgs > 0; seq++ {
		m, err := s.GetMsg(ctx, seq)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, m)
	}
	return out
}

func (e *env) waitDead(t *testing.T, n int) []*jetstream.RawStreamMsg {
	t.Helper()
	var recs []*jetstream.RawStreamMsg
	testutil.Eventually(t, 20*time.Second, func() bool { recs = e.deadRecords(t); return len(recs) >= n },
		fmt.Sprintf("want %d dead letters", n))
	return recs
}

// metric returns the value of one series, or -1 when absent.
func metric(r *Relay, series string) float64 {
	var b bytes.Buffer
	r.m.reg.Write(&b)
	for line := range strings.SplitSeq(b.String(), "\n") {
		if v, ok := strings.CutPrefix(line, series+" "); ok {
			f, _ := strconv.ParseFloat(v, 64)
			return f
		}
	}
	return -1
}

func outcome(name string) string { return `hookgate_relay_deliveries_total{outcome="` + name + `"}` }

func (e *env) streamMsg(t *testing.T, seq uint64) *jetstream.RawStreamMsg {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := e.js.Stream(ctx, eventStream)
	if err != nil {
		t.Fatal(err)
	}
	m, err := s.GetMsg(ctx, seq)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func testLogger(t *testing.T) *slog.Logger { return slog.New(slog.NewTextHandler(t.Output(), nil)) }

// fillDeadStream limits the dead-letter stream to one message and stores one, so dead-letter publishes fail.
func (e *env) fillDeadStream(t *testing.T) jetstream.StreamConfig {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := jetstream.StreamConfig{Name: deadStream, Subjects: []string{"webhooks.dead.>"}, AllowDirect: true, MaxMsgs: 1, Discard: jetstream.DiscardNew}
	if _, err := e.js.UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := e.js.Publish(ctx, "webhooks.dead.filler", nil); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func (e *env) unfillDeadStream(t *testing.T, cfg jetstream.StreamConfig) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg.MaxMsgs = -1
	if _, err := e.js.UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
}

// capEventStream limits the event stream's message size, as a deployment that relies on claim checks does.
func (e *env) capEventStream(t *testing.T, size int) {
	t.Helper()
	if size <= 0 || size > math.MaxInt32 {
		t.Fatalf("size %d", size)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := e.js.Stream(ctx, eventStream)
	if err != nil {
		t.Fatal(err)
	}
	cfg := s.CachedInfo().Config
	cfg.MaxMsgSize = int32(size) //nolint:gosec // G115: bounded by the check above.
	if _, err := e.js.UpdateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
}

// deadID is the dead-letter message ID the relay uses for consumer's event at seq.
func (e *env) deadID(t *testing.T, consumer string, seq uint64) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := e.js.Stream(ctx, eventStream)
	if err != nil {
		t.Fatal(err)
	}
	return "dead:" + eventStream + ":" + strconv.FormatInt(s.CachedInfo().Created.UnixNano(), 10) + ":" + consumer + ":" + strconv.FormatUint(seq, 10)
}
