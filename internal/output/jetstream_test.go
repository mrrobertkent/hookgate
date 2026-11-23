package output

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mrrobertkent/hookgate/internal/claimcheck"
	"github.com/mrrobertkent/hookgate/internal/config"
	"github.com/mrrobertkent/hookgate/internal/contract"
	"github.com/mrrobertkent/hookgate/internal/metrics"
	"github.com/mrrobertkent/hookgate/internal/natsconn"
	"github.com/mrrobertkent/hookgate/internal/testutil"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func testMetrics() *Metrics {
	r := metrics.NewRegistry()
	return &Metrics{
		UpstreamAttempts: r.Counter("u", "u", "source", "result"),
		PublishAttempts:  r.Counter("p", "p", "source", "result"),
		PublishDuration:  r.Histogram("d", "d", metrics.DurationBuckets, "source"),
		ClaimPuts:        r.Counter("c", "c", "source", "result"),
	}
}

// sourceConfig parses a one-source configuration whose output.jetstream block is jsYAML (indented 8 spaces)
// and whose connection authenticates with auth.
func sourceConfig(t *testing.T, urls, auth, maxBody, jsYAML string) config.Source {
	t.Helper()
	t.Setenv("TEST_TOKEN", "token-value-0123456")
	t.Setenv("TEST_SET_HEADER", "set-value")
	cfg, err := config.Parse([]byte(`
connections:
  ev:
    nats: {urls: [` + urls + `], ` + auth + `}
sources:
  - name: src
    path: /e
    max_body_bytes: ` + maxBody + `
    checks: [{token: {header: X-Token, secrets: [{env: TEST_TOKEN}]}}]
    output:
      jetstream:
        connection: ev
        stream: EVENTS
` + jsYAML))
	if err != nil {
		t.Fatal(err)
	}
	return cfg.Sources[0]
}

func connect(t *testing.T, cfg config.NATSConnection) *natsconn.Conn {
	t.Helper()
	c, err := natsconn.Connect("test", cfg, discard, natsconn.Callbacks{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// build connects and builds src's output, then runs its checks once.
func build(t *testing.T, url string, src config.Source) *jetStreamOutput {
	t.Helper()
	nc := config.NATSConnection{URLs: strings.Split(url, ","), None: true}
	nc.ApplyDefaults("ev")
	c := connect(t, nc)
	return buildOn(t, c, src)
}

func buildOn(t *testing.T, c *natsconn.Conn, src config.Source) *jetStreamOutput {
	t.Helper()
	out, err := Build(src, Deps{Conns: map[string]*natsconn.Conn{"ev": c}, Metrics: testMetrics(), Log: discard})
	if err != nil {
		t.Fatal(err)
	}
	o := out.(*jetStreamOutput)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.WaitConnected(ctx); err == nil {
		checkNow(o)
	}
	return o
}

// checkNow runs the destination checks once, as Run does.
func checkNow(o *jetStreamOutput) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tag := o.nc.Stats().Reconnects
	o.setCheck(o.check(ctx), tag)
}

func event(body string, h http.Header) Event {
	if h == nil {
		h = http.Header{}
	}
	return Event{Source: "src", Header: h, Body: []byte(body), ReceivedAt: time.Now()}
}

func deliver(o *jetStreamOutput, ev Event) Result {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return o.Deliver(ctx, ev)
}

func streamMsgs(t *testing.T, js jetstream.JetStream, stream string) []*jetstream.RawStreamMsg {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := js.Stream(ctx, stream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := st.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var out []*jetstream.RawStreamMsg
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq && info.State.Msgs > 0; seq++ {
		m, err := st.GetMsg(ctx, seq)
		if err != nil {
			continue
		}
		out = append(out, m)
	}
	return out
}

func eventsStream(t *testing.T, js jetstream.JetStream, mutate ...func(*jetstream.StreamConfig)) {
	t.Helper()
	cfg := jetstream.StreamConfig{Name: "EVENTS", Subjects: []string{"webhooks.events.>"}, Duplicates: time.Minute, MaxAge: time.Hour,
		Discard: jetstream.DiscardNew}
	for _, m := range mutate {
		m(&cfg)
	}
	testutil.CreateStream(t, js, cfg)
}

const plain = `        pass_headers: [Content-Type, X-Keep-*, X-Token]
        set_headers: {X-Env: {env: TEST_SET_HEADER}}
`

func TestPublishesInlineWithContractHeaders(t *testing.T) {
	s := testutil.RunServer(t)
	_, js := testutil.Connect(t, s.ClientURL())
	eventsStream(t, js)
	o := build(t, s.ClientURL(), sourceConfig(t, s.ClientURL(), "none: true", "1024", plain))
	if err := o.Ready(); err != nil {
		t.Fatal(err)
	}
	h := http.Header{"Content-Type": {"application/json"}, "X-Keep-A": {"1", "2"}, "X-Token": {"token-value-0123456"},
		"X-Drop": {"x"}, "Hookgate-Source": {"forged"}}
	res := deliver(o, event(`{"a":1}`, h))
	if res.Status != 200 || res.Outcome != "published" || string(res.Body) != `{"status":"accepted","duplicate":false}`+"\n" {
		t.Fatalf("result %+v %s", res, res.Body)
	}
	msgs := streamMsgs(t, js, "EVENTS")
	if len(msgs) != 1 {
		t.Fatalf("%d messages", len(msgs))
	}
	m := msgs[0]
	sum := sha256.Sum256([]byte(`{"a":1}`))
	want := map[string]string{
		"content-type":            "application/json",
		"X-Env":                   "set-value",
		contract.HeaderContract:   "1",
		contract.HeaderSource:     "src",
		contract.HeaderBodySHA256: hex.EncodeToString(sum[:]),
		contract.HeaderBodySize:   "7",
	}
	for k, v := range want {
		if got := m.Header[k]; len(got) != 1 || got[0] != v {
			t.Errorf("header %s = %q, want %q", k, got, v)
		}
	}
	if got := m.Header["x-keep-a"]; len(got) != 2 {
		t.Errorf("x-keep-a %q", got)
	}
	for _, absent := range []string{"x-token", "X-Token", "x-drop", contract.HeaderEventID, contract.HeaderSignedAt, contract.HeaderMsgID, contract.HeaderClaimKey} {
		if _, ok := m.Header[absent]; ok {
			t.Errorf("header %s present", absent)
		}
	}
	if _, err := time.Parse(time.RFC3339, m.Header.Get(contract.HeaderReceivedAt)); err != nil || m.Subject != "webhooks.events.src" {
		t.Errorf("received-at %q subject %s", m.Header.Get(contract.HeaderReceivedAt), m.Subject)
	}
	if string(m.Data) != `{"a":1}` {
		t.Errorf("body %q", m.Data)
	}
}

func TestAuthenticatedEventID(t *testing.T) {
	s := testutil.RunServer(t)
	_, js := testutil.Connect(t, s.ClientURL())
	eventsStream(t, js)
	o := build(t, s.ClientURL(), sourceConfig(t, s.ClientURL(), "none: true", "1024", ""))
	ev := event("{}", nil)
	ev.EventID, ev.SignedAt = "msg_1", time.Unix(1_700_000_000, 0)
	if res := deliver(o, ev); res.Outcome != "published" {
		t.Fatalf("%+v", res)
	}
	if res := deliver(o, ev); res.Outcome != "duplicate" || string(res.Body) != `{"status":"accepted","duplicate":true}`+"\n" {
		t.Fatalf("second delivery %+v %s", res, res.Body)
	}
	msgs := streamMsgs(t, js, "EVENTS")
	if len(msgs) != 1 {
		t.Fatalf("%d messages", len(msgs))
	}
	h := msgs[0].Header
	if h.Get(contract.HeaderMsgID) != "src:msg_1" || h.Get(contract.HeaderEventID) != "msg_1" ||
		h.Get(contract.HeaderEventIDAuthenticated) != "true" || h.Get(contract.HeaderSignedAt) != "2023-11-14T22:13:20Z" {
		t.Errorf("headers %v", h)
	}
	ev.EventID = "bad id"
	if res := deliver(o, ev); res.Status != 400 {
		t.Errorf("invalid authenticated id: %d", res.Status)
	}
}

func TestUnauthenticatedEventIDHeader(t *testing.T) {
	s := testutil.RunServer(t)
	_, js := testutil.Connect(t, s.ClientURL())
	eventsStream(t, js)
	o := build(t, s.ClientURL(), sourceConfig(t, s.ClientURL(), "none: true", "1024",
		"        message_id: {header: X-Event-Id, unauthenticated: true}\n"))
	for name, h := range map[string]http.Header{
		"repeated": {"X-Event-Id": {"a", "b"}},
		"empty":    {"X-Event-Id": {""}},
		"charset":  {"X-Event-Id": {"a.b"}},
		"long":     {"X-Event-Id": {strings.Repeat("a", 201)}},
	} {
		if res := deliver(o, event("{}", h)); res.Status != 400 || res.Outcome != "bad_request" {
			t.Errorf("%s: %+v", name, res)
		}
	}
	if res := deliver(o, event("{}", http.Header{"X-Event-Id": {"evt-1"}})); res.Outcome != "published" {
		t.Fatalf("%+v", res)
	}
	if res := deliver(o, event("{}", nil)); res.Outcome != "published" {
		t.Fatalf("no id: %+v", res)
	}
	msgs := streamMsgs(t, js, "EVENTS")
	if len(msgs) != 2 || msgs[0].Header.Get(contract.HeaderEventIDAuthenticated) != "false" ||
		msgs[0].Header.Get(contract.HeaderMsgID) != "src:evt-1" || msgs[1].Header.Get(contract.HeaderMsgID) != "" {
		t.Errorf("messages %d: %v", len(msgs), msgs)
	}
}

func TestMaxHeaderBytes(t *testing.T) {
	s := testutil.RunServer(t)
	_, js := testutil.Connect(t, s.ClientURL())
	eventsStream(t, js)
	o := build(t, s.ClientURL(), sourceConfig(t, s.ClientURL(), "none: true", "1024",
		"        pass_headers: [X-Big]\n        max_header_bytes: 1024\n"))
	if res := deliver(o, event("{}", http.Header{"X-Big": {strings.Repeat("v", 1100)}})); res.Status != 400 {
		t.Errorf("%+v", res)
	}
	if len(streamMsgs(t, js, "EVENTS")) != 0 {
		t.Error("published")
	}
}

func TestBrokerDownAnswers503(t *testing.T) {
	url := "nats://127.0.0.1:" + strconv.Itoa(testutil.FreePort(t))
	o := build(t, url, sourceConfig(t, url, "none: true", "1024", "        attempt_timeout: 100ms\n        timeout: 300ms\n"))
	if !errors.Is(o.Ready(), ErrNotConnected) {
		t.Errorf("Ready = %v", o.Ready())
	}
	start := time.Now()
	if res := deliver(o, event("{}", nil)); res.Status != 503 || res.Outcome != "output_error" || string(res.Body) != "unavailable\n" {
		t.Errorf("%+v", res)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("took %s, over the timeout", d)
	}
}

// A subject bound to another stream fails the publish instead of landing there.
func TestWrongStreamAnswers503(t *testing.T) {
	s := testutil.RunServer(t)
	_, js := testutil.Connect(t, s.ClientURL())
	testutil.CreateStream(t, js, jetstream.StreamConfig{Name: "OTHER", Subjects: []string{"webhooks.events.>"}, Discard: jetstream.DiscardNew})
	eventsStream(t, js, func(c *jetstream.StreamConfig) { c.Subjects = []string{"elsewhere.>"} })
	o := build(t, s.ClientURL(), sourceConfig(t, s.ClientURL(), "none: true", "1024", ""))
	if err := o.Ready(); err != nil {
		t.Fatal(err)
	}
	if res := deliver(o, event("{}", nil)); res.Status != 503 || res.Outcome != "output_error" {
		t.Errorf("%+v", res)
	}
	if n := len(streamMsgs(t, js, "OTHER")) + len(streamMsgs(t, js, "EVENTS")); n != 0 {
		t.Errorf("%d messages stored", n)
	}
}

// maxPayloadJS stands in for a client whose server lowered max_payload after the source's checks passed. A real
// broker cannot produce this: the checks keep inline_max_bytes + max_header_bytes within the client's known
// max_payload, the inline decision uses that same value, and a server that lowers max_payload by reload does
// not tell connected clients, so the client never rejects the publish itself.
type maxPayloadJS struct{ jetstream.JetStream }

func (maxPayloadJS) PublishMsg(context.Context, *nats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return nil, nats.ErrMaxPayload
}

func TestClientMaxPayloadAnswers413(t *testing.T) {
	s := testutil.RunServer(t)
	_, js := testutil.Connect(t, s.ClientURL())
	eventsStream(t, js)
	o := build(t, s.ClientURL(), sourceConfig(t, s.ClientURL(), "none: true", "1024", ""))
	o.js = maxPayloadJS{o.js}
	if res := deliver(o, event("{}", nil)); res.Status != 413 || res.Outcome != "too_large" {
		t.Errorf("%+v", res)
	}
}

const claimed = `        inline_max_bytes: 1024
        claim_check: {object_store: {bucket: CLAIMS}}
        message_id: {header: X-Event-Id, unauthenticated: true}
`

func claimSetup(t *testing.T, bucket jetstream.ObjectStoreConfig) (*server.Server, jetstream.JetStream, *jetStreamOutput) {
	t.Helper()
	s := testutil.RunServer(t)
	_, js := testutil.Connect(t, s.ClientURL())
	eventsStream(t, js)
	bucket.Bucket = "CLAIMS"
	testutil.CreateBucket(t, js, bucket)
	o := build(t, s.ClientURL(), sourceConfig(t, s.ClientURL(), "none: true", "1048576", claimed))
	return s, js, o
}

func bigBody(n int) string {
	var b strings.Builder
	for i := range n {
		b.WriteByte('a' + byte(i%26))
	}
	return b.String()
}

// A body over inline_max_bytes is stored first; the message carries a reference, and a consumer that follows
// it finds the bytes whose SHA-256 the message names.
func TestClaimCheckedBody(t *testing.T) {
	_, js, o := claimSetup(t, jetstream.ObjectStoreConfig{})
	if err := o.Ready(); err != nil {
		t.Fatal(err)
	}
	if res := deliver(o, event("small", nil)); res.Outcome != "published" {
		t.Fatalf("%+v", res)
	}
	body := bigBody(300_000)
	if res := deliver(o, event(body, http.Header{"X-Event-Id": {"evt-1"}})); res.Outcome != "published" {
		t.Fatalf("%+v", res)
	}
	msgs := streamMsgs(t, js, "EVENTS")
	if len(msgs) != 2 || msgs[0].Header.Get(contract.HeaderClaimKey) != "" || string(msgs[0].Data) != "small" {
		t.Fatalf("inline message %v", msgs)
	}
	m := msgs[1]
	sum := sha256.Sum256([]byte(body))
	sha := hex.EncodeToString(sum[:])
	if len(m.Data) != 0 || m.Header.Get(contract.HeaderClaimBucket) != "CLAIMS" ||
		m.Header.Get(contract.HeaderClaimKey) != "src/evt-1/"+sha || m.Header.Get(contract.HeaderBodySize) != "300000" {
		t.Fatalf("claim message %v (%d bytes)", m.Header, len(m.Data))
	}
	consumer := claimcheck.NewObjectStore(js, "CLAIMS", claimcheck.ObjectStoreOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got, err := consumer.Get(ctx, claimcheck.Ref{Bucket: m.Header.Get(contract.HeaderClaimBucket), Key: m.Header.Get(contract.HeaderClaimKey)})
	if err != nil {
		t.Fatal(err)
	}
	gotSum := sha256.Sum256(got)
	if hex.EncodeToString(gotSum[:]) != m.Header.Get(contract.HeaderBodySHA256) {
		t.Error("stored body does not match the published SHA-256")
	}

	// The sender's retry reuses the object and the broker drops the repeat.
	obs, _ := js.ObjectStore(ctx, "CLAIMS")
	before, _ := obs.GetInfo(ctx, "src/evt-1/"+sha)
	if res := deliver(o, event(body, http.Header{"X-Event-Id": {"evt-1"}})); res.Outcome != "duplicate" {
		t.Fatalf("retry %+v", res)
	}
	after, _ := obs.GetInfo(ctx, "src/evt-1/"+sha)
	if before.NUID != after.NUID || !before.ModTime.Equal(after.ModTime) {
		t.Error("retry rewrote the object")
	}
}

// A failed object put answers 503 and publishes nothing.
func TestFailedPutPublishesNothing(t *testing.T) {
	_, js, o := claimSetup(t, jetstream.ObjectStoreConfig{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := js.DeleteObjectStore(ctx, "CLAIMS"); err != nil {
		t.Fatal(err)
	}
	if res := deliver(o, event(bigBody(5000), nil)); res.Status != 503 || res.Outcome != "output_error" {
		t.Errorf("%+v", res)
	}
	if n := len(streamMsgs(t, js, "EVENTS")); n != 0 {
		t.Errorf("%d messages published", n)
	}
}

// A publish that fails after the put leaves the object behind; the bucket TTL removes it.
func TestOrphanExpiresByTTL(t *testing.T) {
	s := testutil.RunServer(t)
	_, js := testutil.Connect(t, s.ClientURL())
	eventsStream(t, js, func(c *jetstream.StreamConfig) { c.MaxAge, c.Duplicates = time.Second, 500*time.Millisecond })
	testutil.CreateBucket(t, js, jetstream.ObjectStoreConfig{Bucket: "CLAIMS", TTL: 3 * time.Second})
	src := sourceConfig(t, s.ClientURL(), "none: true", "1048576", strings.Replace(claimed, "{bucket: CLAIMS}}", "{bucket: CLAIMS}, reuse_within: 1s}", 1)+
		"        attempt_timeout: 200ms\n        timeout: 1s\n")
	// The bucket TTL is far below what the checks require, so the orphan's expiry is observable; Deliver
	// itself does not consult readiness.
	o := build(t, s.ClientURL(), src)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := js.DeleteStream(ctx, "EVENTS"); err != nil {
		t.Fatal(err)
	}
	if res := deliver(o, event(bigBody(5000), nil)); res.Status != 503 {
		t.Fatalf("%+v", res)
	}
	bucket, _ := js.Stream(ctx, "OBJ_CLAIMS")
	info, _ := bucket.Info(ctx)
	if info.State.Msgs == 0 {
		t.Fatal("no orphan was left")
	}
	testutil.Eventually(t, 10*time.Second, func() bool {
		info, err := bucket.Info(ctx)
		return err == nil && info.State.Msgs == 0
	}, "orphan did not expire")
}

type readinessCase struct {
	name    string
	payload int32
	stream  func(*jetstream.StreamConfig) // nil: no event stream
	bucket  func(*jetstream.StreamConfig) // nil: no bucket
	want    string
}

func noChange(*jetstream.StreamConfig) {}

func TestNotReadyChecks(t *testing.T) {
	for _, c := range []readinessCase{
		{"stream missing", 0, nil, noChange, "stream EVENTS not found"},
		{"interest retention", 0, func(c *jetstream.StreamConfig) { c.Retention = jetstream.InterestPolicy }, noChange, "retention"},
		{"stream without acks", 0, func(c *jetstream.StreamConfig) { c.NoAck = true }, noChange, "no_ack"},
		{"stream discard old", 0, func(c *jetstream.StreamConfig) { c.Discard = jetstream.DiscardOld }, noChange, "discard"},
		{"bucket fewer replicas", 0, func(c *jetstream.StreamConfig) { c.Replicas = 1 }, func(c *jetstream.StreamConfig) { c.MaxAge = 27 * time.Hour; c.Storage = jetstream.MemoryStorage }, "durably"},
		{"bucket without acks", 0, noChange, func(c *jetstream.StreamConfig) { c.NoAck = true }, "no_ack"},
		{"server max_payload", 16 << 10, noChange, noChange, "exceed"},
		{"stream max_msg_size", 0, func(c *jetstream.StreamConfig) { c.MaxMsgSize = 20 << 10 }, noChange, "exceed"},
		{"bucket missing", 0, noChange, nil, "bucket not found"},
		{"bucket too small", 0, noChange, func(c *jetstream.StreamConfig) { c.MaxBytes = 1 << 20 }, "max_bytes"},
		{"bucket TTL short", 0, noChange, func(c *jetstream.StreamConfig) { c.MaxAge = 25 * time.Hour }, "TTL"},
		{"bucket chunk limit", 0, noChange, func(c *jetstream.StreamConfig) { c.MaxMsgSize = 64 << 10 }, "chunks"},
		{"bucket discard old", 0, noChange, func(c *jetstream.StreamConfig) { c.Discard = jetstream.DiscardOld }, "discard"},
		{"bucket per-subject limit", 0, noChange, func(c *jetstream.StreamConfig) { c.MaxMsgsPerSubject = 1 }, "max_msgs_per_subject"},
		{"bucket without direct get", 0, noChange, func(c *jetstream.StreamConfig) { c.AllowDirect = false }, "allow_direct"},
		{"bucket interest retention", 0, noChange, func(c *jetstream.StreamConfig) { c.Retention = jetstream.InterestPolicy }, "retention"},
		{"ready", 0, noChange, func(c *jetstream.StreamConfig) { c.MaxAge = 27 * time.Hour }, ""},
	} {
		t.Run(c.name, func(t *testing.T) { checkReadiness(t, c) })
	}
}

func checkReadiness(t *testing.T, c readinessCase) {
	s := testutil.RunServer(t, func(o *server.Options) {
		if c.payload > 0 {
			o.MaxPayload = c.payload
		}
	})
	_, js := testutil.Connect(t, s.ClientURL())
	if c.stream != nil {
		eventsStream(t, js, c.stream)
	}
	if c.bucket != nil {
		cfg := jetstream.StreamConfig{Name: "OBJ_CLAIMS", Subjects: []string{"$O.CLAIMS.C.>", "$O.CLAIMS.M.>"},
			Discard: jetstream.DiscardNew, AllowRollup: true, AllowDirect: true}
		c.bucket(&cfg)
		testutil.CreateStream(t, js, cfg)
	}
	o := build(t, s.ClientURL(), sourceConfig(t, s.ClientURL(), "none: true", "1048576",
		"        inline_max_bytes: 10240\n        claim_check: {object_store: {bucket: CLAIMS}}\n"))
	err := o.Ready()
	if c.want == "" && err != nil || c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)) {
		t.Errorf("Ready = %v, want %q", err, c.want)
	}
}

// Checks run again on reconnect: a stream created while the source was not ready brings it back.
func TestChecksRerunOnReconnect(t *testing.T) {
	s := testutil.RunServer(t)
	_, js := testutil.Connect(t, s.ClientURL())
	o := build(t, s.ClientURL(), sourceConfig(t, s.ClientURL(), "none: true", "1024", ""))
	if o.Ready() == nil {
		t.Fatal("ready without a stream")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { o.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	eventsStream(t, js)
	o.Reconnected()
	testutil.Eventually(t, 5*time.Second, func() bool { return o.Ready() == nil }, "not ready after the stream appeared")
}

// The documented least-privilege permissions are enough for every publish and claim check, and deny purges
// and deletes.
func TestLeastPrivilegePermissions(t *testing.T) {
	s := testutil.RunServerFromConfig(t, `
jetstream { store_dir: "$STORE" }
accounts {
  APP {
    jetstream: enabled
    users: [
      {user: admin, password: admin}
      {user: hookgate, password: hookgate, permissions: {
        publish: {allow: [
          "webhooks.events.>",
          "$O.CLAIMS.C.>", "$O.CLAIMS.M.>",
          "$JS.API.STREAM.INFO.EVENTS", "$JS.API.STREAM.INFO.OBJ_CLAIMS",
          "$JS.API.DIRECT.GET.OBJ_CLAIMS", "$JS.API.DIRECT.GET.OBJ_CLAIMS.>"
        ]}
        subscribe: {allow: ["_INBOX.hookgate.>"]}
      }}
    ]
  }
}
`)
	url := strings.Replace(s.ClientURL(), "nats://", "nats://admin:admin@", 1)
	_, admin := testutil.Connect(t, url)
	eventsStream(t, admin)
	testutil.CreateBucket(t, admin, jetstream.ObjectStoreConfig{Bucket: "CLAIMS"})

	hgURL := strings.Replace(s.ClientURL(), "nats://", "nats://hookgate:hookgate@", 1)
	nc := config.NATSConnection{URLs: []string{hgURL}, None: true, InboxPrefix: "_INBOX.hookgate"}
	nc.ApplyDefaults("ev")
	c := connect(t, nc)
	o := buildOn(t, c, sourceConfig(t, s.ClientURL(), "none: true", "1048576", claimed))
	if err := o.Ready(); err != nil {
		t.Fatal(err)
	}
	body := bigBody(200_000)
	for i, want := range []string{"published", "duplicate"} {
		if res := deliver(o, event(body, http.Header{"X-Event-Id": {"evt-1"}})); res.Outcome != want {
			t.Fatalf("delivery %d: %+v", i, res)
		}
	}
	if res := deliver(o, event("small", nil)); res.Outcome != "published" {
		t.Fatalf("inline: %+v", res)
	}

	assertCannotRemove(t, c.Conn, admin)
}

// assertCannotRemove checks that nc can neither delete nor purge the claim bucket's stream nor delete one of its
// messages, and that the bucket still holds its objects.
func assertCannotRemove(t *testing.T, nc *nats.Conn, admin jetstream.JetStream) {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := js.Stream(ctx, "OBJ_CLAIMS")
	if err != nil {
		t.Fatal(err)
	}
	for name, remove := range map[string]func(context.Context) error{
		"stream delete":  func(ctx context.Context) error { return js.DeleteStream(ctx, "OBJ_CLAIMS") },
		"purge":          func(ctx context.Context) error { return st.Purge(ctx) },
		"message delete": func(ctx context.Context) error { return st.DeleteMsg(ctx, 1) },
	} {
		rctx, rcancel := context.WithTimeout(ctx, time.Second)
		if remove(rctx) == nil {
			t.Errorf("%s allowed", name)
		}
		rcancel()
	}
	bucket, err := admin.Stream(ctx, "OBJ_CLAIMS")
	if err != nil {
		t.Fatal(err)
	}
	if info, err := bucket.Info(ctx); err != nil || info.State.Msgs == 0 {
		t.Errorf("bucket emptied: %v", err)
	}
}

// A PubAck that arrives after its attempt timed out is lost to the client; the retry within the output's budget
// is deduplicated with an event ID and stored twice without one (at least once).
func TestLostPubAckIsRetried(t *testing.T) {
	s := testutil.RunServer(t)
	_, js := testutil.Connect(t, s.ClientURL())
	eventsStream(t, js)
	proxy := testutil.StartProxy(t, strings.TrimPrefix(s.ClientURL(), "nats://"))
	url := "nats://" + proxy.Addr()
	o := build(t, url, sourceConfig(t, url, "none: true", "1024",
		"        message_id: {header: X-Event-Id, unauthenticated: true}\n        attempt_timeout: 300ms\n        timeout: 3s\n"))
	if err := o.Ready(); err != nil {
		t.Fatal(err)
	}
	ack := []byte(`"stream":"EVENTS"`)

	proxy.HoldNext(ack, 700*time.Millisecond)
	if res := deliver(o, event("{}", http.Header{"X-Event-Id": {"lost-1"}})); res.Status != 200 || res.Outcome != "duplicate" {
		t.Fatalf("with an ID: %+v", res)
	}
	if n := len(streamMsgs(t, js, "EVENTS")); n != 1 {
		t.Fatalf("with an ID: %d messages", n)
	}

	proxy.HoldNext(ack, 700*time.Millisecond)
	if res := deliver(o, event("{}", nil)); res.Status != 200 || res.Outcome != "published" {
		t.Fatalf("without an ID: %+v", res)
	}
	// One message from the first delivery, then the second event once per attempt the hold outlasted.
	if n := len(streamMsgs(t, js, "EVENTS")); n < 3 {
		t.Errorf("without an ID: %d messages, want the event stored again by the retry", n)
	}
}

// After a reconnect the claim is stored again before its reference is published, because the server now
// serving the client may not hold what the previous one stored.
func TestClaimStoredAgainAfterReconnect(t *testing.T) {
	s := testutil.RunServer(t)
	_, admin := testutil.Connect(t, s.ClientURL())
	eventsStream(t, admin)
	obs := testutil.CreateBucket(t, admin, jetstream.ObjectStoreConfig{Bucket: "CLAIMS"})
	proxy := testutil.StartProxy(t, strings.TrimPrefix(s.ClientURL(), "nats://"))
	url := "nats://" + proxy.Addr()
	o := build(t, url, sourceConfig(t, url, "none: true", "1048576", claimed))
	body := bigBody(5000)
	sum := sha256.Sum256([]byte(body))
	sha := hex.EncodeToString(sum[:])
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c := &claim{ctx: ctx, o: o, key: "src/" + sha, body: []byte(body), sha: sha}
	h := nats.Header{}
	if err := c.prepare(h); err != nil {
		t.Fatal(err)
	}
	if err := c.prepare(h); err != nil {
		t.Fatal(err)
	}
	// The object vanishes, as on a replacement server, and the client reconnects.
	if err := obs.Delete(ctx, "src/"+sha); err != nil {
		t.Fatal(err)
	}
	before := o.nc.Stats().Reconnects
	proxy.DropConnections()
	testutil.Eventually(t, 10*time.Second, func() bool { return o.nc.Stats().Reconnects > before && o.nc.IsConnected() }, "no reconnect")
	if err := c.prepare(h); err != nil {
		t.Fatal(err)
	}
	if got, err := obs.GetBytes(ctx, h.Get(contract.HeaderClaimKey)); err != nil || string(got) != body {
		t.Errorf("object after reconnect: %v", err)
	}
}

// A ready source keeps checking, so a stream changed under it stops publishes.
func TestReadySourceRechecks(t *testing.T) {
	s := testutil.RunServer(t)
	_, js := testutil.Connect(t, s.ClientURL())
	eventsStream(t, js)
	o := build(t, s.ClientURL(), sourceConfig(t, s.ClientURL(), "none: true", "1024", ""))
	o.recheckReady = 100 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { o.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	testutil.Eventually(t, 5*time.Second, func() bool { return o.Ready() == nil }, "not ready")
	uctx, ucancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer ucancel()
	if _, err := js.UpdateStream(uctx, jetstream.StreamConfig{Name: "EVENTS", Subjects: []string{"webhooks.events.>"},
		Duplicates: time.Minute, MaxAge: time.Hour, Discard: jetstream.DiscardOld}); err != nil {
		t.Fatal(err)
	}
	testutil.Eventually(t, 5*time.Second, func() bool {
		err := o.Ready()
		return err != nil && strings.Contains(err.Error(), "discard")
	}, "still ready after the stream changed")
}
