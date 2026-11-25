package relay

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mrrobertkent/hookgate/internal/claimcheck"
	"github.com/mrrobertkent/hookgate/internal/contract"
	"github.com/mrrobertkent/hookgate/internal/testutil"
)

func TestDeliverAck(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	dir := t.TempDir()
	r := start(t, e.config(t, "rex", helperHandler(t, "record", dir)), true)
	seq := e.publish(t, pub{body: []byte(`{"type":"email.received"}`), eventID: "msg_ack"})
	e.waitSettled(t, "rex", seq)
	recs := readRecords(t, dir)
	if len(recs) != 1 || string(recs[0].Stdin) != `{"type":"email.received"}` || recs[0].Env["HOOKGATE_EVENT_ID"] != "msg_ack" ||
		recs[0].Env["HOOKGATE_MESSAGE_ID"] != "resend:msg_ack" || recs[0].Env["HOOKGATE_ATTEMPT"] != "1" {
		t.Fatalf("records %+v", recs)
	}
	if !strings.Contains(recs[0].Env["HOOKGATE_HEADERS"], `"webhook-id":["msg_ack"]`) || strings.Contains(recs[0].Env["HOOKGATE_HEADERS"], "x-not-passed") {
		t.Errorf("headers %s", recs[0].Env["HOOKGATE_HEADERS"])
	}
	if metric(r.Relay, outcome(outcomeAcked)) != 1 || len(e.deadRecords(t)) != 0 {
		t.Errorf("acked %v, dead %d", metric(r.Relay, outcome(outcomeAcked)), len(e.deadRecords(t)))
	}
	if v := metric(r.Relay, `hookgate_relay_info{stream="`+eventStream+`",consumer="rex"}`); v != 1 {
		t.Errorf("info %v", v)
	}
}

func TestBackoffThenDeadLetter(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	dir := t.TempDir()
	cfg := e.config(t, "rex", helperHandler(t, "record", dir, "75"))
	cfg.MaxAttempts = 4
	cfg.Backoff = []time.Duration{300 * time.Millisecond, 600 * time.Millisecond}
	r := start(t, cfg, true)
	seq := e.publish(t, pub{body: []byte("{}")})
	recs := e.waitDead(t, 1)
	e.waitSettled(t, "rex", seq)
	if got := recs[0].Header.Get(contract.HeaderDeadReason); got != contract.DeadMaxAttempts {
		t.Errorf("reason %q", got)
	}
	if got := recs[0].Header.Get(contract.HeaderDeadAttempts); got != "4" {
		t.Errorf("attempts %q", got)
	}
	if got := recs[0].Header.Get(contract.HeaderDeadDetail); got != "exit status 75" {
		t.Errorf("detail %q", got)
	}
	runs := readRecords(t, dir)
	if len(runs) != 4 {
		t.Fatalf("%d runs", len(runs))
	}
	slices.SortFunc(runs, func(a, b record) int { return a.At.Compare(b.At) })
	for i, want := range []time.Duration{300 * time.Millisecond, 600 * time.Millisecond, 600 * time.Millisecond} {
		gap := runs[i+1].At.Sub(runs[i].At)
		if gap < want || gap > want+time.Second {
			t.Errorf("gap %d = %s, want about %s", i+1, gap, want)
		}
	}
	if metric(r.Relay, outcome(outcomeRetried)) != 3 || metric(r.Relay, outcome(outcomeDeadLettered)) != 1 ||
		metric(r.Relay, `hookgate_relay_dead_letters_total{reason="max_attempts"}`) != 1 {
		t.Error("metrics")
	}
}

func TestRetryThenAck(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	r := start(t, e.config(t, "rex", helperHandler(t, "exit-until", "3", "69")), true)
	seq := e.publish(t, pub{body: []byte("{}")})
	e.waitSettled(t, "rex", seq)
	if metric(r.Relay, outcome(outcomeRetried)) != 2 || metric(r.Relay, outcome(outcomeAcked)) != 1 || len(e.deadRecords(t)) != 0 {
		t.Error("want two retries, then an ack")
	}
}

func TestDeadLetterContent(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	r := start(t, e.config(t, "rex", helperHandler(t, "stderr", "65", "unsupported event")), true)
	body := []byte(`{"type":"email.bounced","data":{"id":"x"}}`)
	seq := e.publish(t, pub{body: body, eventID: "msg_dead", extra: map[string]string{"svix-id": "msg_dead"}})
	recs := e.waitDead(t, 1)
	e.waitSettled(t, "rex", seq)
	rec := recs[0]
	if string(rec.Data) != string(body) {
		t.Errorf("body %q", rec.Data)
	}
	if rec.Subject != "webhooks.dead.rex" {
		t.Errorf("subject %s", rec.Subject)
	}
	want := map[string]string{
		contract.HeaderContract: "1", contract.HeaderSource: "resend", contract.HeaderEventID: "msg_dead",
		contract.HeaderEventIDAuthenticated: "true", contract.HeaderSignedAt: "2026-11-23T10:00:00Z",
		contract.HeaderReceivedAt: "2026-11-23T10:00:00.123Z", contract.HeaderBodySize: "42",
		contract.HeaderDeadReason: contract.DeadHandlerRejected, contract.HeaderDeadDetail: "exit status 65",
		contract.HeaderDeadAttempts: "1", contract.HeaderOriginalSubject: eventSubj, contract.HeaderOriginalStream: eventStream,
		contract.HeaderOriginalConsumer: "rex", contract.HeaderOriginalMsgID: "resend:msg_dead",
		"content-type": "application/json", "webhook-id": "msg_dead", "svix-id": "msg_dead", "x-not-passed": "kept for dead letters",
		contract.HeaderMsgID: e.deadID(t, "rex", seq), contract.HeaderOriginalSeq: itoa(seq),
		"Nats-Expected-Stream": deadStream,
	}
	for k, v := range want {
		if got := rec.Header.Get(k); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	if _, err := time.Parse(time.RFC3339, rec.Header.Get(contract.HeaderDeadAt)); err != nil {
		t.Errorf("dead-at: %v", err)
	}
	// The direct-get response adds Nats-Subject, Nats-Sequence, Nats-Time-Stamp and Nats-Stream.
	allowed := map[string]bool{contract.HeaderBodySHA256: true, contract.HeaderDeadAt: true,
		"Nats-Subject": true, "Nats-Sequence": true, "Nats-Time-Stamp": true, "Nats-Stream": true}
	for k := range rec.Header {
		if _, ok := want[k]; !ok && !allowed[k] {
			t.Errorf("unexpected header %s", k)
		}
	}
	if metric(r.Relay, outcome(outcomeDeadLettered)) != 1 {
		t.Error("metric")
	}
}

func TestDeadLetterPublishFailureKeepsEvent(t *testing.T) {
	e := newEnv(t)
	dl := e.fillDeadStream(t)
	e.consumer(t, "rex")
	cfg := e.config(t, "rex", helperHandler(t, "exit", "65"))
	r := start(t, cfg, true)
	seq := e.publish(t, pub{body: []byte("{}")})
	testutil.Eventually(t, 10*time.Second, func() bool {
		return metric(r.Relay, "hookgate_relay_dead_letter_publish_failures_total") >= 2
	}, "dead-letter publish did not fail")
	if info := e.consumerInfo(t, "rex"); info.AckFloor.Stream >= seq {
		t.Fatal("event settled although its dead letter was never written")
	}
	e.unfillDeadStream(t, dl)
	recs := e.waitDead(t, 2)
	e.waitSettled(t, "rex", seq)
	if recs[1].Header.Get(contract.HeaderOriginalSeq) != itoa(seq) || metric(r.Relay, outcome(outcomeDeadLetterFailed)) < 2 {
		t.Errorf("dead letter %v", recs[1].Header)
	}
}

func TestClaimCheckResolution(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	dir := t.TempDir()
	start(t, e.config(t, "rex", helperHandler(t, "record", dir)), true)
	body := []byte(strings.Repeat("large body ", 40000))
	seq := e.publish(t, pub{body: body, claim: true, claimBucket: "SOMEONE_ELSES_BUCKET"})
	e.waitSettled(t, "rex", seq)
	recs := readRecords(t, dir)
	if len(recs) != 1 || string(recs[0].Stdin) != string(body) {
		t.Fatalf("handler did not get the claim-checked body")
	}
	if strings.Contains(recs[0].Env["HOOKGATE_HEADERS"], "Claim") {
		t.Errorf("claim headers reached the handler: %s", recs[0].Env["HOOKGATE_HEADERS"])
	}
}

func TestChecksumMismatchNeverReachesHandler(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	dir := t.TempDir()
	r := start(t, e.config(t, "rex", helperHandler(t, "record", dir)), true)
	wrong := strings.Repeat("0", 64)
	e.publish(t, pub{body: []byte("claimed body"), claim: true, sha: wrong, eventID: "claimed"})
	e.publish(t, pub{body: []byte("inline body"), sha: wrong, eventID: "inline"})
	recs := e.waitDead(t, 2)
	for _, rec := range recs {
		checkMismatchRecord(t, rec)
	}
	if len(readRecords(t, dir)) != 0 {
		t.Error("handler ran for a checksum mismatch")
	}
	if metric(r.Relay, `hookgate_relay_dead_letters_total{reason="checksum_mismatch"}`) != 2 {
		t.Error("metric")
	}
}

func TestClaimMissing(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	dir := t.TempDir()
	cfg := e.config(t, "rex", helperHandler(t, "record", dir))
	cfg.MaxAttempts = 2
	r := start(t, cfg, true)
	seq := e.publish(t, pub{body: []byte("gone"), claimKey: "resend/gone/" + strings.Repeat("a", 64)})
	recs := e.waitDead(t, 1)
	e.waitSettled(t, "rex", seq)
	if recs[0].Header.Get(contract.HeaderDeadReason) != contract.DeadClaimMissing || recs[0].Header.Get(contract.HeaderDeadAttempts) != "2" ||
		len(recs[0].Data) != 0 || recs[0].Header.Get(contract.HeaderClaimKey) == "" {
		t.Errorf("record %v %q", recs[0].Header, recs[0].Data)
	}
	if len(readRecords(t, dir)) != 0 || metric(r.Relay, outcome(outcomeRetried)) != 1 {
		t.Error("want one retry and no handler run")
	}
}

func TestInvalidMessage(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	dir := t.TempDir()
	start(t, e.config(t, "rex", helperHandler(t, "record", dir)), true)
	e.publish(t, pub{body: []byte("x"), omit: contract.HeaderSource})
	recs := e.waitDead(t, 1)
	if recs[0].Header.Get(contract.HeaderDeadReason) != contract.DeadInvalidMessage || string(recs[0].Data) != "x" {
		t.Errorf("record %v", recs[0].Header)
	}
	if len(readRecords(t, dir)) != 0 {
		t.Error("handler ran for an invalid message")
	}
}

func TestConfigFaultPauses(t *testing.T) {
	redirect := httptest.NewServer(http.RedirectHandler("http://elsewhere/", http.StatusFound))
	t.Cleanup(redirect.Close)
	refused := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }))
	t.Cleanup(refused.Close)
	forbidden := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) }))
	t.Cleanup(forbidden.Close)
	cases := map[string]func(t *testing.T) Handler{
		"exit 127": func(t *testing.T) Handler { return helperHandler(t, "exit", "127") },
		"exit 126": func(t *testing.T) Handler { return helperHandler(t, "exit", "126") },
		"not found": func(t *testing.T) Handler {
			return Handler{Command: &CommandHandler{Path: "/nonexistent/handler", Timeout: time.Second}}
		},
		"http 302": func(*testing.T) Handler { return Handler{HTTP: &HTTPHandler{URL: redirect.URL, Timeout: time.Second}} },
		"exit 78":  func(t *testing.T) Handler { return helperHandler(t, "exit", "78") },
		"http 401": func(*testing.T) Handler {
			return Handler{HTTP: &HTTPHandler{URL: refused.URL, Timeout: time.Second}}
		},
		"http 403": func(*testing.T) Handler { return Handler{HTTP: &HTTPHandler{URL: forbidden.URL, Timeout: time.Second}} },
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			e.consumer(t, "rex")
			r := start(t, e.config(t, "rex", h(t)), true)
			first := e.publish(t, pub{body: []byte("{}")})
			testutil.Eventually(t, 10*time.Second, func() bool { return r.notReady() != nil }, "relay did not pause")
			rec := httptest.NewRecorder()
			r.Admin().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "paused") {
				t.Errorf("readyz %d %s", rec.Code, rec.Body)
			}
			e.publish(t, pub{body: []byte("{}")})
			time.Sleep(500 * time.Millisecond)
			info := e.consumerInfo(t, "rex")
			if info.AckFloor.Stream >= first || info.Delivered.Stream != first || len(e.deadRecords(t)) != 0 {
				t.Errorf("paused relay settled or fetched: %+v", info)
			}
			if metric(r.Relay, "hookgate_relay_paused") != 1 || metric(r.Relay, outcome(outcomePaused)) != 1 {
				t.Error("metrics")
			}
		})
	}
}

func TestDrainFinishesInFlight(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	dir := t.TempDir()
	cfg := e.config(t, "rex", helperHandler(t, "sleep", "1s", dir))
	cfg.DrainTimeout = 10 * time.Second
	r := start(t, cfg, true)
	seq := e.publish(t, pub{body: []byte("{}")})
	testutil.Eventually(t, 10*time.Second, func() bool { return len(readRecords(t, dir)) == 1 }, "handler did not start")
	r.stop()
	if info := e.consumerInfo(t, "rex"); info.AckFloor.Stream < seq {
		t.Errorf("not acked after drain: %+v", info.AckFloor)
	}
	if metric(r.Relay, outcome(outcomeAcked)) != 1 {
		t.Error("metric")
	}
}

func TestDrainTimeoutNaksAndRedelivers(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	dir := t.TempDir()
	cfg := e.config(t, "rex", helperHandler(t, "sleep", "1m", dir))
	cfg.DrainTimeout = 300 * time.Millisecond
	r := start(t, cfg, true)
	seq := e.publish(t, pub{body: []byte("{}")})
	testutil.Eventually(t, 10*time.Second, func() bool { return len(readRecords(t, dir)) == 1 }, "handler did not start")
	began := time.Now()
	r.stop()
	if d := time.Since(began); d > 5*time.Second {
		t.Errorf("stop took %s", d)
	}
	if metric(r.Relay, outcome(outcomeShutdown)) != 1 {
		t.Error("not naked on shutdown")
	}
	next := t.TempDir()
	start(t, e.config(t, "rex", helperHandler(t, "record", next)), true)
	e.waitSettled(t, "rex", seq)
	recs := readRecords(t, next)
	if len(recs) != 1 || recs[0].Env["HOOKGATE_ATTEMPT"] != "2" {
		t.Errorf("redelivery %+v", recs)
	}
}

func TestTwoRelaysShareConsumer(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	var mu sync.Mutex
	seen := map[string]int{}
	var byRelay [2]atomic.Int64
	handler := func(i int) http.HandlerFunc {
		return func(_ http.ResponseWriter, r *http.Request) {
			time.Sleep(10 * time.Millisecond)
			mu.Lock()
			seen[r.Header.Get(contract.HeaderEventID)]++
			mu.Unlock()
			byRelay[i].Add(1)
		}
	}
	for i := range 2 {
		srv := httptest.NewServer(handler(i))
		t.Cleanup(srv.Close)
		cfg := e.config(t, "rex", Handler{HTTP: &HTTPHandler{URL: srv.URL, Timeout: 5 * time.Second}})
		cfg.Concurrency = 3
		start(t, cfg, true)
	}
	const n = 60
	var last uint64
	for i := range n {
		last = e.publish(t, pub{body: []byte("{}"), eventID: "evt_" + itoa(uint64(i))})
	}
	e.waitSettled(t, "rex", last)
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		t.Errorf("%d distinct events handled, want %d", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Errorf("%s handled %d times", id, c)
		}
	}
	if byRelay[0].Load() == 0 || byRelay[1].Load() == 0 {
		t.Errorf("work not shared: %d / %d", byRelay[0].Load(), byRelay[1].Load())
	}
}

func TestReadinessChecks(t *testing.T) {
	cases := map[string]struct {
		modify func(*jetstream.ConsumerConfig)
		want   string
	}{
		"max_deliver": {func(c *jetstream.ConsumerConfig) { c.MaxDeliver = 5 }, "max_deliver is 5"},
		"filters":     {func(c *jetstream.ConsumerConfig) { c.FilterSubjects = []string{eventSubj} }, "filter subjects"},
		"backoff": {func(c *jetstream.ConsumerConfig) {
			c.BackOff = []time.Duration{time.Second, 2 * time.Second}
			c.AckWait = 0
		}, "backoff is set"},
		"ack policy":   {func(c *jetstream.ConsumerConfig) { c.AckPolicy = jetstream.AckAllPolicy }, "ack_policy"},
		"max_expires":  {func(c *jetstream.ConsumerConfig) { c.MaxRequestExpires = 2 * time.Second }, "max_expires"},
		"max_batch":    {func(c *jetstream.ConsumerConfig) { c.MaxRequestBatch = 1 }, "max_batch"},
		"ack_wait":     {func(c *jetstream.ConsumerConfig) { c.AckWait = 500 * time.Millisecond }, "ack_wait"},
		"headers_only": {func(c *jetstream.ConsumerConfig) { c.HeadersOnly = true }, "headers_only"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			e.consumer(t, "rex", tc.modify)
			cfg := e.config(t, "rex", helperHandler(t, "exit", "0"))
			cfg.Concurrency = 2
			r := start(t, cfg, false)
			testutil.Eventually(t, 10*time.Second, func() bool {
				err := r.notReady()
				return err != nil && strings.Contains(err.Error(), tc.want)
			}, "relay ready with a bad consumer")
			seq := e.publish(t, pub{body: []byte("{}")})
			time.Sleep(300 * time.Millisecond)
			if info := e.consumerInfo(t, "rex"); info.Delivered.Stream >= seq {
				t.Error("not-ready relay fetched")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := e.js.DeleteConsumer(ctx, eventStream, "rex"); err != nil {
				t.Fatal(err)
			}
			e.consumer(t, "rex")
			r.trigger()
			testutil.Eventually(t, 10*time.Second, func() bool { return r.notReady() == nil }, "relay not ready after the fix")
			e.waitSettled(t, "rex", seq)
		})
	}
}

func TestReadinessMissingStreams(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	cfg := e.config(t, "rex", helperHandler(t, "exit", "0"))
	cfg.DeadLetter.Stream = "NO_SUCH_STREAM"
	r := start(t, cfg, false)
	testutil.Eventually(t, 10*time.Second, func() bool {
		err := r.notReady()
		return err != nil && strings.Contains(err.Error(), "NO_SUCH_STREAM")
	}, "relay ready without its dead-letter stream")
}

func itoa(n uint64) string { return strconv.FormatUint(n, 10) }

func TestInProgressKeepsLongHandlerAlive(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex", func(c *jetstream.ConsumerConfig) { c.AckWait = time.Second })
	dir := t.TempDir()
	r := start(t, e.config(t, "rex", helperHandler(t, "sleep", "2500ms", dir)), true)
	seq := e.publish(t, pub{body: []byte("{}")})
	e.waitSettled(t, "rex", seq)
	if n := len(readRecords(t, dir)); n != 1 {
		t.Errorf("handler ran %d times; the message was redelivered while in progress", n)
	}
	if info := e.consumerInfo(t, "rex"); info.NumRedelivered != 0 || metric(r.Relay, outcome(outcomeAcked)) != 1 {
		t.Errorf("redelivered %d", info.NumRedelivered)
	}
}

func checkMismatchRecord(t *testing.T, rec *jetstream.RawStreamMsg) {
	t.Helper()
	if rec.Header.Get(contract.HeaderDeadReason) != contract.DeadChecksumMismatch {
		t.Errorf("reason %q", rec.Header.Get(contract.HeaderDeadReason))
	}
	if rec.Header.Get(contract.HeaderEventID) == "inline" {
		if string(rec.Data) != "inline body" || rec.Header.Get(contract.HeaderClaimKey) != "" {
			t.Errorf("inline record: body %q", rec.Data)
		}
		return
	}
	if len(rec.Data) != 0 || rec.Header.Get(contract.HeaderClaimKey) == "" || rec.Header.Get(contract.HeaderClaimBucket) != claimBucket {
		t.Errorf("claimed record: body %q, headers %v", rec.Data, rec.Header)
	}
}

func TestReadinessDeadLetterRetention(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.js.DeleteStream(ctx, deadStream); err != nil {
		t.Fatal(err)
	}
	testutil.CreateStream(t, e.js, jetstream.StreamConfig{Name: deadStream, Subjects: []string{"webhooks.dead.>"}, Retention: jetstream.InterestPolicy})
	r := start(t, e.config(t, "rex", helperHandler(t, "exit", "65")), false)
	testutil.Eventually(t, 10*time.Second, func() bool {
		err := r.notReady()
		return err != nil && strings.Contains(err.Error(), "want limits")
	}, "relay ready with an interest-retention dead-letter stream")
}

// TestPauseStopsBatch publishes more events than the relay may hold and checks that a configuration fault starts
// no handler beyond those already running.
func TestPauseStopsBatch(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	dir := t.TempDir()
	cfg := e.config(t, "rex", helperHandler(t, "record", dir, "127"))
	cfg.Concurrency = 3
	for range 10 {
		e.publish(t, pub{body: []byte("{}")})
	}
	r := start(t, cfg, false)
	testutil.Eventually(t, 10*time.Second, func() bool { return r.paused() }, "relay did not pause")
	time.Sleep(time.Second)
	if n := len(readRecords(t, dir)); n < 1 || n > cfg.Concurrency {
		t.Errorf("%d handlers ran, want 1-%d", n, cfg.Concurrency)
	}
	if info := e.consumerInfo(t, "rex"); info.AckFloor.Stream != 0 || len(e.deadRecords(t)) != 0 {
		t.Errorf("events settled while paused: %+v", info.AckFloor)
	}
}

// TestFailedDeadLetterRerunsHandler: the last attempt fails while the dead-letter stream is full; the event is
// naked, runs again and, now succeeding, is acknowledged. Nothing is dropped.
func TestFailedDeadLetterRerunsHandler(t *testing.T) {
	e := newEnv(t)
	e.fillDeadStream(t)
	e.consumer(t, "rex")
	dir := t.TempDir()
	cfg := e.config(t, "rex", helperHandler(t, "record-until", dir, "3", "75"))
	cfg.MaxAttempts = 2
	r := start(t, cfg, true)
	seq := e.publish(t, pub{body: []byte("{}")})
	e.waitSettled(t, "rex", seq)
	if n := len(readRecords(t, dir)); n != 3 {
		t.Errorf("handler ran %d times, want 3", n)
	}
	if metric(r.Relay, "hookgate_relay_dead_letter_publish_failures_total") != 1 || metric(r.Relay, outcome(outcomeAcked)) != 1 {
		t.Error("want one failed dead-letter publish, then an ack")
	}
	if n := len(e.deadRecords(t)); n != 1 {
		t.Errorf("%d dead-letter records, want only the filler", n)
	}
}

// TestDeadLetterAlreadyWritten: the relay stopped after the dead-letter PubAck but before terminating. The
// redelivery runs the handler, which now succeeds; the earlier record stays (a replay of it is idempotent).
func TestDeadLetterAlreadyWritten(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	seq := e.publish(t, pub{body: []byte("{}")})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cons, err := e.js.Consumer(ctx, eventStream, "rex")
	if err != nil {
		t.Fatal(err)
	}
	msg, err := cons.Next()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.js.PublishMsg(ctx, &nats.Msg{Subject: "webhooks.dead.rex", Header: nats.Header{contract.HeaderDeadReason: {contract.DeadMaxAttempts}}},
		jetstream.WithMsgID(e.deadID(t, "rex", seq))); err != nil {
		t.Fatal(err)
	}
	_ = msg.Nak()
	dir := t.TempDir()
	cfg := e.config(t, "rex", helperHandler(t, "record", dir))
	cfg.MaxAttempts = 1
	start(t, cfg, true)
	e.waitSettled(t, "rex", seq)
	recs := readRecords(t, dir)
	if len(recs) != 1 || recs[0].Env["HOOKGATE_ATTEMPT"] != "2" || len(e.deadRecords(t)) != 1 {
		t.Errorf("handler runs %d, dead letters %d", len(recs), len(e.deadRecords(t)))
	}
}

// TestNoHandlerAfterPause: once paused, an owner preparing an event naks it without running the handler, and a
// pull about to start is not made.
func TestNoHandlerAfterPause(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	dir := t.TempDir()
	r, err := New(e.config(t, "rex", helperHandler(t, "record", dir)), testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	r.js = e.js
	r.pause("test fault")
	e.publish(t, pub{body: []byte("{}")})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cons, err := e.js.Consumer(ctx, eventStream, "rex")
	if err != nil {
		t.Fatal(err)
	}
	slots := make(chan struct{}, 1)
	slots <- struct{}{}
	if got, ferr := r.fetch(ctx, ctx, cons, 1, slots); got != 0 || ferr != nil {
		t.Fatalf("paused relay pulled %d (%v)", got, ferr)
	}
	if info := e.consumerInfo(t, "rex"); info.Delivered.Stream != 0 {
		t.Fatalf("paused relay pulled: %+v", info.Delivered)
	}
	msg, err := cons.Next()
	if err != nil {
		t.Fatal(err)
	}
	meta, _ := msg.Metadata()
	if p := r.decide(ctx, &message{msg: msg, meta: meta}); p.action != actPause {
		t.Errorf("plan %+v", p)
	}
	if n := len(readRecords(t, dir)); n != 0 {
		t.Errorf("handler ran %d times after the pause", n)
	}
}

func TestReadinessDeadLetterNoAck(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := e.js.UpdateStream(ctx, jetstream.StreamConfig{Name: deadStream, Subjects: []string{"webhooks.dead.>"}, AllowDirect: true, NoAck: true, Discard: jetstream.DiscardNew}); err != nil {
		t.Fatal(err)
	}
	r := start(t, e.config(t, "rex", helperHandler(t, "exit", "65")), false)
	testutil.Eventually(t, 10*time.Second, func() bool {
		err := r.notReady()
		return err != nil && strings.Contains(err.Error(), "no_ack")
	}, "relay ready with a no-ack dead-letter stream")
}

// TestSettlementThroughBroker checks retried and paused outcomes end to end for both handler kinds.
func TestSettlementThroughBroker(t *testing.T) {
	statusOnce := func(code int) func(*testing.T) Handler {
		return func(t *testing.T) Handler {
			var n atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if n.Add(1) == 1 {
					w.WriteHeader(code)
				}
			}))
			t.Cleanup(srv.Close)
			return Handler{HTTP: &HTTPHandler{URL: srv.URL, Timeout: 5 * time.Second}}
		}
	}
	cases := map[string]func(*testing.T) Handler{
		"http 429":     statusOnce(http.StatusTooManyRequests),
		"http 500":     statusOnce(http.StatusInternalServerError),
		"http 503":     statusOnce(http.StatusServiceUnavailable),
		"command kill": func(t *testing.T) Handler { return helperHandler(t, "kill-until", "2") },
		"command 71":   func(t *testing.T) Handler { return helperHandler(t, "exit-until", "2", "71") },
		"command timeout": func(t *testing.T) Handler {
			h := helperHandler(t, "sleep-until", "2", "1m")
			h.Command.Timeout = 2 * time.Second
			return h
		},
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			e.consumer(t, "rex")
			r := start(t, e.config(t, "rex", h(t)), true)
			seq := e.publish(t, pub{body: []byte("{}")})
			e.waitSettled(t, "rex", seq)
			if metric(r.Relay, outcome(outcomeRetried)) != 1 || metric(r.Relay, outcome(outcomeAcked)) != 1 || len(e.deadRecords(t)) != 0 {
				t.Errorf("retried %v, acked %v", metric(r.Relay, outcome(outcomeRetried)), metric(r.Relay, outcome(outcomeAcked)))
			}
		})
	}
}

func TestReadinessClaimTTL(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.js.DeleteObjectStore(ctx, claimBucket); err != nil {
		t.Fatal(err)
	}
	testutil.CreateBucket(t, e.js, jetstream.ObjectStoreConfig{Bucket: claimBucket, TTL: 48 * time.Hour})
	s, err := e.js.Stream(ctx, eventStream)
	if err != nil {
		t.Fatal(err)
	}
	sc := s.CachedInfo().Config
	sc.MaxAge = 24 * time.Hour
	if _, err := e.js.UpdateStream(ctx, sc); err != nil {
		t.Fatal(err)
	}
	r := start(t, e.config(t, "rex", helperHandler(t, "exit", "0")), false)
	testutil.Eventually(t, 10*time.Second, func() bool {
		err := r.notReady()
		return err != nil && strings.Contains(err.Error(), "bucket TTL")
	}, "relay ready with a bucket TTL shorter than max_age + 25h")
	sc.MaxAge = 22 * time.Hour
	if _, err := e.js.UpdateStream(ctx, sc); err != nil {
		t.Fatal(err)
	}
	r.trigger()
	testutil.Eventually(t, 10*time.Second, func() bool { return r.notReady() == nil }, "relay not ready with a sufficient TTL")
}

// pausingStore pauses the relay when asked for an object, then never answers, like a claim read that times out
// while another handler hits a configuration fault.
type pausingStore struct {
	claimcheck.Store
	r *Relay
}

func (s pausingStore) Get(ctx context.Context, _ claimcheck.Ref) ([]byte, error) {
	s.r.pause("fault in another handler")
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestNoDeadLetterWhilePaused: a claim read that fails on the last attempt after a pause is naked, not
// dead-lettered.
func TestNoDeadLetterWhilePaused(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	dir := t.TempDir()
	cfg := e.config(t, "rex", helperHandler(t, "record", dir))
	cfg.MaxAttempts = 1
	cfg.DrainTimeout = 200 * time.Millisecond
	r, err := New(cfg, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	r.js = e.js
	r.claims = pausingStore{r: r}
	seq := e.publish(t, pub{body: []byte("claimed"), claimKey: "resend/x/" + strings.Repeat("a", 64)})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cons, err := e.js.Consumer(ctx, eventStream, "rex")
	if err != nil {
		t.Fatal(err)
	}
	msg, err := cons.Next()
	if err != nil {
		t.Fatal(err)
	}
	r.own(ctx, msg)
	if len(e.deadRecords(t)) != 0 || len(readRecords(t, dir)) != 0 {
		t.Fatal("dead-lettered or handled while paused")
	}
	if metric(r, outcome(outcomePaused)) != 1 {
		t.Error("not naked as paused")
	}
	if info := e.consumerInfo(t, "rex"); info.AckFloor.Stream >= seq {
		t.Errorf("settled while paused: %+v", info.AckFloor)
	}
}

func TestReadinessClaimDurability(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.js.DeleteObjectStore(ctx, claimBucket); err != nil {
		t.Fatal(err)
	}
	testutil.CreateBucket(t, e.js, jetstream.ObjectStoreConfig{Bucket: claimBucket, Storage: jetstream.MemoryStorage})
	r := start(t, e.config(t, "rex", helperHandler(t, "exit", "0")), false)
	testutil.Eventually(t, 10*time.Second, func() bool {
		err := r.notReady()
		return err != nil && strings.Contains(err.Error(), "less durably")
	}, "relay ready with a memory bucket for a file stream")
}

func TestReadinessDeadLetterLimits(t *testing.T) {
	cases := map[string]struct {
		modify func(*jetstream.StreamConfig)
		want   string
	}{
		"discard old":   {func(c *jetstream.StreamConfig) { c.Discard = jetstream.DiscardOld }, "discard old"},
		"per subject":   {func(c *jetstream.StreamConfig) { c.MaxMsgsPerSubject = 1 }, "max_msgs_per_subject"},
		"smaller limit": {func(c *jetstream.StreamConfig) { c.MaxMsgSize = 1024 }, "max_msg_size"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			e.consumer(t, "rex")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cfg := jetstream.StreamConfig{Name: deadStream, Subjects: []string{"webhooks.dead.>"}, AllowDirect: true, Discard: jetstream.DiscardNew}
			tc.modify(&cfg)
			if _, err := e.js.UpdateStream(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			r := start(t, e.config(t, "rex", helperHandler(t, "exit", "65")), false)
			testutil.Eventually(t, 10*time.Second, func() bool {
				err := r.notReady()
				return err != nil && strings.Contains(err.Error(), tc.want)
			}, "relay ready with an unsuitable dead-letter stream")
		})
	}
}

// TestDeadLetterTooLarge: a rejected claim-checked event whose whole body exceeds the dead-letter stream's message
// size is retried, never terminated, and counted as too large.
func TestDeadLetterTooLarge(t *testing.T) {
	e := newEnv(t)
	e.capEventStream(t, 2048)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := e.js.UpdateStream(ctx, jetstream.StreamConfig{Name: deadStream, Subjects: []string{"webhooks.dead.>"}, AllowDirect: true, Discard: jetstream.DiscardNew, MaxMsgSize: 2048}); err != nil {
		t.Fatal(err)
	}
	e.consumer(t, "rex")
	r := start(t, e.config(t, "rex", helperHandler(t, "exit", "65")), true)
	seq := e.publish(t, pub{body: []byte(strings.Repeat("x", 10000)), claim: true})
	testutil.Eventually(t, 10*time.Second, func() bool {
		return metric(r.Relay, "hookgate_relay_dead_letter_too_large_total") >= 1
	}, "oversized dead letter not counted")
	if info := e.consumerInfo(t, "rex"); info.AckFloor.Stream >= seq || len(e.deadRecords(t)) != 0 {
		t.Error("oversized event settled")
	}
}

// TestDeadLetterDropsOversizedHeaders: application headers that would push a dead-letter record over the broker's
// header limit are left out and the record says so; the contract headers and metadata stay.
func TestDeadLetterDropsOversizedHeaders(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "rex")
	r := start(t, e.config(t, "rex", helperHandler(t, "exit", "65")), true)
	extra := map[string]string{}
	for i := range 7 {
		extra["x-big-"+itoa(uint64(i))] = strings.Repeat("v", 9150)
	}
	seq := e.publish(t, pub{body: []byte("{}"), eventID: "big_headers", extra: extra})
	recs := e.waitDead(t, 1)
	e.waitSettled(t, "rex", seq)
	h := recs[0].Header
	if h.Get(contract.HeaderDeadHeadersDropped) != "true" || h.Get("x-big-0") != "" || h.Get("webhook-id") != "" {
		t.Errorf("application headers not dropped: %v", h.Get(contract.HeaderDeadHeadersDropped))
	}
	if h.Get(contract.HeaderEventID) != "big_headers" || h.Get(contract.HeaderDeadReason) != contract.DeadHandlerRejected {
		t.Errorf("contract or metadata headers lost")
	}
	if metric(r.Relay, outcome(outcomeDeadLettered)) != 1 {
		t.Error("metric")
	}
}

func TestReadinessRejectsRewritingStreams(t *testing.T) {
	cases := map[string]struct {
		stream string
		modify func(*jetstream.StreamConfig)
		want   string
	}{
		"event transform": {eventStream, func(c *jetstream.StreamConfig) {
			c.SubjectTransform = &jetstream.SubjectTransformConfig{Source: "webhooks.replay.*", Destination: "webhooks.replay.other"}
		}, "subject transform"},
		"event republish": {eventStream, func(c *jetstream.StreamConfig) {
			c.RePublish = &jetstream.RePublish{Source: ">", Destination: "copy.>"}
		}, "republishes"},
		"dead-letter republish": {deadStream, func(c *jetstream.StreamConfig) {
			c.RePublish = &jetstream.RePublish{Source: ">", Destination: "copy.>"}
		}, "republishes"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			e.consumer(t, "rex")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			s, err := e.js.Stream(ctx, tc.stream)
			if err != nil {
				t.Fatal(err)
			}
			cfg := s.CachedInfo().Config
			tc.modify(&cfg)
			if _, err := e.js.UpdateStream(ctx, cfg); err != nil {
				t.Fatal(err)
			}
			r := start(t, e.config(t, "rex", helperHandler(t, "exit", "0")), false)
			testutil.Eventually(t, 10*time.Second, func() bool {
				err := r.notReady()
				return err != nil && strings.Contains(err.Error(), tc.want)
			}, "relay ready with a stream that rewrites subjects")
		})
	}
}
