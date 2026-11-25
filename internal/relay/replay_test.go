package relay

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mrrobertkent/hookgate/internal/contract"
	"github.com/mrrobertkent/hookgate/internal/testutil"
)

func TestReplayReachesOnlyFailedConsumer(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "app")
	e.consumer(t, "other")
	var appAccept atomic.Bool
	var appHandled, otherHandled atomic.Int64
	var replayHeader atomic.Value
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !appAccept.Load() {
			w.WriteHeader(http.StatusUnprocessableEntity)
			return
		}
		replayHeader.Store(r.Header.Get(contract.HeaderReplay))
		appHandled.Add(1)
	}))
	t.Cleanup(app.Close)
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { otherHandled.Add(1) }))
	t.Cleanup(other.Close)
	start(t, e.config(t, "app", Handler{HTTP: &HTTPHandler{URL: app.URL, Timeout: 5 * time.Second}}), true)
	start(t, e.config(t, "other", Handler{HTTP: &HTTPHandler{URL: other.URL, Timeout: 5 * time.Second}}), true)

	seq := e.publish(t, pub{body: []byte(`{"n":1}`), eventID: "msg_replay"})
	recs := e.waitDead(t, 1)
	e.waitSettled(t, "app", seq)
	e.waitSettled(t, "other", seq)
	if recs[0].Header.Get(contract.HeaderDeadReason) != contract.DeadHandlerRejected || recs[0].Header.Get(contract.HeaderDeadDetail) != "status 422" {
		t.Fatalf("dead letter %v", recs[0].Header)
	}

	appAccept.Store(true)
	rp := NewReplayer(e.config(t, "app", helperHandler(t, "exit", "0")), e.js)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := rp.Replay(ctx, recs[0].Sequence)
	if err != nil {
		t.Fatal(err)
	}
	if res.Subject != "webhooks.replay.app" || res.Stream != eventStream || res.Duplicate {
		t.Errorf("replayed %+v", res)
	}
	e.waitSettled(t, "app", res.Sequence)
	time.Sleep(300 * time.Millisecond)
	if info := e.consumerInfo(t, "other"); info.NumPending != 0 || info.NumAckPending != 0 {
		t.Errorf("other consumer has work: %+v", info)
	}
	if appHandled.Load() != 1 || otherHandled.Load() != 1 {
		t.Errorf("app handled %d, other %d", appHandled.Load(), otherHandled.Load())
	}
	if got, _ := replayHeader.Load().(string); got != deadStream+":1" {
		t.Errorf("replay marker %q", got)
	}

	checkReplayed(t, e.streamMsg(t, res.Sequence))

	again, err := rp.Replay(ctx, recs[0].Sequence)
	if err != nil || !again.Duplicate || again.Sequence != res.Sequence {
		t.Errorf("second replay %+v %v", again, err)
	}
}

func TestReplayWithoutBody(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "app")
	dir := t.TempDir()
	cfg := e.config(t, "app", helperHandler(t, "record", dir))
	cfg.MaxAttempts = 1
	start(t, cfg, true)
	e.publish(t, pub{body: []byte("late body"), claimKey: "resend/late/x", eventID: "late"})
	recs := e.waitDead(t, 1)
	if recs[0].Header.Get(contract.HeaderDeadReason) != contract.DeadClaimMissing {
		t.Fatalf("reason %q", recs[0].Header.Get(contract.HeaderDeadReason))
	}
	rp := NewReplayer(cfg, e.js)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := rp.Replay(ctx, recs[0].Sequence); !errors.Is(err, ErrNoBody) {
		t.Fatalf("replay without body: %v", err)
	}
	rp.ClaimReference = true
	res, err := rp.Replay(ctx, recs[0].Sequence)
	if err != nil {
		t.Fatal(err)
	}
	m := e.streamMsg(t, res.Sequence)
	if m.Header.Get(contract.HeaderClaimKey) != "resend/late/x" || len(m.Data) != 0 {
		t.Errorf("claim reference not republished: %v", m.Header)
	}
	if _, err := rp.Replay(ctx, 999); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("missing record: %v", err)
	}
	testutil.Eventually(t, 10*time.Second, func() bool { return len(e.deadRecords(t)) == 2 }, "replayed reference not dead-lettered again")
}

func checkReplayed(t *testing.T, m *jetstream.RawStreamMsg) {
	t.Helper()
	want := map[string]string{
		contract.HeaderMsgID: "replay:WEBHOOK_DEAD:1", contract.HeaderEventID: "msg_replay", contract.HeaderDeadReason: "",
		contract.HeaderOriginalSeq: "", "Nats-Expected-Stream": eventStream, "webhook-id": "msg_replay",
	}
	for k, v := range want {
		if got := m.Header.Get(k); got != v {
			t.Errorf("replayed %s = %q, want %q", k, got, v)
		}
	}
	if string(m.Data) != `{"n":1}` {
		t.Errorf("replayed body %q", m.Data)
	}
}

// TestReplaySizeExact: a record that fits the event stream exactly, counting the headers the publish adds, is
// replayed inline; one byte less of room needs a claim check.
func TestReplaySizeExact(t *testing.T) {
	e := newEnv(t)
	cfg := e.config(t, "app", helperHandler(t, "exit", "0"))
	cfg.ClaimCheck = nil
	rp := NewReplayer(cfg, e.js)
	h := nats.Header{"x-note": {strings.Repeat("n", 700)}}
	h.Set(contract.HeaderSource, "resend")
	msg := &nats.Msg{Subject: "webhooks.replay.app", Header: h, Data: []byte(strings.Repeat("b", 1000))}
	size := publishedSize(msg, eventStream, "replay:WEBHOOK_DEAD:1")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	e.capEventStream(t, size)
	if err := rp.fit(ctx, msg, e.stream(t), "replay:WEBHOOK_DEAD:1"); err != nil {
		t.Fatalf("exact fit refused: %v", err)
	}
	if _, err := e.js.PublishMsg(ctx, msg, jetstream.WithExpectStream(eventStream), jetstream.WithMsgID("replay:WEBHOOK_DEAD:1")); err != nil {
		t.Fatalf("broker refused the exact fit: %v", err)
	}
	e.capEventStream(t, size-1)
	if err := rp.fit(ctx, msg, e.stream(t), "replay:WEBHOOK_DEAD:2"); err == nil {
		t.Fatal("oversized replay accepted without a claim check")
	}
}

// TestReplayRefusesRewritingStream: a replay is not published into an event stream that could rewrite its subject.
func TestReplayRefusesRewritingStream(t *testing.T) {
	e := newEnv(t)
	e.consumer(t, "app")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := e.js.PublishMsg(ctx, &nats.Msg{Subject: "webhooks.dead.app", Data: []byte("{}"), Header: nats.Header{
		contract.HeaderDeadReason: {contract.DeadHandlerRejected}, contract.HeaderOriginalConsumer: {"app"},
		contract.HeaderOriginalStream: {eventStream}, contract.HeaderSource: {"resend"},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.js.UpdateStream(ctx, jetstream.StreamConfig{Name: eventStream, Subjects: []string{"webhooks.events.>", "webhooks.replay.>"},
		Duplicates: time.Minute, SubjectTransform: &jetstream.SubjectTransformConfig{Source: "webhooks.replay.*", Destination: "webhooks.events.{{wildcard(1)}}"}}); err != nil {
		t.Fatal(err)
	}
	before := e.streamState(t)
	if _, err := NewReplayer(e.config(t, "app", helperHandler(t, "exit", "0")), e.js).Replay(ctx, 1); err == nil || !strings.Contains(err.Error(), "subject transform") {
		t.Errorf("replay into a rewriting stream: %v", err)
	}
	if after := e.streamState(t); after != before {
		t.Errorf("stored %d messages", after-before)
	}
}
