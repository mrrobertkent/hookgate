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
	e.consumer(t, "rex")
	e.consumer(t, "other")
	var rexAccept atomic.Bool
	var rexHandled, otherHandled atomic.Int64
	var replayHeader atomic.Value
	rex := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !rexAccept.Load() {
			w.WriteHeader(http.StatusUnprocessableEntity)
			return
		}
		replayHeader.Store(r.Header.Get(contract.HeaderReplay))
		rexHandled.Add(1)
	}))
	t.Cleanup(rex.Close)
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { otherHandled.Add(1) }))
	t.Cleanup(other.Close)
	start(t, e.config(t, "rex", Handler{HTTP: &HTTPHandler{URL: rex.URL, Timeout: 5 * time.Second}}), true)
	start(t, e.config(t, "other", Handler{HTTP: &HTTPHandler{URL: other.URL, Timeout: 5 * time.Second}}), true)

	seq := e.publish(t, pub{body: []byte(`{"n":1}`), eventID: "msg_replay"})
	recs := e.waitDead(t, 1)
	e.waitSettled(t, "rex", seq)
	e.waitSettled(t, "other", seq)
	if recs[0].Header.Get(contract.HeaderDeadReason) != contract.DeadHandlerRejected || recs[0].Header.Get(contract.HeaderDeadDetail) != "status 422" {
		t.Fatalf("dead letter %v", recs[0].Header)
	}

	rexAccept.Store(true)
	rp := NewReplayer(e.config(t, "rex", helperHandler(t, "exit", "0")), e.js)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := rp.Replay(ctx, recs[0].Sequence)
	if err != nil {
		t.Fatal(err)
	}
	if res.Subject != "webhooks.replay.rex" || res.Stream != eventStream || res.Duplicate {
		t.Errorf("replayed %+v", res)
	}
	e.waitSettled(t, "rex", res.Sequence)
	time.Sleep(300 * time.Millisecond)
	if info := e.consumerInfo(t, "other"); info.NumPending != 0 || info.NumAckPending != 0 {
		t.Errorf("other consumer has work: %+v", info)
	}
	if rexHandled.Load() != 1 || otherHandled.Load() != 1 {
		t.Errorf("rex handled %d, other %d", rexHandled.Load(), otherHandled.Load())
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
	e.consumer(t, "rex")
	dir := t.TempDir()
	cfg := e.config(t, "rex", helperHandler(t, "record", dir))
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
	cfg := e.config(t, "rex", helperHandler(t, "exit", "0"))
	cfg.ClaimCheck = nil
	rp := NewReplayer(cfg, e.js)
	h := nats.Header{"x-note": {strings.Repeat("n", 700)}}
	h.Set(contract.HeaderSource, "resend")
	msg := &nats.Msg{Subject: "webhooks.replay.rex", Header: h, Data: []byte(strings.Repeat("b", 1000))}
	size := publishedSize(msg, eventStream, "replay:WEBHOOK_DEAD:1")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	e.capEventStream(t, size)
	if err := rp.fit(ctx, msg, eventStream, "replay:WEBHOOK_DEAD:1"); err != nil {
		t.Fatalf("exact fit refused: %v", err)
	}
	if _, err := e.js.PublishMsg(ctx, msg, jetstream.WithExpectStream(eventStream), jetstream.WithMsgID("replay:WEBHOOK_DEAD:1")); err != nil {
		t.Fatalf("broker refused the exact fit: %v", err)
	}
	e.capEventStream(t, size-1)
	if err := rp.fit(ctx, msg, eventStream, "replay:WEBHOOK_DEAD:2"); err == nil {
		t.Fatal("oversized replay accepted without a claim check")
	}
}
