package relay

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"

	"github.com/mrrobertkent/hookgate/internal/config"
	"github.com/mrrobertkent/hookgate/internal/contract"
	"github.com/mrrobertkent/hookgate/internal/natsconn"
	"github.com/mrrobertkent/hookgate/internal/testutil"
)

// relayPermissions are the exact grants docs/relay.md lists for consumer rex.
const relayPermissions = `{
  publish: { allow: [
    "$JS.API.STREAM.INFO.WEBHOOK_EVENTS",
    "$JS.API.CONSUMER.INFO.WEBHOOK_EVENTS.rex",
    "$JS.API.CONSUMER.MSG.NEXT.WEBHOOK_EVENTS.rex",
    "$JS.ACK.WEBHOOK_EVENTS.rex.>",
    "$JS.API.STREAM.INFO.WEBHOOK_DEAD",
    "webhooks.dead.rex",
    "$JS.API.STREAM.INFO.OBJ_WEBHOOK_CLAIMS",
    "$JS.API.DIRECT.GET.OBJ_WEBHOOK_CLAIMS.$O.WEBHOOK_CLAIMS.M.>",
    "$JS.API.CONSUMER.CREATE.OBJ_WEBHOOK_CLAIMS.*.$O.WEBHOOK_CLAIMS.C.*",
    "$JS.API.CONSUMER.DELETE.OBJ_WEBHOOK_CLAIMS.*",
    "$JS.FC.OBJ_WEBHOOK_CLAIMS.>"
  ] }
  subscribe: { allow: [
    "_INBOX.relay-rex.>",
    "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.WEBHOOK_EVENTS.rex"
  ] }
}`

// replayPermissions are the grants docs/relay.md lists for the replay command.
const replayPermissions = `{
  publish: { allow: [
    "$JS.API.STREAM.INFO.WEBHOOK_DEAD",
    "$JS.API.DIRECT.GET.WEBHOOK_DEAD",
    "$JS.API.STREAM.MSG.GET.WEBHOOK_DEAD",
    "$JS.API.STREAM.INFO.WEBHOOK_EVENTS",
    "webhooks.replay.*",
    "$JS.API.STREAM.INFO.OBJ_WEBHOOK_CLAIMS",
    "$JS.API.DIRECT.GET.OBJ_WEBHOOK_CLAIMS",
    "$JS.API.DIRECT.GET.OBJ_WEBHOOK_CLAIMS.$O.WEBHOOK_CLAIMS.M.>",
    "$O.WEBHOOK_CLAIMS.C.>",
    "$O.WEBHOOK_CLAIMS.M.>"
  ] }
  subscribe: { allow: ["_INBOX.relay-replay.>"] }
}`

type identity struct {
	pub, seedFile string
}

func newIdentity(t *testing.T, dir, name string) identity {
	t.Helper()
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	seed, _ := kp.Seed()
	pub, _ := kp.PublicKey()
	file := filepath.Join(dir, name+".nk")
	if err := os.WriteFile(file, seed, 0o600); err != nil {
		t.Fatal(err)
	}
	return identity{pub: pub, seedFile: file}
}

func TestLeastPrivilegeRelayUser(t *testing.T) {
	dir := t.TempDir()
	admin, relayID, replayID := newIdentity(t, dir, "admin"), newIdentity(t, dir, "relay"), newIdentity(t, dir, "replay")
	srv := testutil.RunServerFromConfig(t, fmt.Sprintf(`
jetstream { store_dir: "$STORE" }
accounts {
  APP {
    jetstream: enabled
    users: [
      { nkey: %s }
      { nkey: %s, permissions: %s }
      { nkey: %s, permissions: %s }
    ]
  }
}`, admin.pub, relayID.pub, relayPermissions, replayID.pub, replayPermissions))
	opt, err := nats.NkeyOptionFromSeed(admin.seedFile)
	if err != nil {
		t.Fatal(err)
	}
	e := setupEnv(t, srv, opt)
	e.capEventStream(t, 64<<10)
	e.consumer(t, "rex")
	e.consumer(t, "other")

	handler := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(contract.HeaderReplay) == "" && r.Header.Get(contract.HeaderEventID) != "inline" {
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(handler.Close)
	cfg := e.config(t, "rex", Handler{HTTP: &HTTPHandler{URL: handler.URL, Timeout: 5 * time.Second}})
	cfg.Connection.NATS = config.NATSConnection{URLs: []string{srv.ClientURL()}, NKeySeed: &config.SecretRef{File: relayID.seedFile}}
	cfg.Connection.NATS.ApplyDefaults("relay-rex")
	relay := start(t, cfg, true)

	e.publish(t, pub{body: []byte("inline"), eventID: "inline"})
	e.publish(t, pub{body: []byte(strings.Repeat("claimed ", 50000)), claim: true, eventID: "claimed"})
	last := e.publish(t, pub{body: []byte("rejected"), eventID: "reject"})
	e.waitSettled(t, "rex", last)
	recs := e.waitDead(t, 2)
	if len(recs) != 2 || len(recs[0].Data) != 400000 {
		t.Fatalf("dead letters %d, first body %d bytes", len(recs), len(recs[0].Data))
	}

	// The claimed event's body exceeds the capped stream, so the replay goes through the claim check.
	t.Run("replay user", func(t *testing.T) {
		rcfg := *cfg
		rcfg.Connection.NATS = config.NATSConnection{URLs: []string{srv.ClientURL()}, NKeySeed: &config.SecretRef{File: replayID.seedFile}}
		rcfg.Connection.NATS.ApplyDefaults("relay-replay")
		for _, rec := range recs {
			res := replayAs(t, &rcfg, rec.Sequence)
			if again := replayAs(t, &rcfg, rec.Sequence); !again.Duplicate || again.Sequence != res.Sequence {
				t.Errorf("repeated replay of %d: %+v", rec.Sequence, again)
			}
			e.waitSettled(t, "rex", res.Sequence)
		}
		if n := metric(relay.Relay, outcome(outcomeAcked)); n != 3 {
			t.Errorf("acked %v, want 3 (inline, two replays)", n)
		}
	})

	t.Run("denied", func(t *testing.T) { checkDenied(t, cfg) })
}

// checkDenied shows that the relay identity cannot reach other consumers, publish events or change streams.
func checkDenied(t *testing.T, cfg *Config) {
	nc, err := natsconn.Connect("probe", cfg.Connection.NATS, slog.New(slog.DiscardHandler), natsconn.Callbacks{})
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc.Conn)
	if err != nil {
		t.Fatal(err)
	}
	probe := func(name string, f func(ctx context.Context) error) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		if err := f(ctx); err == nil {
			t.Errorf("%s allowed", name)
		}
	}
	probe("other consumer", func(ctx context.Context) error { _, err := js.Consumer(ctx, eventStream, "other"); return err })
	probe("delete consumer", func(ctx context.Context) error {
		return js.DeleteConsumer(ctx, eventStream, "rex")
	})
	probe("stream purge", func(context.Context) error {
		_, err := nc.Request("$JS.API.STREAM.PURGE."+eventStream, nil, 300*time.Millisecond)
		return err
	})
	probe("claim bucket purge", func(context.Context) error {
		_, err := nc.Request("$JS.API.STREAM.PURGE.OBJ_"+claimBucket, nil, 300*time.Millisecond)
		return err
	})
	probe("dead-letter delete", func(context.Context) error {
		_, err := nc.Request("$JS.API.STREAM.MSG.DELETE."+deadStream, []byte(`{"seq":1}`), 300*time.Millisecond)
		return err
	})
	probe("publish event", func(ctx context.Context) error { _, err := js.Publish(ctx, eventSubj, []byte("x")); return err })
	probe("other stream info", func(ctx context.Context) error { _, err := js.Stream(ctx, eventStream+"_X"); return err })
	probe("other dead subject", func(ctx context.Context) error { _, err := js.Publish(ctx, "webhooks.dead.other", nil); return err })
}

func replayAs(t *testing.T, cfg *Config, seq uint64) Replayed {
	t.Helper()
	nc, err := natsconn.Connect("replay", cfg.Connection.NATS, slog.New(slog.DiscardHandler), natsconn.Callbacks{})
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if werr := nc.WaitConnected(ctx); werr != nil {
		t.Fatal(werr)
	}
	js, err := jetstream.New(nc.Conn)
	if err != nil {
		t.Fatal(err)
	}
	res, err := NewReplayer(cfg, js).Replay(ctx, seq)
	if err != nil {
		t.Fatal(err)
	}
	return res
}
