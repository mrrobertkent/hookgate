package relay

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"

	"github.com/mrrobertkent/hookgate/internal/config"
	"github.com/mrrobertkent/hookgate/internal/testutil"
)

// A seed that cannot be loaded at start stops the relay and is reported as rejected credentials.
func TestUnusableCredentialsAtStart(t *testing.T) {
	e := newEnv(t)
	bad := filepath.Join(t.TempDir(), "bad")
	if err := os.WriteFile(bad, []byte("not a seed"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := e.config(t, "rex", helperHandler(t, "exit", "0"))
	cfg.Connection.NATS = config.NATSConnection{URLs: []string{e.srv.ClientURL()}, NKeySeed: &config.SecretRef{File: bad}}
	r, err := New(cfg, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err == nil || strings.Contains(err.Error(), "not a seed") {
		t.Fatalf("run: %v", err)
	}
	if v := metric(r, "hookgate_relay_nats_credentials_rejected"); v != 1 {
		t.Errorf("credentials_rejected %v", v)
	}
}

// Credentials the server rejects keep the relay running, not ready, with the rejection reported.
func TestRejectedCredentials(t *testing.T) {
	allowed, _ := nkeys.CreateUser()
	allowedPub, _ := allowed.PublicKey()
	srv := testutil.RunServer(t, func(o *server.Options) { o.Nkeys = []*server.NkeyUser{{Nkey: allowedPub}} })
	other, _ := nkeys.CreateUser()
	seed, _ := other.Seed()
	file := filepath.Join(t.TempDir(), "user.nk")
	if err := os.WriteFile(file, seed, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &Config{
		AdminListen: "127.0.0.1:0", Stream: eventStream, Consumer: "rex",
		Connection:     Connection{NATS: config.NATSConnection{URLs: []string{srv.ClientURL()}, NKeySeed: &config.SecretRef{File: file}}},
		FilterSubjects: []string{eventSubj, "webhooks.replay.rex"}, DeadLetter: DeadLetter{Stream: deadStream},
		Handler: helperHandler(t, "exit", "0"),
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	r := start(t, cfg, false)
	testutil.Eventually(t, 10*time.Second, func() bool {
		return metric(r.Relay, "hookgate_relay_nats_credentials_rejected") == 1
	}, "rejected credentials not reported")
	if err := r.notReady(); err == nil || !strings.Contains(err.Error(), "credentials rejected") {
		t.Errorf("not ready: %v", err)
	}
}

func TestRejectionReasonWinsOverPause(t *testing.T) {
	r := &Relay{connected: true, rejected: true, pauseErr: errors.New("status 401")}
	if err := r.notReady(); err == nil || !strings.Contains(err.Error(), "credentials rejected") {
		t.Errorf("not ready: %v", err)
	}
}
