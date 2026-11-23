package natsconn

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/mrrobertkent/hookgate/internal/config"
	"github.com/mrrobertkent/hookgate/internal/testutil"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func conn(urls []string, auth func(*config.NATSConnection)) config.NATSConnection {
	c := config.NATSConnection{URLs: urls}
	auth(&c)
	c.ApplyDefaults("test")
	return c
}

func TestAuthMethods(t *testing.T) {
	kp, _ := nkeys.CreateUser()
	seed, _ := kp.Seed()
	pub, _ := kp.PublicKey()
	seedFile := filepath.Join(t.TempDir(), "seed")
	if err := os.WriteFile(seedFile, seed, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_NKEY_SEED", string(seed))
	t.Setenv("TEST_NATS_TOKEN", "s3cret-token-value")

	nk := testutil.RunServer(t, func(o *server.Options) { o.Nkeys = []*server.NkeyUser{{Nkey: pub}} })
	tok := testutil.RunServer(t, func(o *server.Options) { o.Authorization = "s3cret-token-value" })

	cases := map[string]config.NATSConnection{
		"nkey file": conn([]string{nk.ClientURL()}, func(c *config.NATSConnection) { c.NKeySeed = &config.SecretRef{File: seedFile} }),
		"nkey env":  conn([]string{nk.ClientURL()}, func(c *config.NATSConnection) { c.NKeySeed = &config.SecretRef{Env: "TEST_NKEY_SEED"} }),
		"token":     conn([]string{tok.ClientURL()}, func(c *config.NATSConnection) { c.Token = &config.SecretRef{Env: "TEST_NATS_TOKEN"} }),
	}
	for name, cfg := range cases {
		c, err := Connect("test", cfg, discard, Callbacks{})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := c.WaitConnected(ctx); err != nil {
			t.Errorf("%s: not connected: %v", name, err)
		}
		cancel()
		c.Close()
	}
}

func TestConfigFaultsFailWithoutConnecting(t *testing.T) {
	bad := filepath.Join(t.TempDir(), "bad")
	if err := os.WriteFile(bad, []byte("not a seed"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_NOT_A_SEED", "not a seed")
	cases := map[string]func(*config.NATSConnection){
		"unreadable seed file": func(c *config.NATSConnection) { c.NKeySeed = &config.SecretRef{File: bad} },
		"missing seed file":    func(c *config.NATSConnection) { c.NKeySeed = &config.SecretRef{File: bad + "-missing"} },
		"bad seed env":         func(c *config.NATSConnection) { c.NKeySeed = &config.SecretRef{Env: "TEST_NOT_A_SEED"} },
		"unset token":          func(c *config.NATSConnection) { c.Token = &config.SecretRef{Env: "TEST_UNSET_TOKEN"} },
		"bad creds":            func(c *config.NATSConnection) { c.Creds = &config.SecretRef{File: bad} },
		"bad ca": func(c *config.NATSConnection) {
			c.None = true
			c.TLS = &config.TLS{CA: &config.SecretRef{File: bad}}
		},
	}
	for name, auth := range cases {
		cfg := conn([]string{"nats://127.0.0.1:1"}, auth)
		if _, err := Connect("test", cfg, discard, Callbacks{}); err == nil {
			t.Errorf("%s: connected", name)
		} else if strings.Contains(err.Error(), "not a seed") {
			t.Errorf("%s: error carries the secret: %v", name, err)
		}
	}
}

// A server that is down at start is retried until it comes up; callbacks report each state change.
func TestRetriesFailedConnectAndDrains(t *testing.T) {
	port := testutil.FreePort(t)
	var connects atomic.Int32
	var up atomic.Bool
	cfg := conn([]string{"nats://127.0.0.1:" + strconv.Itoa(port)}, func(c *config.NATSConnection) { c.None = true })
	c, err := Connect("test", cfg, discard, Callbacks{
		Connected: func(*nats.Conn) { connects.Add(1) },
		State:     up.Store,
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.IsConnected() {
		t.Fatal("connected to nothing")
	}
	if _, err := c.Request("x", nil, 50*time.Millisecond); err == nil {
		t.Fatal("request while disconnected succeeded")
	}
	testutil.RunServer(t, func(o *server.Options) { o.Port = port })
	testutil.Eventually(t, 10*time.Second, func() bool { return connects.Load() == 1 && up.Load() }, "no connect callback")
	if !strings.HasPrefix(c.NewInbox(), "_INBOX.test.") {
		t.Errorf("inbox %s", c.NewInbox())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.Closed():
	default:
		t.Fatal("Drain returned before the connection closed")
	}
	if up.Load() {
		t.Error("state still connected after close")
	}
	if err := c.Drain(ctx); err != nil {
		t.Errorf("second Drain: %v", err)
	}
}
