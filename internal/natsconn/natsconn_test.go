package natsconn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
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
		Sampled:   func(_, cur Status) { up.Store(cur.Connected) },
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
	testutil.Eventually(t, 2*time.Second, func() bool { return !up.Load() && !c.Status().Connected }, "status still connected after close")
	if err := c.Drain(ctx); err != nil {
		t.Errorf("second Drain: %v", err)
	}
}

// Drain returns within its context even while the client is stuck in a handshake with a server that never
// answers.
func TestDrainBoundedDuringStalledHandshake(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			defer c.Close() // accepted and never answered
		}
	}()
	cfg := conn([]string{"nats://" + ln.Addr().String()}, func(c *config.NATSConnection) {
		c.None = true
		c.ConnectTimeout = 3 * time.Second
	})
	c, err := Connect("test", cfg, discard, Callbacks{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_ = c.Drain(ctx)
	if d := time.Since(start); d > time.Second {
		t.Errorf("Drain took %s with a 200ms budget", d)
	}
}

func TestIsCredentialError(t *testing.T) {
	for err, want := range map[error]bool{
		nil:                        false,
		nats.ErrAuthorization:      true,
		nats.ErrAuthExpired:        true,
		nats.ErrAuthRevoked:        true,
		nats.ErrAccountAuthExpired: true,
		errors.New("nats: 'Authorization Violation'"):                                                      true,
		errors.New("nats: user authentication revoked"):                                                    true,
		errors.New("remote error: tls: bad certificate"):                                                   true,
		errors.New("remote error: tls: certificate required"):                                              true,
		x509.UnknownAuthorityError{}:                                                                       true,
		&tls.CertificateVerificationError{Err: errors.New("x")}:                                            true,
		fmt.Errorf("nats: %w", &fs.PathError{Op: "open", Path: "/run/secrets/creds", Err: fs.ErrNotExist}): true,
		fmt.Errorf("error signing nonce: %w", errors.New("nkeys: no nkey seed found")):                     true,
		errors.New("dial tcp: connection refused"):                                                         false,
		io.EOF:                      false,
		nats.ErrNoServers:           false,
		nats.ErrPermissionViolation: false,
	} {
		if got := IsCredentialError(err); got != want {
			t.Errorf("IsCredentialError(%v) = %v, want %v", err, got, want)
		}
	}
}

func TestStatusNext(t *testing.T) {
	t0 := time.Unix(1_700_000_000, 0)
	refused := errors.New("dial tcp: connection refused")
	s := Status{DownSince: t0}
	s = s.next(false, 0, refused, t0.Add(time.Second))
	if !s.DownSince.Equal(t0) || s.Rejected {
		t.Errorf("repeated down sample moved the loss time: %+v", s)
	}
	s = s.next(true, 1, nil, t0.Add(2*time.Second))
	if !s.Connected || !s.DownSince.IsZero() || s.Err != nil || s.Reconnects != 1 {
		t.Errorf("connected: %+v", s)
	}
	t1 := t0.Add(3 * time.Second)
	s = s.next(false, 1, nats.ErrAuthorization, t1)
	if !s.Rejected || !s.DownSince.IsZero() {
		t.Errorf("rejected: %+v", s)
	}
	// A server reports an expiry, then closes the socket: the outage stays a credential fault.
	s = s.next(false, 1, nats.ErrAuthExpired, t1)
	s = s.next(false, 1, io.EOF, t0.Add(4*time.Second))
	s = s.next(false, 1, refused, t0.Add(5*time.Second))
	if !s.Rejected || !s.DownSince.IsZero() {
		t.Errorf("a later generic failure cleared the rejection: %+v", s)
	}
	s = s.next(true, 2, nil, t0.Add(6*time.Second))
	if s.Rejected || !s.Connected {
		t.Errorf("connected: %+v", s)
	}
	t7 := t0.Add(7 * time.Second)
	s = s.next(false, 2, io.EOF, t7)
	if s.Rejected || !s.DownSince.Equal(t7) {
		t.Errorf("a new outage after a success starts its own clock: %+v", s)
	}
	// An expiry reported on the live socket, which still reads as connected until it closes.
	s = s.next(true, 3, nil, t0.Add(8*time.Second))
	s = s.next(false, 3, nats.ErrAuthExpired, t0.Add(9*time.Second))
	s = s.next(true, 3, nil, t0.Add(10*time.Second))
	s = s.next(false, 3, io.EOF, t0.Add(11*time.Second))
	if !s.Rejected || !s.DownSince.IsZero() {
		t.Errorf("the rejecting socket cleared its own rejection: %+v", s)
	}
	// A successful reconnect clears the rejection even when the new connection drops before a sample.
	s = s.cleared()
	t12 := t0.Add(12 * time.Second)
	s = s.next(false, 3, io.EOF, t12)
	if s.Rejected || !s.DownSince.Equal(t12) {
		t.Errorf("outage after a successful reconnect: %+v", s)
	}
	// A reconnect that succeeds and drops before a sample ends the outage: the next one has its own clock.
	s = s.next(false, 3, io.EOF, t0.Add(13*time.Second)).cleared()
	t14 := t0.Add(14 * time.Second)
	s = s.next(false, 4, io.EOF, t14)
	if !s.DownSince.Equal(t14) {
		t.Errorf("a new outage after an unsampled reconnect kept the old clock: %+v", s)
	}
}

// A sample counts only when the client's reconnect count is the same before and after its status read, so a
// sample that read the old count and then the new connection's status is discarded.
func TestSampleBindsStatusToConnection(t *testing.T) {
	c := &Conn{closed: make(chan struct{}), nudge: make(chan struct{}, 1)}
	c.status.Store(&Status{DownSince: time.Now()})
	counts := []uint64{4, 5} // the old count, then the new connection's
	c.client = clientStatus{
		connected: func() bool { return true },
		reconnects: func() uint64 {
			n := counts[0]
			if len(counts) > 1 {
				counts = counts[1:]
			}
			return n
		},
	}
	c.sample()
	if st := c.Status(); st.Connected {
		t.Fatalf("sample across a reconnect counted: %+v", st)
	}
	select {
	case <-c.nudge:
	default:
		t.Error("discarded sample not retried")
	}
	c.sample()
	if st := c.Status(); !st.Connected || st.Reconnects != 5 {
		t.Errorf("consistent sample: %+v", st)
	}

	// A failure recorded while a sample reads the client wins over that sample.
	c.client.connected = func() bool {
		c.fail(io.EOF)
		return true
	}
	c.sample()
	if st := c.Status(); st.Connected {
		t.Errorf("sample overlapping a failure counted: %+v", st)
	}
}

// A seed file that disappears or is corrupted after start is a credential fault when the client signs in again.
func TestUnreadableSeedFileAfterStartIsCredentialFault(t *testing.T) {
	kp, _ := nkeys.CreateUser()
	seed, _ := kp.Seed()
	pub, _ := kp.PublicKey()
	s := testutil.RunServer(t, func(o *server.Options) { o.Nkeys = []*server.NkeyUser{{Nkey: pub}} })
	proxy := testutil.StartProxy(t, strings.TrimPrefix(s.ClientURL(), "nats://"))
	for name, spoil := range map[string]func(string) error{
		"removed":   os.Remove,
		"corrupted": func(f string) error { return os.WriteFile(f, []byte("not a seed"), 0o600) },
	} {
		file := filepath.Join(t.TempDir(), "seed")
		if err := os.WriteFile(file, seed, 0o600); err != nil {
			t.Fatal(err)
		}
		cfg := conn([]string{"nats://" + proxy.Addr()}, func(c *config.NATSConnection) { c.NKeySeed = &config.SecretRef{File: file} })
		c, err := Connect("test", cfg, discard, Callbacks{})
		if err != nil {
			t.Fatal(err)
		}
		testutil.Eventually(t, 5*time.Second, func() bool { return c.Status().Connected }, name+": not connected")
		if err := spoil(file); err != nil {
			t.Fatal(err)
		}
		proxy.DropConnections() // the broker restarts; the client signs in again
		testutil.Eventually(t, 10*time.Second, func() bool { return c.Status().Rejected }, name+": not a credential fault")
		c.Close()
	}
}
