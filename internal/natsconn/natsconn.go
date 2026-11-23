// Package natsconn opens NATS connections from configuration: authentication, TLS, inbox prefix and a
// reconnect policy that never gives up.
package natsconn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/mrrobertkent/hookgate/internal/config"
)

// Callbacks observe a connection. Each may be nil. They run on the client's callback goroutine, one at a time,
// so they must not block.
type Callbacks struct {
	// Connected runs after the first successful connect and after every reconnect.
	Connected func(*nats.Conn)
	// State reports every change between connected (true) and not connected (false).
	State func(connected bool)
}

// Conn is a NATS connection that can be drained to completion.
type Conn struct {
	*nats.Conn
	closed chan struct{}
}

// Options returns the authentication and TLS options for cfg, resolving every secret reference. It fails when a
// secret is missing or unusable, without connecting.
func Options(cfg config.NATSConnection) ([]nats.Option, error) {
	auth, err := authOption(cfg)
	if err != nil {
		return nil, err
	}
	opts := []nats.Option{auth}
	if cfg.TLS != nil {
		tc, err := tlsConfig(*cfg.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, nats.Secure(tc))
	}
	return opts, nil
}

// Connect opens a connection named name. A server that cannot be reached is not an error: the client keeps
// retrying, forever, and Callbacks.Connected runs once it succeeds. Errors are configuration faults such as an
// unreadable secret.
func Connect(name string, cfg config.NATSConnection, log *slog.Logger, cb Callbacks) (*Conn, error) {
	opts, err := Options(cfg)
	if err != nil {
		return nil, err
	}
	c := &Conn{closed: make(chan struct{})}
	var once sync.Once
	state := func(up bool) {
		if cb.State != nil {
			cb.State(up)
		}
	}
	connected := func(nc *nats.Conn) {
		log.Info("nats connected", "connection", name, "server", config.RedactURL(nc.ConnectedUrl()))
		state(true)
		if cb.Connected != nil {
			cb.Connected(nc)
		}
	}
	opts = append(opts,
		nats.Name(name),
		nats.MaxReconnects(-1),
		nats.RetryOnFailedConnect(true),
		// Two identical auth errors in a row would otherwise close the connection for good.
		nats.IgnoreAuthErrorAbort(),
		// Publishing while disconnected fails at once instead of queueing behind a reconnect the caller
		// cannot see; callers retry within their own deadline.
		nats.ReconnectBufSize(-1),
		nats.Timeout(cfg.ConnectTimeout),
		nats.CustomInboxPrefix(cfg.InboxPrefix),
		nats.ConnectHandler(connected),
		nats.ReconnectHandler(connected),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Warn("nats disconnected", "connection", name, "error", errString(err))
			state(false)
		}),
		nats.ClosedHandler(func(*nats.Conn) {
			log.Info("nats connection closed", "connection", name)
			state(false)
			once.Do(func() { close(c.closed) })
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			log.Error("nats error", "connection", name, "error", errString(err))
		}),
	)
	nc, err := nats.Connect(strings.Join(cfg.URLs, ","), opts...)
	if err != nil {
		return nil, fmt.Errorf("nats connection %s: %w", name, err)
	}
	c.Conn = nc
	return c, nil
}

// Drain stops new subscriptions and publishes, flushes what is pending, closes the connection and waits until it
// is closed or ctx ends. When ctx ends first it starts closing the connection at once and returns ctx's error
// without waiting further: a client stuck in a reconnect handshake may hold its lock for up to connect_timeout.
func (c *Conn) Drain(ctx context.Context) error {
	started := make(chan error, 1)
	go func() {
		err := c.Conn.Drain()
		switch {
		case err == nil, errors.Is(err, nats.ErrConnectionClosed):
			started <- nil
		case errors.Is(err, nats.ErrConnectionReconnecting):
			started <- nil // Drain closed the connection: nothing was in flight on it
		default:
			c.Close()
			started <- err
		}
	}()
	select {
	case err := <-started:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		go c.Close()
		return ctx.Err()
	}
	select {
	case <-c.closed:
		return nil
	case <-ctx.Done():
		go c.Close()
		return ctx.Err()
	}
}

// Closed is closed once the connection is closed.
func (c *Conn) Closed() <-chan struct{} { return c.closed }

func authOption(cfg config.NATSConnection) (nats.Option, error) {
	switch {
	case cfg.NKeySeed != nil:
		return nkeyOption(*cfg.NKeySeed)
	case cfg.Creds != nil:
		return credsOption(*cfg.Creds)
	case cfg.Token != nil:
		v, err := cfg.Token.Resolve()
		if err != nil {
			return nil, fmt.Errorf("nats token: %w", err)
		}
		return nats.Token(v), nil
	}
	return func(*nats.Options) error { return nil }, nil
}

// nkeyOption reads a seed file at every connect, so a rotated file takes effect on the next reconnect.
func nkeyOption(ref config.SecretRef) (nats.Option, error) {
	if ref.File != "" {
		o, err := nats.NkeyOptionFromSeed(ref.File)
		if err != nil {
			return nil, fmt.Errorf("nats nkey_seed %s: %w", ref, err)
		}
		return o, nil
	}
	seed, err := ref.Resolve()
	if err != nil {
		return nil, fmt.Errorf("nats nkey_seed: %w", err)
	}
	kp, err := nkeys.ParseDecoratedNKey([]byte(seed))
	if err != nil {
		return nil, fmt.Errorf("nats nkey_seed %s: not an nkey seed", ref)
	}
	defer kp.Wipe()
	pub, err := kp.PublicKey()
	if err != nil || !nkeys.IsValidPublicUserKey(pub) {
		return nil, fmt.Errorf("nats nkey_seed %s: not a user nkey seed", ref)
	}
	return nats.Nkey(pub, func(nonce []byte) ([]byte, error) {
		kp, err := nkeys.ParseDecoratedNKey([]byte(seed))
		if err != nil {
			return nil, err
		}
		defer kp.Wipe()
		return kp.Sign(nonce)
	}), nil
}

// credsOption reads a creds file at every connect, so a rotated file takes effect on the next reconnect.
func credsOption(ref config.SecretRef) (nats.Option, error) {
	v, err := ref.Resolve()
	if err != nil {
		return nil, fmt.Errorf("nats creds: %w", err)
	}
	if err := checkCreds([]byte(v)); err != nil {
		return nil, fmt.Errorf("nats creds %s: %w", ref, err)
	}
	if ref.File != "" {
		return nats.UserCredentials(ref.File), nil
	}
	return nats.UserCredentialBytes([]byte(v)), nil
}

// checkCreds accepts a decorated creds file: a user JWT followed by a user nkey seed.
func checkCreds(b []byte) error {
	jwt, err := nkeys.ParseDecoratedJWT(b)
	if err != nil || strings.Count(jwt, ".") != 2 {
		return errors.New("no user JWT")
	}
	kp, err := nkeys.ParseDecoratedUserNKey(b)
	if err != nil {
		return errors.New("no user nkey seed")
	}
	kp.Wipe()
	return nil
}

func tlsConfig(t config.TLS) (*tls.Config, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if t.CA != nil {
		pem, err := t.CA.Resolve()
		if err != nil {
			return nil, fmt.Errorf("nats tls.ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(pem)) {
			return nil, fmt.Errorf("nats tls.ca %s: no PEM certificate", t.CA)
		}
		tc.RootCAs = pool
	}
	if t.Cert != nil && t.Key != nil {
		certPEM, err := t.Cert.Resolve()
		if err != nil {
			return nil, fmt.Errorf("nats tls.cert: %w", err)
		}
		keyPEM, err := t.Key.Resolve()
		if err != nil {
			return nil, fmt.Errorf("nats tls.key: %w", err)
		}
		cert, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
		if err != nil {
			return nil, fmt.Errorf("nats tls.cert %s / tls.key %s: not a matching PEM pair", t.Cert, t.Key)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// WaitConnected waits until c is connected or ctx ends.
func (c *Conn) WaitConnected(ctx context.Context) error {
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for !c.IsConnected() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	return nil
}
