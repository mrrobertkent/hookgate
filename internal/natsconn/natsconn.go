// Package natsconn opens NATS connections from configuration: authentication, TLS, inbox prefix and a
// reconnect policy that never gives up.
package natsconn

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"

	"github.com/mrrobertkent/hookgate/internal/config"
)

// Callbacks observe a connection. Each may be nil and must not block.
type Callbacks struct {
	// Connected runs on the client's callback goroutine after the first successful connect and after every
	// reconnect.
	Connected func(*nats.Conn)
	// Sampled runs after every status update, with the previous and the new status. Calls never overlap.
	Sampled func(prev, cur Status)
}

// sampleEvery is how often a connection's status is sampled, besides after every connection event.
const sampleEvery = 250 * time.Millisecond

// Conn is a NATS connection that can be drained to completion. Status never waits on the client's lock, which a
// reconnect attempt holds while it dials and handshakes: a disconnect or failed attempt marks the connection
// down at once, and a monitor goroutine samples the client to see it connected again.
type Conn struct {
	*nats.Conn
	closed  chan struct{}
	nudge   chan struct{}
	lastErr atomic.Pointer[error]
	status  atomic.Pointer[Status]
	sampled func(prev, cur Status)

	client clientStatus

	mu     sync.Mutex // serialises status updates; never held while calling the client
	events uint64     // failures recorded; a sample that overlaps one is discarded
}

// clientStatus reads the client's state; both calls wait on the client's lock.
type clientStatus struct {
	connected  func() bool
	reconnects func() uint64
}

// Status is a connection's state as last observed.
type Status struct {
	Connected  bool
	Reconnects uint64    // the client's reconnect count when sampled
	DownSince  time.Time // first sampled down for a reason other than rejected credentials; zero otherwise
	Rejected   bool      // its credentials or certificates are rejected (see IsCredentialError)
	RejectedAt uint64    // the reconnect count when the rejection was recorded
	Err        error     // the latest disconnect or failed connection attempt; nil once connected
}

// next is the status after an observation at now. An outage starts when first seen. Rejected credentials are a
// configuration fault that stops the outage clock, whatever errors follow (a server reports an expiry, then
// closes the socket). Only a new connection clears a rejection: the socket that reported it may still read as
// connected until it closes.
func (s Status) next(up bool, reconnects uint64, err error, now time.Time) Status {
	n := Status{Connected: up, Reconnects: reconnects, Err: err, DownSince: s.DownSince,
		Rejected: s.Rejected, RejectedAt: s.RejectedAt}
	switch {
	case up:
		n.DownSince, n.Err = time.Time{}, nil
		if reconnects > s.RejectedAt {
			n.Rejected, n.RejectedAt = false, 0
		}
	case IsCredentialError(err):
		n.DownSince = time.Time{}
		if !s.Rejected {
			n.Rejected, n.RejectedAt = true, reconnects
		}
	case s.Rejected:
		n.DownSince = time.Time{}
	case n.DownSince.IsZero():
		n.DownSince = now
	}
	return n
}

// cleared is the status after a successful (re)connect: the outage is over and the new connection's credentials
// were accepted.
func (s Status) cleared() Status {
	s.DownSince, s.Rejected, s.RejectedAt, s.Err = time.Time{}, false, 0, nil
	return s
}

// sample reads the client's status, which may wait on its lock, and records it unless a failure was recorded or
// the client changed connections meanwhile; a discarded sample is retried.
func (c *Conn) sample() {
	c.mu.Lock()
	events := c.events
	c.mu.Unlock()
	// The reconnect count names the connection: the client raises it, under its lock, before a new connection
	// turns connected. Equal counts around the status read tie the status to that connection.
	before := c.client.reconnects()
	up := c.client.connected()
	reconnects := c.client.reconnects()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.events != events || before != reconnects {
		c.poke()
		return
	}
	c.update(func(prev Status) Status { return prev.next(up, reconnects, c.ConnectError(), time.Now()) })
}

// succeeded records a successful (re)connect without waiting on the client.
func (c *Conn) succeeded() {
	c.setConnectError(nil)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.update(Status.cleared)
}

// fail records a disconnect or failed connection attempt without waiting on the client.
func (c *Conn) fail(err error) {
	c.setConnectError(err)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events++
	c.update(func(prev Status) Status { return prev.next(false, prev.Reconnects, err, time.Now()) })
}

// update stores the next status; c.mu must be held.
func (c *Conn) update(next func(Status) Status) {
	prev := c.Status()
	cur := next(prev)
	c.status.Store(&cur)
	if c.sampled != nil {
		c.sampled(prev, cur)
	}
}

// Status returns the connection's last observed status without waiting on the client.
func (c *Conn) Status() Status { return *c.status.Load() }

func (c *Conn) poke() {
	select {
	case c.nudge <- struct{}{}:
	default:
	}
}

// monitor samples the status every sampleEvery and after every connection event until the connection closes.
func (c *Conn) monitor() {
	t := time.NewTicker(sampleEvery)
	defer t.Stop()
	for {
		c.sample()
		select {
		case <-c.closed:
			c.sample()
			return
		case <-t.C:
		case <-c.nudge:
		}
	}
}

// errDisconnected stands for a disconnect the client reported without an error.
var errDisconnected = errors.New("disconnected")

// ConnectError returns the error of the latest disconnect or failed connection attempt, or nil when the latest
// attempt succeeded. It never waits on the client's lock.
func (c *Conn) ConnectError() error {
	if p := c.lastErr.Load(); p != nil {
		return *p
	}
	return nil
}

func (c *Conn) setConnectError(err error) {
	if err == nil {
		c.lastErr.Store(nil)
		return
	}
	c.lastErr.Store(&err)
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
	c := &Conn{closed: make(chan struct{}), nudge: make(chan struct{}, 1), sampled: cb.Sampled}
	c.client = clientStatus{
		connected:  func() bool { return c.IsConnected() },
		reconnects: func() uint64 { return c.Stats().Reconnects },
	}
	servers := make([]string, len(cfg.URLs))
	for i, u := range cfg.URLs {
		servers[i] = config.RedactURL(u)
	}
	c.status.Store(&Status{DownSince: time.Now()})
	var once sync.Once
	failed := func(err error) {
		if err == nil {
			err = errDisconnected
		}
		c.fail(err)
		c.poke()
	}
	// Callbacks never call into the client: a reconnect attempt may hold its lock for connect_timeout.
	connected := func(nc *nats.Conn) {
		log.Info("nats connected", "connection", name, "servers", servers)
		c.succeeded()
		c.poke()
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
			failed(err)
		}),
		nats.ClosedHandler(func(*nats.Conn) {
			log.Info("nats connection closed", "connection", name)
			once.Do(func() { close(c.closed) })
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			log.Error("nats error", "connection", name, "error", errString(err))
			if IsCredentialError(err) {
				failed(err)
			}
		}),
		nats.ReconnectErrHandler(func(_ *nats.Conn, err error) { failed(err) }),
	)
	nc, err := nats.Connect(strings.Join(cfg.URLs, ","), opts...)
	if err != nil {
		return nil, fmt.Errorf("nats connection %s: %w", name, err)
	}
	c.Conn = nc
	go c.monitor()
	return c, nil
}

// authErrors are the server's rejections of a connection's credentials, as nats.go reports them.
var authErrors = []struct {
	err  error
	text string
}{
	{nats.ErrAuthorization, nats.AUTHORIZATION_ERR},
	{nats.ErrAuthExpired, nats.AUTHENTICATION_EXPIRED_ERR},
	{nats.ErrAuthRevoked, nats.AUTHENTICATION_REVOKED_ERR},
	{nats.ErrAccountAuthExpired, nats.ACCOUNT_AUTHENTICATION_EXPIRED_ERR},
}

// tlsRejections are the TLS alerts a server sends when it refuses the client's certificate (RFC 8446 section
// 6.2). They reach the client only as alert text.
var tlsRejections = []string{
	"tls: bad certificate",
	"tls: unsupported certificate",
	"tls: revoked certificate",
	"tls: expired certificate",
	"tls: unknown certificate",
	"tls: unknown certificate authority",
	"tls: certificate required",
	"tls: access denied",
	"tls: failed to verify certificate",
}

// IsCredentialError reports whether err is a fault in the connection's credentials rather than a broker that
// cannot be reached: the server refusing its authentication (authorization violation, expired or revoked
// authentication), the server refusing its client certificate, the client failing to verify the server's
// certificate against the configured CA, or a seed or creds file that cannot be read or parsed when the client
// signs in again. Rejections during the handshake reach callers only as text, so the text is matched too.
func IsCredentialError(err error) bool {
	if err == nil {
		return false
	}
	if isCertificateError(err) || isCredentialFileError(err) {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, a := range authErrors {
		if errors.Is(err, a.err) || strings.Contains(msg, a.text) {
			return true
		}
	}
	for _, text := range tlsRejections {
		if strings.Contains(msg, text) {
			return true
		}
	}
	return false
}

func isCertificateError(err error) bool {
	var verify *tls.CertificateVerificationError
	var unknownCA x509.UnknownAuthorityError
	var invalid x509.CertificateInvalidError
	var hostname x509.HostnameError
	return errors.As(err, &verify) || errors.As(err, &unknownCA) || errors.As(err, &invalid) || errors.As(err, &hostname)
}

// isCredentialFileError matches a seed or creds file the client could not read or parse while signing in: the
// client opens no other file after start.
func isCredentialFileError(err error) bool {
	var path *fs.PathError
	msg := strings.ToLower(err.Error())
	return errors.As(err, &path) || strings.Contains(msg, "error signing nonce") || strings.Contains(msg, "nkeys: ")
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

// WaitConnected waits until c's sampled status is connected or ctx ends.
func (c *Conn) WaitConnected(ctx context.Context) error {
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for !c.Status().Connected {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
	return nil
}
