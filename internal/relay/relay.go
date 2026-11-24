// Package relay consumes webhook events from a JetStream durable consumer and delivers each to an application
// handler (a command or an HTTP endpoint), with retries, dead-lettering and graceful drain.
package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mrrobertkent/hookgate/internal/claimcheck"
	"github.com/mrrobertkent/hookgate/internal/metrics"
	"github.com/mrrobertkent/hookgate/internal/natsconn"
)

// Timings that are not configuration.
const (
	checkInterval     = 30 * time.Second // readiness re-check
	checkTimeout      = 10 * time.Second // all readiness requests together
	fetchWait         = 5 * time.Second  // one pull request's expiry
	fetchErrorPause   = time.Second      // after a failed pull request
	claimFetchTimeout = 30 * time.Second
	publishTimeout    = 10 * time.Second // dead-letter and replay publishes
	settleTimeout     = 10 * time.Second // DoubleAck
	connDrainTimeout  = 10 * time.Second
	defaultAckWait    = 30 * time.Second // the server default, until the consumer is read
)

// Relay delivers events from one durable consumer to one handler.
type Relay struct {
	cfg          *Config
	log          *slog.Logger
	m            *relayMetrics
	handler      deliverer
	passPatterns []string
	deadSubject  string

	nc     *natsconn.Conn
	js     jetstream.JetStream
	claims claimcheck.Store

	mu            sync.Mutex
	connected     bool // the connection's recorded status, which never waits on the client's lock
	checkErr      error
	pauseErr      error
	draining      bool
	cons          jetstream.Consumer
	ackWait       time.Duration
	stopFetch     context.CancelFunc // ends the pull request in progress
	streamCreated time.Time          // creation time of the event stream, from the last readiness check

	wake    chan struct{} // readiness may have changed
	recheck chan struct{} // run the readiness checks now
	owners  sync.WaitGroup
}

// New builds a relay, resolving the handler's secrets. It does not connect.
func New(cfg *Config, log *slog.Logger) (*Relay, error) {
	r := &Relay{
		cfg:         cfg,
		log:         log,
		m:           newMetrics(),
		deadSubject: cfg.deadSubject(),
		checkErr:    errors.New("not checked yet"),
		ackWait:     defaultAckWait,
		wake:        make(chan struct{}, 1),
		recheck:     make(chan struct{}, 1),
	}
	for _, p := range cfg.PassHeaders {
		r.passPatterns = append(r.passPatterns, strings.ToLower(p))
	}
	var err error
	if cfg.Handler.Command != nil {
		r.handler, err = newCommandDeliverer(*cfg.Handler.Command, log)
	} else {
		r.handler, err = newHTTPDeliverer(*cfg.Handler.HTTP)
	}
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Admin serves /healthz (process alive), /readyz (200 only while fetching is possible) and /metrics.
func (r *Relay) Admin() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if err := r.notReady(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "not ready: "+err.Error()+"\n")
			return
		}
		_, _ = io.WriteString(w, "ready\n")
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", metrics.ContentType)
		r.m.reg.Write(w)
	})
	return mux
}

// Run serves the admin endpoints, connects and delivers until ctx is cancelled, then drains: it stops fetching,
// gives in-flight events drain_timeout to finish, cancels the rest (each is naked with shutdown_nak_delay) and
// drains the connection.
func (r *Relay) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", r.cfg.AdminListen)
	if err != nil {
		return err
	}
	admin := &http.Server{Handler: r.Admin(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = admin.Serve(ln) }()
	defer admin.Close()
	if err := r.connect(); err != nil {
		return err
	}
	r.log.Info("relay started", "admin", ln.Addr().String(), "stream", r.cfg.Stream, "consumer", r.cfg.Consumer, "handler", r.cfg.handlerTarget())

	checkCtx, stopChecks := context.WithCancel(context.Background())
	checksDone := make(chan struct{})
	go func() { r.checkLoop(checkCtx); close(checksDone) }()
	work, cancelWork := context.WithCancel(context.Background())
	defer cancelWork()

	r.fetchLoop(ctx, work)
	r.drain(cancelWork)
	stopChecks()
	<-checksDone
	dctx, cancel := context.WithTimeout(context.Background(), connDrainTimeout)
	defer cancel()
	if err := r.nc.Drain(dctx); err != nil {
		r.log.Warn("nats drain", "error", err)
	}
	r.log.Info("relay stopped")
	return nil
}

func (r *Relay) connect() error {
	nc, err := natsconn.Connect("relay-"+r.cfg.Consumer, r.cfg.Connection.NATS, r.log, natsconn.Callbacks{
		Connected: func(*nats.Conn) { r.trigger() },
		Sampled: func(prev, cur natsconn.Status) {
			if prev.Connected != cur.Connected {
				r.setConnected(cur.Connected)
			}
		},
	})
	if err != nil {
		return err
	}
	js, err := jetstream.New(nc.Conn)
	if err != nil {
		nc.Close()
		return err
	}
	r.nc, r.js = nc, js
	if cc := r.cfg.ClaimCheck; cc != nil {
		r.claims = claimcheck.NewObjectStore(js, cc.ObjectStore.Bucket, claimcheck.ObjectStoreOptions{RequestTimeout: publishTimeout})
	}
	advisory := "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES." + r.cfg.Stream + "." + r.cfg.Consumer
	if _, err := nc.Subscribe(advisory, func(*nats.Msg) {
		r.m.maxDeliveries.Inc()
		r.log.Error("a message of this consumer reached the server's max_deliver and will not be redelivered", "stream", r.cfg.Stream, "consumer", r.cfg.Consumer)
	}); err != nil {
		nc.Close()
		return fmt.Errorf("subscribe %s: %w", advisory, err)
	}
	return nil
}

// trigger asks the check loop to run now.
func (r *Relay) trigger() {
	select {
	case r.recheck <- struct{}{}:
	default:
	}
}

// signal wakes the fetch loop after a readiness change.
func (r *Relay) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *Relay) setConnected(up bool) {
	r.mu.Lock()
	r.connected = up
	r.mu.Unlock()
	r.m.connected.SetBool(up)
	r.updateReady()
}

// notReady returns why the relay is not ready, or nil.
func (r *Relay) notReady() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.notReadyLocked()
}

func (r *Relay) notReadyLocked() error {
	switch {
	case r.draining:
		return errors.New("draining")
	case r.pauseErr != nil:
		return fmt.Errorf("paused until restart: %w", r.pauseErr)
	case !r.connected:
		return errors.New("not connected to NATS")
	case r.checkErr != nil:
		return r.checkErr
	}
	return nil
}

func (r *Relay) updateReady() {
	r.m.ready.SetBool(r.notReady() == nil)
	r.signal()
}

// pause stops fetching after a handler configuration fault, until the relay is restarted.
func (r *Relay) pause(detail string) {
	r.mu.Lock()
	first := r.pauseErr == nil
	stopFetch := r.stopFetch
	if first {
		r.pauseErr = errors.New(detail)
	}
	r.mu.Unlock()
	if stopFetch != nil {
		stopFetch()
	}
	if first {
		r.log.Error("HANDLER CONFIGURATION FAULT: relay paused and not ready; events wait in the stream; fix the handler and restart the relay", "detail", detail)
		r.m.paused.Set(1)
		r.updateReady()
	}
}

func (r *Relay) paused() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pauseErr != nil
}

func (r *Relay) returnPaused(msg jetstream.Msg) {
	meta, err := msg.Metadata()
	delay := r.cfg.Backoff[0]
	if err == nil {
		delay = r.backoff(meta)
	}
	if err := msg.NakWithDelay(delay); err != nil {
		r.m.settleFailed.Inc()
	}
	r.m.deliveries.Inc(outcomePaused)
}

// ready returns the consumer to fetch from when the relay is ready.
func (r *Relay) ready() (jetstream.Consumer, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.notReadyLocked() != nil {
		return nil, false
	}
	return r.cons, true
}

// waitReady blocks until the relay is ready or ctx ends.
func (r *Relay) waitReady(ctx context.Context) (jetstream.Consumer, bool) {
	for {
		if cons, ok := r.ready(); ok {
			return cons, true
		}
		select {
		case <-ctx.Done():
			return nil, false
		case <-r.wake:
		case <-time.After(time.Second):
		}
	}
}

// fetchLoop pulls at most as many messages as there are free handler slots and gives each to its own owner
// goroutine, until ctx ends.
func (r *Relay) fetchLoop(ctx, work context.Context) {
	slots := make(chan struct{}, r.cfg.Concurrency)
	for {
		if _, ok := r.waitReady(ctx); !ok {
			return
		}
		n := acquire(ctx, slots)
		if n == 0 {
			return
		}
		cons, ok := r.ready()
		if !ok {
			release(slots, n)
			continue
		}
		got, err := r.fetch(ctx, work, cons, n, slots)
		release(slots, n-got)
		if err != nil && ctx.Err() == nil {
			r.log.Warn("fetch", "error", err)
			r.trigger()
			sleep(ctx, fetchErrorPause)
		}
	}
}

func (r *Relay) fetch(ctx, work context.Context, cons jetstream.Consumer, n int, slots chan struct{}) (int, error) {
	// The deadline sets the pull request's expiry; cancelling ends the request at shutdown or on a pause.
	fctx, cancel := context.WithTimeout(ctx, fetchWait)
	defer cancel()
	r.mu.Lock()
	r.stopFetch = cancel
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.stopFetch = nil
		r.mu.Unlock()
	}()
	if r.paused() {
		// Paused after the readiness check but before this pull could be stopped.
		return 0, nil
	}
	batch, err := cons.Fetch(n, jetstream.FetchContext(fctx))
	if err != nil {
		return 0, err
	}
	got := 0
	for msg := range batch.Messages() {
		if r.paused() {
			// Already received when the relay paused: returned without starting a handler.
			r.returnPaused(msg)
			continue
		}
		got++
		r.owners.Add(1)
		r.m.inFlight.Add(1)
		go func() {
			defer func() {
				r.m.inFlight.Add(-1)
				<-slots
				r.owners.Done()
			}()
			r.own(work, msg)
		}()
	}
	err = batch.Error()
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		err = nil
	}
	return got, err
}

// acquire takes one free slot, waiting for it, then every other free slot; it returns 0 when ctx ends first.
func acquire(ctx context.Context, slots chan struct{}) int {
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return 0
	}
	n := 1
	for n < cap(slots) {
		select {
		case slots <- struct{}{}:
			n++
		default:
			return n
		}
	}
	return n
}

func release(slots chan struct{}, n int) {
	for range n {
		<-slots
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// drain waits drain_timeout for in-flight events, then cancels the rest and waits for their owners to settle
// them.
func (r *Relay) drain(cancelWork context.CancelFunc) {
	r.mu.Lock()
	r.draining = true
	r.mu.Unlock()
	r.updateReady()
	done := make(chan struct{})
	go func() { r.owners.Wait(); close(done) }()
	t := time.NewTimer(r.cfg.DrainTimeout)
	defer t.Stop()
	select {
	case <-done:
		return
	case <-t.C:
	}
	r.log.Warn("drain timeout: cancelling in-flight handlers; their events are redelivered", "drain_timeout", r.cfg.DrainTimeout.String())
	cancelWork()
	<-done
}
