// Package gateway serves the webhook endpoints: it verifies each request against its source's checks and
// forwards only verified requests upstream.
package gateway

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/mrrobertkent/hookgate/internal/config"
	"github.com/mrrobertkent/hookgate/internal/verify"
)

type source struct {
	cfg       config.Source
	verifiers []verify.Verifier
	forwarder *forwarder
	notReady  error // set when a secret is missing; the source then answers 503 and forwards nothing
}

// Gateway is the webhook handler plus its admin endpoints.
type Gateway struct {
	cfg     *config.Config
	log     *slog.Logger
	sources map[string]*source
	sem     chan struct{}
	m       *metrics
}

// New builds the gateway. A source whose secrets cannot be resolved stays configured but not ready: it answers
// 503 and never forwards, while every other source keeps serving.
func New(cfg *config.Config, log *slog.Logger) *Gateway {
	g := &Gateway{
		cfg:     cfg,
		log:     log,
		sources: map[string]*source{},
		sem:     make(chan struct{}, cfg.MaxConcurrent),
		m:       newMetrics(),
	}
	for _, sc := range cfg.Sources {
		s := &source{cfg: sc}
		for _, c := range sc.Checks {
			v, err := verify.Build(c)
			if err != nil {
				s.notReady = err
				break
			}
			s.verifiers = append(s.verifiers, v)
		}
		if s.notReady == nil {
			f, err := newForwarder(sc.Forward)
			if err != nil {
				s.notReady = err
			}
			s.forwarder = f
		}
		if s.notReady != nil {
			log.Error("source not ready; it answers 503 until its secrets are set", "source", sc.Name, "error", s.notReady)
		}
		g.m.setReady(sc.Name, s.notReady == nil)
		g.sources[sc.Path] = s
	}
	return g
}

// ServeHTTP handles webhook requests.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	s, ok := g.sources[r.URL.Path]
	if !ok {
		http.NotFound(w, r)
		return
	}
	name := s.cfg.Name
	reply := func(status int, outcome, reason string, attrs ...any) {
		g.m.request(name, outcome)
		w.WriteHeader(status)
		args := append([]any{"source", name, "outcome", outcome, "status", status,
			"remote", r.RemoteAddr, "forwarded_for", r.Header.Get("X-Forwarded-For"),
			"duration_ms", time.Since(start).Milliseconds()}, attrs...)
		if reason != "" {
			args = append(args, "reason", reason)
		}
		level := slog.LevelInfo
		if status >= 500 {
			level = slog.LevelError
		} else if outcome != "forwarded" {
			level = slog.LevelWarn
		}
		g.log.Log(r.Context(), level, "webhook", args...)
	}

	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		reply(http.StatusMethodNotAllowed, "bad_request", "method "+r.Method)
		return
	}
	if r.URL.RawQuery != "" && !s.cfg.AllowQuery {
		reply(http.StatusBadRequest, "bad_request", "query string not allowed")
		return
	}
	if ce := r.Header.Get("Content-Encoding"); ce != "" && ce != "identity" {
		reply(http.StatusUnsupportedMediaType, "bad_request", "content-encoding "+ce)
		return
	}
	if s.notReady != nil {
		reply(http.StatusServiceUnavailable, "not_ready", "source secrets not set")
		return
	}
	select {
	case g.sem <- struct{}{}:
		defer func() { <-g.sem }()
	default:
		reply(http.StatusServiceUnavailable, "busy", "max_concurrent reached")
		return
	}
	g.m.inFlight.Add(1)
	defer g.m.inFlight.Add(-1)

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			reply(http.StatusRequestEntityTooLarge, "too_large", "body over max_body_bytes")
			return
		}
		reply(http.StatusBadRequest, "bad_request", "reading body: "+err.Error())
		return
	}
	for _, v := range s.verifiers {
		if verr := v.Verify(r.Header, body); verr != nil {
			reply(s.cfg.RejectStatus, "rejected", verr.Error(), "bytes", len(body))
			return
		}
	}

	res, err := s.forwarder.deliver(r.Context(), r, body, func(result string) { g.m.attempt(name, result) })
	if err != nil {
		reply(http.StatusBadGateway, "upstream_error", err.Error(), "bytes", len(body))
		return
	}
	if ct := res.header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	outcome := "forwarded"
	if res.status >= 400 {
		outcome = "upstream_error"
	}
	g.m.request(name, outcome)
	w.WriteHeader(res.status)
	_, _ = w.Write(res.body)
	level := slog.LevelInfo
	if res.status >= 400 {
		level = slog.LevelError
	}
	g.log.Log(r.Context(), level, "webhook", "source", name, "outcome", outcome, "status", res.status,
		"remote", r.RemoteAddr, "forwarded_for", r.Header.Get("X-Forwarded-For"),
		"bytes", len(body), "duration_ms", time.Since(start).Milliseconds())
}

// Admin serves /healthz (process alive), /readyz (503 while draining) and /metrics.
func (g *Gateway) Admin() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if g.m.draining.Load() {
			http.Error(w, "draining", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ready\n")
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		g.m.write(w)
	})
	return mux
}

// Run serves until ctx is cancelled, then drains: /readyz turns 503, the listener keeps accepting for
// drain_delay so load balancers can stop routing here, and in-flight requests get shutdown_timeout to finish.
func (g *Gateway) Run(ctx context.Context) error {
	srv := &http.Server{
		Addr:              g.cfg.Listen,
		Handler:           g,
		ReadHeaderTimeout: g.cfg.ReadHeaderTimeout,
		ReadTimeout:       g.cfg.ReadTimeout,
		WriteTimeout:      g.cfg.WriteTimeout,
		IdleTimeout:       g.cfg.IdleTimeout,
		MaxHeaderBytes:    64 << 10,
	}
	admin := &http.Server{Addr: g.cfg.AdminListen, Handler: g.Admin(), ReadHeaderTimeout: 5 * time.Second}

	ln, err := net.Listen("tcp", srv.Addr)
	if err != nil {
		return err
	}
	aln, err := net.Listen("tcp", admin.Addr)
	if err != nil {
		ln.Close()
		return err
	}
	errc := make(chan error, 2)
	go func() { errc <- srv.Serve(ln) }()
	go func() { errc <- admin.Serve(aln) }()
	g.log.Info("listening", "addr", ln.Addr().String(), "admin", aln.Addr().String(), "sources", strconv.Itoa(len(g.sources)))

	select {
	case serveErr := <-errc:
		return serveErr
	case <-ctx.Done():
	}
	g.m.draining.Store(true)
	g.log.Info("draining", "drain_delay", g.cfg.DrainDelay.String())
	time.Sleep(g.cfg.DrainDelay)
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), g.cfg.ShutdownTimeout)
	defer cancel()
	err = srv.Shutdown(sctx)
	_ = admin.Shutdown(sctx)
	g.log.Info("stopped")
	return err
}
