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
	"net/netip"
	"strconv"
	"strings"
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

// route holds the sources that share one path: those that list hosts, by host, and the one without hosts.
type route struct {
	hosts map[string]*source
	any   *source
}

// Gateway is the webhook handler plus its admin endpoints.
type Gateway struct {
	cfg    *config.Config
	log    *slog.Logger
	routes map[string]*route // by exact path
	sem    chan struct{}
	m      *metrics
}

// New builds the gateway. A source whose secrets cannot be resolved stays configured but not ready: it answers
// 503 and never forwards, while every other source keeps serving.
func New(cfg *config.Config, log *slog.Logger) *Gateway {
	g := &Gateway{
		cfg:    cfg,
		log:    log,
		routes: map[string]*route{},
		sem:    make(chan struct{}, cfg.MaxConcurrent),
		m:      newMetrics(),
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
		g.add(s)
	}
	return g
}

// add registers s under its path and hosts; config validation guarantees every (host, path) pair is unique.
func (g *Gateway) add(s *source) {
	rt := g.routes[s.cfg.Path]
	if rt == nil {
		rt = &route{hosts: map[string]*source{}}
		g.routes[s.cfg.Path] = rt
	}
	if len(s.cfg.Hosts) == 0 {
		rt.any = s
	}
	for _, h := range s.cfg.Hosts {
		rt.hosts[h] = s
	}
}

// match selects the source for a request: the path must match exactly, and a source listing the request's
// host beats the path's source without hosts. Only Host (HTTP/2 :authority) counts, never X-Forwarded-Host.
func (g *Gateway) match(r *http.Request) *source {
	rt := g.routes[r.URL.Path]
	if rt == nil {
		return nil
	}
	if s := rt.hosts[requestHost(r.Host)]; s != nil {
		return s
	}
	return rt.any
}

// requestHost normalises a Host header value the way config.NormalizeHost normalises configured hosts:
// lowercase, port and IPv6 brackets removed, one trailing dot removed, IP literals in canonical form.
func requestHost(h string) string {
	h = strings.ToLower(h)
	if host, _, err := net.SplitHostPort(h); err == nil {
		h = host
	} else if inner, ok := strings.CutPrefix(h, "["); ok {
		h = strings.TrimSuffix(inner, "]")
	}
	h = strings.TrimSuffix(h, ".")
	if a, err := netip.ParseAddr(h); err == nil {
		return a.String()
	}
	return h
}

// ServeHTTP handles webhook requests.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	s := g.match(r)
	if s == nil {
		http.NotFound(w, r)
		return
	}
	x := &exchange{g: g, w: w, r: r, name: s.cfg.Name, start: start}
	if rej := precheck(w, r, s); rej != nil {
		x.reply(*rej)
		return
	}
	select {
	case g.sem <- struct{}{}:
		defer func() { <-g.sem }()
	default:
		x.reply(rejection{http.StatusServiceUnavailable, "busy", "max_concurrent reached", nil})
		return
	}
	g.m.inFlight.Add(1)
	defer g.m.inFlight.Add(-1)

	body, rej := readBody(w, r, s.cfg.MaxBodyBytes)
	if rej != nil {
		x.reply(*rej)
		return
	}
	if rej := s.verify(r, body); rej != nil {
		x.reply(*rej)
		return
	}
	res, err := s.forwarder.deliver(r.Context(), r, body, func(result string) { g.m.attempt(s.cfg.Name, result) })
	if err != nil {
		x.reply(rejection{http.StatusBadGateway, "upstream_error", err.Error(), []any{"bytes", len(body)}})
		return
	}
	x.relay(res, len(body))
}

// exchange is one request to a matched source, for writing its response, log line and metrics.
type exchange struct {
	g     *Gateway
	w     http.ResponseWriter
	r     *http.Request
	name  string
	start time.Time
}

// rejection is a response hookgate answers itself, without the upstream's.
type rejection struct {
	status          int
	outcome, reason string
	attrs           []any
}

func (x *exchange) reply(rej rejection) {
	x.g.m.request(x.name, rej.outcome)
	x.w.WriteHeader(rej.status)
	args := append([]any{"source", x.name, "outcome", rej.outcome, "status", rej.status,
		"remote", x.r.RemoteAddr, "forwarded_for", x.r.Header.Get("X-Forwarded-For"),
		"duration_ms", time.Since(x.start).Milliseconds()}, rej.attrs...)
	if rej.reason != "" {
		args = append(args, "reason", rej.reason)
	}
	level := slog.LevelWarn
	if rej.status >= 500 {
		level = slog.LevelError
	}
	x.g.log.Log(x.r.Context(), level, "webhook", args...)
}

// relay returns the upstream's final response to the sender.
func (x *exchange) relay(res *upstreamResult, n int) {
	if ct := res.header.Get("Content-Type"); ct != "" {
		x.w.Header().Set("Content-Type", ct)
	}
	outcome, level := "forwarded", slog.LevelInfo
	if res.status >= 400 {
		outcome, level = "upstream_error", slog.LevelError
	}
	x.g.m.request(x.name, outcome)
	x.w.WriteHeader(res.status)
	_, _ = x.w.Write(res.body)
	x.g.log.Log(x.r.Context(), level, "webhook", "source", x.name, "outcome", outcome, "status", res.status,
		"remote", x.r.RemoteAddr, "forwarded_for", x.r.Header.Get("X-Forwarded-For"),
		"bytes", n, "duration_ms", time.Since(x.start).Milliseconds())
}

// precheck rejects a request that no check could accept, before its body is read.
func precheck(w http.ResponseWriter, r *http.Request, s *source) *rejection {
	switch ce := r.Header.Get("Content-Encoding"); {
	case r.Method != http.MethodPost:
		w.Header().Set("Allow", http.MethodPost)
		return &rejection{http.StatusMethodNotAllowed, "bad_request", "method " + r.Method, nil}
	case r.URL.RawQuery != "" && !s.cfg.AllowQuery:
		return &rejection{http.StatusBadRequest, "bad_request", "query string not allowed", nil}
	case ce != "" && ce != "identity":
		return &rejection{http.StatusUnsupportedMediaType, "bad_request", "content-encoding " + ce, nil}
	case s.notReady != nil:
		return &rejection{http.StatusServiceUnavailable, "not_ready", "source secrets not set", nil}
	}
	return nil
}

func readBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, *rejection) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err == nil {
		return body, nil
	}
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		return nil, &rejection{http.StatusRequestEntityTooLarge, "too_large", "body over max_body_bytes", nil}
	}
	return nil, &rejection{http.StatusBadRequest, "bad_request", "reading body: " + err.Error(), nil}
}

func (s *source) verify(r *http.Request, body []byte) *rejection {
	for _, v := range s.verifiers {
		if err := v.Verify(r.Header, body); err != nil {
			return &rejection{s.cfg.RejectStatus, "rejected", err.Error(), []any{"bytes", len(body)}}
		}
	}
	return nil
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
	g.log.Info("listening", "addr", ln.Addr().String(), "admin", aln.Addr().String(), "sources", strconv.Itoa(len(g.cfg.Sources)))

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
