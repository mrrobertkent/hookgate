// Package gateway serves the webhook endpoints: it verifies each request against its source's checks and
// delivers only verified requests to the source's output.
package gateway

import (
	"bytes"
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
	"github.com/mrrobertkent/hookgate/internal/metrics"
	"github.com/mrrobertkent/hookgate/internal/natsconn"
	"github.com/mrrobertkent/hookgate/internal/output"
	"github.com/mrrobertkent/hookgate/internal/verify"
)

const contentType = "Content-Type"

type source struct {
	cfg       config.Source
	verifiers []verify.Verifier
	out       output.Output
	notReady  error // set when a secret is missing; the source then answers 503 and delivers nothing
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
	m      *gatewayMetrics
	conns  map[string]*natsconn.Conn
	links  *links
	stop   context.CancelFunc // ends the output watchers
}

// New builds the gateway and opens the connections its outputs use. A source whose secrets cannot be resolved
// stays configured but not ready: it answers 503 and never delivers, while every other source keeps serving.
// Close releases the connections when Run is not used.
func New(cfg *config.Config, log *slog.Logger) *Gateway {
	g := &Gateway{
		cfg:    cfg,
		log:    log,
		routes: map[string]*route{},
		sem:    make(chan struct{}, cfg.MaxConcurrent),
		m:      newMetrics(),
		links:  newLinks(),
	}
	g.m.reg.GaugeFunc("hookgate_ready", "1 while /readyz answers 200, else 0.", func() float64 {
		if g.notReady() == "" {
			return 1
		}
		return 0
	})
	ctx, stop := context.WithCancel(context.Background())
	g.stop = stop
	connErrs, watchers := g.connect()
	for _, sc := range cfg.Sources {
		s := g.build(sc, connErrs)
		if w, ok := s.out.(output.Watcher); ok {
			conn := sc.Output.JetStream.Connection
			watchers.add(conn, w)
			g.links.use(conn)
			go w.Run(ctx)
		}
		g.add(s)
	}
	return g
}

// build resolves a source's checks and output.
func (g *Gateway) build(sc config.Source, connErrs map[string]error) *source {
	s := &source{cfg: sc}
	for _, c := range sc.Checks {
		v, err := verify.Build(c)
		if err != nil {
			s.notReady = err
			break
		}
		s.verifiers = append(s.verifiers, v)
	}
	if s.notReady == nil && sc.Output.JetStream != nil {
		s.notReady = connErrs[sc.Output.JetStream.Connection]
	}
	if s.notReady == nil {
		s.out, s.notReady = output.Build(sc, output.Deps{
			Conns:       g.conns,
			Metrics:     g.m.outputs,
			Log:         g.log,
			ReportReady: func(err error) { g.m.setReady(sc.Name, err == nil) },
		})
	}
	if s.notReady != nil {
		g.log.Error("source not ready; it answers 503 until its secrets are set", "source", sc.Name, "error", s.notReady)
	}
	_, watched := s.out.(output.Watcher)
	g.m.setReady(sc.Name, s.notReady == nil && !watched)
	return s
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

// match selects the source for a request: the decoded path must match exactly, and a source listing the
// request's host beats the path's source without hosts. The host is r.Host, the effective authority (the
// absolute-form request target, else Host; :authority in HTTP/2), never X-Forwarded-Host.
func (g *Gateway) match(r *http.Request) *source {
	rt := g.routes[r.URL.Path]
	if rt == nil {
		return nil
	}
	if h, ok := requestHost(r.Host); ok {
		if s := rt.hosts[h]; s != nil {
			return s
		}
	}
	return rt.any
}

// requestHost normalises a request authority the way config.NormalizeHost normalises configured hosts:
// lowercase, port and IPv6 brackets removed, one trailing dot removed, IP literals in canonical form. It
// reports false for a malformed authority (non-ASCII bytes, a non-numeric or overlong port, unbalanced brackets, brackets
// around anything but an IPv6 address without a zone, a bare IPv6 address), which then matches no listed host.
func requestHost(authority string) (string, bool) {
	for i := 0; i < len(authority); i++ {
		if authority[i] >= 0x80 { // strings.ToLower would map some non-ASCII letters to ASCII ones
			return "", false
		}
	}
	host, ok := splitAuthority(strings.ToLower(authority))
	if !ok {
		return "", false
	}
	if v6, ok := strings.CutPrefix(host, "["); ok {
		a, err := netip.ParseAddr(strings.TrimSuffix(v6, "]"))
		if err != nil || !a.Is6() || a.Zone() != "" {
			return "", false
		}
		return a.String(), true
	}
	if strings.ContainsAny(host, ":[]") {
		return "", false
	}
	host = strings.TrimSuffix(host, ".")
	if a, err := netip.ParseAddr(host); err == nil {
		return a.String(), true
	}
	return host, true
}

// splitAuthority drops the port from host[:port] or [v6][:port] (RFC 3986: the port is up to five digits,
// possibly none), keeping the brackets for requestHost to check.
func splitAuthority(a string) (string, bool) {
	host, port := a, ""
	if strings.HasPrefix(a, "[") {
		end := strings.IndexByte(a, ']')
		if end < 0 {
			return "", false
		}
		host, port = a[:end+1], a[end+1:]
		if port != "" && port[0] != ':' {
			return "", false
		}
	} else if i := strings.LastIndexByte(a, ':'); i >= 0 {
		host, port = a[:i], a[i:]
	}
	port = strings.TrimPrefix(port, ":")
	if len(port) > 5 || strings.Trim(port, "0123456789") != "" {
		return "", false
	}
	return host, true
}

// ServeHTTP handles webhook requests.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	received := time.Now()
	s := g.match(r)
	if s == nil {
		http.NotFound(w, r)
		return
	}
	x := &exchange{g: g, w: w, r: r, name: s.cfg.Name, start: received}
	if rej := precheck(w, r, s); rej != nil {
		x.reply(*rej)
		return
	}
	select {
	case g.sem <- struct{}{}:
		defer func() { <-g.sem }()
	default:
		x.reply(rejection{status: http.StatusServiceUnavailable, outcome: "busy", reason: "max_concurrent reached"})
		return
	}
	g.m.inFlight.Add(1)
	defer g.m.inFlight.Add(-1)

	body, rej := readBody(w, r, s.cfg.MaxBodyBytes)
	if rej != nil {
		x.reply(*rej)
		return
	}
	x.bytes = len(body)
	verified, rej := s.verify(r, body)
	if rej == nil {
		// Readiness can change while a slow body is read.
		rej = outputUnready(s)
	}
	if rej != nil {
		x.reply(*rej)
		return
	}
	x.finish(s.out.Deliver(r.Context(), output.Event{
		Source:     s.cfg.Name,
		Header:     r.Header,
		Body:       body,
		EventID:    verified.EventID,
		SignedAt:   verified.SignedAt,
		ReceivedAt: received,
	}))
}

// exchange is one request to a matched source, for writing its response, log line and metrics.
type exchange struct {
	g     *Gateway
	w     http.ResponseWriter
	r     *http.Request
	name  string
	start time.Time
	bytes int // body size; 0 until the body is read
}

// rejection is a response hookgate answers before delivering anything.
type rejection struct {
	status          int
	outcome, reason string
	header          http.Header
	body            []byte
}

func (x *exchange) reply(rej rejection) {
	x.finish(output.Result{Status: rej.status, Outcome: rej.outcome, Reason: rej.reason, Header: rej.header, Body: rej.body})
}

// finish writes the response, counts it and logs one line.
func (x *exchange) finish(res output.Result) {
	for k, v := range res.Header {
		x.w.Header()[k] = v
	}
	x.g.m.request(x.name, res.Outcome)
	x.w.WriteHeader(res.Status)
	_, _ = x.w.Write(res.Body)
	args := []any{"source", x.name, "outcome", res.Outcome, "status", res.Status,
		"remote", x.r.RemoteAddr, "forwarded_for", x.r.Header.Get("X-Forwarded-For"),
		"duration_ms", time.Since(x.start).Milliseconds()}
	if x.bytes > 0 {
		args = append(args, "bytes", x.bytes)
	}
	if res.Reason != "" {
		args = append(args, "reason", res.Reason)
	}
	x.g.log.Log(x.r.Context(), logLevel(res), "webhook", args...)
}

func logLevel(res output.Result) slog.Level {
	switch {
	case res.Status >= 500, res.Outcome == "upstream_error":
		return slog.LevelError
	case res.Status >= 400:
		return slog.LevelWarn
	}
	return slog.LevelInfo
}

var unavailableBody = []byte("unavailable\n")

// precheck rejects a request that no check could accept or no output could take, before its body is read.
func precheck(w http.ResponseWriter, r *http.Request, s *source) *rejection {
	switch ce := r.Header.Get("Content-Encoding"); {
	case r.Method != http.MethodPost:
		w.Header().Set("Allow", http.MethodPost)
		return &rejection{status: http.StatusMethodNotAllowed, outcome: "bad_request", reason: "method " + r.Method}
	case r.URL.RawQuery != "" && !s.cfg.AllowQuery:
		return &rejection{status: http.StatusBadRequest, outcome: "bad_request", reason: "query string not allowed"}
	case ce != "" && ce != "identity":
		return &rejection{status: http.StatusUnsupportedMediaType, outcome: "bad_request", reason: "content-encoding " + ce}
	case s.notReady != nil:
		return &rejection{status: http.StatusServiceUnavailable, outcome: "not_ready", reason: "source secrets not set"}
	}
	return outputUnready(s)
}

// outputUnready rejects a request while the source's output cannot deliver.
func outputUnready(s *source) *rejection {
	err := s.out.Ready()
	if err == nil {
		return nil
	}
	outcome := "not_ready"
	if errors.Is(err, output.ErrNotConnected) {
		outcome = "output_error"
	}
	return &rejection{status: http.StatusServiceUnavailable, outcome: outcome, reason: err.Error(),
		header: http.Header{contentType: {"text/plain; charset=utf-8"}}, body: unavailableBody}
}

func readBody(w http.ResponseWriter, r *http.Request, limit int64) ([]byte, *rejection) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err == nil {
		return body, nil
	}
	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		return nil, &rejection{status: http.StatusRequestEntityTooLarge, outcome: "too_large", reason: "body over max_body_bytes"}
	}
	return nil, &rejection{status: http.StatusBadRequest, outcome: "bad_request", reason: "reading body: " + err.Error()}
}

// verify runs every check and returns what they authenticated about the request.
func (s *source) verify(r *http.Request, body []byte) (verify.Result, *rejection) {
	var out verify.Result
	for _, v := range s.verifiers {
		res, err := v.Verify(r.Header, body)
		// A JetStream output publishes the ID, so a malformed one is a bad request; other outputs keep the
		// configured rejection.
		if errors.Is(err, verify.ErrInvalidEventID) && s.cfg.Output.JetStream != nil {
			return verify.Result{}, &rejection{status: http.StatusBadRequest, outcome: "bad_request", reason: err.Error()}
		}
		if err != nil {
			return verify.Result{}, &rejection{status: s.cfg.RejectStatus, outcome: "rejected", reason: err.Error()}
		}
		if res.EventID != "" {
			out.EventID = res.EventID
		}
		if !res.SignedAt.IsZero() {
			out.SignedAt = res.SignedAt
		}
	}
	return out, nil
}

// Admin serves /healthz (process alive), /readyz (503 while draining, or while an output connection is lost when
// readiness.require_output_connections is set) and /metrics.
func (g *Gateway) Admin() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if reason := g.notReady(); reason != "" {
			http.Error(w, reason, http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ready\n")
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(contentType, metrics.ContentType)
		// Rendered first: the registry holds each family's lock while it writes, and a slow scraper must not
		// hold up the counters deliveries update.
		var buf bytes.Buffer
		g.m.reg.Write(&buf)
		_, _ = w.Write(buf.Bytes())
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
		return errors.Join(err, g.closeWithin(ctx))
	}
	aln, err := net.Listen("tcp", admin.Addr)
	if err != nil {
		ln.Close()
		return errors.Join(err, g.closeWithin(ctx))
	}
	errc := make(chan error, 2)
	go func() { errc <- srv.Serve(ln) }()
	go func() { errc <- admin.Serve(aln) }()
	g.log.Info("listening", "addr", ln.Addr().String(), "admin", aln.Addr().String(), "sources", strconv.Itoa(len(g.cfg.Sources)))

	select {
	case serveErr := <-errc:
		return errors.Join(serveErr, g.closeWithin(ctx))
	case <-ctx.Done():
	}
	g.m.draining.Store(true)
	g.log.Info("draining", "drain_delay", g.cfg.DrainDelay.String())
	time.Sleep(g.cfg.DrainDelay)
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), g.cfg.ShutdownTimeout)
	defer cancel()
	err = srv.Shutdown(sctx)
	// Every admitted request has finished its publish (or the budget is spent); only now may the
	// connections go.
	cerr := g.Close(sctx)
	_ = admin.Shutdown(sctx)
	g.log.Info("stopped")
	return errors.Join(err, cerr)
}

// closeWithin releases the connections and watchers when Run ends without a shutdown, bounded by
// shutdown_timeout.
func (g *Gateway) closeWithin(ctx context.Context) error {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), g.cfg.ShutdownTimeout)
	defer cancel()
	return g.Close(cctx)
}
