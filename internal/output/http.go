package output

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/mrrobertkent/hookgate/internal/config"
	"github.com/mrrobertkent/hookgate/internal/metrics"
)

// maxUpstreamResponse bounds the upstream response body relayed to the sender.
const maxUpstreamResponse = 64 << 10

// httpOutput forwards verified bytes to HTTP upstreams.
type httpOutput struct {
	source     string
	cfg        config.HTTPOutput
	setHeaders http.Header
	pass       headerFilter
	client     *http.Client
	resolver   *net.Resolver
	attempts   *metrics.Counter
}

type upstreamResult struct {
	status int
	header http.Header
	body   []byte
}

func newHTTP(src config.Source, d Deps) (*httpOutput, error) {
	cfg := *src.Output.HTTP
	f := &httpOutput{
		source:     src.Name,
		cfg:        cfg,
		setHeaders: http.Header{},
		pass:       newHeaderFilter(cfg.PassHeaders, src.CredentialHeaders()),
		resolver:   net.DefaultResolver,
		attempts:   d.Metrics.UpstreamAttempts,
		client: &http.Client{
			// One connection per attempt: a pooled connection the upstream already closed would fail the
			// attempt, and an attempt is cheap at webhook rates.
			Transport: &http.Transport{
				Proxy:                 nil,
				DisableKeepAlives:     true,
				DisableCompression:    true,
				DialContext:           (&net.Dialer{Timeout: 2 * time.Second}).DialContext,
				TLSHandshakeTimeout:   5 * time.Second,
				ResponseHeaderTimeout: cfg.AttemptTimeout,
			},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
	set, err := resolveSetHeaders(cfg.SetHeaders)
	if err != nil {
		return nil, err
	}
	for name, v := range set {
		f.setHeaders.Set(name, v)
	}
	return f, nil
}

// Ready is always nil: an upstream's health shows per request.
func (f *httpOutput) Ready() error { return nil }

// Deliver forwards ev. The sender gets the upstream's final status, Content-Type and body (bounded), or 502
// when no upstream answered.
func (f *httpOutput) Deliver(ctx context.Context, ev Event) Result {
	res, err := f.deliver(ctx, ev)
	if err != nil {
		return Result{Status: http.StatusBadGateway, Outcome: "upstream_error", Reason: err.Error()}
	}
	out := Result{Status: res.status, Outcome: "forwarded", Body: res.body}
	if res.status >= 400 {
		out.Outcome = "upstream_error"
	}
	if ct := res.header.Get(contentType); ct != "" {
		out.Header = http.Header{contentType: {ct}}
	}
	return out
}

// targets expands the configured URLs into attempt targets. With resolve_all, each host name is resolved to
// every address it has (for example every task behind a service name), in random order.
func (f *httpOutput) targets(ctx context.Context) []*url.URL {
	var out []*url.URL
	for _, raw := range f.cfg.URLs {
		u, _ := url.Parse(raw) // validated at load
		if !f.cfg.ResolveAll || net.ParseIP(u.Hostname()) != nil {
			out = append(out, u)
			continue
		}
		addrs, err := f.resolver.LookupHost(ctx, u.Hostname())
		if err != nil || len(addrs) == 0 {
			out = append(out, u)
			continue
		}
		//nolint:gosec // G404: spreads attempts across upstream instances; not a security decision.
		rand.Shuffle(len(addrs), func(i, j int) { addrs[i], addrs[j] = addrs[j], addrs[i] })
		for _, a := range addrs {
			v := *u
			if p := u.Port(); p != "" {
				v.Host = net.JoinHostPort(a, p)
			} else {
				v.Host = a
			}
			out = append(out, &v)
		}
	}
	return out
}

// deliver sends the verified bytes upstream. Transport errors and 5xx responses move on to the next target;
// any other response is final. The verified body is the only body ever sent.
func (f *httpOutput) deliver(ctx context.Context, ev Event) (*upstreamResult, error) {
	ctx, cancel := context.WithTimeout(ctx, f.cfg.Timeout)
	defer cancel()

	targets := f.targets(ctx)
	var last *upstreamResult
	var lastErr error
	for i := 0; i < f.cfg.MaxAttempts; i++ {
		if ctx.Err() != nil {
			break
		}
		t := targets[i%len(targets)]
		res, err := f.attempt(ctx, t, ev)
		switch {
		case err != nil:
			f.attempts.Inc(f.source, "error")
			lastErr = err
		case res.status >= 500:
			f.attempts.Inc(f.source, "5xx")
			last, lastErr = res, nil
		default:
			f.attempts.Inc(f.source, fmt.Sprintf("%dxx", res.status/100))
			return res, nil
		}
	}
	if last != nil {
		return last, nil
	}
	if lastErr == nil {
		lastErr = errors.New("no attempt made before the forward timeout")
	}
	return nil, lastErr
}

func (f *httpOutput) attempt(ctx context.Context, target *url.URL, ev Event) (*upstreamResult, error) {
	ctx, cancel := context.WithTimeout(ctx, f.cfg.AttemptTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(ev.Body))
	if err != nil {
		return nil, err
	}
	for name, vals := range ev.Header {
		if f.pass.allows(name) {
			req.Header[name] = append([]string(nil), vals...)
		}
	}
	for name, vals := range f.setHeaders {
		req.Header[name] = append([]string(nil), vals...)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamResponse))
	if err != nil && resp.StatusCode < 500 {
		// The status already arrived; a truncated body does not change the outcome.
		err = nil
	}
	if err != nil {
		return nil, err
	}
	return &upstreamResult{status: resp.StatusCode, header: resp.Header, body: b}, nil
}
