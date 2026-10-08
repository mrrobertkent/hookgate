package gateway

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
	"strings"
	"time"

	"github.com/mrrobertkent/hookgate/internal/config"
)

// maxUpstreamResponse bounds the upstream response body relayed to the sender.
const maxUpstreamResponse = 64 << 10

type forwarder struct {
	cfg        config.Forward
	setHeaders http.Header
	pass       []string // canonical names; a trailing * matches a prefix
	client     *http.Client
	resolver   *net.Resolver
}

type upstreamResult struct {
	status int
	header http.Header
	body   []byte
}

func newForwarder(cfg config.Forward) (*forwarder, error) {
	f := &forwarder{
		cfg:        cfg,
		setHeaders: http.Header{},
		resolver:   net.DefaultResolver,
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
	for name, ref := range cfg.SetHeaders {
		v, err := ref.Resolve()
		if err != nil {
			return nil, fmt.Errorf("set_headers %s: %w", name, err)
		}
		if v == "" {
			return nil, fmt.Errorf("set_headers %s: %s is empty", name, ref)
		}
		f.setHeaders.Set(name, v)
	}
	for _, p := range cfg.PassHeaders {
		if strings.HasSuffix(p, "*") {
			f.pass = append(f.pass, http.CanonicalHeaderKey(strings.TrimSuffix(p, "*"))+"*")
		} else {
			f.pass = append(f.pass, http.CanonicalHeaderKey(p))
		}
	}
	return f, nil
}

func (f *forwarder) passes(name string) bool {
	for _, p := range f.pass {
		if prefix, ok := strings.CutSuffix(p, "*"); ok {
			if strings.HasPrefix(name, prefix) {
				return true
			}
		} else if name == p {
			return true
		}
	}
	return false
}

// targets expands the configured URLs into attempt targets. With resolve_all, each host name is resolved to
// every address it has (for example every task behind a service name), in random order.
func (f *forwarder) targets(ctx context.Context) []*url.URL {
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
func (f *forwarder) deliver(ctx context.Context, in *http.Request, body []byte, onAttempt func(result string)) (*upstreamResult, error) {
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
		res, err := f.attempt(ctx, t, in, body)
		switch {
		case err != nil:
			onAttempt("error")
			lastErr = err
		case res.status >= 500:
			onAttempt("5xx")
			last, lastErr = res, nil
		default:
			onAttempt(fmt.Sprintf("%dxx", res.status/100))
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

func (f *forwarder) attempt(ctx context.Context, target *url.URL, in *http.Request, body []byte) (*upstreamResult, error) {
	ctx, cancel := context.WithTimeout(ctx, f.cfg.AttemptTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for name, vals := range in.Header {
		if f.passes(name) {
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
