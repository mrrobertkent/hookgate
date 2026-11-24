package relay

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"syscall"

	"github.com/mrrobertkent/hookgate/internal/contract"
)

// responseDrain bounds how much of a handler's response body is read before the connection is reused.
const responseDrain = 64 << 10

// httpDeliverer posts each event to the configured URL. It uses the transport directly, so a redirect is a
// response like any other and its Location is never parsed or followed.
type httpDeliverer struct {
	cfg       HTTPHandler
	headers   http.Header // set_headers, resolved at start
	transport http.RoundTripper
}

// newHTTPDeliverer resolves set_headers secrets once, at start.
func newHTTPDeliverer(cfg HTTPHandler) (*httpDeliverer, error) {
	h := http.Header{}
	for name, ref := range cfg.SetHeaders {
		v, err := ref.Resolve()
		if err != nil {
			return nil, fmt.Errorf("handler.http.set_headers.%s: %w", name, err)
		}
		if v != "" || !ref.Optional {
			h.Set(name, v)
		}
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.Proxy = nil
	return &httpDeliverer{cfg: cfg, headers: h, transport: tr}, nil
}

func (d *httpDeliverer) deliver(ctx context.Context, ev *event) result {
	rctx, cancel := context.WithTimeout(ctx, d.cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodPost, d.cfg.URL, bytes.NewReader(ev.body))
	if err != nil {
		return resultf(verdictPause, "build request: %v", err)
	}
	d.setHeaders(req, ev)
	resp, err := d.transport.RoundTrip(req)
	if err != nil {
		return d.transportResult(ctx, rctx, "request", err)
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusSwitchingProtocols:
		return result{verdict: verdictPause, detail: "status 101 (the handler must answer the request, not upgrade it)"}
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		return resultf(verdictPause, "status %d (redirects are not followed; fix handler.http.url)", resp.StatusCode)
	}
	// The response body is read within the request context, so a stalled handler cannot outlast the timeout.
	if _, err := io.Copy(io.Discard, io.LimitReader(resp.Body, responseDrain)); err != nil {
		return d.transportResult(ctx, rctx, fmt.Sprintf("status %d, response body", resp.StatusCode), err)
	}
	return statusResult(resp.StatusCode)
}

// setHeaders adds the event's headers, the attempt and set_headers; a configured header replaces a passed one of
// the same name in any letter case. A configured Host sets the request's host, which is what Go sends.
func (d *httpDeliverer) setHeaders(req *http.Request, ev *event) {
	h := req.Header
	for name, v := range ev.headers {
		if !strings.EqualFold(name, "Host") {
			h[name] = v
		}
	}
	h.Set(contract.HeaderAttempt, strconv.FormatUint(ev.attempt, 10))
	for name, v := range d.headers {
		if strings.EqualFold(name, "Host") {
			req.Host = v[0]
			continue
		}
		for k := range h {
			if strings.EqualFold(k, name) {
				delete(h, k)
			}
		}
		h[name] = v
	}
}

func (d *httpDeliverer) transportResult(ctx, rctx context.Context, what string, err error) result {
	switch {
	case ctx.Err() != nil:
		return result{verdict: verdictCancelled, detail: "cancelled by shutdown"}
	case errors.Is(rctx.Err(), context.DeadlineExceeded):
		return resultf(verdictRetry, "timed out after %s", d.cfg.Timeout)
	}
	return resultf(verdictRetry, "%s: %s", what, errorClass(err))
}

// errorClass names a transport failure by kind. The error text itself is never used: it can quote whatever the
// handler sent, credentials included.
func errorClass(err error) string {
	var dnsErr *net.DNSError
	var netErr net.Error
	var tlsAlert tls.AlertError
	var tlsRecord tls.RecordHeaderError
	var certErr *tls.CertificateVerificationError
	switch {
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection refused"
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
		return "connection reset or closed"
	case errors.As(err, &dnsErr):
		return "name resolution failed"
	case errors.As(err, &tlsAlert), errors.As(err, &tlsRecord), errors.As(err, &certErr):
		return "TLS failure"
	case errors.As(err, &netErr) && netErr.Timeout():
		return "timeout"
	case errors.As(err, &netErr):
		return "network error"
	}
	return "malformed response"
}

func statusResult(code int) result {
	detail := "status " + strconv.Itoa(code)
	switch {
	case code >= 200 && code < 300:
		return result{verdict: verdictAck}
	case code == http.StatusTooManyRequests || code >= 500:
		return result{verdict: verdictRetry, detail: detail}
	case code >= 400:
		return result{verdict: verdictReject, detail: detail}
	}
	return result{verdict: verdictRetry, detail: detail}
}
