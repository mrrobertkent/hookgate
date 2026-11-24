package relay

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"

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
	if res, fault := configFault(resp.StatusCode); fault {
		return res
	}
	var after time.Duration
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable {
		after = retryAfter(resp.Header, time.Now())
	}
	res := statusResult(resp.StatusCode)
	// The response body is read within the request context, so a stalled handler cannot outlast the timeout.
	if _, err := io.Copy(io.Discard, io.LimitReader(resp.Body, responseDrain)); err != nil {
		res = d.transportResult(ctx, rctx, fmt.Sprintf("status %d, response body", resp.StatusCode), err)
	}
	if after > 0 && res.verdict == verdictRetry {
		res.after = after
		res.detail += ", Retry-After " + after.String()
	}
	return res
}

// configFault reports the statuses that mean the handler cannot accept any event: an upgrade, a redirect, or a
// refused credential.
func configFault(code int) (result, bool) {
	switch {
	case code == http.StatusSwitchingProtocols:
		return result{verdict: verdictPause, detail: "status 101 (the handler must answer the request, not upgrade it)"}, true
	case code >= 300 && code < 400:
		return resultf(verdictPause, "status %d (redirects are not followed; fix handler.http.url)", code), true
	case code == http.StatusUnauthorized || code == http.StatusForbidden:
		return resultf(verdictPause, "status %d (the handler refuses the relay's credentials; fix handler.http.set_headers)", code), true
	}
	return result{}, false
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

// statusResult maps a final status. Only statuses that say the request itself is unacceptable reject the event;
// any other failure, including an unknown 4xx, retries.
func statusResult(code int) result {
	detail := "status " + strconv.Itoa(code)
	switch {
	case code >= 200 && code < 300:
		return result{verdict: verdictAck}
	case code == http.StatusBadRequest, code == http.StatusRequestEntityTooLarge,
		code == http.StatusUnsupportedMediaType, code == http.StatusUnprocessableEntity:
		return result{verdict: verdictReject, detail: detail}
	}
	return result{verdict: verdictRetry, detail: detail}
}

// maxRetryAfter bounds a parsed Retry-After before it becomes a Duration.
const maxRetryAfter = math.MaxInt64 / int64(time.Second)

// retryAfter returns the delay a Retry-After header asks for (RFC 9110, section 10.2.3: delay-seconds or an
// HTTP-date), or 0 when the header is absent, repeated, malformed or in the past.
func retryAfter(h http.Header, now time.Time) time.Duration {
	v := h.Values("Retry-After")
	if len(v) != 1 {
		return 0
	}
	s := strings.TrimSpace(v[0])
	if s != "" && strings.Trim(s, "0123456789") == "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n > maxRetryAfter {
			n = maxRetryAfter // only an overflow fails on digits
		}
		return time.Duration(n) * time.Second
	}
	t, err := http.ParseTime(s)
	if err != nil {
		return 0
	}
	return max(t.Sub(now), 0)
}
