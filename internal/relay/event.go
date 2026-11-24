package relay

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mrrobertkent/hookgate/internal/claimcheck"
	"github.com/mrrobertkent/hookgate/internal/contract"
)

// event is what a handler receives: the resolved body and the headers it may see.
type event struct {
	subject   string
	msgID     string // the stored Nats-Msg-Id, if any
	streamSeq uint64
	attempt   uint64
	body      []byte
	// headers holds the contract headers (without the claim-check pair) and the passed application headers,
	// under the names they were published with.
	headers nats.Header
}

func (e *event) get(name string) string { return e.headers.Get(name) }

// message is one delivery being processed by its owner.
type message struct {
	msg   jetstream.Msg
	meta  *jetstream.MsgMetadata
	msgID string
	claim *claimcheck.Ref // set when the event names a claim check
}

// handlerContractHeaders are the contract headers a handler sees, in addition to passed application headers.
var handlerContractHeaders = []string{
	contract.HeaderContract, contract.HeaderSource, contract.HeaderEventID, contract.HeaderEventIDAuthenticated,
	contract.HeaderSignedAt, contract.HeaderBodySHA256, contract.HeaderBodySize, contract.HeaderReceivedAt,
	contract.HeaderReplay,
}

// deadContractHeaders are the contract headers a dead-letter record keeps.
var deadContractHeaders = []string{
	contract.HeaderContract, contract.HeaderSource, contract.HeaderEventID, contract.HeaderEventIDAuthenticated,
	contract.HeaderSignedAt, contract.HeaderReceivedAt, contract.HeaderBodySHA256, contract.HeaderBodySize,
}

// failure is a non-handler outcome of preparing an event: a dead letter with a reason, or a retry.
type failure struct {
	retry  bool
	reason string // dead-letter reason when !retry
	detail string
}

// checkContract verifies that every required contract header is present exactly once and well formed, and
// returns the declared body size and digest.
func checkContract(h nats.Header) (int64, string, *failure) {
	for _, name := range contract.RequiredHeaders {
		if len(h.Values(name)) != 1 || h.Get(name) == "" {
			return 0, "", invalid("header %s missing or repeated", name)
		}
	}
	if v := h.Get(contract.HeaderContract); v != contract.Version {
		return 0, "", invalid("unsupported contract version %q", truncate(v, 16))
	}
	size, err := strconv.ParseInt(h.Get(contract.HeaderBodySize), 10, 64)
	if err != nil || size < 0 {
		return 0, "", invalid("header %s is not a byte count", contract.HeaderBodySize)
	}
	sum := h.Get(contract.HeaderBodySHA256)
	if len(sum) != sha256.Size*2 || strings.ToLower(sum) != sum {
		return 0, "", invalid("header %s is not a lowercase SHA-256 hex digest", contract.HeaderBodySHA256)
	}
	if _, err := hex.DecodeString(sum); err != nil {
		return 0, "", invalid("header %s is not a lowercase SHA-256 hex digest", contract.HeaderBodySHA256)
	}
	return size, sum, nil
}

func invalid(format string, args ...any) *failure {
	return &failure{reason: contract.DeadInvalidMessage, detail: fmt.Sprintf(format, args...)}
}

// claimRef returns the claim check an event names, if any. The bucket header is not trusted; it only has to
// be present alongside the key.
func claimRef(h nats.Header, body []byte) (*claimcheck.Ref, *failure) {
	bucket, key := h.Get(contract.HeaderClaimBucket), h.Get(contract.HeaderClaimKey)
	switch {
	case bucket == "" && key == "":
		return nil, nil
	case bucket == "" || key == "":
		return nil, invalid("headers %s and %s go together", contract.HeaderClaimBucket, contract.HeaderClaimKey)
	case len(body) != 0:
		return nil, invalid("a claim-checked event has an empty message body")
	}
	return &claimcheck.Ref{Bucket: bucket, Key: key}, nil
}

// verifyBody checks a body against the declared size and SHA-256.
func verifyBody(body []byte, size int64, sum string) bool {
	if int64(len(body)) != size {
		return false
	}
	got := sha256.Sum256(body)
	want, _ := hex.DecodeString(sum)
	return subtle.ConstantTimeCompare(got[:], want) == 1
}

// resolve returns the event body: the message body, or the claim-checked object from the configured bucket.
// A missing object or failed fetch is retryable; on the last attempt a missing object is a dead letter.
func (r *Relay) resolve(ctx context.Context, m *message, size int64, sum string) ([]byte, *failure) {
	if m.claim == nil {
		body := m.msg.Data()
		if !verifyBody(body, size, sum) {
			return nil, &failure{reason: contract.DeadChecksumMismatch, detail: "message body does not match its size and SHA-256 headers"}
		}
		return body, nil
	}
	if r.claims == nil {
		return nil, r.missing(m, "claim-checked event but no claim_check bucket is configured")
	}
	body, err := r.fetchClaim(ctx, m.claim.Key)
	switch {
	case errors.Is(err, claimcheck.ErrCorrupt):
		return nil, &failure{reason: contract.DeadChecksumMismatch, detail: "claim-check object does not match its stored digest"}
	case errors.Is(err, claimcheck.ErrNotFound):
		return nil, r.missing(m, "claim-check object not found")
	case err != nil:
		return nil, &failure{retry: true, detail: truncate("claim-check fetch: "+err.Error(), contract.MaxDeadDetailBytes)}
	case !verifyBody(body, size, sum):
		return nil, &failure{reason: contract.DeadChecksumMismatch, detail: "claim-check object does not match the event's size and SHA-256 headers"}
	}
	return body, nil
}

// fetchClaim reads a claim-checked body within its own deadline, no longer than drain_timeout. When ctx ends first
// it stops waiting at once: the stock reader cannot always be interrupted mid-read, and shutdown must not wait on it.
func (r *Relay) fetchClaim(ctx context.Context, key string) ([]byte, error) {
	fctx, cancel := context.WithTimeout(ctx, min(claimFetchTimeout, r.cfg.DrainTimeout))
	type fetched struct {
		body []byte
		err  error
	}
	ch := make(chan fetched, 1)
	go func() {
		defer cancel()
		b, err := r.claims.Get(fctx, claimcheck.Ref{Bucket: r.cfg.ClaimCheck.ObjectStore.Bucket, Key: key})
		ch <- fetched{b, err}
	}()
	select {
	case f := <-ch:
		return f.body, f.err
	case <-fctx.Done():
		// The reader cancels fctx only after delivering its result; a delivered result wins.
		select {
		case f := <-ch:
			return f.body, f.err
		default:
			return nil, fctx.Err()
		}
	}
}

// missing is retryable until the last attempt, then a claim_missing dead letter.
func (r *Relay) missing(m *message, detail string) *failure {
	if r.lastAttempt(m.meta) {
		return &failure{reason: contract.DeadClaimMissing, detail: detail}
	}
	return &failure{retry: true, detail: detail}
}

// handlerHeaders selects the headers a handler sees: contract headers and passed application headers.
func (r *Relay) handlerHeaders(h nats.Header) nats.Header {
	out := nats.Header{}
	for _, name := range handlerContractHeaders {
		if v := h.Values(name); len(v) > 0 {
			out[name] = append([]string(nil), v...)
		}
	}
	for name, v := range h {
		if !contract.IsReserved(name) && r.passes(name) {
			out[name] = append([]string(nil), v...)
		}
	}
	return out
}

// passes reports whether a header name matches pass_headers, ignoring case.
func (r *Relay) passes(name string) bool {
	n := strings.ToLower(name)
	for _, p := range r.passPatterns {
		if prefix, ok := strings.CutSuffix(p, "*"); ok {
			if strings.HasPrefix(n, prefix) {
				return true
			}
		} else if n == p {
			return true
		}
	}
	return false
}

// truncate cuts s to at most n bytes without splitting a UTF-8 sequence, and replaces control characters,
// which header values cannot carry, with spaces.
func truncate(s string, n int) string {
	s = strings.Map(func(r rune) rune {
		if isControl(r) {
			return ' '
		}
		return r
	}, strings.ToValidUTF8(s, "?"))
	if len(s) <= n {
		return s
	}
	cut := n
	for cut > 0 && !isRuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

func isRuneStart(b byte) bool { return b&0xC0 != 0x80 }
