// Package verify checks that a webhook request comes from its configured sender.
package verify

import (
	"crypto/hmac"
	"crypto/sha1" //nolint:gosec // HMAC-SHA1 is still used by some webhook senders; it is selected per source.
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"net/http"
	"strings"
	"time"

	"github.com/mrrobertkent/hookgate/internal/config"
)

// MinSecretBytes is the shortest token or HMAC secret accepted. A shorter secret makes its check fail to build,
// so the source stays not ready instead of verifying with a guessable key.
const MinSecretBytes = 16

// Result is what a passing check learned about the request.
type Result struct {
	// EventID is the sender's event identifier, set only by a check whose signature covers it.
	EventID string
	// SignedAt is the signing time the sender's signature covers; zero unless a check authenticated it.
	SignedAt time.Time
}

// ErrInvalidEventID matches a rejection caused by the request's event ID alone: repeated, or, once the
// signature matched, empty or outside the event-ID character set. The request is malformed rather than forged.
var ErrInvalidEventID = errors.New("invalid event id")

type eventIDError struct{ msg string }

func (e eventIDError) Error() string      { return e.msg }
func (eventIDError) Is(target error) bool { return target == ErrInvalidEventID }

// Verifier is one configured check with its secrets resolved.
type Verifier interface {
	// Verify returns a nil error when the request passes, or a short reason safe to log (never a secret, a
	// signature or body content).
	Verify(h http.Header, body []byte) (Result, error)
}

// Build resolves the secrets of a check. It fails when a secret is unusable (shorter than MinSecretBytes, or
// not a valid Standard Webhooks key) or no usable secret remains, so a source with a missing or weak secret
// can never verify anything.
func Build(c config.Check) (Verifier, error) {
	return build(c, time.Now)
}

func build(c config.Check, now func() time.Time) (Verifier, error) {
	switch {
	case c.Token != nil:
		return buildToken(c.Token)
	case c.HMAC != nil:
		return buildHMAC(c.HMAC)
	case c.StandardWebhooks != nil:
		return buildStandardWebhooks(c.StandardWebhooks, now)
	}
	return nil, errors.New("empty check")
}

// resolve returns the key of every set secret. parse turns a secret value into a key, or explains, without
// quoting the value, why the value is unusable.
func resolve(refs []config.SecretRef, parse func(string) ([]byte, error)) ([][]byte, error) {
	var keys [][]byte
	for _, r := range refs {
		v, err := r.Resolve()
		if err != nil {
			return nil, err
		}
		if v == "" {
			continue
		}
		k, err := parse(v)
		if err != nil {
			return nil, fmt.Errorf("%s %w", r, err)
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return nil, errors.New("no secret is set")
	}
	return keys, nil
}

func rawKey(v string) ([]byte, error) {
	if len(v) < MinSecretBytes {
		return nil, fmt.Errorf("is shorter than %d bytes", MinSecretBytes)
	}
	return []byte(v), nil
}

// single returns the only value of a header; a missing or repeated header is an error.
func single(h http.Header, name string) (string, error) {
	vals := h.Values(name)
	if len(vals) == 0 {
		return "", fmt.Errorf("missing header %s", name)
	}
	if len(vals) > 1 {
		return "", fmt.Errorf("repeated header %s", name)
	}
	return vals[0], nil
}

type token struct {
	header  string
	digests [][32]byte
}

func buildToken(c *config.TokenCheck) (*token, error) {
	keys, err := resolve(c.Secrets, rawKey)
	if err != nil {
		return nil, fmt.Errorf("token %s: %w", c.Header, err)
	}
	t := &token{header: c.Header, digests: make([][32]byte, len(keys))}
	for i, k := range keys {
		t.digests[i] = sha256.Sum256(k)
	}
	return t, nil
}

// Verify compares SHA-256 digests in constant time, so neither the value nor its length leaks through timing.
func (t *token) Verify(h http.Header, _ []byte) (Result, error) {
	v, err := single(h, t.header)
	if err != nil {
		return Result{}, err
	}
	got := sha256.Sum256([]byte(v))
	ok := 0
	for _, d := range t.digests {
		ok |= subtle.ConstantTimeCompare(got[:], d[:])
	}
	if ok != 1 {
		return Result{}, fmt.Errorf("header %s does not match", t.header)
	}
	return Result{}, nil
}

type signature struct {
	cfg     config.HMACCheck
	keys    [][]byte
	newHash func() hash.Hash
}

func buildHMAC(c *config.HMACCheck) (*signature, error) {
	keys, err := resolve(c.Secrets, rawKey)
	if err != nil {
		return nil, fmt.Errorf("hmac %s: %w", c.Header, err)
	}
	s := &signature{cfg: *c, keys: keys}
	switch c.Algorithm {
	case "sha1":
		s.newHash = sha1.New
	case "sha256":
		s.newHash = sha256.New
	case "sha512":
		s.newHash = sha512.New
	default:
		return nil, fmt.Errorf("hmac %s: unknown algorithm %q", c.Header, c.Algorithm)
	}
	return s, nil
}

// Verify accepts the request when any signature in the header matches the HMAC of the raw body under any key.
func (s *signature) Verify(h http.Header, body []byte) (Result, error) {
	v, err := single(h, s.cfg.Header)
	if err != nil {
		return Result{}, err
	}
	sigs := s.signatures(v)
	if len(sigs) == 0 {
		return Result{}, fmt.Errorf("header %s carries no well-formed signature", s.cfg.Header)
	}
	if !s.matches(sigs, body) {
		return Result{}, fmt.Errorf("signature in %s does not match", s.cfg.Header)
	}
	return Result{}, nil
}

// signatures decodes every well-formed signature in a header value; candidates without the configured prefix
// or in the wrong encoding are skipped.
func (s *signature) signatures(v string) [][]byte {
	candidates := []string{v}
	if s.cfg.Separator != "" {
		candidates = strings.Split(v, s.cfg.Separator)
	}
	var sigs [][]byte
	for _, c := range candidates {
		enc, found := strings.CutPrefix(strings.TrimSpace(c), s.cfg.Prefix)
		if !found {
			continue
		}
		if b, err := decode(s.cfg.Encoding, enc); err == nil && len(b) > 0 {
			sigs = append(sigs, b)
		}
	}
	return sigs
}

// matches compares every signature with the HMAC under every key, without stopping at the first match.
func (s *signature) matches(sigs [][]byte, body []byte) bool {
	matched := false
	for _, key := range s.keys {
		m := hmac.New(s.newHash, key)
		m.Write(body)
		want := m.Sum(nil)
		for _, sig := range sigs {
			if hmac.Equal(sig, want) {
				matched = true
			}
		}
	}
	return matched
}

func decode(encoding, s string) ([]byte, error) {
	switch encoding {
	case "hex":
		return hex.DecodeString(strings.ToLower(s))
	case "base64":
		return base64.StdEncoding.DecodeString(s)
	case "base64url":
		if b, err := base64.URLEncoding.DecodeString(s); err == nil {
			return b, nil
		}
		return base64.RawURLEncoding.DecodeString(s)
	}
	return nil, fmt.Errorf("unknown encoding %q", encoding)
}
