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
	"fmt"
	"hash"
	"net/http"
	"strings"

	"github.com/mrrobertkent/hookgate/internal/config"
)

// Verifier is one configured check with its secrets resolved.
type Verifier interface {
	// Verify returns nil when the request passes, or a short reason safe to log (never a secret or a signature).
	Verify(h http.Header, body []byte) error
}

// Build resolves the secrets of a check. It fails when no usable secret remains, so a source with a missing
// secret can never verify anything.
func Build(c config.Check) (Verifier, error) {
	switch {
	case c.Token != nil:
		secrets, err := resolve(c.Token.Secrets)
		if err != nil {
			return nil, fmt.Errorf("token %s: %w", c.Token.Header, err)
		}
		digests := make([][32]byte, len(secrets))
		for i, s := range secrets {
			digests[i] = sha256.Sum256([]byte(s))
		}
		return &token{header: c.Token.Header, digests: digests}, nil
	case c.HMAC != nil:
		secrets, err := resolve(c.HMAC.Secrets)
		if err != nil {
			return nil, fmt.Errorf("hmac %s: %w", c.HMAC.Header, err)
		}
		v := &signature{cfg: *c.HMAC}
		for _, s := range secrets {
			v.keys = append(v.keys, []byte(s))
		}
		switch c.HMAC.Algorithm {
		case "sha1":
			v.newHash = sha1.New
		case "sha256":
			v.newHash = sha256.New
		case "sha512":
			v.newHash = sha512.New
		}
		return v, nil
	}
	return nil, fmt.Errorf("empty check")
}

func resolve(refs []config.SecretRef) ([]string, error) {
	var out []string
	for _, r := range refs {
		v, err := r.Resolve()
		if err != nil {
			return nil, err
		}
		if v != "" {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no secret is set")
	}
	return out, nil
}

type token struct {
	header  string
	digests [][32]byte
}

// Verify compares SHA-256 digests in constant time, so neither the value nor its length leaks through timing.
func (t *token) Verify(h http.Header, _ []byte) error {
	vals := h.Values(t.header)
	if len(vals) == 0 {
		return fmt.Errorf("missing header %s", t.header)
	}
	if len(vals) > 1 {
		return fmt.Errorf("repeated header %s", t.header)
	}
	got := sha256.Sum256([]byte(vals[0]))
	ok := 0
	for _, d := range t.digests {
		ok |= subtle.ConstantTimeCompare(got[:], d[:])
	}
	if ok != 1 {
		return fmt.Errorf("header %s does not match", t.header)
	}
	return nil
}

type signature struct {
	cfg     config.HMACCheck
	keys    [][]byte
	newHash func() hash.Hash
}

// Verify accepts the request when any signature in the header matches the HMAC of the raw body under any key.
func (s *signature) Verify(h http.Header, body []byte) error {
	vals := h.Values(s.cfg.Header)
	if len(vals) == 0 {
		return fmt.Errorf("missing header %s", s.cfg.Header)
	}
	if len(vals) > 1 {
		return fmt.Errorf("repeated header %s", s.cfg.Header)
	}
	candidates := []string{vals[0]}
	if s.cfg.Separator != "" {
		candidates = strings.Split(vals[0], s.cfg.Separator)
	}
	var sigs [][]byte
	for _, c := range candidates {
		c = strings.TrimSpace(c)
		if s.cfg.Prefix != "" {
			rest, found := strings.CutPrefix(c, s.cfg.Prefix)
			if !found {
				continue
			}
			c = rest
		}
		if b, err := decode(s.cfg.Encoding, c); err == nil && len(b) > 0 {
			sigs = append(sigs, b)
		}
	}
	if len(sigs) == 0 {
		return fmt.Errorf("header %s carries no well-formed signature", s.cfg.Header)
	}
	for _, key := range s.keys {
		m := hmac.New(s.newHash, key)
		m.Write(body)
		want := m.Sum(nil)
		for _, sig := range sigs {
			if hmac.Equal(sig, want) {
				return nil
			}
		}
	}
	return fmt.Errorf("signature in %s does not match", s.cfg.Header)
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
