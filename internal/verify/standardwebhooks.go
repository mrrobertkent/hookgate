package verify

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mrrobertkent/hookgate/internal/config"
)

// minStandardWebhooksKeyBytes is the Standard Webhooks minimum for a decoded symmetric key.
const minStandardWebhooksKeyBytes = 24

// maxEventIDBytes bounds an event id, which downstream becomes part of message ids and object keys. The id
// character set excludes '.', the separator of the signed content, so no id and timestamp pair can be
// re-split into another that signs the same bytes.
const maxEventIDBytes = 200

// headerFamilies maps the headers option to the names of the id, timestamp and signature headers.
var headerFamilies = map[string][3]string{
	"standard": {"webhook-id", "webhook-timestamp", "webhook-signature"},
	"svix":     {"svix-id", "svix-timestamp", "svix-signature"},
}

type standardWebhooks struct {
	id, timestamp, signature string
	tolerance                time.Duration
	keys                     [][]byte
	now                      func() time.Time
	// schemes verifies one signature of the version it is keyed by. Entries of other versions are ignored, so
	// a sender can add a scheme alongside one this check knows.
	schemes map[string]func(m *message, sig string) bool
}

// message is the signed part of a request; v1 caches the expected v1 signatures once computed.
type message struct {
	id, timestamp string
	body          []byte
	v1            [][]byte
}

func buildStandardWebhooks(c *config.StandardWebhooksCheck, now func() time.Time) (*standardWebhooks, error) {
	names, ok := headerFamilies[c.Headers]
	if !ok {
		return nil, fmt.Errorf("standard_webhooks: unknown headers %q", c.Headers)
	}
	if c.Tolerance == nil || *c.Tolerance <= 0 {
		return nil, errors.New("standard_webhooks: tolerance must be > 0")
	}
	keys, err := resolve(c.Secrets, standardWebhooksKey)
	if err != nil {
		return nil, fmt.Errorf("standard_webhooks %s: %w", c.Headers, err)
	}
	s := &standardWebhooks{
		id: names[0], timestamp: names[1], signature: names[2],
		tolerance: *c.Tolerance, keys: keys, now: now,
	}
	s.schemes = map[string]func(*message, string) bool{"v1": s.matchV1}
	return s, nil
}

// standardWebhooksKey decodes a secret in the format senders display: standard base64 with an optional whsec_
// prefix.
func standardWebhooksKey(v string) ([]byte, error) {
	k, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(v, "whsec_"))
	if err != nil || len(k) < minStandardWebhooksKeyBytes {
		return nil, fmt.Errorf("is not a base64 key of at least %d bytes", minStandardWebhooksKeyBytes)
	}
	return k, nil
}

// Verify accepts the request when its timestamp is within the tolerance, any signature in the signature header
// matches under any key, and the id is a valid event id. The id and time it returns are covered by that
// signature.
func (s *standardWebhooks) Verify(h http.Header, body []byte) (Result, error) {
	id, err := single(h, s.id)
	if err != nil {
		return Result{}, err
	}
	ts, err := single(h, s.timestamp)
	if err != nil {
		return Result{}, err
	}
	sigs, err := single(h, s.signature)
	if err != nil {
		return Result{}, err
	}
	m := &message{id: id, timestamp: ts, body: body}
	signedAt, err := s.checkTimestamp(m.timestamp)
	if err != nil {
		return Result{}, err
	}
	if !s.matches(m, sigs) {
		return Result{}, fmt.Errorf("signature in %s does not match", s.signature)
	}
	if !validEventID(m.id) {
		return Result{}, fmt.Errorf("header %s must be 1-%d bytes of A-Z a-z 0-9 _ ~ = + -", s.id, maxEventIDBytes)
	}
	return Result{EventID: m.id, SignedAt: signedAt}, nil
}

// matches compares every signature entry with every key, without stopping at the first match, so the work done
// depends only on the header and the configured keys.
func (s *standardWebhooks) matches(m *message, sigs string) bool {
	matched := false
	for entry := range strings.SplitSeq(sigs, " ") {
		version, sig, _ := strings.Cut(entry, ",")
		if match, ok := s.schemes[version]; ok && match(m, sig) {
			matched = true
		}
	}
	return matched
}

func validEventID(id string) bool {
	if id == "" || len(id) > maxEventIDBytes {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		alnum := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if !alnum && !strings.ContainsRune("_~=+-", rune(c)) {
			return false
		}
	}
	return true
}

// checkTimestamp returns the time of a canonical decimal Unix timestamp (digits, no sign, no leading zero
// except "0") within the tolerance of now, in either direction. Senders sign the canonical form, so the value
// is signed as received.
func (s *standardWebhooks) checkTimestamp(v string) (time.Time, error) {
	if !canonicalUnix(v) {
		return time.Time{}, fmt.Errorf("header %s is not a Unix timestamp", s.timestamp)
	}
	sec, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("header %s is not a Unix timestamp", s.timestamp)
	}
	now := s.now()
	tooOld := func() error { return fmt.Errorf("header %s is more than %s old", s.timestamp, s.tolerance) }
	tooNew := func() error { return fmt.Errorf("header %s is more than %s in the future", s.timestamp, s.tolerance) }
	// Seconds outside the window on their own are rejected before time.Unix, whose result overflows near the
	// int64 limits; the rest are compared as exact instants.
	window := int64(s.tolerance/time.Second) + 2
	switch {
	case sec < now.Unix()-window:
		return time.Time{}, tooOld()
	case sec > now.Unix()+window:
		return time.Time{}, tooNew()
	}
	ts := time.Unix(sec, 0)
	switch {
	case ts.Before(now.Add(-s.tolerance)):
		return time.Time{}, tooOld()
	case ts.After(now.Add(s.tolerance)):
		return time.Time{}, tooNew()
	}
	return ts.UTC(), nil
}

// canonicalUnix reports whether v matches ^(0|[1-9][0-9]{0,18})$.
func canonicalUnix(v string) bool {
	if v == "" || len(v) > 19 || (v[0] == '0' && v != "0") {
		return false
	}
	return strings.Trim(v, "0123456789") == ""
}

// matchV1 compares a v1 signature, as sent (standard base64), with the HMAC-SHA256 of "id.timestamp.body"
// under each key.
func (s *standardWebhooks) matchV1(m *message, sig string) bool {
	if m.v1 == nil {
		m.v1 = make([][]byte, len(s.keys))
		for i, k := range s.keys {
			mac := hmac.New(sha256.New, k)
			mac.Write([]byte(m.id))
			mac.Write([]byte{'.'})
			mac.Write([]byte(m.timestamp))
			mac.Write([]byte{'.'})
			mac.Write(m.body)
			m.v1[i] = base64.StdEncoding.AppendEncode(nil, mac.Sum(nil))
		}
	}
	matched := false
	for _, want := range m.v1 {
		if hmac.Equal([]byte(sig), want) {
			matched = true
		}
	}
	return matched
}
