package relay

import (
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/nats-io/nats.go"

	"github.com/mrrobertkent/hookgate/internal/contract"
)

// FuzzMessageHeaders checks that contract-header parsing never panics, accepts only well-formed values and that
// dead-letter details stay bounded, valid UTF-8 without control characters.
func FuzzMessageHeaders(f *testing.F) {
	f.Add("1", "resend", "64", "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08", "", []byte("body"))
	f.Add("1", "resend", "0", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "B/k", []byte{})
	f.Add("2", "", "-1", "XYZ", "/k", []byte("x"))
	f.Fuzz(func(t *testing.T, version, source, size, sum, claim string, body []byte) {
		// claim is "bucket/key": the two claim headers vary together.
		bucket, key, _ := strings.Cut(claim, "/")
		h := nats.Header{}
		for name, v := range map[string]string{
			contract.HeaderContract: version, contract.HeaderSource: source, contract.HeaderBodySize: size,
			contract.HeaderBodySHA256: sum, contract.HeaderClaimBucket: bucket, contract.HeaderClaimKey: key,
		} {
			if v != "" {
				h.Set(name, v)
			}
		}
		checkParsed(t, h, body)
		checkDetail(t, truncate(source+sum+string(body), contract.MaxDeadDetailBytes))
	})
}

func checkParsed(t *testing.T, h nats.Header, body []byte) {
	n, digest, fail := checkContract(h)
	if fail != nil {
		return
	}
	if h.Get(contract.HeaderContract) != contract.Version || n < 0 || len(digest) != 64 {
		t.Fatalf("accepted %v", h)
	}
	ref, f := claimRef(h, body)
	if f == nil && ref != nil && (len(body) != 0 || ref.Key == "") {
		t.Fatalf("accepted claim %+v with body %d", ref, len(body))
	}
	_ = verifyBody(body, n, digest)
}

func checkDetail(t *testing.T, d string) {
	if len(d) > contract.MaxDeadDetailBytes || !utf8.ValidString(d) || strings.ContainsFunc(d, isControl) {
		t.Fatalf("detail %q", d)
	}
}

// FuzzFilterSubject checks that every subject a validated filter matches is a valid literal subject.
func FuzzFilterSubject(f *testing.F) {
	f.Add("webhooks.events.>", "webhooks.events.resend")
	f.Add("a.*.c", "a.b.c")
	f.Fuzz(func(t *testing.T, filter, subject string) {
		if validateFilter(filter) != nil || contract.ValidateSubject(subject) != nil {
			return
		}
		if filter == subject && !subjectMatches(filter, subject) {
			t.Fatalf("%q does not match itself", filter)
		}
		_ = subjectMatches(filter, subject)
	})
}

// FuzzReplayIsolation checks that a filter the relay accepts besides its own replay subject never matches any
// relay's replay subject, whatever prefix and consumer it has.
func FuzzReplayIsolation(f *testing.F) {
	f.Add("webhooks.events.resend", "webhooks", "rex")
	f.Add("webhooks.replay.*", "custom", "b")
	f.Add("*.events.x", "a.b", "c")
	f.Fuzz(func(t *testing.T, filter, prefix, consumer string) {
		subject := prefix + "." + replayToken + "." + consumer
		if validateFilter(filter) != nil || contract.ValidateSubject(subject) != nil || !validName(consumer) ||
			slices.Contains(strings.Split(prefix, "."), replayToken) {
			return
		}
		if !mayMatchReplay(filter) && subjectMatches(filter, subject) {
			t.Fatalf("accepted filter %q matches replay subject %q", filter, subject)
		}
	})
}
