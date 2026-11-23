package verify

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	standardwebhooks "github.com/standard-webhooks/standard-webhooks/libraries/go"

	"github.com/mrrobertkent/hookgate/internal/config"
)

// FuzzHMAC checks that no header value other than the true signature verifies a body, and that parsing never
// panics.
func FuzzHMAC(f *testing.F) {
	f.Add([]byte(`{"a":1}`), "AAAA")
	f.Add([]byte{}, "")
	f.Add([]byte("x"), "v1=,v1=zz")
	f.Setenv("FUZZ_KEY", "fuzz-signing-key-0123")
	v, err := Build(config.Check{HMAC: &config.HMACCheck{Header: "X-Sig", Algorithm: "sha256", Encoding: "base64",
		Separator: ",", Secrets: []config.SecretRef{{Env: "FUZZ_KEY"}}}})
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, body []byte, sig string) {
		h := http.Header{}
		h.Set("X-Sig", sig)
		if _, err := v.Verify(h, body); err == nil {
			want := base64.StdEncoding.EncodeToString(mac(sha256.New, "fuzz-signing-key-0123", string(body)))
			if !containsSig(sig, want) {
				t.Fatalf("accepted forged signature %q for body %q", sig, body)
			}
		}
	})
}

// containsSig reports whether the header carries want, ignoring the whitespace the base64 decoder skips.
func containsSig(header, want string) bool {
	header = strings.NewReplacer("\r", "", "\n", "", " ", "", "\t", "").Replace(header)
	for i := 0; i+len(want) <= len(header); i++ {
		if header[i:i+len(want)] == want {
			return true
		}
	}
	return false
}

var timestampPattern = regexp.MustCompile(`^(0|[1-9][0-9]{0,18})$`)

var eventIDPattern = regexp.MustCompile(`^[A-Za-z0-9_~=+-]{1,200}$`)

// FuzzStandardWebhooks checks that a request passes only with a canonical timestamp inside the tolerance, a
// signature entry equal to the reference library's for the same id, timestamp and body, and a valid event id,
// and that parsing never panics. With signed set, the signature header carries the reference signature for the
// fuzzed id at the reference time, so the event id rules decide the outcome.
func FuzzStandardWebhooks(f *testing.F) {
	ts := strconv.FormatInt(refTime.Unix(), 10)
	valid := refSign(f, refSecret, refMsgID, refTime, refPayload)
	f.Add([]byte(refPayload), refMsgID, ts, valid, false)
	f.Add([]byte(refPayload), refMsgID, ts, "v1,AAAA v2a,"+strings.TrimPrefix(valid, "v1,")+"  "+valid, false)
	f.Add([]byte(refPayload), refMsgID, "+"+ts, valid, false)
	f.Add([]byte(refPayload), refMsgID, "0"+ts, valid, false)
	f.Add([]byte{}, "", "", "", false)
	f.Add([]byte("x"), "id", "9223372036854775807", "v1,", false)
	f.Add([]byte(refPayload), "evt.100", "", "", true)
	f.Add([]byte(refPayload), refMsgID, "", "", true)
	v := buildSW(f, "standard", refSecret)
	f.Fuzz(func(t *testing.T, body []byte, id, ts, sig string, signed bool) {
		if signed {
			ts, sig = strconv.FormatInt(refTime.Unix(), 10), refSign(t, refSecret, id, refTime, string(body))
		}
		res, err := v.Verify(swHeader("webhook", id, ts, sig), body)
		if signed && (err == nil) != eventIDPattern.MatchString(id) {
			t.Fatalf("signed request with id %q: err=%v", id, err)
		}
		if err != nil {
			if res != (Result{}) {
				t.Fatalf("rejected request returned %+v", res)
			}
			return
		}
		sec, perr := strconv.ParseInt(ts, 10, 64)
		when := time.Unix(sec, 0)
		if perr != nil || !timestampPattern.MatchString(ts) || when.Sub(refTime).Abs() > 5*time.Minute {
			t.Fatalf("accepted timestamp %q", ts)
		}
		want := refSign(t, refSecret, id, when, string(body))
		if !slices.Contains(strings.Split(sig, " "), want) || res.EventID != id || !res.SignedAt.Equal(when) {
			t.Fatalf("accepted forged request: id %q, timestamp %q, signature %q, body %q", id, ts, sig, body)
		}
		if !eventIDPattern.MatchString(id) {
			t.Fatalf("accepted event id %q", id)
		}
	})
}

// FuzzStandardWebhooksSecret checks that a secret is accepted exactly when the reference library accepts it and
// it decodes to at least 24 bytes, and that an accepted key signs as the reference library does.
func FuzzStandardWebhooksSecret(f *testing.F) {
	f.Add(refSecret)
	f.Add(strings.TrimPrefix(refSecret, "whsec_"))
	f.Add("whsec_" + base64.RawStdEncoding.EncodeToString([]byte(strings.Repeat("k", 25))))
	f.Add("whsec_whsec_" + refSecret)
	f.Add("")
	f.Fuzz(func(t *testing.T, secret string) {
		key, err := standardWebhooksKey(secret)
		ref, refErr := standardwebhooks.NewWebhook(secret)
		decoded, _ := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, "whsec_"))
		if wantOK := refErr == nil && len(decoded) >= minStandardWebhooksKeyBytes; (err == nil) != wantOK {
			t.Fatalf("secret %q: err=%v, reference err=%v", secret, err, refErr)
		}
		if err != nil {
			if err.Error() != "is not a base64 key of at least 24 bytes" {
				t.Fatalf("unexpected diagnostic: %v", err)
			}
			return
		}
		want, _ := ref.Sign(refMsgID, refTime, []byte(refPayload))
		m := hmac.New(sha256.New, key)
		m.Write([]byte(refMsgID + ".1614265330." + refPayload))
		if got := "v1," + base64.StdEncoding.EncodeToString(m.Sum(nil)); got != want {
			t.Fatalf("secret %q signs %s, reference %s", secret, got, want)
		}
	})
}
