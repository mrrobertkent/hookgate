package verify

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	standardwebhooks "github.com/standard-webhooks/standard-webhooks/libraries/go"

	"github.com/mrrobertkent/hookgate/internal/config"
)

// The secret, message id and payload of the reference library's tests and of the Svix verification example.
const (
	refSecret  = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw"
	refMsgID   = "msg_p5jXN8AQM9LWM0D4loKWxJek"
	refPayload = `{"test": 2432232314}`
	// otherSecret decodes to 29 bytes.
	otherSecret = "whsec_c2Vjb25kLXN0YW5kYXJkLXdlYmhvb2tzLWtleSE="
)

var refTime = time.Unix(1614265330, 0)

func fixedClock() time.Time { return refTime }

// refSign signs with the reference implementation, the oracle for every accepted signature.
func refSign(t testing.TB, secret, id string, ts time.Time, body string) string {
	t.Helper()
	wh, err := standardwebhooks.NewWebhook(secret)
	if err != nil {
		t.Fatal(err)
	}
	sig, err := wh.Sign(id, ts, []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	return sig
}

func dur(d time.Duration) *time.Duration { return &d }

func buildSW(t testing.TB, headers string, secrets ...string) Verifier {
	t.Helper()
	var refs []config.SecretRef
	for i, s := range secrets {
		name := fmt.Sprintf("SW_SECRET_%d", i)
		t.Setenv(name, s)
		refs = append(refs, config.SecretRef{Env: name, Optional: i > 0})
	}
	v, err := build(config.Check{StandardWebhooks: &config.StandardWebhooksCheck{
		Headers: headers, Tolerance: dur(5 * time.Minute), Secrets: refs}}, fixedClock)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func swHeader(family, id, ts, sig string) http.Header {
	return hdr(family+"-id", id, family+"-timestamp", ts, family+"-signature", sig)
}

// TestStandardWebhooksPublishedVector checks the signature published in the Svix and Resend verification
// examples (https://resend.com/docs/webhooks/verify-webhooks-requests), independently of the reference library.
func TestStandardWebhooksPublishedVector(t *testing.T) {
	v := buildSW(t, "svix", refSecret)
	h := swHeader("svix", refMsgID, "1614265330", "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=")
	res, err := v.Verify(h, []byte(refPayload))
	if err != nil || res.EventID != refMsgID {
		t.Fatalf("published vector: %+v, %v", res, err)
	}
}

func TestStandardWebhooks(t *testing.T) {
	ts := strconv.FormatInt(refTime.Unix(), 10)
	valid := refSign(t, refSecret, refMsgID, refTime, refPayload)
	forged := refSign(t, otherSecret, refMsgID, refTime, refPayload)
	validB64 := strings.TrimPrefix(valid, "v1,")
	at := func(d time.Duration) (string, string) {
		when := refTime.Add(d)
		return strconv.FormatInt(when.Unix(), 10), refSign(t, refSecret, refMsgID, when, refPayload)
	}
	staleTS, staleSig := at(-5*time.Minute - time.Second)
	oldTS, oldSig := at(-5 * time.Minute)
	futureTS, futureSig := at(5*time.Minute + time.Second)
	aheadTS, aheadSig := at(5 * time.Minute)
	without := func(h http.Header, name string) http.Header { h.Del(name); return h }
	plus := func(h http.Header, kv ...string) http.Header {
		for i := 0; i+1 < len(kv); i += 2 {
			h.Add(kv[i], kv[i+1])
		}
		return h
	}

	cases := []struct {
		name string
		h    http.Header
		body string
		ok   bool
	}{
		{"valid", swHeader("webhook", refMsgID, ts, valid), refPayload, true},
		{"forged", swHeader("webhook", refMsgID, ts, forged), refPayload, false},
		{"missing id", without(swHeader("webhook", refMsgID, ts, valid), "webhook-id"), refPayload, false},
		{"missing timestamp", without(swHeader("webhook", refMsgID, ts, valid), "webhook-timestamp"), refPayload, false},
		{"missing signature", without(swHeader("webhook", refMsgID, ts, valid), "webhook-signature"), refPayload, false},
		{"repeated id", plus(swHeader("webhook", refMsgID, ts, valid), "webhook-id", refMsgID), refPayload, false},
		{"repeated timestamp", plus(swHeader("webhook", refMsgID, ts, valid), "webhook-timestamp", ts), refPayload, false},
		{"repeated signature", plus(swHeader("webhook", refMsgID, ts, valid), "webhook-signature", valid), refPayload, false},
		{"empty id", swHeader("webhook", "", ts, refSign(t, refSecret, "", refTime, refPayload)), refPayload, false},
		{"id not signed", swHeader("webhook", refMsgID+"x", ts, valid), refPayload, false},
		{"stale timestamp", swHeader("webhook", refMsgID, staleTS, staleSig), refPayload, false},
		{"oldest accepted timestamp", swHeader("webhook", refMsgID, oldTS, oldSig), refPayload, true},
		{"future timestamp", swHeader("webhook", refMsgID, futureTS, futureSig), refPayload, false},
		{"latest accepted timestamp", swHeader("webhook", refMsgID, aheadTS, aheadSig), refPayload, true},
		{"timestamp not signed", swHeader("webhook", refMsgID, aheadTS, valid), refPayload, false},
		{"body altered by one byte", swHeader("webhook", refMsgID, ts, valid), strings.Replace(refPayload, "4}", "5}", 1), false},
		{"body with trailing newline", swHeader("webhook", refMsgID, ts, valid), refPayload + "\n", false},
		{"several signatures, last valid", swHeader("webhook", refMsgID, ts, forged+" v1,AAAA "+valid), refPayload, true},
		{"several signatures, none valid", swHeader("webhook", refMsgID, ts, forged+" "+forged), refPayload, false},
		{"double space between signatures", swHeader("webhook", refMsgID, ts, forged+"  "+valid), refPayload, true},
		{"unknown versions ignored", swHeader("webhook", refMsgID, ts, "v2a,"+validB64+" v1a,"+validB64+" v0,x "+valid), refPayload, true},
		{"only unknown versions", swHeader("webhook", refMsgID, ts, "v2,"+validB64+" v2a,"+validB64), refPayload, false},
		{"upper-case version", swHeader("webhook", refMsgID, ts, "V1,"+validB64), refPayload, false},
		{"version without signature", swHeader("webhook", refMsgID, ts, "v1"), refPayload, false},
		{"empty v1 signature", swHeader("webhook", refMsgID, ts, "v1,"), refPayload, false},
		{"space after comma", swHeader("webhook", refMsgID, ts, "v1, "+validB64), refPayload, false},
		{"signature without padding", swHeader("webhook", refMsgID, ts, strings.TrimRight(valid, "=")), refPayload, false},
		{"empty signature header", swHeader("webhook", refMsgID, ts, ""), refPayload, false},
		{"svix headers for standard check", swHeader("svix", refMsgID, ts, valid), refPayload, false},
		{"both families, configured one valid", plus(swHeader("webhook", refMsgID, ts, valid), "svix-id", "other", "svix-timestamp", "1", "svix-signature", forged), refPayload, true},
		{"both families, configured one forged", plus(swHeader("webhook", refMsgID, ts, forged), "svix-id", refMsgID, "svix-timestamp", ts, "svix-signature", valid), refPayload, false},
	}
	for _, bad := range []string{"", "abc", "+" + ts, "-" + ts, " " + ts, ts + " ", ts + ".0", "0" + ts, "1e9", "0x5f", "1_614_265_330", "99999999999999999999"} {
		cases = append(cases, struct {
			name string
			h    http.Header
			body string
			ok   bool
		}{"timestamp " + strconv.Quote(bad), swHeader("webhook", refMsgID, bad, valid), refPayload, false})
	}

	v := buildSW(t, "standard", refSecret)
	secrets := []string{validB64, strings.TrimPrefix(forged, "v1,"), refPayload, "MfKQ9r8G"}
	for _, c := range cases {
		res, err := v.Verify(c.h, []byte(c.body))
		if (err == nil) != c.ok {
			t.Errorf("%s: got err=%v, want ok=%v", c.name, err, c.ok)
			continue
		}
		if c.ok {
			checkAccepted(t, c.name, res, refMsgID, c.h.Get("webhook-timestamp"))
		} else {
			checkRejected(t, c.name, res, err, secrets)
		}
	}
}

func checkAccepted(t *testing.T, name string, res Result, id, ts string) {
	t.Helper()
	sec, _ := strconv.ParseInt(ts, 10, 64)
	if res.EventID != id || !res.SignedAt.Equal(time.Unix(sec, 0)) || res.SignedAt.Location() != time.UTC {
		t.Errorf("%s: result %+v, want EventID %q signed at %s UTC", name, res, id, ts)
	}
}

func checkRejected(t *testing.T, name string, res Result, err error, secrets []string) {
	t.Helper()
	if res != (Result{}) {
		t.Errorf("%s: rejected request returned %+v", name, res)
	}
	for _, s := range secrets {
		if s != "" && strings.Contains(err.Error(), s) {
			t.Errorf("%s: error %q leaks a signature, the body or the secret", name, err)
		}
	}
}

func TestStandardWebhooksEventID(t *testing.T) {
	ts := strconv.FormatInt(refTime.Unix(), 10)
	v := buildSW(t, "standard", refSecret)
	cases := map[string]bool{
		"msg_2Kq~=+-Zz09":        true,
		"evt.100":                false,
		strings.Repeat("a", 200): true,
		strings.Repeat("a", 201): false,
		"msg 1":                  false,
		"msg/1":                  false,
		"msg:1":                  false,
		"msg\x00":                false,
		"msg\u00e9":              false,
		"":                       false,
	}
	for id, ok := range cases {
		h := swHeader("webhook", id, ts, refSign(t, refSecret, id, refTime, refPayload))
		res, err := v.Verify(h, []byte(refPayload))
		if (err == nil) != ok {
			t.Errorf("id %q: got err=%v, want ok=%v", id, err, ok)
			continue
		}
		if ok {
			checkAccepted(t, strconv.Quote(id), res, id, ts)
		} else {
			checkRejected(t, strconv.Quote(id), res, err, []string{id})
		}
	}
}

func TestStandardWebhooksTimestampBounds(t *testing.T) {
	v := buildSW(t, "standard", refSecret)
	cases := map[string]string{
		"0":                    "old",
		"1":                    "old",
		"9223372036854775807":  "future",
		"9223372036854775808":  "not a Unix timestamp",
		"10000000000000000000": "not a Unix timestamp",
		"00":                   "not a Unix timestamp",
	}
	for ts, want := range cases {
		sec, _ := strconv.ParseInt(ts, 10, 64)
		h := swHeader("webhook", refMsgID, ts, refSign(t, refSecret, refMsgID, time.Unix(sec, 0), refPayload))
		_, err := v.Verify(h, []byte(refPayload))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("timestamp %s: got %v, want an error containing %q", ts, err, want)
		}
	}
}

// TestStandardWebhooksSubSecondTolerance compares exact instants: the whole-second timestamp against a clock
// that is part-way through a second.
func TestStandardWebhooksSubSecondTolerance(t *testing.T) {
	t.Setenv("SW_SUB", refSecret)
	cases := []struct {
		clock time.Duration // after refTime
		ts    int64         // seconds after refTime
		want  string        // "" accepts
	}{
		{750 * time.Millisecond, 1, ""},
		{750 * time.Millisecond, 0, "old"},
		{750 * time.Millisecond, 2, "future"},
		{750 * time.Millisecond, 5, "future"},
		{500 * time.Millisecond, 0, ""},
		{500 * time.Millisecond, 1, ""},
		{500*time.Millisecond + time.Nanosecond, 0, "old"},
		{500*time.Millisecond - time.Nanosecond, 1, "future"},
	}
	for _, c := range cases {
		now := refTime.Add(c.clock)
		v, err := build(config.Check{StandardWebhooks: &config.StandardWebhooksCheck{Headers: "standard",
			Tolerance: dur(500 * time.Millisecond), Secrets: []config.SecretRef{{Env: "SW_SUB"}}}},
			func() time.Time { return now })
		if err != nil {
			t.Fatal(err)
		}
		signedAt := refTime.Add(time.Duration(c.ts) * time.Second)
		ts := strconv.FormatInt(signedAt.Unix(), 10)
		res, err := v.Verify(swHeader("webhook", refMsgID, ts, refSign(t, refSecret, refMsgID, signedAt, refPayload)), []byte(refPayload))
		name := fmt.Sprintf("clock +%s, timestamp +%ds", c.clock, c.ts)
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%s: %v", name, err)
		case c.want == "" && !res.SignedAt.Equal(signedAt):
			t.Errorf("%s: SignedAt %s, want %s", name, res.SignedAt, signedAt)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("%s: got %v, want an error containing %q", name, err, c.want)
		}
	}
}

func TestStandardWebhooksSvixHeaders(t *testing.T) {
	ts := strconv.FormatInt(refTime.Unix(), 10)
	valid := refSign(t, refSecret, refMsgID, refTime, refPayload)
	v := buildSW(t, "svix", refSecret)
	if res, err := v.Verify(swHeader("svix", refMsgID, ts, valid), []byte(refPayload)); err != nil || res.EventID != refMsgID {
		t.Errorf("svix headers: %+v, %v", res, err)
	}
	if _, err := v.Verify(swHeader("webhook", refMsgID, ts, valid), []byte(refPayload)); err == nil {
		t.Error("svix check accepted webhook-* headers")
	}
}

func TestStandardWebhooksSecrets(t *testing.T) {
	ts := strconv.FormatInt(refTime.Unix(), 10)
	signedBy := func(secret string) http.Header {
		return swHeader("webhook", refMsgID, ts, refSign(t, secret, refMsgID, refTime, refPayload))
	}
	cases := []struct {
		name    string
		secrets []string
		h       http.Header
		ok      bool
	}{
		{"whsec_ prefix", []string{refSecret}, signedBy(refSecret), true},
		{"no prefix", []string{strings.TrimPrefix(refSecret, "whsec_")}, signedBy(refSecret), true},
		{"rotation, first secret", []string{refSecret, otherSecret}, signedBy(refSecret), true},
		{"rotation, second secret", []string{refSecret, otherSecret}, signedBy(otherSecret), true},
		{"other secret", []string{refSecret}, signedBy(otherSecret), false},
	}
	for _, c := range cases {
		v := buildSW(t, "standard", c.secrets...)
		if _, err := v.Verify(c.h, []byte(refPayload)); (err == nil) != c.ok {
			t.Errorf("%s: got err=%v, want ok=%v", c.name, err, c.ok)
		}
	}
}

func TestStandardWebhooksBadSecretFailsClosed(t *testing.T) {
	key := func(n int) []byte { return []byte(strings.Repeat("k", n)) }
	cases := map[string]string{
		"not base64":         "whsec_this-is-not!base64?at-all-0123456789",
		"23 bytes":           "whsec_" + base64.StdEncoding.EncodeToString(key(23)),
		"25 bytes unpadded":  "whsec_" + base64.RawStdEncoding.EncodeToString(key(25)),
		"base64url alphabet": "whsec_" + base64.URLEncoding.EncodeToString([]byte(strings.Repeat("\xfb\xff", 12))),
		"doubled prefix":     "whsec_" + refSecret,
	}
	for name, secret := range cases {
		t.Setenv("SW_BAD", secret)
		_, err := build(config.Check{StandardWebhooks: &config.StandardWebhooksCheck{Headers: "standard",
			Tolerance: dur(time.Minute), Secrets: []config.SecretRef{{Env: "SW_BAD"}}}}, fixedClock)
		if err == nil {
			t.Errorf("%s: secret accepted", name)
			continue
		}
		if !strings.Contains(err.Error(), "env:SW_BAD") || strings.Contains(err.Error(), strings.TrimPrefix(secret, "whsec_")) {
			t.Errorf("%s: error must name the reference and never the value: %v", name, err)
		}
	}
	for name, secret := range map[string]string{
		"24 bytes":        "whsec_" + base64.StdEncoding.EncodeToString(key(24)),
		"25 bytes padded": "whsec_" + base64.StdEncoding.EncodeToString(key(25)),
	} {
		t.Setenv("SW_OK", secret)
		if _, err := Build(config.Check{StandardWebhooks: &config.StandardWebhooksCheck{Headers: "standard",
			Tolerance: dur(time.Minute), Secrets: []config.SecretRef{{Env: "SW_OK"}}}}); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestStandardWebhooksMissingSecretFailsClosed(t *testing.T) {
	t.Setenv("EMPTY", "")
	for _, refs := range [][]config.SecretRef{
		{{Env: "UNSET_VAR_FOR_TEST"}},
		{{Env: "EMPTY"}},
		{{Env: "EMPTY", Optional: true}},
		{{File: "/nonexistent/secret"}},
	} {
		if _, err := Build(config.Check{StandardWebhooks: &config.StandardWebhooksCheck{Headers: "standard",
			Tolerance: dur(time.Minute), Secrets: refs}}); err == nil {
			t.Errorf("Build with %v succeeded with no usable secret", refs)
		}
	}
}

// TestStandardWebhooksWallClock checks that Build verifies against the current time.
func TestStandardWebhooksWallClock(t *testing.T) {
	t.Setenv("SW_NOW", refSecret)
	v, err := Build(config.Check{StandardWebhooks: &config.StandardWebhooksCheck{Headers: "standard",
		Tolerance: dur(5 * time.Minute), Secrets: []config.SecretRef{{Env: "SW_NOW"}}}})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	h := swHeader("webhook", refMsgID, strconv.FormatInt(now.Unix(), 10), refSign(t, refSecret, refMsgID, now, refPayload))
	if _, err := v.Verify(h, []byte(refPayload)); err != nil {
		t.Error(err)
	}
	h = swHeader("webhook", refMsgID, "1614265330", refSign(t, refSecret, refMsgID, refTime, refPayload))
	if _, err := v.Verify(h, []byte(refPayload)); err == nil {
		t.Error("a 2021 timestamp passed against the wall clock")
	}
}
