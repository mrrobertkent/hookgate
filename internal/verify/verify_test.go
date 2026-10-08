package verify

import (
	"crypto/hmac"
	"crypto/sha1" //nolint:gosec // test vectors for the sha1 option
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"hash"
	"net/http"
	"strings"
	"testing"

	"github.com/mrrobertkent/hookgate/internal/config"
)

const (
	key1 = "first-signing-key-0123"
	key2 = "second-signing-key-456"
)

func mac(h func() hash.Hash, key, body string) []byte {
	m := hmac.New(h, []byte(key))
	m.Write([]byte(body))
	return m.Sum(nil)
}

func hdr(kv ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Add(kv[i], kv[i+1])
	}
	return h
}

func envRefs(t *testing.T, kv ...string) []config.SecretRef {
	t.Helper()
	var refs []config.SecretRef
	for i := 0; i+1 < len(kv); i += 2 {
		t.Setenv(kv[i], kv[i+1])
		refs = append(refs, config.SecretRef{Env: kv[i], Optional: i > 0})
	}
	return refs
}

func TestToken(t *testing.T) {
	refs := envRefs(t, "TOK", "current-token-value", "TOK_NEXT", "next-token-value")
	v, err := Build(config.Check{Token: &config.TokenCheck{Header: "X-Token", Secrets: refs}})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		h    http.Header
		ok   bool
	}{
		{"current", hdr("X-Token", "current-token-value"), true},
		{"next during rotation", hdr("X-Token", "next-token-value"), true},
		{"wrong", hdr("X-Token", "current-token-valuX"), false},
		{"prefix of secret", hdr("X-Token", "current"), false},
		{"empty value", hdr("X-Token", ""), false},
		{"missing", hdr(), false},
		{"repeated", hdr("X-Token", "current-token-value", "X-Token", "current-token-value"), false},
	}
	for _, c := range cases {
		if err := v.Verify(c.h, nil); (err == nil) != c.ok {
			t.Errorf("%s: got err=%v, want ok=%v", c.name, err, c.ok)
		}
	}
}

func TestMissingSecretFailsClosed(t *testing.T) {
	t.Setenv("EMPTY", "")
	for _, c := range []config.Check{
		{Token: &config.TokenCheck{Header: "X-Token", Secrets: []config.SecretRef{{Env: "EMPTY"}}}},
		{Token: &config.TokenCheck{Header: "X-Token", Secrets: []config.SecretRef{{Env: "UNSET_VAR_FOR_TEST"}}}},
		{Token: &config.TokenCheck{Header: "X-Token", Secrets: []config.SecretRef{{Env: "EMPTY", Optional: true}}}},
		{HMAC: &config.HMACCheck{Header: "X-Sig", Algorithm: "sha256", Encoding: "hex", Secrets: []config.SecretRef{{File: "/nonexistent/secret"}}}},
	} {
		if _, err := Build(c); err == nil {
			t.Errorf("Build(%+v) succeeded with no usable secret", c)
		}
	}
}

func TestShortSecretFailsClosed(t *testing.T) {
	t.Setenv("SHORT", "fifteen-bytes!!")
	t.Setenv("LONG", "sixteen-bytes!!!")
	short := []config.SecretRef{{Env: "SHORT"}}
	for _, c := range []config.Check{
		{Token: &config.TokenCheck{Header: "X-Token", Secrets: short}},
		{HMAC: &config.HMACCheck{Header: "X-Sig", Algorithm: "sha256", Encoding: "hex", Secrets: short}},
		{HMAC: &config.HMACCheck{Header: "X-Sig", Algorithm: "sha256", Encoding: "hex",
			Secrets: []config.SecretRef{{Env: "LONG"}, {Env: "SHORT", Optional: true}}}},
	} {
		_, err := Build(c)
		if err == nil {
			t.Fatalf("Build(%+v) accepted a 15-byte secret", c)
		}
		if !strings.Contains(err.Error(), "env:SHORT") || strings.Contains(err.Error(), "fifteen-bytes") {
			t.Errorf("error must name the reference and never the value: %v", err)
		}
	}
	if _, err := Build(config.Check{Token: &config.TokenCheck{Header: "X-Token", Secrets: []config.SecretRef{{Env: "LONG"}}}}); err != nil {
		t.Errorf("16-byte secret rejected: %v", err)
	}
}

func TestHMAC(t *testing.T) {
	body := `[{"eventName":"s3:ObjectCreated:Put"}]` + "\r\n"
	type tc struct {
		name string
		cfg  config.HMACCheck
		sig  string
		body string
		ok   bool
	}
	b64 := base64.StdEncoding.EncodeToString(mac(sha256.New, key1, body))
	b64next := base64.StdEncoding.EncodeToString(mac(sha256.New, key2, body))
	hx := hex.EncodeToString(mac(sha256.New, key1, body))
	e2 := config.HMACCheck{Header: "X-Sig", Algorithm: "sha256", Encoding: "base64"}
	gh := config.HMACCheck{Header: "X-Sig", Algorithm: "sha256", Encoding: "hex", Prefix: "sha256="}
	multi := config.HMACCheck{Header: "X-Sig", Algorithm: "sha256", Encoding: "hex", Prefix: "v1=", Separator: ","}
	cases := []tc{
		{"base64 valid", e2, b64, body, true},
		{"base64 rotation key", e2, b64next, body, true},
		{"base64 tampered body", e2, b64, body + " ", false},
		{"base64 sig sent as hex", e2, hx, body, false},
		{"base64 garbage", e2, "!!!", body, false},
		{"hex with prefix", gh, "sha256=" + hx, body, true},
		{"hex upper case", gh, "sha256=" + strings.ToUpper(hx), body, true},
		{"hex missing prefix", gh, hx, body, false},
		{"multi second matches", multi, "v1=00ff,v1=" + hx, body, true},
		{"multi none match", multi, "v1=00ff,v0=" + hx, body, false},
		{"sha1", config.HMACCheck{Header: "X-Sig", Algorithm: "sha1", Encoding: "hex"}, hex.EncodeToString(mac(sha1.New, key1, body)), body, true},
		{"sha512 base64url", config.HMACCheck{Header: "X-Sig", Algorithm: "sha512", Encoding: "base64url"}, base64.RawURLEncoding.EncodeToString(mac(sha512.New, key1, body)), body, true},
		{"empty signature", e2, "", body, false},
	}
	t.Setenv("K1", key1)
	t.Setenv("K2", key2)
	for _, c := range cases {
		c.cfg.Secrets = []config.SecretRef{{Env: "K1"}, {Env: "K2", Optional: true}}
		v, err := Build(config.Check{HMAC: &c.cfg})
		if err != nil {
			t.Fatal(err)
		}
		h := hdr("X-Sig", c.sig)
		if err := v.Verify(h, []byte(c.body)); (err == nil) != c.ok {
			t.Errorf("%s: got err=%v, want ok=%v", c.name, err, c.ok)
		}
	}
	v, _ := Build(config.Check{HMAC: &config.HMACCheck{Header: "X-Sig", Algorithm: "sha256", Encoding: "base64", Secrets: []config.SecretRef{{Env: "K1"}}}})
	if err := v.Verify(hdr(), []byte(body)); err == nil {
		t.Error("missing signature header accepted")
	}
}
