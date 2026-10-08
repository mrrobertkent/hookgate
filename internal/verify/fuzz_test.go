package verify

import (
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"strings"
	"testing"

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
		if v.Verify(h, body) == nil {
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
