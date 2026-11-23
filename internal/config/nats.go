package config

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// NATSConnection is one connection to a NATS cluster. Exactly one of NKeySeed, Creds, Token and None is set.
type NATSConnection struct {
	URLs           []string      `yaml:"urls"`
	NKeySeed       *SecretRef    `yaml:"nkey_seed"`
	Creds          *SecretRef    `yaml:"creds"`
	Token          *SecretRef    `yaml:"token"`
	None           bool          `yaml:"none"`
	TLS            *TLS          `yaml:"tls"`
	InboxPrefix    string        `yaml:"inbox_prefix"`
	ConnectTimeout time.Duration `yaml:"connect_timeout"`
}

// TLS configures the client side of TLS: CA to trust (else the system roots) and an optional client certificate.
type TLS struct {
	CA   *SecretRef `yaml:"ca"`
	Cert *SecretRef `yaml:"cert"`
	Key  *SecretRef `yaml:"key"`
}

// ObjectStoreRef names a JetStream Object Store bucket.
type ObjectStoreRef struct {
	Bucket string `yaml:"bucket"`
}

// ApplyDefaults fills unset keys; name is the connection's name, which the default inbox prefix uses.
func (n *NATSConnection) ApplyDefaults(name string) {
	if n.InboxPrefix == "" {
		n.InboxPrefix = "_INBOX." + name
	}
	if n.ConnectTimeout == 0 {
		n.ConnectTimeout = 5 * time.Second
	}
}

// Validate checks a connection after ApplyDefaults.
func (n *NATSConnection) Validate(where string) []error {
	var errs []error
	if len(n.URLs) == 0 {
		errs = append(errs, fmt.Errorf("%s: nats.urls is required", where))
	}
	for _, u := range n.URLs {
		pu, err := url.Parse(u)
		if err != nil || !validNATSScheme(pu.Scheme) || pu.Host == "" || pu.User != nil {
			errs = append(errs, fmt.Errorf("%s: nats url %q must be nats://, tls://, ws:// or wss:// host:port without credentials", where, redactURL(u)))
		}
	}
	errs = append(errs, n.validateAuth(where)...)
	if n.TLS != nil {
		errs = append(errs, n.TLS.validate(where)...)
	}
	if !validInboxPrefix(n.InboxPrefix) {
		errs = append(errs, fmt.Errorf("%s: nats.inbox_prefix %q must be dot-separated tokens without whitespace or wildcards", where, n.InboxPrefix))
	}
	if n.ConnectTimeout <= 0 {
		errs = append(errs, fmt.Errorf("%s: nats.connect_timeout must be > 0", where))
	}
	return errs
}

func validNATSScheme(s string) bool {
	switch s {
	case "nats", "tls", "ws", "wss":
		return true
	}
	return false
}

func (n *NATSConnection) validateAuth(where string) []error {
	set := 0
	var errs []error
	for _, a := range []struct {
		key string
		ref *SecretRef
	}{{"nkey_seed", n.NKeySeed}, {"creds", n.Creds}, {"token", n.Token}} {
		if a.ref == nil {
			continue
		}
		set++
		errs = append(errs, validateRequiredSecret(fmt.Sprintf("%s: nats.%s", where, a.key), *a.ref)...)
	}
	if n.None {
		set++
	}
	if set != 1 {
		errs = append(errs, fmt.Errorf("%s: nats must set exactly one of nkey_seed, creds, token, none", where))
	}
	return errs
}

func (t *TLS) validate(where string) []error {
	var errs []error
	if (t.Cert == nil) != (t.Key == nil) {
		errs = append(errs, fmt.Errorf("%s: nats.tls.cert and nats.tls.key go together", where))
	}
	for _, r := range []struct {
		key string
		ref *SecretRef
	}{{"ca", t.CA}, {"cert", t.Cert}, {"key", t.Key}} {
		if r.ref != nil {
			errs = append(errs, validateRequiredSecret(fmt.Sprintf("%s: nats.tls.%s", where, r.key), *r.ref)...)
		}
	}
	return errs
}

// validateRequiredSecret checks a single secret reference that may not be optional.
func validateRequiredSecret(where string, r SecretRef) []error {
	var errs []error
	if (r.Env == "") == (r.File == "") {
		errs = append(errs, fmt.Errorf("%s must set exactly one of env, file", where))
	}
	if r.Optional {
		errs = append(errs, fmt.Errorf("%s must not be optional", where))
	}
	return errs
}

// validInboxPrefix follows nats.CustomInboxPrefix: no wildcards, no empty tokens, no trailing dot.
func validInboxPrefix(p string) bool {
	if p == "" {
		return false
	}
	for tok := range strings.SplitSeq(p, ".") {
		if tok == "" || strings.ContainsAny(tok, "*> \t\r\n") {
			return false
		}
	}
	return true
}

var bucketRE = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// Validate checks the bucket name against the Object Store naming rule.
func (o ObjectStoreRef) Validate(where string) []error {
	if !bucketRE.MatchString(o.Bucket) {
		return []error{fmt.Errorf("%s: object_store.bucket must match %s", where, bucketRE)}
	}
	return nil
}

// redactURL hides any user information in a URL for errors and logs, and the whole value when it does not parse.
func redactURL(u string) string {
	pu, err := url.Parse(u)
	if err != nil {
		return "(unparseable URL)"
	}
	if pu.User != nil {
		pu.User = url.User("redacted")
	}
	return pu.String()
}
