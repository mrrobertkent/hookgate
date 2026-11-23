// Package config loads and validates the hookgate configuration file.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Config is the top-level configuration.
type Config struct {
	Listen            string        `yaml:"listen"`
	AdminListen       string        `yaml:"admin_listen"`
	MaxConcurrent     int           `yaml:"max_concurrent"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	ReadTimeout       time.Duration `yaml:"read_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
	DrainDelay        time.Duration `yaml:"drain_delay"`
	ShutdownTimeout   time.Duration `yaml:"shutdown_timeout"`
	Sources           []Source      `yaml:"sources"`
}

// Source is one webhook sender: where it posts, how it proves itself, and where verified requests go.
type Source struct {
	Name         string   `yaml:"name"`
	Hosts        HostList `yaml:"hosts"` // exact names; none matches any host
	Path         string   `yaml:"path"`
	MaxBodyBytes int64    `yaml:"max_body_bytes"`
	AllowQuery   bool     `yaml:"allow_query"`
	RejectStatus int      `yaml:"reject_status"`
	Checks       []Check  `yaml:"checks"`
	Forward      Forward  `yaml:"forward"`
}

// Check is one verification step; exactly one of its fields is set. All checks of a source must pass.
type Check struct {
	Token            *TokenCheck            `yaml:"token"`
	HMAC             *HMACCheck             `yaml:"hmac"`
	StandardWebhooks *StandardWebhooksCheck `yaml:"standard_webhooks"`
}

// TokenCheck compares a request header with a shared secret.
type TokenCheck struct {
	Header  string      `yaml:"header"`
	Secrets []SecretRef `yaml:"secrets"`
}

// HMACCheck verifies a keyed-hash signature of the raw request body carried in a header.
type HMACCheck struct {
	Header    string      `yaml:"header"`
	Algorithm string      `yaml:"algorithm"` // sha1 | sha256 | sha512
	Encoding  string      `yaml:"encoding"`  // hex | base64 | base64url
	Prefix    string      `yaml:"prefix"`    // e.g. "sha256="; required on every signature when set
	Separator string      `yaml:"separator"` // splits a header carrying several signatures
	Secrets   []SecretRef `yaml:"secrets"`
}

// StandardWebhooksCheck verifies a Standard Webhooks signature: an HMAC-SHA256 over the event id, timestamp and
// raw body, with the timestamp bounded in both directions.
type StandardWebhooksCheck struct {
	Headers   string         `yaml:"headers"`   // standard (webhook-*) | svix (svix-*)
	Tolerance *time.Duration `yaml:"tolerance"` // largest accepted distance between the timestamp and now; nil until defaulted
	Secrets   []SecretRef    `yaml:"secrets"`
}

// SecretRef names where a secret value comes from: an environment variable or a file.
type SecretRef struct {
	Env      string `yaml:"env"`
	File     string `yaml:"file"`
	Optional bool   `yaml:"optional"`
}

// Forward describes the upstream that receives verified requests.
type Forward struct {
	URLs           []string             `yaml:"urls"`
	ResolveAll     bool                 `yaml:"resolve_all"`
	PassHeaders    []string             `yaml:"pass_headers"`
	SetHeaders     map[string]SecretRef `yaml:"set_headers"`
	AttemptTimeout time.Duration        `yaml:"attempt_timeout"`
	Timeout        time.Duration        `yaml:"timeout"`
	MaxAttempts    int                  `yaml:"max_attempts"`
}

// Load reads, defaults and validates a configuration file. Unknown keys are errors.
func Load(file string) (*Config, error) {
	raw, err := os.ReadFile(file) //nolint:gosec // G304: the operator chooses the config path.
	if err != nil {
		return nil, err
	}
	return Parse(raw)
}

// Parse decodes, defaults and validates configuration bytes. Unknown keys are errors.
func Parse(raw []byte) (*Config, error) {
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := rejectNullHosts(raw); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	def := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	if c.AdminListen == "" {
		c.AdminListen = ":9090"
	}
	if c.MaxConcurrent == 0 {
		c.MaxConcurrent = 64
	}
	def(&c.ReadHeaderTimeout, 5*time.Second)
	def(&c.ReadTimeout, 15*time.Second)
	def(&c.WriteTimeout, 60*time.Second)
	def(&c.IdleTimeout, 120*time.Second)
	def(&c.ShutdownTimeout, 30*time.Second)
	for i := range c.Sources {
		s := &c.Sources[i]
		if s.MaxBodyBytes == 0 {
			s.MaxBodyBytes = 1 << 20
		}
		if s.RejectStatus == 0 {
			s.RejectStatus = 401
		}
		f := &s.Forward
		def(&f.AttemptTimeout, 5*time.Second)
		def(&f.Timeout, 20*time.Second)
		if f.MaxAttempts == 0 {
			f.MaxAttempts = 3
		}
		for j := range s.Checks {
			s.Checks[j].applyDefaults()
		}
	}
}

func (ch *Check) applyDefaults() {
	if ch.HMAC != nil && ch.HMAC.Encoding == "" {
		ch.HMAC.Encoding = "hex"
	}
	if sw := ch.StandardWebhooks; sw != nil {
		if sw.Headers == "" {
			sw.Headers = "standard"
		}
		if sw.Tolerance == nil {
			d := 5 * time.Minute
			sw.Tolerance = &d
		}
	}
}

func (c *Config) validate() error {
	var errs []error
	if c.MaxConcurrent < 1 {
		errs = append(errs, errors.New("max_concurrent must be >= 1"))
	}
	if len(c.Sources) == 0 {
		errs = append(errs, errors.New("at least one source is required"))
	}
	names := map[string]bool{}
	routes := map[route]string{}
	for i := range c.Sources {
		s := &c.Sources[i]
		where := fmt.Sprintf("sources[%d]", i)
		if s.Name != "" {
			where = fmt.Sprintf("source %q", s.Name)
		}
		errs = append(errs, validateName(where, s.Name, names)...)
		errs = append(errs, s.validateRoutes(where, routes)...)
		errs = append(errs, s.validate(where)...)
	}
	return errors.Join(errs...)
}

// route is one (host, path) pair a source answers; an empty host stands for the source without hosts.
type route struct{ host, path string }

var nameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,62}$`)

func validateName(where, name string, seen map[string]bool) []error {
	var errs []error
	switch {
	case name == "":
		errs = append(errs, fmt.Errorf("%s: name is required", where))
	case !nameRE.MatchString(name):
		errs = append(errs, fmt.Errorf("%s: name must match %s", where, nameRE))
	case seen[name]:
		errs = append(errs, fmt.Errorf("%s: duplicate name", where))
	}
	seen[name] = true
	return errs
}

// validateRoutes checks the path and hosts, replaces each host with its normal form, and claims every
// (host, path) pair in claimed, which maps a pair to the source that holds it.
func (s *Source) validateRoutes(where string, claimed map[route]string) []error {
	var errs []error
	if !strings.HasPrefix(s.Path, "/") || path.Clean(s.Path) != s.Path {
		errs = append(errs, fmt.Errorf("%s: path must be an absolute, clean URL path", where))
	}
	claim := func(r route) {
		if owner, ok := claimed[r]; ok {
			errs = append(errs, duplicateRoute(where, r, owner))
			return
		}
		claimed[r] = s.Name
	}
	if len(s.Hosts) == 0 {
		claim(route{path: s.Path})
	}
	for k, h := range s.Hosts {
		n, err := NormalizeHost(h)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: hosts[%d]: %w", where, k, err))
			continue
		}
		s.Hosts[k] = n
		claim(route{host: n, path: s.Path})
	}
	return errs
}

func duplicateRoute(where string, r route, owner string) error {
	if r.host == "" {
		return fmt.Errorf("%s: path %s without hosts is already used by source %q", where, r.path, owner)
	}
	return fmt.Errorf("%s: host %s on path %s is already used by source %q", where, r.host, r.path, owner)
}

func (s *Source) validate(where string) []error {
	var errs []error
	if s.MaxBodyBytes < 1 {
		errs = append(errs, fmt.Errorf("%s: max_body_bytes must be >= 1", where))
	}
	if s.RejectStatus < 200 || s.RejectStatus > 599 {
		errs = append(errs, fmt.Errorf("%s: reject_status must be an HTTP status code", where))
	}
	if len(s.Checks) == 0 {
		errs = append(errs, fmt.Errorf("%s: at least one check is required (a source never forwards unverified requests)", where))
	}
	errs = append(errs, validateChecks(where, s.Checks)...)
	return append(errs, s.Forward.validate(where)...)
}

// validateChecks also allows at most one check that authenticates an event id, so a source has one event id.
func validateChecks(where string, checks []Check) []error {
	var errs []error
	ids := 0
	for j, ch := range checks {
		errs = append(errs, ch.validate(fmt.Sprintf("%s checks[%d]", where, j))...)
		if ch.StandardWebhooks != nil {
			ids++
		}
	}
	if ids > 1 {
		errs = append(errs, fmt.Errorf("%s: at most one standard_webhooks check is allowed", where))
	}
	return errs
}

func (ch Check) validate(where string) []error {
	set := 0
	for _, kind := range []bool{ch.Token != nil, ch.HMAC != nil, ch.StandardWebhooks != nil} {
		if kind {
			set++
		}
	}
	switch {
	case set != 1:
		return []error{fmt.Errorf("%s: set exactly one of token, hmac, standard_webhooks", where)}
	case ch.Token != nil:
		return ch.Token.validate(where)
	case ch.HMAC != nil:
		return ch.HMAC.validate(where)
	default:
		return ch.StandardWebhooks.validate(where)
	}
}

func (t *TokenCheck) validate(where string) []error {
	var errs []error
	if t.Header == "" {
		errs = append(errs, fmt.Errorf("%s: token.header is required", where))
	}
	return append(errs, validateSecrets(where+" token", t.Secrets)...)
}

func (h *HMACCheck) validate(where string) []error {
	var errs []error
	if h.Header == "" {
		errs = append(errs, fmt.Errorf("%s: hmac.header is required", where))
	}
	switch h.Algorithm {
	case "sha1", "sha256", "sha512":
	default:
		errs = append(errs, fmt.Errorf("%s: hmac.algorithm must be sha1, sha256 or sha512", where))
	}
	switch h.Encoding {
	case "hex", "base64", "base64url":
	default:
		errs = append(errs, fmt.Errorf("%s: hmac.encoding must be hex, base64 or base64url", where))
	}
	return append(errs, validateSecrets(where+" hmac", h.Secrets)...)
}

func (f *Forward) validate(where string) []error {
	var errs []error
	if len(f.URLs) == 0 {
		errs = append(errs, fmt.Errorf("%s: forward.urls is required", where))
	}
	for _, u := range f.URLs {
		pu, err := url.Parse(u)
		if err != nil || (pu.Scheme != "http" && pu.Scheme != "https") || pu.Host == "" {
			errs = append(errs, fmt.Errorf("%s: forward url %q must be an absolute http(s) URL", where, u))
		}
	}
	for name, ref := range f.SetHeaders {
		errs = append(errs, validateSecrets(fmt.Sprintf("%s forward.set_headers[%s]", where, name), []SecretRef{ref})...)
	}
	if f.AttemptTimeout <= 0 || f.Timeout < f.AttemptTimeout {
		errs = append(errs, fmt.Errorf("%s: forward timeouts must satisfy 0 < attempt_timeout <= timeout", where))
	}
	if f.MaxAttempts < 1 {
		errs = append(errs, fmt.Errorf("%s: forward.max_attempts must be >= 1", where))
	}
	return errs
}

func (sw *StandardWebhooksCheck) validate(where string) []error {
	var errs []error
	if sw.Headers != "standard" && sw.Headers != "svix" {
		errs = append(errs, fmt.Errorf("%s: standard_webhooks.headers must be standard or svix", where))
	}
	if sw.Tolerance == nil || *sw.Tolerance <= 0 || *sw.Tolerance > time.Hour {
		errs = append(errs, fmt.Errorf("%s: standard_webhooks.tolerance must be > 0 and <= 1h", where))
	}
	return append(errs, validateSecrets(where+" standard_webhooks", sw.Secrets)...)
}

func validateSecrets(where string, refs []SecretRef) []error {
	var errs []error
	if len(refs) == 0 {
		errs = append(errs, fmt.Errorf("%s: at least one secret is required", where))
	}
	for k, r := range refs {
		if (r.Env == "") == (r.File == "") {
			errs = append(errs, fmt.Errorf("%s: secrets[%d] must set exactly one of env, file", where, k))
		}
	}
	return errs
}

// Resolve returns the secret value. An unset or empty value is an error unless the reference is optional,
// in which case it resolves to "" and is skipped by callers.
func (r SecretRef) Resolve() (string, error) {
	var v string
	switch {
	case r.Env != "":
		v = os.Getenv(r.Env)
		if v == "" && !r.Optional {
			return "", fmt.Errorf("environment variable %s is unset or empty", r.Env)
		}
	default:
		b, err := os.ReadFile(r.File)
		if err != nil {
			if r.Optional && errors.Is(err, os.ErrNotExist) {
				return "", nil
			}
			return "", fmt.Errorf("secret file %s: %w", r.File, err)
		}
		v = strings.TrimRight(string(b), "\r\n")
		if v == "" && !r.Optional {
			return "", fmt.Errorf("secret file %s is empty", r.File)
		}
	}
	return v, nil
}

// String names the reference without its value, for logs.
func (r SecretRef) String() string {
	if r.Env != "" {
		return "env:" + r.Env
	}
	return "file:" + r.File
}
