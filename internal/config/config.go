// Package config loads and validates the hookgate configuration file.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
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
	Name         string  `yaml:"name"`
	Path         string  `yaml:"path"`
	MaxBodyBytes int64   `yaml:"max_body_bytes"`
	AllowQuery   bool    `yaml:"allow_query"`
	RejectStatus int     `yaml:"reject_status"`
	Checks       []Check `yaml:"checks"`
	Forward      Forward `yaml:"forward"`
}

// Check is one verification step; exactly one of its fields is set. All checks of a source must pass.
type Check struct {
	Token *TokenCheck `yaml:"token"`
	HMAC  *HMACCheck  `yaml:"hmac"`
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
		for _, ch := range s.Checks {
			if ch.HMAC != nil && ch.HMAC.Encoding == "" {
				ch.HMAC.Encoding = "hex"
			}
		}
	}
}

func (c *Config) validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	if c.MaxConcurrent < 1 {
		add("max_concurrent must be >= 1")
	}
	if len(c.Sources) == 0 {
		add("at least one source is required")
	}
	names, paths := map[string]bool{}, map[string]bool{}
	for i, s := range c.Sources {
		where := fmt.Sprintf("sources[%d]", i)
		if s.Name == "" {
			add("%s: name is required", where)
		} else {
			where = fmt.Sprintf("source %q", s.Name)
		}
		if names[s.Name] {
			add("%s: duplicate name", where)
		}
		names[s.Name] = true
		if !strings.HasPrefix(s.Path, "/") || path.Clean(s.Path) != s.Path {
			add("%s: path must be an absolute, clean URL path", where)
		}
		if paths[s.Path] {
			add("%s: duplicate path %s", where, s.Path)
		}
		paths[s.Path] = true
		if s.MaxBodyBytes < 1 {
			add("%s: max_body_bytes must be >= 1", where)
		}
		if s.RejectStatus < 200 || s.RejectStatus > 599 {
			add("%s: reject_status must be an HTTP status code", where)
		}
		if len(s.Checks) == 0 {
			add("%s: at least one check is required (a source never forwards unverified requests)", where)
		}
		for j, ch := range s.Checks {
			cw := fmt.Sprintf("%s checks[%d]", where, j)
			switch {
			case ch.Token != nil && ch.HMAC != nil, ch.Token == nil && ch.HMAC == nil:
				add("%s: set exactly one of token, hmac", cw)
			case ch.Token != nil:
				if ch.Token.Header == "" {
					add("%s: token.header is required", cw)
				}
				errs = append(errs, validateSecrets(cw+" token", ch.Token.Secrets)...)
			case ch.HMAC != nil:
				h := ch.HMAC
				if h.Header == "" {
					add("%s: hmac.header is required", cw)
				}
				switch h.Algorithm {
				case "sha1", "sha256", "sha512":
				default:
					add("%s: hmac.algorithm must be sha1, sha256 or sha512", cw)
				}
				switch h.Encoding {
				case "hex", "base64", "base64url":
				default:
					add("%s: hmac.encoding must be hex, base64 or base64url", cw)
				}
				errs = append(errs, validateSecrets(cw+" hmac", h.Secrets)...)
			}
		}
		f := s.Forward
		if len(f.URLs) == 0 {
			add("%s: forward.urls is required", where)
		}
		for _, u := range f.URLs {
			pu, err := url.Parse(u)
			if err != nil || (pu.Scheme != "http" && pu.Scheme != "https") || pu.Host == "" {
				add("%s: forward url %q must be an absolute http(s) URL", where, u)
			}
		}
		for name, ref := range f.SetHeaders {
			errs = append(errs, validateSecrets(fmt.Sprintf("%s forward.set_headers[%s]", where, name), []SecretRef{ref})...)
		}
		if f.AttemptTimeout <= 0 || f.Timeout < f.AttemptTimeout {
			add("%s: forward timeouts must satisfy 0 < attempt_timeout <= timeout", where)
		}
		if f.MaxAttempts < 1 {
			add("%s: forward.max_attempts must be >= 1", where)
		}
	}
	return errors.Join(errs...)
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
