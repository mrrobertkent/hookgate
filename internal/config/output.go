package config

import (
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"time"

	"github.com/mrrobertkent/hookgate/internal/contract"
)

// Connection is a named connection that outputs refer to; exactly one kind is set.
type Connection struct {
	NATS *NATSConnection `yaml:"nats"`
}

// Output is where a source's verified requests go; exactly one kind is set.
type Output struct {
	HTTP      *HTTPOutput      `yaml:"http"`
	JetStream *JetStreamOutput `yaml:"jetstream"`
}

// HTTPOutput forwards verified requests to HTTP upstreams.
type HTTPOutput struct {
	URLs           []string             `yaml:"urls"`
	ResolveAll     bool                 `yaml:"resolve_all"`
	PassHeaders    []string             `yaml:"pass_headers"`
	SetHeaders     map[string]SecretRef `yaml:"set_headers"`
	AttemptTimeout time.Duration        `yaml:"attempt_timeout"`
	Timeout        time.Duration        `yaml:"timeout"`
	MaxAttempts    int                  `yaml:"max_attempts"`
}

// JetStreamOutput publishes verified requests to a NATS JetStream stream.
type JetStreamOutput struct {
	Connection     string               `yaml:"connection"`
	Subject        string               `yaml:"subject"` // template; {source} is the only placeholder
	Stream         string               `yaml:"stream"`
	PassHeaders    []string             `yaml:"pass_headers"`
	SetHeaders     map[string]SecretRef `yaml:"set_headers"`
	MessageID      MessageID            `yaml:"message_id"`
	ClaimCheck     *ClaimCheck          `yaml:"claim_check"`
	InlineMaxBytes int64                `yaml:"inline_max_bytes"`
	MaxHeaderBytes int64                `yaml:"max_header_bytes"`
	AttemptTimeout time.Duration        `yaml:"attempt_timeout"`
	Timeout        time.Duration        `yaml:"timeout"`
}

// MessageID says where an event ID may come from besides a check that authenticates one.
type MessageID struct {
	Header          string `yaml:"header"`
	Unauthenticated bool   `yaml:"unauthenticated"`
}

// ClaimCheck stores bodies above the inline limit and publishes a reference instead.
type ClaimCheck struct {
	ObjectStore ObjectStoreRef `yaml:"object_store"`
	ReuseWithin time.Duration  `yaml:"reuse_within"`
}

// DefaultSubject is the JetStream output's subject template when none is set.
const DefaultSubject = "webhooks.events.{source}"

func (o *Output) applyDefaults() {
	if h := o.HTTP; h != nil {
		setDefault(&h.AttemptTimeout, 5*time.Second)
		setDefault(&h.Timeout, 20*time.Second)
		setDefault(&h.MaxAttempts, 3)
	}
	if j := o.JetStream; j != nil {
		setDefault(&j.Subject, DefaultSubject)
		setDefault(&j.InlineMaxBytes, 256<<10)
		setDefault(&j.MaxHeaderBytes, 16<<10)
		setDefault(&j.AttemptTimeout, 5*time.Second)
		setDefault(&j.Timeout, 10*time.Second)
		if j.ClaimCheck != nil {
			setDefault(&j.ClaimCheck.ReuseWithin, 24*time.Hour)
		}
	}
}

func (c *Config) validateConnections() []error {
	var errs []error
	names := make([]string, 0, len(c.Connections))
	for name := range c.Connections {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		where := fmt.Sprintf("connection %q", name)
		if !nameRE.MatchString(name) {
			errs = append(errs, fmt.Errorf("%s: name must match %s", where, nameRE))
		}
		conn := c.Connections[name]
		if conn.NATS == nil {
			errs = append(errs, fmt.Errorf("%s: set nats", where))
			continue
		}
		errs = append(errs, conn.NATS.Validate(where)...)
	}
	return errs
}

func (o *Output) validate(where string, s *Source, c *Config) []error {
	switch {
	case (o.HTTP == nil) == (o.JetStream == nil):
		return []error{fmt.Errorf("%s: output must set exactly one of http, jetstream", where)}
	case o.HTTP != nil:
		return o.HTTP.validate(where)
	default:
		return o.JetStream.validate(where, s, c)
	}
}

func (h *HTTPOutput) validate(where string) []error {
	var errs []error
	if len(h.URLs) == 0 {
		errs = append(errs, fmt.Errorf("%s: output.http.urls is required", where))
	}
	for _, u := range h.URLs {
		pu, err := url.Parse(u)
		if err != nil || (pu.Scheme != "http" && pu.Scheme != "https") || pu.Host == "" {
			errs = append(errs, fmt.Errorf("%s: output.http url %q must be an absolute http(s) URL", where, u))
		}
	}
	for name, ref := range h.SetHeaders {
		errs = append(errs, validateSecrets(fmt.Sprintf("%s output.http.set_headers[%s]", where, name), []SecretRef{ref})...)
	}
	if h.AttemptTimeout <= 0 || h.Timeout < h.AttemptTimeout {
		errs = append(errs, fmt.Errorf("%s: output.http timeouts must satisfy 0 < attempt_timeout <= timeout", where))
	}
	if h.MaxAttempts < 1 {
		errs = append(errs, fmt.Errorf("%s: output.http.max_attempts must be >= 1", where))
	}
	return errs
}

// streamNameRE follows JetStream's stream naming rule.
var streamNameRE = regexp.MustCompile(`^[^.*>/\\\s\x00-\x1f]{1,255}$`)

func (j *JetStreamOutput) validate(where string, s *Source, c *Config) []error {
	w := where + ": output.jetstream"
	var errs []error
	if conn, ok := c.Connections[j.Connection]; !ok || conn.NATS == nil {
		errs = append(errs, fmt.Errorf("%s.connection %q must name a nats connection", w, j.Connection))
	}
	if err := contract.ValidateSubjectTemplate(j.Subject, "source"); err != nil {
		errs = append(errs, fmt.Errorf("%s.subject: %w", w, err))
	}
	if !streamNameRE.MatchString(j.Stream) {
		errs = append(errs, fmt.Errorf("%s.stream is required and must be a stream name (no '.', '*', '>', '/', '\\' or whitespace)", w))
	}
	errs = append(errs, j.validateHeaders(w)...)
	errs = append(errs, j.validateSizes(w, s)...)
	errs = append(errs, j.validateTimes(w, s, c)...)
	return errs
}

func (j *JetStreamOutput) validateHeaders(w string) []error {
	var errs []error
	for _, p := range j.PassHeaders {
		if !headerPatternRE.MatchString(p) {
			errs = append(errs, fmt.Errorf("%s.pass_headers: %q is not a header name or a prefix ending in *", w, p))
		} else if contract.MayMatchReserved(p) {
			errs = append(errs, fmt.Errorf("%s.pass_headers: %q can match a reserved name (Hookgate-*, Nats-*, Status, Description)", w, p))
		}
	}
	for _, name := range sortedNames(j.SetHeaders) {
		if !headerNameRE.MatchString(name) {
			errs = append(errs, fmt.Errorf("%s.set_headers: %q is not a header name", w, name))
		} else if contract.IsReserved(name) {
			errs = append(errs, fmt.Errorf("%s.set_headers: %q is a reserved name (Hookgate-*, Nats-*, Status, Description)", w, name))
		}
		errs = append(errs, validateRequiredSecret(fmt.Sprintf("%s.set_headers[%s]", w, name), j.SetHeaders[name])...)
	}
	m := j.MessageID
	if m.Header != "" && !headerNameRE.MatchString(m.Header) {
		errs = append(errs, fmt.Errorf("%s.message_id.header %q is not a header name", w, m.Header))
	}
	if (m.Header != "") != m.Unauthenticated {
		errs = append(errs, fmt.Errorf("%s.message_id: header and unauthenticated: true go together (an unsigned header is anyone's choice)", w))
	}
	return errs
}

func (j *JetStreamOutput) validateSizes(w string, s *Source) []error {
	var errs []error
	if j.InlineMaxBytes < 1 {
		errs = append(errs, fmt.Errorf("%s.inline_max_bytes must be >= 1", w))
	}
	if j.MaxHeaderBytes < 1024 {
		errs = append(errs, fmt.Errorf("%s.max_header_bytes must be >= 1024", w))
	}
	if j.ClaimCheck == nil {
		if s.MaxBodyBytes > j.InlineMaxBytes {
			errs = append(errs, fmt.Errorf("%s: max_body_bytes (%d) exceeds inline_max_bytes (%d), so claim_check is required", w, s.MaxBodyBytes, j.InlineMaxBytes))
		}
		return errs
	}
	errs = append(errs, j.ClaimCheck.ObjectStore.Validate(w+".claim_check")...)
	if j.ClaimCheck.ReuseWithin <= 0 {
		errs = append(errs, fmt.Errorf("%s.claim_check.reuse_within must be > 0", w))
	}
	return errs
}

func (j *JetStreamOutput) validateTimes(w string, s *Source, c *Config) []error {
	var errs []error
	if j.AttemptTimeout <= 0 || j.Timeout < j.AttemptTimeout {
		errs = append(errs, fmt.Errorf("%s: timeouts must satisfy 0 < attempt_timeout <= timeout", w))
	}
	if c.ReadTimeout+j.Timeout > c.ShutdownTimeout {
		errs = append(errs, fmt.Errorf("%s: read_timeout (%s) + timeout (%s) must not exceed shutdown_timeout (%s), so every admitted request can finish its publish",
			w, c.ReadTimeout, j.Timeout, c.ShutdownTimeout))
	}
	if s.RejectStatus >= 200 && s.RejectStatus < 300 {
		errs = append(errs, fmt.Errorf("%s: reject_status must not be 2xx (a 2xx tells the sender its event is stored)", w))
	}
	return errs
}

// headerNameRE is an RFC 9110 field name (token).
var (
	headerNameRE    = regexp.MustCompile("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")
	headerPatternRE = regexp.MustCompile("^[!#$%&'+.^_`|~0-9A-Za-z-]+\\*?$|^\\*$")
)

func sortedNames[V any](m map[string]V) []string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	slices.Sort(names)
	return names
}
