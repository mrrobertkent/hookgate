package relay

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/mrrobertkent/hookgate/internal/config"
	"github.com/mrrobertkent/hookgate/internal/contract"
)

// Config is the relay configuration.
type Config struct {
	AdminListen      string          `yaml:"admin_listen"`
	Connection       Connection      `yaml:"connection"`
	Stream           string          `yaml:"stream"`
	Consumer         string          `yaml:"consumer"`
	FilterSubjects   []string        `yaml:"filter_subjects"`
	ReplaySubject    string          `yaml:"replay_subject"`
	PassHeaders      []string        `yaml:"pass_headers"`
	Concurrency      int             `yaml:"concurrency"`
	MaxAttempts      int             `yaml:"max_attempts"`
	Backoff          []time.Duration `yaml:"backoff"`
	ClaimCheck       *ClaimCheck     `yaml:"claim_check"`
	DeadLetter       DeadLetter      `yaml:"dead_letter"`
	DrainTimeout     time.Duration   `yaml:"drain_timeout"`
	ShutdownNakDelay time.Duration   `yaml:"shutdown_nak_delay"`
	Handler          Handler         `yaml:"handler"`
}

// Connection is the relay's NATS connection.
type Connection struct {
	NATS config.NATSConnection `yaml:"nats"`
}

// ClaimCheck names the bucket claim-checked bodies are fetched from.
type ClaimCheck struct {
	ObjectStore config.ObjectStoreRef `yaml:"object_store"`
}

// DeadLetter is where events that cannot be delivered are written.
type DeadLetter struct {
	Subject string `yaml:"subject"`
	Stream  string `yaml:"stream"`
}

// Handler receives each event; exactly one field is set.
type Handler struct {
	Command *CommandHandler `yaml:"command"`
	HTTP    *HTTPHandler    `yaml:"http"`
}

// CommandHandler runs a program per event, without a shell.
type CommandHandler struct {
	Path     string                      `yaml:"path"`
	Args     []string                    `yaml:"args"`
	EnvAllow []string                    `yaml:"env_allow"`
	SetEnv   map[string]config.SecretRef `yaml:"set_env"`
	Timeout  time.Duration               `yaml:"timeout"`
}

// HTTPHandler posts each event to a URL.
type HTTPHandler struct {
	URL        string                      `yaml:"url"`
	Timeout    time.Duration               `yaml:"timeout"`
	SetHeaders map[string]config.SecretRef `yaml:"set_headers"`
}

// Subject template placeholder for the consumer name.
const consumerPlaceholder = "consumer"

// Defaults.
const (
	DefaultAdminListen      = ":9091"
	DefaultReplaySubject    = "webhooks.replay.{consumer}"
	DefaultDeadSubject      = "webhooks.dead.{consumer}"
	DefaultMaxAttempts      = 10
	DefaultDrainTimeout     = 25 * time.Second
	DefaultShutdownNakDelay = 5 * time.Second
	DefaultCommandTimeout   = 60 * time.Second
	DefaultHTTPTimeout      = 30 * time.Second
)

// DefaultBackoff is the retry delay schedule; its last value repeats.
var DefaultBackoff = []time.Duration{10 * time.Second, time.Minute, 5 * time.Minute, 15 * time.Minute, time.Hour}

// LoadConfig reads, defaults and validates a relay configuration file. Unknown keys are errors.
func LoadConfig(file string) (*Config, error) {
	raw, err := os.ReadFile(file) //nolint:gosec // G304: the operator chooses the config path.
	if err != nil {
		return nil, err
	}
	return ParseConfig(raw)
}

// ParseConfig decodes, defaults and validates configuration bytes. Unknown keys are errors.
func ParseConfig(raw []byte) (*Config, error) {
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

func setDefault[T comparable](v *T, def T) {
	var zero T
	if *v == zero {
		*v = def
	}
}

func (c *Config) applyDefaults() {
	setDefault(&c.AdminListen, DefaultAdminListen)
	setDefault(&c.ReplaySubject, DefaultReplaySubject)
	setDefault(&c.DeadLetter.Subject, DefaultDeadSubject)
	setDefault(&c.Concurrency, 1)
	setDefault(&c.MaxAttempts, DefaultMaxAttempts)
	setDefault(&c.DrainTimeout, DefaultDrainTimeout)
	setDefault(&c.ShutdownNakDelay, DefaultShutdownNakDelay)
	if len(c.Backoff) == 0 {
		c.Backoff = append([]time.Duration(nil), DefaultBackoff...)
	}
	c.Connection.NATS.ApplyDefaults("relay-" + c.Consumer)
	if h := c.Handler.Command; h != nil {
		setDefault(&h.Timeout, DefaultCommandTimeout)
	}
	if h := c.Handler.HTTP; h != nil {
		setDefault(&h.Timeout, DefaultHTTPTimeout)
	}
}

func (c *Config) validate() error {
	var errs []error
	errs = append(errs, c.Connection.NATS.Validate("connection")...)
	errs = append(errs, c.validateNames()...)
	errs = append(errs, c.validateSubjects()...)
	errs = append(errs, validatePassHeaders(c.PassHeaders)...)
	errs = append(errs, c.validateDelivery()...)
	if c.ClaimCheck != nil {
		errs = append(errs, c.ClaimCheck.ObjectStore.Validate("claim_check")...)
	}
	errs = append(errs, c.Handler.validate()...)
	return errors.Join(errs...)
}

// nameRE restricts stream and consumer names to characters that cannot be confused with the ':' separating the
// parts of dead-letter and replay message IDs.
var nameRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,255}$`)

func validName(n string) bool { return nameRE.MatchString(n) }

func isControl(r rune) bool { return r < ' ' || r == 0x7f }

func (c *Config) validateNames() []error {
	var errs []error
	for _, n := range []struct{ key, v string }{
		{"stream", c.Stream}, {"consumer", c.Consumer}, {"dead_letter.stream", c.DeadLetter.Stream},
	} {
		if !validName(n.v) {
			errs = append(errs, fmt.Errorf("%s %q must be 1-255 characters of A-Z a-z 0-9 _ -", n.key, n.v))
		}
	}
	if c.DeadLetter.Stream != "" && c.DeadLetter.Stream == c.Stream {
		errs = append(errs, errors.New("dead_letter.stream must differ from stream"))
	}
	return errs
}

func (c *Config) validateSubjects() []error {
	var errs []error
	if len(c.FilterSubjects) == 0 {
		errs = append(errs, errors.New("filter_subjects is required"))
	}
	for _, f := range c.FilterSubjects {
		if err := validateFilter(f); err != nil {
			errs = append(errs, fmt.Errorf("filter_subjects: %w", err))
		}
	}
	errs = append(errs, c.validateReplaySubject()...)
	if err := contract.ValidateSubjectTemplate(c.DeadLetter.Subject, consumerPlaceholder); err != nil {
		errs = append(errs, fmt.Errorf("dead_letter.subject: %w", err))
	} else if validName(c.Consumer) && matchesAny(c.FilterSubjects, c.deadSubject()) {
		errs = append(errs, fmt.Errorf("dead_letter.subject: %s is matched by filter_subjects, so dead letters would be consumed again", c.deadSubject()))
	}
	return errs
}

// replayToken is reserved for replay subjects: every relay's replay subject is "<prefix>.replay.<consumer>", and no
// other filter subject may match a subject with this token after its first position. Replays therefore reach
// only the consumer named in their subject, whatever prefix any relay uses.
const replayToken = "replay"

// validateReplaySubject keeps replays isolated: the template is "<prefix>.replay.{consumer}", this consumer's
// filters list its own replay subject, and no other filter can match a subject carrying the reserved token.
func (c *Config) validateReplaySubject() []error {
	if err := contract.ValidateSubjectTemplate(c.ReplaySubject, consumerPlaceholder); err != nil {
		return []error{fmt.Errorf("replay_subject: %w", err)}
	}
	prefix, ok := strings.CutSuffix(c.ReplaySubject, "."+replayToken+".{"+consumerPlaceholder+"}")
	if !ok || strings.Contains(prefix, "{") || slices.Contains(strings.Split(prefix, "."), replayToken) {
		return []error{fmt.Errorf("replay_subject %q must be <prefix>.%s.{%s}, with %q only there", c.ReplaySubject, replayToken, consumerPlaceholder, replayToken)}
	}
	if !validName(c.Consumer) {
		return nil
	}
	var errs []error
	own := c.replaySubject()
	if !slices.Contains(c.FilterSubjects, own) {
		errs = append(errs, fmt.Errorf("filter_subjects must list %s, the consumer's replay subject, exactly", own))
	}
	for _, f := range c.FilterSubjects {
		if f != own && mayMatchReplay(f) {
			errs = append(errs, fmt.Errorf("filter_subjects: %s could match a replay subject (a %q token, or a wildcard, after the first token); list event subjects literally", f, replayToken))
		}
	}
	return errs
}

// mayMatchReplay reports whether a filter can match a subject with the reserved replay token in a position other
// than the first: a literal replay token there, or a wildcard that can stand for one.
func mayMatchReplay(filter string) bool {
	toks := strings.Split(filter, ".")
	if toks[0] == ">" {
		return true
	}
	for _, t := range toks[1:] {
		if t == replayToken || t == "*" || t == ">" {
			return true
		}
	}
	return false
}

func (c *Config) replaySubject() string {
	return contract.RenderSubject(c.ReplaySubject, consumerPlaceholder, c.Consumer)
}

func (c *Config) deadSubject() string {
	return contract.RenderSubject(c.DeadLetter.Subject, consumerPlaceholder, c.Consumer)
}

func (c *Config) validateDelivery() []error {
	var errs []error
	if c.Concurrency < 1 {
		errs = append(errs, errors.New("concurrency must be >= 1"))
	}
	if c.MaxAttempts < 1 {
		errs = append(errs, errors.New("max_attempts must be >= 1"))
	}
	for i, d := range c.Backoff {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("backoff[%d] must be > 0", i))
		}
	}
	if c.DrainTimeout <= 0 {
		errs = append(errs, errors.New("drain_timeout must be > 0"))
	}
	if c.ShutdownNakDelay <= 0 {
		errs = append(errs, errors.New("shutdown_nak_delay must be > 0"))
	}
	return errs
}

// headerNameRE is an HTTP field name (RFC 9110 token), optionally ending in * for a prefix.
var headerNameRE = regexp.MustCompile("^[!#$%&'+.^_`|~0-9A-Za-z-]+\\*?$")

func validatePassHeaders(patterns []string) []error {
	var errs []error
	for _, p := range patterns {
		switch {
		case !headerNameRE.MatchString(p) || p == "*":
			errs = append(errs, fmt.Errorf("pass_headers: %q must be a header name, optionally ending in *", p))
		case contract.MayMatchReserved(p):
			errs = append(errs, fmt.Errorf("pass_headers: %q may match a reserved header (Hookgate-*, Nats-*, Status, Description)", p))
		}
	}
	return errs
}

func (h Handler) validate() []error {
	switch {
	case (h.Command == nil) == (h.HTTP == nil):
		return []error{errors.New("handler must set exactly one of command, http")}
	case h.Command != nil:
		return h.Command.validate()
	default:
		return h.HTTP.validate()
	}
}

var envNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// envPrefix is reserved for the variables the relay sets for each event.
const envPrefix = "HOOKGATE_"

func validEnvName(n string) bool {
	return envNameRE.MatchString(n) && !strings.HasPrefix(strings.ToUpper(n), envPrefix)
}

func (h *CommandHandler) validate() []error {
	var errs []error
	if !filepath.IsAbs(h.Path) || filepath.Clean(h.Path) != h.Path {
		errs = append(errs, fmt.Errorf("handler.command.path %q must be a clean absolute path", h.Path))
	}
	seen := map[string]bool{}
	for _, n := range h.EnvAllow {
		if !validEnvName(n) {
			errs = append(errs, fmt.Errorf("handler.command.env_allow: %q must be an environment variable name not starting with %s", n, envPrefix))
		}
		seen[n] = true
	}
	for n, ref := range h.SetEnv {
		if !validEnvName(n) {
			errs = append(errs, fmt.Errorf("handler.command.set_env: %q must be an environment variable name not starting with %s", n, envPrefix))
		}
		if seen[n] {
			errs = append(errs, fmt.Errorf("handler.command.set_env: %s is also in env_allow", n))
		}
		errs = append(errs, validateSecretRef("handler.command.set_env."+n, ref)...)
	}
	if h.Timeout <= 0 {
		errs = append(errs, errors.New("handler.command.timeout must be > 0"))
	}
	return errs
}

func (h *HTTPHandler) validate() []error {
	var errs []error
	u, err := url.Parse(h.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		errs = append(errs, fmt.Errorf("handler.http.url %q must be an absolute http(s) URL", config.RedactURL(h.URL)))
	} else if u.User != nil {
		errs = append(errs, errors.New("handler.http.url must not carry credentials; use set_headers"))
	}
	seen := map[string]string{}
	for n, ref := range h.SetHeaders {
		if prev, dup := seen[strings.ToLower(n)]; dup {
			errs = append(errs, fmt.Errorf("handler.http.set_headers: %q and %q are the same header", prev, n))
		}
		seen[strings.ToLower(n)] = n
		switch {
		case !headerNameRE.MatchString(n) || strings.HasSuffix(n, "*"):
			errs = append(errs, fmt.Errorf("handler.http.set_headers: %q must be a header name", n))
		case contract.IsReserved(n):
			errs = append(errs, fmt.Errorf("handler.http.set_headers: %q is a reserved header", n))
		}
		errs = append(errs, validateSecretRef("handler.http.set_headers."+n, ref)...)
	}
	if h.Timeout <= 0 {
		errs = append(errs, errors.New("handler.http.timeout must be > 0"))
	}
	return errs
}

func validateSecretRef(where string, r config.SecretRef) []error {
	if (r.Env == "") == (r.File == "") {
		return []error{fmt.Errorf("%s must set exactly one of env, file", where)}
	}
	return nil
}

// validateFilter checks a consumer filter subject: non-empty tokens, * only as a whole token, > only as the last.
func validateFilter(f string) error {
	toks := strings.Split(f, ".")
	for i, tok := range toks {
		if tok == "*" || (tok == ">" && i == len(toks)-1) {
			continue
		}
		if err := contract.ValidateSubject(tok); err != nil {
			return fmt.Errorf("%q: tokens must be non-empty, without whitespace; * and > only as whole tokens, > last", f)
		}
	}
	return nil
}

// subjectMatches reports whether a literal subject matches a filter with * and > wildcards.
func subjectMatches(filter, subject string) bool {
	ft, st := strings.Split(filter, "."), strings.Split(subject, ".")
	for i, f := range ft {
		if f == ">" {
			return len(st) > i
		}
		if i >= len(st) || (f != "*" && f != st[i]) {
			return false
		}
	}
	return len(ft) == len(st)
}

func matchesAny(filters []string, subject string) bool {
	for _, f := range filters {
		if subjectMatches(f, subject) {
			return true
		}
	}
	return false
}

// handlerTarget names the handler for logs: the program path, or the URL without credentials or query.
func (c *Config) handlerTarget() string {
	if c.Handler.Command != nil {
		return c.Handler.Command.Path
	}
	return config.RedactURL(c.Handler.HTTP.URL)
}
