package relay

import (
	"strings"
	"testing"
	"time"
)

const minimalConfig = `
connection: {nats: {urls: ["nats://127.0.0.1:4222"], none: true}}
stream: WEBHOOK_EVENTS
consumer: rex
filter_subjects: [webhooks.events.resend, webhooks.replay.rex]
dead_letter: {stream: WEBHOOK_DEAD}
handler:
  command: {path: /usr/local/bin/handler}
`

func TestConfigDefaults(t *testing.T) {
	c, err := ParseConfig([]byte(minimalConfig))
	if err != nil {
		t.Fatal(err)
	}
	if c.AdminListen != ":9091" || c.Concurrency != 1 || c.MaxAttempts != 10 || c.DrainTimeout != 25*time.Second ||
		c.ShutdownNakDelay != 5*time.Second || c.ReplaySubject != "webhooks.replay.{consumer}" ||
		c.DeadLetter.Subject != "webhooks.dead.{consumer}" || c.Handler.Command.Timeout != time.Minute {
		t.Errorf("defaults: %+v", c)
	}
	if len(c.Backoff) != 5 || c.Backoff[0] != 10*time.Second || c.Backoff[4] != time.Hour {
		t.Errorf("backoff %v", c.Backoff)
	}
	if c.Connection.NATS.InboxPrefix != "_INBOX.relay-rex" {
		t.Errorf("inbox prefix %q", c.Connection.NATS.InboxPrefix)
	}
	if c.deadSubject() != "webhooks.dead.rex" || c.replaySubject() != "webhooks.replay.rex" {
		t.Errorf("subjects %s %s", c.deadSubject(), c.replaySubject())
	}
}

func TestConfigHTTPDefaults(t *testing.T) {
	raw := strings.Replace(minimalConfig, "command: {path: /usr/local/bin/handler}", "http: {url: http://handler:8080/events}", 1)
	c, err := ParseConfig([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if c.Handler.HTTP.Timeout != 30*time.Second {
		t.Errorf("timeout %s", c.Handler.HTTP.Timeout)
	}
}

func TestConfigErrors(t *testing.T) {
	cases := []struct {
		name, from, to, want string
	}{
		{"unknown key", "stream: WEBHOOK_EVENTS", "stream: WEBHOOK_EVENTS\nnope: 1", "field nope not found"},
		{"no stream", "stream: WEBHOOK_EVENTS", "", "stream \"\""},
		{"dotted consumer", "consumer: rex", "consumer: re.x", "consumer \"re.x\""},
		{"colon in stream", "stream: WEBHOOK_EVENTS", "stream: \"A:B\"", "stream \"A:B\""},
		{"colon in consumer", "consumer: rex", "consumer: \"b:rex\"", "consumer \"b:rex\""},
		{"static replay subject", "stream: WEBHOOK_EVENTS", "stream: WEBHOOK_EVENTS\nreplay_subject: webhooks.replay.rex", "must be <prefix>.replay.{consumer}"},
		{"custom prefix, default wildcard", "stream: WEBHOOK_EVENTS", "stream: WEBHOOK_EVENTS\nreplay_subject: \"custom.replay.{consumer}\"", "must list custom.replay.rex"},
		{"custom prefix catching others", "webhooks.replay.rex]", "custom.replay.rex, webhooks.replay.*]\nreplay_subject: \"custom.replay.{consumer}\"", "could match a replay subject"},
		{"wildcard event catch-all", "webhooks.events.resend,", "webhooks.events.>,", "could match a replay subject"},
		{"custom prefix, other replay literally", "webhooks.replay.rex]", "custom.replay.rex, webhooks.replay.b]\nreplay_subject: \"custom.replay.{consumer}\"", "could match a replay subject"},
		{"wildcard before the last token", "webhooks.events.resend,", "\"webhooks.*.resend\",", "could match a replay subject"},
		{"replay token in prefix", "stream: WEBHOOK_EVENTS", "stream: WEBHOOK_EVENTS\nreplay_subject: \"replay.x.replay.{consumer}\"", "must be <prefix>.replay.{consumer}"},
		{"replay wildcard", "webhooks.replay.rex]", "webhooks.replay.*]", "could match a replay subject"},
		{"other consumer's replay", "webhooks.replay.rex]", "webhooks.replay.rex, webhooks.replay.b]", "could match a replay subject"},
		{"other replay by token", "webhooks.replay.rex]", "webhooks.replay.rex, \"*.replay.b\"]", "could match a replay subject"},
		{"replay placeholder inside a token", "stream: WEBHOOK_EVENTS", "stream: WEBHOOK_EVENTS\nreplay_subject: \"webhooks.replay.to-{consumer}.event\"", "must be <prefix>.replay.{consumer}"},
		{"replay placeholder not last", "stream: WEBHOOK_EVENTS", "stream: WEBHOOK_EVENTS\nreplay_subject: \"webhooks.{consumer}.replay\"", "must be <prefix>.replay.{consumer}"},
		{"replay catch-all", "webhooks.replay.rex]", "webhooks.replay.rex, webhooks.>]", "could match a replay subject"},
		{"http header twice", "command: {path: /usr/local/bin/handler}", "http: {url: http://x, set_headers: {Authorization: {env: A}, authorization: {env: B}}}", "are the same header"},
		{"same streams", "dead_letter: {stream: WEBHOOK_DEAD}", "dead_letter: {stream: WEBHOOK_EVENTS}", "must differ"},
		{"no filters", "filter_subjects: [webhooks.events.resend, webhooks.replay.rex]", "", "filter_subjects is required"},
		{"bad filter", "webhooks.events.resend,", "webhooks.>.resend,", "filter_subjects"},
		{"replay not filtered", "webhooks.replay.rex]", "webhooks.replay.other]", "must list webhooks.replay.rex"},
		{"dead letters consumed", "webhooks.replay.rex]", "webhooks.replay.rex, webhooks.dead.>]", "dead_letter.subject"},
		{"bad replay template", "stream: WEBHOOK_EVENTS", "stream: WEBHOOK_EVENTS\nreplay_subject: \"replay.{source}\"", "replay_subject"},
		{"reserved pass header", "stream: WEBHOOK_EVENTS", "stream: WEBHOOK_EVENTS\npass_headers: [Nats-*]", "reserved"},
		{"reserved pass prefix", "stream: WEBHOOK_EVENTS", "stream: WEBHOOK_EVENTS\npass_headers: [hook*]", "reserved"},
		{"pass everything", "stream: WEBHOOK_EVENTS", "stream: WEBHOOK_EVENTS\npass_headers: [\"*\"]", "pass_headers"},
		{"concurrency", "stream: WEBHOOK_EVENTS", "stream: WEBHOOK_EVENTS\nconcurrency: -1", "concurrency"},
		{"max attempts", "stream: WEBHOOK_EVENTS", "stream: WEBHOOK_EVENTS\nmax_attempts: -1", "max_attempts"},
		{"backoff", "stream: WEBHOOK_EVENTS", "stream: WEBHOOK_EVENTS\nbackoff: [1s, -1s]", "backoff[1]"},
		{"bucket", "stream: WEBHOOK_EVENTS", "stream: WEBHOOK_EVENTS\nclaim_check: {object_store: {bucket: a.b}}", "bucket"},
		{"no handler", "handler:\n  command: {path: /usr/local/bin/handler}", "", "exactly one of command, http"},
		{"two handlers", "command: {path: /usr/local/bin/handler}", "command: {path: /usr/local/bin/handler}\n  http: {url: http://x}", "exactly one of command, http"},
		{"relative path", "/usr/local/bin/handler", "handler", "clean absolute path"},
		{"reserved env", "{path: /usr/local/bin/handler}", "{path: /usr/local/bin/handler, env_allow: [HOOKGATE_X]}", "env_allow"},
		{"set_env ref", "{path: /usr/local/bin/handler}", "{path: /usr/local/bin/handler, set_env: {TOKEN: {}}}", "exactly one of env, file"},
		{"env twice", "{path: /usr/local/bin/handler}", "{path: /usr/local/bin/handler, env_allow: [A], set_env: {A: {env: B}}}", "also in env_allow"},
		{"http url", "command: {path: /usr/local/bin/handler}", "http: {url: ftp://x}", "absolute http(s) URL"},
		{"http credentials", "command: {path: /usr/local/bin/handler}", "http: {url: \"http://u:p@x\"}", "credentials"},
		{"http reserved header", "command: {path: /usr/local/bin/handler}", "http: {url: http://x, set_headers: {Hookgate-Attempt: {env: X}}}", "reserved"},
		{"nats", "none: true", "none: false", "exactly one of nkey_seed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := strings.Replace(minimalConfig, tc.from, tc.to, 1)
			if raw == minimalConfig {
				t.Fatalf("replacement %q not applied", tc.from)
			}
			_, err := ParseConfig([]byte(raw))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %v, want %q", err, tc.want)
			}
		})
	}
}

func TestSubjectMatches(t *testing.T) {
	cases := []struct {
		filter, subject string
		want            bool
	}{
		{"a.b", "a.b", true},
		{"a.b", "a.c", false},
		{"a.*", "a.b", true},
		{"a.*", "a.b.c", false},
		{"a.>", "a.b.c", true},
		{"a.>", "a", false},
		{">", "a", true},
		{"a.*.c", "a.b.c", true},
		{"a.b.c", "a.b", false},
	}
	for _, tc := range cases {
		if got := subjectMatches(tc.filter, tc.subject); got != tc.want {
			t.Errorf("subjectMatches(%q, %q) = %v", tc.filter, tc.subject, got)
		}
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("a\nb", 10); got != "a b" {
		t.Errorf("%q", got)
	}
	if got := truncate("ééé", 3); got != "é" {
		t.Errorf("%q", got)
	}
	if got := truncate(strings.Repeat("x", 300), 256); len(got) != 256 {
		t.Errorf("len %d", len(got))
	}
}

func TestTailBuffer(t *testing.T) {
	b := &tailBuffer{max: 8}
	_, _ = b.Write([]byte("hello "))
	_, _ = b.Write([]byte("world\nlast"))
	if b.String() != "rld\nlast" {
		t.Errorf("%q", b.String())
	}
	_, _ = b.Write([]byte("0123456789"))
	if b.String() != "23456789" {
		t.Errorf("%q", b.String())
	}
}

// TestDeadMessageIDsUnambiguous shows that the name rule makes "dead:<stream>:<consumer>:<seq>" unique: names
// that would collide ("a:b"/"c" and "a"/"b:c") are rejected.
func TestDeadMessageIDsUnambiguous(t *testing.T) {
	for _, n := range []string{"a:b", "b:c", "a.b", "a b", "a*", ">", ""} {
		if validName(n) {
			t.Errorf("%q accepted", n)
		}
	}
	for _, n := range []string{"WEBHOOK_EVENTS", "rex", "relay-1"} {
		if !validName(n) {
			t.Errorf("%q rejected", n)
		}
	}
}

func TestMayMatchReplay(t *testing.T) {
	cases := map[string]bool{
		"webhooks.events.resend": false, "*.events.resend": false, "events": false, "webhooks.events.*": false,
		"*.events.*": false, "a.b.replay": false, "webhooks.replay.x.y": false, "replay": false, "*": false,
		"a.*": false, "replay.a": true, "*.a": true,
		"webhooks.replay.b": true, "webhooks.>": true, ">": true, "*.replay.x": true, "a.>": true, "a.b.c.>": true,
		"custom.replay.rex": true, "webhooks.*.*": true, "*.*.*": true, "webhooks.*.resend": true,
	}
	for f, want := range cases {
		if got := mayMatchReplay(f); got != want {
			t.Errorf("mayMatchReplay(%q) = %v", f, got)
		}
	}
}

// TestWildcardEventFilterAccepted: a wildcard over event subjects cannot match a replay subject and is accepted.
func TestWildcardEventFilterAccepted(t *testing.T) {
	for _, f := range []string{"webhooks.events.*", "*.events.*", "webhooks.replay.x.y"} {
		raw := strings.Replace(minimalConfig, "webhooks.events.resend,", "\""+f+"\",", 1)
		if _, err := ParseConfig([]byte(raw)); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
}
