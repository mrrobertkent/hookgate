package contract

import (
	"strings"
	"testing"
	"time"
)

func TestIsReserved(t *testing.T) {
	for _, n := range []string{"Hookgate-Contract", "hookgate-anything", "NATS-MSG-ID", "Nats-Expected-Stream", "status", "Description"} {
		if !IsReserved(n) {
			t.Errorf("IsReserved(%q) = false", n)
		}
	}
	for _, n := range []string{"Content-Type", "webhook-id", "Hookgate", "Natsx", "Statuses", "X-Status"} {
		if IsReserved(n) {
			t.Errorf("IsReserved(%q) = true", n)
		}
	}
}

func TestMayMatchReserved(t *testing.T) {
	for _, p := range []string{"*", "H*", "hook*", "Hookgate-*", "nats-msg*", "Nats-Msg-Id", "S*", "status", "desc*"} {
		if !MayMatchReserved(p) {
			t.Errorf("MayMatchReserved(%q) = false", p)
		}
	}
	for _, p := range []string{"X-*", "Webhook-*", "Content-Type", "Svix-*", "hookgates*"} {
		if MayMatchReserved(p) {
			t.Errorf("MayMatchReserved(%q) = true", p)
		}
	}
}

func TestValidEventID(t *testing.T) {
	for _, id := range []string{"a", "msg_2Kx~=+-9", strings.Repeat("z", MaxEventIDBytes)} {
		if !ValidEventID(id) {
			t.Errorf("ValidEventID(%q) = false", id)
		}
	}
	for _, id := range []string{"", strings.Repeat("z", MaxEventIDBytes+1), "a b", "a.b", "a:b", "a/b", "a*", "a>", "ä", "a\n"} {
		if ValidEventID(id) {
			t.Errorf("ValidEventID(%q) = true", id)
		}
	}
}

func TestSourceNames(t *testing.T) {
	for _, n := range []string{"a", "Resend", "0", "a_b-c", strings.Repeat("a", 63)} {
		if !ValidSourceName(n) {
			t.Errorf("ValidSourceName(%q) = false", n)
		}
	}
	for _, n := range []string{"", "-a", "_a", "a:b", "a/b", "a.b", "a b", strings.Repeat("a", 64)} {
		if ValidSourceName(n) {
			t.Errorf("ValidSourceName(%q) = true", n)
		}
	}
}

func TestIDsAndKeys(t *testing.T) {
	if got := MessageID("resend", "msg_1"); got != "resend:msg_1" {
		t.Errorf("MessageID = %q", got)
	}
	if got := ClaimKey("resend", "msg_1", "ab"); got != "resend/msg_1/ab" {
		t.Errorf("ClaimKey = %q", got)
	}
	if got := ClaimKey("resend", "", "ab"); got != "resend/ab" {
		t.Errorf("ClaimKey without ID = %q", got)
	}
	at := time.Date(2026, 1, 2, 3, 4, 5, 6_000_000, time.FixedZone("x", 3600))
	if got := FormatTime(at); got != "2026-01-02T02:04:05.006Z" {
		t.Errorf("FormatTime = %q", got)
	}
}

func TestSubjectTemplates(t *testing.T) {
	for _, s := range []string{"webhooks.events.{source}", "events", "a.{source}.b", "{source}"} {
		if err := ValidateSubjectTemplate(s, "source"); err != nil {
			t.Errorf("%q: %v", s, err)
		}
	}
	for _, s := range []string{"", "a..b", ".a", "a.", "a.*", "a.>", "a b", "a.{consumer}", "a.{source", "a}", "a.\tb"} {
		if err := ValidateSubjectTemplate(s, "source"); err == nil {
			t.Errorf("%q accepted", s)
		}
	}
	if got := RenderSubject("webhooks.events.{source}", "source", "resend"); got != "webhooks.events.resend" {
		t.Errorf("RenderSubject = %q", got)
	}
}

// A template that validates renders, with any valid source name, to a subject that validates too.
func FuzzSubjectTemplate(f *testing.F) {
	for _, s := range []string{"webhooks.events.{source}", "a.{source}.{source}", "{source}", "a..b", "a.{x}"} {
		f.Add(s, "resend")
	}
	f.Fuzz(func(t *testing.T, tmpl, name string) {
		if ValidateSubjectTemplate(tmpl, "source") != nil || !ValidSourceName(name) {
			return
		}
		subj := RenderSubject(tmpl, "source", name)
		if err := ValidateSubject(subj); err != nil {
			t.Fatalf("template %q with %q rendered %q: %v", tmpl, name, subj, err)
		}
		if strings.ContainsAny(subj, "{}") {
			t.Fatalf("rendered %q still has braces", subj)
		}
	})
}

// ValidEventID agrees with an independent statement of the rule.
func FuzzEventID(f *testing.F) {
	for _, s := range []string{"msg_1", "", "a b", "a:b", strings.Repeat("x", 201)} {
		f.Add(s)
	}
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_~=+-"
	f.Fuzz(func(t *testing.T, id string) {
		want := len(id) >= 1 && len(id) <= MaxEventIDBytes && strings.Trim(id, charset) == ""
		if got := ValidEventID(id); got != want {
			t.Fatalf("ValidEventID(%q) = %v, want %v", id, got, want)
		}
	})
}
