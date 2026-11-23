// Package contract defines what a published webhook event looks like to its consumers: the contract version,
// header names, message IDs, claim-check keys and subjects. Changing any of it breaks consumers.
package contract

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Version is the contract version every published message carries in HeaderContract.
const Version = "1"

// Headers on every published event. Names are case-sensitive on NATS and are published exactly as written.
const (
	HeaderContract             = "Hookgate-Contract"
	HeaderSource               = "Hookgate-Source"
	HeaderEventID              = "Hookgate-Event-Id"
	HeaderEventIDAuthenticated = "Hookgate-Event-Id-Authenticated"
	HeaderSignedAt             = "Hookgate-Signed-At"
	HeaderBodySHA256           = "Hookgate-Body-Sha256"
	HeaderBodySize             = "Hookgate-Body-Size"
	HeaderReceivedAt           = "Hookgate-Received-At"
	HeaderClaimBucket          = "Hookgate-Claim-Bucket"
	HeaderClaimKey             = "Hookgate-Claim-Key"
)

// Headers added by the relay: on replayed events, dead-letter records and requests to HTTP handlers.
const (
	HeaderReplay           = "Hookgate-Replay"
	HeaderAttempt          = "Hookgate-Attempt"
	HeaderDeadReason       = "Hookgate-Dead-Reason"
	HeaderDeadDetail       = "Hookgate-Dead-Detail"
	HeaderDeadAttempts     = "Hookgate-Dead-Attempts"
	HeaderDeadAt           = "Hookgate-Dead-At"
	HeaderOriginalSubject  = "Hookgate-Original-Subject"
	HeaderOriginalStream   = "Hookgate-Original-Stream"
	HeaderOriginalSeq      = "Hookgate-Original-Seq"
	HeaderOriginalConsumer = "Hookgate-Original-Consumer"
	HeaderOriginalMsgID    = "Hookgate-Original-Msg-Id"
)

// HeaderMsgID is the JetStream deduplication header.
const HeaderMsgID = "Nats-Msg-Id"

// Dead-letter reasons carried in HeaderDeadReason.
const (
	DeadHandlerRejected  = "handler_rejected"
	DeadMaxAttempts      = "max_attempts"
	DeadChecksumMismatch = "checksum_mismatch"
	DeadClaimMissing     = "claim_missing"
	DeadInvalidMessage   = "invalid_message"
)

// MaxDeadDetailBytes bounds HeaderDeadDetail.
const MaxDeadDetailBytes = 256

// RequiredHeaders are carried by every message of this contract version; a message missing one is invalid.
var RequiredHeaders = []string{HeaderContract, HeaderSource, HeaderBodySHA256, HeaderBodySize}

// IsReserved reports whether a header name belongs to hookgate or NATS and so must never come from configuration
// or a sender: Hookgate-*, Nats-*, and the Status and Description headers NATS uses for control messages.
// The comparison ignores case.
func IsReserved(name string) bool {
	n := strings.ToLower(name)
	return strings.HasPrefix(n, "hookgate-") || strings.HasPrefix(n, "nats-") || n == "status" || n == "description"
}

// MayMatchReserved reports whether a header pattern (an exact name, or a prefix ending in *) can match a
// reserved name. The comparison ignores case.
func MayMatchReserved(pattern string) bool {
	prefix, wildcard := strings.CutSuffix(pattern, "*")
	if !wildcard {
		return IsReserved(pattern)
	}
	p := strings.ToLower(prefix)
	for _, r := range []string{"hookgate-", "nats-", "status", "description"} {
		if strings.HasPrefix(r, p) || strings.HasPrefix(p, r) {
			return true
		}
	}
	return false
}

// MaxEventIDBytes bounds an event ID.
const MaxEventIDBytes = 200

// ValidEventID reports whether id is 1-200 bytes of A-Z a-z 0-9 _ ~ = + -. A dot is excluded: Standard Webhooks
// signs "id.timestamp.body", which a dot in the ID would make ambiguous.
func ValidEventID(id string) bool {
	if id == "" || len(id) > MaxEventIDBytes {
		return false
	}
	for i := 0; i < len(id); i++ {
		if !eventIDByte(id[i]) {
			return false
		}
	}
	return true
}

func eventIDByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	}
	return strings.IndexByte("_~=+-", c) >= 0
}

// SourceNamePattern is the rule every source name follows; names never contain ':' or '/', which separate the
// parts of message IDs and claim keys.
const SourceNamePattern = `^[A-Za-z0-9][A-Za-z0-9_-]{0,62}$`

var sourceNameRE = regexp.MustCompile(SourceNamePattern)

// ValidSourceName reports whether name matches SourceNamePattern.
func ValidSourceName(name string) bool { return sourceNameRE.MatchString(name) }

// MessageID is the deduplication ID of an event: "<source>:<event id>". Consumers read the event ID from
// HeaderEventID, never by parsing this value.
func MessageID(source, eventID string) string { return source + ":" + eventID }

// ClaimKey names the stored body of a claim-checked event: "<source>/<event id>/<sha256hex>", or
// "<source>/<sha256hex>" when the event has no ID.
func ClaimKey(source, eventID, sha256hex string) string {
	if eventID == "" {
		return source + "/" + sha256hex
	}
	return source + "/" + eventID + "/" + sha256hex
}

// FormatTime renders a time as RFC 3339 in UTC with millisecond precision, the form of every time header.
func FormatTime(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z07:00") }

// ValidateSubjectTemplate checks a subject template whose only placeholder is "{" + placeholder + "}": every
// dot-separated token is non-empty, without whitespace or wildcards, and braces appear only in the placeholder.
// Values substituted for the placeholder must themselves be valid subject tokens.
func ValidateSubjectTemplate(tmpl, placeholder string) error {
	if tmpl == "" {
		return errors.New("subject must not be empty")
	}
	rest := strings.ReplaceAll(tmpl, "{"+placeholder+"}", "x")
	if strings.ContainsAny(rest, "{}") {
		return fmt.Errorf("subject %q: the only placeholder is {%s}", tmpl, placeholder)
	}
	return ValidateSubject(rest)
}

// ValidateSubject checks a literal NATS subject for publishing: dot-separated non-empty tokens without
// whitespace, control characters or the wildcards * and >.
func ValidateSubject(subject string) error {
	for tok := range strings.SplitSeq(subject, ".") {
		if !validToken(tok) {
			return fmt.Errorf("subject %q: tokens must be non-empty, without whitespace, control characters, * or >", subject)
		}
	}
	return nil
}

func validToken(tok string) bool {
	if tok == "" || tok == "*" || tok == ">" {
		return false
	}
	for _, r := range tok {
		if r <= ' ' || r == 0x7f || r == '*' || r == '>' {
			return false
		}
	}
	return true
}

// RenderSubject substitutes value for "{" + placeholder + "}" in a template accepted by ValidateSubjectTemplate.
func RenderSubject(tmpl, placeholder, value string) string {
	return strings.ReplaceAll(tmpl, "{"+placeholder+"}", value)
}
