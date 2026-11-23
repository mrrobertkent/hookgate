package config

import (
	"errors"
	"fmt"
	"net/netip"
	"strings"

	"go.yaml.in/yaml/v3"
)

// NormalizeHost returns the form of a configured host that requests are matched against: lowercase, without
// one trailing dot, and IP literals in canonical form (IPv6 without brackets, RFC 5952). It rejects an empty
// value, a wildcard, a port, an IPv6 zone and anything that is not a DNS name or an IP literal.
func NormalizeHost(h string) (string, error) {
	if h == "" {
		return "", errors.New("host must not be empty")
	}
	if !isASCII(h) {
		return "", errors.New("host must be ASCII; write internationalised names in punycode")
	}
	if strings.Contains(h, "*") {
		return "", errors.New("host must be exact; wildcards are not supported")
	}
	h = strings.ToLower(h)
	if inner, ok := strings.CutPrefix(h, "["); ok {
		inner, ok = strings.CutSuffix(inner, "]")
		if a, err := netip.ParseAddr(inner); ok && err == nil && a.Is6() && a.Zone() == "" {
			return a.String(), nil
		}
		return "", errors.New("host in brackets must be an IPv6 address, without a port or zone")
	}
	if a, err := netip.ParseAddr(h); err == nil {
		if a.Zone() != "" {
			return "", errors.New("host must not carry an IPv6 zone")
		}
		return a.String(), nil
	}
	if strings.Contains(h, ":") {
		return "", errors.New("host must not include a port")
	}
	h = strings.TrimSuffix(h, ".")
	if !isDNSName(h) {
		return "", errors.New("host must be a DNS name (letters, digits and hyphens; IDNs in punycode) or an IP address")
	}
	return h, nil
}

// isASCII must gate strings.ToLower, which maps some non-ASCII letters to ASCII ones (U+0130 to "i").
func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}

// isDNSName reports whether lowercase h is a host name under RFC 1123: dot-separated labels of 1-63 letters,
// digits and hyphens, not starting or ending with a hyphen, 253 bytes at most.
func isDNSName(h string) bool {
	if h == "" || len(h) > 253 {
		return false
	}
	for label := range strings.SplitSeq(h, ".") {
		if !isDNSLabel(label) {
			return false
		}
	}
	return true
}

func isDNSLabel(l string) bool {
	if l == "" || len(l) > 63 || l[0] == '-' || l[len(l)-1] == '-' {
		return false
	}
	return strings.Trim(l, "abcdefghijklmnopqrstuvwxyz0123456789-") == ""
}

// HostList is a source's hosts. Decoding rejects null and non-scalar entries and an explicitly empty or null
// list: the YAML decoder would otherwise drop a null entry, and a list left with no hosts matches any host.
type HostList []string

// UnmarshalYAML implements yaml.Unmarshaler.
func (l *HostList) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind != yaml.SequenceNode || len(n.Content) == 0 {
		return fmt.Errorf("line %d: hosts must be a non-empty list; omit it to match any host", n.Line)
	}
	out := make(HostList, 0, len(n.Content))
	for _, e := range n.Content {
		if e.Kind != yaml.ScalarNode || e.ShortTag() == "!!null" {
			return fmt.Errorf("line %d: each host must be a string", e.Line)
		}
		out = append(out, e.Value)
	}
	*l = out
	return nil
}

// rejectNullHosts fails on a source whose hosts key is present but null (`hosts:` or `hosts: ~`), which the
// decoder leaves indistinguishable from an absent key. raw has already decoded successfully.
func rejectNullHosts(raw []byte) error {
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return err
	}
	if len(doc.Content) == 0 {
		return nil
	}
	sources := mappingValue(doc.Content[0], "sources")
	if sources == nil || sources.Kind != yaml.SequenceNode {
		return nil
	}
	for _, src := range sources.Content {
		if h := mappingValue(src, "hosts"); h != nil && h.ShortTag() == "!!null" {
			return fmt.Errorf("line %d: hosts must be a non-empty list; omit it to match any host", h.Line)
		}
	}
	return nil
}

func mappingValue(m *yaml.Node, key string) *yaml.Node {
	if m.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}
