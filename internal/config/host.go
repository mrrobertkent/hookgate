package config

import (
	"errors"
	"net/netip"
	"strings"
)

// NormalizeHost returns the form of a configured host that requests are matched against: lowercase, without
// one trailing dot, and IP literals in canonical form (IPv6 without brackets, RFC 5952). It rejects an empty
// value, a wildcard, a port, an IPv6 zone and anything that is not a DNS name or an IP literal.
func NormalizeHost(h string) (string, error) {
	if h == "" {
		return "", errors.New("host must not be empty")
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

// isDNSName reports whether lowercase h is a host name under RFC 1123: dot-separated labels of 1-63 letters,
// digits and hyphens, not starting or ending with a hyphen, 253 bytes at most.
func isDNSName(h string) bool {
	if h == "" || len(h) > 253 {
		return false
	}
	for label := range strings.SplitSeq(h, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
				return false
			}
		}
	}
	return true
}
