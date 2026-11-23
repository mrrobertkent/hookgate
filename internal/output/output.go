// Package output delivers verified webhook requests to their destination: an HTTP upstream or a NATS JetStream
// stream. Verification and routing never depend on the kind of output.
package output

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/mrrobertkent/hookgate/internal/config"
	"github.com/mrrobertkent/hookgate/internal/metrics"
	"github.com/mrrobertkent/hookgate/internal/natsconn"
)

// Event is one verified request.
type Event struct {
	Source     string
	Header     http.Header
	Body       []byte
	EventID    string    // set only when a check authenticated it
	SignedAt   time.Time // signing time a check authenticated; zero when none
	ReceivedAt time.Time
}

// Result is the answer for the sender, plus what to log and count.
type Result struct {
	Status  int
	Outcome string // label of hookgate_requests_total
	Header  http.Header
	Body    []byte
	Reason  string // logged, never sent
}

// Output delivers events.
type Output interface {
	// Deliver sends ev and returns the response for the sender.
	Deliver(ctx context.Context, ev Event) Result
	// Ready returns nil when Deliver can succeed now, ErrNotConnected while the output's connection is down,
	// and another error while the output is misconfigured against its destination.
	Ready() error
}

// Watcher is an Output that checks its destination in the background: on Run, on every Reconnected call, and
// periodically while not ready.
type Watcher interface {
	Output
	Run(ctx context.Context)
	Reconnected()
}

// ErrNotConnected reports that the output's broker connection is down.
var ErrNotConnected = errors.New("not connected to the broker")

// Metrics are the counters and histograms outputs update; label "source" comes first in each.
type Metrics struct {
	UpstreamAttempts *metrics.Counter   // source, result
	PublishAttempts  *metrics.Counter   // source, result
	PublishDuration  *metrics.Histogram // source
	ClaimPuts        *metrics.Counter   // source, result
}

// Deps is what Build needs besides the source's configuration.
type Deps struct {
	Conns   map[string]*natsconn.Conn
	Metrics *Metrics
	Log     *slog.Logger
	// ReportReady is called with each readiness check's result (nil when ready); it must not block.
	ReportReady func(err error)
}

// Build creates the output of src. It fails when a secret the output needs is not set.
func Build(src config.Source, d Deps) (Output, error) {
	switch {
	case src.Output.HTTP != nil:
		return newHTTP(src, d)
	case src.Output.JetStream != nil:
		return newJetStream(src, d)
	}
	return nil, errors.New("no output configured")
}

const contentType = "Content-Type"

// headerFilter selects request headers to pass on: names matching a pattern (exact, or a prefix ending in *),
// compared without regard to case, minus the headers checks read credentials from.
type headerFilter struct {
	patterns []string // lowercase
	exclude  map[string]bool
}

func newHeaderFilter(patterns, exclude []string) headerFilter {
	f := headerFilter{exclude: map[string]bool{}}
	for _, p := range patterns {
		f.patterns = append(f.patterns, strings.ToLower(p))
	}
	for _, e := range exclude {
		f.exclude[strings.ToLower(e)] = true
	}
	return f
}

func (f headerFilter) allows(name string) bool {
	n := strings.ToLower(name)
	if f.exclude[n] {
		return false
	}
	for _, p := range f.patterns {
		if prefix, ok := strings.CutSuffix(p, "*"); ok {
			if strings.HasPrefix(n, prefix) {
				return true
			}
		} else if n == p {
			return true
		}
	}
	return false
}

// resolveSetHeaders resolves set_headers values; each must be set, non-empty and a single line.
func resolveSetHeaders(refs map[string]config.SecretRef) (map[string]string, error) {
	out := make(map[string]string, len(refs))
	for name, ref := range refs {
		v, err := ref.Resolve()
		if err != nil {
			return nil, fmt.Errorf("set_headers %s: %w", name, err)
		}
		if v == "" {
			return nil, fmt.Errorf("set_headers %s: %s is empty", name, ref)
		}
		if strings.ContainsAny(v, "\r\n\x00") {
			return nil, fmt.Errorf("set_headers %s: %s contains a line break or NUL", name, ref)
		}
		out[name] = v
	}
	return out, nil
}
