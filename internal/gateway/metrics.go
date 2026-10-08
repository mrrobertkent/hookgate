package gateway

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
)

// metrics is a minimal Prometheus text-format registry; hookgate exposes only counters and gauges.
type metrics struct {
	mu       sync.Mutex
	requests map[[2]string]uint64 // {source, outcome}
	attempts map[[2]string]uint64 // {source, result}
	ready    map[string]bool
	inFlight atomic.Int64
	draining atomic.Bool
}

func newMetrics() *metrics {
	return &metrics{
		requests: map[[2]string]uint64{},
		attempts: map[[2]string]uint64{},
		ready:    map[string]bool{},
	}
}

func (m *metrics) request(source, outcome string) {
	m.mu.Lock()
	m.requests[[2]string{source, outcome}]++
	m.mu.Unlock()
}

func (m *metrics) attempt(source, result string) {
	m.mu.Lock()
	m.attempts[[2]string{source, result}]++
	m.mu.Unlock()
}

func (m *metrics) setReady(source string, ok bool) {
	m.mu.Lock()
	m.ready[source] = ok
	m.mu.Unlock()
}

func (m *metrics) write(w io.Writer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	writePairs(w, "hookgate_requests_total", "Webhook requests by source and outcome.", "outcome", m.requests)
	writePairs(w, "hookgate_upstream_attempts_total", "Upstream delivery attempts by source and result.", "result", m.attempts)

	fmt.Fprintln(w, "# HELP hookgate_source_ready 1 when every secret of the source is set, else 0 (the source answers 503).")
	fmt.Fprintln(w, "# TYPE hookgate_source_ready gauge")
	names := make([]string, 0, len(m.ready))
	for n := range m.ready {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		v := 0
		if m.ready[n] {
			v = 1
		}
		fmt.Fprintf(w, "hookgate_source_ready{source=%q} %d\n", n, v)
	}

	fmt.Fprintln(w, "# HELP hookgate_in_flight_requests Requests being processed.")
	fmt.Fprintln(w, "# TYPE hookgate_in_flight_requests gauge")
	fmt.Fprintf(w, "hookgate_in_flight_requests %d\n", m.inFlight.Load())

	d := 0
	if m.draining.Load() {
		d = 1
	}
	fmt.Fprintln(w, "# HELP hookgate_draining 1 after a shutdown signal, while in-flight requests finish.")
	fmt.Fprintln(w, "# TYPE hookgate_draining gauge")
	fmt.Fprintf(w, "hookgate_draining %d\n", d)
}

func writePairs(w io.Writer, name, help, label string, series map[[2]string]uint64) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
	keys := make([][2]string, 0, len(series))
	for k := range series {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i][0] != keys[j][0] {
			return keys[i][0] < keys[j][0]
		}
		return keys[i][1] < keys[j][1]
	})
	for _, k := range keys {
		fmt.Fprintf(w, "%s{source=%q,%s=%q} %d\n", name, k[0], label, k[1], series[k])
	}
}
