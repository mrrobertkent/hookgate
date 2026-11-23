// Package metrics is a minimal Prometheus text-format registry: labelled counters, gauges and histograms with
// fixed buckets.
package metrics

import (
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// DurationBuckets are the histogram upper bounds, in seconds, for request and publish durations.
var DurationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}

// Registry holds metric families and writes them in registration order.
type Registry struct {
	mu       sync.Mutex
	families []family
}

type family interface {
	write(w io.Writer)
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{} }

func (r *Registry) add(f family) {
	r.mu.Lock()
	r.families = append(r.families, f)
	r.mu.Unlock()
}

// Write writes every family in the Prometheus text exposition format, version 0.0.4.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	fams := slices.Clone(r.families)
	r.mu.Unlock()
	for _, f := range fams {
		f.write(w)
	}
}

// ContentType is the Content-Type of Write's output.
const ContentType = "text/plain; version=0.0.4; charset=utf-8"

// desc names a family and its labels.
type desc struct {
	name, help, kind string
	labels           []string
}

func (d desc) header(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", d.name, d.help, d.name, d.kind)
}

// key joins label values; the separator cannot appear in a valid UTF-8 label value.
func (d desc) key(values []string) string {
	if len(values) != len(d.labels) {
		panic(fmt.Sprintf("metric %s: %d label values for %d labels", d.name, len(values), len(d.labels)))
	}
	return strings.Join(values, "\xff")
}

// series renders name{l1="v1",...} with extra appended as a final label pair when set.
func (d desc) series(name string, values []string, extra ...string) string {
	var b strings.Builder
	b.WriteString(name)
	if len(values) == 0 && len(extra) == 0 {
		return b.String()
	}
	b.WriteByte('{')
	for i, l := range d.labels {
		if i > 0 {
			b.WriteByte(',')
		}
		writePair(&b, l, values[i])
	}
	if len(extra) == 2 {
		if len(d.labels) > 0 {
			b.WriteByte(',')
		}
		writePair(&b, extra[0], extra[1])
	}
	b.WriteByte('}')
	return b.String()
}

func writePair(b *strings.Builder, name, value string) {
	b.WriteString(name)
	b.WriteString(`="`)
	b.WriteString(labelEscaper.Replace(value))
	b.WriteByte('"')
}

var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func splitKey(k string, n int) []string {
	if n == 0 {
		return nil
	}
	return strings.Split(k, "\xff")
}

func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// Counter is a family of monotonically increasing counters.
type Counter struct {
	desc
	mu     sync.Mutex
	values map[string]uint64
}

// Counter registers a counter family.
func (r *Registry) Counter(name, help string, labels ...string) *Counter {
	c := &Counter{desc: desc{name, help, "counter", labels}, values: map[string]uint64{}}
	r.add(c)
	return c
}

// Inc adds one to the series with the given label values.
func (c *Counter) Inc(labelValues ...string) { c.Add(1, labelValues...) }

// Add adds n to the series with the given label values.
func (c *Counter) Add(n uint64, labelValues ...string) {
	k := c.key(labelValues)
	c.mu.Lock()
	c.values[k] += n
	c.mu.Unlock()
}

func (c *Counter) write(w io.Writer) {
	c.header(w)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range sortedKeys(c.values) {
		fmt.Fprintf(w, "%s %d\n", c.series(c.name, splitKey(k, len(c.labels))), c.values[k])
	}
}

// Gauge is a family of values that go up and down.
type Gauge struct {
	desc
	mu     sync.Mutex
	values map[string]float64
}

// Gauge registers a gauge family.
func (r *Registry) Gauge(name, help string, labels ...string) *Gauge {
	g := &Gauge{desc: desc{name, help, "gauge", labels}, values: map[string]float64{}}
	r.add(g)
	return g
}

// Set sets the series with the given label values.
func (g *Gauge) Set(v float64, labelValues ...string) {
	k := g.key(labelValues)
	g.mu.Lock()
	g.values[k] = v
	g.mu.Unlock()
}

// SetBool sets the series to 1 when b is true, else 0.
func (g *Gauge) SetBool(b bool, labelValues ...string) {
	v := 0.0
	if b {
		v = 1
	}
	g.Set(v, labelValues...)
}

// Add adds delta to the series with the given label values.
func (g *Gauge) Add(delta float64, labelValues ...string) {
	k := g.key(labelValues)
	g.mu.Lock()
	g.values[k] += delta
	g.mu.Unlock()
}

func (g *Gauge) write(w io.Writer) {
	g.header(w)
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, k := range sortedKeys(g.values) {
		fmt.Fprintf(w, "%s %s\n", g.series(g.name, splitKey(k, len(g.labels))), formatFloat(g.values[k]))
	}
}

// GaugeFunc is an unlabelled gauge read at scrape time.
type GaugeFunc struct {
	desc
	f func() float64
}

// GaugeFunc registers an unlabelled gauge whose value f returns at each scrape.
func (r *Registry) GaugeFunc(name, help string, f func() float64) *GaugeFunc {
	g := &GaugeFunc{desc: desc{name, help, "gauge", nil}, f: f}
	r.add(g)
	return g
}

func (g *GaugeFunc) write(w io.Writer) {
	g.header(w)
	fmt.Fprintf(w, "%s %s\n", g.name, formatFloat(g.f()))
}

// Histogram is a family of histograms sharing fixed bucket bounds.
type Histogram struct {
	desc
	bounds []float64
	mu     sync.Mutex
	values map[string]*histogramSeries
}

type histogramSeries struct {
	counts []uint64 // per bucket, not cumulative
	count  uint64
	sum    float64
}

// Histogram registers a histogram family; bounds are the ascending bucket upper bounds, without +Inf.
func (r *Registry) Histogram(name, help string, bounds []float64, labels ...string) *Histogram {
	if !slices.IsSorted(bounds) {
		panic("metric " + name + ": bucket bounds must ascend")
	}
	h := &Histogram{desc: desc{name, help, "histogram", labels}, bounds: slices.Clone(bounds), values: map[string]*histogramSeries{}}
	r.add(h)
	return h
}

// Observe records v in the series with the given label values.
func (h *Histogram) Observe(v float64, labelValues ...string) {
	k := h.key(labelValues)
	i, _ := slices.BinarySearch(h.bounds, v)
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.values[k]
	if s == nil {
		s = &histogramSeries{counts: make([]uint64, len(h.bounds))}
		h.values[k] = s
	}
	if i < len(h.bounds) {
		s.counts[i]++
	}
	s.count++
	s.sum += v
}

func (h *Histogram) write(w io.Writer) {
	h.header(w)
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, k := range sortedKeys(h.values) {
		s, values := h.values[k], splitKey(k, len(h.labels))
		var cum uint64
		for i, b := range h.bounds {
			cum += s.counts[i]
			fmt.Fprintf(w, "%s %d\n", h.series(h.name+"_bucket", values, "le", formatFloat(b)), cum)
		}
		fmt.Fprintf(w, "%s %d\n", h.series(h.name+"_bucket", values, "le", "+Inf"), s.count)
		fmt.Fprintf(w, "%s %s\n", h.series(h.name+"_sum", values), formatFloat(s.sum))
		fmt.Fprintf(w, "%s %d\n", h.series(h.name+"_count", values), s.count)
	}
}
