package metrics

import (
	"strings"
	"testing"
)

func TestTextFormat(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("x_total", "Things.", "source", "result")
	c.Inc("b", "ok")
	c.Add(2, "a", `we"ird\`+"\n")
	g := r.Gauge("x_ready", "Ready.", "source")
	g.SetBool(true, "a")
	g.SetBool(false, "b")
	r.GaugeFunc("x_in_flight", "In flight.", func() float64 { return 3 })
	h := r.Histogram("x_seconds", "Durations.", []float64{0.1, 1}, "source")
	h.Observe(0.05, "a")
	h.Observe(0.1, "a")
	h.Observe(0.5, "a")
	h.Observe(7, "a")

	var b strings.Builder
	r.Write(&b)
	want := `# HELP x_total Things.
# TYPE x_total counter
x_total{source="a",result="we\"ird\\\n"} 2
x_total{source="b",result="ok"} 1
# HELP x_ready Ready.
# TYPE x_ready gauge
x_ready{source="a"} 1
x_ready{source="b"} 0
# HELP x_in_flight In flight.
# TYPE x_in_flight gauge
x_in_flight 3
# HELP x_seconds Durations.
# TYPE x_seconds histogram
x_seconds_bucket{source="a",le="0.1"} 2
x_seconds_bucket{source="a",le="1"} 3
x_seconds_bucket{source="a",le="+Inf"} 4
x_seconds_sum{source="a"} 7.65
x_seconds_count{source="a"} 4
`
	if b.String() != want {
		t.Errorf("got:\n%s\nwant:\n%s", b.String(), want)
	}
}

func TestLabelCountMismatchPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("no panic")
		}
	}()
	NewRegistry().Counter("x_total", "x", "a").Inc()
}
