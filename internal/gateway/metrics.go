package gateway

import (
	"sync/atomic"

	"github.com/mrrobertkent/hookgate/internal/metrics"
	"github.com/mrrobertkent/hookgate/internal/output"
)

type gatewayMetrics struct {
	reg       *metrics.Registry
	requests  *metrics.Counter
	attempts  *metrics.Counter
	ready     *metrics.Gauge
	connected *metrics.Gauge
	outputs   *output.Metrics
	inFlight  atomic.Int64
	draining  atomic.Bool
}

func newMetrics() *gatewayMetrics {
	m := &gatewayMetrics{reg: metrics.NewRegistry()}
	m.requests = m.reg.Counter("hookgate_requests_total", "Webhook requests by source and outcome.", "source", "outcome")
	m.attempts = m.reg.Counter("hookgate_upstream_attempts_total", "Upstream delivery attempts by source and result.", "source", "result")
	m.ready = m.reg.Gauge("hookgate_source_ready", "1 when the source's secrets are set and its output's destination checks pass, else 0 (the source answers 503).", "source")
	m.outputs = &output.Metrics{
		UpstreamAttempts: m.attempts,
		PublishAttempts:  m.reg.Counter("hookgate_publish_attempts_total", "JetStream publish attempts by source and result.", "source", "result"),
		PublishDuration:  m.reg.Histogram("hookgate_publish_duration_seconds", "JetStream publish attempt duration, until the stream's acknowledgement or failure.", metrics.DurationBuckets, "source"),
		ClaimPuts:        m.reg.Counter("hookgate_claim_check_puts_total", "Claim-check body stores by source and result.", "source", "result"),
	}
	m.connected = m.reg.Gauge("hookgate_nats_connected", "1 while the named NATS connection is connected, else 0.", "connection")
	m.reg.GaugeFunc("hookgate_in_flight_requests", "Requests being processed.", func() float64 { return float64(m.inFlight.Load()) })
	m.reg.GaugeFunc("hookgate_draining", "1 after a shutdown signal, while in-flight requests finish.", func() float64 {
		if m.draining.Load() {
			return 1
		}
		return 0
	})
	return m
}

func (m *gatewayMetrics) request(source, outcome string) { m.requests.Inc(source, outcome) }

func (m *gatewayMetrics) setReady(source string, ok bool) { m.ready.SetBool(ok, source) }
