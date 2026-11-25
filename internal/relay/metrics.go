package relay

import "github.com/mrrobertkent/hookgate/internal/metrics"

// Delivery outcomes, the values of the outcome label.
const (
	outcomeAcked            = "acked"
	outcomeRetried          = "retried"
	outcomeDeadLettered     = "dead_lettered"
	outcomeDeadLetterFailed = "dead_letter_failed"
	outcomePaused           = "paused"
	outcomeShutdown         = "shutdown"
)

type relayMetrics struct {
	reg                *metrics.Registry
	info               *metrics.Gauge
	deliveries         *metrics.Counter
	handlerDuration    *metrics.Histogram
	deadLetters        *metrics.Counter
	deadLetterFailed   *metrics.Counter
	deadLetterTooLarge *metrics.Counter
	settleFailed       *metrics.Counter
	maxDeliveries      *metrics.Counter
	pending            *metrics.Gauge
	ackPending         *metrics.Gauge
	redelivered        *metrics.Gauge
	connected          *metrics.Gauge
	rejected           *metrics.Gauge
	ready              *metrics.Gauge
	paused             *metrics.Gauge
	inFlight           *metrics.Gauge
}

// newMetrics builds the relay's registry; stream and consumer are the validated configuration's names.
func newMetrics(stream, consumer string) *relayMetrics {
	r := metrics.NewRegistry()
	m := &relayMetrics{
		reg:                r,
		info:               r.Gauge("hookgate_relay_info", "Always 1; labels name the stream and durable consumer this relay delivers from.", "stream", "consumer"),
		deliveries:         r.Counter("hookgate_relay_deliveries_total", "Deliveries settled, by outcome.", "outcome"),
		handlerDuration:    r.Histogram("hookgate_relay_handler_duration_seconds", "Handler run time, by result.", metrics.DurationBuckets, "result"),
		deadLetters:        r.Counter("hookgate_relay_dead_letters_total", "Events written to the dead-letter stream, by reason.", "reason"),
		deadLetterFailed:   r.Counter("hookgate_relay_dead_letter_publish_failures_total", "Dead-letter publishes that failed; the event is retried."),
		deadLetterTooLarge: r.Counter("hookgate_relay_dead_letter_too_large_total", "Dead-letter publishes refused for size (server max_payload or dead-letter stream max_msg_size); also counted as publish failures."),
		settleFailed:       r.Counter("hookgate_relay_settle_failures_total", "Acks, naks or terms that could not be sent or, for acks, were not confirmed; the event is redelivered after the consumer's ack wait."),
		maxDeliveries:      r.Counter("hookgate_relay_max_deliveries_advisories_total", "Server advisories that a message of this consumer reached its max_deliver."),
		pending:            r.Gauge("hookgate_relay_consumer_pending", "Messages not yet delivered to the consumer."),
		ackPending:         r.Gauge("hookgate_relay_consumer_ack_pending", "Messages delivered and not yet acknowledged."),
		redelivered:        r.Gauge("hookgate_relay_consumer_redelivered", "Messages redelivered and not yet acknowledged."),
		connected:          r.Gauge("hookgate_relay_connected", "1 while connected to NATS."),
		rejected:           r.Gauge("hookgate_relay_nats_credentials_rejected", "1 while the NATS credentials or certificates are rejected or cannot be loaded, else 0."),
		ready:              r.Gauge("hookgate_relay_ready", "1 while the relay is ready and fetching."),
		paused:             r.Gauge("hookgate_relay_paused", "1 after a handler configuration fault; fetching stops until restart."),
		inFlight:           r.Gauge("hookgate_relay_in_flight", "Events being processed."),
	}
	m.info.Set(1, stream, consumer)
	return m
}
