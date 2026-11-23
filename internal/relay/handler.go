package relay

import (
	"context"
	"fmt"
)

// verdict is a handler's answer about one event.
type verdict int

const (
	verdictAck       verdict = iota // processed; acknowledge
	verdictRetry                    // transient failure; redeliver after the backoff delay
	verdictReject                   // the event cannot be processed; dead-letter it
	verdictPause                    // the handler cannot run at all (configuration fault); stop fetching
	verdictCancelled                // the relay is shutting down
)

func (v verdict) String() string {
	return [...]string{"ack", "retry", "reject", "pause", "cancelled"}[v]
}

// result is a verdict with a short, body-free explanation.
type result struct {
	verdict verdict
	detail  string
}

func resultf(v verdict, format string, args ...any) result {
	return result{verdict: v, detail: fmt.Sprintf(format, args...)}
}

// deliverer hands one event to the application.
type deliverer interface {
	deliver(ctx context.Context, ev *event) result
}

// tailBuffer keeps the last max bytes written to it.
type tailBuffer struct {
	max int
	buf []byte
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if len(p) >= t.max {
		t.buf = append(t.buf[:0], p[len(p)-t.max:]...)
		return n, nil
	}
	if over := len(t.buf) + len(p) - t.max; over > 0 {
		t.buf = append(t.buf[:0], t.buf[over:]...)
	}
	t.buf = append(t.buf, p...)
	return n, nil
}

func (t *tailBuffer) String() string { return string(t.buf) }
