package relay

import (
	"context"
	"errors"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mrrobertkent/hookgate/internal/contract"
)

// action is how an owner settles its message.
type action int

const (
	actAck        action = iota // DoubleAck
	actRetry                    // NakWithDelay(backoff)
	actDeadLetter               // publish a dead letter, then Term
	actPause                    // NakWithDelay(backoff); the relay pauses
	actShutdown                 // NakWithDelay(shutdown_nak_delay)
)

// plan is an owner's decision about its message.
type plan struct {
	action   action
	reason   string // dead-letter reason
	detail   string
	after    time.Duration // the handler's requested retry delay
	body     []byte        // the resolved body, when resolved
	resolved bool
}

// minKeepAlive bounds how often in-progress acknowledgements are sent.
const minKeepAlive = 100 * time.Millisecond

// pausedPlan returns an event untouched to the stream while the relay is paused: a paused relay neither runs
// handlers nor dead-letters.
var pausedPlan = plan{action: actPause, detail: "relay paused"}

// own processes one message from receipt to settlement. Only its owner ever settles a message.
func (r *Relay) own(work context.Context, msg jetstream.Msg) {
	meta, err := msg.Metadata()
	if err != nil {
		// Not a JetStream delivery; nothing to settle against.
		r.log.Error("message without delivery metadata", "subject", msg.Subject(), "error", err)
		return
	}
	m := &message{msg: msg, meta: meta, msgID: msg.Headers().Get(contract.HeaderMsgID)}
	stop := r.keepAlive(msg)
	p := r.decide(work, m)
	outcome := r.execute(work, m, &p)
	stop()
	r.settle(m, p, outcome)
}

// keepAlive tells the server the message is still being worked on every third of the ack wait, until stopped.
func (r *Relay) keepAlive(msg jetstream.Msg) (stop func()) {
	r.mu.Lock()
	every := max(r.ackWait/3, minKeepAlive)
	r.mu.Unlock()
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				_ = msg.InProgress()
			}
		}
	})
	return func() { close(done); wg.Wait() }
}

// decide checks the contract, resolves the body, runs the handler and turns the outcome into a plan.
func (r *Relay) decide(work context.Context, m *message) plan {
	if r.paused() {
		return pausedPlan
	}
	h := m.msg.Headers()
	size, sum, f := checkContract(h)
	if f == nil {
		m.claim, f = claimRef(h, m.msg.Data())
	}
	var body []byte
	if f == nil {
		body, f = r.resolve(work, m, size, sum)
	}
	if f != nil {
		if work.Err() != nil {
			return plan{action: actShutdown, detail: f.detail}
		}
		if f.retry {
			return r.retryPlan(m, f.detail, nil, false)
		}
		return plan{action: actDeadLetter, reason: f.reason, detail: f.detail}
	}
	ev := &event{
		subject:   m.msg.Subject(),
		msgID:     m.msgID,
		streamSeq: m.meta.Sequence.Stream,
		attempt:   m.meta.NumDelivered,
		body:      body,
		headers:   r.handlerHeaders(h),
	}
	if r.paused() {
		// The relay paused while this event was being prepared; no handler starts after a pause.
		return pausedPlan
	}
	start := time.Now()
	res := r.handler.deliver(work, ev)
	r.m.handlerDuration.Observe(time.Since(start).Seconds(), res.verdict.String())
	return r.handlerPlan(work, m, res, body)
}

func (r *Relay) handlerPlan(work context.Context, m *message, res result, body []byte) plan {
	if work.Err() != nil && res.verdict != verdictAck {
		return plan{action: actShutdown, detail: res.detail}
	}
	switch res.verdict {
	case verdictAck:
		return plan{action: actAck}
	case verdictRetry:
		p := r.retryPlan(m, res.detail, body, true)
		p.after = res.after
		return p
	case verdictReject:
		return plan{action: actDeadLetter, reason: contract.DeadHandlerRejected, detail: res.detail, body: body, resolved: true}
	case verdictPause:
		r.pause(res.detail)
		return plan{action: actPause, detail: res.detail}
	}
	return plan{action: actShutdown, detail: res.detail}
}

// retryPlan retries, or dead-letters when this was the last allowed attempt.
func (r *Relay) retryPlan(m *message, detail string, body []byte, resolved bool) plan {
	if r.lastAttempt(m.meta) {
		return plan{action: actDeadLetter, reason: contract.DeadMaxAttempts, detail: detail, body: body, resolved: resolved}
	}
	return plan{action: actRetry, detail: detail}
}

func (r *Relay) lastAttempt(meta *jetstream.MsgMetadata) bool {
	return attempt(meta) >= r.cfg.MaxAttempts
}

// attempt is the delivery attempt number, 1 on first delivery.
func attempt(meta *jetstream.MsgMetadata) int {
	n := meta.NumDelivered
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	return int(n)
}

// backoff is the retry delay after delivery attempt n: backoff[n-1], the last value repeating.
func (r *Relay) backoff(meta *jetstream.MsgMetadata) time.Duration {
	return r.cfg.Backoff[min(max(attempt(meta), 1)-1, len(r.cfg.Backoff)-1)]
}

// retryDelay is the backoff delay, or the handler's requested delay when that is longer, capped at the last
// backoff value.
func (r *Relay) retryDelay(meta *jetstream.MsgMetadata, after time.Duration) time.Duration {
	return max(r.backoff(meta), min(after, r.cfg.Backoff[len(r.cfg.Backoff)-1]))
}

// execute writes the dead letter a plan calls for, while the message is still kept alive, and returns the
// outcome to record. A failed dead-letter publish turns into a retry; it never terminates the message.
func (r *Relay) execute(work context.Context, m *message, p *plan) string {
	switch p.action {
	case actAck:
		return outcomeAcked
	case actRetry:
		return outcomeRetried
	case actPause:
		return outcomePaused
	case actShutdown:
		return outcomeShutdown
	}
	if r.paused() {
		*p = pausedPlan
		return outcomePaused
	}
	if err := r.publishDeadLetter(work, m, p); err != nil {
		if work.Err() != nil {
			p.action = actShutdown
			return outcomeShutdown
		}
		r.m.deadLetterFailed.Inc()
		if tooLarge(err) {
			r.m.deadLetterTooLarge.Inc()
			r.log.Error("dead-letter record exceeds the server's max_payload or the dead-letter stream's max_msg_size; raise them above the largest body plus headers", "seq", m.meta.Sequence.Stream, "reason", p.reason, "bytes", len(p.body))
		} else {
			r.log.Error("dead-letter publish failed; the event is retried", "seq", m.meta.Sequence.Stream, "reason", p.reason, "error", err)
		}
		p.action = actRetry
		return outcomeDeadLetterFailed
	}
	r.m.deadLetters.Inc(p.reason)
	return outcomeDeadLettered
}

// settle acknowledges, naks or terminates the message and records the outcome.
func (r *Relay) settle(m *message, p plan, outcome string) {
	var err error
	switch p.action {
	case actAck:
		ctx, cancel := context.WithTimeout(context.Background(), settleTimeout)
		err = m.msg.DoubleAck(ctx)
		cancel()
	case actRetry:
		err = m.msg.NakWithDelay(r.retryDelay(m.meta, p.after))
	case actPause:
		err = m.msg.NakWithDelay(r.backoff(m.meta))
	case actShutdown:
		err = m.msg.NakWithDelay(r.cfg.ShutdownNakDelay)
	case actDeadLetter:
		// The record is acknowledged: the event is settled for good, paused or not.
		err = m.msg.TermWithReason(p.reason)
	}
	r.m.deliveries.Inc(outcome)
	attrs := []any{
		"seq", m.meta.Sequence.Stream, "subject", m.msg.Subject(), "attempt", m.meta.NumDelivered,
		"source", m.msg.Headers().Get(contract.HeaderSource), "event_id", m.msg.Headers().Get(contract.HeaderEventID),
		"outcome", outcome,
	}
	if p.reason != "" {
		attrs = append(attrs, "reason", p.reason)
	}
	if p.detail != "" {
		attrs = append(attrs, "detail", p.detail)
	}
	if err != nil {
		r.m.settleFailed.Inc()
		r.log.Error("delivery not settled; the server redelivers it after the ack wait", append(attrs, "error", err)...)
		return
	}
	r.log.Info("delivery", attrs...)
}

// publishDeadLetter writes the dead-letter record and waits for the stream's acknowledgement.
func (r *Relay) publishDeadLetter(work context.Context, m *message, p *plan) error {
	ctx, cancel := context.WithTimeout(work, publishTimeout)
	defer cancel()
	id := r.deadMsgID(m.meta)
	msg := r.deadLetter(m, p, id, time.Now())
	_, err := r.js.PublishMsg(ctx, msg, jetstream.WithExpectStream(r.cfg.DeadLetter.Stream), jetstream.WithMsgID(id))
	return err
}

// deadMsgID identifies a dead letter by event stream incarnation, consumer and sequence:
// "dead:<stream>:<stream created, unix nanoseconds>:<consumer>:<seq>". A stream recreated under the same name
// reuses sequences; its new creation time keeps their records apart.
func (r *Relay) deadMsgID(meta *jetstream.MsgMetadata) string {
	r.mu.Lock()
	created := r.streamCreated
	r.mu.Unlock()
	return "dead:" + meta.Stream + ":" + strconv.FormatInt(created.UnixNano(), 10) + ":" + meta.Consumer + ":" +
		strconv.FormatUint(meta.Sequence.Stream, 10)
}

// maxDeadHeaderBytes keeps a dead-letter record's headers, with those the publish adds, under the broker's
// 64 KiB header limit, with room to spare.
const maxDeadHeaderBytes = 65535 - 1024

// headerSize is the encoded size of h plus the message ID and expected-stream headers the publish adds.
func headerSize(h nats.Header, id, stream string) int {
	c := nats.Header{}
	for k, v := range h {
		c[k] = v
	}
	c.Set(contract.HeaderMsgID, id)
	c.Set(jetstream.ExpectedStreamHeader, stream)
	return (&nats.Msg{Header: c}).Size()
}

// deadLetter builds a dead-letter record from an allowlist of the original's headers, never a copy of them:
// stored broker headers would make the publish fail or deduplicate wrongly.
func (r *Relay) deadLetter(m *message, p *plan, id string, at time.Time) *nats.Msg {
	orig := m.msg.Headers()
	h := nats.Header{}
	for _, name := range deadContractHeaders {
		copyHeader(h, orig, name)
	}
	for name, v := range orig {
		if !contract.IsReserved(name) {
			h[name] = append([]string(nil), v...)
		}
	}
	body := p.body
	if !p.resolved {
		body = nil
		if m.claim == nil {
			body = m.msg.Data()
		}
		copyHeader(h, orig, contract.HeaderClaimBucket)
		copyHeader(h, orig, contract.HeaderClaimKey)
	}
	h.Set(contract.HeaderDeadReason, p.reason)
	if p.detail != "" {
		h.Set(contract.HeaderDeadDetail, truncate(p.detail, contract.MaxDeadDetailBytes))
	}
	h.Set(contract.HeaderDeadAttempts, strconv.FormatUint(m.meta.NumDelivered, 10))
	h.Set(contract.HeaderDeadAt, contract.FormatTime(at))
	h.Set(contract.HeaderOriginalSubject, m.msg.Subject())
	h.Set(contract.HeaderOriginalStream, m.meta.Stream)
	h.Set(contract.HeaderOriginalSeq, strconv.FormatUint(m.meta.Sequence.Stream, 10))
	h.Set(contract.HeaderOriginalConsumer, m.meta.Consumer)
	if m.msgID != "" {
		h.Set(contract.HeaderOriginalMsgID, m.msgID)
	}
	if headerSize(h, id, r.cfg.DeadLetter.Stream) > maxDeadHeaderBytes {
		// The contract headers and the dead-letter metadata always fit; the event's own headers give way.
		for name := range h {
			if !contract.IsReserved(name) {
				delete(h, name)
			}
		}
		h.Set(contract.HeaderDeadHeadersDropped, "true")
	}
	return &nats.Msg{Subject: r.deadSubject, Header: h, Data: body}
}

func copyHeader(dst, src nats.Header, name string) {
	if v := src.Values(name); len(v) > 0 {
		dst[name] = append([]string(nil), v...)
	}
}

// tooLarge reports a publish refused for its size, by the client (max_payload) or by the stream (max_msg_size).
func tooLarge(err error) bool {
	var apiErr *jetstream.APIError
	return errors.Is(err, nats.ErrMaxPayload) ||
		(errors.As(err, &apiErr) && apiErr.ErrorCode == jetstream.JSErrCodeStreamMessageExceedsMaximum)
}
