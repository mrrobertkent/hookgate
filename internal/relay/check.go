package relay

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/mrrobertkent/hookgate/internal/claimcheck"
)

// checkLoop runs the readiness checks at start, every checkInterval and whenever triggered (each reconnect).
func (r *Relay) checkLoop(ctx context.Context) {
	t := time.NewTicker(checkInterval)
	defer t.Stop()
	for {
		r.check(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-r.recheck:
		}
	}
}

func (r *Relay) check(ctx context.Context) {
	if !r.nc.IsConnected() {
		r.setCheck(nil, 0, errors.New("not connected to NATS"))
		return
	}
	cctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	cons, info, err := r.inspect(cctx)
	if ctx.Err() != nil {
		return
	}
	if info != nil {
		r.m.pending.Set(float64(info.NumPending))
		r.m.ackPending.Set(float64(info.NumAckPending))
		r.m.redelivered.Set(float64(info.NumRedelivered))
	}
	var ackWait time.Duration
	if info != nil {
		ackWait = info.Config.AckWait
	}
	r.setCheck(cons, ackWait, err)
}

func (r *Relay) setCheck(cons jetstream.Consumer, ackWait time.Duration, err error) {
	r.mu.Lock()
	changed := (r.checkErr == nil) != (err == nil) || (err != nil && r.checkErr.Error() != err.Error())
	r.checkErr = err
	if err == nil {
		r.cons = cons
		if ackWait > 0 {
			r.ackWait = ackWait
		}
	}
	r.mu.Unlock()
	if changed {
		if err != nil {
			r.log.Error("not ready", "error", err)
		} else {
			r.log.Info("ready")
		}
	}
	r.updateReady()
}

// inspect reads the stream, consumer, dead-letter stream and claim bucket and checks the consumer's settings.
// It returns the consumer and its info whenever they could be read, even if a check fails.
func (r *Relay) inspect(ctx context.Context) (jetstream.Consumer, *jetstream.ConsumerInfo, error) {
	st, err := r.js.Stream(ctx, r.cfg.Stream)
	if err != nil {
		return nil, nil, fmt.Errorf("stream %s: %w", r.cfg.Stream, err)
	}
	r.mu.Lock()
	r.streamCreated = st.CachedInfo().Created
	r.mu.Unlock()
	if rerr := checkRouting("stream "+r.cfg.Stream, st.CachedInfo().Config); rerr != nil {
		return nil, nil, rerr
	}
	var cons jetstream.Consumer
	cons, err = r.js.Consumer(ctx, r.cfg.Stream, r.cfg.Consumer)
	var info *jetstream.ConsumerInfo
	if err == nil {
		info, err = cons.Info(ctx)
	}
	if err != nil {
		return nil, nil, r.consumerError(err)
	}
	if cerr := r.checkConsumer(info); cerr != nil {
		return cons, info, cerr
	}
	dl, err := r.js.Stream(ctx, r.cfg.DeadLetter.Stream)
	if err != nil {
		return cons, info, fmt.Errorf("dead_letter.stream %s: %w", r.cfg.DeadLetter.Stream, err)
	}
	if err := r.checkDeadStream(dl.CachedInfo().Config, st.CachedInfo().Config); err != nil {
		return cons, info, err
	}
	if r.claims != nil {
		if err := checkClaimBucket(ctx, r.claims, st.CachedInfo().Config); err != nil {
			return cons, info, err
		}
	}
	return cons, info, nil
}

// checkDeadStream requires a stream that captures the dead-letter subject and keeps every record it acknowledges:
// with interest or work-queue retention it can acknowledge a record no consumer wants and store nothing, and with
// discard old or a per-subject limit a later record can evict one whose message ID still deduplicates a retry.
// Its message size limit must be at least the event stream's, since records carry whole bodies.
func (r *Relay) checkDeadStream(c, events jetstream.StreamConfig) error {
	name := r.cfg.DeadLetter.Stream
	var errs []error
	if !matchesAny(c.Subjects, r.deadSubject) {
		errs = append(errs, fmt.Errorf("does not capture %s", r.deadSubject))
	}
	if c.NoAck {
		errs = append(errs, errors.New("no_ack is set; the relay needs the acknowledgement before terminating an event"))
	}
	if c.Retention != jetstream.LimitsPolicy {
		errs = append(errs, fmt.Errorf("retention is %s, want limits", c.Retention))
	}
	if err := checkRouting("", c); err != nil {
		errs = append(errs, err)
	}
	if c.Discard != jetstream.DiscardNew {
		errs = append(errs, errors.New("discard old, want discard new"))
	}
	if c.MaxMsgsPerSubject > 0 {
		errs = append(errs, fmt.Errorf("max_msgs_per_subject is %d, want unlimited", c.MaxMsgsPerSubject))
	}
	if c.MaxMsgSize > 0 && (events.MaxMsgSize <= 0 || c.MaxMsgSize < events.MaxMsgSize) {
		errs = append(errs, fmt.Errorf("max_msg_size %d is below stream %s's (%d, or unlimited)", c.MaxMsgSize, events.Name, events.MaxMsgSize))
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("dead_letter.stream %s: %w", name, errors.Join(errs...))
}

// checkConsumer requires a durable pull consumer with explicit acks whose terminal decision and retry delays
// belong to the relay: no server delivery limit and no BackOff.
func (r *Relay) checkConsumer(info *jetstream.ConsumerInfo) error {
	c := info.Config
	var errs []error
	if c.AckPolicy != jetstream.AckExplicitPolicy {
		errs = append(errs, fmt.Errorf("ack_policy is %s, want explicit", c.AckPolicy))
	}
	if c.MaxDeliver != -1 {
		errs = append(errs, fmt.Errorf("max_deliver is %d, want -1 (the relay dead-letters after max_attempts)", c.MaxDeliver))
	}
	if len(c.BackOff) > 0 {
		errs = append(errs, errors.New("backoff is set, want none (the relay delays retries itself)"))
	}
	if c.InactiveThreshold > 0 {
		errs = append(errs, errors.New("inactive_threshold is set, want a durable consumer without one"))
	}
	if got := consumerFilters(c); !sameSet(got, r.cfg.FilterSubjects) {
		errs = append(errs, fmt.Errorf("filter subjects are %q, want %q", got, r.cfg.FilterSubjects))
	}
	if c.HeadersOnly {
		errs = append(errs, errors.New("headers_only is set, want whole messages"))
	}
	if c.AckWait < time.Second {
		errs = append(errs, fmt.Errorf("ack_wait is %s, want at least 1s", c.AckWait))
	}
	if info.Paused {
		errs = append(errs, errors.New("paused on the server"))
	}
	errs = append(errs, r.checkPullLimits(c)...)
	if len(errs) == 0 {
		return nil
	}
	return r.consumerError(errors.Join(errs...))
}

func (r *Relay) consumerError(err error) error {
	return fmt.Errorf("consumer %s: %w", r.cfg.Consumer, err)
}

// checkPullLimits requires the consumer to accept the relay's pull requests: up to concurrency messages, expiring
// after fetchWait, without a byte limit.
func (r *Relay) checkPullLimits(c jetstream.ConsumerConfig) []error {
	var errs []error
	if c.MaxRequestBatch > 0 && c.MaxRequestBatch < r.cfg.Concurrency {
		errs = append(errs, fmt.Errorf("max_batch is %d, want 0 or at least concurrency (%d)", c.MaxRequestBatch, r.cfg.Concurrency))
	}
	if c.MaxRequestExpires > 0 && c.MaxRequestExpires < fetchWait {
		errs = append(errs, fmt.Errorf("max_expires is %s, want 0 or at least %s", c.MaxRequestExpires, fetchWait))
	}
	return errs
}

func consumerFilters(c jetstream.ConsumerConfig) []string {
	if c.FilterSubject != "" {
		return append([]string{c.FilterSubject}, c.FilterSubjects...)
	}
	return c.FilterSubjects
}

func sameSet(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(slices.Compact(a), slices.Compact(b))
}

// claimTTLMargin is added to the event stream's max_age and the claim reuse window when checking the bucket TTL.
const claimTTLMargin = time.Hour

// checkClaimBucket requires the claim bucket to keep bodies as durably as the event stream keeps events, and at
// least as long as an event referring to one can wait in the stream: a reused object may already be
// replayReuseWithin old when the event is published.
func checkClaimBucket(ctx context.Context, claims claimcheck.Store, sc jetstream.StreamConfig) error {
	lim, err := claims.Check(ctx)
	if err != nil {
		return fmt.Errorf("claim_check: %w", err)
	}
	if lim.Replicas < max(sc.Replicas, 1) || (sc.Storage == jetstream.FileStorage && !lim.FileStorage) {
		return fmt.Errorf("claim_check: bucket keeps bodies less durably than stream %s keeps events (replicas %d < %d, or memory storage for a file stream)",
			sc.Name, lim.Replicas, max(sc.Replicas, 1))
	}
	maxAge := sc.MaxAge
	if lim.TTL == 0 {
		return nil
	}
	need := maxAge + replayReuseWithin + claimTTLMargin
	if maxAge == 0 || lim.TTL < need {
		return fmt.Errorf("claim_check: bucket TTL %s is shorter than the event stream's max_age + %s + %s (%s); objects could expire before their events are delivered",
			lim.TTL, replayReuseWithin, claimTTLMargin, describeAge(maxAge, need))
	}
	return nil
}

func describeAge(maxAge, need time.Duration) string {
	if maxAge == 0 {
		return "the stream keeps events without an age limit"
	}
	return need.String()
}

// checkRouting rejects a stream that rewrites or copies subjects: a subject transform could turn one consumer's
// replay subject into another's, and republishing duplicates events onto other subjects.
func checkRouting(where string, c jetstream.StreamConfig) error {
	var what string
	switch {
	case c.SubjectTransform != nil:
		what = "has a subject transform"
	case c.RePublish != nil:
		what = "republishes messages"
	default:
		return nil
	}
	if where == "" {
		return errors.New(what)
	}
	return fmt.Errorf("%s %s; the relay needs subjects stored as published", where, what)
}
