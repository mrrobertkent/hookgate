package output

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mrrobertkent/hookgate/internal/claimcheck"
	"github.com/mrrobertkent/hookgate/internal/config"
	"github.com/mrrobertkent/hookgate/internal/contract"
	"github.com/mrrobertkent/hookgate/internal/natsconn"
)

// bucketHeadroom is the space a bucket must have beyond max_body_bytes, for metadata and orphaned chunks.
const bucketHeadroom = 1 << 20

// recheckEvery is how often a not-ready output checks its destination again between reconnects.
const recheckEvery = 5 * time.Second

// claimKeyReserve bounds the generation suffix ("/<unix seconds>") a claim key may gain.
const claimKeyReserve = 21

var (
	errNotChecked     = errors.New("destination not checked yet")
	jsonHeader        = http.Header{contentType: {"application/json"}}
	unavailableHeader = http.Header{contentType: {"text/plain; charset=utf-8"}}
)

// acceptedBody is the 200 response body.
func acceptedBody(duplicate bool) []byte {
	return []byte(`{"status":"accepted","duplicate":` + strconv.FormatBool(duplicate) + "}\n")
}

// jetStreamOutput publishes verified events to a stream, storing large bodies in a claim-check store first.
type jetStreamOutput struct {
	source     string
	cfg        config.JetStreamOutput
	maxBody    int64
	subject    string
	nc         *natsconn.Conn
	js         jetstream.JetStream
	store      claimcheck.Store // nil without claim_check
	setHeaders map[string]string
	pass       headerFilter
	m          *Metrics
	log        *slog.Logger
	report     func(error)

	kick chan struct{}

	mu       sync.Mutex
	checkErr error
	maxMsg   int64 // stream max_msg_size; <= 0 unlimited
}

func newJetStream(src config.Source, d Deps) (*jetStreamOutput, error) {
	cfg := *src.Output.JetStream
	nc := d.Conns[cfg.Connection]
	if nc == nil {
		return nil, fmt.Errorf("connection %s is not available", cfg.Connection)
	}
	set, err := resolveSetHeaders(cfg.SetHeaders)
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc.Conn)
	if err != nil {
		return nil, err
	}
	o := &jetStreamOutput{
		source:     src.Name,
		cfg:        cfg,
		maxBody:    src.MaxBodyBytes,
		subject:    contract.RenderSubject(cfg.Subject, "source", src.Name),
		nc:         nc,
		js:         js,
		setHeaders: set,
		pass:       newHeaderFilter(cfg.PassHeaders, src.CredentialHeaders()),
		m:          d.Metrics,
		log:        d.Log,
		report:     d.ReportReady,
		kick:       make(chan struct{}, 1),
		checkErr:   errNotChecked,
	}
	if cc := cfg.ClaimCheck; cc != nil {
		o.store = claimcheck.NewObjectStore(js, cc.ObjectStore.Bucket, claimcheck.ObjectStoreOptions{
			ReuseWithin:    cc.ReuseWithin,
			RequestTimeout: cfg.AttemptTimeout,
		})
	}
	return o, nil
}

// Ready reports ErrNotConnected while the connection is down, else the last check's result.
func (o *jetStreamOutput) Ready() error {
	if !o.nc.IsConnected() {
		return ErrNotConnected
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.checkErr
}

// Reconnected asks Run to check the destination again.
func (o *jetStreamOutput) Reconnected() {
	select {
	case o.kick <- struct{}{}:
	default:
	}
}

// Run checks the destination now, after every Reconnected call and every recheckEvery while not ready, until
// ctx ends.
func (o *jetStreamOutput) Run(ctx context.Context) {
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-o.kick:
		case <-t.C:
		}
		if !o.nc.IsConnected() {
			continue // the reconnect callback kicks again
		}
		err := o.check(ctx)
		if ctx.Err() != nil {
			return
		}
		o.setCheck(err)
		if err != nil {
			t.Reset(recheckEvery)
		}
	}
}

func (o *jetStreamOutput) setCheck(err error) {
	o.mu.Lock()
	changed := (o.checkErr == nil) != (err == nil) || (err != nil && o.checkErr.Error() != err.Error())
	o.checkErr = err
	o.mu.Unlock()
	if changed {
		if err != nil {
			o.log.Error("source not ready; it answers 503 until its output checks pass", "source", o.source, "error", err)
		} else {
			o.log.Info("source ready", "source", o.source, "stream", o.cfg.Stream)
		}
	}
	if o.report != nil {
		o.report(err)
	}
}

// check verifies the stream exists and every publish fits its limits, and that the claim-check bucket exists
// and can hold every body for as long as the stream keeps its message.
func (o *jetStreamOutput) check(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, o.cfg.Timeout)
	defer cancel()
	st, err := o.js.Stream(ctx, o.cfg.Stream)
	if errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("stream %s not found", o.cfg.Stream)
	}
	if err != nil {
		return fmt.Errorf("stream %s: %w", o.cfg.Stream, err)
	}
	sc := st.CachedInfo().Config
	o.mu.Lock()
	o.maxMsg = int64(sc.MaxMsgSize)
	o.mu.Unlock()
	limit := o.publishLimit()
	if o.cfg.InlineMaxBytes+o.cfg.MaxHeaderBytes > limit {
		return fmt.Errorf("inline_max_bytes (%d) + max_header_bytes (%d) exceed %d, the smaller of the server's max_payload and stream %s max_msg_size",
			o.cfg.InlineMaxBytes, o.cfg.MaxHeaderBytes, limit, o.cfg.Stream)
	}
	if o.store == nil {
		return nil
	}
	return o.checkStore(ctx, sc.MaxAge)
}

func (o *jetStreamOutput) checkStore(ctx context.Context, streamMaxAge time.Duration) error {
	cc := o.cfg.ClaimCheck
	lim, err := o.store.Check(ctx)
	if err != nil {
		return err
	}
	b := cc.ObjectStore.Bucket
	if lim.MaxBytes > 0 && lim.MaxBytes < o.maxBody+bucketHeadroom {
		return fmt.Errorf("bucket %s max_bytes %d is below max_body_bytes + 1 MiB (%d)", b, lim.MaxBytes, o.maxBody+bucketHeadroom)
	}
	if lim.TTL > 0 && (streamMaxAge <= 0 || lim.TTL < streamMaxAge+cc.ReuseWithin) {
		return fmt.Errorf("bucket %s TTL %s is shorter than stream %s max_age (%s) + reuse_within (%s); a claimed body could expire before its message",
			b, lim.TTL, o.cfg.Stream, streamMaxAge, cc.ReuseWithin)
	}
	chunkLimit := o.nc.MaxPayload()
	if lim.MaxMsgSize > 0 {
		chunkLimit = min(chunkLimit, int64(lim.MaxMsgSize))
	}
	if claimcheck.ChunkSize+o.cfg.MaxHeaderBytes > chunkLimit {
		return fmt.Errorf("bucket %s cannot store %d-byte chunks: the limit is %d", b, claimcheck.ChunkSize, chunkLimit)
	}
	return nil
}

// publishLimit is the largest message (headers and body) a publish may carry.
func (o *jetStreamOutput) publishLimit() int64 {
	limit := o.nc.MaxPayload()
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.maxMsg > 0 {
		limit = min(limit, o.maxMsg)
	}
	return limit
}

// Deliver publishes ev and answers 200 only after the stream acknowledged it.
func (o *jetStreamOutput) Deliver(ctx context.Context, ev Event) Result {
	id, authenticated, rej := o.eventID(ev)
	if rej != "" {
		return Result{Status: http.StatusBadRequest, Outcome: "bad_request", Reason: rej}
	}
	sum := sha256.Sum256(ev.Body)
	sha := hex.EncodeToString(sum[:])
	h := o.headers(ev, id, authenticated, sha)
	msgID := ""
	if id != "" {
		msgID = contract.MessageID(o.source, id)
	}
	inlineHeader := headerBytes(h, msgID, o.cfg.Stream)
	ctx, cancel := context.WithTimeout(ctx, o.cfg.Timeout)
	defer cancel()

	data := ev.Body
	if o.store != nil && !o.fitsInline(len(ev.Body), inlineHeader) {
		key := contract.ClaimKey(o.source, id, sha)
		h.Set(contract.HeaderClaimBucket, o.cfg.ClaimCheck.ObjectStore.Bucket)
		h.Set(contract.HeaderClaimKey, key+strings.Repeat("x", claimKeyReserve))
		if n := headerBytes(h, msgID, o.cfg.Stream); n > o.cfg.MaxHeaderBytes {
			return tooManyHeaderBytes(n)
		}
		ref, err := o.store.Put(ctx, key, ev.Body, sha)
		if err != nil {
			o.m.ClaimPuts.Inc(o.source, "error")
			return unavailable("claim check: " + err.Error())
		}
		o.m.ClaimPuts.Inc(o.source, "ok")
		h.Set(contract.HeaderClaimKey, ref.Key)
		data = nil
	} else if inlineHeader > o.cfg.MaxHeaderBytes {
		return tooManyHeaderBytes(inlineHeader)
	}
	ack, err := o.publish(ctx, h, data, msgID)
	switch {
	case errors.Is(err, nats.ErrMaxPayload):
		return Result{Status: http.StatusRequestEntityTooLarge, Outcome: "too_large", Reason: "message over the server's max_payload"}
	case err != nil:
		return unavailable("publish: " + err.Error())
	case ack.Duplicate:
		return Result{Status: http.StatusOK, Outcome: "duplicate", Header: jsonHeader.Clone(), Body: acceptedBody(true)}
	}
	return Result{Status: http.StatusOK, Outcome: "published", Header: jsonHeader.Clone(), Body: acceptedBody(false)}
}

func (o *jetStreamOutput) fitsInline(bodyLen int, headerLen int64) bool {
	return int64(bodyLen) <= o.cfg.InlineMaxBytes && int64(bodyLen)+headerLen <= o.publishLimit()
}

func unavailable(reason string) Result {
	return Result{Status: http.StatusServiceUnavailable, Outcome: "output_error", Header: unavailableHeader.Clone(),
		Body: []byte("unavailable\n"), Reason: reason}
}

func tooManyHeaderBytes(n int64) Result {
	return Result{Status: http.StatusBadRequest, Outcome: "bad_request", Reason: fmt.Sprintf("published headers would be %d bytes, over max_header_bytes", n)}
}

// eventID returns the event's ID: the one a check authenticated, else, when configured, the unsigned header.
// A non-empty rejection reason means the request must be refused.
func (o *jetStreamOutput) eventID(ev Event) (id string, authenticated bool, rejection string) {
	if ev.EventID != "" {
		if !contract.ValidEventID(ev.EventID) {
			return "", false, "authenticated event id is not 1-200 bytes of A-Z a-z 0-9 _ ~ = + -"
		}
		return ev.EventID, true, ""
	}
	name := o.cfg.MessageID.Header
	if name == "" {
		return "", false, ""
	}
	vals := ev.Header.Values(name)
	switch {
	case len(vals) == 0:
		return "", false, ""
	case len(vals) > 1:
		return "", false, "repeated header " + name
	case !contract.ValidEventID(vals[0]):
		return "", false, "header " + name + " must be 1-200 bytes of A-Z a-z 0-9 _ ~ = + -"
	}
	return vals[0], false, ""
}

// headers builds the published headers: passed request headers under lowercase names, set_headers as written,
// then the contract headers, which nothing else can override.
func (o *jetStreamOutput) headers(ev Event, id string, authenticated bool, sha string) nats.Header {
	h := nats.Header{}
	for name, vals := range ev.Header {
		if o.pass.allows(name) && !contract.IsReserved(name) {
			lower := strings.ToLower(name)
			h[lower] = append(h[lower], vals...)
		}
	}
	for name, v := range o.setHeaders {
		h[name] = []string{v}
	}
	h[contract.HeaderContract] = []string{contract.Version}
	h[contract.HeaderSource] = []string{o.source}
	if id != "" {
		h[contract.HeaderEventID] = []string{id}
		h[contract.HeaderEventIDAuthenticated] = []string{strconv.FormatBool(authenticated)}
	}
	if !ev.SignedAt.IsZero() {
		h[contract.HeaderSignedAt] = []string{ev.SignedAt.UTC().Format(time.RFC3339)}
	}
	h[contract.HeaderBodySHA256] = []string{sha}
	h[contract.HeaderBodySize] = []string{strconv.Itoa(len(ev.Body))}
	h[contract.HeaderReceivedAt] = []string{contract.FormatTime(ev.ReceivedAt)}
	return h
}

// headerBytes is the encoded size of h plus the headers the publish options add, as the client counts it
// against max_payload.
func headerBytes(h nats.Header, msgID, stream string) int64 {
	m := nats.Msg{Header: make(nats.Header, len(h)+2)}
	for k, v := range h {
		m.Header[k] = v
	}
	m.Header[jetstream.ExpectedStreamHeader] = []string{stream}
	if msgID != "" {
		m.Header[jetstream.MsgIDHeader] = []string{msgID}
	}
	return int64(m.Size())
}

// publish retries transient failures within ctx, each attempt bounded by attempt_timeout. Every attempt is a
// fresh message, since a publish adds its option headers to the message it sends.
func (o *jetStreamOutput) publish(ctx context.Context, h nats.Header, data []byte, msgID string) (*jetstream.PubAck, error) {
	opts := []jetstream.PublishOpt{jetstream.WithExpectStream(o.cfg.Stream)}
	if msgID != "" {
		opts = append(opts, jetstream.WithMsgID(msgID))
	}
	wait := 50 * time.Millisecond
	for {
		actx, cancel := context.WithTimeout(ctx, o.cfg.AttemptTimeout)
		start := time.Now()
		ack, err := o.js.PublishMsg(actx, &nats.Msg{Subject: o.subject, Header: cloneHeader(h), Data: data}, opts...)
		cancel()
		o.m.PublishDuration.Observe(time.Since(start).Seconds(), o.source)
		o.m.PublishAttempts.Inc(o.source, attemptResult(ack, err))
		if err == nil || !retryable(err) {
			return ack, err
		}
		select {
		case <-ctx.Done():
			return nil, err
		case <-time.After(wait):
		}
		wait = min(2*wait, time.Second)
	}
}

func cloneHeader(h nats.Header) nats.Header {
	c := make(nats.Header, len(h))
	for k, v := range h {
		c[k] = append([]string(nil), v...)
	}
	return c
}

// retryable reports failures that say nothing about the event: timeouts, no stream responding, a connection
// that is down, and server-side (5xx) JetStream errors.
func retryable(err error) bool {
	for _, target := range []error{context.DeadlineExceeded, nats.ErrTimeout, nats.ErrNoResponders, jetstream.ErrNoStreamResponse,
		nats.ErrConnectionReconnecting, nats.ErrReconnectBufExceeded, nats.ErrDisconnected} {
		if errors.Is(err, target) {
			return true
		}
	}
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) && apiErr.Code >= 500
}

func attemptResult(ack *jetstream.PubAck, err error) string {
	switch {
	case err == nil && ack.Duplicate:
		return "duplicate"
	case err == nil:
		return "ok"
	case errors.Is(err, nats.ErrMaxPayload):
		return "too_large"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, nats.ErrTimeout):
		return "timeout"
	case retryable(err):
		return "unavailable"
	}
	return "error"
}
