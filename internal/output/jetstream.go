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

// ttlMargin is added to the time a claimed body must outlive its upload.
const ttlMargin = time.Hour

// recheckEvery is how often a not-ready output checks its destination again between reconnects.
const recheckEvery = 5 * time.Second

// recheckReady is how often a ready output checks again, so a stream or bucket changed underneath it is noticed.
const recheckReady = 30 * time.Second

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

	kick         chan struct{}
	recheckReady time.Duration

	mu       sync.Mutex
	checked  bool
	checkTag uint64 // the client's reconnect count when checkErr was obtained
	checkErr error
	warned   string // stream durability settings last warned about
	maxMsg   int64  // stream max_msg_size; <= 0 unlimited
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
		source:       src.Name,
		cfg:          cfg,
		maxBody:      src.MaxBodyBytes,
		subject:      contract.RenderSubject(cfg.Subject, "source", src.Name),
		nc:           nc,
		js:           js,
		setHeaders:   set,
		pass:         newHeaderFilter(cfg.PassHeaders, src.CredentialHeaders()),
		m:            d.Metrics,
		log:          d.Log,
		report:       d.ReportReady,
		kick:         make(chan struct{}, 1),
		recheckReady: recheckReady,
		checkErr:     errNotChecked,
	}
	if cc := cfg.ClaimCheck; cc != nil {
		o.store = claimcheck.NewObjectStore(js, cc.ObjectStore.Bucket, claimcheck.ObjectStoreOptions{
			ReuseWithin:    cc.ReuseWithin,
			RequestTimeout: cfg.AttemptTimeout,
		})
	}
	return o, nil
}

// Ready reports ErrNotConnected while the connection is down, and otherwise the result of the last check, which
// counts only when it ran on the current connection: the client's reconnect count is the same as when it ran.
// It reads the connection's sampled status, so admitting a request never waits on the client's lock.
func (o *jetStreamOutput) Ready() error {
	st := o.nc.Status()
	if !st.Connected {
		return ErrNotConnected
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.checked || o.checkTag != st.Reconnects {
		return errNotChecked
	}
	return o.checkErr
}

// Reconnected asks Run to check the destination on the new connection.
func (o *jetStreamOutput) Reconnected() {
	o.reportReady(errNotChecked)
	select {
	case o.kick <- struct{}{}:
	default:
	}
}

// Disconnected reports the source not ready; Ready already answers ErrNotConnected.
func (o *jetStreamOutput) Disconnected() { o.reportReady(ErrNotConnected) }

func (o *jetStreamOutput) reportReady(err error) {
	if o.report != nil {
		o.report(err)
	}
}

// Run checks the destination now, after every Reconnected call, every recheckEvery while not ready and every
// recheckReady while ready, until ctx ends.
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
		tag := o.nc.Stats().Reconnects
		err := o.check(ctx)
		if ctx.Err() != nil {
			return
		}
		next := o.recheckReady
		if !o.setCheck(err, tag) || err != nil {
			next = recheckEvery
		}
		t.Reset(next)
	}
}

// setCheck records a check's result unless the client reconnected while it ran; it reports whether it did.
func (o *jetStreamOutput) setCheck(err error, tag uint64) bool {
	if o.nc.Stats().Reconnects != tag {
		return false
	}
	o.mu.Lock()
	prev := o.checkErr
	if !o.checked || o.checkTag != tag {
		prev = errNotChecked
	}
	o.checkErr, o.checkTag, o.checked = err, tag, true
	o.mu.Unlock()
	if (prev == nil) != (err == nil) || (err != nil && prev.Error() != err.Error()) {
		if err != nil {
			o.log.Error("source not ready; it answers 503 until its output checks pass", "source", o.source, "error", err)
		} else {
			o.log.Info("source ready", "source", o.source, "stream", o.cfg.Stream)
		}
	}
	o.reportReady(err)
	return true
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
	if err := o.streamFault(sc); err != nil {
		return err
	}
	o.warnDurability(sc)
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
	return o.checkStore(ctx, sc)
}

func (o *jetStreamOutput) checkStore(ctx context.Context, sc jetstream.StreamConfig) error {
	cc := o.cfg.ClaimCheck
	streamMaxAge := sc.MaxAge
	lim, err := o.store.Check(ctx)
	if err != nil {
		return err
	}
	b := cc.ObjectStore.Bucket
	if lim.Replicas < max(sc.Replicas, 1) || (sc.Storage == jetstream.FileStorage && !lim.FileStorage) {
		return fmt.Errorf("bucket %s keeps bodies less durably than stream %s keeps events (replicas %d < %d, or memory storage for a file stream)",
			b, o.cfg.Stream, lim.Replicas, max(sc.Replicas, 1))
	}
	if lim.MaxBytes > 0 && lim.MaxBytes < o.maxBody+bucketHeadroom {
		return fmt.Errorf("bucket %s max_bytes %d is below max_body_bytes + 1 MiB (%d)", b, lim.MaxBytes, o.maxBody+bucketHeadroom)
	}
	if need := streamMaxAge + cc.ReuseWithin + o.cfg.Timeout + ttlMargin; lim.TTL > 0 && (streamMaxAge <= 0 || lim.TTL < need) {
		return fmt.Errorf("bucket %s TTL %s is shorter than stream %s max_age (%s) + reuse_within (%s) + timeout (%s) + 1h (%s); a claimed body could expire before its message",
			b, lim.TTL, o.cfg.Stream, streamMaxAge, cc.ReuseWithin, o.cfg.Timeout, need)
	}
	chunkLimit := o.nc.MaxPayload()
	if lim.MaxMsgSize > 0 {
		chunkLimit = min(chunkLimit, int64(lim.MaxMsgSize))
	}
	chunkHeader := headerBytes(nil, "", "OBJ_"+b)
	if claimcheck.ChunkSize+chunkHeader > chunkLimit {
		return fmt.Errorf("bucket %s cannot store %d-byte chunks: the limit is %d", b, claimcheck.ChunkSize, chunkLimit)
	}
	return nil
}

// streamFault reports a stream that does not store the source's subject or could acknowledge an event without
// keeping it.
func (o *jetStreamOutput) streamFault(sc jetstream.StreamConfig) error {
	switch {
	case !bindsSubject(sc, o.subject):
		return fmt.Errorf("stream %s does not store subject %s", o.cfg.Stream, o.subject)
	case sc.NoAck:
		return fmt.Errorf("stream %s has no_ack set; publishes would never be acknowledged", o.cfg.Stream)
	case sc.Retention != jetstream.LimitsPolicy:
		return fmt.Errorf("stream %s retention is %s; it must be limits, or the stream may acknowledge an event without storing it", o.cfg.Stream, sc.Retention)
	case sc.Discard != jetstream.DiscardNew:
		return fmt.Errorf("stream %s discard is %s; it must be new, or a full stream evicts acknowledged events", o.cfg.Stream, sc.Discard)
	}
	return nil
}

// bindsSubject reports whether the stream's subjects (its name when it lists none) match subject.
func bindsSubject(sc jetstream.StreamConfig, subject string) bool {
	filters := sc.Subjects
	if len(filters) == 0 {
		filters = []string{sc.Name}
	}
	for _, f := range filters {
		if subjectMatches(f, subject) {
			return true
		}
	}
	return false
}

// subjectMatches reports whether a literal subject matches a NATS subject filter: "*" matches one token and a
// final ">" one or more.
func subjectMatches(filter, subject string) bool {
	ft, st := strings.Split(filter, "."), strings.Split(subject, ".")
	for i, f := range ft {
		switch {
		case f == ">" && i == len(ft)-1:
			return len(st) > i
		case i >= len(st):
			return false
		case f != "*" && f != st[i]:
			return false
		}
	}
	return len(ft) == len(st)
}

// warnDurability logs when an acknowledgement from the stream does not mean the event survives a server loss.
func (o *jetStreamOutput) warnDurability(sc jetstream.StreamConfig) {
	key := fmt.Sprintf("%s/%d", sc.Storage, sc.Replicas)
	o.mu.Lock()
	seen := o.warned == key
	o.warned = key
	o.mu.Unlock()
	if !seen && (sc.Storage == jetstream.MemoryStorage || sc.Replicas < 2) {
		o.log.Warn("stream is not replicated file storage; an acknowledged event can be lost with one server",
			"source", o.source, "stream", o.cfg.Stream, "storage", sc.Storage.String(), "replicas", sc.Replicas)
	}
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
	var prepare func(context.Context, nats.Header) error
	if o.store != nil && !o.fitsInline(len(ev.Body), inlineHeader) {
		c := &claim{o: o, key: contract.ClaimKey(o.source, id, sha), body: ev.Body, sha: sha, msgID: msgID}
		h.Set(contract.HeaderClaimBucket, o.cfg.ClaimCheck.ObjectStore.Bucket)
		h.Set(contract.HeaderClaimKey, c.key)
		if n := headerBytes(h, msgID, o.cfg.Stream); n > o.cfg.MaxHeaderBytes {
			return tooManyHeaderBytes(n)
		}
		data, prepare = nil, c.prepare
	} else if inlineHeader > o.cfg.MaxHeaderBytes {
		return tooManyHeaderBytes(inlineHeader)
	}
	ack, err := o.publish(ctx, h, data, msgID, prepare)
	var big headerSizeError
	var dest destinationError
	switch {
	case errors.As(err, &big):
		return tooManyHeaderBytes(int64(big))
	case errors.As(err, &dest):
		r := unavailable(dest.Error())
		r.Outcome = "not_ready"
		return r
	case errors.Is(err, errClaim):
		return unavailable(err.Error())
	case errors.Is(err, nats.ErrMaxPayload):
		return Result{Status: http.StatusRequestEntityTooLarge, Outcome: "too_large", Reason: "message over the server's max_payload"}
	case err != nil:
		return unavailable("publish: " + err.Error())
	case ack.Duplicate:
		return Result{Status: http.StatusOK, Outcome: "duplicate", Header: jsonHeader.Clone(), Body: acceptedBody(true)}
	}
	return Result{Status: http.StatusOK, Outcome: "published", Header: jsonHeader.Clone(), Body: acceptedBody(false)}
}

// errClaim marks a failed claim-check store.
var errClaim = errors.New("claim check")

// headerSizeError is the encoded header size that exceeded max_header_bytes.
type headerSizeError int64

func (e headerSizeError) Error() string { return fmt.Sprintf("headers of %d bytes", int64(e)) }

// claim stores a body before its reference is published. The store is (re)done before the first publish attempt
// and before any attempt that follows a reconnect, since the new server may not hold what the old one stored.
type claim struct {
	o           *jetStreamOutput
	key, sha    string
	msgID       string
	body        []byte
	stored      bool
	storedOnRev uint64 // the client's reconnect count before the store
}

func (c *claim) prepare(ctx context.Context, h nats.Header) error {
	o := c.o
	reconnects := o.nc.Stats().Reconnects
	if c.stored && c.storedOnRev == reconnects {
		return nil
	}
	ref, err := o.store.Put(ctx, c.key, c.body, c.sha)
	if err != nil {
		o.m.ClaimPuts.Inc(o.source, "error")
		return fmt.Errorf("%w: %w", errClaim, err)
	}
	o.m.ClaimPuts.Inc(o.source, "ok")
	c.stored, c.storedOnRev = true, reconnects
	h.Set(contract.HeaderClaimKey, ref.Key)
	// A generation key is a few bytes longer than the key checked before.
	if n := headerBytes(h, c.msgID, o.cfg.Stream); n > o.cfg.MaxHeaderBytes {
		return headerSizeError(n)
	}
	return nil
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

// publish retries transient failures within ctx, each attempt bounded by attempt_timeout and preceded by the
// destination check on the current connection and by prepare when set; an error from either ends it. Every attempt is a fresh message, since a publish adds its option
// headers to the message it sends.
func (o *jetStreamOutput) publish(ctx context.Context, h nats.Header, data []byte, msgID string, prepare func(context.Context, nats.Header) error) (*jetstream.PubAck, error) {
	opts := []jetstream.PublishOpt{jetstream.WithExpectStream(o.cfg.Stream)}
	if msgID != "" {
		opts = append(opts, jetstream.WithMsgID(msgID))
	}
	wait := 50 * time.Millisecond
	for {
		ack, err := o.attempt(ctx, h, data, opts, prepare)
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

// attempt publishes once over a connection whose destination check passed. The ack counts only when the client
// kept that connection throughout: a stream of the same name on another server may not store what it
// acknowledges (an interest-based stream without consumers).
func (o *jetStreamOutput) attempt(ctx context.Context, h nats.Header, data []byte, opts []jetstream.PublishOpt,
	prepare func(context.Context, nats.Header) error) (*jetstream.PubAck, error) {
	tag, err := o.checkCurrent(ctx)
	if err != nil {
		return nil, err
	}
	if prepare != nil {
		if err = prepare(ctx, h); err != nil {
			return nil, err
		}
	}
	actx, cancel := context.WithTimeout(ctx, o.cfg.AttemptTimeout)
	start := time.Now()
	ack, err := o.js.PublishMsg(actx, &nats.Msg{Subject: o.subject, Header: cloneHeader(h), Data: data}, opts...)
	cancel()
	if err == nil && o.nc.Stats().Reconnects != tag {
		ack, err = nil, errReconnected
	}
	o.m.PublishDuration.Observe(time.Since(start).Seconds(), o.source)
	o.m.PublishAttempts.Inc(o.source, attemptResult(ack, err))
	return ack, err
}

// transient reports a check that failed because the connection is changing, not because of the destination.
func transient(err error) bool {
	return err != nil && (retryable(err) || errors.Is(err, nats.ErrConnectionClosed))
}

// errReconnected reports that the client changed connections during an attempt; the attempt is retried.
var errReconnected = errors.New("reconnected during the attempt")

// destinationError is a destination check that failed on the current connection.
type destinationError struct{ err error }

func (e destinationError) Error() string { return "destination check: " + e.err.Error() }
func (e destinationError) Unwrap() error { return e.err }

// checkCurrent returns the reconnect count of the client's current connection once the destination check has
// passed on it, running the check now when the last one ran on another connection. A check that cannot finish
// because the connection is changing is retried.
func (o *jetStreamOutput) checkCurrent(ctx context.Context) (uint64, error) {
	tag := o.nc.Stats().Reconnects
	o.mu.Lock()
	current, checkErr := o.checked && o.checkTag == tag, o.checkErr
	o.mu.Unlock()
	if !current {
		checkErr = o.check(ctx)
		if ctx.Err() != nil {
			return 0, ctx.Err() // the request ended; its check says nothing about the destination
		}
		if transient(checkErr) || !o.setCheck(checkErr, tag) {
			return 0, errReconnected
		}
	}
	if transient(checkErr) {
		return 0, errReconnected
	}
	if checkErr != nil {
		return 0, destinationError{checkErr}
	}
	return tag, nil
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
	for _, target := range []error{errReconnected, context.DeadlineExceeded, nats.ErrTimeout, nats.ErrNoResponders, jetstream.ErrNoStreamResponse,
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
