package relay

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mrrobertkent/hookgate/internal/claimcheck"
	"github.com/mrrobertkent/hookgate/internal/contract"
)

// ErrNoBody reports a dead-letter record without the event body: its claim-check object was missing or did not
// match. Replaying it republishes only the claim-check reference.
var ErrNoBody = errors.New("the record holds no body, only a claim-check reference")

// Replayer republishes dead-letter records to the replay subject of the consumer that failed them.
type Replayer struct {
	cfg    *Config
	js     jetstream.JetStream
	claims claimcheck.Store
	// ClaimReference allows replaying records whose body was never resolved by republishing their claim-check
	// reference; the consumer then fetches the object again.
	ClaimReference bool
}

// NewReplayer returns a replayer using js.
func NewReplayer(cfg *Config, js jetstream.JetStream) *Replayer {
	rp := &Replayer{cfg: cfg, js: js}
	if cc := cfg.ClaimCheck; cc != nil {
		rp.claims = claimcheck.NewObjectStore(js, cc.ObjectStore.Bucket, claimcheck.ObjectStoreOptions{
			ReuseWithin: replayReuseWithin, RequestTimeout: publishTimeout,
		})
	}
	return rp
}

// Replayed describes one published replay.
type Replayed struct {
	Subject   string
	Stream    string
	Sequence  uint64 // in the event stream
	Duplicate bool   // the record was already replayed within the stream's duplicate window
}

// Replay republishes the dead-letter record at seq.
func (rp *Replayer) Replay(ctx context.Context, seq uint64) (Replayed, error) {
	dl, err := rp.js.Stream(ctx, rp.cfg.DeadLetter.Stream)
	if err != nil {
		return Replayed{}, fmt.Errorf("dead_letter.stream %s: %w", rp.cfg.DeadLetter.Stream, err)
	}
	rec, err := dl.GetMsg(ctx, seq)
	if err != nil {
		return Replayed{}, fmt.Errorf("%s:%d: %w", rp.cfg.DeadLetter.Stream, seq, err)
	}
	msg, stream, err := rp.replayMsg(rec)
	if err != nil {
		return Replayed{}, fmt.Errorf("%s:%d: %w", rp.cfg.DeadLetter.Stream, seq, err)
	}
	id := "replay:" + rp.cfg.DeadLetter.Stream + ":" + strconv.FormatUint(seq, 10)
	if err = rp.fit(ctx, msg, stream, id); err != nil {
		return Replayed{}, fmt.Errorf("%s:%d: %w", rp.cfg.DeadLetter.Stream, seq, err)
	}
	pctx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()
	ack, err := rp.js.PublishMsg(pctx, msg, jetstream.WithExpectStream(stream), jetstream.WithMsgID(id))
	if err != nil {
		return Replayed{}, fmt.Errorf("publish %s: %w", msg.Subject, err)
	}
	return Replayed{Subject: msg.Subject, Stream: ack.Stream, Sequence: ack.Sequence, Duplicate: ack.Duplicate}, nil
}

// replayMsg rebuilds the event from a record: contract and application headers only, plus the replay marker.
func (rp *Replayer) replayMsg(rec *jetstream.RawStreamMsg) (*nats.Msg, string, error) {
	h := rec.Header
	if h.Get(contract.HeaderDeadReason) == "" {
		return nil, "", errors.New("not a dead-letter record")
	}
	consumer, stream := h.Get(contract.HeaderOriginalConsumer), h.Get(contract.HeaderOriginalStream)
	if !validName(consumer) || !validName(stream) {
		return nil, "", fmt.Errorf("record has no valid %s and %s", contract.HeaderOriginalConsumer, contract.HeaderOriginalStream)
	}
	out := nats.Header{}
	for _, name := range deadContractHeaders {
		copyHeader(out, h, name)
	}
	for name, v := range h {
		if !contract.IsReserved(name) {
			out[name] = append([]string(nil), v...)
		}
	}
	if h.Get(contract.HeaderClaimKey) != "" {
		if !rp.ClaimReference {
			return nil, "", fmt.Errorf("%w (reason %s)", ErrNoBody, h.Get(contract.HeaderDeadReason))
		}
		copyHeader(out, h, contract.HeaderClaimBucket)
		copyHeader(out, h, contract.HeaderClaimKey)
	}
	out.Set(contract.HeaderReplay, rp.cfg.DeadLetter.Stream+":"+strconv.FormatUint(rec.Sequence, 10))
	subject := contract.RenderSubject(rp.cfg.ReplaySubject, consumerPlaceholder, consumer)
	return &nats.Msg{Subject: subject, Header: out, Data: rec.Data}, stream, nil
}

// replayReuseWithin is how old a stored replay body may be and still be reused by a repeated replay of the same
// record, the gateway's default reuse window; the bucket's TTL must cover the event stream's max_age plus it.
const replayReuseWithin = 24 * time.Hour

// fit moves a body too large for the event stream into the claim-check store, as the gateway does, and replaces
// it with a claim reference.
func (rp *Replayer) fit(ctx context.Context, msg *nats.Msg, stream, id string) error {
	if len(msg.Data) == 0 {
		return nil
	}
	st, err := rp.js.Stream(ctx, stream)
	if err != nil {
		return fmt.Errorf("stream %s: %w", stream, err)
	}
	limit := rp.js.Conn().MaxPayload()
	if m := int64(st.CachedInfo().Config.MaxMsgSize); m > 0 {
		limit = min(limit, m)
	}
	if int64(publishedSize(msg, stream, id)) <= limit {
		return nil
	}
	if rp.claims == nil {
		return fmt.Errorf("body of %d bytes exceeds what stream %s accepts inline and claim_check is not configured", len(msg.Data), stream)
	}
	if err = checkClaimBucket(ctx, rp.claims, st.CachedInfo().Config); err != nil {
		return err
	}
	h := msg.Header
	sum := h.Get(contract.HeaderBodySHA256)
	key := replayClaimKey(h.Get(contract.HeaderSource), h.Get(contract.HeaderReplay), sum, time.Now())
	ref, err := rp.claims.Put(ctx, key, msg.Data, sum)
	if err != nil {
		return err
	}
	h.Set(contract.HeaderClaimBucket, ref.Bucket)
	h.Set(contract.HeaderClaimKey, ref.Key)
	msg.Data = nil
	return nil
}

// replayClaimKey names the stored body of a replay:
// "<source>/replay/<dead-letter stream>-<seq>/<sha256hex>/<start of the current reuse window, unix seconds>".
// Replays of a record within one window share the object, so a repeated replay reuses it and its publish
// deduplicates; an object is therefore never reused once older than replayReuseWithin.
func replayClaimKey(source, marker, sha256hex string, now time.Time) string {
	window := int64(replayReuseWithin / time.Second)
	return source + "/replay/" + strings.Replace(marker, ":", "-", 1) + "/" + sha256hex + "/" + strconv.FormatInt(now.Unix()/window*window, 10)
}

// publishedSize is the size the broker checks against its limits: the body plus the encoded headers, including
// the message ID and expected stream the publish adds.
func publishedSize(msg *nats.Msg, stream, id string) int {
	h := nats.Header{}
	for k, v := range msg.Header {
		h[k] = v
	}
	h.Set(contract.HeaderMsgID, id)
	h.Set(jetstream.ExpectedStreamHeader, stream)
	m := &nats.Msg{Header: h, Data: msg.Data}
	return m.Size()
}
