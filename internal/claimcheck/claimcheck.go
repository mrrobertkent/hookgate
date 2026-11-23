// Package claimcheck stores event bodies too large to publish inline and fetches them back by reference.
package claimcheck

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nuid"
)

var (
	// ErrNotFound reports that the referenced object does not exist (never stored, or expired).
	ErrNotFound = errors.New("claim check: object not found")
	// ErrCorrupt reports that the stored bytes do not match the object's digest.
	ErrCorrupt = errors.New("claim check: object does not match its digest")
	// ErrBucketNotFound reports that the configured bucket does not exist.
	ErrBucketNotFound = errors.New("claim check: bucket not found")
	// ErrWrongBucket reports a reference to a bucket other than the store's.
	ErrWrongBucket = errors.New("claim check: reference names another bucket")
	// ErrBucketUnsuitable reports a bucket whose stream could drop or evict stored chunks, or that a reader
	// limited to direct gets cannot read.
	ErrBucketUnsuitable = errors.New("claim check: bucket unsuitable")
)

// Ref locates a stored body.
type Ref struct {
	Bucket string
	Key    string
}

// Limits are the store's limits that callers check their own settings against.
type Limits struct {
	MaxBytes   int64         // total bytes the store holds; <= 0 is unlimited
	TTL        time.Duration // age at which objects expire; 0 is never
	MaxMsgSize int32         // largest single stored message; <= 0 is unlimited
}

// Store keeps bodies for claim-checked events.
type Store interface {
	// Put stores body under key unless an identical, recent object already exists there, and returns where the
	// body is. It never overwrites or deletes an object.
	Put(ctx context.Context, key string, body []byte, sha256hex string) (Ref, error)
	// Get returns the body ref names, verified against its stored digest.
	Get(ctx context.Context, ref Ref) ([]byte, error)
	// Check binds to the store and returns its limits.
	Check(ctx context.Context) (Limits, error)
}

// ChunkSize is the size of each stored chunk, the Object Store default.
const ChunkSize = 128 << 10

// ObjectStoreOptions tune an ObjectStore.
type ObjectStoreOptions struct {
	// ReuseWithin is the age up to which an existing identical object is reused instead of written again.
	ReuseWithin time.Duration
	// RequestTimeout bounds each JetStream request; 0 leaves only the caller's context.
	RequestTimeout time.Duration
}

// ObjectStore is a Store on a JetStream Object Store bucket. It writes the Object Store wire format itself, so
// that an object becomes visible only once every chunk is stored and an existing object is never replaced;
// stock Object Store clients read what it writes.
type ObjectStore struct {
	js     jetstream.JetStream
	bucket string
	opts   ObjectStoreOptions

	mu     sync.Mutex
	stream jetstream.Stream
	reader jetstream.ObjectStore
}

// NewObjectStore returns a store on bucket. It makes no request until used.
func NewObjectStore(js jetstream.JetStream, bucket string, opts ObjectStoreOptions) *ObjectStore {
	return &ObjectStore{js: js, bucket: bucket, opts: opts}
}

func (s *ObjectStore) streamName() string            { return "OBJ_" + s.bucket }
func (s *ObjectStore) chunkSubject(id string) string { return "$O." + s.bucket + ".C." + id }
func (s *ObjectStore) metaSubject(name string) string {
	return "$O." + s.bucket + ".M." + base64.URLEncoding.EncodeToString([]byte(name))
}

// Check binds to the bucket's stream and returns its limits.
func (s *ObjectStore) Check(ctx context.Context) (Limits, error) {
	ctx, cancel := s.request(ctx)
	defer cancel()
	st, err := s.js.Stream(ctx, s.streamName())
	if err != nil {
		if errors.Is(err, jetstream.ErrStreamNotFound) {
			return Limits{}, fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
		}
		return Limits{}, err
	}
	c := st.CachedInfo().Config
	if err := suitable(c); err != nil {
		return Limits{}, fmt.Errorf("%w: %s: %w", ErrBucketUnsuitable, s.bucket, err)
	}
	s.mu.Lock()
	s.stream = st
	s.mu.Unlock()
	return Limits{MaxBytes: c.MaxBytes, TTL: c.MaxAge, MaxMsgSize: c.MaxMsgSize}, nil
}

// suitable rejects stream settings under which an acknowledged chunk can disappear before the bucket TTL, and
// buckets without direct get, which the store's lookups use.
func suitable(c jetstream.StreamConfig) error {
	switch {
	case c.NoAck:
		return errors.New("no_ack must be false")
	case c.Retention != jetstream.LimitsPolicy:
		return errors.New("retention must be limits")
	case c.Discard != jetstream.DiscardNew:
		return errors.New("discard must be new")
	case c.MaxMsgsPerSubject > 0:
		return errors.New("max_msgs_per_subject must be unlimited")
	case !c.AllowDirect:
		return errors.New("allow_direct must be true")
	}
	return nil
}

func (s *ObjectStore) bound(ctx context.Context) (jetstream.Stream, error) {
	s.mu.Lock()
	st := s.stream
	s.mu.Unlock()
	if st != nil {
		return st, nil
	}
	if _, err := s.Check(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stream, nil
}

func (s *ObjectStore) request(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.opts.RequestTimeout > 0 {
		return context.WithTimeout(ctx, s.opts.RequestTimeout)
	}
	return context.WithCancel(ctx)
}

// object is what a Put stores.
type object struct {
	body   []byte
	digest string // Object Store digest form: "SHA-256=" + base64url
}

// Put stores body under key. An existing object there is reused when it is complete, holds the same bytes and
// its first chunk is no older than ReuseWithin. An existing object that is not reusable leaves key untouched and
// the body goes to the generation key "<key>/<n>", where n numbers the ReuseWithin-long window of the current
// time, so retries within one window find the same generation.
func (s *ObjectStore) Put(ctx context.Context, key string, body []byte, sha256hex string) (Ref, error) {
	if len(body) == 0 {
		return Ref{}, errors.New("claim check: empty body")
	}
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != sha256hex {
		return Ref{}, errors.New("claim check: body does not match its SHA-256")
	}
	st, err := s.bound(ctx)
	if err != nil {
		return Ref{}, err
	}
	o := object{body: body, digest: "SHA-256=" + base64.URLEncoding.EncodeToString(sum[:])}
	for _, name := range []string{key, s.generationKey(key)} {
		done, err := s.reuseOrCreate(ctx, st, name, o)
		if err != nil {
			return Ref{}, err
		}
		if done {
			return Ref{Bucket: s.bucket, Key: name}, nil
		}
	}
	return Ref{}, fmt.Errorf("claim check: objects under %s exist and are not reusable", key)
}

func (s *ObjectStore) generationKey(key string) string {
	window := max(int64(s.opts.ReuseWithin/time.Second), 1)
	return fmt.Sprintf("%s/%d", key, time.Now().Unix()/window)
}

// reuseOrCreate reports done when name holds o afterwards, and false when name holds something else.
func (s *ObjectStore) reuseOrCreate(ctx context.Context, st jetstream.Stream, name string, o object) (bool, error) {
	info, found, err := s.lookup(ctx, st, name)
	if err != nil {
		return false, err
	}
	if found {
		return s.reusable(ctx, st, info, o)
	}
	err = s.write(ctx, name, o)
	if err == nil {
		return true, nil
	}
	if !isConflict(err) {
		return false, err
	}
	// Another writer created name first; its object is as good as ours if it holds the same bytes. Direct gets
	// may be served by a replica that has not applied the winner's metadata yet.
	for wait := 25 * time.Millisecond; ; wait *= 2 {
		info, found, err = s.lookup(ctx, st, name)
		if err != nil || found || wait > 200*time.Millisecond {
			break
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(wait):
		}
	}
	if err != nil {
		return false, err
	}
	if ok, err := s.reusable(ctx, st, info, o); err != nil || ok {
		return ok, err
	}
	return false, fmt.Errorf("claim check: object %s was created concurrently and is not reusable", name)
}

// lookup returns the current metadata of name. Metadata that does not parse is found with a nil info.
func (s *ObjectStore) lookup(ctx context.Context, st jetstream.Stream, name string) (*jetstream.ObjectInfo, bool, error) {
	rctx, cancel := s.request(ctx)
	defer cancel()
	m, err := st.GetLastMsgForSubject(rctx, s.metaSubject(name))
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var info jetstream.ObjectInfo
	if json.Unmarshal(m.Data, &info) != nil {
		return nil, true, nil
	}
	return &info, true, nil
}

// reusable reports whether info describes a complete copy of o whose first chunk is recent enough: every chunk
// the metadata counts is still stored, and size and digest match.
func (s *ObjectStore) reusable(ctx context.Context, st jetstream.Stream, info *jetstream.ObjectInfo, o object) (bool, error) {
	if info == nil || info.Deleted || info.NUID == "" || info.Chunks == 0 ||
		info.Size != uint64(len(o.body)) || info.Digest != o.digest {
		return false, nil
	}
	subj := s.chunkSubject(info.NUID)
	rctx, cancel := s.request(ctx)
	defer cancel()
	first, err := st.GetMsg(rctx, 1, jetstream.WithGetMsgSubject(subj))
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if time.Since(first.Time) > s.opts.ReuseWithin {
		return false, nil
	}
	si, err := st.Info(rctx, jetstream.WithSubjectFilter(subj))
	if err != nil {
		return false, err
	}
	return si.State.Subjects[subj] == uint64(info.Chunks), nil
}

// write stores o as name: every chunk with its own acknowledged publish, then the metadata, created only if
// name has no metadata yet. A failure leaves only chunks that no metadata names; they expire with the bucket TTL.
func (s *ObjectStore) write(ctx context.Context, name string, o object) error {
	id := nuid.Next()
	subj := s.chunkSubject(id)
	chunks := uint32(0)
	for off := 0; off < len(o.body); off += ChunkSize {
		end := min(off+ChunkSize, len(o.body))
		if err := s.publish(ctx, &nats.Msg{Subject: subj, Data: o.body[off:end]}); err != nil {
			return fmt.Errorf("claim check: store chunk: %w", err)
		}
		chunks++
	}
	info := jetstream.ObjectInfo{
		ObjectMeta: jetstream.ObjectMeta{Name: name, Opts: &jetstream.ObjectMetaOptions{ChunkSize: ChunkSize}},
		Bucket:     s.bucket,
		NUID:       id,
		Size:       uint64(len(o.body)),
		Chunks:     chunks,
		Digest:     o.digest,
	}
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	if err := s.publish(ctx, &nats.Msg{Subject: s.metaSubject(name), Data: data}, jetstream.WithExpectLastSequencePerSubject(0)); err != nil {
		return fmt.Errorf("claim check: store metadata: %w", err)
	}
	return nil
}

func (s *ObjectStore) publish(ctx context.Context, m *nats.Msg, opts ...jetstream.PublishOpt) error {
	rctx, cancel := s.request(ctx)
	defer cancel()
	_, err := s.js.PublishMsg(rctx, m, append(opts, jetstream.WithExpectStream(s.streamName()))...)
	return err
}

func isConflict(err error) bool {
	var apiErr *jetstream.APIError
	return errors.As(err, &apiErr) &&
		(apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence || apiErr.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequenceConstant)
}

// Get fetches and verifies a stored body with the stock Object Store reader.
func (s *ObjectStore) Get(ctx context.Context, ref Ref) ([]byte, error) {
	if ref.Bucket != s.bucket {
		return nil, fmt.Errorf("%w: %q, not %q", ErrWrongBucket, ref.Bucket, s.bucket)
	}
	r, err := s.objectStore(ctx)
	if err != nil {
		return nil, err
	}
	b, err := r.GetBytes(ctx, ref.Key)
	switch {
	case errors.Is(err, jetstream.ErrObjectNotFound):
		return nil, fmt.Errorf("%w: %s", ErrNotFound, ref.Key)
	case errors.Is(err, jetstream.ErrDigestMismatch):
		return nil, fmt.Errorf("%w: %s", ErrCorrupt, ref.Key)
	case err != nil:
		return nil, err
	}
	return b, nil
}

func (s *ObjectStore) objectStore(ctx context.Context) (jetstream.ObjectStore, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reader != nil {
		return s.reader, nil
	}
	rctx, cancel := s.request(ctx)
	defer cancel()
	r, err := s.js.ObjectStore(rctx, s.bucket)
	if errors.Is(err, jetstream.ErrBucketNotFound) {
		return nil, fmt.Errorf("%w: %s", ErrBucketNotFound, s.bucket)
	}
	if err != nil {
		return nil, err
	}
	s.reader = r
	return r, nil
}
