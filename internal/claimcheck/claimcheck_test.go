package claimcheck

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/mrrobertkent/hookgate/internal/testutil"
)

func setup(t *testing.T, cfg jetstream.ObjectStoreConfig) (jetstream.JetStream, jetstream.ObjectStore) {
	t.Helper()
	s := testutil.RunServer(t)
	_, js := testutil.Connect(t, s.ClientURL())
	if cfg.Bucket == "" {
		cfg.Bucket = "CLAIMS"
	}
	return js, testutil.CreateBucket(t, js, cfg)
}

func body(n int, seed byte) ([]byte, string) {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i*7)
	}
	sum := sha256.Sum256(b)
	return b, hex.EncodeToString(sum[:])
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

func streamMsgs(t *testing.T, js jetstream.JetStream, subject string) uint64 {
	t.Helper()
	st, err := js.Stream(ctx(t), "OBJ_CLAIMS")
	if err != nil {
		t.Fatal(err)
	}
	info, err := st.Info(ctx(t), jetstream.WithSubjectFilter(subject))
	if err != nil {
		t.Fatal(err)
	}
	var n uint64
	for _, c := range info.State.Subjects {
		n += c
	}
	return n
}

// Stock readers read what the store writes, across several chunks, and the metadata has the stock shape.
func TestWrittenObjectsReadWithStockClient(t *testing.T) {
	js, obs := setup(t, jetstream.ObjectStoreConfig{})
	s := NewObjectStore(js, "CLAIMS", ObjectStoreOptions{ReuseWithin: time.Hour, RequestTimeout: 5 * time.Second})
	b, sum := body(3*ChunkSize+17, 1)
	ref, err := s.Put(ctx(t), "src/evt_1/"+sum, b, sum)
	if err != nil {
		t.Fatal(err)
	}
	if ref != (Ref{Bucket: "CLAIMS", Key: "src/evt_1/" + sum}) {
		t.Fatalf("ref %+v", ref)
	}
	info, err := obs.GetInfo(ctx(t), ref.Key)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != uint64(len(b)) || info.Chunks != 4 || info.Opts == nil || info.Opts.ChunkSize != ChunkSize || info.Bucket != "CLAIMS" {
		t.Errorf("info %+v", info)
	}
	got, err := obs.GetBytes(ctx(t), ref.Key)
	if err != nil || !bytes.Equal(got, b) {
		t.Fatalf("stock GetBytes: %v (equal %v)", err, bytes.Equal(got, b))
	}
	if got, err := s.Get(ctx(t), ref); err != nil || !bytes.Equal(got, b) {
		t.Fatalf("Get: %v", err)
	}

	// The metadata carries exactly the keys stock Put writes.
	stock, _ := body(10, 9)
	if _, err := obs.PutBytes(ctx(t), "stock", stock); err != nil {
		t.Fatal(err)
	}
	if a, b := metaKeys(t, js, ref.Key), metaKeys(t, js, "stock"); a != b {
		t.Errorf("metadata keys %s, stock %s", a, b)
	}
}

func metaKeys(t *testing.T, js jetstream.JetStream, name string) string {
	t.Helper()
	st, _ := js.Stream(ctx(t), "OBJ_CLAIMS")
	s := NewObjectStore(js, "CLAIMS", ObjectStoreOptions{})
	m, err := st.GetLastMsgForSubject(ctx(t), s.metaSubject(name))
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(m.Data, &fields); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return fmt.Sprint(keys)
}

// An object a stock client stored is reused as it is.
func TestReusesStockWrittenObject(t *testing.T) {
	js, obs := setup(t, jetstream.ObjectStoreConfig{})
	b, sum := body(ChunkSize+1, 2)
	if _, err := obs.PutBytes(ctx(t), "src/"+sum, b); err != nil {
		t.Fatal(err)
	}
	before := streamMsgs(t, js, "$O.CLAIMS.>")
	s := NewObjectStore(js, "CLAIMS", ObjectStoreOptions{ReuseWithin: time.Hour})
	ref, err := s.Put(ctx(t), "src/"+sum, b, sum)
	if err != nil || ref.Key != "src/"+sum {
		t.Fatalf("Put = %+v, %v", ref, err)
	}
	if after := streamMsgs(t, js, "$O.CLAIMS.>"); after != before {
		t.Errorf("reuse wrote messages: %d -> %d", before, after)
	}
}

// A retry finds the existing object and leaves it unchanged.
func TestRetryReusesObjectUnchanged(t *testing.T) {
	js, obs := setup(t, jetstream.ObjectStoreConfig{})
	s := NewObjectStore(js, "CLAIMS", ObjectStoreOptions{ReuseWithin: time.Hour})
	b, sum := body(2*ChunkSize, 3)
	first, err := s.Put(ctx(t), "k", b, sum)
	if err != nil {
		t.Fatal(err)
	}
	info1, _ := obs.GetInfo(ctx(t), "k")
	msgs := streamMsgs(t, js, "$O.CLAIMS.>")
	second, err := s.Put(ctx(t), "k", b, sum)
	if err != nil || second != first {
		t.Fatalf("retry = %+v, %v", second, err)
	}
	info2, _ := obs.GetInfo(ctx(t), "k")
	if info1.NUID != info2.NUID || !info1.ModTime.Equal(info2.ModTime) || streamMsgs(t, js, "$O.CLAIMS.>") != msgs {
		t.Error("retry changed the stored object")
	}
}

// An object older than ReuseWithin is left alone and the body goes to a generation key.
func TestStaleObjectGetsGenerationKey(t *testing.T) {
	js, obs := setup(t, jetstream.ObjectStoreConfig{})
	s := NewObjectStore(js, "CLAIMS", ObjectStoreOptions{ReuseWithin: 50 * time.Millisecond})
	b, sum := body(100, 4)
	if _, err := s.Put(ctx(t), "k", b, sum); err != nil {
		t.Fatal(err)
	}
	old, _ := obs.GetInfo(ctx(t), "k")
	time.Sleep(100 * time.Millisecond)
	ref, err := s.Put(ctx(t), "k", b, sum)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(ref.Key, "k/") || len(ref.Key) < len("k/1700000000") {
		t.Fatalf("key %q", ref.Key)
	}
	if now, _ := obs.GetInfo(ctx(t), "k"); now.NUID != old.NUID {
		t.Error("stale object was replaced")
	}
	if got, err := s.Get(ctx(t), ref); err != nil || !bytes.Equal(got, b) {
		t.Errorf("Get generation: %v", err)
	}
}

// Concurrent puts of one key all succeed and agree on one object; the losers' chunks are orphans.
func TestConcurrentPutsCreateOneObject(t *testing.T) {
	js, obs := setup(t, jetstream.ObjectStoreConfig{})
	b, sum := body(ChunkSize/2, 5)
	var wg sync.WaitGroup
	refs := make([]Ref, 8)
	errs := make([]error, 8)
	for i := range refs {
		wg.Go(func() {
			s := NewObjectStore(js, "CLAIMS", ObjectStoreOptions{ReuseWithin: time.Hour})
			refs[i], errs[i] = s.Put(ctx(t), "race", b, sum)
		})
	}
	wg.Wait()
	for i := range refs {
		if errs[i] != nil || refs[i].Key != "race" {
			t.Errorf("put %d: %+v, %v", i, refs[i], errs[i])
		}
	}
	s := NewObjectStore(js, "CLAIMS", ObjectStoreOptions{})
	if n := streamMsgs(t, js, s.metaSubject("race")); n != 1 {
		t.Errorf("%d metadata messages", n)
	}
	if got, err := obs.GetBytes(ctx(t), "race"); err != nil || !bytes.Equal(got, b) {
		t.Errorf("GetBytes: %v", err)
	}
}

func TestGetErrors(t *testing.T) {
	js, _ := setup(t, jetstream.ObjectStoreConfig{})
	s := NewObjectStore(js, "CLAIMS", ObjectStoreOptions{ReuseWithin: time.Hour})
	if _, err := s.Get(ctx(t), Ref{Bucket: "CLAIMS", Key: "missing"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing: %v", err)
	}
	if _, err := s.Get(ctx(t), Ref{Bucket: "OTHER", Key: "k"}); !errors.Is(err, ErrWrongBucket) {
		t.Errorf("other bucket: %v", err)
	}
	// Chunks of one body under metadata that names another body's digest.
	b, _ := body(1000, 6)
	_, otherSum := body(1000, 7)
	other, _ := hex.DecodeString(otherSum)
	if err := s.write(ctx(t), "corrupt", object{body: b, digest: "SHA-256=" + base64.URLEncoding.EncodeToString(other)}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx(t), Ref{Bucket: "CLAIMS", Key: "corrupt"}); !errors.Is(err, ErrCorrupt) {
		t.Errorf("corrupt: %v", err)
	}
	if _, err := s.Put(ctx(t), "k", b, otherSum); err == nil {
		t.Error("Put accepted a body that does not match its SHA-256")
	}
}

func TestCheck(t *testing.T) {
	js, _ := setup(t, jetstream.ObjectStoreConfig{TTL: 16 * time.Hour, MaxBytes: 1 << 30})
	lim, err := NewObjectStore(js, "CLAIMS", ObjectStoreOptions{}).Check(ctx(t))
	if err != nil || lim.TTL != 16*time.Hour || lim.MaxBytes != 1<<30 {
		t.Errorf("Check = %+v, %v", lim, err)
	}
	if _, err := NewObjectStore(js, "NOPE", ObjectStoreOptions{}).Check(ctx(t)); !errors.Is(err, ErrBucketNotFound) {
		t.Errorf("missing bucket: %v", err)
	}
	b, sum := body(10, 1)
	if _, err := NewObjectStore(js, "NOPE", ObjectStoreOptions{}).Put(ctx(t), "k", b, sum); !errors.Is(err, ErrBucketNotFound) {
		t.Errorf("Put to missing bucket: %v", err)
	}
}

// Objects expire with the bucket TTL; nothing else removes them.
func TestObjectsExpireByTTL(t *testing.T) {
	js, _ := setup(t, jetstream.ObjectStoreConfig{TTL: time.Second})
	s := NewObjectStore(js, "CLAIMS", ObjectStoreOptions{ReuseWithin: time.Hour})
	b, sum := body(ChunkSize+5, 8)
	ref, err := s.Put(ctx(t), "k", b, sum)
	if err != nil {
		t.Fatal(err)
	}
	testutil.Eventually(t, 10*time.Second, func() bool { return streamMsgs(t, js, "$O.CLAIMS.>") == 0 }, "object did not expire")
	if _, err := s.Get(ctx(t), ref); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after expiry: %v", err)
	}
}

// A failed write publishes no metadata, so no reader ever sees a partial object.
func TestFailedWriteLeavesNoMetadata(t *testing.T) {
	js, _ := setup(t, jetstream.ObjectStoreConfig{})
	s := NewObjectStore(js, "CLAIMS", ObjectStoreOptions{ReuseWithin: time.Hour})
	b, sum := body(3*ChunkSize, 9)
	if _, err := s.Check(ctx(t)); err != nil {
		t.Fatal(err)
	}
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Put(cctx, "k", b, sum); err == nil {
		t.Fatal("Put with a cancelled context succeeded")
	}
	if n := streamMsgs(t, js, s.metaSubject("k")); n != 0 {
		t.Errorf("%d metadata messages after a failed write", n)
	}
}

// bucketStream creates OBJ_CLAIMS directly, as a bucket with non-default settings.
func bucketStream(t *testing.T, mutate func(*jetstream.StreamConfig)) jetstream.JetStream {
	t.Helper()
	s := testutil.RunServer(t)
	_, js := testutil.Connect(t, s.ClientURL())
	cfg := jetstream.StreamConfig{
		Name: "OBJ_CLAIMS", Subjects: []string{"$O.CLAIMS.C.>", "$O.CLAIMS.M.>"},
		Discard: jetstream.DiscardNew, AllowRollup: true, AllowDirect: true,
	}
	mutate(&cfg)
	testutil.CreateStream(t, js, cfg)
	return js
}

func TestCheckRejectsUnsuitableBuckets(t *testing.T) {
	for name, mutate := range map[string]func(*jetstream.StreamConfig){
		"interest retention":   func(c *jetstream.StreamConfig) { c.Retention = jetstream.InterestPolicy },
		"discard old":          func(c *jetstream.StreamConfig) { c.Discard = jetstream.DiscardOld },
		"one message per subj": func(c *jetstream.StreamConfig) { c.MaxMsgsPerSubject = 1 },
		"no direct get":        func(c *jetstream.StreamConfig) { c.AllowDirect = false },
		"no acks":              func(c *jetstream.StreamConfig) { c.NoAck = true },
	} {
		js := bucketStream(t, mutate)
		if _, err := NewObjectStore(js, "CLAIMS", ObjectStoreOptions{}).Check(ctx(t)); !errors.Is(err, ErrBucketUnsuitable) {
			t.Errorf("%s: Check = %v", name, err)
		}
	}
	js := bucketStream(t, func(*jetstream.StreamConfig) {})
	if _, err := NewObjectStore(js, "CLAIMS", ObjectStoreOptions{}).Check(ctx(t)); err != nil {
		t.Errorf("suitable bucket: %v", err)
	}
}

// Metadata that counts more chunks than are stored is not reused; the body goes to a generation key.
func TestIncompleteObjectIsNotReused(t *testing.T) {
	js, _ := setup(t, jetstream.ObjectStoreConfig{})
	s := NewObjectStore(js, "CLAIMS", ObjectStoreOptions{ReuseWithin: time.Hour})
	b, sum := body(2*ChunkSize, 10)
	if _, err := s.Check(ctx(t)); err != nil {
		t.Fatal(err)
	}
	// One chunk stored, metadata claiming two.
	id := "partialnuid"
	if err := s.publish(ctx(t), &nats.Msg{Subject: s.chunkSubject(id), Data: b[:ChunkSize]}); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(b)
	meta, _ := json.Marshal(jetstream.ObjectInfo{ObjectMeta: jetstream.ObjectMeta{Name: "k"}, Bucket: "CLAIMS", NUID: id,
		Size: uint64(len(b)), Chunks: 2, Digest: "SHA-256=" + base64.URLEncoding.EncodeToString(digest[:])})
	if err := s.publish(ctx(t), &nats.Msg{Subject: s.metaSubject("k"), Data: meta}); err != nil {
		t.Fatal(err)
	}
	ref, err := s.Put(ctx(t), "k", b, sum)
	if err != nil {
		t.Fatal(err)
	}
	if ref.Key == "k" {
		t.Fatal("reused an incomplete object")
	}
	if got, err := s.Get(ctx(t), ref); err != nil || !bytes.Equal(got, b) {
		t.Errorf("Get generation: %v", err)
	}
}

// Within one ReuseWithin window, retries after the base object went stale reuse one generation.
func TestGenerationIsReusedWithinWindow(t *testing.T) {
	js, _ := setup(t, jetstream.ObjectStoreConfig{})
	s := NewObjectStore(js, "CLAIMS", ObjectStoreOptions{ReuseWithin: time.Hour})
	b, sum := body(100, 11)
	if _, err := s.Check(ctx(t)); err != nil {
		t.Fatal(err)
	}
	// A base object that is not reusable: same name, different content.
	other, _ := body(100, 12)
	if err := s.write(ctx(t), "k", object{body: other, digest: "SHA-256=x"}); err != nil {
		t.Fatal(err)
	}
	first, err := s.Put(ctx(t), "k", b, sum)
	if err != nil || first.Key != s.generationKey("k") {
		t.Fatalf("first = %+v, %v", first, err)
	}
	msgs := streamMsgs(t, js, "$O.CLAIMS.>")
	second, err := s.Put(ctx(t), "k", b, sum)
	if err != nil || second != first || streamMsgs(t, js, "$O.CLAIMS.>") != msgs {
		t.Errorf("retry = %+v, %v; wrote again: %v", second, err, streamMsgs(t, js, "$O.CLAIMS.>") != msgs)
	}
}
