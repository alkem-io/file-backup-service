package s3

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// s3Stub is a minimal S3-over-HTTP stub speaking just enough for minio-go's path-style
// requests (a 127.0.0.1 endpoint forces path style). It routes HEAD requests by path depth:
// a HEAD on /{bucket}/{key...} is a StatObject (object stat); a HEAD on /{bucket} or
// /{bucket}/ is a BucketExists. It always 404s the object (so Exists takes the confirm-bucket
// branch) and returns bucketStatus for the bucket HEAD, counting how many bucket checks ran.
type s3Stub struct {
	bucketStatus atomic.Int32 // 200 = bucket present, 404 = gone (atomic so a test can flip it mid-sweep)
	bucketChecks atomic.Int32 // how many BucketExists HEADs arrived
}

func (s *s3Stub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(p, "/", 2)
	bucketLevel := len(parts) < 2 || parts[1] == "" // "bucket" or "bucket/" → BucketExists
	switch {
	case r.Method == http.MethodHead && bucketLevel:
		s.bucketChecks.Add(1)
		w.WriteHeader(int(s.bucketStatus.Load()))
	case r.Method == http.MethodHead: // StatObject → object always absent (404)
		w.WriteHeader(http.StatusNotFound)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func newStubSink(t *testing.T, bucketStatus int) (*Sink, *s3Stub) {
	t.Helper()
	stub := &s3Stub{}
	stub.bucketStatus.Store(int32(bucketStatus)) //nolint:gosec // test constant status codes
	srv := httptest.NewServer(stub)
	t.Cleanup(srv.Close)
	sink, err := New(Config{
		Name:      "s3",
		Endpoint:  strings.TrimPrefix(srv.URL, "http://"), // host:port, path-style
		Region:    "us-east-1",
		Bucket:    "bkt",
		AccessKey: "AK", SecretKey: "SK",
		UseSSL: false,
	})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	return sink, stub
}

// TestExistsGoneBucketIsError: a StatObject 404 against a MISSING bucket must surface as an
// ERROR (so audit reports the target Unverifiable, not every object as silent Missing loss) —
// minio-go maps a HEAD 404 to NoSuchKey regardless of bucket-vs-object, so Exists confirms the
// bucket before ever reporting "absent". (R6-1)
func TestExistsGoneBucketIsError(t *testing.T) {
	sink, _ := newStubSink(t, http.StatusNotFound) // bucket gone
	present, err := sink.Exists(context.Background(), strings.Repeat("a", 64))
	if err == nil {
		t.Fatal("Exists against a gone bucket must return an error, not (absent, nil)")
	}
	if present {
		t.Fatal("Exists must not report present for a gone bucket")
	}
}

// TestExistsPresentBucketNotCached (re-review C5): a 404 with the bucket PRESENT is a genuinely
// absent object → (false, nil); the PRESENT verdict is NOT cached — it is re-checked on each
// absent-object probe so a bucket that vanishes MID-SWEEP is caught (later 404s → error, not false
// silent-loss), matching the filesystem sink's per-call confirmRoot. (Only the terminal GONE verdict
// is cached — see TestExistsGoneBucketCached.)
func TestExistsPresentBucketNotCached(t *testing.T) {
	sink, stub := newStubSink(t, http.StatusOK) // bucket present
	for i := 0; i < 3; i++ {
		present, err := sink.Exists(context.Background(), strings.Repeat("a", 64))
		if err != nil || present {
			t.Fatalf("probe %d: bucket present + object absent must be (false, nil), got present=%v err=%v", i, present, err)
		}
	}
	if n := stub.bucketChecks.Load(); n != 3 {
		t.Fatalf("BucketExists ran %d times, want 3 (a PRESENT bucket must NOT be cached, so a mid-sweep vanish is caught)", n)
	}
}

// TestExistsMidSweepBucketVanishCaught (re-review C5): a bucket present on the first probe but GONE on
// the next must surface the vanish as an ERROR (Unverifiable), not a cached "present" that reads the
// missing object as false silent-loss.
func TestExistsMidSweepBucketVanishCaught(t *testing.T) {
	sink, stub := newStubSink(t, http.StatusOK)
	if _, err := sink.Exists(context.Background(), strings.Repeat("a", 64)); err != nil {
		t.Fatalf("first probe (bucket present) must be clean, got %v", err)
	}
	stub.bucketStatus.Store(http.StatusNotFound) // the bucket vanishes mid-sweep
	if _, err := sink.Exists(context.Background(), strings.Repeat("a", 64)); err == nil {
		t.Fatal("a bucket that vanishes mid-sweep must surface as an error, not a cached present")
	}
}
