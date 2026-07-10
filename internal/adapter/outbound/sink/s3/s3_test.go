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

// TestExistsPresentBucketCached (root cause B): a 404 with the bucket PRESENT is a genuinely absent
// object → (false, nil); the PRESENT verdict is CACHED, so a burst of absent-object probes (a
// silent-loss audit sample) collapses to a SINGLE BucketExists instead of one HEAD each. Caching
// present is what removes the bucketMu lock-contention that previously flipped a genuinely-MISSING
// object to `errored` — which masked real silent loss as Unverifiable (a benign pass on a WORM
// target). (The terminal GONE verdict is likewise cached — see TestExistsGoneBucketCached.)
func TestExistsPresentBucketCached(t *testing.T) {
	sink, stub := newStubSink(t, http.StatusOK) // bucket present
	for i := 0; i < 3; i++ {
		present, err := sink.Exists(context.Background(), strings.Repeat("a", 64))
		if err != nil || present {
			t.Fatalf("probe %d: bucket present + object absent must be (false, nil), got present=%v err=%v", i, present, err)
		}
	}
	if n := stub.bucketChecks.Load(); n != 1 {
		t.Fatalf("BucketExists ran %d times, want 1 (a PRESENT bucket is cached, collapsing the per-probe HEAD storm)", n)
	}
}

// TestExistsPresentCachedThenVanishReadsAbsent (root cause B): once the bucket is confirmed present
// (and cached), a later mid-sweep vanish is NOT re-detected — the absent object reads as (false, nil).
// This is the SAFE direction: the audit layer counts it `missing` → Drift, which PAGES; the old
// no-cache behavior's lock-contention could instead flip missing→errored→Unverifiable (a SILENT WORM
// pass). A gone bucket also fails the write path loudly, and a worker restart re-checks.
func TestExistsPresentCachedThenVanishReadsAbsent(t *testing.T) {
	sink, stub := newStubSink(t, http.StatusOK)
	if _, err := sink.Exists(context.Background(), strings.Repeat("a", 64)); err != nil {
		t.Fatalf("first probe (bucket present) must be clean, got %v", err)
	}
	stub.bucketStatus.Store(http.StatusNotFound) // the bucket vanishes AFTER the present verdict is cached
	present, err := sink.Exists(context.Background(), strings.Repeat("a", 64))
	if err != nil || present {
		t.Fatalf("a cached-present bucket reads a later absent object as (false, nil) — the safe direction (→ Drift/missing, pages); got present=%v err=%v", present, err)
	}
	if n := stub.bucketChecks.Load(); n != 1 {
		t.Fatalf("BucketExists ran %d times, want 1 (present cached — no re-check)", n)
	}
}
