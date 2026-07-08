package s3

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// s3ErrStub is a distinct S3-over-HTTP stub for the Exists/confirmBucket error paths. It routes
// HEADs by path depth (like s3_test.go's s3Stub) but returns a CONFIGURABLE status for both the
// object HEAD (StatObject) and the bucket HEAD (BucketExists), so a test can drive an object
// 200/403 and a transient (non-404) bucket fault. It counts bucket HEADs to prove the caching
// policy (a transient bucket error must NOT be cached).
type s3ErrStub struct {
	objStatus    int          // StatObject: 200 present, 403 denied, 404 absent
	bucketStatus int          // BucketExists: 200 present, 404 gone, 5xx/403 transient
	bucketChecks atomic.Int32 // how many BucketExists HEADs arrived
}

func (s *s3ErrStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(p, "/", 2)
	bucketLevel := len(parts) < 2 || parts[1] == ""
	switch {
	case r.Method == http.MethodHead && bucketLevel:
		s.bucketChecks.Add(1)
		w.WriteHeader(s.bucketStatus)
	case r.Method == http.MethodHead: // StatObject
		if s.objStatus == http.StatusOK {
			// StatObject parses these headers into ObjectInfo; a missing Last-Modified errors.
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.Header().Set("ETag", `"obj-etag"`)
			w.Header().Set("Content-Length", "0")
		}
		w.WriteHeader(s.objStatus)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func newErrStubSink(t *testing.T, objStatus, bucketStatus int) (*Sink, *s3ErrStub) {
	t.Helper()
	stub := &s3ErrStub{objStatus: objStatus, bucketStatus: bucketStatus}
	srv := httptest.NewServer(stub)
	t.Cleanup(srv.Close)
	sink, err := New(Config{
		Name:      "s3err",
		Endpoint:  strings.TrimPrefix(srv.URL, "http://"),
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

// TestExistsObjectPresent: a StatObject 200 means the object is present, so Exists returns
// (true, nil) WITHOUT ever needing a bucket check (a present object proves the bucket exists).
func TestExistsObjectPresent(t *testing.T) {
	sink, stub := newErrStubSink(t, http.StatusOK, http.StatusOK)
	present, err := sink.Exists(context.Background(), strings.Repeat("a", 64))
	if err != nil {
		t.Fatalf("a present object must be (true, nil), got err %v", err)
	}
	if !present {
		t.Fatal("a StatObject 200 must report the object present")
	}
	if n := stub.bucketChecks.Load(); n != 0 {
		t.Fatalf("a present object must not trigger a bucket check, got %d", n)
	}
}

// TestExistsAccessDeniedIsError: a 403/AccessDenied on StatObject (a PutObject-only WORM
// credential, or a real permission/endpoint fault) must surface as an ERROR, not "absent" — so
// reconcile never mistakes a permission fault for a gap to refill. The bucket is never consulted
// (a 403 is not a 404, so the confirm-bucket branch is skipped).
func TestExistsAccessDeniedIsError(t *testing.T) {
	sink, stub := newErrStubSink(t, http.StatusForbidden, http.StatusOK)
	present, err := sink.Exists(context.Background(), strings.Repeat("a", 64))
	if err == nil {
		t.Fatal("a 403 on Exists must return an error, not (absent, nil)")
	}
	if present {
		t.Fatal("a 403 must not report the object present")
	}
	if n := stub.bucketChecks.Load(); n != 0 {
		t.Fatalf("a 403 (not a 404) must not trigger the confirm-bucket branch, got %d checks", n)
	}
}

// TestConfirmBucketTransientNotCached: on an object 404 with a TRANSIENT bucket fault (a non-404
// BucketExists error), Exists errors (fail-closed — it can't confirm the bucket) AND does NOT
// cache the verdict, so a later probe re-checks. Two probes therefore issue two bucket HEADs.
func TestConfirmBucketTransientNotCached(t *testing.T) {
	// A 403 on the bucket HEAD is a non-404 (transient/permission) fault minio-go does NOT
	// retry — distinct from a 5xx, which minio-go retries internally and would inflate the count.
	sink, stub := newErrStubSink(t, http.StatusNotFound, http.StatusForbidden)
	for i := 0; i < 2; i++ {
		present, err := sink.Exists(context.Background(), strings.Repeat("a", 64))
		if err == nil {
			t.Fatalf("probe %d: a transient bucket fault must fail-closed (error), not (absent, nil)", i)
		}
		if present {
			t.Fatalf("probe %d: a transient bucket fault must not report present", i)
		}
	}
	if n := stub.bucketChecks.Load(); n != 2 {
		t.Fatalf("a transient bucket error must NOT be cached: want 2 re-checks, got %d", n)
	}
}

// TestFetchGetErrorWraps: a GetObject that fails synchronously (an empty object key fails
// minio-go's name validation before any request) surfaces as a wrapped Fetch error rather than
// a nil reader — the restore/reconcile read path must see the fault, not a silent empty stream.
func TestFetchGetErrorWraps(t *testing.T) {
	sink, err := New(Config{
		Name:      "s3fetch",
		Endpoint:  "127.0.0.1:9000",
		Region:    "us-east-1",
		Bucket:    "bkt",
		AccessKey: "AK", SecretKey: "SK",
	})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	if _, ferr := sink.Fetch(context.Background(), ""); ferr == nil {
		t.Fatal("Fetch with an invalid (empty) object key must return a wrapped GetObject error")
	}
}
