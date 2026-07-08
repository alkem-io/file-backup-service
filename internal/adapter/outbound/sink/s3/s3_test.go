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
	bucketStatus int          // 200 = bucket present, 404 = gone
	bucketChecks atomic.Int32 // how many BucketExists HEADs arrived
}

func (s *s3Stub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(p, "/", 2)
	bucketLevel := len(parts) < 2 || parts[1] == "" // "bucket" or "bucket/" → BucketExists
	switch {
	case r.Method == http.MethodHead && bucketLevel:
		s.bucketChecks.Add(1)
		w.WriteHeader(s.bucketStatus)
	case r.Method == http.MethodHead: // StatObject → object always absent (404)
		w.WriteHeader(http.StatusNotFound)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func newStubSink(t *testing.T, bucketStatus int) (*Sink, *s3Stub) {
	t.Helper()
	stub := &s3Stub{bucketStatus: bucketStatus}
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

// TestExistsPresentBucketAbsentObject: a 404 with the bucket PRESENT is a genuinely absent
// object → (false, nil), and the bucket check is CACHED (one BucketExists per sink, not one
// per probe) across repeated Exists calls. (R6-1)
func TestExistsPresentBucketAbsentObject(t *testing.T) {
	sink, stub := newStubSink(t, http.StatusOK) // bucket present
	for i := 0; i < 5; i++ {
		present, err := sink.Exists(context.Background(), strings.Repeat("a", 64))
		if err != nil {
			t.Fatalf("probe %d: bucket present + object absent must be (false, nil), got err %v", i, err)
		}
		if present {
			t.Fatalf("probe %d: object must be reported absent", i)
		}
	}
	if n := stub.bucketChecks.Load(); n != 1 {
		t.Fatalf("BucketExists ran %d times, want exactly 1 (the verdict must be cached)", n)
	}
}
