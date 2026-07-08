package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"testing"
	"time"
)

// errReader fails on the first read with a non-EOF error, to exercise putStream's
// upstream-read failure path.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("upstream read failed") }

// storeStub is an independent S3-over-HTTP stub for the write/read paths (Store,
// PutManifest, Fetch, Preflight). It is deliberately separate from s3_test.go's
// s3Stub — that one models only the Exists/BucketExists HEAD paths. storeStub speaks
// enough of the S3 REST API for minio-go's path-style requests (a 127.0.0.1 endpoint
// forces path style): a single PutObject (PUT), the multipart trio a size=-1 stream
// uses (POST ?uploads → initiate, PUT ?partNumber → part, POST ?uploadId → complete),
// and GetObject (GET). It records what arrived so tests can assert the routing.
type storeStub struct {
	mu sync.Mutex

	getStatus      int    // GET: 200 returns getBody; anything else writes an S3 error
	getBody        []byte // GET body when getStatus == 200
	putStatus      int    // status for a single-object PutObject (empty/manifest/preflight)
	initiateStatus int    // status for POST ?uploads; 0 or 200 => success

	singlePuts []string // object keys received via a single PUT (no ?partNumber)
	initiates  []string // object keys received via POST ?uploads (multipart initiate)
	partPuts   int      // number of PUT ?partNumber requests
	completes  int      // number of POST ?uploadId (complete) requests
	gets       []string // object keys received via GET
}

func (s *storeStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Drain the request body first so a streaming-signature part upload completes
	// cleanly before we respond (GET bodies are empty, so this is a no-op there).
	_, _ = io.Copy(io.Discard, r.Body)

	p := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(p, "/", 2)
	bucket := parts[0]
	key := ""
	if len(parts) == 2 {
		key = parts[1]
	}
	q := r.URL.Query()

	s.mu.Lock()
	defer s.mu.Unlock()

	switch {
	case r.Method == http.MethodPost && q.Has("uploads"): // initiate multipart
		s.initiates = append(s.initiates, key)
		if s.initiateStatus != 0 && s.initiateStatus != http.StatusOK {
			writeStatusOrS3Error(w, s.initiateStatus, "AccessDenied")
			return
		}
		writeXML(w, `<InitiateMultipartUploadResult><Bucket>`+bucket+`</Bucket><Key>`+key+
			`</Key><UploadId>test-upload-id</UploadId></InitiateMultipartUploadResult>`)
	case r.Method == http.MethodPost && q.Get("uploadId") != "": // complete multipart
		s.completes++
		writeXML(w, `<CompleteMultipartUploadResult><Location>http://x/`+key+`</Location><Bucket>`+
			bucket+`</Bucket><Key>`+key+`</Key><ETag>"complete-etag"</ETag></CompleteMultipartUploadResult>`)
	case r.Method == http.MethodPut && q.Get("partNumber") != "": // upload part
		s.partPuts++
		w.Header().Set("ETag", `"part-etag"`)
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPut: // single-object PutObject
		s.singlePuts = append(s.singlePuts, key)
		if s.putStatus == http.StatusOK {
			w.Header().Set("ETag", `"obj-etag"`)
		}
		writeStatusOrS3Error(w, s.putStatus, "AccessDenied")
	case r.Method == http.MethodGet: // GetObject
		s.gets = append(s.gets, key)
		if s.getStatus == http.StatusOK {
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			w.Header().Set("ETag", `"obj-etag"`)
			_, _ = w.Write(s.getBody) // implicit 200 + auto Content-Length
			return
		}
		writeStatusOrS3Error(w, s.getStatus, "NoSuchKey")
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// writeXML writes a 200 XML response (the standard S3 envelope prefix + body).
func writeXML(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/xml")
	_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?>`+body) //nolint:gosec // test stub writes test-controlled XML
}

// writeStatusOrS3Error writes a bare 200, or the given status with an S3 <Error> body
// so minio-go surfaces a proper error.
func writeStatusOrS3Error(w http.ResponseWriter, status int, code string) {
	if status == http.StatusOK {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>`+code+
		`</Code><Message>`+code+`</Message></Error>`)
}

func newStoreSink(t *testing.T, stub *storeStub, prefix string) *Sink {
	t.Helper()
	srv := httptest.NewServer(stub)
	t.Cleanup(srv.Close)
	sink, err := New(Config{
		Name:      "s3store",
		Endpoint:  strings.TrimPrefix(srv.URL, "http://"), // host:port, path-style
		Region:    "us-east-1",
		Bucket:    "bkt",
		Prefix:    prefix,
		AccessKey: "AK", SecretKey: "SK",
		UseSSL: false,
	})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	return sink
}

// TestStoreNonEmptyUsesMultipart: a non-empty object streams with size=-1, which
// minio-go uploads as a multipart (POST initiate → PUT part → POST complete), and
// Store returns the real byte count read from the stream. The object routes to the
// two-level sharded key. (S1)
func TestStoreNonEmptyUsesMultipart(t *testing.T) {
	stub := &storeStub{putStatus: http.StatusOK}
	sink := newStoreSink(t, stub, "")
	data := []byte("backup me now") // 13 bytes, non-empty

	n, err := sink.Store(context.Background(), strings.Repeat("a", 64), bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if n != int64(len(data)) {
		t.Fatalf("Store returned %d bytes, want %d", n, len(data))
	}

	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.initiates) != 1 || stub.partPuts < 1 || stub.completes != 1 {
		t.Fatalf("non-empty Store must use the multipart trio: initiates=%d parts=%d completes=%d",
			len(stub.initiates), stub.partPuts, stub.completes)
	}
	if len(stub.singlePuts) != 0 {
		t.Fatalf("a size=-1 Store must not fall back to a single PutObject, got %v", stub.singlePuts)
	}
	wantKey := path.Join("aa", "aa", strings.Repeat("a", 64))
	if stub.initiates[0] != wantKey {
		t.Fatalf("multipart key = %q, want sharded %q", stub.initiates[0], wantKey)
	}
}

// TestStoreEmptyUsesSinglePut: a 0-byte object takes putStream's empty branch — one
// single PutObject, never a multipart (an empty multipart part is rejected by many S3
// backends) — and Store reports 0 bytes. (S1)
func TestStoreEmptyUsesSinglePut(t *testing.T) {
	stub := &storeStub{putStatus: http.StatusOK}
	sink := newStoreSink(t, stub, "")

	n, err := sink.Store(context.Background(), strings.Repeat("b", 64), bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("Store(empty): %v", err)
	}
	if n != 0 {
		t.Fatalf("empty Store must report 0 bytes, got %d", n)
	}

	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.singlePuts) != 1 {
		t.Fatalf("empty Store must do exactly one single PutObject, got %d", len(stub.singlePuts))
	}
	if len(stub.initiates) != 0 || stub.partPuts != 0 {
		t.Fatalf("empty Store must NOT open a multipart upload: initiates=%d parts=%d",
			len(stub.initiates), stub.partPuts)
	}
}

// TestFetchReturnsObjectBytes: a GET that serves object bytes yields a reader with
// exactly those bytes. (S1)
func TestFetchReturnsObjectBytes(t *testing.T) {
	want := []byte("stored object contents")
	stub := &storeStub{getStatus: http.StatusOK, getBody: want}
	sink := newStoreSink(t, stub, "")

	rc, err := sink.Fetch(context.Background(), strings.Repeat("a", 64))
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read fetched object: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Fetch bytes = %q, want %q", got, want)
	}
}

// TestFetchMissingObjectErrors: a GET 404 surfaces as an error. minio-go's GetObject
// is lazy — the request is issued on first read — so the error may appear either at
// the Fetch call or when the returned reader is consumed; both are accepted. (S1)
func TestFetchMissingObjectErrors(t *testing.T) {
	stub := &storeStub{getStatus: http.StatusNotFound}
	sink := newStoreSink(t, stub, "")

	rc, err := sink.Fetch(context.Background(), strings.Repeat("a", 64))
	if err == nil { // lazy GET: error surfaces on read
		defer func() { _ = rc.Close() }()
		if _, rerr := io.ReadAll(rc); rerr == nil {
			t.Fatal("reading a 404 object must return an error")
		}
	}
}

// TestPreflightSuccess: the sentinel 0-byte PutObject succeeding (200) makes Preflight
// pass, and it writes exactly one object under the reserved _preflight/ prefix. (S1)
func TestPreflightSuccess(t *testing.T) {
	stub := &storeStub{putStatus: http.StatusOK}
	sink := newStoreSink(t, stub, "")

	if err := sink.Preflight(context.Background()); err != nil {
		t.Fatalf("Preflight with a writable target must succeed: %v", err)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.singlePuts) != 1 {
		t.Fatalf("Preflight must do one sentinel PutObject, got %d", len(stub.singlePuts))
	}
	if !strings.HasPrefix(stub.singlePuts[0], "_preflight/") {
		t.Fatalf("Preflight sentinel key = %q, want under _preflight/", stub.singlePuts[0])
	}
}

// TestPreflightDeniedIsError: a 403 on the sentinel PutObject (a wrong cred / missing
// write grant / gone bucket all return the real S3 code on a PUT) fails Preflight
// loudly rather than dead-lettering every object. (S1)
func TestPreflightDeniedIsError(t *testing.T) {
	stub := &storeStub{putStatus: http.StatusForbidden}
	sink := newStoreSink(t, stub, "")

	if err := sink.Preflight(context.Background()); err == nil {
		t.Fatal("Preflight must error when the sentinel PutObject is denied (403)")
	}
}

// TestPutManifestRoutesToManifestPrefix: PutManifest streams a ledger snapshot through
// putStream to a key under <prefix>/_manifest/. A non-empty snapshot goes multipart, so
// the key is captured at the initiate request — this also exercises the target-prefix
// join end-to-end. (S1)
func TestPutManifestRoutesToManifestPrefix(t *testing.T) {
	stub := &storeStub{putStatus: http.StatusOK}
	sink := newStoreSink(t, stub, "backups")

	if err := sink.PutManifest(context.Background(), "snapshot-7", bytes.NewReader([]byte("ledger snapshot"))); err != nil {
		t.Fatalf("PutManifest: %v", err)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.initiates) != 1 {
		t.Fatalf("PutManifest of a non-empty snapshot must open one multipart upload, got %d", len(stub.initiates))
	}
	if stub.initiates[0] != "backups/_manifest/snapshot-7" {
		t.Fatalf("manifest key = %q, want backups/_manifest/snapshot-7", stub.initiates[0])
	}
}

// TestKeyLayoutAndName: the derived object key is <prefix>/<h[0:2]>/<h[2:4]>/<hash>,
// prefixed joins under the target prefix, and Name echoes the config — the layout the
// restore path re-derives from the hash, so a drift here would silently break restore.
// (S1)
func TestKeyLayoutAndName(t *testing.T) {
	sink, err := New(Config{
		Name:      "primary",
		Endpoint:  "127.0.0.1:9000",
		Region:    "us-east-1",
		Bucket:    "bkt",
		Prefix:    "backups",
		AccessKey: "AK", SecretKey: "SK",
	})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	if sink.Name() != "primary" {
		t.Fatalf("Name() = %q, want primary", sink.Name())
	}
	hash := strings.Repeat("a", 62) + "ff" // 64 hex chars
	want := path.Join("backups", hash[0:2], hash[2:4], hash)
	if got := sink.key(hash); got != want {
		t.Fatalf("key(%q) = %q, want prefixed+sharded %q", hash, got, want)
	}
	if got := sink.prefixed("_manifest/x"); got != "backups/_manifest/x" {
		t.Fatalf("prefixed = %q, want backups/_manifest/x", got)
	}
}

// TestStoreUpstreamReadErrors: when the source reader fails (non-EOF) before the
// empty/non-empty branch decision, putStream surfaces the read error and never issues
// a PUT. (S1)
func TestStoreUpstreamReadErrors(t *testing.T) {
	stub := &storeStub{putStatus: http.StatusOK}
	sink := newStoreSink(t, stub, "")

	if _, err := sink.Store(context.Background(), strings.Repeat("a", 64), errReader{}); err == nil {
		t.Fatal("Store must fail when the source reader errors")
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.singlePuts)+len(stub.initiates) != 0 {
		t.Fatalf("a failed upstream read must not issue any PUT/initiate: puts=%d initiates=%d",
			len(stub.singlePuts), len(stub.initiates))
	}
}

// TestStoreEmptyPutDenied: a denied PutObject on the empty-object branch (0-byte
// stream) surfaces as an error rather than a false success. (S1)
func TestStoreEmptyPutDenied(t *testing.T) {
	stub := &storeStub{putStatus: http.StatusForbidden}
	sink := newStoreSink(t, stub, "")

	if _, err := sink.Store(context.Background(), strings.Repeat("b", 64), bytes.NewReader(nil)); err == nil {
		t.Fatal("Store of an empty object must fail when the PutObject is denied")
	}
}

// TestStoreMultipartInitiateDenied: a non-empty stream whose multipart initiate is
// denied (403) fails Store rather than silently dropping the object. (S1)
func TestStoreMultipartInitiateDenied(t *testing.T) {
	stub := &storeStub{putStatus: http.StatusOK, initiateStatus: http.StatusForbidden}
	sink := newStoreSink(t, stub, "")

	if _, err := sink.Store(context.Background(), strings.Repeat("c", 64), bytes.NewReader([]byte("payload"))); err == nil {
		t.Fatal("Store must fail when the multipart initiate is denied")
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if stub.completes != 0 {
		t.Fatalf("a denied initiate must never reach complete, got %d completes", stub.completes)
	}
}

// TestNewWithSSE: constructing a sink with server-side encryption enabled succeeds
// (the SSE option is wired into the put options). (S1)
func TestNewWithSSE(t *testing.T) {
	sink, err := New(Config{
		Name:      "sse",
		Endpoint:  "127.0.0.1:9000",
		Region:    "us-east-1",
		Bucket:    "bkt",
		AccessKey: "AK", SecretKey: "SK",
		SSE: true,
	})
	if err != nil {
		t.Fatalf("New with SSE must succeed: %v", err)
	}
	if sink.opts.ServerSideEncryption == nil {
		t.Fatal("SSE=true must set ServerSideEncryption on the put options")
	}
}

// TestNewRejectsBadEndpoint: a malformed endpoint (a fully-qualified URL, not a
// host:port) fails construction rather than yielding a broken client. (S1)
func TestNewRejectsBadEndpoint(t *testing.T) {
	if _, err := New(Config{
		Name:      "bad",
		Endpoint:  "http://example.com/with/path",
		Region:    "us-east-1",
		Bucket:    "bkt",
		AccessKey: "AK", SecretKey: "SK",
	}); err == nil {
		t.Fatal("New must reject a malformed endpoint")
	}
}
