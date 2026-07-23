package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

// wormStub is a minimal S3-over-HTTP stub for the object-lock/versioning/list/get surface the WORM
// drift-check (CheckImmutability) and inventory reader (LatestManifest) exercise. A 127.0.0.1
// endpoint forces path-style, so a bucket-level request carries a query (?object-lock / ?versioning
// / ?list-type) and an object GET carries a key path.
type wormStub struct {
	lockStatus       int    // HTTP status for GET ?object-lock (200 or 403)
	lockErrCode      string // on a 404 lockStatus, the S3 <Code> to emit (default ObjectLockConfigurationNotFoundError)
	lockEnabled      bool   // ObjectLockEnabled value in the 200 body
	lockNoRule       bool   // if set, an Enabled config with NO default retention Rule (drift — objects get no retention)
	versioningStatus string // Status in the versioning body ("Enabled"/"Suspended")
	versioningErr    int    // non-zero HTTP status to fail the versioning GET
	listErr          int    // non-zero HTTP status to fail the list-objects call
	listKeys         []string
	manifest         []byte
	pointerName      string      // if set, GET of _manifest/LATEST returns this (the pointer fast-path)
	getFail          bool        // fail the object GET (the manifest fetch)
	bucketGone       atomic.Bool // if set, a bucket-level HEAD (BucketExists) returns 404 (gone bucket); atomic so a test can flip it mid-run (a handler goroutine reads it)
}

// serveList renders the list-objects XML, honoring the bounded StartAfter (exclusive) query param.
func (s *wormStub) serveList(w http.ResponseWriter, after string) {
	if s.listErr != 0 {
		w.WriteHeader(s.listErr)
		return
	}
	var b strings.Builder
	b.WriteString(`<ListBucketResult><Name>bkt</Name><IsTruncated>false</IsTruncated>`)
	for _, k := range s.listKeys {
		if after != "" && k <= after {
			continue
		}
		fmt.Fprintf(&b, `<Contents><Key>%s</Key><Size>%d</Size></Contents>`, k, len(s.manifest))
	}
	b.WriteString(`</ListBucketResult>`)
	_, _ = io.WriteString(w, b.String())
}

func (s *wormStub) serveObjectLock(w http.ResponseWriter) {
	switch s.lockStatus {
	case http.StatusOK:
		lock := ""
		if s.lockEnabled {
			lock = "Enabled"
		}
		// A real WORM bucket carries a DEFAULT retention Rule (mode+period); the sink treats Enabled
		// WITHOUT a Rule as drift (objects get no retention). Emit the Rule unless lockNoRule exercises
		// that drift case.
		rule := `<Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Days>30</Days></DefaultRetention></Rule>`
		if s.lockNoRule {
			rule = ""
		}
		_, _ = fmt.Fprintf(w, `<ObjectLockConfiguration><ObjectLockEnabled>%s</ObjectLockEnabled>%s</ObjectLockConfiguration>`, lock, rule)
	case http.StatusNotFound:
		// A 404 — but the CODE decides the meaning. Only ObjectLockConfigurationNotFoundError is the
		// definitive DRIFT signal (a WORM bucket that lost its lock config); any OTHER 404 code (e.g.
		// NoSuchBucket) is a different fault → Unverifiable. lockErrCode overrides the default code so
		// a test can drive a non-drift 404.
		code := s.lockErrCode
		if code == "" {
			code = "ObjectLockConfigurationNotFoundError"
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprintf(w, `<Error><Code>%s</Code><Message>404</Message></Error>`, code)
	default:
		w.WriteHeader(s.lockStatus) // e.g. 403 AccessDenied — a read-denying credential
	}
}

// serveHead answers a HEAD: a bucket-level HEAD (path "bkt"/"bkt/") is BucketExists; anything deeper is
// a StatObject (LatestManifest's EAGER obj.Stat()).
func (s *wormStub) serveHead(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(p, "/", 2)
	if len(parts) < 2 || parts[1] == "" { // bucket-level HEAD → BucketExists
		if s.bucketGone.Load() {
			w.WriteHeader(http.StatusNotFound) // gone bucket → confirmBucket surfaces an error (Unverifiable)
			return
		}
		w.WriteHeader(http.StatusOK) // bucket present
		return
	}
	if s.getFail {
		w.WriteHeader(http.StatusForbidden) // a read-deny surfaces at the eager stat → unverifiable
		return
	}
	// StatObject parses Last-Modified (a missing one errors) + Content-Length into ObjectInfo.
	w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", `"manifest-etag"`)
	w.Header().Set("Content-Length", strconv.Itoa(len(s.manifest)))
	w.WriteHeader(http.StatusOK)
}

func (s *wormStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodGet && q.Has("object-lock"):
		s.serveObjectLock(w)
	case r.Method == http.MethodGet && q.Has("versioning"):
		if s.versioningErr != 0 {
			w.WriteHeader(s.versioningErr)
			return
		}
		_, _ = fmt.Fprintf(w, `<VersioningConfiguration><Status>%s</Status></VersioningConfiguration>`, s.versioningStatus)
	case r.Method == http.MethodGet && q.Has("list-type"):
		s.serveList(w, q.Get("start-after"))
	case r.Method == http.MethodHead:
		s.serveHead(w, r)
	case r.Method == http.MethodGet: // object GET
		if s.getFail {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		// minio's GetObject parses Last-Modified; httptest omits it by default.
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		w.Header().Set("Content-Type", "application/octet-stream")
		if strings.HasSuffix(r.URL.Path, "/LATEST") { // the pointer fast-path
			_, _ = io.WriteString(w, s.pointerName)
			return
		}
		_, _ = w.Write(s.manifest)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func newWormSink(t *testing.T, stub *wormStub) *Sink {
	t.Helper()
	srv := httptest.NewServer(stub)
	t.Cleanup(srv.Close)
	sink, err := New(Config{
		Name: "worm", Endpoint: strings.TrimPrefix(srv.URL, "http://"),
		Region: "us-east-1", Bucket: "bkt", AccessKey: "AK", SecretKey: "SK", UseSSL: false,
	})
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	return sink
}

// TestImmutabilityReadableAuditCredential (re-review B1): a sink WITHOUT an audit/read credential is
// NOT ImmutabilityReadable (the drift-check is N/A → the domain reports NoData, silent); a sink WITH
// one IS readable and runs CheckImmutability via the audit client.
func TestImmutabilityReadableAuditCredential(t *testing.T) {
	stub := &wormStub{lockStatus: http.StatusOK, lockEnabled: true, versioningStatus: "Enabled"}
	// No audit credential → not readable.
	if noCred := newWormSink(t, stub); noCred.ImmutabilityReadable() {
		t.Fatal("a sink without an audit credential must NOT be ImmutabilityReadable")
	}
	// With an audit credential → readable, and the drift-check actually runs (via the audit client).
	srv := httptest.NewServer(stub)
	t.Cleanup(srv.Close)
	sink, err := New(Config{
		Name: "worm", Endpoint: strings.TrimPrefix(srv.URL, "http://"), Region: "us-east-1", Bucket: "bkt",
		AccessKey: "AK", SecretKey: "SK", AuditAccessKey: "AAK", AuditSecretKey: "ASK",
	})
	if err != nil {
		t.Fatalf("new sink with audit cred: %v", err)
	}
	if !sink.ImmutabilityReadable() {
		t.Fatal("a sink WITH an audit credential must be ImmutabilityReadable")
	}
	lock, ver, cerr := sink.CheckImmutability(context.Background())
	if cerr != nil || !lock || !ver {
		t.Fatalf("CheckImmutability via the audit client must run: lock=%v ver=%v err=%v", lock, ver, cerr)
	}
}

func TestCheckImmutabilityEnabled(t *testing.T) {
	sink := newWormSink(t, &wormStub{lockStatus: http.StatusOK, lockEnabled: true, versioningStatus: "Enabled"})
	lock, ver, err := sink.CheckImmutability(context.Background())
	if err != nil {
		t.Fatalf("CheckImmutability: %v", err)
	}
	if !lock || !ver {
		t.Fatalf("want object-lock + versioning enabled, got lock=%v versioning=%v", lock, ver)
	}
}

func TestCheckImmutabilityEnabledNoRetentionRuleIsDrift(t *testing.T) {
	// Object-lock Enabled but with NO default retention Rule: the worker PUTs with no per-object
	// retention header, so new objects get NO retention (deletable) — the WORM guarantee is lost even
	// though the Enabled flag is on. Must read as drift (lock=false), not Verified.
	sink := newWormSink(t, &wormStub{lockStatus: http.StatusOK, lockEnabled: true, lockNoRule: true, versioningStatus: "Enabled"})
	lock, _, err := sink.CheckImmutability(context.Background())
	if err != nil {
		t.Fatalf("CheckImmutability: %v", err)
	}
	if lock {
		t.Fatal("object-lock Enabled with no default retention rule must read as drift (lock=false), got lock=true")
	}
}

func TestCheckImmutabilityVersioningSuspendedIsDrift(t *testing.T) {
	// Object-lock enabled but versioning SUSPENDED (which silently defeats WORM) must be reported
	// as versioning=false so the domain flags drift.
	sink := newWormSink(t, &wormStub{lockStatus: http.StatusOK, lockEnabled: true, versioningStatus: "Suspended"})
	lock, ver, err := sink.CheckImmutability(context.Background())
	if err != nil {
		t.Fatalf("CheckImmutability: %v", err)
	}
	if !lock || ver {
		t.Fatalf("suspended versioning must read as drift (lock=true versioning=false), got lock=%v versioning=%v", lock, ver)
	}
}

func TestCheckImmutabilityReadDeniedErrors(t *testing.T) {
	// A read-denying (PutObject-only) credential 403s the config GET — CheckImmutability must
	// return the error so the domain reports the target UNVERIFIABLE (not drift).
	sink := newWormSink(t, &wormStub{lockStatus: http.StatusForbidden})
	if _, _, err := sink.CheckImmutability(context.Background()); err == nil {
		t.Fatal("a 403 on the object-lock config must return an error (→ unverifiable), not (false,false,nil)")
	}
}

// TestCheckImmutabilityOtherNotFoundIsUnverifiable (re-review item 3): a 404 with a DIFFERENT code
// (e.g. NoSuchBucket — a gone/renamed bucket) is NOT drift; it must return an error → Unverifiable.
// Only the specific ObjectLockConfigurationNotFoundError code is drift. Must FAIL if the match is
// widened to any 404.
func TestCheckImmutabilityOtherNotFoundIsUnverifiable(t *testing.T) {
	sink := newWormSink(t, &wormStub{lockStatus: http.StatusNotFound, lockErrCode: "NoSuchBucket"})
	if _, _, err := sink.CheckImmutability(context.Background()); err == nil {
		t.Fatal("a 404 with a non-object-lock code (NoSuchBucket) must return an error (→ Unverifiable), not (false,false,nil) Drift")
	}
}

// TestCheckImmutabilityObjectLockRemovedIsDrift (review #2): a definitive
// ObjectLockConfigurationNotFoundError (HTTP 404) means the WORM bucket LOST its object-lock — this
// is DRIFT (objectLock=false, no error), NOT "unverifiable". It must not be masked as unreadable.
func TestCheckImmutabilityObjectLockRemovedIsDrift(t *testing.T) {
	sink := newWormSink(t, &wormStub{lockStatus: http.StatusNotFound, versioningStatus: "Enabled"})
	lock, ver, err := sink.CheckImmutability(context.Background())
	if err != nil {
		t.Fatalf("a 404 object-lock (removed) must NOT error (it's a definitive drift answer), got %v", err)
	}
	if lock {
		t.Fatal("object-lock removed (404) must report objectLock=false (drift)")
	}
	if !ver {
		t.Fatal("versioning was still enabled — must report true")
	}
}

// TestCheckImmutabilityObjectLockRemovedDriftSurvivesVersioningReadError: the key new invariant —
// object-lock is DEFINITIVELY removed (404) AND the best-effort versioning read ALSO fails (403). The
// definitive drift must win: CheckImmutability returns (objectLock=false, versioning=false, err=nil).
// A failed versioning read must NOT discard/mask the definitive object-lock-removed drift as merely
// "unverifiable" — that would silently drop a WORM-lost alert.
func TestCheckImmutabilityObjectLockRemovedDriftSurvivesVersioningReadError(t *testing.T) {
	sink := newWormSink(t, &wormStub{lockStatus: http.StatusNotFound, versioningErr: http.StatusForbidden})
	lock, ver, err := sink.CheckImmutability(context.Background())
	if err != nil {
		t.Fatalf("a definitive object-lock-removed drift must NOT error even when versioning read fails, got %v", err)
	}
	if lock {
		t.Fatal("object-lock removed (404) must report objectLock=false (drift) regardless of the versioning read")
	}
	if ver {
		t.Fatal("a failed versioning read must report versioning=false (best-effort detail), not true")
	}
}

func TestCheckImmutabilityVersioningErrorPropagates(t *testing.T) {
	// Object-lock reads OK but the versioning GET 403s (a partial read-deny) — the error must
	// propagate so the target reads as unverifiable, not a false ok.
	sink := newWormSink(t, &wormStub{lockStatus: http.StatusOK, lockEnabled: true, versioningStatus: "", versioningErr: http.StatusForbidden})
	if _, _, err := sink.CheckImmutability(context.Background()); err == nil {
		t.Fatal("a versioning-config error must propagate")
	}
}

func TestLatestManifestGetErrorPropagates(t *testing.T) {
	// The list finds a manifest key but the object GET fails (e.g. a read-denied object) — the
	// error must propagate (the domain maps it to unverifiable).
	sink := newWormSink(t, &wormStub{
		listKeys: []string{"_manifest/2026-01-01T000000.000000000Z.jsonl"},
		getFail:  true,
	})
	rc, err := sink.LatestManifest(context.Background())
	if err == nil {
		_ = rc.Close()
		// minio GetObject is lazy — the error may surface on the first Read.
		if _, rerr := io.ReadAll(rc); rerr == nil {
			t.Fatal("a manifest object read error must surface")
		}
	}
}

func TestLatestManifestPicksNewest(t *testing.T) {
	body := []byte(`{"externalID":"abc"}` + "\n")
	sink := newWormSink(t, &wormStub{
		listKeys: []string{
			"_manifest/2026-01-01T000000.000000000Z.jsonl",
			"_manifest/2026-06-01T000000.000000000Z.jsonl", // newest (lexicographically highest)
			"_manifest/2026-03-01T000000.000000000Z.jsonl",
		},
		manifest: body,
	})
	rc, err := sink.LatestManifest(context.Background())
	if err != nil {
		t.Fatalf("LatestManifest: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil || string(got) != string(body) {
		t.Fatalf("manifest content mismatch: %q err=%v", got, err)
	}
}

func TestLatestManifestNoneIsNotExist(t *testing.T) {
	sink := newWormSink(t, &wormStub{listKeys: nil})
	_, err := sink.LatestManifest(context.Background())
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("no manifest must be a wrapped os.ErrNotExist (→ unverifiable, not a failure), got %v", err)
	}
}

// TestLatestManifestUncachedBucketCatchesVanish (Alt#1): the inventory direction must catch a vanished
// bucket as an ERROR (→ Unverifiable) even after the existence sweep cached the bucket PRESENT — it
// re-checks the bucket UNCACHED. With the old cached confirmBucket this masked the vanished bucket as
// os.ErrNotExist → NoData ("no manifest, lost nothing"), diverging from the filesystem sink.
func TestLatestManifestUncachedBucketCatchesVanish(t *testing.T) {
	stub := &wormStub{} // bucket present initially
	sink := newWormSink(t, stub)
	// Prime confirmBucket's PRESENT cache, exactly as a prior existence sweep on the shared *Sink would.
	if err := sink.confirmBucket(context.Background()); err != nil {
		t.Fatalf("prime confirmBucket present: %v", err)
	}
	stub.bucketGone.Store(true) // the bucket vanishes AFTER present was cached
	_, err := sink.LatestManifest(context.Background())
	if err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("LatestManifest must re-check the bucket UNCACHED and surface a vanished bucket as a non-ErrNotExist error (Unverifiable), got %v", err)
	}
}

// TestLatestManifestPointerCurrentUsed: the pointer fast-path reads _manifest/LATEST, then a BOUNDED
// StartAfter=<pointer> list confirms NOTHING newer exists (C1 staleness check), so the pointer's
// manifest is used (no full scan of the whole prefix).
func TestLatestManifestPointerCurrentUsed(t *testing.T) {
	body := []byte(`{"externalID":"abc"}` + "\n")
	stub := &wormStub{
		pointerName: "2026-06-01T000000.000000000Z.jsonl",
		manifest:    body,
		listKeys:    []string{"_manifest/2026-06-01T000000.000000000Z.jsonl"}, // only the pointer's own key → nothing newer
	}
	sink := newWormSink(t, stub)
	rc, err := sink.LatestManifest(context.Background())
	if err != nil {
		t.Fatalf("LatestManifest via a current pointer must not error, got %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil || string(got) != string(body) {
		t.Fatalf("pointer manifest content mismatch: %q err=%v", got, err)
	}
}

func TestLatestManifestListErrorPropagates(t *testing.T) {
	// A read-denying credential can't LIST — the error must propagate (not read as "no manifest").
	sink := newWormSink(t, &wormStub{listErr: http.StatusForbidden})
	_, err := sink.LatestManifest(context.Background())
	if err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a list error must propagate as a real error, got %v", err)
	}
}

// TestLatestManifestGoneBucketIsError (Altitude #4): a gone/unreachable bucket surfaces as an ERROR
// (→ Unverifiable) from LatestManifest's upfront confirmBucket — symmetric with the filesystem sink's
// confirmRoot — even though the (empty) LIST would otherwise resolve name="" → NoData. A vanished
// target must not read as benign "no manifest yet" (it has NOT lost nothing).
func TestLatestManifestGoneBucketIsError(t *testing.T) {
	stub := &wormStub{} // no listKeys → an empty list would be NoData
	stub.bucketGone.Store(true)
	sink := newWormSink(t, stub)
	_, err := sink.LatestManifest(context.Background())
	if err == nil {
		t.Fatal("LatestManifest against a gone bucket must return an error (Unverifiable), not (nil, NoData)")
	}
	if errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a gone bucket must NOT map to os.ErrNotExist/NoData, got %v", err)
	}
}
