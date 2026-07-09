package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// wormStub is a minimal S3-over-HTTP stub for the object-lock/versioning/list/get surface the WORM
// drift-check (CheckImmutability) and inventory reader (LatestManifest) exercise. A 127.0.0.1
// endpoint forces path-style, so a bucket-level request carries a query (?object-lock / ?versioning
// / ?list-type) and an object GET carries a key path.
type wormStub struct {
	lockStatus       int    // HTTP status for GET ?object-lock (200 or 403)
	lockEnabled      bool   // ObjectLockEnabled value in the 200 body
	versioningStatus string // Status in the versioning body ("Enabled"/"Suspended")
	versioningErr    int    // non-zero HTTP status to fail the versioning GET
	listErr          int    // non-zero HTTP status to fail the list-objects call
	listKeys         []string
	manifest         []byte
	getFail          bool // fail the object GET (the manifest fetch)
}

func (s *wormStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	switch {
	case r.Method == http.MethodGet && q.Has("object-lock"):
		if s.lockStatus != http.StatusOK {
			w.WriteHeader(s.lockStatus) // e.g. 403 AccessDenied — a read-denying credential
			return
		}
		lock := ""
		if s.lockEnabled {
			lock = "Enabled"
		}
		_, _ = fmt.Fprintf(w, `<ObjectLockConfiguration><ObjectLockEnabled>%s</ObjectLockEnabled></ObjectLockConfiguration>`, lock)
	case r.Method == http.MethodGet && q.Has("versioning"):
		if s.versioningErr != 0 {
			w.WriteHeader(s.versioningErr)
			return
		}
		_, _ = fmt.Fprintf(w, `<VersioningConfiguration><Status>%s</Status></VersioningConfiguration>`, s.versioningStatus)
	case r.Method == http.MethodGet && q.Has("list-type"):
		if s.listErr != 0 {
			w.WriteHeader(s.listErr)
			return
		}
		var b strings.Builder
		b.WriteString(`<ListBucketResult><Name>bkt</Name><IsTruncated>false</IsTruncated>`)
		for _, k := range s.listKeys {
			fmt.Fprintf(&b, `<Contents><Key>%s</Key><Size>%d</Size></Contents>`, k, len(s.manifest))
		}
		b.WriteString(`</ListBucketResult>`)
		_, _ = io.WriteString(w, b.String())
	case r.Method == http.MethodGet: // object GET (the manifest)
		if s.getFail {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		// minio's GetObject parses Last-Modified; httptest omits it by default.
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		w.Header().Set("Content-Type", "application/octet-stream")
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

func TestLatestManifestListErrorPropagates(t *testing.T) {
	// A read-denying credential can't LIST — the error must propagate (not read as "no manifest").
	sink := newWormSink(t, &wormStub{listErr: http.StatusForbidden})
	_, err := sink.LatestManifest(context.Background())
	if err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a list error must propagate as a real error, got %v", err)
	}
}
