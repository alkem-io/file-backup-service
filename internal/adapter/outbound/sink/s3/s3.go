// Package s3 implements the Sink port over S3-compatible object storage
// (Scaleway Object Storage), with server-side encryption and 2-level hex
// sharding. Object-lock/WORM retention is a bucket-level policy (infra-ops); the
// worker's credentials are PutObject-only on the immutable target.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"

	"github.com/alkem-io/file-backup-service/internal/fsutil"
)

// Config configures an S3 sink.
type Config struct {
	Name      string
	Endpoint  string
	Region    string
	Bucket    string
	Prefix    string
	AccessKey string
	SecretKey string
	// AuditAccessKey/AuditSecretKey are an OPTIONAL read/audit credential (see config.Target): when
	// BOTH are set, the WORM immutability drift-check runs against a SECOND, read-capable client;
	// unset → the drift-check is N/A (ImmutabilityReadable()==false → the domain reports NoData).
	AuditAccessKey string
	AuditSecretKey string
	UseSSL         bool
	SSE            bool
}

// Sink is the S3-compatible target.
type Sink struct {
	name string
	// client is the worker credential (PutObject-only on a WORM target).
	client *minio.Client
	// auditClient is the OPTIONAL read/audit credential, used ONLY for the WORM immutability
	// drift-check (which the write-only worker client can't read). nil when no audit cred is set.
	auditClient *minio.Client
	bucket      string
	prefix      string
	opts        minio.PutObjectOptions // constant for the sink's life (PartSize + SSE)

	// bucketMu guards a one-shot, cached bucket-existence verdict (see confirmBucket): a 404
	// from StatObject can't tell a missing OBJECT from a missing BUCKET, so Exists confirms the
	// bucket once before ever reporting "absent".
	bucketMu      sync.Mutex
	bucketChecked bool  // BucketExists returned a DEFINITIVE answer (present or gone)
	bucketGone    error // non-nil once the bucket is confirmed gone; nil = present or unchecked
}

// New constructs an S3 Sink.
func New(cfg Config) (*Sink, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
		Region: cfg.Region, // explicit: PutObject-only creds can't auto-discover it (SigV4 signs this region)
	})
	if err != nil {
		return nil, fmt.Errorf("s3 client %q: %w", cfg.Name, err)
	}
	// Bound the streaming (size=-1) multipart buffer once. For an unknown length
	// minio-go defaults to a ~528 MiB part (5 TiB / 10000 parts) and does make([]byte,
	// part) PER upload — which OOMs the co-located RWO node at concurrency. 5 MiB is the
	// S3 multipart minimum: live heap = concurrency x #targets x 5 MiB, and 5 MiB x 10,000
	// parts = ~48 GiB, comfortably above the ~2 GiB real max object size (the domain object cap
	// = 4 GiB). If that cap is ever raised past ~48 GiB, revisit this together.
	opts := minio.PutObjectOptions{PartSize: 5 << 20}
	if cfg.SSE {
		opts.ServerSideEncryption = encrypt.NewSSE()
	}
	sink := &Sink{name: cfg.Name, client: client, bucket: cfg.Bucket, prefix: cfg.Prefix, opts: opts}
	// Build the optional read/audit client (both keys required) — used ONLY for the WORM immutability
	// drift-check; the worker's own PutObject-only credential can't read GetObjectLockConfig.
	if cfg.AuditAccessKey != "" && cfg.AuditSecretKey != "" {
		auditClient, aerr := minio.New(cfg.Endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(cfg.AuditAccessKey, cfg.AuditSecretKey, ""),
			Secure: cfg.UseSSL,
			Region: cfg.Region,
		})
		if aerr != nil {
			return nil, fmt.Errorf("s3 audit client %q: %w", cfg.Name, aerr)
		}
		sink.auditClient = auditClient
	}
	return sink, nil
}

// ImmutabilityReadable reports whether this target has an audit/read credential able to read its
// object-lock/versioning config. false → the WORM immutability drift-check is legitimately N/A (a
// PutObject-only worker credential can't read it) → the domain reports NoData (silent), never a
// false pass; the immutability is asserted by object-lock + the audit + never_verified.
func (s *Sink) ImmutabilityReadable() bool { return s.auditClient != nil }

// Name returns the target name.
func (s *Sink) Name() string { return s.name }

// prefixed joins a slash-style fsutil key under the target prefix — the one place
// prefix handling lives, so objects and manifests can't diverge on the layout.
func (s *Sink) prefixed(key string) string { return path.Join(s.prefix, key) }

func (s *Sink) key(hash string) string { return s.prefixed(fsutil.ShardKey(hash)) }

// Preflight validates the target end-to-end at startup rather than dead-lettering
// every object. It does a sentinel 0-byte PutObject — the ONLY operation that
// exercises a PutObject-only WORM credential (creds + region + SSE + write grant +
// bucket existence). BucketExists (a HEAD) can't: a write-only cred can't HEAD, and a
// HEAD 403 is indistinguishable from a wrong secret (no response body for the real
// code). A PUT's error DOES carry the real S3 code, so a wrong cred / missing bucket /
// bad region all fail loudly. The key is unique per run so object-lock can't reject an
// overwrite on restart; these tiny objects live under the reserved fsutil.PreflightKey
// prefix (reconcile/audit skip it) and expire with the bucket's retention.
func (s *Sink) Preflight(ctx context.Context) error {
	key := s.prefixed(fsutil.PreflightKey())
	if _, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(nil), 0, s.opts); err != nil {
		return fmt.Errorf("s3 preflight %q (creds/region/bucket/write-grant?): %w", s.name, err)
	}
	return nil
}

// Exists reports whether the object is present. A 404 is "absent" ONLY once the bucket is
// confirmed present — a StatObject is a HEAD (no response body), so minio-go maps EVERY 404 to
// NoSuchKey, unable to tell a missing OBJECT from a missing/deleted/typo'd BUCKET. Without the
// bucket check, a gone bucket would read as "every object absent", and audit — which skips the
// target write-preflight — would report the whole sample as silent LOSS ("N objects missing")
// instead of a bucket-level fault, paging the operator toward the wrong investigation. A gone
// or unreachable bucket now surfaces as an ERROR (audit → Unverifiable, not Missing). A
// 403/AccessDenied (expected on a PutObject-only WORM credential, and also what a real
// credential/endpoint fault returns) is likewise an ERROR, not "absent", so reconcile never
// treats a permission fault as a gap to refill. Dedup is answered by the ledger, not this method.
func (s *Sink) Exists(ctx context.Context, hash string) (bool, error) {
	_, err := s.client.StatObject(ctx, s.bucket, s.key(hash), minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	resp := minio.ToErrorResponse(err)
	if resp.StatusCode == http.StatusNotFound || resp.Code == "NoSuchKey" {
		if berr := s.confirmBucket(ctx); berr != nil {
			return false, berr // the "object" 404 is really a gone/unreachable bucket
		}
		return false, nil // bucket present → the object is genuinely absent
	}
	return false, fmt.Errorf("stat %s: %w", s.key(hash), err)
}

// confirmBucket verifies the bucket exists, so Exists never mistakes a missing bucket for a missing
// object. It caches ONLY the terminal GONE verdict (a deleted bucket won't come back mid-run, and
// caching it collapses N per-object HEADs to one); a PRESENT bucket is NOT cached, so a bucket that
// vanishes MID-SWEEP is caught on the next absent-object probe (later 404s → Unverifiable, not false
// silent-loss) — matching the filesystem sink's per-call confirmRoot re-check. A transient error is
// not cached either (fail-closed re-check). The lock is held across the one network call so 16
// concurrent audit probes collapse to a single BucketExists rather than a thundering herd.
func (s *Sink) confirmBucket(ctx context.Context) error {
	s.bucketMu.Lock()
	defer s.bucketMu.Unlock()
	if s.bucketChecked { // set ONLY once the bucket is confirmed GONE (terminal)
		return s.bucketGone
	}
	ok, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("bucket %q existence check: %w", s.bucket, err) // transient — don't cache; re-check next probe
	}
	if !ok {
		s.bucketChecked = true // cache the terminal gone verdict (no repeated HEADs on a gone bucket)
		s.bucketGone = fmt.Errorf("bucket %q does not exist (deleted/renamed/misconfigured?)", s.bucket)
		return s.bucketGone
	}
	return nil // present — NOT cached, so a mid-sweep vanish is caught on the next probe
}

// putStream uploads r to key with SSE, size=-1 (streamed to EOF) so a commit is gated
// on the caller's upstream verification, never a known length. But a 0-byte stream then
// completes a multipart with an empty part, which Scaleway (and many S3 backends)
// reject (EntityTooSmall); detect empty with a single-byte read (no per-object bufio
// buffer) and use one empty PutObject, re-prepending the read byte via MultiReader.
//
// WORM / incomplete-multipart caveat (operational, cross-repo): size=-1 makes minio-go use
// a MULTIPART upload even for sub-part objects. We CANNOT pass the real size to force a
// single-PUT: a length-gated PutObject commits after reading exactly N bytes, BEFORE the
// caller's VerifyReader reaches EOF and checks the hash — the very early-commit this size=-1
// prevents. The tradeoff: on a mid-stream abort (per-object timeout, VerifyReader hash
// mismatch, or shutdown ctx-cancel), minio-go's deferred AbortMultipartUpload runs, but on a
// PutObject-only WORM credential it 403s and the error is swallowed, leaving an orphaned
// incomplete multipart that this worker can never reclaim. The immutable target bucket MUST
// therefore carry an `AbortIncompleteMultipartUpload` lifecycle rule (provisioned in
// infrastructure-operations) to reap them — see specs/008 data-model (Backup Target).
func (s *Sink) putStream(ctx context.Context, key string, r io.Reader) (int64, error) {
	var one [1]byte
	n, err := io.ReadFull(r, one[:]) // 1-byte buf: (1,nil) if a byte, (0,io.EOF) if empty
	if errors.Is(err, io.EOF) {      // empty object
		if _, perr := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(nil), 0, s.opts); perr != nil {
			return 0, fmt.Errorf("put (empty): %w", perr)
		}
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read: %w", err)
	}
	info, err := s.client.PutObject(ctx, s.bucket, key, io.MultiReader(bytes.NewReader(one[:n]), r), -1, s.opts)
	if err != nil {
		return 0, fmt.Errorf("put: %w", err)
	}
	return info.Size, nil
}

// Store uploads bytes for hash (bucket default retention provides WORM).
func (s *Sink) Store(ctx context.Context, hash string, r io.Reader) (int64, error) {
	return s.putStream(ctx, s.key(hash), r)
}

// Fetch streams the stored object.
func (s *Sink) Fetch(ctx context.Context, hash string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, s.key(hash), minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get: %w", err)
	}
	return obj, nil
}

// PutManifest writes a ledger snapshot object under _manifest/ (empty-safe: an empty ledger
// snapshot is a legitimate 0-byte object) and BEST-EFFORT overwrites the `_manifest/LATEST` pointer
// with the snapshot's name, so a reader single-GETs the pointer instead of scanning the whole
// prefix. The pointer PUT creates a new object VERSION on a versioned/object-lock bucket
// (object-lock requires versioning), so it's WORM-safe. The pointer is only a read-time
// OPTIMIZATION — SelectLatestManifest falls back to a prefix scan when it is absent/stale, so it is
// self-healing — therefore a pointer-only write failure must NOT fail the whole PutManifest: the
// manifest object itself is durably written, and the next successful pass rewrites the pointer.
func (s *Sink) PutManifest(ctx context.Context, name string, r io.Reader) error {
	if _, err := s.putStream(ctx, s.prefixed(fsutil.ManifestKey(name)), r); err != nil {
		return err
	}
	// Best-effort pointer update: swallow a failure (the scan fallback covers correctness).
	_, _ = s.putStream(ctx, s.prefixed(fsutil.ManifestLatestKey()), strings.NewReader(name))
	return nil
}

// CheckImmutability reports whether the bucket still enforces object-lock AND versioning — the
// WORM drift-check (T032): a Scaleway/S3 immutable target loses its guarantee if either is
// disabled. It is the s3 sink's half of domain's optional immutabilityChecker capability (the
// domain type-asserts it). A read-denying (PutObject-only) credential 403s these GET calls, so
// the error is returned and the domain reports the target UNVERIFIABLE, not drifted — the
// immutable off-site copy's write-only credential legitimately can't read its own config.
//
// A DEFINITIVE object-lock-removed answer (ObjectLockConfigurationNotFoundError / HTTP 404) — or a
// present-but-not-"Enabled" config — is DRIFT (lock=false), and is returned as such REGARDLESS of
// the subsequent versioning read: versioning is read only best-effort for the detail, so a transient
// versioning-read failure can no longer DISCARD a definitive drift and mask it as "unverifiable".
func (s *Sink) CheckImmutability(ctx context.Context) (objectLock, versioning bool, err error) {
	// Read via the AUDIT credential (ImmutabilityReadable gates the caller, so it is non-nil here); the
	// worker's own PutObject-only credential can't read these.
	rc := s.readClient()
	lockStatus, _, _, _, lerr := rc.GetObjectLockConfig(ctx, s.bucket)
	switch {
	case lerr != nil && isObjectLockNotFound(lerr):
		// Definitive: object-lock removed → DRIFT. lock=false regardless of versioning (a failed
		// versioning read must NOT mask this); report versioning best-effort for the detail.
		return false, s.versioningEnabled(ctx), nil
	case lerr != nil:
		// A 403 / gone bucket / transient / other error — we couldn't read the lock config → unverifiable.
		return false, false, fmt.Errorf("object-lock config %q: %w", s.bucket, lerr)
	case !strings.EqualFold(lockStatus, "Enabled"):
		// Definitive: object-lock present but not enabled → DRIFT; versioning best-effort.
		return false, s.versioningEnabled(ctx), nil
	}
	// Object-lock enabled → versioning is now load-bearing (a manual Suspend silently defeats WORM),
	// so a versioning-read failure here IS genuinely unverifiable.
	ver, verr := rc.GetBucketVersioning(ctx, s.bucket)
	if verr != nil {
		return false, false, fmt.Errorf("versioning config %q: %w", s.bucket, verr)
	}
	return true, ver.Enabled(), nil
}

// readClient returns the credential used for the immutability drift-check: the audit/read client
// when configured, else the worker client (which will 403 — but ImmutabilityReadable gates the
// caller so this fallback is only a defensive default).
func (s *Sink) readClient() *minio.Client {
	if s.auditClient != nil {
		return s.auditClient
	}
	return s.client
}

// isObjectLockNotFound reports whether err is the DEFINITIVE "object-lock is not configured" answer
// (a WORM bucket that LOST its lock config) — matched ONLY by the specific
// ObjectLockConfigurationNotFoundError code, NOT by any HTTP 404. A bare 404 can be a gone/renamed
// bucket (NoSuchBucket) or a transient 404; treating those as Drift would false-alert, so every
// error OTHER than this exact code is Unverifiable (consistent with Exists/confirmBucket).
func isObjectLockNotFound(err error) bool {
	return minio.ToErrorResponse(err).Code == "ObjectLockConfigurationNotFoundError"
}

// versioningEnabled reads the bucket versioning state, returning false on any read error — used only
// on the definitive-drift path where the lock verdict is already decided, so a versioning-read
// failure is a missing detail, never a reason to mask the drift.
func (s *Sink) versioningEnabled(ctx context.Context) bool {
	ver, err := s.readClient().GetBucketVersioning(ctx, s.bucket)
	if err != nil {
		return false
	}
	return ver.Enabled()
}

// LatestManifest returns the newest ledger-snapshot manifest object under _manifest/ — the s3
// sink's half of domain's optional inventoryReader capability (audit target→ledger). The newest name
// is resolved by the shared fsutil.SelectLatestManifest (pointer fast-path, else a prefix scan
// filtered to VALID timestamped manifests), so s3 and filesystem can't diverge on selection. When no
// manifest exists yet it returns a wrapped os.ErrNotExist (the domain maps that to NoData, not a
// failure). It then EAGERLY confirms the object is readable (a Stat) so a not-found/read-deny
// surfaces NOW — a vanished object as os.ErrNotExist (benign, like the filesystem sink), a read-deny
// as an error (unverifiable) — rather than letting a LAZY GetObject 404 surface mid-READ, where the
// inventory diff would misread it as a non-worm read-path failure while the filesystem sink stays
// benign (sink parity).
func (s *Sink) LatestManifest(ctx context.Context) (io.ReadCloser, error) {
	name, err := fsutil.SelectLatestManifest(
		func() (string, bool) { return s.readManifestPointer(ctx) },
		func(after string) ([]string, error) { return s.listManifestNamesAfter(ctx, after) })
	if err != nil {
		return nil, err
	}
	if name == "" {
		return nil, fmt.Errorf("no manifest under %s: %w", s.prefixed(fsutil.ManifestDir()), os.ErrNotExist)
	}
	obj, err := s.client.GetObject(ctx, s.bucket, s.prefixed(fsutil.ManifestKey(name)), minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get manifest %s: %w", name, err)
	}
	if _, serr := obj.Stat(); serr != nil {
		_ = obj.Close()
		if resp := minio.ToErrorResponse(serr); resp.StatusCode == http.StatusNotFound || resp.Code == "NoSuchKey" {
			return nil, fmt.Errorf("manifest %s vanished: %w", name, os.ErrNotExist) // benign — like fs
		}
		return nil, fmt.Errorf("stat manifest %s: %w", name, serr) // read-deny / other → unverifiable
	}
	return obj, nil
}

// readManifestPointer reads the `_manifest/LATEST` pointer's raw contents (a manifest base name),
// returning ok=false when the pointer is absent/unreadable/empty (or a 403 read-deny) so
// SelectLatestManifest falls back to the scan — which surfaces the same 403 as the audit's
// unverifiable signal. The name is VALIDATED by SelectLatestManifest, not here.
func (s *Sink) readManifestPointer(ctx context.Context) (string, bool) {
	obj, err := s.client.GetObject(ctx, s.bucket, s.prefixed(fsutil.ManifestLatestKey()), minio.GetObjectOptions{})
	if err != nil {
		return "", false
	}
	defer func() { _ = obj.Close() }()
	b, err := io.ReadAll(io.LimitReader(obj, 4096)) // a base name — tiny
	if err != nil {
		return "", false // includes a lazy 404/403 surfacing here → fall back to the scan
	}
	name := strings.TrimSpace(string(b))
	return name, name != ""
}

// listManifestNamesAfter lists the _manifest/ prefix for object base names STRICTLY AFTER `after`
// (a bounded StartAfter list — when `after` is the LATEST pointer this returns only manifests newer
// than it, so SelectLatestManifest cheaply detects a stale pointer; `after==""` is a full scan). The
// list runs under a CHILD ctx cancelled on return, so minio's producer goroutine is torn down whether
// the scan finishes or bails (a leaked list goroutine per audit would otherwise accumulate).
func (s *Sink) listManifestNamesAfter(ctx context.Context, after string) ([]string, error) {
	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	prefix := s.prefixed(fsutil.ManifestDir()) + "/"
	opts := minio.ListObjectsOptions{Prefix: prefix, Recursive: true}
	if after != "" {
		opts.StartAfter = s.prefixed(fsutil.ManifestKey(after)) // exclusive: only keys after the pointer
	}
	var names []string
	for obj := range s.client.ListObjects(lctx, s.bucket, opts) {
		if obj.Err != nil {
			return nil, fmt.Errorf("list manifests under %s: %w", prefix, obj.Err)
		}
		names = append(names, path.Base(obj.Key))
	}
	return names, nil
}
