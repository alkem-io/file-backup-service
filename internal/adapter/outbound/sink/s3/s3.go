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

	"github.com/alkem-io/file-backup-service/internal/domain"
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

	// bucketMu guards a cached bucket-existence verdict (see confirmBucket): a 404 from StatObject
	// can't tell a missing OBJECT from a missing BUCKET, so Exists confirms the bucket before ever
	// reporting "absent". The DEFINITIVE verdict (present OR gone) is cached, so a burst of
	// absent-object probes collapses to a single BucketExists instead of one HEAD each.
	bucketMu      sync.Mutex
	bucketChecked bool  // a DEFINITIVE BucketExists answer (present or gone) is cached in bucketGone
	bucketGone    error // once bucketChecked: nil = present, non-nil = gone; a transient error is NOT cached
}

// New constructs an S3 Sink.
func New(cfg Config) (*Sink, error) {
	client, err := newMinioClient(cfg, cfg.AccessKey, cfg.SecretKey, "s3 client")
	if err != nil {
		return nil, err
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
		auditClient, aerr := newMinioClient(cfg, cfg.AuditAccessKey, cfg.AuditSecretKey, "s3 audit client")
		if aerr != nil {
			return nil, aerr
		}
		sink.auditClient = auditClient
	}
	return sink, nil
}

// newMinioClient builds a minio client for cfg's endpoint with the given credentials — the ONE owner of
// the client construction (Secure/Region options, error labelling), shared by the worker and the audit
// credential so any future transport hardening (a custom http.Transport, TLS minimum, BucketLookup)
// applies to BOTH, never leaving the audit/read (DR-verify) path on different settings. label
// distinguishes the two in errors. Region is explicit: PutObject-only creds can't auto-discover it
// (SigV4 signs this region).
func newMinioClient(cfg Config, access, secret, label string) (*minio.Client, error) {
	c, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(access, secret, ""),
		Secure: cfg.UseSSL,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("%s %q: %w", label, cfg.Name, err)
	}
	return c, nil
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
	// Read via the audit/read credential when configured (a WORM target's worker credential is
	// PutObject-only and 403s a HEAD): on a read-capable target this returns a true present/absent,
	// on a write-only WORM copy it 403s → an error (Unverifiable, never false "absent").
	_, err := s.readClient().StatObject(ctx, s.bucket, s.key(hash), minio.StatObjectOptions{})
	if err == nil {
		return true, nil
	}
	if isNoSuchKey(err) {
		if berr := s.confirmBucket(ctx); berr != nil {
			return false, berr // the "object" 404 is really a gone/unreachable bucket
		}
		return false, nil // bucket present → the object is genuinely absent
	}
	if isReadDenied(err) {
		// A DEFINITIVE read-permission denial (a PutObject-only WORM credential). Tag it so the audit
		// can early-stop a doomed full sweep of a write-only WORM target once a whole page uniformly
		// read-denies — see domain.ErrReadDenied.
		return false, fmt.Errorf("stat %s: %w: %w", s.key(hash), domain.ErrReadDenied, err)
	}
	return false, fmt.Errorf("stat %s: %w", s.key(hash), err)
}

// isNoSuchKey reports whether err is a missing-OBJECT answer (HTTP 404 / NoSuchKey) — the ONE owner of
// that test, shared by Exists and the LatestManifest open closure so the two never disagree on "gone
// object vs gone bucket" (each pairs it with a confirmBucket to make that distinction). A bare 404 can
// also be a gone/renamed BUCKET, so callers confirm the bucket before treating it as a genuine absence.
func isNoSuchKey(err error) bool {
	resp := minio.ToErrorResponse(err)
	return resp.StatusCode == http.StatusNotFound || resp.Code == "NoSuchKey"
}

// isReadDenied reports whether err is a DEFINITIVE 403/AccessDenied read denial (a PutObject-only
// credential), as opposed to a 404, a transient 5xx/timeout, or a network fault.
func isReadDenied(err error) bool {
	resp := minio.ToErrorResponse(err)
	return resp.StatusCode == http.StatusForbidden || resp.Code == "AccessDenied"
}

// bucketPresentUncached does a FRESH BucketExists (bypassing confirmBucket's present-cache), returning
// an error if the bucket is gone OR unreachable. The INVENTORY direction (LatestManifest) uses this, NOT
// the cached confirmBucket: once the existence sweep (or this call's own first check) has cached the
// bucket PRESENT, a cached confirmBucket on a later manifest-Stat 404 would mask a bucket that vanished
// in the list→open window as os.ErrNotExist → NoData ("no manifest, lost nothing"). A fresh probe
// instead catches the vanished bucket as an ERROR → Unverifiable, matching the filesystem sink's
// uncached confirmRoot. (The EXISTENCE direction deliberately keeps the cache: there a cached-present-
// then-gone bucket reads as objects missing → Drift, which pages — the safe direction. So the two
// directions intentionally use different bucket checks.) Reads via the audit/read credential.
func (s *Sink) bucketPresentUncached(ctx context.Context) error {
	ok, err := s.readClient().BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("bucket %q existence check: %w", s.bucket, err)
	}
	if !ok {
		return fmt.Errorf("bucket %q does not exist (deleted/renamed/misconfigured?)", s.bucket)
	}
	return nil
}

// confirmBucket verifies the bucket exists, so Exists never mistakes a missing bucket for a missing
// object. It caches the DEFINITIVE verdict — present OR gone — for the sink's life: caching PRESENT is
// what collapses a burst of absent-object probes (a silent-loss audit sample) to a SINGLE BucketExists
// instead of one serialized HEAD per probe. Not caching present (the old behavior) let 16 concurrent
// probes each block on bucketMu across the network BucketExists; a probe that timed out WHILE waiting
// on the lock then ran BucketExists on a cancelled ctx and returned an error, so a genuinely-MISSING
// object was miscounted as `errored` rather than `missing` — flipping a real Drift to Unverifiable
// (which, for a WORM target, is a benign pass) and MASKING the silent loss the audit exists to catch.
// With present cached, a mid-sweep bucket vanish instead reads as objects genuinely absent → Drift
// (missing>0), which pages — the safe direction. A transient error is NOT cached (fail-closed
// re-check). Reads go through the audit/read credential (a write-only WORM credential can't HEAD a
// bucket). The lock is held across the one network call so concurrent probes collapse to one check.
func (s *Sink) confirmBucket(ctx context.Context) error {
	s.bucketMu.Lock()
	defer s.bucketMu.Unlock()
	if s.bucketChecked { // a definitive present/gone verdict is cached
		return s.bucketGone
	}
	ok, err := s.readClient().BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("bucket %q existence check: %w", s.bucket, err) // transient — don't cache; re-check next probe
	}
	s.bucketChecked = true // cache the definitive verdict (present → bucketGone stays nil; gone → set below)
	if !ok {
		s.bucketGone = fmt.Errorf("bucket %q does not exist (deleted/renamed/misconfigured?)", s.bucket)
	}
	return s.bucketGone
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

// Fetch streams the stored object. It reads via the audit/read credential when configured, so a WORM
// target with a read-capable audit credential can be restored/verified/drilled even though its worker
// credential is PutObject-only (a write-only WORM copy with no audit credential still 403s here — the
// operator must supply a read-capable credential, per wormReadSource's guidance).
func (s *Sink) Fetch(ctx context.Context, hash string) (io.ReadCloser, error) {
	obj, err := s.readClient().GetObject(ctx, s.bucket, s.key(hash), minio.GetObjectOptions{})
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
// OPTIMIZATION — OpenLatestManifest falls back to a prefix scan when it is absent/stale, so it is
// self-healing — therefore a pointer-only write failure must NOT fail the whole PutManifest: the
// manifest object itself is durably written, and the next successful pass rewrites the pointer.
func (s *Sink) PutManifest(ctx context.Context, name string, r io.Reader) error {
	return fsutil.WriteManifestWithPointer(name,
		func() error { _, err := s.putStream(ctx, s.prefixed(fsutil.ManifestKey(name)), r); return err },
		func(body io.Reader) error {
			_, err := s.putStream(ctx, s.prefixed(fsutil.ManifestLatestKey()), body)
			return err
		})
}

// CheckImmutability reports whether the bucket still enforces object-lock AND versioning — the
// WORM drift-check (T032): a Scaleway/S3 immutable target loses its guarantee if either is
// disabled. It is the s3 sink's half of domain's optional immutabilityChecker capability (the
// domain type-asserts it). A read-denying (PutObject-only) credential 403s these GET calls, so
// the error is returned and the domain reports the target UNVERIFIABLE, not drifted — the
// immutable off-site copy's write-only credential legitimately can't read its own config.
//
// A DEFINITIVE object-lock-removed answer (ObjectLockConfigurationNotFoundError / HTTP 404), a
// present-but-not-"Enabled" config, OR object-lock Enabled but with NO DEFAULT RETENTION RULE is DRIFT
// (lock=false), returned REGARDLESS of the subsequent versioning read: versioning is read only
// best-effort for the detail, so a transient versioning-read failure can no longer DISCARD a definitive
// drift and mask it as "unverifiable".
//
// The default-retention-rule check is load-bearing: the worker PUTs objects with NO per-object
// retention header (putStream uses only PartSize+SSE), so an object is immutable ONLY if the bucket's
// object-lock DEFAULT retention rule applies one. GetObjectLockConfig returns that rule's mode
// (nil when no default rule is configured); object-lock Enabled but mode==nil means new objects get NO
// retention (deletable/overwritable) — the WORM guarantee is lost even though the Enabled flag is still
// on, so it MUST read as drift, not Verified.
func (s *Sink) CheckImmutability(ctx context.Context) (objectLock, versioning bool, err error) {
	// Read via the AUDIT credential (ImmutabilityReadable gates the caller, so it is non-nil here); the
	// worker's own PutObject-only credential can't read these.
	rc := s.readClient()
	lockStatus, mode, _, _, lerr := rc.GetObjectLockConfig(ctx, s.bucket)
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
	case mode == nil || *mode == "":
		// Definitive: object-lock Enabled but NO default retention rule → the worker's no-retention-header
		// PUTs land unretained → WORM guarantee lost → DRIFT; versioning best-effort.
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

// readClient returns the credential used for EVERY read operation (Exists, Fetch, bucket-existence,
// the immutability drift-check, and inventory/manifest reads): the audit/read client when configured,
// else the worker client. On a normal (non-WORM) target no audit client is set, so the worker client
// — which reads AND writes — is the real read path. On a WORM target the worker client is
// PutObject-only, so a read WITHOUT an audit client 403s, surfacing as Unverifiable / read-deny (the
// by-design write-only copy), never a false "absent"; WITH an audit client the WORM target becomes
// fully readable, so existence/inventory/immutability all verify uniformly with a filesystem target.
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

// LatestManifest returns the newest ledger-snapshot manifest object that STILL EXISTS under
// _manifest/ — the s3 sink's half of domain's optional inventoryReader capability (audit
// target→ledger). Selection + the stale/deleted-pointer fallback are owned by the shared
// fsutil.OpenLatestManifest (pointer fast-path, else a prefix scan filtered to VALID timestamped
// manifests; if the selected tip is GONE it scans for the newest SURVIVING manifest), so s3 and
// filesystem can't diverge. The open closure reads via the audit/read credential and EAGERLY confirms
// the object (a Stat) so the outcome surfaces NOW: a vanished object → a wrapped os.ErrNotExist (the
// resolver falls back / the domain maps it to NoData), a 404 that is really a gone/unreachable BUCKET
// (bucketPresentUncached) → an error (Unverifiable, not a benign "no manifest"), a read-deny → an error
// (Unverifiable) — matching the filesystem sink (sink parity).
func (s *Sink) LatestManifest(ctx context.Context) (io.ReadCloser, error) {
	// Upfront bucket confirmation, symmetric with the filesystem sink's confirmRoot: a gone/unreachable
	// bucket surfaces as an ERROR (→ Unverifiable) even if a backend were to return an EMPTY non-erroring
	// LIST for it — otherwise OpenLatestManifest would resolve name="" → NoData, and a vanished target
	// would wrongly read as "no manifest yet" (lost nothing). UNCACHED (bucketPresentUncached, not the
	// existence direction's cached confirmBucket): the cache — populated PRESENT by a prior existence
	// sweep sharing this *Sink — would otherwise defeat this guard and mask a vanished bucket as NoData.
	if err := s.bucketPresentUncached(ctx); err != nil {
		return nil, err
	}
	return fsutil.OpenLatestManifest(
		func() (string, bool) { return s.readManifestPointer(ctx) },
		func(after string) ([]string, error) { return s.listManifestNamesAfter(ctx, after) },
		func(name string) (io.ReadCloser, error) {
			obj, err := s.readClient().GetObject(ctx, s.bucket, s.prefixed(fsutil.ManifestKey(name)), minio.GetObjectOptions{})
			if err != nil {
				return nil, fmt.Errorf("get manifest %s: %w", name, err)
			}
			if _, serr := obj.Stat(); serr != nil {
				_ = obj.Close()
				if isNoSuchKey(serr) {
					// A 404 on the object HEAD can be a gone OBJECT or a gone BUCKET (NoSuchBucket is also
					// 404). Distinguish with a FRESH (uncached) bucket probe: a present bucket → the manifest
					// vanished (os.ErrNotExist, the resolver tries an older one); a gone/unreachable bucket →
					// an error (Unverifiable). Uncached because a cached-present verdict (from the existence
					// sweep) would mask a bucket that vanished in the list→open TOCTOU.
					if berr := s.bucketPresentUncached(ctx); berr != nil {
						return nil, berr
					}
					return nil, fmt.Errorf("manifest %s vanished: %w", name, os.ErrNotExist)
				}
				return nil, fmt.Errorf("stat manifest %s: %w", name, serr) // read-deny / other → unverifiable
			}
			return obj, nil
		})
}

// readManifestPointer reads the `_manifest/LATEST` pointer's raw contents (a manifest base name),
// returning ok=false when the pointer is absent/unreadable/empty (or a 403 read-deny) so
// OpenLatestManifest falls back to the scan — which surfaces the same 403 as the audit's
// unverifiable signal. The name is VALIDATED by OpenLatestManifest, not here.
func (s *Sink) readManifestPointer(ctx context.Context) (string, bool) {
	obj, err := s.readClient().GetObject(ctx, s.bucket, s.prefixed(fsutil.ManifestLatestKey()), minio.GetObjectOptions{})
	if err != nil {
		return "", false
	}
	defer func() { _ = obj.Close() }()
	b, err := io.ReadAll(io.LimitReader(obj, 4096)) // a base name — tiny
	if err != nil {
		return "", false // includes a lazy 404/403 surfacing here → fall back to the scan
	}
	return fsutil.ParseManifestPointer(b) // shared encoding — the s3 + filesystem sinks can't diverge
}

// listManifestNamesAfter lists the _manifest/ prefix for object base names STRICTLY AFTER `after`
// (a bounded StartAfter list — when `after` is the LATEST pointer this returns only manifests newer
// than it, so OpenLatestManifest cheaply detects a stale pointer; `after==""` is a full scan). The
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
	for obj := range s.readClient().ListObjects(lctx, s.bucket, opts) {
		if obj.Err != nil {
			return nil, fmt.Errorf("list manifests under %s: %w", prefix, obj.Err)
		}
		names = append(names, path.Base(obj.Key))
	}
	return names, nil
}
