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
	UseSSL    bool
	SSE       bool
}

// Sink is the S3-compatible target.
type Sink struct {
	name   string
	client *minio.Client
	bucket string
	prefix string
	opts   minio.PutObjectOptions // constant for the sink's life (PartSize + SSE)

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
	return &Sink{name: cfg.Name, client: client, bucket: cfg.Bucket, prefix: cfg.Prefix, opts: opts}, nil
}

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

// confirmBucket verifies the bucket exists, so Exists never mistakes a missing bucket for a
// missing object. The verdict is cached: BucketExists runs at most ONCE per sink (on the first
// 404), then every later probe reads the cached result — so a full audit costs one bucket HEAD,
// not one per object. A DEFINITIVE answer (present or gone) is cached; a transient error is NOT
// (the next probe re-checks), fail-closed so an integrity check can't pass a bucket it couldn't
// confirm. The lock is held across the one network call so 16 concurrent audit probes collapse
// to a single BucketExists rather than a thundering herd.
func (s *Sink) confirmBucket(ctx context.Context) error {
	s.bucketMu.Lock()
	defer s.bucketMu.Unlock()
	if s.bucketChecked {
		return s.bucketGone
	}
	ok, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("bucket %q existence check: %w", s.bucket, err) // transient — don't cache; re-check next probe
	}
	s.bucketChecked = true
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

// Fetch streams the stored object.
func (s *Sink) Fetch(ctx context.Context, hash string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, s.key(hash), minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get: %w", err)
	}
	return obj, nil
}

// PutManifest writes a ledger snapshot object under _manifest/ (empty-safe: an empty
// ledger snapshot is a legitimate 0-byte object).
func (s *Sink) PutManifest(ctx context.Context, name string, r io.Reader) error {
	_, err := s.putStream(ctx, s.prefixed(fsutil.ManifestKey(name)), r)
	return err
}

// CheckImmutability reports whether the bucket still enforces object-lock AND versioning — the
// WORM drift-check (T032): a Scaleway/S3 immutable target loses its guarantee if either is
// disabled. It is the s3 sink's half of domain's optional immutabilityChecker capability (the
// domain type-asserts it). A read-denying (PutObject-only) credential 403s these GET calls, so
// the error is returned and the domain reports the target UNVERIFIABLE, not drifted — the
// immutable off-site copy's write-only credential legitimately can't read its own config.
func (s *Sink) CheckImmutability(ctx context.Context) (objectLock, versioning bool, err error) {
	lockStatus, _, _, _, lerr := s.client.GetObjectLockConfig(ctx, s.bucket)
	lockEnabled := false
	if lerr != nil {
		// A DEFINITIVE "object-lock is not configured" answer — ObjectLockConfigurationNotFoundError
		// / HTTP 404 — is DRIFT (a WORM bucket that LOST its lock config), NOT unverifiable: report
		// lock=false so the caller flags it, rather than masking the loss as "couldn't read". Only a
		// 403 / transient / other error is unverifiable (we genuinely couldn't determine it — e.g. a
		// PutObject-only WORM credential that can't read its own config).
		resp := minio.ToErrorResponse(lerr)
		if resp.Code == "ObjectLockConfigurationNotFoundError" || resp.StatusCode == http.StatusNotFound {
			lockEnabled = false
		} else {
			return false, false, fmt.Errorf("object-lock config %q: %w", s.bucket, lerr)
		}
	} else {
		// GetObjectLockConfig returns the bucket's ObjectLockEnabled value ("Enabled" when the
		// bucket was created with object-lock).
		lockEnabled = strings.EqualFold(lockStatus, "Enabled")
	}
	ver, verr := s.client.GetBucketVersioning(ctx, s.bucket)
	if verr != nil {
		return false, false, fmt.Errorf("versioning config %q: %w", s.bucket, verr)
	}
	// Versioning is a hard prerequisite S3 enforces for object-lock, but check it explicitly so a
	// manual Suspend (which silently defeats WORM) is caught as drift, not masked by the lock flag
	// alone. A bucket with versioning never/no-longer enabled returns an empty config (Status="",
	// Enabled()=false), so a suspended/absent versioning surfaces as drift, not an error.
	return lockEnabled, ver.Enabled(), nil
}

// LatestManifest returns the newest ledger-snapshot manifest object under _manifest/ — the s3
// sink's half of domain's optional inventoryReader capability (audit target→ledger). Manifest
// names are UTC-nanosecond timestamps, so the lexicographically-highest key is the newest. When
// no manifest exists yet it returns a wrapped os.ErrNotExist (the domain maps that to
// "unverifiable — nothing to diff", not a failure). A read-denying credential's List/Get 403s
// surface as a plain error → the domain reports the target unverifiable.
func (s *Sink) LatestManifest(ctx context.Context) (io.ReadCloser, error) {
	latest, err := s.latestManifestKey(ctx)
	if err != nil {
		return nil, err
	}
	if latest == "" {
		return nil, fmt.Errorf("no manifest under %s: %w", s.prefixed(fsutil.ManifestDir()), os.ErrNotExist)
	}
	obj, err := s.client.GetObject(ctx, s.bucket, latest, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get manifest %s: %w", latest, err)
	}
	return obj, nil
}

// latestManifestKey lists the _manifest/ prefix and returns the highest (newest) key, or "" when
// none exist. The list runs under a CHILD ctx cancelled on return, so minio's producer goroutine
// is torn down whether the scan finishes or bails on an error (a leaked list goroutine per audit
// would otherwise accumulate). The subsequent GetObject uses the parent ctx (its reader outlives
// this function).
func (s *Sink) latestManifestKey(ctx context.Context) (string, error) {
	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	prefix := s.prefixed(fsutil.ManifestDir()) + "/"
	var latest string
	for obj := range s.client.ListObjects(lctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return "", fmt.Errorf("list manifests under %s: %w", prefix, obj.Err)
		}
		if obj.Key > latest {
			latest = obj.Key
		}
	}
	return latest, nil
}
