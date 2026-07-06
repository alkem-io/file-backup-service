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
	"path"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"

	"github.com/alkem-io/file-backup-service/internal/domain"
	"github.com/alkem-io/file-backup-service/internal/fsutil"
)

// s3MaxParts is S3's hard limit on multipart parts per object.
const s3MaxParts = 10000

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
	// minio-go defaults to a ~528 MiB part (5 TiB / 10000 parts) and does
	// make([]byte, part) PER upload — which OOMs the co-located RWO node at concurrency.
	// Size it from domain.MaxObjectBytes so 10,000 parts admit any supported object (else
	// a 48–64 GiB object stores to a filesystem target but is rejected by S3, silently
	// dead-lettering on the symmetric done-gate). Rounded up to a MiB, floored at the
	// 5 MiB S3 minimum: live heap = concurrency x #targets x PartSize.
	opts := minio.PutObjectOptions{PartSize: s3PartSize()}
	if cfg.SSE {
		opts.ServerSideEncryption = encrypt.NewSSE()
	}
	return &Sink{name: cfg.Name, client: client, bucket: cfg.Bucket, prefix: cfg.Prefix, opts: opts}, nil
}

// s3PartSize is the smallest multipart part size (rounded to a MiB, floored at the S3
// 5 MiB minimum) whose 10,000-part ceiling still admits a domain.MaxObjectBytes object.
func s3PartSize() uint64 {
	const mib = 1 << 20
	per := (uint64(domain.MaxObjectBytes) + s3MaxParts - 1) / s3MaxParts // ceil bytes/part
	per = ((per + mib - 1) / mib) * mib                                  // round up to a MiB
	if per < 5*mib {
		per = 5 * mib
	}
	return per
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

// Exists reports whether the object is present. Only a definite 404/NoSuchKey is
// "absent"; a 403/AccessDenied (expected on a PutObject-only WORM credential, and
// also what a real credential/endpoint fault returns) is surfaced as an ERROR, not
// "absent", so a future reconcile never treats a permission fault as a gap to
// refill. Dedup is answered by the ledger, not this method.
func (s *Sink) Exists(ctx context.Context, hash string) (bool, error) {
	_, err := s.client.StatObject(ctx, s.bucket, s.key(hash), minio.StatObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.StatusCode == http.StatusNotFound || resp.Code == "NoSuchKey" {
			return false, nil
		}
		return false, fmt.Errorf("stat %s: %w", s.key(hash), err)
	}
	return true, nil
}

// putStream uploads r to key with SSE, size=-1 (streamed to EOF) so a commit is gated
// on the caller's upstream verification, never a known length. But a 0-byte stream then
// completes a multipart with an empty part, which Scaleway (and many S3 backends)
// reject (EntityTooSmall); detect empty with a single-byte read (no per-object bufio
// buffer) and use one empty PutObject, re-prepending the read byte via MultiReader.
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
