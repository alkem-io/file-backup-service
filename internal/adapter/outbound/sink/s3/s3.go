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
	sse    encrypt.ServerSide
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
	s := &Sink{name: cfg.Name, client: client, bucket: cfg.Bucket, prefix: cfg.Prefix}
	if cfg.SSE {
		s.sse = encrypt.NewSSE()
	}
	return s, nil
}

// Name returns the target name.
func (s *Sink) Name() string { return s.name }

func (s *Sink) key(hash string) string {
	return path.Join(s.prefix, fsutil.ShardKey(hash))
}

func (s *Sink) putOpts() minio.PutObjectOptions {
	// Bound the streaming (size=-1) multipart buffer. For an unknown length minio-go
	// defaults to a ~528 MiB part (5 TiB / 10000 parts) and does make([]byte, part)
	// PER upload — which OOMs the co-located RWO node at concurrency. 5 MiB is the S3
	// multipart minimum: live heap = concurrency x #targets x 5 MiB, and it still
	// supports ~48 GiB objects (5 MiB x 10000 parts), far above this file corpus.
	opts := minio.PutObjectOptions{PartSize: 5 << 20}
	if s.sse != nil {
		opts.ServerSideEncryption = s.sse
	}
	return opts
}

// Preflight checks the target is reachable at startup rather than dead-lettering
// every object. BucketExists returns (true,nil) when the bucket exists and is
// introspectable, (false,nil) when it is MISSING, and an error when unreachable or
// access-denied. Note the limit: BucketExists is a HEAD, which has no body for
// minio-go to read the real S3 code from, so every 403 (a write-only WORM cred, a
// wrong secret, or a wrong key) collapses to "AccessDenied" — those are
// indistinguishable here and can only be caught by an actual write. So this catches
// the common misconfigs loudly (missing/typo'd bucket, unreachable endpoint, wrong
// region that isn't 403) and treats any 403 as "reachable, write-only".
func (s *Sink) Preflight(ctx context.Context) error {
	ok, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		if minio.ToErrorResponse(err).Code == "AccessDenied" {
			return nil // 403: write-only WORM cred (or an unverifiable wrong cred) — reachable
		}
		return fmt.Errorf("s3 preflight %q (creds/region/endpoint?): %w", s.name, err)
	}
	if !ok {
		return fmt.Errorf("s3 preflight %q: bucket %q does not exist", s.name, s.bucket)
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
		// Only a definite 404/NoSuchKey means absent. A 403/AccessDenied is NOT
		// "absent" — on a PutObject-only WORM credential HEAD is always denied, and
		// on a real credential/endpoint fault everything is denied; either way we
		// cannot conclude the object is missing, so surface it as an error rather
		// than letting a future reconcile treat a permission fault as a gap to refill.
		if resp.StatusCode == http.StatusNotFound || resp.Code == "NoSuchKey" {
			return false, nil
		}
		return false, fmt.Errorf("stat %s: %w", s.key(hash), err)
	}
	return true, nil
}

// Store uploads bytes for hash (SSE applied; bucket default retention provides WORM).
// size is always -1 (streamed) so minio reads to EOF and the commit is gated on the
// upstream VerifyReader hash check. But a 0-byte object then completes a multipart
// with an empty part, which Scaleway (and many S3 backends) reject (EntityTooSmall);
// detect empty with a single-byte read (no per-object bufio buffer) and use one empty
// PutObject instead, re-prepending the read byte via MultiReader for the normal path.
func (s *Sink) Store(ctx context.Context, hash string, r io.Reader, _ int64) (int64, error) {
	var one [1]byte
	n, err := io.ReadFull(r, one[:]) // 1-byte buf: (1,nil) if a byte, (0,io.EOF) if empty
	if errors.Is(err, io.EOF) {      // empty object (already hash-verified upstream)
		if _, perr := s.client.PutObject(ctx, s.bucket, s.key(hash), bytes.NewReader(nil), 0, s.putOpts()); perr != nil {
			return 0, fmt.Errorf("put (empty): %w", perr)
		}
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read: %w", err)
	}
	body := io.MultiReader(bytes.NewReader(one[:n]), r)
	info, err := s.client.PutObject(ctx, s.bucket, s.key(hash), body, -1, s.putOpts())
	if err != nil {
		return 0, fmt.Errorf("put: %w", err)
	}
	return info.Size, nil
}

// Fetch streams the stored object.
func (s *Sink) Fetch(ctx context.Context, hash string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, s.key(hash), minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("get: %w", err)
	}
	return obj, nil
}

// PutManifest writes a ledger snapshot object under _manifest/.
func (s *Sink) PutManifest(ctx context.Context, name string, r io.Reader) error {
	key := path.Join(s.prefix, fsutil.ManifestKey(name))
	if _, err := s.client.PutObject(ctx, s.bucket, key, r, -1, s.putOpts()); err != nil {
		return fmt.Errorf("put manifest: %w", err)
	}
	return nil
}
