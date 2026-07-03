// Package s3 implements the Sink port over S3-compatible object storage
// (Scaleway Object Storage), with server-side encryption and 2-level hex
// sharding. Object-lock/WORM retention is a bucket-level policy (infra-ops); the
// worker's credentials are PutObject-only on the immutable target.
package s3

import (
	"context"
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
	opts := minio.PutObjectOptions{}
	if s.sse != nil {
		opts.ServerSideEncryption = s.sse
	}
	return opts
}

// Exists reports whether the object is present. On an immutable target the
// worker's credentials are PutObject-only, so a HEAD returns 403 — treat that
// (and a 404) as "not present / unknown" rather than a hard error, since the
// ledger is the authoritative dedup source anyway.
func (s *Sink) Exists(ctx context.Context, hash string) (bool, error) {
	_, err := s.client.StatObject(ctx, s.bucket, s.key(hash), minio.StatObjectOptions{})
	if err != nil {
		resp := minio.ToErrorResponse(err)
		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusForbidden ||
			resp.Code == "NoSuchKey" || resp.Code == "AccessDenied" {
			return false, nil
		}
		return false, fmt.Errorf("stat: %w", err)
	}
	return true, nil
}

// Store uploads bytes for hash (SSE applied; bucket default retention provides WORM).
func (s *Sink) Store(ctx context.Context, hash string, r io.Reader, size int64) (int64, error) {
	info, err := s.client.PutObject(ctx, s.bucket, s.key(hash), r, size, s.putOpts())
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
	key := path.Join(s.prefix, "_manifest", name)
	if _, err := s.client.PutObject(ctx, s.bucket, key, r, -1, s.putOpts()); err != nil {
		return fmt.Errorf("put manifest: %w", err)
	}
	return nil
}
