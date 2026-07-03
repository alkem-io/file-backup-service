// Package s3 implements the Sink port over S3-compatible object storage.
//
// TODO(T011): implement with aws-sdk-go-v2 (PutObject + HeadObject, SSE,
// object-lock on the immutable target, PutObject-only creds, 2-level sharding).
package s3

import (
	"context"
	"errors"
	"io"
)

var errNotImplemented = errors.New("s3 sink: not implemented (TODO T011)")

// Sink is the S3-compatible target.
type Sink struct {
	name string
}

// New constructs an S3 Sink. The endpoint/bucket/prefix/credential arguments are
// accepted here and wired in T011.
func New(name string, _, _, _ string) *Sink { return &Sink{name: name} }

// Name returns the target name.
func (s *Sink) Name() string { return s.name }

// Store is not yet implemented.
func (s *Sink) Store(context.Context, string, io.Reader, int64) (int64, error) {
	return 0, errNotImplemented
}

// Exists is not yet implemented.
func (s *Sink) Exists(context.Context, string) (bool, error) { return false, errNotImplemented }

// Fetch is not yet implemented.
func (s *Sink) Fetch(context.Context, string) (io.ReadCloser, error) { return nil, errNotImplemented }

// PutManifest is not yet implemented.
func (s *Sink) PutManifest(context.Context, string, io.Reader) error { return errNotImplemented }
