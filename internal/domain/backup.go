package domain

import "context"

// Pipeline backs up one object to all configured targets and verifies it.
type Pipeline struct {
	// Source fetches object bytes by file id.
	Source Source
	// Targets are the configured sinks (required + optional).
	Targets []Sink
}

// NewPipeline constructs a Pipeline.
func NewPipeline(src Source, targets []Sink) *Pipeline {
	return &Pipeline{Source: src, Targets: targets}
}

// BackupOne fetches the object by fileID and stores it (keyed by externalID) on
// each target, verifying the content hash.
//
// TODO(T016): stream fetch → per-target transform → Store → Verify → ledger
// upsert → mark outbox done when all required targets confirm. The scaffold
// wires the shape only.
func (p *Pipeline) BackupOne(ctx context.Context, fileID, externalID string) error {
	rc, err := p.Source.FetchContent(ctx, fileID)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	_ = externalID
	return ErrNotImplemented
}
