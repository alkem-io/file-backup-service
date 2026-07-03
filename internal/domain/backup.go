package domain

import (
	"context"
	"fmt"
	"io"
)

// Target pairs a Sink with its per-target policy.
type Target struct {
	// Sink is the content-addressed store.
	Sink Sink
	// Required gates "done": every required target must store successfully.
	Required bool
	// Codec is the per-target transform (none | zstd).
	Codec Codec
}

// Pipeline backs up one object to all configured targets and updates the ledger.
type Pipeline struct {
	// Source fetches object bytes by file id.
	Source Source
	// Ledger records object + per-target status and answers dedup.
	Ledger Ledger
	// Targets are the configured sinks.
	Targets []Target
	// Metrics receives observations (Nop if unset).
	Metrics Metrics
}

// NewPipeline constructs a Pipeline.
func NewPipeline(src Source, ledger Ledger, targets []Target) *Pipeline {
	return &Pipeline{Source: src, Ledger: ledger, Targets: targets, Metrics: Nop{}}
}

// BackupOne stores the object on every target, fully streamed (bounded memory).
// Dedup is answered by our own ledger — never by re-reading the target — so it
// works with PutObject-only credentials on an immutable/WORM target. Returns true
// when all REQUIRED targets are stored.
func (p *Pipeline) BackupOne(ctx context.Context, e OutboxEntry) (bool, error) {
	allRequiredOK := true
	for _, t := range p.Targets {
		name := t.Sink.Name()
		if state, _, err := p.Ledger.TargetState(ctx, e.ExternalID, name); err == nil && state == "stored" {
			p.Metrics.ObjectDedup(name)
			continue
		}
		plaintext, stored, err := p.streamToTarget(ctx, t, e)
		if err != nil {
			p.Metrics.ObjectFailed(name)
			// Best-effort; no-ops if no object row exists yet (target_status FK).
			_ = p.Ledger.UpsertTargetStatus(ctx, e.ExternalID, name, "failed", 0)
			if t.Required {
				allRequiredOK = false
			}
			continue
		}
		// The object row must exist before its target_status (FK). UpsertObject is
		// idempotent (ON CONFLICT DO NOTHING), so repeating it per target is cheap.
		if err := p.Ledger.UpsertObject(ctx, ObjectMeta{ExternalID: e.ExternalID, Size: plaintext}); err != nil {
			return false, fmt.Errorf("ledger object: %w", err)
		}
		p.Metrics.ObjectStored(name, stored)
		if err := p.Ledger.UpsertTargetStatus(ctx, e.ExternalID, name, "stored", stored); err != nil {
			return false, fmt.Errorf("ledger target status: %w", err)
		}
	}
	return allRequiredOK, nil
}

// streamToTarget streams the object from the source through a hash-verifying
// reader (and the target codec) straight into the sink. The VerifyReader fails
// the stream at EOF on a hash mismatch, so the sink's atomic write never commits
// corrupt data. Returns the plaintext size and the stored (post-codec) size.
func (p *Pipeline) streamToTarget(ctx context.Context, t Target, e OutboxEntry) (plaintext int64, stored int64, err error) {
	rc, err := p.Source.FetchContent(ctx, e.FileID)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = rc.Close() }()

	vr := NewVerifyReader(rc, e.ExternalID)
	var reader io.Reader = vr
	if t.Codec == CodecZstd {
		zr := ZstdReader(vr)
		defer func() { _ = zr.Close() }()
		reader = zr
	}
	stored, err = t.Sink.Store(ctx, e.ExternalID, reader, -1)
	if err != nil {
		return 0, 0, err
	}
	return vr.Total, stored, nil
}
