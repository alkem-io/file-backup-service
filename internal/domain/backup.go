package domain

import (
	"bytes"
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
	// Ledger records object + per-target status.
	Ledger Ledger
	// Targets are the configured sinks.
	Targets []Target
	// MinGain is the adaptive-compression keep threshold.
	MinGain float64
	// Metrics receives observations (Nop if unset).
	Metrics Metrics
}

// NewPipeline constructs a Pipeline with the default compression threshold.
func NewPipeline(src Source, ledger Ledger, targets []Target) *Pipeline {
	return &Pipeline{Source: src, Ledger: ledger, Targets: targets, MinGain: DefaultMinGain, Metrics: Nop{}}
}

// BackupOne fetches the object, verifies it against its content hash, and stores
// it on every target. Returns true when all REQUIRED targets stored successfully.
//
// The object is buffered in memory because adaptive compression must compare
// sizes. TODO(large-object streaming) for multi-hundred-MB objects.
func (p *Pipeline) BackupOne(ctx context.Context, e OutboxEntry) (bool, error) {
	rc, err := p.Source.FetchContent(ctx, e.FileID)
	if err != nil {
		return false, err
	}
	data, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return false, fmt.Errorf("read source: %w", err)
	}
	if ok, _ := Verify(e.ExternalID, bytes.NewReader(data)); !ok {
		return false, fmt.Errorf("source integrity: bytes do not match %s", e.ExternalID)
	}
	if err := p.Ledger.UpsertObject(ctx, ObjectMeta{ExternalID: e.ExternalID, Size: int64(len(data))}); err != nil {
		return false, fmt.Errorf("ledger object: %w", err)
	}
	allRequiredOK := true
	for _, t := range p.Targets {
		if err := p.storeOne(ctx, t, e.ExternalID, data); err != nil && t.Required {
			allRequiredOK = false
		}
	}
	return allRequiredOK, nil
}

func (p *Pipeline) storeOne(ctx context.Context, t Target, hash string, data []byte) error {
	name := t.Sink.Name()
	if present, err := t.Sink.Exists(ctx, hash); err == nil && present {
		p.Metrics.ObjectDedup(name)
		return p.Ledger.UpsertTargetStatus(ctx, hash, name, "stored", int64(len(data)))
	}
	payload := data
	if t.Codec == CodecZstd {
		if comp, kept, cerr := CompressAdaptive(data, p.MinGain); cerr == nil && kept {
			payload = comp
		}
	}
	n, err := t.Sink.Store(ctx, hash, bytes.NewReader(payload), int64(len(payload)))
	if err != nil {
		p.Metrics.ObjectFailed(name)
		_ = p.Ledger.UpsertTargetStatus(ctx, hash, name, "failed", 0)
		return err
	}
	p.Metrics.ObjectStored(name, n)
	return p.Ledger.UpsertTargetStatus(ctx, hash, name, "stored", n)
}
