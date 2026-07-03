package domain

import (
	"context"
	"fmt"
	"io"
	"sync"
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

// BackupOne stores the object on every target that still needs it. The source is
// fetched ONCE and fanned out to all targets concurrently (streamed, bounded
// memory), so adding a second target does not multiply read load on the primary
// store — FR: N configurable targets. Dedup is answered by our own ledger (never
// by re-reading a target), so it works with PutObject-only WORM credentials.
// Returns true when all REQUIRED targets are stored.
func (p *Pipeline) BackupOne(ctx context.Context, e OutboxEntry) (bool, error) {
	pending := make([]Target, 0, len(p.Targets))
	for _, t := range p.Targets {
		if state, _, err := p.Ledger.TargetState(ctx, e.ExternalID, t.Sink.Name()); err == nil && state == "stored" {
			p.Metrics.ObjectDedup(t.Sink.Name())
			continue
		}
		pending = append(pending, t)
	}
	if len(pending) == 0 {
		return true, nil // every target already has it
	}

	results, size, err := p.fanOut(ctx, e, pending)
	if err != nil {
		return false, err // source fetch failed — outbox retries the whole object
	}

	allRequiredOK := true
	objectRecorded := false
	for i, t := range pending {
		name := t.Sink.Name()
		if results[i].err != nil {
			p.Metrics.ObjectFailed(name)
			if objectRecorded {
				_ = p.Ledger.UpsertTargetStatus(ctx, e.ExternalID, name, "failed", 0)
			}
			if t.Required {
				allRequiredOK = false
			}
			continue
		}
		// Object row must exist before its target_status (FK); idempotent.
		if err := p.Ledger.UpsertObject(ctx, ObjectMeta{ExternalID: e.ExternalID, Size: size}); err != nil {
			return false, fmt.Errorf("ledger object: %w", err)
		}
		objectRecorded = true
		p.Metrics.ObjectStored(name, results[i].stored)
		if err := p.Ledger.UpsertTargetStatus(ctx, e.ExternalID, name, "stored", results[i].stored); err != nil {
			return false, fmt.Errorf("ledger target status: %w", err)
		}
	}
	return allRequiredOK, nil
}

type targetResult struct {
	stored int64
	err    error
}

// fanOut fetches the source once and copies the hash-verified plaintext to every
// target concurrently (each applying its own codec). A per-target failure is
// isolated — its pipe is dropped and the remaining targets keep receiving bytes.
// A corrupt source (VerifyReader fails at EOF) aborts every target's write, so
// nothing is committed anywhere. Returns per-target results and the plaintext size.
func (p *Pipeline) fanOut(ctx context.Context, e OutboxEntry, targets []Target) ([]targetResult, int64, error) {
	rc, err := p.Source.FetchContent(ctx, e.FileID)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = rc.Close() }()
	vr := NewVerifyReader(rc, e.ExternalID)

	n := len(targets)
	results := make([]targetResult, n)
	writers := make([]*io.PipeWriter, n)
	var wg sync.WaitGroup
	for i, t := range targets {
		pr, pw := io.Pipe()
		writers[i] = pw
		wg.Add(1)
		go func(i int, t Target, pr *io.PipeReader) {
			defer wg.Done()
			var reader io.Reader = pr
			if t.Codec == CodecZstd {
				zr := ZstdReader(pr)
				defer func() { _ = zr.Close() }()
				reader = zr
			}
			// size = -1 is load-bearing, not merely "unknown". Commit is gated on
			// the dispatcher closing the pipes AFTER VerifyReader checks the hash;
			// a known byte-count would let a sink finalize (e.g. minio single-PUT)
			// on length alone, before verification, committing unverified bytes.
			stored, serr := t.Sink.Store(ctx, e.ExternalID, reader, -1)
			results[i] = targetResult{stored: stored, err: serr}
			// Unblock the dispatcher if Store bailed before draining the pipe.
			_ = pr.CloseWithError(serr)
		}(i, t, pr)
	}

	fw := &fanoutWriter{writers: make([]io.Writer, n), dead: make([]bool, n)}
	for i := range writers {
		fw.writers[i] = writers[i]
	}
	_, copyErr := io.Copy(fw, vr) // copyErr set on a source integrity failure
	for _, pw := range writers {
		_ = pw.CloseWithError(copyErr)
	}
	wg.Wait()
	return results, vr.Total, nil
}

// fanoutWriter writes each chunk to every still-live target pipe. A pipe write
// error marks that target dead (its Store exited) and the rest continue; it
// stops the source copy only once every target is gone.
type fanoutWriter struct {
	writers []io.Writer
	dead    []bool
}

func (f *fanoutWriter) Write(p []byte) (int, error) {
	live := 0
	for i, w := range f.writers {
		if f.dead[i] {
			continue
		}
		if _, err := w.Write(p); err != nil {
			f.dead[i] = true
			continue
		}
		live++
	}
	if live == 0 {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}
