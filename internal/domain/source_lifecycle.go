package domain

import (
	"context"
	"errors"
	"fmt"
)

// FileReferences checks every file reference to a shared content hash, including temporary files.
type FileReferences interface {
	// HasReferences includes every document sharing this hash, regardless of temporary status.
	HasReferences(context.Context, string) (bool, error)
}

// SourceLifecycleLedger can retire failed metadata, but never a stored backup.
type SourceLifecycleLedger interface {
	// RecordBackup preserves existing stored statuses while recording missing copies.
	RecordBackup(context.Context, ObjectMeta, []TargetStatus) error
	// ForgetUnstored atomically refuses retirement if any target has a stored copy.
	ForgetUnstored(context.Context, string) (bool, error)
}

// ErrReferencedSourceGone identifies a missing primary blob that is still needed by a file.
// It deliberately does not wrap ErrSourceGone: the consumer must retry rather than skip it.
var ErrReferencedSourceGone = errors.New("source blob is missing but still referenced")

// SourceLifecycle classifies an observed source 404 consistently for replication and reconciliation.
type SourceLifecycle struct {
	files   FileReferences
	ledger  SourceLifecycleLedger
	targets []string
}

// NewSourceLifecycle binds the reference lookup and guarded ledger cleanup to the configured targets.
func NewSourceLifecycle(files FileReferences, ledger SourceLifecycleLedger, targets []Target) *SourceLifecycle {
	return &SourceLifecycle{files: files, ledger: ledger, targets: TargetNames(targets)}
}

// ResolveSourceGone is called only after the primary source returned ErrSourceGone.
// It returns true when failed-only ledger metadata was removed. Existing stored copies are retained.
func (s *SourceLifecycle) ResolveSourceGone(ctx context.Context, e BackupItem) (bool, error) {
	referenced, err := s.files.HasReferences(ctx, e.ExternalID)
	if err != nil {
		return false, fmt.Errorf("check source references: %w", err)
	}
	if !referenced {
		return s.ledger.ForgetUnstored(ctx, e.ExternalID)
	}
	statuses := make([]TargetStatus, len(s.targets))
	for i, name := range s.targets {
		statuses[i] = TargetStatus{Target: name, State: StateFailed}
	}
	// RecordBackup never downgrades stored copies. Record a first-fetch failure too, so
	// a referenced missing blob remains visible to the coverage sampler after retries end.
	if err := s.ledger.RecordBackup(ctx, ObjectMeta{
		ExternalID: e.ExternalID, Size: e.Size, CreatedBy: e.CreatedBy, SourceCreatedDate: e.CreatedDate,
	}, statuses); err != nil {
		return false, fmt.Errorf("record referenced missing source: %w", err)
	}
	return false, fmt.Errorf("%w: %s", ErrReferencedSourceGone, e.ExternalID)
}
