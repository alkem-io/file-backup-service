package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

// FileRepo reads the file-service `file` table (Alkemio DB) — the corpus source of
// truth for backfill (US2/T022): the authoritative list of what SHOULD be backed up,
// including objects created before this service (which the outbox never carried).
// Read-only; the scoped role needs SELECT on `file` (Probe fails loud if it doesn't).
type FileRepo struct{ p *Pool }

// NewFileRepo binds a FileRepo to the alkemio pool.
func NewFileRepo(p *Pool) *FileRepo { return &FileRepo{p: p} }

// EachFile streams every non-temporary file as a domain.OutboxEntry (the backfill
// work-list), ordered by id for a stable, resumable pass. temporaryLocation files are
// excluded — mid-upload staging the producer also never enqueues (data-model §1).
// Streaming (not ReadAll) keeps memory bounded across the whole corpus.
func (r *FileRepo) EachFile(ctx context.Context, fn func(domain.OutboxEntry) error) error {
	// Native uuid: id → OutboxEntry.FileID (uuid.UUID), createdBy → CreatedBy
	// (uuid.NullUUID) — pgx scans both directly, no ::text. temporaryLocation IS NOT TRUE
	// (not "= FALSE") so a NULL never silently drops a real object from the sweep.
	const q = `SELECT id, "externalID", "createdBy", "createdDate", size
	FROM file WHERE "temporaryLocation" IS NOT TRUE ORDER BY id`
	rows, err := r.p.Query(ctx, q)
	if err != nil {
		return fmt.Errorf("stream files: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var e domain.OutboxEntry
		var createdDate pgtype.Timestamptz
		if err := rows.Scan(&e.FileID, &e.ExternalID, &e.CreatedBy, &createdDate, &e.Size); err != nil {
			return fmt.Errorf("scan file: %w", err)
		}
		e.CreatedDate = nullTime(createdDate)
		if err := fn(e); err != nil {
			return err
		}
	}
	return rows.Err()
}

// Probe verifies the `file` table is readable via the pool's scoped role, so a missing
// SELECT grant fails loud at startup instead of a silent zero-object backfill.
func (r *FileRepo) Probe(ctx context.Context) error {
	if _, err := r.p.Exec(ctx, `SELECT 1 FROM file LIMIT 1`); err != nil {
		return fmt.Errorf("file table not readable (scoped role SELECT grant on file?): %w", err)
	}
	return nil
}
