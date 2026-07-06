package db

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

// EachFile invokes fn for every non-temporary file (the backfill work-list), ordered by
// id for a stable, resumable pass. It KEYSET-PAGES the `file` table (id > after ORDER BY
// id LIMIT), releasing the pool connection between pages — a backfill runs the full
// rate-limited BackupOne per object, so a held cursor would pin one connection + an open
// snapshot on the SHARED production Alkemio DB for the whole (hours-to-days) pass,
// holding back xmin and blocking autovacuum. Paging keeps each query short.
// temporaryLocation IS NOT TRUE (not "= FALSE") so a NULL never silently drops a file.
func (r *FileRepo) EachFile(ctx context.Context, fn func(domain.OutboxEntry) error) error {
	after := uuid.Nil // the zero uuid sorts first; no real file.id is nil (gen_random_uuid)
	for {
		page, err := r.filesPage(ctx, after, dbPageSize)
		if err != nil {
			return err
		}
		for i := range page {
			if err := fn(page[i]); err != nil {
				return err
			}
		}
		if len(page) < dbPageSize {
			return nil // a short page is the last
		}
		after = page[len(page)-1].FileID
	}
}

// filesPage returns one keyset page of non-temporary files after `after` (id order).
func (r *FileRepo) filesPage(ctx context.Context, after uuid.UUID, limit int) ([]domain.OutboxEntry, error) {
	// Native uuid: id → FileID, createdBy → CreatedBy (pgx scans both directly). The `file`
	// PK on id serves the id > $1 ORDER BY id keyset.
	const q = `SELECT id, "externalID", "createdBy", "createdDate", size
	FROM file WHERE "temporaryLocation" IS NOT TRUE AND id > $1 ORDER BY id LIMIT $2`
	rows, err := r.p.Query(ctx, q, after, limit)
	if err != nil {
		return nil, fmt.Errorf("files page: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.OutboxEntry, error) {
		var e domain.OutboxEntry
		var createdDate pgtype.Timestamptz
		if err := row.Scan(&e.FileID, &e.ExternalID, &e.CreatedBy, &createdDate, &e.Size); err != nil {
			return e, err
		}
		e.CreatedDate = nullTime(createdDate)
		return e, nil
	})
}

// Probe verifies the `file` table is readable via the scoped role AND has every column
// EachFile reads (id/externalID/createdBy/createdDate/size/temporaryLocation) — so a
// missing SELECT grant OR a server-side schema/column drift fails LOUD at startup, not
// mid-pass when EachFile's Scan hits a renamed column (mirrors OutboxRepo.Probe).
func (r *FileRepo) Probe(ctx context.Context) error {
	const q = `SELECT id, "externalID", "createdBy", "createdDate", size, "temporaryLocation"
	FROM file LIMIT 1`
	if _, err := r.p.Exec(ctx, q); err != nil {
		return fmt.Errorf("file table not readable (scoped role SELECT grant / schema drift on file?): %w", err)
	}
	return nil
}
