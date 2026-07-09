package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

// FileRepo reads the file-service `file` table (Alkemio DB) — the corpus source of
// truth for backfill (US2/T022): the authoritative list of what SHOULD be backed up,
// including objects created before this service (which the outbox never carried).
// Read-only; the scoped role needs SELECT on `file` (Probe fails loud if it doesn't).
//
// Constitution §IV waiver (as with OutboxRepo): these queries are hand-written pgx, not
// sqlc. `file` is a FOREIGN, server-owned table in the Alkemio DB — NOT in this repo's
// ledger migrations that sqlc's schema points at — so sqlc has no schema to type it. The
// column-covering Probe is the compensating control (a server-side schema drift fails at
// startup, not mid-backfill).
type FileRepo struct{ p PgxDB }

// NewFileRepo binds a FileRepo to the alkemio pool.
func NewFileRepo(p PgxDB) *FileRepo { return &FileRepo{p: p} }

// EachFile invokes fn for every non-temporary file (the backfill work-list), ordered by
// id for a stable, resumable pass. It KEYSET-PAGES the `file` table (id > after ORDER BY
// id LIMIT), releasing the pool connection between pages — a backfill runs the full
// rate-limited BackupOne per object, so a held cursor would pin one connection + an open
// snapshot on the SHARED production Alkemio DB for the whole (hours-to-days) pass,
// holding back xmin and blocking autovacuum. Paging keeps each query short.
// temporaryLocation IS NOT TRUE (not "= FALSE") so a NULL never silently drops a file.
func (r *FileRepo) EachFile(ctx context.Context, fn func(domain.BackupItem) error) error {
	// uuid.Nil sorts first; no real file.id is nil (gen_random_uuid).
	return domain.KeysetLoop(uuid.Nil, domain.KeysetPageSize,
		func(after uuid.UUID, limit int) ([]domain.BackupItem, error) { return r.filesPage(ctx, after, limit) },
		func(e domain.BackupItem) uuid.UUID { return e.FileID },
		fn)
}

// filesPage returns one keyset page of non-temporary files after `after` (id order).
func (r *FileRepo) filesPage(ctx context.Context, after uuid.UUID, limit int) ([]domain.BackupItem, error) {
	// Native uuid: id → FileID, createdBy → CreatedBy (pgx scans both directly). The `file`
	// PK on id serves the id > $1 ORDER BY id keyset.
	//
	// Defend against the FOREIGN, server-owned column NULLs exactly as the sibling outbox
	// Claim does (COALESCE(size,0)): a NULL size would fail Scan into int64 and abort the
	// WHOLE enumeration at that row — and since KeysetLoop can't advance its cursor past a
	// failed page, backfill would re-hit the same row and die on every re-run, permanently
	// stalling the sweep past the first bad row. Also skip a NULL/empty externalID: a file
	// with no content hash yet (not-yet-stored) has no backup key, so it isn't backfillable —
	// exclude it (it enters via the outbox once file-service stores it) rather than error the
	// sweep or fabricate an empty-key object.
	const q = `SELECT id, "externalID", "createdBy", "createdDate", COALESCE(size,0)
	FROM file WHERE "temporaryLocation" IS NOT TRUE AND "externalID" IS NOT NULL AND "externalID" <> ''
	  AND id > $1 ORDER BY id LIMIT $2`
	rows, err := r.p.Query(ctx, q, after, limit)
	if err != nil {
		return nil, fmt.Errorf("files page: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (domain.BackupItem, error) {
		var e domain.BackupItem
		var createdDate pgtype.Timestamptz
		if err := row.Scan(&e.FileID, &e.ExternalID, &e.CreatedBy, &createdDate, &e.Size); err != nil {
			return e, err
		}
		e.CreatedDate = nullTime(createdDate)
		return e, nil
	})
}

// FileByID resolves a file's CURRENT content hash (externalID) and the time its CURRENT version
// became live (COALESCE("updatedDate","createdDate") — last-modified, so a file that was REPLACED
// in place, same id + new externalID, reports the REPLACE time, not its original creation), so
// `restore version` can map a --file-id to the hash to restore. The `file` table holds only the
// CURRENT version, not its history — so `restore version --at <past>` uses it best-effort: if the
// current version became live at/before <at> it IS the version as of <at>; if it was replaced
// AFTER <at>, the historical hash is NOT in the live table (it needs DB PITR — see
// contracts/restore-and-ops.md), and the caller directs the operator to pass the PITR-recovered
// hash via --hash. found=false when no such file row (or a NULL/empty externalID — a not-yet-
// stored file has no backup key). Uses the last-modified time (fail-loud if `updatedDate` is
// absent) rather than createdDate, so a replaced file can't SILENTLY resolve to the wrong (current)
// version for a past --at. Hand-written pgx (§IV waiver: `file` is the foreign, server-owned table).
func (r *FileRepo) FileByID(ctx context.Context, id uuid.UUID) (externalID string, versionTime time.Time, found bool, err error) {
	const q = `SELECT "externalID", COALESCE("updatedDate", "createdDate") FROM file WHERE id = $1`
	var ext pgtype.Text
	var vt pgtype.Timestamptz
	if serr := r.p.QueryRow(ctx, q, id).Scan(&ext, &vt); serr != nil {
		if errors.Is(serr, pgx.ErrNoRows) {
			return "", time.Time{}, false, nil
		}
		return "", time.Time{}, false, fmt.Errorf("file by id: %w", serr)
	}
	if !ext.Valid || ext.String == "" {
		return "", nullTime(vt), false, nil // a file with no content hash yet has no backup to restore
	}
	return ext.String, nullTime(vt), true, nil
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
