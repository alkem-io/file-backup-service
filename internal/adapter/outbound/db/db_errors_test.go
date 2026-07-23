package db

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v4"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

// TestNewPoolParseError: a malformed DSN fails at ParseConfig BEFORE any network I/O, so a
// bad ledger/alkemio DSN is a fast, deterministic startup error (no live DB required).
func TestNewPoolParseError(t *testing.T) {
	_, err := NewPool(context.Background(), "postgres://u:p@localhost:5432/db?sslmode=bogus", 4, time.Second)
	if err == nil {
		t.Fatal("a malformed DSN must fail NewPool at parse time")
	}
}

// TestNewPoolValidDSNLazyConnect: a well-formed DSN with an explicit MaxConns and
// statement_timeout builds a pool WITHOUT eagerly connecting (MinConns defaults to 0), so
// NewPool returns a usable *Pool that the test closes — exercising the config-application
// branches (MaxConns + statement_timeout) that the parse-error case never reaches.
func TestNewPoolValidDSNLazyConnect(t *testing.T) {
	p, err := NewPool(context.Background(), "postgres://u:p@127.0.0.1:1/db", 7, 500*time.Millisecond)
	if err != nil {
		t.Fatalf("a valid DSN must build a pool without connecting: %v", err)
	}
	if p == nil || p.Pool == nil {
		t.Fatal("NewPool must return a non-nil pool")
	}
	if got := p.Config().MaxConns; got != 7 {
		t.Fatalf("MaxConns = %d, want the explicit 7", got)
	}
	if got := p.Config().ConnConfig.RuntimeParams["statement_timeout"]; got != "500" {
		t.Fatalf("statement_timeout = %q, want 500 (ms)", got)
	}
	p.Close()
}

// TestLedgerStoredObjectsPageError: a query fault on the manifest-export page must propagate
// (an incomplete manifest must never read as an empty target inventory).
func TestLedgerStoredObjectsPageError(t *testing.T) {
	r, mock := newMockLedger(t)
	mock.ExpectQuery("file_backup_object").WithArgs("t1", "", int32(1000)).WillReturnError(errors.New("boom"))
	if _, err := r.StoredObjectsPage(context.Background(), "t1", "", 1000); err == nil {
		t.Fatal("a stored-objects page query error must propagate")
	}
}

// TestLedgerStoredExternalIDsPageError: a query fault on the audit sweep page must propagate.
func TestLedgerStoredExternalIDsPageError(t *testing.T) {
	r, mock := newMockLedger(t)
	mock.ExpectQuery("file_backup_target_status").WithArgs("t1", "", int32(1000)).WillReturnError(errors.New("boom"))
	if _, err := r.StoredExternalIDsPage(context.Background(), "t1", "", 1000); err == nil {
		t.Fatal("a stored-external-ids page query error must propagate")
	}
}

// TestLedgerCoverageGapsError: a query fault on the coverage backstop must propagate as an
// error, never a silent 0 (a backstop that under-reports is worse than none).
func TestLedgerCoverageGapsError(t *testing.T) {
	r, mock := newMockLedger(t)
	mock.ExpectQuery("file_backup_object").WithArgs([]string{"t1", "t2"}, int32(2)).WillReturnError(errors.New("boom"))
	if _, err := r.CoverageGaps(context.Background(), []string{"t1", "t2"}); err == nil {
		t.Fatal("a coverage-gaps query error must propagate")
	}
}

// TestLedgerLastVerifiedAgeError: a query fault on the RPO signal must propagate as ok=false
// with an error, never a clean zero-age reading.
func TestLedgerLastVerifiedAgeError(t *testing.T) {
	r, mock := newMockLedger(t)
	mock.ExpectQuery("file_backup_target_status").WithArgs([]string{"t1"}).WillReturnError(errors.New("boom"))
	if _, _, ok, err := r.LastVerifiedAge(context.Background(), []string{"t1"}); err == nil || ok {
		t.Fatalf("a last-verified-age query error must be (ok=false, err!=nil), got ok=%v err=%v", ok, err)
	}
}

// TestLedgerStoredTargetsError: a query fault on the dedup source-of-truth must propagate, so a
// backup never proceeds on a phantom empty stored-target set (which would re-store everywhere).
func TestLedgerStoredTargetsError(t *testing.T) {
	r, mock := newMockLedger(t)
	mock.ExpectQuery("file_backup_target_status").WithArgs("hashA").WillReturnError(errors.New("boom"))
	if _, err := r.StoredTargets(context.Background(), "hashA"); err == nil {
		t.Fatal("a stored-targets query error must propagate")
	}
}

// TestLedgerStoredCountByTargetError: a query fault on the restore-all pre-count must propagate, so a
// count error surfaces rather than a phantom empty/partial per-target snapshot.
func TestLedgerStoredCountByTargetError(t *testing.T) {
	r, mock := newMockLedger(t)
	mock.ExpectQuery("file_backup_target_status").WithArgs([]string{"t1", "t2"}).WillReturnError(errors.New("boom"))
	if _, err := r.StoredCountByTarget(context.Background(), []string{"t1", "t2"}); err == nil {
		t.Fatal("a stored-count-by-target query error must propagate")
	}
}

// TestLedgerTargetGapsPageError: a query fault on the reconcile work-list's first page must
// abort the sweep (an errored page must not read as "no gaps").
func TestLedgerTargetGapsPageError(t *testing.T) {
	r, mock := newMockLedger(t)
	mock.ExpectQuery("file_backup_object").WithArgs(anyArgs(4)...).WillReturnError(errors.New("boom"))
	err := r.TargetGaps(context.Background(), []string{"t1", "t2"}, func(string, map[string]bool) error { return nil })
	if err == nil {
		t.Fatal("a target-gaps page query error must abort the reconcile sweep")
	}
}

// TestLedgerTargetGapsCursorAdvance: a FULL first page (KeysetPageSize rows) forces the keyset
// loop to issue a SECOND query whose `after` cursor is the last externalID of page 1 — so a
// gap sweep past 1000 objects doesn't silently stop at the first page. The second (empty) page
// ends the loop. Asserting the second query's cursor arg defends the cursor-advance closure.
func TestLedgerTargetGapsCursorAdvance(t *testing.T) {
	r, mock := newMockLedger(t)
	targets := []string{"t1", "t2"}
	full := pgxmock.NewRows([]string{"externalID", "stored"})
	var lastID string
	for i := 0; i < domain.KeysetPageSize; i++ {
		lastID = fmt.Sprintf("%064d", i)
		full.AddRow(lastID, []string{"t1"})
	}
	mock.ExpectQuery("file_backup_object").WithArgs(anyArgs(4)...).WillReturnRows(full)
	// The cursor MUST be the last externalID of the full page; a wrong cursor fails this arg match.
	mock.ExpectQuery("file_backup_object").WithArgs(targets, lastID, int32(2), int32(1000)).
		WillReturnRows(pgxmock.NewRows([]string{"externalID", "stored"}))

	seen := 0
	err := r.TargetGaps(context.Background(), targets, func(string, map[string]bool) error { seen++; return nil })
	if err != nil {
		t.Fatalf("target gaps cursor advance: %v", err)
	}
	if seen != domain.KeysetPageSize {
		t.Fatalf("callback fired %d times, want %d (a full page then advance)", seen, domain.KeysetPageSize)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the second (cursor-advanced) page query must have been issued: %v", err)
	}
}

// TestLedgerRecordBackupNonZeroSourceDate: a non-zero SourceCreatedDate maps to a VALID
// timestamptz (timestamptzOrNull's set branch), distinct from the zero→NULL path the other
// RecordBackup test covers.
func TestLedgerRecordBackupNonZeroSourceDate(t *testing.T) {
	r, mock := newMockLedger(t)
	mock.ExpectExec("file_backup_object").WithArgs(anyArgs(6)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	err := r.RecordBackup(context.Background(),
		domain.ObjectMeta{ExternalID: "hashA", Size: 3, SourceCreatedDate: time.Now()},
		[]domain.TargetStatus{{Target: "t1", State: domain.StateStored, StoredBytes: 3}})
	if err != nil {
		t.Fatalf("record backup with a set source date: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// TestOutboxBacklogStatsError: a query fault on the backlog-depth/lag signal must propagate,
// never a clean (0,0) that would mask a wedged Alkemio DB behind a green RPO gauge.
func TestOutboxBacklogStatsError(t *testing.T) {
	r, mock := newMockOutbox(t)
	mock.ExpectQuery("file_backup_outbox").WillReturnError(errors.New("conn reset"))
	if _, _, err := r.BacklogStats(context.Background()); err == nil {
		t.Fatal("a backlog-stats query error must propagate")
	}
}

// TestFilesPageScanError: a row whose column types don't match the scan targets aborts the
// enumeration with the scan error (defends filesPage's per-row Scan error return — a drifted
// foreign `file` column must fail loudly, not silently drop rows).
func TestFilesPageScanError(t *testing.T) {
	r, mock := newMockFiles(t)
	// The row is missing the size column (4 values) but filesPage scans 5 destinations, so
	// Scan fails on the destination-count mismatch — a drifted foreign `file` shape.
	mock.ExpectQuery("FROM file").WithArgs(anyArgs(2)...).
		WillReturnRows(pgxmock.NewRows([]string{"id", "externalID", "createdBy", "createdDate"}).
			AddRow(uuid.New(), "h", uuid.NullUUID{}, nil))
	err := r.EachFile(context.Background(), func(domain.BackupItem) error { return nil })
	if err == nil {
		t.Fatal("a row that fails Scan must abort the enumeration with an error")
	}
}

// TestFilesEachFileCursorAdvance: a FULL first page forces filesPage's keyset loop to issue a
// second query keyed on the last file id — the corpus backfill must not stop at 1000 files.
func TestFilesEachFileCursorAdvance(t *testing.T) {
	r, mock := newMockFiles(t)
	full := pgxmock.NewRows([]string{"id", "externalID", "createdBy", "createdDate", "size"})
	ids := make([]uuid.UUID, domain.KeysetPageSize)
	for i := range ids {
		ids[i] = uuid.New()
		full.AddRow(ids[i], "h", uuid.NullUUID{}, nil, int64(1))
	}
	mock.ExpectQuery("FROM file").WithArgs(anyArgs(2)...).WillReturnRows(full)
	// Second page keyed on the last id of the full page; an empty page ends the loop.
	mock.ExpectQuery("FROM file").WithArgs(ids[len(ids)-1], 1000).
		WillReturnRows(pgxmock.NewRows([]string{"id", "externalID", "createdBy", "createdDate", "size"}))

	seen := 0
	if err := r.EachFile(context.Background(), func(domain.BackupItem) error { seen++; return nil }); err != nil {
		t.Fatalf("each file cursor advance: %v", err)
	}
	if seen != domain.KeysetPageSize {
		t.Fatalf("callback fired %d times, want %d", seen, domain.KeysetPageSize)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the second (cursor-advanced) page query must have been issued: %v", err)
	}
}
