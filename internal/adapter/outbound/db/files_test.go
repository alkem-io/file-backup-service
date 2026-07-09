package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pashagolub/pgxmock/v4"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

func newMockFiles(t *testing.T) (*FileRepo, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock: %v", err)
	}
	t.Cleanup(mock.Close)
	return NewFileRepo(mock), mock
}

func TestFilesEachFile(t *testing.T) {
	r, mock := newMockFiles(t)
	fid, cb := uuid.New(), uuid.New()
	now := time.Now()
	// First keyset page yields one file; the second (empty) page ends the loop.
	mock.ExpectQuery("FROM file").WithArgs(anyArgs(2)...).
		WillReturnRows(pgxmock.NewRows([]string{"id", "externalID", "createdBy", "createdDate", "size"}).
			AddRow(fid, "hashA", uuid.NullUUID{UUID: cb, Valid: true}, pgtype.Timestamptz{Time: now, Valid: true}, int64(77)))
	mock.ExpectQuery("FROM file").WithArgs(anyArgs(2)...).
		WillReturnRows(pgxmock.NewRows([]string{"id", "externalID", "createdBy", "createdDate", "size"}))

	var got []domain.BackupItem
	err := r.EachFile(context.Background(), func(e domain.BackupItem) error {
		got = append(got, e)
		return nil
	})
	if err != nil {
		t.Fatalf("each file: %v", err)
	}
	if len(got) != 1 || got[0].FileID != fid || got[0].ExternalID != "hashA" || got[0].Size != 77 {
		t.Fatalf("scanned corpus file mismatch: %+v", got)
	}
}

func TestFilesEachFileQueryError(t *testing.T) {
	r, mock := newMockFiles(t)
	mock.ExpectQuery("FROM file").WithArgs(anyArgs(2)...).WillReturnError(errors.New("no grant"))
	err := r.EachFile(context.Background(), func(domain.BackupItem) error { return nil })
	if err == nil {
		t.Fatal("a query error must abort the sweep")
	}
}

func TestFilesEachFileCallbackErrorStops(t *testing.T) {
	r, mock := newMockFiles(t)
	mock.ExpectQuery("FROM file").WithArgs(anyArgs(2)...).
		WillReturnRows(pgxmock.NewRows([]string{"id", "externalID", "createdBy", "createdDate", "size"}).
			AddRow(uuid.New(), "h", uuid.NullUUID{}, pgtype.Timestamptz{}, int64(1)))
	sentinel := errors.New("stop")
	err := r.EachFile(context.Background(), func(domain.BackupItem) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("a callback error must stop and propagate, got %v", err)
	}
}

func TestFileByID(t *testing.T) {
	fid := uuid.New()
	ver := time.Now().Add(-time.Hour)

	t.Run("found", func(t *testing.T) {
		r, mock := newMockFiles(t)
		mock.ExpectQuery("FROM file WHERE id").WithArgs(fid).
			WillReturnRows(pgxmock.NewRows([]string{"externalID", "versionTime"}).
				AddRow(pgtype.Text{String: "hashZ", Valid: true}, pgtype.Timestamptz{Time: ver, Valid: true}))
		ext, vt, found, err := r.FileByID(context.Background(), fid)
		if err != nil || !found || ext != "hashZ" || !vt.Equal(ver) {
			t.Fatalf("found file: ext=%q vt=%v found=%v err=%v", ext, vt, found, err)
		}
	})

	t.Run("no-row", func(t *testing.T) {
		r, mock := newMockFiles(t)
		mock.ExpectQuery("FROM file WHERE id").WithArgs(fid).WillReturnError(pgx.ErrNoRows)
		_, _, found, err := r.FileByID(context.Background(), fid)
		if err != nil || found {
			t.Fatalf("an unknown id must be (found=false, nil), got found=%v err=%v", found, err)
		}
	})

	t.Run("null-externalID", func(t *testing.T) {
		r, mock := newMockFiles(t)
		// A file row with no content hash yet (NULL externalID) → not backup-restorable → found=false.
		mock.ExpectQuery("FROM file WHERE id").WithArgs(fid).
			WillReturnRows(pgxmock.NewRows([]string{"externalID", "versionTime"}).
				AddRow(pgtype.Text{}, pgtype.Timestamptz{Time: ver, Valid: true}))
		_, _, found, err := r.FileByID(context.Background(), fid)
		if err != nil || found {
			t.Fatalf("a NULL externalID must be (found=false, nil), got found=%v err=%v", found, err)
		}
	})

	t.Run("query-error", func(t *testing.T) {
		r, mock := newMockFiles(t)
		mock.ExpectQuery("FROM file WHERE id").WithArgs(fid).WillReturnError(errors.New("boom"))
		if _, _, _, err := r.FileByID(context.Background(), fid); err == nil {
			t.Fatal("a query error must propagate")
		}
	})
}

func TestFilesProbe(t *testing.T) {
	r, mock := newMockFiles(t)
	mock.ExpectExec("FROM file").WillReturnResult(pgxmock.NewResult("SELECT", 0))
	if err := r.Probe(context.Background()); err != nil {
		t.Fatalf("probe: %v", err)
	}
	t.Run("no-grant-fails", func(t *testing.T) {
		r, mock := newMockFiles(t)
		mock.ExpectExec("FROM file").WillReturnError(errors.New("permission denied for table file"))
		if err := r.Probe(context.Background()); err == nil {
			t.Fatal("a missing SELECT grant on file must fail the probe")
		}
	})
}
