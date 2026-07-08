package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v4"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

func newMockLedger(t *testing.T) (*LedgerRepo, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock: %v", err)
	}
	t.Cleanup(mock.Close)
	return NewLedgerRepo(mock), mock
}

// anyArgs returns n AnyArg matchers — for queries whose exact args (a jsonb blob, keyset
// cursors across pages) aren't the assertion under test; the SQL text and the scanned result
// are. pgxmock treats a missing WithArgs as "expect zero args", so multi-arg queries need this.
func anyArgs(n int) []any {
	a := make([]any, n)
	for i := range a {
		a[i] = pgxmock.AnyArg()
	}
	return a
}

func TestLedgerRecordBackup(t *testing.T) {
	r, mock := newMockLedger(t)
	mock.ExpectExec("file_backup_object").WithArgs(anyArgs(6)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	err := r.RecordBackup(context.Background(),
		domain.ObjectMeta{ExternalID: "hashA", Size: 10, SizeVerified: true, CreatedBy: uuid.NullUUID{}},
		[]domain.TargetStatus{{Target: "t1", State: domain.StateStored, StoredBytes: 10}})
	if err != nil {
		t.Fatalf("record backup: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	t.Run("error", func(t *testing.T) {
		r, mock := newMockLedger(t)
		mock.ExpectExec("file_backup_object").WithArgs(anyArgs(6)...).WillReturnError(errors.New("fk violation"))
		if err := r.RecordBackup(context.Background(), domain.ObjectMeta{ExternalID: "h"},
			[]domain.TargetStatus{{Target: "t", State: domain.StateStored}}); err == nil {
			t.Fatal("a record error must propagate")
		}
	})
}

func TestLedgerStoredTargets(t *testing.T) {
	r, mock := newMockLedger(t)
	mock.ExpectQuery("file_backup_target_status").WithArgs("hashA").
		WillReturnRows(pgxmock.NewRows([]string{"target"}).AddRow("t1").AddRow("t2"))
	set, err := r.StoredTargets(context.Background(), "hashA")
	if err != nil {
		t.Fatalf("stored targets: %v", err)
	}
	if !set["t1"] || !set["t2"] || len(set) != 2 {
		t.Fatalf("want {t1,t2}, got %v", set)
	}
}

func TestLedgerStoredExternalIDsPage(t *testing.T) {
	r, mock := newMockLedger(t)
	mock.ExpectQuery("file_backup_target_status").WithArgs("t1", "after", int32(1000)).
		WillReturnRows(pgxmock.NewRows([]string{"externalID"}).AddRow("h1").AddRow("h2"))
	ids, err := r.StoredExternalIDsPage(context.Background(), "t1", "after", 1000)
	if err != nil {
		t.Fatalf("stored external ids page: %v", err)
	}
	if len(ids) != 2 || ids[0] != "h1" || ids[1] != "h2" {
		t.Fatalf("want [h1 h2], got %v", ids)
	}
}

func TestLedgerStoredObjectsPage(t *testing.T) {
	r, mock := newMockLedger(t)
	cb := uuid.New()
	now := time.Now()
	mock.ExpectQuery("file_backup_object").WithArgs("t1", "", int32(1000)).
		WillReturnRows(pgxmock.NewRows([]string{"externalID", "size", "createdBy", "sourceCreatedDate"}).
			AddRow("h1", int64(5), uuid.NullUUID{UUID: cb, Valid: true}, now))
	objs, err := r.StoredObjectsPage(context.Background(), "t1", "", 1000)
	if err != nil {
		t.Fatalf("stored objects page: %v", err)
	}
	if len(objs) != 1 || objs[0].ExternalID != "h1" || objs[0].Size != 5 {
		t.Fatalf("scanned object mismatch: %+v", objs)
	}
}

func TestLedgerCoverageGaps(t *testing.T) {
	t.Run("counts", func(t *testing.T) {
		r, mock := newMockLedger(t)
		mock.ExpectQuery("file_backup_object").WithArgs([]string{"t1", "t2"}, int32(2)).
			WillReturnRows(pgxmock.NewRows([]string{"gaps"}).AddRow(int64(9)))
		n, err := r.CoverageGaps(context.Background(), []string{"t1", "t2"})
		if err != nil || n != 9 {
			t.Fatalf("want (9,nil), got (%d,%v)", n, err)
		}
	})
	t.Run("zero-targets-short-circuit", func(t *testing.T) {
		r, mock := newMockLedger(t) // NO expectation: an empty target set must issue no query
		n, err := r.CoverageGaps(context.Background(), nil)
		if err != nil || n != 0 {
			t.Fatalf("zero targets must be (0,nil) with no query, got (%d,%v)", n, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestLedgerLastVerifiedAge(t *testing.T) {
	t.Run("some-verified-ok", func(t *testing.T) {
		r, mock := newMockLedger(t)
		mock.ExpectQuery("file_backup_target_status").WithArgs([]string{"t1", "t2"}).
			WillReturnRows(pgxmock.NewRows([]string{"never", "age"}).AddRow(int64(1), 42.0))
		age, never, ok, err := r.LastVerifiedAge(context.Background(), []string{"t1", "t2"})
		if err != nil || age != 42.0 || never != 1 || !ok {
			t.Fatalf("want (42,1,true,nil), got (%v,%d,%v,%v)", age, never, ok, err)
		}
	})
	t.Run("none-verified-not-ok", func(t *testing.T) {
		r, mock := newMockLedger(t)
		mock.ExpectQuery("file_backup_target_status").WithArgs([]string{"t1", "t2"}).
			WillReturnRows(pgxmock.NewRows([]string{"never", "age"}).AddRow(int64(2), 0.0))
		_, never, ok, err := r.LastVerifiedAge(context.Background(), []string{"t1", "t2"})
		if err != nil || never != 2 || ok {
			t.Fatalf("all-never-verified must be ok=false, got never=%d ok=%v err=%v", never, ok, err)
		}
	})
	t.Run("zero-targets-short-circuit", func(t *testing.T) {
		r, mock := newMockLedger(t)
		_, _, ok, err := r.LastVerifiedAge(context.Background(), nil)
		if err != nil || ok {
			t.Fatalf("zero targets must be ok=false,nil with no query, got ok=%v err=%v", ok, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestLedgerProbe(t *testing.T) {
	r, mock := newMockLedger(t)
	mock.ExpectQuery("file_backup_object").
		WillReturnRows(pgxmock.NewRows([]string{"obj", "status"}).AddRow(true, false))
	if err := r.Probe(context.Background()); err != nil {
		t.Fatalf("probe: %v", err)
	}
	t.Run("missing-table-fails", func(t *testing.T) {
		r, mock := newMockLedger(t)
		mock.ExpectQuery("file_backup_object").WillReturnError(errors.New("relation does not exist"))
		if err := r.Probe(context.Background()); err == nil {
			t.Fatal("a missing ledger table must fail the probe")
		}
	})
}

func TestLedgerTargetGaps(t *testing.T) {
	r, mock := newMockLedger(t)
	// One page then an empty page ends the keyset loop.
	mock.ExpectQuery("file_backup_object").WithArgs(anyArgs(4)...).
		WillReturnRows(pgxmock.NewRows([]string{"externalID", "stored"}).
			AddRow("h1", []string{"t1"}))
	mock.ExpectQuery("file_backup_object").WithArgs(anyArgs(4)...).
		WillReturnRows(pgxmock.NewRows([]string{"externalID", "stored"}))

	var got []string
	err := r.TargetGaps(context.Background(), []string{"t1", "t2"}, func(id string, stored map[string]bool) error {
		got = append(got, id)
		if !stored["t1"] || stored["t2"] {
			t.Fatalf("gap %s: want stored={t1}, got %v", id, stored)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("target gaps: %v", err)
	}
	if len(got) != 1 || got[0] != "h1" {
		t.Fatalf("want [h1], got %v", got)
	}
}
