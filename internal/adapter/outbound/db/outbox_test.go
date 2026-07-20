package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"
)

// newMockOutbox returns an OutboxRepo backed by a pgxmock pool (no live DB), plus the mock to
// program expectations on. Constitution §VII: the pgx adapters are covered with pgxmock,
// asserting the exact SQL/params/scans, not against a container.
// maxAttempts/maxDeliveries the outbox tests run against — the mock returns canned results, so
// these only need to be the values the tests assert in WithArgs.
const (
	testMaxAttempts   = 10
	testMaxDeliveries = 50
)

func newMockOutbox(t *testing.T) (*OutboxRepo, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock: %v", err)
	}
	t.Cleanup(mock.Close)
	return NewOutboxRepo(mock, testMaxAttempts, testMaxDeliveries), mock
}

func TestOutboxClaim(t *testing.T) {
	r, mock := newMockOutbox(t)
	fid, cb := uuid.New(), uuid.New()
	now := time.Now()
	mock.ExpectQuery("file_backup_outbox").WithArgs(2).
		WillReturnRows(pgxmock.NewRows(
			[]string{"id", "fileId", "externalID", "size", "createdBy", "createdDate"}).
			AddRow(int64(7), fid, "hashA", int64(123), uuid.NullUUID{UUID: cb, Valid: true}, now))

	entries, err := r.Claim(context.Background(), 2)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	e := entries[0]
	if e.ID != 7 || e.FileID != fid || e.ExternalID != "hashA" || e.Size != 123 || e.CreatedBy.UUID != cb {
		t.Fatalf("scanned entry mismatch: %+v", e)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestOutboxClaimQueryError(t *testing.T) {
	r, mock := newMockOutbox(t)
	mock.ExpectQuery("file_backup_outbox").WithArgs(1).WillReturnError(errors.New("boom"))
	if _, err := r.Claim(context.Background(), 1); err == nil {
		t.Fatal("a query error must propagate")
	}
}

// TestOutboxTransitions covers MarkDone/Release/Skip — each an in_progress-guarded UPDATE that
// is a no-op if the claim is lost. Asserts the distinguishing SET clause reaches the DB.
func TestOutboxTransitions(t *testing.T) {
	cases := []struct {
		name   string
		run    func(*OutboxRepo) error
		expect string // a fragment the UPDATE must contain
	}{
		{"markdone", func(r *OutboxRepo) error { return r.MarkDone(context.Background(), 5) }, "status='done'"},
		{"release", func(r *OutboxRepo) error { return r.Release(context.Background(), 5) }, "visibleAt.*NULL"},
		{"skip", func(r *OutboxRepo) error { return r.Skip(context.Background(), 5) }, "status='skipped'"},
		{"defer", func(r *OutboxRepo) error { return r.Defer(context.Background(), 5) }, "attempts=0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, mock := newMockOutbox(t)
			mock.ExpectExec(tc.expect).WithArgs(int64(5)).WillReturnResult(pgxmock.NewResult("UPDATE", 1))
			if err := tc.run(r); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestOutboxTransitionError(t *testing.T) {
	r, mock := newMockOutbox(t)
	mock.ExpectExec("file_backup_outbox").WithArgs(int64(5)).WillReturnError(errors.New("db down"))
	if err := r.MarkDone(context.Background(), 5); err == nil {
		t.Fatal("an exec error must propagate")
	}
}

// TestOutboxFail covers the three Fail outcomes: dead-lettered=true at the limit, re-queued
// (false) below it, and a lost claim (ErrNoRows) → (false, nil) no-op.
func TestOutboxFail(t *testing.T) {
	t.Run("dead-lettered", func(t *testing.T) {
		r, mock := newMockOutbox(t)
		mock.ExpectQuery("file_backup_outbox").WithArgs(int64(3), 10, "boom").
			WillReturnRows(pgxmock.NewRows([]string{"dead"}).AddRow(true))
		dl, err := r.Fail(context.Background(), 3, "boom")
		if err != nil || !dl {
			t.Fatalf("want dead-lettered=true err=nil, got %v %v", dl, err)
		}
	})
	t.Run("requeued", func(t *testing.T) {
		r, mock := newMockOutbox(t)
		mock.ExpectQuery("file_backup_outbox").WithArgs(int64(3), 10, "boom").
			WillReturnRows(pgxmock.NewRows([]string{"dead"}).AddRow(false))
		dl, err := r.Fail(context.Background(), 3, "boom")
		if err != nil || dl {
			t.Fatalf("want dead-lettered=false err=nil, got %v %v", dl, err)
		}
	})
	t.Run("lost-claim-noop", func(t *testing.T) {
		r, mock := newMockOutbox(t)
		mock.ExpectQuery("file_backup_outbox").WithArgs(int64(3), 10, "boom").WillReturnError(pgx.ErrNoRows)
		dl, err := r.Fail(context.Background(), 3, "boom")
		if err != nil || dl {
			t.Fatalf("a lost claim must be a no-op (false,nil), got %v %v", dl, err)
		}
	})
	t.Run("db-error", func(t *testing.T) {
		r, mock := newMockOutbox(t)
		mock.ExpectQuery("file_backup_outbox").WithArgs(int64(3), 10, "boom").WillReturnError(errors.New("conn reset"))
		if _, err := r.Fail(context.Background(), 3, "boom"); err == nil {
			t.Fatal("a non-ErrNoRows error must propagate")
		}
	})
}

func TestOutboxProbe(t *testing.T) {
	t.Run("empty-table-ok", func(t *testing.T) {
		r, mock := newMockOutbox(t)
		mock.ExpectQuery("file_backup_outbox").WillReturnError(pgx.ErrNoRows)
		if err := r.Probe(context.Background()); err != nil {
			t.Fatalf("an empty outbox (ErrNoRows) must be success, got %v", err)
		}
	})
	t.Run("schema-drift-fails", func(t *testing.T) {
		r, mock := newMockOutbox(t)
		mock.ExpectQuery("file_backup_outbox").WillReturnError(errors.New(`column "visibleAt" does not exist`))
		if err := r.Probe(context.Background()); err == nil {
			t.Fatal("a missing column must fail the probe loudly")
		}
	})
}

func TestOutboxBacklogStats(t *testing.T) {
	r, mock := newMockOutbox(t)
	mock.ExpectQuery("file_backup_outbox").
		WillReturnRows(pgxmock.NewRows([]string{"pending", "age"}).AddRow(4, 92.5))
	pending, age, err := r.BacklogStats(context.Background())
	if err != nil || pending != 4 || age != 92.5 {
		t.Fatalf("want (4, 92.5, nil), got (%d, %v, %v)", pending, age, err)
	}
}

func TestOutboxCheckWriteGrant(t *testing.T) {
	t.Run("granted", func(t *testing.T) {
		r, mock := newMockOutbox(t)
		mock.ExpectExec("file_backup_outbox").WillReturnResult(pgxmock.NewResult("UPDATE", 0))
		if err := r.CheckWriteGrant(context.Background()); err != nil {
			t.Fatalf("granted write must pass, got %v", err)
		}
	})
	t.Run("denied", func(t *testing.T) {
		r, mock := newMockOutbox(t)
		mock.ExpectExec("file_backup_outbox").WillReturnError(errors.New("permission denied for table"))
		if err := r.CheckWriteGrant(context.Background()); err == nil {
			t.Fatal("a SELECT-only role must fail the write-grant check")
		}
	})
}

func TestOutboxReapStale(t *testing.T) {
	r, mock := newMockOutbox(t)
	ttl := 30 * time.Minute
	mock.ExpectQuery("file_backup_outbox").WithArgs(ttl.Seconds(), 50).
		WillReturnRows(pgxmock.NewRows([]string{"dead"}).AddRow(2))
	n, err := r.ReapStale(context.Background(), ttl)
	if err != nil || n != 2 {
		t.Fatalf("want (2, nil), got (%d, %v)", n, err)
	}
	t.Run("error", func(t *testing.T) {
		r, mock := newMockOutbox(t)
		mock.ExpectQuery("file_backup_outbox").WithArgs(ttl.Seconds(), 50).WillReturnError(errors.New("boom"))
		if _, err := r.ReapStale(context.Background(), ttl); err == nil {
			t.Fatal("reap error must propagate")
		}
	})
}

// SourceStillReferenced maps the file-corpus EXISTS query to a bool, and surfaces a query error.
func TestSourceStillReferenced(t *testing.T) {
	t.Run("referenced", func(t *testing.T) {
		repo, mock := newMockOutbox(t)
		mock.ExpectQuery("EXISTS").WithArgs("h1").
			WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
		got, err := repo.SourceStillReferenced(context.Background(), "h1")
		if err != nil || !got {
			t.Fatalf("got (%v,%v), want (true,nil)", got, err)
		}
	})
	t.Run("gone", func(t *testing.T) {
		repo, mock := newMockOutbox(t)
		mock.ExpectQuery("EXISTS").WithArgs("h2").
			WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(false))
		got, err := repo.SourceStillReferenced(context.Background(), "h2")
		if err != nil || got {
			t.Fatalf("got (%v,%v), want (false,nil)", got, err)
		}
	})
	t.Run("query error", func(t *testing.T) {
		repo, mock := newMockOutbox(t)
		mock.ExpectQuery("EXISTS").WithArgs("h3").WillReturnError(errors.New("boom"))
		if _, err := repo.SourceStillReferenced(context.Background(), "h3"); err == nil {
			t.Fatal("want error from a failed corpus query")
		}
	})
}
