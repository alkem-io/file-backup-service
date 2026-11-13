package db

import (
	"context"
	"errors"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
)

func TestLedgerForgetUnstored(t *testing.T) {
	for _, stored := range []bool{false, true} {
		r, mock := newMockLedger(t)
		mock.ExpectBegin()
		mock.ExpectQuery("LockLedgerObject").WithArgs("hash").
			WillReturnRows(pgxmock.NewRows([]string{"externalID"}).AddRow("hash"))
		rows := pgxmock.NewRows([]string{"target"})
		if stored {
			rows.AddRow("no-longer-configured-target")
		}
		mock.ExpectQuery("ListStoredTargets").WithArgs("hash").WillReturnRows(rows)
		if stored {
			mock.ExpectRollback()
		} else {
			mock.ExpectExec("DeleteUnstoredStatuses").WithArgs("hash").WillReturnResult(pgxmock.NewResult("DELETE", 2))
			mock.ExpectExec("DeleteEmptyLedgerObject").WithArgs("hash").WillReturnResult(pgxmock.NewResult("DELETE", 1))
			mock.ExpectCommit()
		}
		forgotten, err := r.ForgetUnstored(context.Background(), "hash")
		if err != nil || forgotten == stored {
			t.Fatalf("stored=%v forgotten=%v err=%v", stored, forgotten, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLedgerForgetAbsentIsIdempotent(t *testing.T) {
	r, mock := newMockLedger(t)
	mock.ExpectBegin()
	mock.ExpectQuery("LockLedgerObject").WithArgs("hash").WillReturnRows(pgxmock.NewRows([]string{"externalID"}))
	mock.ExpectRollback()
	forgotten, err := r.ForgetUnstored(context.Background(), "hash")
	if err != nil || forgotten {
		t.Fatalf("absent metadata must be a no-op: forgotten=%v err=%v", forgotten, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestLedgerForgetErrorsRollback(t *testing.T) {
	for _, stage := range []string{"begin", "lock", "copies", "statuses", "object", "commit"} {
		t.Run(stage, func(t *testing.T) {
			r, mock := newMockLedger(t)
			cause := errors.New("retirement fault")
			expectRetirementFailure(mock, stage, cause)
			forgotten, err := r.ForgetUnstored(context.Background(), "hash")
			if forgotten || !errors.Is(err, cause) {
				t.Fatalf("%s failure must propagate, forgotten=%v err=%v", stage, forgotten, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func expectRetirementFailure(mock pgxmock.PgxPoolIface, stage string, cause error) {
	begin := mock.ExpectBegin()
	if stage == "begin" {
		begin.WillReturnError(cause)
		return
	}
	lock := mock.ExpectQuery("LockLedgerObject").WithArgs("hash")
	if stage == "lock" {
		lock.WillReturnError(cause)
		mock.ExpectRollback()
		return
	}
	lock.WillReturnRows(pgxmock.NewRows([]string{"externalID"}).AddRow("hash"))
	copies := mock.ExpectQuery("ListStoredTargets").WithArgs("hash")
	if stage == "copies" {
		copies.WillReturnError(cause)
		mock.ExpectRollback()
		return
	}
	copies.WillReturnRows(pgxmock.NewRows([]string{"target"}))
	statuses := mock.ExpectExec("DeleteUnstoredStatuses").WithArgs("hash")
	if stage == "statuses" {
		statuses.WillReturnError(cause)
		mock.ExpectRollback()
		return
	}
	statuses.WillReturnResult(pgxmock.NewResult("DELETE", 1))
	object := mock.ExpectExec("DeleteEmptyLedgerObject").WithArgs("hash")
	if stage == "object" {
		object.WillReturnError(cause)
		mock.ExpectRollback()
		return
	}
	object.WillReturnResult(pgxmock.NewResult("DELETE", 1))
	mock.ExpectCommit().WillReturnError(cause)
	mock.ExpectRollback()
}
