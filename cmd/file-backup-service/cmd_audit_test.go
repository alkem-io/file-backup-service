package main

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"

	"github.com/alkem-io/file-backup-service/internal/adapter/outbound/db"
	"github.com/alkem-io/file-backup-service/internal/domain"
)

// immCmdSink is a fakeSink that also reports a fixed object-lock/versioning verdict — so the
// auditImmutability report branches (ok / drift) can be driven without a live S3 backend.
type immCmdSink struct {
	*fakeSink
	lock, versioning bool
	err              error
}

func (s immCmdSink) CheckImmutability(context.Context) (bool, bool, error) {
	return s.lock, s.versioning, s.err
}

func TestAuditImmutabilityBranches(t *testing.T) {
	ctx := context.Background()
	// ok: a verifiable WORM target with lock + versioning enabled passes.
	if err := auditImmutability(ctx, []domain.Target{
		{Sink: immCmdSink{fakeSink: &fakeSink{name: "ok"}, lock: true, versioning: true}, Worm: true},
	}); err != nil {
		t.Fatalf("ok immutability must pass, got %v", err)
	}
	// drift: object-lock disabled on a verifiable WORM target fails the audit.
	if err := auditImmutability(ctx, []domain.Target{
		{Sink: immCmdSink{fakeSink: &fakeSink{name: "drift"}, lock: false, versioning: true}, Worm: true},
	}); err == nil {
		t.Fatal("a WORM target with disabled object-lock must fail the audit")
	}
	// unverifiable: a WORM target whose sink can't report object-lock (a fakeSink — no capability)
	// is unverifiable, not a failure.
	if err := auditImmutability(ctx, []domain.Target{{Sink: &fakeSink{name: "fsworm"}, Worm: true}}); err != nil {
		t.Fatalf("an unverifiable WORM target must not fail, got %v", err)
	}
	// no WORM targets → nothing checked, nil verdict.
	if err := auditImmutability(ctx, []domain.Target{{Sink: &fakeSink{name: "plain"}}}); err != nil {
		t.Fatalf("no WORM targets must yield a nil verdict, got %v", err)
	}
}

func TestAuditInventoryUnverifiableBranch(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock: %v", err)
	}
	t.Cleanup(mock.Close)
	ledger := db.NewLedgerRepo(mock)
	// A WORM target whose sink can't enumerate a manifest (fakeSink — no LatestManifest) is
	// unverifiable-benign: auditInventory prints it and the ledger is never queried (no
	// expectations), so the joined verdict is nil.
	if err := auditInventory(context.Background(), ledger, []domain.Target{{Sink: &fakeSink{name: "nocap"}, Worm: true}}); err != nil {
		t.Fatalf("an unverifiable-benign inventory target must yield a nil verdict, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("no ledger query should have run for an unverifiable target: %v", err)
	}
}
