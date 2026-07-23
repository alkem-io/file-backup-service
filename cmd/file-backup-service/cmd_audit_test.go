package main

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"

	"github.com/alkem-io/file-backup-service/internal/adapter/outbound/db"
	"github.com/alkem-io/file-backup-service/internal/domain"
)

// immCmdSink is a fakeSink that also reports a fixed object-lock/versioning verdict — so the
// immutability direction's verdict branches (verified / drift / unverifiable) can be driven through
// the cmd printAudit reduction without a live S3 backend.
type immCmdSink struct {
	*fakeSink
	lock, versioning bool
	err              error
}

func (s immCmdSink) CheckImmutability(context.Context) (bool, bool, error) {
	return s.lock, s.versioning, s.err
}

// reduce runs a direction's report through the cmd printAudit reducer and returns the returned
// verdict error — the ONE cmd-side reduction every audit direction shares.
func reduce(rep domain.VerdictReport) error {
	return printAudit("test", rep)
}

func TestPrintAuditImmutabilityBranches(t *testing.T) {
	ctx := context.Background()
	// verified: a WORM target with lock + versioning enabled passes.
	if err := reduce(domain.CheckImmutability(ctx, []domain.Target{
		{Sink: immCmdSink{fakeSink: &fakeSink{name: "ok"}, lock: true, versioning: true}, Worm: true},
	})); err != nil {
		t.Fatalf("verified immutability must pass, got %v", err)
	}
	// drift: object-lock disabled on a verifiable WORM target fails.
	if err := reduce(domain.CheckImmutability(ctx, []domain.Target{
		{Sink: immCmdSink{fakeSink: &fakeSink{name: "drift"}, lock: false, versioning: true}, Worm: true},
	})); err == nil {
		t.Fatal("a WORM target with disabled object-lock must fail the audit")
	}
	// no-data: a WORM target whose sink can't report object-lock (a fakeSink — no capability) is
	// benign, not a failure.
	if err := reduce(domain.CheckImmutability(ctx, []domain.Target{{Sink: &fakeSink{name: "fsworm"}, Worm: true}})); err != nil {
		t.Fatalf("a no-capability WORM target must not fail, got %v", err)
	}
	// no WORM targets → nothing checked, nil verdict.
	if err := reduce(domain.CheckImmutability(ctx, []domain.Target{{Sink: &fakeSink{name: "plain"}}})); err != nil {
		t.Fatalf("no WORM targets must yield a nil verdict, got %v", err)
	}
}

func TestPrintAuditInventoryNocapNoLedgerQuery(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock: %v", err)
	}
	t.Cleanup(mock.Close)
	ledger := db.NewLedgerRepo(mock)
	// A WORM target whose sink can't enumerate a manifest (fakeSink — no LatestManifest) is a benign
	// NoData verdict: the ledger is never queried (no expectations), so the reduced verdict is nil.
	if err := reduce(domain.AuditInventory(context.Background(), ledger, []domain.Target{{Sink: &fakeSink{name: "nocap"}, Worm: true}})); err != nil {
		t.Fatalf("a NoData inventory target must yield a nil verdict, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("no ledger query should have run for a NoData target: %v", err)
	}
}
