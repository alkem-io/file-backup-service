//go:build integration

package db

import (
	"context"
	"crypto/sha3"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alkem-io/file-backup-service/internal/domain"
	"github.com/alkem-io/file-backup-service/internal/testsupport/pg"
)

// harness is one Postgres container shared by the whole db integration suite (starting a
// container per test would be minutes of overhead). Migrations run once in TestMain, so the
// ledger tables exist before any ledger test.
var harness *pg.Harness

func TestMain(m *testing.M) {
	ctx := context.Background()
	h, err := pg.Start(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration harness:", err)
		os.Exit(1)
	}
	harness = h
	if err := Migrate(h.LedgerDSN()); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		h.Cleanup(ctx)
		os.Exit(1)
	}
	code := m.Run()
	h.Cleanup(ctx)
	os.Exit(code)
}

func ledgerPool(t *testing.T) *Pool {
	t.Helper()
	p, err := NewPool(context.Background(), harness.LedgerDSN(), 4, 30*time.Second)
	if err != nil {
		t.Fatalf("ledger pool: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

func alkemioPool(t *testing.T) *Pool {
	t.Helper()
	p, err := NewPool(context.Background(), harness.AlkemioDSN(), 4, 30*time.Second)
	if err != nil {
		t.Fatalf("alkemio pool: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

// TestIntegrationMigrateIdempotent: migrations already ran in TestMain; running them again is a
// no-op (ErrNoChange is not an error), and the ledger tables are present + queryable.
func TestIntegrationMigrateIdempotent(t *testing.T) {
	if err := Migrate(harness.LedgerDSN()); err != nil {
		t.Fatalf("re-migrate must be idempotent, got %v", err)
	}
	if err := NewLedgerRepo(ledgerPool(t)).Probe(context.Background()); err != nil {
		t.Fatalf("ledger tables must exist after migrate, probe: %v", err)
	}
}

// TestIntegrationNewPoolStatementTimeout: NewPool connects to a real DB and applies the
// server-side statement_timeout (the V3 hardening) — verified by reading it back.
func TestIntegrationNewPoolStatementTimeout(t *testing.T) {
	p := ledgerPool(t)
	var st string
	if err := p.QueryRow(context.Background(), "SHOW statement_timeout").Scan(&st); err != nil {
		t.Fatalf("show statement_timeout: %v", err)
	}
	if st != "30s" {
		t.Fatalf("statement_timeout = %q, want 30s (NewPool must set it)", st)
	}
}

// ledgerRT records one object on target "a" (of two configured targets) and returns the repo +
// its hash, so the read-side integration tests share one real-SQL fixture.
func ledgerRT(t *testing.T) (*LedgerRepo, string) {
	t.Helper()
	led := NewLedgerRepo(ledgerPool(t))
	h := sum64("ledger-rt")
	if err := led.RecordBackup(context.Background(),
		domain.ObjectMeta{ExternalID: h, Size: 42, SizeVerified: true},
		[]domain.TargetStatus{{Target: "a", State: domain.StateStored, StoredBytes: 42}}); err != nil {
		t.Fatalf("record backup: %v", err)
	}
	return led, h
}

// TestIntegrationLedgerStoredReads exercises the real per-object read SQL after a record.
func TestIntegrationLedgerStoredReads(t *testing.T) {
	ctx := context.Background()
	led, h := ledgerRT(t)

	stored, err := led.StoredTargets(ctx, h)
	if err != nil || !stored["a"] || stored["b"] {
		t.Fatalf("StoredTargets = %v err=%v (want {a})", stored, err)
	}
	ids, err := led.StoredExternalIDsPage(ctx, "a", "", 100)
	if err != nil || len(ids) != 1 || ids[0] != h {
		t.Fatalf("StoredExternalIDsPage(a) = %v err=%v (want [%s])", ids, err, h)
	}
	objs, err := led.StoredObjectsPage(ctx, "a", "", 100)
	if err != nil || len(objs) != 1 || objs[0].Size != 42 {
		t.Fatalf("StoredObjectsPage(a) = %+v err=%v", objs, err)
	}
}

// TestIntegrationLedgerGapReads exercises the real coverage/gap/RPO SQL: the object is
// under-replicated on target "b".
func TestIntegrationLedgerGapReads(t *testing.T) {
	ctx := context.Background()
	led, h := ledgerRT(t)
	targets := []string{"a", "b"}

	gaps, err := led.CoverageGaps(ctx, targets)
	if err != nil || gaps != 1 {
		t.Fatalf("CoverageGaps = %d err=%v (want 1 under-replicated)", gaps, err)
	}
	var gapIDs []string
	err = led.TargetGaps(ctx, targets, func(id string, s map[string]bool) error {
		gapIDs = append(gapIDs, id)
		if !s["a"] || s["b"] {
			t.Fatalf("gap %s stored=%v (want {a})", id, s)
		}
		return nil
	})
	if err != nil || len(gapIDs) != 1 || gapIDs[0] != h {
		t.Fatalf("TargetGaps ids=%v err=%v (want [%s])", gapIDs, err, h)
	}
	_, never, ok, err := led.LastVerifiedAge(ctx, targets)
	if err != nil || !ok || never != 1 { // a verified, b never
		t.Fatalf("LastVerifiedAge never=%d ok=%v err=%v (want never=1 ok=true)", never, ok, err)
	}
}

// TestIntegrationOutboxRoundTrip exercises the real outbox SQL: seed a pending row, claim it,
// and drive it through the transitions + the dead-letter threshold.
func TestIntegrationOutboxRoundTrip(t *testing.T) {
	ctx := context.Background()
	// Fresh row per run: a unique fileId/externalID avoids cross-test coupling on the shared table.
	fid := uuid.New()
	ext := sum64("outbox-" + fid.String())
	if err := harness.Exec(ctx, harness.AlkemioDB,
		`INSERT INTO file_backup_outbox ("fileId","externalID",size,status) VALUES ($1,$2,$3,'pending')`,
		fid, ext, int64(7)); err != nil {
		t.Fatalf("seed outbox: %v", err)
	}
	repo := NewOutboxRepo(alkemioPool(t), 3, 5)

	if err := repo.Probe(ctx); err != nil {
		t.Fatalf("probe: %v", err)
	}
	if err := repo.CheckWriteGrant(ctx); err != nil {
		t.Fatalf("write grant: %v", err)
	}
	pending, _, err := repo.BacklogStats(ctx)
	if err != nil || pending < 1 {
		t.Fatalf("BacklogStats pending=%d err=%v (want >=1)", pending, err)
	}

	entries, err := repo.Claim(ctx, 10)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	var claimed *domain.OutboxEntry
	for i := range entries {
		if entries[i].ExternalID == ext {
			claimed = &entries[i]
		}
	}
	if claimed == nil {
		t.Fatalf("our seeded row was not claimed (got %d entries)", len(entries))
	}
	if claimed.FileID != fid || claimed.Size != 7 {
		t.Fatalf("claimed row mismatch: %+v", claimed)
	}
	if err := repo.MarkDone(ctx, claimed.ID); err != nil {
		t.Fatalf("mark done: %v", err)
	}
	var status string
	if err := harness.ScalarRow(ctx, harness.AlkemioDB,
		fmt.Sprintf(`SELECT status FROM file_backup_outbox WHERE id=%d`, claimed.ID), &status); err != nil {
		t.Fatalf("read status: %v", err)
	}
	if status != "done" {
		t.Fatalf("status after MarkDone = %q, want done", status)
	}
}

// TestIntegrationOutboxDeadLetters: a claimed row Failed maxAttempts times reaches dead_letter
// via the real backoff/threshold SQL.
func TestIntegrationOutboxDeadLetters(t *testing.T) {
	ctx := context.Background()
	fid := uuid.New()
	ext := sum64("dl-" + fid.String())
	if err := harness.Exec(ctx, harness.AlkemioDB,
		`INSERT INTO file_backup_outbox ("fileId","externalID",status) VALUES ($1,$2,'pending')`,
		fid, ext); err != nil {
		t.Fatalf("seed: %v", err)
	}
	repo := NewOutboxRepo(alkemioPool(t), 2, 5) // dead-letter at 2 attempts

	var dead bool
	for i := 0; i < 2; i++ {
		entries, err := repo.Claim(ctx, 10)
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		var id int64
		for _, e := range entries {
			if e.ExternalID == ext {
				id = e.ID
			}
		}
		if id == 0 {
			t.Fatalf("attempt %d: row not claimable (visibleAt backoff?)", i)
		}
		// Fail's backoff sets visibleAt in the future; make it due again for the next attempt.
		if dead, err = repo.Fail(ctx, id, "boom"); err != nil {
			t.Fatalf("fail %d: %v", i, err)
		}
		if err := harness.Exec(ctx, harness.AlkemioDB,
			`UPDATE file_backup_outbox SET "visibleAt"=now()-interval '1s' WHERE "externalID"=$1 AND status='pending'`, ext); err != nil {
			t.Fatalf("make due: %v", err)
		}
	}
	if !dead {
		t.Fatal("row must be dead-lettered after maxAttempts failures")
	}
}

// TestIntegrationFilesCorpus exercises the real `file` corpus SQL: a non-temporary file with a
// hash is enumerated; a temporary one and a hash-less one are excluded.
func TestIntegrationFilesCorpus(t *testing.T) {
	ctx := context.Background()
	good, tmp, nohash := uuid.New(), uuid.New(), uuid.New()
	ext := sum64("corpus-" + good.String())
	seed := func(id uuid.UUID, extID string, temporary bool) {
		if err := harness.Exec(ctx, harness.AlkemioDB,
			`INSERT INTO file (id,"externalID",size,"temporaryLocation") VALUES ($1,$2,$3,$4)`,
			id, extID, int64(9), temporary); err != nil {
			t.Fatalf("seed file: %v", err)
		}
	}
	seed(good, ext, false)
	seed(tmp, sum64("tmp"+tmp.String()), true) // temporary — excluded
	seed(nohash, "", false)                    // no externalID — excluded

	repo := NewFileRepo(alkemioPool(t))
	if err := repo.Probe(ctx); err != nil {
		t.Fatalf("file probe: %v", err)
	}
	seen := map[string]bool{}
	if err := repo.EachFile(ctx, func(e domain.BackupItem) error {
		seen[e.ExternalID] = true
		return nil
	}); err != nil {
		t.Fatalf("each file: %v", err)
	}
	if !seen[ext] {
		t.Fatal("the good file must be enumerated")
	}
	if seen[""] {
		t.Fatal("a hash-less file must be excluded")
	}
}

// sum64 is a deterministic 64-hex content-address for a label (SHA3-256, the real algorithm) —
// the ledger/outbox key on it as an opaque id; a stable 64-hex string suffices.
func sum64(label string) string {
	sum := sha3.Sum256([]byte(label))
	return hex.EncodeToString(sum[:])
}
