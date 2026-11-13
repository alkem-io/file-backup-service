//go:build integration

package db

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

func TestIntegrationForgetUnstoredPreservesEveryStoredCopy(t *testing.T) {
	ctx := context.Background()
	pool := ledgerPool(t)
	led := NewLedgerRepo(pool)
	for _, storedTarget := range []string{"", "offsite", "removed-target"} {
		t.Run("copy-on-"+storedTarget, func(t *testing.T) {
			h := sum64(uuid.NewString())
			t.Cleanup(func() {
				_ = harness.Exec(ctx, harness.LedgerDB, `DELETE FROM file_backup_target_status WHERE "externalID"=$1`, h)
				_ = harness.Exec(ctx, harness.LedgerDB, `DELETE FROM file_backup_object WHERE "externalID"=$1`, h)
			})
			statuses := []domain.TargetStatus{{Target: "offsite", State: domain.StateFailed}}
			if storedTarget == "offsite" {
				statuses[0] = domain.TargetStatus{Target: storedTarget, State: domain.StateStored, StoredBytes: 42}
			} else if storedTarget != "" {
				statuses = append(statuses, domain.TargetStatus{Target: storedTarget, State: domain.StateStored, StoredBytes: 42})
			}
			if err := led.RecordBackup(ctx, domain.ObjectMeta{ExternalID: h, Size: 42, SizeVerified: true}, statuses); err != nil {
				t.Fatal(err)
			}
			retired, err := led.ForgetUnstored(ctx, h)
			if err != nil || retired != (storedTarget == "") {
				t.Fatalf("stored target=%q retired=%v err=%v", storedTarget, retired, err)
			}
			var objects, targetRows int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM file_backup_object WHERE "externalID"=$1`, h).Scan(&objects); err != nil {
				t.Fatal(err)
			}
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM file_backup_target_status WHERE "externalID"=$1`, h).Scan(&targetRows); err != nil {
				t.Fatal(err)
			}
			if storedTarget == "" && (objects != 0 || targetRows != 0) {
				t.Fatalf("failed-only metadata must be fully removed: objects=%d statuses=%d", objects, targetRows)
			}
			if storedTarget != "" && (objects != 1 || targetRows == 0) {
				t.Fatalf("stored backup metadata must survive: objects=%d statuses=%d", objects, targetRows)
			}
			if _, err := led.ForgetUnstored(ctx, h); err != nil {
				t.Fatalf("repeat retirement must be idempotent: %v", err)
			}
		})
	}
}

func TestIntegrationFileReferencesIncludeDeduplicatedTemporaryRows(t *testing.T) {
	ctx := context.Background()
	pool := alkemioPool(t)
	files := NewFileRepo(pool)
	h := sum64(uuid.NewString())
	permanent, temporary := uuid.New(), uuid.New()
	if err := harness.Exec(ctx, harness.AlkemioDB,
		`INSERT INTO file (id,"externalID","temporaryLocation") VALUES ($1,$3,false),($2,$3,true)`, permanent, temporary, h); err != nil {
		t.Fatal(err)
	}
	for _, id := range []uuid.UUID{permanent, temporary} {
		referenced, err := files.HasReferences(ctx, h)
		if err != nil || !referenced {
			t.Fatalf("every surviving shared reference must protect the hash: referenced=%v err=%v", referenced, err)
		}
		if err := harness.Exec(ctx, harness.AlkemioDB, `DELETE FROM file WHERE id=$1`, id); err != nil {
			t.Fatal(err)
		}
	}
	referenced, err := files.HasReferences(ctx, h)
	if err != nil || referenced {
		t.Fatalf("only deletion of the last reference makes a hash unreferenced: referenced=%v err=%v", referenced, err)
	}
}
