//go:build integration

package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/alkem-io/file-backup-service/internal/adapter/outbound/db"
	"github.com/alkem-io/file-backup-service/internal/domain"
)

func TestIntegrationReconcileSourceLifecycle(t *testing.T) {
	cases := []struct {
		name       string
		references int
		primary    bool
		stored     bool
		retire     bool
	}{
		{name: "deleted-with-no-outbox-retry", retire: true},
		{name: "shared-reference-survives", references: 2},
		{name: "primary-still-exists", primary: true},
		{name: "backup-on-removed-target-survives", stored: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			body := []byte(uuid.NewString())
			h := sha3hex(body)
			var bodies [][]byte
			if tc.primary {
				bodies = append(bodies, body)
			}
			source := stubFileService(t, bodies...)
			cfgPath, _, target := drConfig(t, source.URL)
			for i := 0; i < tc.references; i++ {
				// The second shared reference is temporary and must still prevent cleanup.
				if err := harness.Exec(ctx, harness.AlkemioDB,
					`INSERT INTO file (id,"externalID","temporaryLocation") VALUES ($1,$2,$3)`, uuid.New(), h, i > 0); err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() { _ = harness.Exec(ctx, harness.AlkemioDB, `DELETE FROM file WHERE "externalID"=$1`, h) })
			pool, err := db.NewPool(ctx, harness.LedgerDSN(), 4, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(pool.Close)
			ledger := db.NewLedgerRepo(pool)
			statuses := []domain.TargetStatus{{Target: target, State: domain.StateFailed}}
			if tc.stored {
				oldTarget := uniqueTarget()
				cleanupLedger(t, oldTarget)
				statuses = append(statuses, domain.TargetStatus{Target: oldTarget, State: domain.StateStored, StoredBytes: int64(len(body))})
			}
			if err := ledger.RecordBackup(ctx, domain.ObjectMeta{ExternalID: h}, statuses); err != nil {
				t.Fatal(err)
			}
			err = runReconcile([]string{"--config", cfgPath})
			if (err == nil) != tc.retire {
				t.Fatalf("retire=%v reconcile error=%v", tc.retire, err)
			}
			var remaining int
			if err := pool.QueryRow(ctx, `SELECT count(*) FROM file_backup_object WHERE "externalID"=$1`, h).Scan(&remaining); err != nil {
				t.Fatal(err)
			}
			if (remaining == 0) != tc.retire {
				t.Fatalf("retire=%v remaining ledger rows=%d", tc.retire, remaining)
			}
			if tc.retire {
				if err := runReconcile([]string{"--config", cfgPath}); err != nil {
					t.Fatalf("repeat reconciliation must converge: %v", err)
				}
			}
		})
	}
}

func TestIntegrationBackfillMissingSourceRemainsAlertableUntilLastReferenceDeleted(t *testing.T) {
	ctx := context.Background()
	h := sha3hex([]byte(uuid.NewString()))
	if err := harness.Exec(ctx, harness.AlkemioDB,
		`INSERT INTO file (id,"externalID","temporaryLocation") VALUES ($1,$2,false)`, uuid.New(), h); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = harness.Exec(ctx, harness.AlkemioDB, `DELETE FROM file WHERE "externalID"=$1`, h) })
	source := stubFileService(t)
	cfgPath, _, target := drConfig(t, source.URL)
	if err := runBackfill([]string{"--config", cfgPath}); err == nil {
		t.Fatal("a still-referenced missing primary must fail, not count as a benign skip")
	}
	var failed int
	if err := harness.ScalarRow(ctx, harness.LedgerDB,
		fmt.Sprintf(`SELECT count(*) FROM file_backup_target_status WHERE "externalID"='%s' AND target='%s' AND state='failed'`, h, target), &failed); err != nil {
		t.Fatal(err)
	}
	if failed != 1 {
		t.Fatal("first-fetch failure must remain visible to coverage")
	}
	if err := harness.Exec(ctx, harness.AlkemioDB, `DELETE FROM file WHERE "externalID"=$1`, h); err != nil {
		t.Fatal(err)
	}
	if err := runReconcile([]string{"--config", cfgPath}); err != nil {
		t.Fatalf("after deleting the last reference, reconcile must retire failed metadata: %v", err)
	}
}
