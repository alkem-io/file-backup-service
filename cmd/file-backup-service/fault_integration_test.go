//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

// outboxStatusOf reads the (unique) outbox row's status for a content hash.
func outboxStatusOf(t *testing.T, h string) string {
	t.Helper()
	var s string
	if err := harness.ScalarRow(context.Background(), harness.AlkemioDB,
		fmt.Sprintf(`SELECT status FROM file_backup_outbox WHERE "externalID"='%s' ORDER BY id DESC LIMIT 1`, h), &s); err != nil {
		t.Fatalf("read outbox status for %s: %v", h, err)
	}
	return s
}

func fileExists(path string) bool { _, err := os.Stat(path); return err == nil }

// waitForServe polls cond until true or the deadline, failing fast if serveCtx returns early.
func waitForServe(t *testing.T, done <-chan error, timeout time.Duration, cond func() bool, desc string) {
	t.Helper()
	deadline := time.After(timeout)
	for !cond() {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %s", desc)
		case err := <-done:
			t.Fatalf("serveCtx returned early while waiting for %s: %v", desc, err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// TestIntegrationFaultInjectionResumeNoLoss (T021/SC-005): with one target offline, the worker
// stores every object on the healthy target but never marks a row done (no false "done", no loss);
// after a worker restart AND the target coming back online it RESUMES and completes every object
// on the recovered target — nothing lost.
func TestIntegrationFaultInjectionResumeNoLoss(t *testing.T) {
	ctx := context.Background()
	content := map[uuid.UUID][]byte{}
	var hashes []string
	for i := 0; i < 3; i++ {
		body := []byte(fmt.Sprintf("fault-inject %d — %s", i, uuid.NewString()))
		fid := uuid.New()
		content[fid] = body
		h := sha3hex(body)
		if err := harness.Exec(ctx, harness.AlkemioDB,
			`INSERT INTO file_backup_outbox ("fileId","externalID",size,status) VALUES ($1,$2,$3,'pending')`,
			fid, h, int64(len(body))); err != nil {
			t.Fatalf("seed outbox: %v", err)
		}
		hashes = append(hashes, h)
	}
	fs := stubFileService(t, content)

	goodDir := t.TempDir()
	// The "bad" target's root is a regular FILE, so MkdirAll fails and every Store to it errors —
	// the target is "offline". Removing the file later brings it online (MkdirAll then succeeds).
	badPath := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(badPath, []byte("x"), 0o644); err != nil { //nolint:gosec // test fixture
		t.Fatalf("make blocker: %v", err)
	}
	good, bad := uniqueTarget(), uniqueTarget()
	cleanupLedger(t, good, bad) // return the shared ledger to its prior state at test end
	yaml := harness.ConfigYAML(fs.URL, map[string]string{good: goodDir, bad: badPath})
	// High maxAttempts/circuitThreshold so the failing target neither dead-letters nor trips its
	// circuit during the test — objects simply Fail with a short backoff and re-claim. Distinct
	// metrics port so the two serve phases don't clash with the other integration serve tests.
	yaml += "\nmetricsPort: 14201\npollEverySec: 1\nmaxAttempts: 100\ncircuitThreshold: 90\n"
	cfgPath := writeConfig(t, t.TempDir(), yaml)

	// Phase 1: serve with the bad target offline.
	ctx1, cancel1 := context.WithCancel(ctx)
	done1 := make(chan error, 1)
	go func() { done1 <- serveCtx(ctx1, cfgPath) }()
	waitForServe(t, done1, 30*time.Second, func() bool {
		for _, h := range hashes {
			if !fileExists(storedPath(goodDir, h)) {
				return false
			}
		}
		return true
	}, "the healthy target to receive every object")
	// No false "done": the bad target has nothing, so no object may be marked done yet.
	for _, h := range hashes {
		if st := outboxStatusOf(t, h); st == "done" {
			t.Fatalf("object %s marked done while a target is offline — a FALSE done (data would be under-replicated)", h)
		}
	}
	cancel1()
	select {
	case err := <-done1:
		if err != nil && !isCanceled(err) {
			t.Fatalf("phase-1 serve must exit cleanly on cancel, got %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("phase-1 serve did not stop on cancel")
	}

	// Bring the bad target online.
	if err := os.Remove(badPath); err != nil {
		t.Fatalf("bring target online: %v", err)
	}

	// Phase 2: restart the worker — it must RESUME and complete every object on the recovered target.
	ctx2, cancel2 := context.WithCancel(ctx)
	defer cancel2()
	done2 := make(chan error, 1)
	go func() { done2 <- serveCtx(ctx2, cfgPath) }()
	waitForServe(t, done2, 90*time.Second, func() bool {
		for _, h := range hashes {
			if outboxStatusOf(t, h) != "done" {
				return false
			}
		}
		return true
	}, "every object to complete after the target recovered")
	// No data loss: the recovered target now holds every object.
	for _, h := range hashes {
		if !fileExists(storedPath(badPath, h)) {
			t.Fatalf("object %s not on the recovered target — data loss after resume", h)
		}
	}
}

// TestIntegrationThroughputSoak (T038/SC-004): sustain a batch of concurrent saves and confirm the
// backlog drains fully within a bounded window (CI-fast — a small N + a short deadline, not a
// multi-minute soak). Proves steady-state throughput with backlog age kept bounded.
func TestIntegrationThroughputSoak(t *testing.T) {
	ctx := context.Background()
	const n = 40
	content := map[uuid.UUID][]byte{}
	var hashes []string
	for i := 0; i < n; i++ {
		body := []byte(fmt.Sprintf("throughput %d — %s", i, uuid.NewString()))
		fid := uuid.New()
		content[fid] = body
		h := sha3hex(body)
		if err := harness.Exec(ctx, harness.AlkemioDB,
			`INSERT INTO file_backup_outbox ("fileId","externalID",size,status) VALUES ($1,$2,$3,'pending')`,
			fid, h, int64(len(body))); err != nil {
			t.Fatalf("seed outbox: %v", err)
		}
		hashes = append(hashes, h)
	}
	fs := stubFileService(t, content)
	cfgPath, targetDir, _ := drConfig(t, fs.URL, "metricsPort: 14202", "pollEverySec: 1", "concurrency: 8")

	ctxS, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- serveCtx(ctxS, cfgPath) }()
	waitForServe(t, done, 45*time.Second, func() bool {
		for _, h := range hashes {
			if outboxStatusOf(t, h) != "done" {
				return false
			}
		}
		return true
	}, "the whole batch to drain")
	elapsed := time.Since(start)
	cancel()
	t.Logf("drained %d objects in %s (%.0f/s)", n, elapsed, float64(n)/elapsed.Seconds())
	for _, h := range hashes {
		if !fileExists(storedPath(targetDir, h)) {
			t.Fatalf("object %s missing on the target — a save was lost", h)
		}
	}
}
