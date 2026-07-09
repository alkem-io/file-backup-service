package main

import (
	"io"
	"os"
	"strings"
	"testing"
)

// TestRunDispatch covers the run() exit-code mapping (extracted from main so it's testable):
// no subcommand and an unknown one print usage and exit 2; every DB/DR subcommand with an invalid
// config fails validation and exits 1 (before any pool opens). drill is now implemented — with an
// invalid config it fails validation (exit 1), like the other DR subcommands.
func TestRunDispatch(t *testing.T) {
	bad := invalidCfg(t)
	cases := []struct {
		name string
		argv []string
		want int
	}{
		{"no-subcommand", []string{"file-backup-service"}, 2},
		{"unknown", []string{"file-backup-service", "bogus"}, 2},
		{"drill-bad-config", []string{"file-backup-service", "drill", "--config", bad}, 1},
		{"migrate-bad-config", []string{"file-backup-service", "migrate", "--config", bad}, 1},
		{"serve-bad-config", []string{"file-backup-service", "serve", "--config", bad}, 1},
		{"reconcile-bad-config", []string{"file-backup-service", "reconcile", "--config", bad}, 1},
		{"audit-bad-config", []string{"file-backup-service", "audit", "--config", bad}, 1},
		{"backfill-bad-config", []string{"file-backup-service", "backfill", "--config", bad}, 1},
		{"restore-all-bad-config", []string{"file-backup-service", "restore", "all", "--config", bad}, 1},
		{"restore-version-bad-config", []string{"file-backup-service", "restore", "version", "--file-id", "6f1e2d3c-4b5a-6978-8a9b-0c1d2e3f4a5b", "--at", "2026-07-01T00:00:00Z", "--config", bad}, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if code := run(tc.argv); code != tc.want {
				t.Fatalf("run(%v) = %d, want %d", tc.argv[1:], code, tc.want)
			}
		})
	}
}

// TestUsage asserts the usage banner is written to stderr and lists the subcommands — a real
// output assertion, not a coverage-only call (constitution §VII: no padding).
func TestUsage(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	old := os.Stderr
	os.Stderr = w
	// Restore + close via Cleanup so a panic in usage() can't leave os.Stderr pointed at the
	// closed pipe for the rest of the package's tests (and r isn't leaked).
	t.Cleanup(func() { os.Stderr = old; _ = r.Close() })
	usage()
	_ = w.Close() // close the write end so ReadAll sees EOF
	out, _ := io.ReadAll(r)
	got := string(out)
	for _, want := range []string{"usage:", "file-backup-service", "serve", "migrate"} {
		if !strings.Contains(got, want) {
			t.Fatalf("usage() stderr = %q, want it to contain %q", got, want)
		}
	}
}
