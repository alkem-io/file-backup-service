package main

import (
	"io"
	"os"
	"strings"
	"testing"
)

// TestRunDispatch covers the run() exit-code mapping (extracted from main so it's testable):
// no subcommand and an unknown one print usage and exit 2; drill exits 1 (not implemented); a
// DB subcommand with an invalid config fails validation and exits 1 (before any pool opens).
func TestRunDispatch(t *testing.T) {
	bad := invalidCfg(t)
	cases := []struct {
		name string
		argv []string
		want int
	}{
		{"no-subcommand", []string{"file-backup-service"}, 2},
		{"unknown", []string{"file-backup-service", "bogus"}, 2},
		{"drill-not-implemented", []string{"file-backup-service", "drill"}, 1},
		{"migrate-bad-config", []string{"file-backup-service", "migrate", "--config", bad}, 1},
		{"serve-bad-config", []string{"file-backup-service", "serve", "--config", bad}, 1},
		{"reconcile-bad-config", []string{"file-backup-service", "reconcile", "--config", bad}, 1},
		{"audit-bad-config", []string{"file-backup-service", "audit", "--config", bad}, 1},
		{"backfill-bad-config", []string{"file-backup-service", "backfill", "--config", bad}, 1},
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
	usage()
	_ = w.Close()
	os.Stderr = old
	out, _ := io.ReadAll(r)
	got := string(out)
	for _, want := range []string{"usage:", "file-backup-service", "serve", "migrate"} {
		if !strings.Contains(got, want) {
			t.Fatalf("usage() stderr = %q, want it to contain %q", got, want)
		}
	}
}
