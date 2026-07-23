package fsutil

import (
	"strings"
	"testing"
)

func TestPreflightKeyUniqueAndReserved(t *testing.T) {
	a, b := PreflightKey(), PreflightKey()
	if a == b {
		t.Fatalf("PreflightKey must be unique per call, got %q twice", a)
	}
	for _, k := range []string{a, b} {
		if !strings.HasPrefix(k, preflightPrefix+"/") {
			t.Fatalf("PreflightKey %q must be under the reserved %q prefix", k, preflightPrefix)
		}
	}
}

func TestManifestKey(t *testing.T) {
	got := ManifestKey("2026-07-06.jsonl")
	if got != manifestPrefix+"/2026-07-06.jsonl" {
		t.Fatalf("ManifestKey = %q", got)
	}
}

func TestShardKey(t *testing.T) {
	if got := ShardKey("abcdef0123456789"); got != "ab/cd/abcdef0123456789" {
		t.Fatalf("ShardKey = %q", got)
	}
}
