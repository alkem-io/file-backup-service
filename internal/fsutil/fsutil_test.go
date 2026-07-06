package fsutil

import "testing"

func TestPreflightKeyUniqueAndReserved(t *testing.T) {
	a, b := PreflightKey(), PreflightKey()
	if a == b {
		t.Fatalf("PreflightKey must be unique per call, got %q twice", a)
	}
	for _, k := range []string{a, b} {
		if !IsReserved(k) {
			t.Fatalf("PreflightKey %q must be a reserved key", k)
		}
	}
}

func TestIsReserved(t *testing.T) {
	cases := map[string]bool{
		ManifestKey("2026-07-06.jsonl"): true,
		"_manifest/x.jsonl":             true,
		"_preflight/123-abcd":           true,
		ShardKey("abcdef0123456789"):    false, // a normal sharded object
		"ab/cd/abcdef0123456789":        false,
		"_manifestlike/not-really":      false, // prefix must be a full path segment
	}
	for key, want := range cases {
		if got := IsReserved(key); got != want {
			t.Errorf("IsReserved(%q) = %v, want %v", key, got, want)
		}
	}
}
