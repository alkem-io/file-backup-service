package config

import (
	"os"
	"path/filepath"
	"testing"
)

// baseEnv sets the non-target config (fileServiceBase + both DBs) so a Load("") is otherwise valid.
func baseEnv(t *testing.T) {
	t.Helper()
	t.Setenv("FBS_FILESERVICEBASE", "http://fs:4003")
	t.Setenv("FBS_ALKEMIODB_HOST", "postgres")
	t.Setenv("FBS_ALKEMIODB_USER", "u")
	t.Setenv("FBS_ALKEMIODB_DBNAME", "alkemio")
	t.Setenv("FBS_LEDGERDB_HOST", "postgres")
	t.Setenv("FBS_LEDGERDB_USER", "u")
	t.Setenv("FBS_LEDGERDB_DBNAME", "filebackup")
}

func writeTempConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// TestEnvOnlyTargets_Filesystem: with NO config file, the entire config — including the target
// LIST — comes from env (full 12-factor). FBS_TARGETS names the targets; FBS_TARGET_<N>_* supplies
// their fields (incl. TYPE + PATH).
func TestEnvOnlyTargets_Filesystem(t *testing.T) {
	baseEnv(t)
	t.Setenv("FBS_TARGETS", "local")
	t.Setenv("FBS_TARGET_LOCAL_TYPE", "filesystem")
	t.Setenv("FBS_TARGET_LOCAL_PATH", "/storage")

	cfg, err := Load("") // no file
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if len(cfg.Targets) != 1 {
		t.Fatalf("want 1 target, got %d: %+v", len(cfg.Targets), cfg.Targets)
	}
	if tgt := cfg.Targets[0]; tgt.Name != "local" || tgt.Type != "filesystem" || tgt.Path != "/storage" {
		t.Fatalf("target not built from env: %+v", tgt)
	}
}

// TestEnvOnlyTargets_Multiple: FBS_TARGETS defines several targets, in order, each with its own
// fields — an s3 target's structural fields + secrets all arrive via env.
func TestEnvOnlyTargets_Multiple(t *testing.T) {
	baseEnv(t)
	t.Setenv("FBS_TARGETS", "local,offsite")
	t.Setenv("FBS_TARGET_LOCAL_TYPE", "filesystem")
	t.Setenv("FBS_TARGET_LOCAL_PATH", "/storage")
	t.Setenv("FBS_TARGET_OFFSITE_TYPE", "s3")
	t.Setenv("FBS_TARGET_OFFSITE_ENDPOINT", "s3.example.com")
	t.Setenv("FBS_TARGET_OFFSITE_REGION", "r")
	t.Setenv("FBS_TARGET_OFFSITE_BUCKET", "b")
	t.Setenv("FBS_TARGET_OFFSITE_USESSL", "true")
	t.Setenv("FBS_TARGET_OFFSITE_SSE", "true")
	t.Setenv("FBS_TARGET_OFFSITE_ACCESSKEY", "AK")
	t.Setenv("FBS_TARGET_OFFSITE_SECRETKEY", "SK")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if len(cfg.Targets) != 2 || cfg.Targets[0].Name != "local" || cfg.Targets[1].Name != "offsite" {
		t.Fatalf("targets/order wrong: %+v", cfg.Targets)
	}
	s3 := cfg.Targets[1]
	if s3.Type != "s3" || s3.Bucket != "b" || !s3.UseSSL || !s3.SSE || s3.AccessKey != "AK" || s3.SecretKey != "SK" {
		t.Fatalf("s3 target from env wrong: %+v", s3)
	}
}

// TestEnvTargets_AuthoritativeOverYAML: when FBS_TARGETS is set it defines the set — a YAML target
// not listed is dropped, and a listed one keeps its YAML base with env overlaid on top.
func TestEnvTargets_AuthoritativeOverYAML(t *testing.T) {
	yml := writeTempConfig(t, `
fileServiceBase: http://fs
alkemioDB: { host: h, user: u, dbName: alkemio }
ledgerDB:  { host: h, user: u, dbName: filebackup }
targets:
  - { name: keep, type: filesystem, path: /a }
  - { name: drop, type: filesystem, path: /b }
`)
	t.Setenv("FBS_TARGETS", "keep")         // only keep survives; drop is removed
	t.Setenv("FBS_TARGET_KEEP_PATH", "/a2") // env overlays the retained YAML base

	cfg, err := Load(yml)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if len(cfg.Targets) != 1 || cfg.Targets[0].Name != "keep" || cfg.Targets[0].Type != "filesystem" || cfg.Targets[0].Path != "/a2" {
		t.Fatalf("FBS_TARGETS not authoritative: %+v", cfg.Targets)
	}
}

// TestEnvTargets_TokenEquivalentMatchesYAML: FBS_TARGETS may reference a YAML-declared target by a
// token-equivalent spelling (case/punctuation) and MUST keep its entry — the original Name plus any
// YAML-only field env doesn't re-supply — because applyTargetEnv + ValidateTargets identify targets
// by env-var token, not raw name. (With raw-name matching this rebuilt a bare target, dropping the
// compression field and flipping the ledger name.)
func TestEnvTargets_TokenEquivalentMatchesYAML(t *testing.T) {
	yml := writeTempConfig(t, `
fileServiceBase: http://fs
alkemioDB: { host: h, user: u, dbName: alkemio }
ledgerDB:  { host: h, user: u, dbName: filebackup }
targets:
  - { name: offsite, type: filesystem, path: /a, compression: zstd }
`)
	t.Setenv("FBS_TARGETS", "OFFSITE") // uppercase — token-equivalent to the YAML "offsite"

	cfg, err := Load(yml)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if len(cfg.Targets) != 1 {
		t.Fatalf("want 1 target, got %+v", cfg.Targets)
	}
	if tgt := cfg.Targets[0]; tgt.Name != "offsite" || tgt.Compression != "zstd" || tgt.Path != "/a" {
		t.Fatalf("token-equivalent FBS_TARGETS must keep the YAML base (Name + fields): %+v", tgt)
	}
}

// TestEnvTargets_DedupAndBlanks: FBS_TARGETS trims blanks and de-dups, preserving first-seen order.
func TestEnvTargets_DedupAndBlanks(t *testing.T) {
	baseEnv(t)
	t.Setenv("FBS_TARGETS", "local, ,local,off")
	t.Setenv("FBS_TARGET_LOCAL_TYPE", "filesystem")
	t.Setenv("FBS_TARGET_LOCAL_PATH", "/s")
	t.Setenv("FBS_TARGET_OFF_TYPE", "filesystem")
	t.Setenv("FBS_TARGET_OFF_PATH", "/o")

	cfg, err := Load("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.Targets) != 2 || cfg.Targets[0].Name != "local" || cfg.Targets[1].Name != "off" {
		t.Fatalf("dedup/blank handling wrong: %+v", cfg.Targets)
	}
}

// TestEnvTargets_UnsetLeavesYAML: without FBS_TARGETS, the YAML target list stands unchanged
// (backward compatible) — env still overlays per-target fields.
func TestEnvTargets_UnsetLeavesYAML(t *testing.T) {
	yml := writeTempConfig(t, `
fileServiceBase: http://fs
alkemioDB: { host: h, user: u, dbName: alkemio }
ledgerDB:  { host: h, user: u, dbName: filebackup }
targets:
  - { name: only, type: filesystem, path: /a }
`)
	t.Setenv("FBS_TARGET_ONLY_PATH", "/a3")
	cfg, err := Load(yml)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(cfg.Targets) != 1 || cfg.Targets[0].Name != "only" || cfg.Targets[0].Path != "/a3" {
		t.Fatalf("unset FBS_TARGETS should keep YAML + overlay: %+v", cfg.Targets)
	}
}
