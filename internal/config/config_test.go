package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadYAMLPlusEnv(t *testing.T) {
	dir := t.TempDir()
	yml := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(yml, []byte(`
fileServiceBase: http://fs
alkemioDB: { host: h, user: u, dbName: alkemio }
ledgerDB:  { host: h, user: u, dbName: filebackup }
targets:
  - name: offsite
    type: s3
    endpoint: e
    bucket: b
    useSSL: true
    sse: true
`), 0o600); err != nil {
		t.Fatal(err)
	}

	// Env overrides a scalar, injects a DB password and per-target secrets, and
	// overrides a per-target scalar.
	t.Setenv("FBS_CONCURRENCY", "16")
	t.Setenv("FBS_ALKEMIODB_PASSWORD", "secretpw")
	t.Setenv("FBS_TARGET_OFFSITE_ACCESSKEY", "AK")
	t.Setenv("FBS_TARGET_OFFSITE_SECRETKEY", "SK")
	t.Setenv("FBS_TARGET_OFFSITE_BUCKET", "override-bucket")

	cfg, err := Load(yml)
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.Concurrency != 16 {
		t.Fatalf("env override concurrency: got %d", cfg.Concurrency)
	}
	if cfg.AlkemioDB.Password != "secretpw" {
		t.Fatal("db password should come from env")
	}
	if cfg.AlkemioDB.Port != 5432 || cfg.AlkemioDB.SSLMode != "require" {
		t.Fatalf("db defaults not applied: %+v", cfg.AlkemioDB)
	}
	tgt := cfg.Targets[0]
	if tgt.AccessKey != "AK" || tgt.SecretKey != "SK" {
		t.Fatalf("per-target secret injection failed: %+v", tgt)
	}
	if tgt.Bucket != "override-bucket" {
		t.Fatalf("per-target scalar override failed: %q", tgt.Bucket)
	}
	if want := "host=h port=5432 user=u password=secretpw dbname=alkemio sslmode=require"; cfg.AlkemioDB.DSN() != want {
		t.Fatalf("composed DSN mismatch: %q", cfg.AlkemioDB.DSN())
	}
}

func TestLoadEnvOnlyNoFile(t *testing.T) {
	// A missing file is valid — env-only config, defaults applied.
	cfg, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Concurrency != 8 || cfg.MetricsPort != 4004 {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
}

func TestValidateRejectsInsecureS3(t *testing.T) {
	c := &Config{
		FileServiceBase:     "http://fs",
		AlkemioDB:           DBConfig{Host: "h", User: "u", DBName: "a"},
		LedgerDB:            DBConfig{Host: "h", User: "u", DBName: "l"},
		PerObjectTimeoutSec: 1800,
		StaleTTLSec:         3600,
		Targets:             []Target{{Name: "s3", Type: "s3", Endpoint: "e", Bucket: "b"}}, // no useSSL/sse
	}
	if err := c.Validate(); err == nil {
		t.Fatal("expected the s3 TLS+SSE requirement to reject this config")
	}
}
