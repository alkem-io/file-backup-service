package config

import (
	"net/url"
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
	want := "postgres://u:secretpw@h:5432/alkemio?sslmode=require" //nolint:gosec // test fixture, not a real credential
	if cfg.AlkemioDB.DSN() != want {
		t.Fatalf("composed DSN mismatch: %q", cfg.AlkemioDB.DSN())
	}
}

// TestDSNEscapesSpecialChars guards the finding that a keyword DSN silently
// mis-parses a password with a space / '=' — the URL form must percent-encode it.
func TestDSNEscapesSpecialChars(t *testing.T) {
	d := DBConfig{Host: "h", Port: 5432, User: "u", Password: "p@ss w=rd", DBName: "alkemio", SSLMode: "require"}
	dsn := d.DSN()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("DSN is not a parseable URL: %q: %v", dsn, err)
	}
	pw, _ := u.User.Password()
	if pw != "p@ss w=rd" {
		t.Fatalf("password did not round-trip through the DSN: got %q", pw)
	}
	if u.Host != "h:5432" || u.Path != "/alkemio" || u.Query().Get("sslmode") != "require" {
		t.Fatalf("DSN fields wrong: %q", dsn)
	}
}

// TestConcurrencyFloor: a negative concurrency must not survive into pool sizing.
func TestConcurrencyFloor(t *testing.T) {
	t.Setenv("FBS_CONCURRENCY", "-4")
	cfg, err := Load(filepath.Join(t.TempDir(), "none.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Concurrency != 8 {
		t.Fatalf("negative concurrency should floor to 8, got %d", cfg.Concurrency)
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
