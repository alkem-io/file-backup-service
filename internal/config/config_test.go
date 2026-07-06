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
    region: r
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

// TestLoadRejectsMalformedEnv: a malformed numeric env override must fail Load
// loudly, not silently revert to the default (which would degrade the SLO).
func TestLoadRejectsMalformedEnv(t *testing.T) {
	t.Setenv("FBS_STALETTLSEC", "1h") // has units — not a valid integer
	if _, err := Load(filepath.Join(t.TempDir(), "none.yaml")); err == nil {
		t.Fatal("expected Load to reject a malformed numeric env override")
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

// validConfig is a fully-valid serve config with the given targets — so a test can
// isolate the ONE field it wants Validate to reject.
func validConfig(targets ...Target) *Config {
	return &Config{
		FileServiceBase:     "http://fs",
		AlkemioDB:           DBConfig{Host: "h", Port: 5432, User: "u", DBName: "a"},
		LedgerDB:            DBConfig{Host: "h", Port: 5432, User: "u", DBName: "l"},
		PerObjectTimeoutSec: 1800,
		StaleTTLSec:         3600,
		PollEverySec:        10,
		MetricsPort:         4004,
		MaxAttempts:         10,
		MaxDeliveries:       50,
		FanoutStallSec:      60,
		CircuitCooldownSec:  60,
		Targets:             targets,
	}
}

func TestValidateRejectsStallNotBelowTimeout(t *testing.T) {
	c := validConfig(Target{Name: "fs", Type: "filesystem", Path: "/a"})
	c.FanoutStallSec = c.PerObjectTimeoutSec // stall must fire BEFORE the per-object timeout
	if err := c.Validate(); err == nil {
		t.Fatal("expected fanoutStallSec >= perObjectTimeoutSec to be rejected (stall-drop would never fire first)")
	}
}

func TestValidateRejectsOverflowingCircuitKnobs(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*Config)
	}{
		{"fanoutStallSec", func(c *Config) { c.FanoutStallSec = 1 << 40 }},
		{"circuitCooldownSec", func(c *Config) { c.CircuitCooldownSec = 1 << 40 }},
	} {
		c := validConfig(Target{Name: "fs", Type: "filesystem", Path: "/a"})
		tc.set(c)
		if err := c.Validate(); err == nil {
			t.Fatalf("expected an overflowing %s to be rejected (would wrap to a negative Duration)", tc.name)
		}
	}
}

func TestValidateRejectsInsecureS3(t *testing.T) {
	c := validConfig(Target{Name: "s3", Type: "s3", Endpoint: "e", Bucket: "b", Region: "r"}) // no useSSL/sse
	if err := c.Validate(); err == nil {
		t.Fatal("expected the s3 TLS+SSE requirement to reject this config")
	}
}

func TestValidateRejectsEnvTokenCollision(t *testing.T) {
	// Distinct names that collapse to the same FBS_TARGET_<TOKEN>_* prefix must be
	// rejected, else they'd silently share injected secrets.
	c := validConfig(
		Target{Name: "s3-eu", Type: "filesystem", Path: "/a"},
		Target{Name: "s3_eu", Type: "filesystem", Path: "/b"},
	)
	if err := c.Validate(); err == nil {
		t.Fatal("expected two targets colliding on the env-var token to be rejected")
	}
}

func TestValidateRejectsBadDBPort(t *testing.T) {
	c := validConfig(Target{Name: "fs", Type: "filesystem", Path: "/a"})
	c.AlkemioDB.Port = -5432
	if err := c.Validate(); err == nil {
		t.Fatal("expected a negative DB port to be rejected at Validate, not deferred to DSN parse")
	}
}
