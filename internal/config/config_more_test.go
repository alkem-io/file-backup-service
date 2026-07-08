package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestDBConfigValidateRequiredFields covers each missing-required-part branch of
// DBConfig.Validate (host/user/dbName) — a malformed DB config must fail with a clear
// labelled error, not defer to an opaque pgx parse.
func TestDBConfigValidateRequiredFields(t *testing.T) {
	for _, tc := range []struct {
		name string
		d    DBConfig
		want string
	}{
		{"host", DBConfig{User: "u", DBName: "d", Port: 5432}, "host is required"},
		{"user", DBConfig{Host: "h", DBName: "d", Port: 5432}, "user is required"},
		{"dbName", DBConfig{Host: "h", User: "u", Port: 5432}, "dbName is required"},
	} {
		err := tc.d.Validate("ledgerDB")
		if err == nil {
			t.Fatalf("%s: expected a validation error", tc.name)
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: got %v, want it to mention %q", tc.name, err, tc.want)
		}
	}
}

// TestDurationAccessors: each *Sec knob must render as sec*time.Second, and NewCircuitBreaker
// must build a real breaker from the configured threshold+cooldown.
func TestDurationAccessors(t *testing.T) {
	c := &Config{
		PerObjectTimeoutSec: 2, StaleTTLSec: 3, PollEverySec: 4, ManifestEverySec: 5,
		CircuitCooldownSec: 6, FanoutStallSec: 7, DBTimeoutSec: 8, CircuitThreshold: 2,
	}
	for _, tc := range []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"PerObjectTimeout", c.PerObjectTimeout(), 2 * time.Second},
		{"StaleTTL", c.StaleTTL(), 3 * time.Second},
		{"PollEvery", c.PollEvery(), 4 * time.Second},
		{"ManifestEvery", c.ManifestEvery(), 5 * time.Second},
		{"CircuitCooldown", c.CircuitCooldown(), 6 * time.Second},
		{"FanoutStall", c.FanoutStall(), 7 * time.Second},
		{"DBTimeout", c.DBTimeout(), 8 * time.Second},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %v, want %v", tc.name, tc.got, tc.want)
		}
	}
	if cb := c.NewCircuitBreaker(); cb == nil {
		t.Fatal("NewCircuitBreaker must build a breaker")
	}
}

// TestLoadRejectsMalformedYAML: a syntactically-broken config file must fail Load loudly,
// not silently fall through to an env-only config.
func TestLoadRejectsMalformedYAML(t *testing.T) {
	f := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(f, []byte("fileServiceBase: [unterminated\n  bad: :"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(f); err == nil {
		t.Fatal("malformed YAML must fail Load")
	}
}

// TestLoadReadError: a config path that exists but is unreadable (a directory) must surface a
// read error — distinct from the benign missing-file (env-only) case.
func TestLoadReadError(t *testing.T) {
	dir := t.TempDir() // a directory: os.ReadFile returns a non-ErrNotExist error
	if _, err := Load(dir); err == nil {
		t.Fatal("reading a directory as a config file must fail Load, not be treated as env-only")
	}
}

// TestValidateRequiresFileServiceBase: serve Validate must reject an empty fileServiceBase.
func TestValidateRequiresFileServiceBase(t *testing.T) {
	c := validConfig(Target{Name: "fs", Type: "filesystem", Path: "/a"})
	c.FileServiceBase = ""
	if err := c.Validate(); err == nil {
		t.Fatal("empty fileServiceBase must be rejected")
	}
}

// TestValidateRejectsBadLedgerDB: a malformed ledgerDB must fail serve Validate.
func TestValidateRejectsBadLedgerDB(t *testing.T) {
	c := validConfig(Target{Name: "fs", Type: "filesystem", Path: "/a"})
	c.LedgerDB.DBName = ""
	if err := c.Validate(); err == nil {
		t.Fatal("a ledgerDB missing dbName must be rejected")
	}
}

// TestValidateDR: the DR subcommand check validates limits + ledgerDB + targets but NOT
// fileServiceBase (so it still runs in the degraded/DR environment).
func TestValidateDR(t *testing.T) {
	// A valid config with NO fileServiceBase still passes ValidateDR.
	ok := validConfig(Target{Name: "fs", Type: "filesystem", Path: "/a"})
	ok.FileServiceBase = ""
	if err := ok.ValidateDR(); err != nil {
		t.Fatalf("ValidateDR must ignore fileServiceBase: %v", err)
	}
	// A bad numeric limit fails ValidateDR.
	badLimits := validConfig(Target{Name: "fs", Type: "filesystem", Path: "/a"})
	badLimits.CircuitThreshold = badLimits.MaxAttempts // circuitThreshold must be < maxAttempts
	if err := badLimits.ValidateDR(); err == nil {
		t.Fatal("ValidateDR must reject a bad numeric limit")
	}
	// A malformed ledgerDB fails ValidateDR (the DR subcommands connect to it).
	badLedger := validConfig(Target{Name: "fs", Type: "filesystem", Path: "/a"})
	badLedger.LedgerDB.Host = ""
	if err := badLedger.ValidateDR(); err == nil {
		t.Fatal("ValidateDR must reject a malformed ledgerDB")
	}
}

// TestValidateTargetsRejectsEmpty: zero targets is a silent-total-loss config and must fail.
func TestValidateTargetsRejectsEmpty(t *testing.T) {
	if err := validConfig().ValidateTargets(); err == nil {
		t.Fatal("a config with zero targets must be rejected")
	}
}

// TestPoolSize: the pool size is Concurrency+headroom, clamped to [1,1024].
func TestPoolSize(t *testing.T) {
	c := validConfig()
	c.Concurrency = 8
	if got := c.PoolSize(8); got != 16 {
		t.Fatalf("PoolSize = %d, want 16", got)
	}
	// Clamped: even an out-of-range Concurrency can't overflow the pool sizing.
	c.Concurrency = 100000
	if got := c.PoolSize(8); got != 1032 {
		t.Fatalf("PoolSize clamp = %d, want 1032 (1024+8)", got)
	}
}

// TestValidateLimitsBoundaries walks each numeric-limit rejection so an operator misconfig
// fails at Validate rather than as a runtime overflow/panic.
func TestValidateLimitsBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*Config)
	}{
		{"concurrency>1024", func(c *Config) { c.Concurrency = 2000 }},
		{"staleTTL floor", func(c *Config) { c.StaleTTLSec = c.PerObjectTimeoutSec }},
		{"circuitThreshold>=maxAttempts", func(c *Config) { c.CircuitThreshold = c.MaxAttempts }},
		{"metricsPort>65535", func(c *Config) { c.MetricsPort = 70000 }},
		{"maxAttempts>1000", func(c *Config) { c.MaxAttempts = 2000 }},
		{"maxDeliveries>1000", func(c *Config) { c.MaxDeliveries = 2000 }},
	} {
		c := validConfig(Target{Name: "fs", Type: "filesystem", Path: "/a"})
		tc.set(c)
		if err := c.Validate(); err == nil {
			t.Fatalf("%s must be rejected by validateLimits", tc.name)
		}
	}
}

// TestValidateTargetErrors covers each per-target rejection: an empty name, an over-long name
// (the ledger column is VARCHAR(64)), a duplicate name, an unknown type, an unknown codec, an
// s3 target missing region, and a filesystem target missing path.
func TestValidateTargetErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		targets []Target
		want    string
	}{
		{"empty name", []Target{{Type: "filesystem", Path: "/a"}}, "name is required"},
		{"name too long", []Target{{Name: strings.Repeat("x", 65), Type: "filesystem", Path: "/a"}}, "exceeds 64 chars"},
		{"duplicate name", []Target{
			{Name: "same", Type: "filesystem", Path: "/a"},
			{Name: "same", Type: "filesystem", Path: "/b"},
		}, "duplicate target name"},
		{"unknown type", []Target{{Name: "t", Type: "weird"}}, "unknown type"},
		{"unknown codec", []Target{{Name: "t", Type: "filesystem", Path: "/a", Compression: "gzip"}}, "unknown compression"},
		{"s3 missing region", []Target{{Name: "s", Type: "s3", Endpoint: "e", Bucket: "b", AccessKey: "AK", SecretKey: "SK", UseSSL: true, SSE: true}}, "endpoint, bucket, and region"},
		{"fs missing path", []Target{{Name: "f", Type: "filesystem"}}, "filesystem requires path"},
	} {
		err := validConfig(tc.targets...).ValidateTargets()
		if err == nil {
			t.Fatalf("%s: expected a validation error", tc.name)
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: got %v, want it to mention %q", tc.name, err, tc.want)
		}
	}
}
