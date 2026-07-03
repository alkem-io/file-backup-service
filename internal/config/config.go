// Package config loads the file-backup-service worker configuration.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

// Target is one configured backup sink.
type Target struct {
	Name        string `json:"name"`
	Type        string `json:"type"` // "s3" | "filesystem"
	Endpoint    string `json:"endpoint,omitempty"`
	Region      string `json:"region,omitempty"` // s3 region (Scaleway can't auto-discover on PutObject-only creds)
	Bucket      string `json:"bucket,omitempty"`
	Prefix      string `json:"prefix,omitempty"`
	Path        string `json:"path,omitempty"`
	Compression string `json:"compression,omitempty"` // "" | "none" | "zstd"
	// S3 target credentials + options (templated from secrets in k8s).
	AccessKey string `json:"accessKey,omitempty"`
	SecretKey string `json:"secretKey,omitempty"`
	UseSSL    bool   `json:"useSSL,omitempty"`
	SSE       bool   `json:"sse,omitempty"`      // server-side encryption at rest (MUST — constitution §V)
	Insecure  bool   `json:"insecure,omitempty"` // conscious opt-out of the TLS+SSE requirement (local dev only)
}

// Config is the worker configuration.
type Config struct {
	FileServiceBase     string   `json:"fileServiceBase"`
	AlkemioDB           string   `json:"alkemioDB"` // scoped role: outbox SELECT/UPDATE
	LedgerDB            string   `json:"ledgerDB"`  // this service's own database
	Targets             []Target `json:"targets"`
	Concurrency         int      `json:"concurrency"`
	BackfillRatePerSec  int      `json:"backfillRatePerSec"`
	MetricsPort         int      `json:"metricsPort"`
	PerObjectTimeoutSec int      `json:"perObjectTimeoutSec"` // per-object backup deadline
	StaleTTLSec         int      `json:"staleTTLSec"`         // reap in_progress older than this
	PollEverySec        int      `json:"pollEverySec"`        // polling floor
}

// PerObjectTimeout is the per-object backup deadline.
func (c *Config) PerObjectTimeout() time.Duration {
	return time.Duration(c.PerObjectTimeoutSec) * time.Second
}

// StaleTTL is how long a claim may stay in_progress before the reaper requeues it.
func (c *Config) StaleTTL() time.Duration { return time.Duration(c.StaleTTLSec) * time.Second }

// PollEvery is the polling floor.
func (c *Config) PollEvery() time.Duration { return time.Duration(c.PollEverySec) * time.Second }

// Load reads a JSON config file, applies defaults, and validates it.
//
// TODO(008): switch to YAML (matches quickstart) once a yaml dependency lands.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path) //nolint:gosec // operator-supplied config path
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}
	if c.Concurrency == 0 {
		c.Concurrency = 8
	}
	if c.MetricsPort == 0 {
		c.MetricsPort = 4004
	}
	if c.PerObjectTimeoutSec == 0 {
		c.PerObjectTimeoutSec = 1800 // 30 min — must exceed the slowest legit backup
	}
	if c.StaleTTLSec == 0 {
		c.StaleTTLSec = 3600 // 1 h — must exceed PerObjectTimeout so a running object isn't reaped
	}
	if c.PollEverySec == 0 {
		c.PollEverySec = 10
	}
	// Validation is NOT run here — it is a serve-time concern. migrate / restore /
	// verify need only a subset (the ledger DSN, or one named target), and must
	// not be rejected for missing serve-only fields.
	return &c, nil
}

// Validate checks the full serve configuration. Call it from serve, not Load.
func (c *Config) Validate() error {
	if c.FileServiceBase == "" {
		return errors.New("fileServiceBase is required")
	}
	if c.AlkemioDB == "" {
		return errors.New("alkemioDB is required")
	}
	if c.LedgerDB == "" {
		return errors.New("ledgerDB is required")
	}
	if len(c.Targets) == 0 {
		return errors.New("at least one target is required")
	}
	if c.StaleTTLSec <= c.PerObjectTimeoutSec {
		return fmt.Errorf("staleTTLSec (%d) must exceed perObjectTimeoutSec (%d), else a running object is reaped",
			c.StaleTTLSec, c.PerObjectTimeoutSec)
	}
	seen := make(map[string]bool, len(c.Targets))
	for i, t := range c.Targets {
		if err := validateTarget(i, t, seen); err != nil {
			return err
		}
	}
	return nil
}

func validateTarget(i int, t Target, seen map[string]bool) error {
	if t.Name == "" {
		return fmt.Errorf("target[%d]: name is required", i)
	}
	if seen[t.Name] {
		return fmt.Errorf("duplicate target name %q", t.Name)
	}
	seen[t.Name] = true
	switch t.Type {
	case "filesystem":
		if t.Path == "" {
			return fmt.Errorf("target %q: filesystem requires path", t.Name)
		}
	case "s3":
		if t.Endpoint == "" || t.Bucket == "" {
			return fmt.Errorf("target %q: s3 requires endpoint and bucket", t.Name)
		}
		if !t.Insecure && (!t.UseSSL || !t.SSE) {
			return fmt.Errorf("target %q: s3 requires useSSL and sse (constitution §V: TLS + SSE at rest); "+
				"set \"insecure\": true only for local dev", t.Name)
		}
	default:
		return fmt.Errorf("target %q: unknown type %q (want s3|filesystem)", t.Name, t.Type)
	}
	switch t.Compression {
	case "", "none", "zstd":
		return nil
	default:
		return fmt.Errorf("target %q: unknown compression %q (want none|zstd)", t.Name, t.Compression)
	}
}
