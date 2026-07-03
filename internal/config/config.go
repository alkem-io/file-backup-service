// Package config loads the file-backup-service worker configuration.
package config

import (
	"encoding/json"
	"fmt"
	"os"
)

// Target is one configured backup sink.
type Target struct {
	Name           string `json:"name"`
	Type           string `json:"type"` // "s3" | "filesystem"
	Endpoint       string `json:"endpoint,omitempty"`
	Bucket         string `json:"bucket,omitempty"`
	Prefix         string `json:"prefix,omitempty"`
	Path           string `json:"path,omitempty"`
	Compression    string `json:"compression,omitempty"` // "none" | "zstd"
	Immutable      bool   `json:"immutable,omitempty"`
	CredentialsRef string `json:"credentialsRef,omitempty"`
	// S3 target credentials + options (templated from secrets in k8s).
	AccessKey string `json:"accessKey,omitempty"`
	SecretKey string `json:"secretKey,omitempty"`
	UseSSL    bool   `json:"useSSL,omitempty"`
	SSE       bool   `json:"sse,omitempty"` // server-side encryption at rest
}

// Config is the worker configuration.
type Config struct {
	FileServiceBase    string   `json:"fileServiceBase"`
	AlkemioDB          string   `json:"alkemioDB"` // scoped role: outbox SELECT/UPDATE
	LedgerDB           string   `json:"ledgerDB"`  // this service's own database
	Targets            []Target `json:"targets"`
	Concurrency        int      `json:"concurrency"`
	BackfillRatePerSec int      `json:"backfillRatePerSec"`
	MetricsPort        int      `json:"metricsPort"`
}

// Load reads a JSON config file and applies defaults.
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
	return &c, nil
}
