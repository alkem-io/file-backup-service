// Package config loads the file-backup-service worker configuration: a YAML base
// (structure, incl. the symmetric target list) overlaid by 12-factor environment
// variables. Env wins; secrets (DB passwords, S3 keys) come from env only.
//
//	Scalars:     FBS_FILESERVICEBASE, FBS_CONCURRENCY, FBS_METRICSPORT, ...
//	DB parts:    FBS_ALKEMIODB_HOST/PORT/USER/PASSWORD/DBNAME/SSLMODE, FBS_LEDGERDB_*
//	Per target:  FBS_TARGET_<NAME>_ACCESSKEY / _SECRETKEY / _BUCKET / ...
//	             (<NAME> = target name upcased, non-alphanumerics -> '_')
package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const envPrefix = "FBS_"

// Target is one configured backup sink (symmetric — no required/optional).
type Target struct {
	Name        string `yaml:"name"`
	Type        string `yaml:"type"` // "s3" | "filesystem"
	Endpoint    string `yaml:"endpoint,omitempty"`
	Region      string `yaml:"region,omitempty"` // s3 region (Scaleway can't auto-discover on PutObject-only creds)
	Bucket      string `yaml:"bucket,omitempty"`
	Prefix      string `yaml:"prefix,omitempty"`
	Path        string `yaml:"path,omitempty"`
	Compression string `yaml:"compression,omitempty"` // "" | "none" | "zstd"
	AccessKey   string `yaml:"accessKey,omitempty"`   // secret — normally injected via env
	SecretKey   string `yaml:"secretKey,omitempty"`   // secret — normally injected via env
	UseSSL      bool   `yaml:"useSSL,omitempty"`
	SSE         bool   `yaml:"sse,omitempty"`      // server-side encryption at rest (MUST — constitution §V)
	Insecure    bool   `yaml:"insecure,omitempty"` // conscious opt-out of TLS+SSE (local dev only)
}

// DBConfig is a Postgres connection built from parts (so deployments can reuse
// the shared DATABASE_HOST/PORT via env and keep the password in a secret).
type DBConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"dbName"`
	SSLMode  string `yaml:"sslMode"`
}

// DSN renders a postgres:// URL (accepted by both pgxpool and the migrate step).
// A URL is used, not a libpq keyword string, so a password/user/dbname containing
// a space, quote, '=', or other special byte is percent-encoded — a keyword DSN
// would silently mis-parse it (e.g. a spaced password swallows the next keyword,
// or `sslmode=disable` in a password downgrades TLS).
func (d DBConfig) DSN() string {
	userinfo := url.User(d.User)
	if d.Password != "" {
		userinfo = url.UserPassword(d.User, d.Password)
	}
	u := url.URL{
		Scheme:   "postgres",
		User:     userinfo,
		Host:     net.JoinHostPort(d.Host, strconv.Itoa(d.Port)),
		Path:     "/" + d.DBName,
		RawQuery: url.Values{"sslmode": {d.SSLMode}}.Encode(),
	}
	return u.String()
}

func (d DBConfig) validate(name string) error {
	switch {
	case d.Host == "":
		return fmt.Errorf("%s.host is required", name)
	case d.User == "":
		return fmt.Errorf("%s.user is required", name)
	case d.DBName == "":
		return fmt.Errorf("%s.dbName is required", name)
	}
	return nil
}

// Config is the worker configuration.
type Config struct {
	FileServiceBase     string   `yaml:"fileServiceBase"`
	AlkemioDB           DBConfig `yaml:"alkemioDB"`
	LedgerDB            DBConfig `yaml:"ledgerDB"`
	Targets             []Target `yaml:"targets"`
	Concurrency         int      `yaml:"concurrency"`
	BackfillRatePerSec  int      `yaml:"backfillRatePerSec"`
	MetricsPort         int      `yaml:"metricsPort"`
	PerObjectTimeoutSec int      `yaml:"perObjectTimeoutSec"`
	StaleTTLSec         int      `yaml:"staleTTLSec"`
	PollEverySec        int      `yaml:"pollEverySec"`
	MaxAttempts         int      `yaml:"maxAttempts"`   // genuine-failure dead-letter threshold (FR-029)
	MaxDeliveries       int      `yaml:"maxDeliveries"` // crash-loop dead-letter threshold (FR-029)
}

// PerObjectTimeout is the per-object backup deadline.
func (c *Config) PerObjectTimeout() time.Duration {
	return time.Duration(c.PerObjectTimeoutSec) * time.Second
}

// StaleTTL is how long a claim may stay in_progress before the reaper requeues it.
func (c *Config) StaleTTL() time.Duration { return time.Duration(c.StaleTTLSec) * time.Second }

// PollEvery is the polling floor.
func (c *Config) PollEvery() time.Duration { return time.Duration(c.PollEverySec) * time.Second }

// Load reads YAML from path (if present — env-only is also valid), overlays env
// (FBS_* scalars/DB, FBS_TARGET_<NAME>_* per target), then applies defaults.
// Validation is a serve-time concern (Config.Validate).
func Load(path string) (*Config, error) {
	var c Config
	if path != "" {
		b, err := os.ReadFile(path) //nolint:gosec // operator-supplied config path
		switch {
		case err == nil:
			if err := yaml.Unmarshal(b, &c); err != nil {
				return nil, fmt.Errorf("parse config %q: %w", path, err)
			}
		case errors.Is(err, os.ErrNotExist):
			// env-only configuration is valid
		default:
			return nil, fmt.Errorf("read config %q: %w", path, err)
		}
	}
	if err := c.applyEnv(); err != nil {
		return nil, fmt.Errorf("env config: %w", err)
	}
	c.applyDefaults()
	return &c, nil
}

// applyEnv overlays FBS_* env vars, failing loudly on any malformed numeric/bool so
// a mis-typed SLO knob (e.g. FBS_STALETTLSEC=1h) can't silently revert to the default.
func (c *Config) applyEnv() error {
	var errs []error
	add := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	setStr(&c.FileServiceBase, envPrefix+"FILESERVICEBASE")
	add(setInt(&c.Concurrency, envPrefix+"CONCURRENCY"))
	add(setInt(&c.BackfillRatePerSec, envPrefix+"BACKFILLRATEPERSEC"))
	add(setInt(&c.MetricsPort, envPrefix+"METRICSPORT"))
	add(setInt(&c.PerObjectTimeoutSec, envPrefix+"PEROBJECTTIMEOUTSEC"))
	add(setInt(&c.StaleTTLSec, envPrefix+"STALETTLSEC"))
	add(setInt(&c.PollEverySec, envPrefix+"POLLEVERYSEC"))
	add(setInt(&c.MaxAttempts, envPrefix+"MAXATTEMPTS"))
	add(setInt(&c.MaxDeliveries, envPrefix+"MAXDELIVERIES"))
	add(applyDBEnv(&c.AlkemioDB, envPrefix+"ALKEMIODB_"))
	add(applyDBEnv(&c.LedgerDB, envPrefix+"LEDGERDB_"))
	add(c.applyTargetEnv())
	return errors.Join(errs...)
}

func applyDBEnv(d *DBConfig, p string) error {
	setStr(&d.Host, p+"HOST")
	setStr(&d.User, p+"USER")
	setStr(&d.Password, p+"PASSWORD")
	setStr(&d.DBName, p+"DBNAME")
	setStr(&d.SSLMode, p+"SSLMODE")
	return setInt(&d.Port, p+"PORT")
}

// applyTargetEnv overlays per-target fields from FBS_TARGET_<NAME>_<FIELD> — the
// path by which per-target secrets (ACCESSKEY/SECRETKEY) are injected from k8s
// secrets, and any structural field can be overridden.
func (c *Config) applyTargetEnv() error {
	errs := make([]error, 0, 3*len(c.Targets))
	for i := range c.Targets {
		p := envPrefix + "TARGET_" + envToken(c.Targets[i].Name) + "_"
		setStr(&c.Targets[i].Endpoint, p+"ENDPOINT")
		setStr(&c.Targets[i].Region, p+"REGION")
		setStr(&c.Targets[i].Bucket, p+"BUCKET")
		setStr(&c.Targets[i].Prefix, p+"PREFIX")
		setStr(&c.Targets[i].Path, p+"PATH")
		setStr(&c.Targets[i].Compression, p+"COMPRESSION")
		setStr(&c.Targets[i].AccessKey, p+"ACCESSKEY")
		setStr(&c.Targets[i].SecretKey, p+"SECRETKEY")
		errs = append(errs,
			setBool(&c.Targets[i].UseSSL, p+"USESSL"),
			setBool(&c.Targets[i].SSE, p+"SSE"),
			setBool(&c.Targets[i].Insecure, p+"INSECURE"))
	}
	return errors.Join(errs...)
}

func (c *Config) applyDefaults() {
	// All floors are <= 0 (not == 0): a negative env override must not survive into
	// pool sizing, a ":-1" listen addr, or a negative-duration ticker (panics).
	if c.Concurrency <= 0 {
		c.Concurrency = 8
	}
	if c.MetricsPort <= 0 {
		c.MetricsPort = 4004
	}
	if c.PerObjectTimeoutSec <= 0 {
		c.PerObjectTimeoutSec = 1800 // 30 min — must exceed the slowest legit backup
	}
	if c.StaleTTLSec <= 0 {
		c.StaleTTLSec = 3600 // 1 h — must exceed PerObjectTimeout so a running object isn't reaped
	}
	if c.PollEverySec <= 0 {
		c.PollEverySec = 10
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 10
	}
	if c.MaxDeliveries <= 0 {
		c.MaxDeliveries = 50
	}
	for _, d := range []*DBConfig{&c.AlkemioDB, &c.LedgerDB} {
		if d.Port == 0 {
			d.Port = 5432
		}
		if d.SSLMode == "" {
			d.SSLMode = "require"
		}
	}
}

// envToken upcases a target name and replaces non-alphanumerics with '_' so it is
// a valid env-var segment.
func envToken(name string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, strings.ToUpper(name))
}

func setStr(dst *string, key string) {
	if v, ok := os.LookupEnv(key); ok {
		*dst = v
	}
}

func setInt(dst *int, key string) error {
	if v, ok := os.LookupEnv(key); ok {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("%s: invalid integer %q", key, v)
		}
		*dst = n
	}
	return nil
}

func setBool(dst *bool, key string) error {
	if v, ok := os.LookupEnv(key); ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("%s: invalid bool %q", key, v)
		}
		*dst = b
	}
	return nil
}

// Validate checks the full serve configuration. Call it from serve, not Load.
func (c *Config) Validate() error {
	if c.FileServiceBase == "" {
		return errors.New("fileServiceBase is required")
	}
	if err := c.AlkemioDB.validate("alkemioDB"); err != nil {
		return err
	}
	if err := c.LedgerDB.validate("ledgerDB"); err != nil {
		return err
	}
	if len(c.Targets) == 0 {
		return errors.New("at least one target is required")
	}
	if c.Concurrency > 1024 { // keep pool sizing (int32(Concurrency)+8) sane and non-wrapping
		return fmt.Errorf("concurrency (%d) is unreasonably high (max 1024)", c.Concurrency)
	}
	if c.StaleTTLSec <= c.PerObjectTimeoutSec {
		return fmt.Errorf("staleTTLSec (%d) must exceed perObjectTimeoutSec (%d), else a running object is reaped",
			c.StaleTTLSec, c.PerObjectTimeoutSec)
	}
	if c.MetricsPort < 1 || c.MetricsPort > 65535 {
		return fmt.Errorf("metricsPort (%d) out of range 1-65535", c.MetricsPort)
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
		if t.Endpoint == "" || t.Bucket == "" || t.Region == "" {
			// region is mandatory: PutObject-only creds can't auto-discover it and SigV4
			// signs it — an empty region fails every request with SignatureDoesNotMatch.
			return fmt.Errorf("target %q: s3 requires endpoint, bucket, and region", t.Name)
		}
		if !t.Insecure && (!t.UseSSL || !t.SSE) {
			return fmt.Errorf("target %q: s3 requires useSSL and sse (constitution §V: TLS + SSE at rest); "+
				"set insecure=true only for local dev", t.Name)
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
