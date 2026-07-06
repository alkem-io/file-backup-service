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

	"github.com/alkem-io/file-backup-service/internal/domain"
)

const envPrefix = "FBS_"

// Target type vocabulary — one owner, so config validation and the sink builder can't
// disagree on a type string (as compression is owned by domain.ParseCodec).
const (
	TargetTypeS3         = "s3"
	TargetTypeFilesystem = "filesystem"
)

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
	// Worm marks a write-once target whose credential can't read (PutObject-only, e.g. the
	// immutable off-site copy): audit EXPECTS its Exists to always deny, so it isn't an
	// alert — whereas a normally-readable target that suddenly can't be verified IS.
	Worm bool `yaml:"worm,omitempty"`
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

// Validate checks the DB connection parts (host/user/dbName required, port in range).
// name labels the errors (e.g. "ledgerDB"). Used by serve/full Validate and by migrate.
func (d DBConfig) Validate(name string) error {
	switch {
	case d.Host == "":
		return fmt.Errorf("%s.host is required", name)
	case d.User == "":
		return fmt.Errorf("%s.user is required", name)
	case d.DBName == "":
		return fmt.Errorf("%s.dbName is required", name)
	case d.Port < 1 || d.Port > 65535:
		// applyDefaults only fills Port==0; catch a negative/out-of-range here with a
		// clear message instead of an opaque pgx parse error on the composed DSN.
		return fmt.Errorf("%s.port (%d) out of range 1-65535", name, d.Port)
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
	MetricsPort         int      `yaml:"metricsPort"`
	PerObjectTimeoutSec int      `yaml:"perObjectTimeoutSec"`
	StaleTTLSec         int      `yaml:"staleTTLSec"`
	PollEverySec        int      `yaml:"pollEverySec"`
	MaxAttempts         int      `yaml:"maxAttempts"`        // genuine-failure dead-letter threshold (FR-029)
	MaxDeliveries       int      `yaml:"maxDeliveries"`      // crash-loop dead-letter threshold (FR-029)
	ManifestEverySec    int      `yaml:"manifestEverySec"`   // ledger-snapshot cadence to each target (FR-015)
	CircuitThreshold    int      `yaml:"circuitThreshold"`   // consecutive per-target failures that trip its circuit open (T017a)
	CircuitCooldownSec  int      `yaml:"circuitCooldownSec"` // how long a tripped target's circuit stays open before a probe half-opens it
	FanoutStallSec      int      `yaml:"fanoutStallSec"`     // per-chunk drain deadline: a target not consuming a fan-out chunk in this long is dropped (hung-sink isolation)
	// ScratchDir is where reconcile stages a decoded object before re-fanning it out.
	// Empty = the OS temp dir; point it at a SIZED volume (not a small memory-backed
	// emptyDir/tmpfs) so a large-object repair on the recovery host can't fill /tmp.
	ScratchDir string `yaml:"scratchDir"`
}

// PerObjectTimeout is the per-object backup deadline.
func (c *Config) PerObjectTimeout() time.Duration {
	return time.Duration(c.PerObjectTimeoutSec) * time.Second
}

// StaleTTL is how long a claim may stay in_progress before the reaper requeues it.
func (c *Config) StaleTTL() time.Duration { return time.Duration(c.StaleTTLSec) * time.Second }

// PollEvery is the polling floor.
func (c *Config) PollEvery() time.Duration { return time.Duration(c.PollEverySec) * time.Second }

// ManifestEvery is the ledger-snapshot cadence.
func (c *Config) ManifestEvery() time.Duration {
	return time.Duration(c.ManifestEverySec) * time.Second
}

// CircuitCooldown is how long a tripped target's circuit stays open before a probe.
func (c *Config) CircuitCooldown() time.Duration {
	return time.Duration(c.CircuitCooldownSec) * time.Second
}

// FanoutStall is the per-chunk drain deadline before a non-consuming target is dropped.
func (c *Config) FanoutStall() time.Duration {
	return time.Duration(c.FanoutStallSec) * time.Second
}

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
	setStr(&c.ScratchDir, envPrefix+"SCRATCHDIR")
	add(setInt(&c.Concurrency, envPrefix+"CONCURRENCY"))
	add(setInt(&c.MetricsPort, envPrefix+"METRICSPORT"))
	add(setInt(&c.PerObjectTimeoutSec, envPrefix+"PEROBJECTTIMEOUTSEC"))
	add(setInt(&c.StaleTTLSec, envPrefix+"STALETTLSEC"))
	add(setInt(&c.PollEverySec, envPrefix+"POLLEVERYSEC"))
	add(setInt(&c.MaxAttempts, envPrefix+"MAXATTEMPTS"))
	add(setInt(&c.MaxDeliveries, envPrefix+"MAXDELIVERIES"))
	add(setInt(&c.ManifestEverySec, envPrefix+"MANIFESTEVERYSEC"))
	add(setInt(&c.CircuitThreshold, envPrefix+"CIRCUITTHRESHOLD"))
	add(setInt(&c.CircuitCooldownSec, envPrefix+"CIRCUITCOOLDOWNSEC"))
	add(setInt(&c.FanoutStallSec, envPrefix+"FANOUTSTALLSEC"))
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
	if c.ManifestEverySec <= 0 {
		c.ManifestEverySec = 24 * 60 * 60 // daily
	}
	if c.CircuitThreshold <= 0 {
		c.CircuitThreshold = 5 // trip a target's circuit after 5 consecutive failures
	}
	if c.CircuitCooldownSec <= 0 {
		c.CircuitCooldownSec = 60 // re-probe a down target once a minute
	}
	if c.FanoutStallSec <= 0 {
		c.FanoutStallSec = 60 // a target not draining a 1 MiB chunk in 60s is hung, not slow
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

// setParsed overlays *dst from the env var key via parse, failing loudly on a
// malformed value. setStr stays separate — its assignment can't fail.
func setParsed[T any](dst *T, key, kind string, parse func(string) (T, error)) error {
	if v, ok := os.LookupEnv(key); ok {
		n, err := parse(v)
		if err != nil {
			return fmt.Errorf("%s: invalid %s %q", key, kind, v)
		}
		*dst = n
	}
	return nil
}

func setInt(dst *int, key string) error   { return setParsed(dst, key, "integer", strconv.Atoi) }
func setBool(dst *bool, key string) error { return setParsed(dst, key, "bool", strconv.ParseBool) }

// Validate checks the full serve configuration. Call it from serve, not Load.
func (c *Config) Validate() error {
	if c.FileServiceBase == "" {
		return errors.New("fileServiceBase is required")
	}
	if err := c.AlkemioDB.Validate("alkemioDB"); err != nil {
		return err
	}
	if err := c.LedgerDB.Validate("ledgerDB"); err != nil {
		return err
	}
	if err := c.validateLimits(); err != nil {
		return err
	}
	return c.ValidateTargets()
}

// ValidateDR is the check the ledger-DB DR subcommands (reconcile/audit) need: the
// numeric limits (so a huge perObjectTimeoutSec can't overflow to a negative Duration on
// this path), the LedgerDB (they connect to it — so a malformed DSN fails with a clear
// 'ledgerDB.host is required' here, not an opaque pgx parse error later), PLUS the target
// set — but NOT fileServiceBase/outbox, so it still runs in the degraded/DR environment.
func (c *Config) ValidateDR() error {
	if err := c.validateLimits(); err != nil {
		return err
	}
	if err := c.LedgerDB.Validate("ledgerDB"); err != nil {
		return err
	}
	return c.ValidateTargets()
}

// ValidateTargets validates the target set — each target's fields plus no env-token
// collisions (two names mapping to one FBS_TARGET_<TOKEN>_* would silently share
// secrets). Used by restore/verify (which need only a single sink built).
func (c *Config) ValidateTargets() error {
	if len(c.Targets) == 0 {
		return errors.New("at least one target is required")
	}
	seen := make(map[string]string, len(c.Targets))
	for i, t := range c.Targets {
		if err := validateTarget(i, t, seen); err != nil {
			return err
		}
	}
	return nil
}

// PoolSize returns a pgx pool max-conns of Concurrency + headroom, CLAMPED to a sane
// range. serve validates Concurrency (errors on >1024), but the DR subcommands
// (reconcile/audit) only validate targets, so this clamp is their safety net — it makes
// the int32 conversion provably non-overflowing on every path, not just serve's.
func (c *Config) PoolSize(headroom int) int32 {
	n := c.Concurrency
	if n < 1 {
		n = 1
	}
	if n > 1024 {
		n = 1024
	}
	return int32(n + headroom) //nolint:gosec // n clamped to <=1024, so n+headroom fits int32
}

// validateLimits range-checks the numeric knobs (kept out of Validate for clarity +
// cyclomatic budget).
func (c *Config) validateLimits() error {
	if c.Concurrency > 1024 { // keep pool sizing (int32(Concurrency)+8) sane and non-wrapping
		return fmt.Errorf("concurrency (%d) is unreasonably high (max 1024)", c.Concurrency)
	}
	// Cap the second-valued knobs well below the int64-nanosecond overflow point, so
	// sec*time.Second can't wrap to a negative Duration (which would panic
	// time.NewTicker or fire an instant timeout). One week is far above any sane value.
	const maxSec = 7 * 24 * 60 * 60
	for _, f := range []struct {
		name string
		sec  int
	}{
		{"perObjectTimeoutSec", c.PerObjectTimeoutSec}, {"staleTTLSec", c.StaleTTLSec},
		{"pollEverySec", c.PollEverySec}, {"manifestEverySec", c.ManifestEverySec},
		{"circuitCooldownSec", c.CircuitCooldownSec}, {"fanoutStallSec", c.FanoutStallSec},
	} {
		if f.sec > maxSec {
			return fmt.Errorf("%s (%d) exceeds the max %d", f.name, f.sec, maxSec)
		}
	}
	// staleTTL must exceed the per-object timeout PLUS the detached-bookkeeping window: an
	// object can hit the full per-object timeout and then spend up to BookkeepingTimeout
	// writing its MarkDone/Fail on a detached ctx while still status='in_progress'. If the
	// reaper's TTL falls inside that window it requeues the completed object (deliveries++),
	// creeping it toward the crash-loop dead-letter for a non-problem.
	minStaleTTL := c.PerObjectTimeoutSec + int(domain.BookkeepingTimeout.Seconds())
	if c.StaleTTLSec <= minStaleTTL {
		return fmt.Errorf("staleTTLSec (%d) must exceed perObjectTimeoutSec + bookkeeping (%d), else the reaper can requeue a settling object",
			c.StaleTTLSec, minStaleTTL)
	}
	// The stall-drop MUST fire before the per-object timeout, or a hung target stalls the
	// whole fan-out barrier until the shared timeout aborts every target in lockstep (the
	// circuit never trips → the object Fails instead of Defers → the corpus dead-letters,
	// the exact T017a failure mode). Enforce the ordering instead of leaving it to luck.
	if c.FanoutStallSec >= c.PerObjectTimeoutSec {
		return fmt.Errorf("fanoutStallSec (%d) must be < perObjectTimeoutSec (%d), else a hung target is never dropped before the object times out",
			c.FanoutStallSec, c.PerObjectTimeoutSec)
	}
	// Only upper bounds here — applyDefaults already floors each of these <=0 to a
	// positive default before Validate runs (as with Concurrency above), so a `< 1`
	// check would be dead and misleadingly imply 0 is rejected when it becomes the default.
	if c.MetricsPort > 65535 {
		return fmt.Errorf("metricsPort (%d) exceeds 65535", c.MetricsPort)
	}
	if c.MaxAttempts > 1000 {
		return fmt.Errorf("maxAttempts (%d) exceeds 1000", c.MaxAttempts)
	}
	if c.MaxDeliveries > 1000 {
		return fmt.Errorf("maxDeliveries (%d) exceeds 1000", c.MaxDeliveries)
	}
	return nil
}

func validateTarget(i int, t Target, seen map[string]string) error {
	if t.Name == "" {
		return fmt.Errorf("target[%d]: name is required", i)
	}
	// Dedup on the env-var token, not just the raw name: two distinct names that
	// collapse to the same FBS_TARGET_<TOKEN>_* prefix (e.g. "s3-eu" / "s3_eu") would
	// silently share injected secrets/bucket, so reject the collision.
	tok := envToken(t.Name)
	if prev, ok := seen[tok]; ok {
		if prev == t.Name {
			return fmt.Errorf("duplicate target name %q", t.Name)
		}
		return fmt.Errorf("targets %q and %q collide on env-var namespace FBS_TARGET_%s_*", prev, t.Name, tok)
	}
	seen[tok] = t.Name
	switch t.Type {
	case TargetTypeFilesystem:
		if t.Path == "" {
			return fmt.Errorf("target %q: filesystem requires path", t.Name)
		}
	case TargetTypeS3:
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
	if _, err := domain.ParseCodec(t.Compression); err != nil {
		return fmt.Errorf("target %q: %w", t.Name, err)
	}
	return nil
}
