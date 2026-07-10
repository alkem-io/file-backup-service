// Package config loads the file-backup-service worker configuration: an OPTIONAL YAML base
// overlaid by 12-factor environment variables. Env wins, and a config file is NOT required — the
// entire config, INCLUDING the target list, can be supplied by env. Secrets (DB passwords, S3
// keys) come from env only.
//
//	Scalars:  FBS_FILESERVICEBASE, FBS_CONCURRENCY, FBS_METRICSPORT, ...
//	DB parts: FBS_ALKEMIODB_HOST/PORT/USER/PASSWORD/DBNAME/SSLMODE, FBS_LEDGERDB_*
//	Targets:  FBS_TARGETS=<comma-list of names> defines the LIST; each target's fields come from
//	          FBS_TARGET_<NAME>_<FIELD> — TYPE / PATH / ENDPOINT / BUCKET / REGION / ACCESSKEY /
//	          SECRETKEY / USESSL / SSE / ... (<NAME> = name upcased, non-alphanumerics -> '_').
//	          Without FBS_TARGETS the YAML target list stands; with it, it is authoritative.
package config

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/alkem-io/file-backup-service/internal/adapter/outbound/sink/filesystem"
	"github.com/alkem-io/file-backup-service/internal/adapter/outbound/sink/s3"
	"github.com/alkem-io/file-backup-service/internal/domain"
)

const envPrefix = "FBS_"

// defaultDBTimeoutSec is the default single-DB-operation bound (pool statement_timeout + client
// deadline) — the ONE owner, used both by the applyDefaults floor and by DBTimeout's degrade-to-default
// overflow guard, so a non-positive/absurd DBTimeoutSec on the unvalidated DR path can't yield a
// negative Duration (an unbounded pool).
const defaultDBTimeoutSec = 30

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
	// AuditAccessKey/AuditSecretKey are an OPTIONAL read/audit credential for a WORM target: the
	// worker's own credential is PutObject-only (can't read GetObjectLockConfig), so the immutability
	// drift-check needs a read-capable credential to actually run. When BOTH are set the drift-check
	// runs (Verified/Drift); when unset it is legitimately N/A → SILENT (no series, no alert, no false
	// pass) — the immutability is then asserted by object-lock + the audit + never_verified. Secrets,
	// injected via FBS_TARGET_<NAME>_AUDITACCESSKEY / _AUDITSECRETKEY.
	AuditAccessKey string `yaml:"auditAccessKey,omitempty"`
	AuditSecretKey string `yaml:"auditSecretKey,omitempty"`
	UseSSL         bool   `yaml:"useSSL,omitempty"`
	SSE            bool   `yaml:"sse,omitempty"`      // server-side encryption at rest (MUST — constitution §V)
	Insecure       bool   `yaml:"insecure,omitempty"` // conscious opt-out of TLS+SSE (local dev only)
	// Worm marks a write-once (object-lock/immutable) target. It is the target's IMMUTABILITY
	// declaration, NOT a claim that the target is unreadable: whether a WORM target can be VERIFIED
	// depends on whether an audit/read credential is set (AuditAccessKey/AuditSecretKey), not on this
	// flag. A WORM target WITH an audit credential is fully verified (existence + immutability +
	// inventory all run via the read credential); a WORM target WITHOUT one is legitimately N/A →
	// NoData (silent, no false alert AND no false pass), its immutability asserted by object-lock + the
	// audit + never_verified. The pass/fail axis is read-capability, not this flag (see
	// domain.targetUnverifiableExempt): only a WORM target that CANNOT be read is exempt from an
	// Unverifiable failing the audit. Configure the audit credential to enable the drift-check.
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
	CircuitThreshold    int      `yaml:"circuitThreshold"`   // per-target failures within the last 2x this many outcomes that trip its circuit open (T017a)
	CircuitCooldownSec  int      `yaml:"circuitCooldownSec"` // how long a tripped target's circuit stays open before a probe half-opens it
	FanoutStallSec      int      `yaml:"fanoutStallSec"`     // per-chunk drain deadline: a target not consuming a fan-out chunk in this long is dropped (hung-sink isolation)
	DBTimeoutSec        int      `yaml:"dbTimeoutSec"`       // bound on a single DB operation (pool statement_timeout + the claim/reap query deadline) so a slow/wedged DB can't hang a worker forever
	// ScratchDir is where reconcile stages a decoded object before re-fanning it out.
	// Empty = the OS temp dir; point it at a SIZED volume (not a small memory-backed
	// emptyDir/tmpfs) so a large-object repair on the recovery host can't fill /tmp.
	ScratchDir string `yaml:"scratchDir"`
}

// secondsToDuration converts a whole-seconds config value to a Duration, degrading a non-positive OR
// int64-nanosecond-OVERFLOWING value to fallback rather than to a negative/instant-expiry Duration — the
// ONE owner of that overflow guard, shared by the DR-path duration knobs (PerObjectTimeout, DBTimeout)
// which are read on paths that do NOT run validateLimits, so a hostile/absurd seconds value must degrade
// safely, never to a deadline that fails every op or (via NewPool's `>0` gate) unbounds the pool.
func secondsToDuration(sec int, fallback time.Duration) time.Duration {
	if sec <= 0 || int64(sec) > math.MaxInt64/int64(time.Second) {
		return fallback
	}
	return time.Duration(sec) * time.Second
}

// PerObjectTimeout is the per-object backup deadline. It returns 0 for a non-positive/overflowing value
// (the single-object DR read paths — cmd's sourceOp / restore current — do NOT run validateLimits); a 0
// return signals the caller to floor to the default via domain.NormalizePerObjectTimeout.
func (c *Config) PerObjectTimeout() time.Duration {
	return secondsToDuration(c.PerObjectTimeoutSec, 0)
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

// NewCircuitBreaker builds the per-target circuit breaker from config (threshold + cooldown) —
// one owner so serve/reconcile/backfill wire it identically.
func (c *Config) NewCircuitBreaker() *domain.CircuitBreaker {
	return domain.NewCircuitBreaker(c.CircuitThreshold, c.CircuitCooldown())
}

// FanoutStall is the per-chunk drain deadline before a non-consuming target is dropped.
func (c *Config) FanoutStall() time.Duration {
	return time.Duration(c.FanoutStallSec) * time.Second
}

// DBTimeout bounds a single DB operation — the pool's server-side statement_timeout AND the
// client-side deadline on the otherwise-unbounded claim/reap queries — so a slow or wedged
// Alkemio/ledger DB fails the op (retried) instead of parking a worker forever. It degrades a
// non-positive/overflowing DBTimeoutSec to the default (defaultDBTimeoutSec) rather than a NEGATIVE
// Duration, which would make NewPool's `statementTimeout > 0` gate skip statement_timeout ENTIRELY —
// silently opening an UNBOUNDED pool on the unvalidated DR path (restore current → openPool). Same
// overflow guard as PerObjectTimeout (secondsToDuration).
func (c *Config) DBTimeout() time.Duration {
	return secondsToDuration(c.DBTimeoutSec, defaultDBTimeoutSec*time.Second)
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
	add(setInt(&c.DBTimeoutSec, envPrefix+"DBTIMEOUTSEC"))
	add(applyDBEnv(&c.AlkemioDB, envPrefix+"ALKEMIODB_"))
	add(applyDBEnv(&c.LedgerDB, envPrefix+"LEDGERDB_"))
	c.seedTargetsFromEnv() // define the target LIST from env (full 12-factor) before overlaying fields
	add(c.applyTargetEnv())
	return errors.Join(errs...)
}

// seedTargetsFromEnv lets the target LIST itself be defined by env — full 12-factor, no config
// file required. FBS_TARGETS is a comma-separated list of target NAMES; each becomes a target
// whose fields are then supplied by applyTargetEnv from FBS_TARGET_<NAME>_* (TYPE / PATH / BUCKET
// / ENDPOINT / …). When set it is authoritative: a name that also appears in the YAML keeps that
// entry as a base (env overlays it), any other name is created fresh, and YAML targets NOT listed
// are dropped. Unset ⇒ the YAML target list stands (backward compatible). Order + de-dup follow
// FBS_TARGETS; blank names are skipped.
func (c *Config) seedTargetsFromEnv() {
	raw := os.Getenv(envPrefix + "TARGETS")
	if strings.TrimSpace(raw) == "" {
		return
	}
	// Key the YAML base by env-var TOKEN, not raw name — the identity applyTargetEnv and
	// ValidateTargets use — so FBS_TARGETS can reference a YAML-declared target by any
	// token-equivalent spelling (e.g. "OFFSITE" ↔ "offsite") and keep its entry (its Name and any
	// YAML-only fields). Matching by raw name would silently rebuild it as a bare target.
	base := make(map[string]Target, len(c.Targets))
	for _, t := range c.Targets {
		base[envToken(t.Name)] = t
	}
	out := make([]Target, 0, len(c.Targets)+1)
	seen := make(map[string]bool)
	for _, name := range strings.Split(raw, ",") {
		name = strings.TrimSpace(name)
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		if t, ok := base[envToken(name)]; ok {
			out = append(out, t) // keep the YAML-declared base (Name + fields); applyTargetEnv overlays it
		} else {
			out = append(out, Target{Name: name})
		}
	}
	c.Targets = out
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
	errs := make([]error, 0, 4*len(c.Targets)) // 4 setBool calls per target (useSSL/sse/insecure/worm)
	for i := range c.Targets {
		p := envPrefix + "TARGET_" + envToken(c.Targets[i].Name) + "_"
		setStr(&c.Targets[i].Type, p+"TYPE")
		setStr(&c.Targets[i].Endpoint, p+"ENDPOINT")
		setStr(&c.Targets[i].Region, p+"REGION")
		setStr(&c.Targets[i].Bucket, p+"BUCKET")
		setStr(&c.Targets[i].Prefix, p+"PREFIX")
		setStr(&c.Targets[i].Path, p+"PATH")
		setStr(&c.Targets[i].Compression, p+"COMPRESSION")
		setStr(&c.Targets[i].AccessKey, p+"ACCESSKEY")
		setStr(&c.Targets[i].SecretKey, p+"SECRETKEY")
		setStr(&c.Targets[i].AuditAccessKey, p+"AUDITACCESSKEY")
		setStr(&c.Targets[i].AuditSecretKey, p+"AUDITSECRETKEY")
		errs = append(errs,
			setBool(&c.Targets[i].UseSSL, p+"USESSL"),
			setBool(&c.Targets[i].SSE, p+"SSE"),
			setBool(&c.Targets[i].Insecure, p+"INSECURE"),
			setBool(&c.Targets[i].Worm, p+"WORM")) // structural: audit reads it — must be env-overridable like the rest
	}
	return errors.Join(errs...)
}

func (c *Config) applyDefaults() {
	// All floors are <= 0 (not == 0): a negative env override must not survive into pool
	// sizing, a ":-1" listen addr, or a negative-duration ticker (panics). orDefault holds the
	// per-field branch so this stays a flat list (one edit to add a knob, no cyclo creep).
	c.Concurrency = orDefault(c.Concurrency, 8)
	c.MetricsPort = orDefault(c.MetricsPort, 4004)
	c.PerObjectTimeoutSec = orDefault(c.PerObjectTimeoutSec, 1800) // 30 min — must exceed the slowest legit backup
	c.StaleTTLSec = orDefault(c.StaleTTLSec, 3600)                 // 1 h — must exceed PerObjectTimeout so a running object isn't reaped
	c.PollEverySec = orDefault(c.PollEverySec, 10)
	c.MaxAttempts = orDefault(c.MaxAttempts, 10)
	c.MaxDeliveries = orDefault(c.MaxDeliveries, 50)
	c.ManifestEverySec = orDefault(c.ManifestEverySec, 24*60*60)    // daily
	c.CircuitThreshold = orDefault(c.CircuitThreshold, 5)           // trip a target's circuit at 5 failures within its last 10 outcomes
	c.CircuitCooldownSec = orDefault(c.CircuitCooldownSec, 60)      // re-probe a down target once a minute
	c.FanoutStallSec = orDefault(c.FanoutStallSec, 60)              // a target not draining a 1 MiB chunk in 60s is hung, not slow
	c.DBTimeoutSec = orDefault(c.DBTimeoutSec, defaultDBTimeoutSec) // generous vs any healthy indexed/paged query; bounds a wedged DB
	for _, d := range []*DBConfig{&c.AlkemioDB, &c.LedgerDB} {
		if d.Port == 0 {
			d.Port = 5432
		}
		if d.SSLMode == "" {
			d.SSLMode = "require"
		}
	}
}

// orDefault returns v when it is positive, else d — the "floor a non-positive knob to its
// default" rule, in one place so applyDefaults stays a flat assignment list.
func orDefault(v, d int) int {
	if v <= 0 {
		return d
	}
	return v
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

// ValidateDRLimits validates the DR common config every ledger-DB subcommand needs: the numeric
// limits (so a huge perObjectTimeoutSec can't overflow to a negative Duration on this path) and the
// LedgerDB DSN (a malformed DSN fails with a clear 'ledgerDB.host is required' here, not an opaque pgx
// parse error later) — but NOT fileServiceBase/outbox (it runs in the degraded/DR environment) and
// NOT the target set. The SINGLE-source DR ops (restore-all / drill) use only this plus their ONE
// source target's validation (config.SelectReadTarget + ValidateTargetFields), so an UNRELATED
// misconfigured target can't block a restore/drill from a healthy --from (Pillar 4c — extended from
// build-time to validation-time).
func (c *Config) ValidateDRLimits() error {
	if err := c.validateLimits(); err != nil {
		return err
	}
	return c.LedgerDB.Validate("ledgerDB")
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
// range. Both serve (Validate) and the DR subcommands (ValidateDR) run validateLimits,
// which rejects Concurrency>1024 — so this clamp is a belt-and-suspenders guard that makes
// the int32 conversion provably non-overflowing at the call site regardless of validation.
func (c *Config) PoolSize(headroom int) int32 {
	n := min(max(c.Concurrency, 1), 1024)
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
		{"dbTimeoutSec", c.DBTimeoutSec},
	} {
		if f.sec > maxSec {
			return fmt.Errorf("%s (%d) exceeds the max %d", f.name, f.sec, maxSec)
		}
	}
	// staleTTL must exceed the per-object timeout PLUS the detached-bookkeeping window. After
	// the per-object timeout, an object does TWO sequential detached writes while still
	// status='in_progress', each up to BookkeepingTimeout: first the pipeline's RecordBackup
	// (ledger), then the consumer's MarkDone/Fail/Defer (outbox). So the true worst-case
	// settling time is perObjectTimeout + 2*BookkeepingTimeout; if the reaper's TTL falls
	// inside it, it requeues the completed object (deliveries++), creeping it toward the
	// crash-loop dead-letter for a non-problem.
	minStaleTTL := c.PerObjectTimeoutSec + 2*int(domain.BookkeepingTimeout.Seconds())
	if c.StaleTTLSec <= minStaleTTL {
		return fmt.Errorf("staleTTLSec (%d) must exceed perObjectTimeoutSec + 2*bookkeeping (%d), else the reaper can requeue a settling object",
			c.StaleTTLSec, minStaleTTL)
	}
	// circuitThreshold must be < maxAttempts, or an object needing a persistently-down target
	// dead-letters (Fail at maxAttempts) BEFORE the per-target circuit accumulates threshold
	// failures to trip — so it FAILS instead of DEFERRING (T017a), and in a single/all-target
	// outage it stores nowhere and reconcile can't repair it. The circuit must trip first.
	if c.CircuitThreshold >= c.MaxAttempts {
		return fmt.Errorf("circuitThreshold (%d) must be < maxAttempts (%d), else an object dead-letters before its target's circuit trips (defeating T017a defer-not-dead-letter)",
			c.CircuitThreshold, c.MaxAttempts)
	}
	// dbTimeout is the pool's server-side statement_timeout, so it must not fire BEFORE the
	// detached-bookkeeping budget (BookkeepingTimeout): a MarkDone/Fail/RecordBackup that runs
	// AFTER a per-object timeout/shutdown gets a fresh BookkeepingTimeout ctx, but it still
	// executes on a pooled connection whose statement_timeout would abort it early if dbTimeout
	// were smaller — stranding the row in_progress until the reaper requeues it (deliveries++
	// toward the crash-loop dead-letter for a non-problem). The default (30s) clears the 15s
	// budget; guard an operator override that doesn't.
	if minDB := int(domain.BookkeepingTimeout.Seconds()); c.DBTimeoutSec < minDB {
		return fmt.Errorf("dbTimeoutSec (%d) must be >= bookkeeping timeout (%ds), else the pool's statement_timeout aborts a detached bookkeeping write before its budget and strands the row",
			c.DBTimeoutSec, minDB)
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
	// CircuitThreshold feeds window = 2*threshold in NewCircuitBreaker; cap it so (a) the
	// per-target `recent []bool` can't be sized into GBs and (b) 2*threshold can't overflow
	// to a NEGATIVE window (a value > MaxInt/2), which would panic the breaker's slice reslice
	// on the FIRST outcome and — via the consumer's recover — march every object to dead-letter
	// while /health stays green. 10000 is far above any sane failure count to trip on.
	if c.CircuitThreshold > 10000 {
		return fmt.Errorf("circuitThreshold (%d) exceeds 10000", c.CircuitThreshold)
	}
	return nil
}

func validateTarget(i int, t Target, seen map[string]string) error {
	if err := validateTargetName(i, t, seen); err != nil {
		return err
	}
	return ValidateTargetFields(t)
}

// validateTargetName checks a target's NAME + the set-wide env-token collision guard (seen
// accumulates the tokens seen so far). Split from the field validation so a single-source DR op can
// run the collision guard over the whole set (a sibling must not have injected the chosen target's
// secret) WITHOUT validating every other target's fields (Pillar 4c).
func validateTargetName(i int, t Target, seen map[string]string) error {
	if t.Name == "" {
		return fmt.Errorf("target[%d]: name is required", i)
	}
	// The ledger's file_backup_target_status.target is VARCHAR(64); reject an over-long name
	// at config time, else every RecordBackup INSERT for it fails at runtime ("value too long
	// for type character varying(64)") → all its objects retry → dead-letter.
	if len(t.Name) > 64 {
		return fmt.Errorf("target %q: name exceeds 64 chars (ledger target column is VARCHAR(64))", t.Name)
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
	return nil
}

// ValidateTargetFields validates ONE target's structural fields (type, per-type required fields,
// codec) independently of the set — the exported half the single-source DR build path uses so it can
// validate + build ONLY the chosen target and not be blocked by an unrelated target's misconfig
// (Pillar 4c).
func ValidateTargetFields(t Target) error {
	f, ok := targetFactories[t.Type]
	if !ok {
		return fmt.Errorf("target %q: unknown type %q (want s3|filesystem)", t.Name, t.Type)
	}
	if err := f.validate(t); err != nil {
		return err
	}
	if _, err := domain.ParseCodec(t.Compression); err != nil {
		return fmt.Errorf("target %q: %w", t.Name, err)
	}
	return nil
}

// CheckTargetCollisions validates only that at least one target exists and that no two target names
// collide on the FBS_TARGET_<TOKEN>_* env namespace — the set-wide guard a single-source DR read runs
// (so a sibling target can't have injected the chosen target's secret) WITHOUT validating every
// target's fields. Returns nil for a well-named set.
func (c *Config) CheckTargetCollisions() error {
	if len(c.Targets) == 0 {
		return errors.New("at least one target is required")
	}
	seen := make(map[string]string, len(c.Targets))
	for i, t := range c.Targets {
		if err := validateTargetName(i, t, seen); err != nil {
			return err
		}
	}
	return nil
}

// SelectReadTarget resolves which configured target a READ op (restore/verify/drill) should use: the
// named `from` (honored as given — a WORM target IS allowed for an EXPLICIT choice, per Pillar 4b:
// restoring from the SOLE surviving immutable copy must not be refused; a read-deny then surfaces as
// a clear error), or, when `from` is empty, the FIRST readable (non-WORM) target — all readable
// targets are symmetric holders of the same content. It resolves off the CONFIG (names + Worm flags),
// so no sink is built for a target the op doesn't use (Pillar 4c).
func SelectReadTarget(targets []Target, from string) (Target, error) {
	if from != "" {
		for _, t := range targets {
			if t.Name == from {
				return t, nil // explicit choice honored, incl. a WORM target
			}
		}
		return Target{}, fmt.Errorf("target %q not found in config", from)
	}
	for _, t := range targets {
		if !t.Worm {
			return t, nil
		}
	}
	return Target{}, errors.New("no readable (non-WORM) target configured by default — restore/drill needs a target it can read; name a WORM/immutable copy explicitly via --from to attempt an admin-credential read")
}

// validateS3Target checks the s3-specific required fields (split out of validateTarget to
// keep its cyclomatic complexity in budget).
func validateS3Target(t Target) error {
	if t.Endpoint == "" || t.Bucket == "" || t.Region == "" {
		// region is mandatory: PutObject-only creds can't auto-discover it and SigV4 signs it —
		// an empty region fails every request with SignatureDoesNotMatch.
		return fmt.Errorf("target %q: s3 requires endpoint, bucket, and region", t.Name)
	}
	if t.AccessKey == "" || t.SecretKey == "" {
		// The s3 sink signs with STATIC credentials (credentials.NewStaticV4) — there is no
		// IAM/instance-profile fallback — so empty keys sign requests ANONYMOUSLY and every
		// PutObject 403s. Fail loud at config time (like every other s3 field) instead of a
		// degraded-target startup or an opaque 403 at preflight / restore fetch time.
		return fmt.Errorf("target %q: s3 requires accessKey and secretKey (inject via FBS_TARGET_%s_ACCESSKEY / _SECRETKEY)", t.Name, envToken(t.Name))
	}
	// The OPTIONAL audit/read credential is all-or-nothing: s3.New only builds the audit client when
	// BOTH keys are set, so a HALF-set pair would SILENTLY leave the WORM drift-check the operator
	// intended to enable disabled (ImmutabilityReadable()==false → NoData, no error, no alert). Fail
	// loud on a half-set pair (mirrors the primary pair).
	if (t.AuditAccessKey == "") != (t.AuditSecretKey == "") {
		return fmt.Errorf("target %q: the audit credential is all-or-nothing — set BOTH FBS_TARGET_%s_AUDITACCESSKEY and _AUDITSECRETKEY, or neither", t.Name, envToken(t.Name))
	}
	if !t.Insecure && (!t.UseSSL || !t.SSE) {
		return fmt.Errorf("target %q: s3 requires useSSL and sse (constitution §V: TLS + SSE at rest); "+
			"set insecure=true only for local dev", t.Name)
	}
	return nil
}

// targetFactory owns ONE target type's validation + sink construction in a single place, so
// adding a type — or a new required field on an existing one — is one self-contained
// registration rather than an edit to two separate switches (a per-type validate here and a
// per-type build in the cmd wiring) that have no compiler link and can silently drift out of
// sync. Trade-off: the config package imports the sink adapters (and transitively minio) so
// the build half can live with the validate half — accepted to make "add a type = one entry".
type targetFactory struct {
	validate func(Target) error
	build    func(Target) (domain.Sink, error)
}

var targetFactories = map[string]targetFactory{
	TargetTypeFilesystem: {validateFilesystemTarget, buildFilesystemSink},
	TargetTypeS3:         {validateS3Target, buildS3Sink},
}

// BuildSink constructs the sink for an (already-validated) target — the single dispatch point
// the cmd wiring calls, so type→constructor lives with type→validation in the registry above.
func BuildSink(t Target) (domain.Sink, error) {
	f, ok := targetFactories[t.Type]
	if !ok {
		return nil, fmt.Errorf("target %q: unknown type %q (want s3|filesystem)", t.Name, t.Type)
	}
	return f.build(t)
}

func validateFilesystemTarget(t Target) error {
	if t.Path == "" {
		return fmt.Errorf("target %q: filesystem requires path", t.Name)
	}
	return nil
}

func buildFilesystemSink(t Target) (domain.Sink, error) {
	return filesystem.New(t.Name, t.Path), nil
}

func buildS3Sink(t Target) (domain.Sink, error) {
	return s3.New(s3.Config{
		Name: t.Name, Endpoint: t.Endpoint, Region: t.Region, Bucket: t.Bucket, Prefix: t.Prefix,
		AccessKey: t.AccessKey, SecretKey: t.SecretKey, UseSSL: t.UseSSL, SSE: t.SSE,
		AuditAccessKey: t.AuditAccessKey, AuditSecretKey: t.AuditSecretKey,
	})
}
