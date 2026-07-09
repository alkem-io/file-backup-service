# Configuration Reference

Everything needed to configure and run `file-backup-service`. Configuration is a
**YAML** base file overlaid by **environment variables** — so a deployment can
keep the structure in a mounted `config.yaml` and inject secrets + per-environment
overrides from env.

- **Env wins.** Any `FBS_*` variable overrides the matching YAML scalar (12-factor).
- **Secrets come from env only** — DB passwords and S3 keys should never be in the
  committed YAML. The example file omits them on purpose.
- **Defaults are applied** when a value is absent or non-positive, so a minimal
  config is valid. Every default and constraint is listed below.
- A ready-to-copy starting point is [`config.example.yaml`](../config.example.yaml).

The config file path is passed with `--config` (default `config.yaml`) to the
subcommands that read it. `restore`/`verify` accept `--config` too but only use
the `targets` section.

---

## Quick start

Minimal `config.yaml` to run the worker against one local filesystem target (dev,
no TLS/SSE):

```yaml
fileServiceBase: http://file-service:4000

alkemioDB:
  host: alkemio-postgres
  user: file_backup_ro       # a SELECT/UPDATE-scoped role on the outbox
  dbName: alkemio
ledgerDB:
  host: filebackup-postgres
  user: filebackup
  dbName: filebackup

targets:
  - name: local
    type: filesystem
    path: /backup
```

Then inject secrets and run:

```sh
export FBS_ALKEMIODB_PASSWORD=…
export FBS_LEDGERDB_PASSWORD=…
file-backup-service migrate   # once: create the ledger schema
file-backup-service serve     # the worker
```

---

## Top-level parameters

| YAML key | Env override | Type | Default | Constraints | Description |
|---|---|---|---|---|---|
| `fileServiceBase` | `FBS_FILESERVICEBASE` | string (URL) | — | **required** (serve/backfill) | Base URL of the file-service internal content API (`GET {base}/internal/file/{id}/content`). |
| `concurrency` | `FBS_CONCURRENCY` | int | `8` | ≤ 1024 | In-flight objects: the fan-out worker pool size (serve) and the batch sweep parallelism (backfill/reconcile). Also sizes the pgx pools. |
| `perObjectTimeoutSec` | `FBS_PEROBJECTTIMEOUTSEC` | int (s) | `1800` | ≤ 604800 (1 wk) | Deadline for backing up / repairing one object. A hung fetch/sink fails that object, not the pass. |
| `staleTTLSec` | `FBS_STALETTLSEC` | int (s) | `3600` | > `perObjectTimeoutSec` + 30s; ≤ 1 wk | How long a claimed outbox row may stay `in_progress` before the reaper requeues it. Must exceed the per-object timeout + the bookkeeping window so a settling object isn't reaped. |
| `pollEverySec` | `FBS_POLLEVERYSEC` | int (s) | `10` | ≤ 1 wk | Polling floor: how often an idle worker re-checks the outbox even if a `NOTIFY` was missed. |
| `maxAttempts` | `FBS_MAXATTEMPTS` | int | `10` | ≤ 1000 | Genuine-failure dead-letter threshold: an object that fails this many times is dead-lettered. |
| `maxDeliveries` | `FBS_MAXDELIVERIES` | int | `50` | ≤ 1000 | Crash-loop dead-letter threshold: a row reaped (worker crashed mid-delivery) this many times is dead-lettered. |
| `manifestEverySec` | `FBS_MANIFESTEVERYSEC` | int (s) | `86400` (daily) | ≤ 1 wk | Cadence of the ledger-snapshot manifest written to each target (standalone restorability). |
| `circuitThreshold` | `FBS_CIRCUITTHRESHOLD` | int | `5` | < `maxAttempts`; ≤ 10000 | Per-target circuit trips open after this many failures within its last `2×` this many outcomes. Must be `< maxAttempts` so a down target trips (defer) before an object dead-letters. |
| `circuitCooldownSec` | `FBS_CIRCUITCOOLDOWNSEC` | int (s) | `60` | ≤ 1 wk | How long a tripped target's circuit stays open before one probe half-opens it. |
| `fanoutStallSec` | `FBS_FANOUTSTALLSEC` | int (s) | `60` | < `perObjectTimeoutSec`; ≤ 1 wk | Per-chunk drain deadline: a target not consuming a fan-out chunk within this is dropped as hung (before the object times out). |
| `dbTimeoutSec` | `FBS_DBTIMEOUTSEC` | int (s) | `30` | ≥ 15 (bookkeeping); ≤ 1 wk | Bound on a single DB operation — the pool's `statement_timeout` plus the claim/reap query deadline — so a slow/wedged DB fails the op instead of parking a worker. Must be ≥ the 15s bookkeeping window. |
| `metricsPort` | `FBS_METRICSPORT` | int | `4004` | ≤ 65535 | Port for the `/live` `/health` `/metrics` HTTP surface. |
| `scratchDir` | `FBS_SCRATCHDIR` | string (path) | `""` (OS temp) | — | Where `reconcile` stages a decoded object before re-fan-out. Point at a **sized, disk-backed** volume; a tmpfs `/tmp` can OOM on a large object. |

---

## Database blocks — `alkemioDB` and `ledgerDB`

Two Postgres connections, built from parts. `alkemioDB` is the **outbox** DB
(file-service's, accessed via a **scoped SELECT/UPDATE role** on the outbox +
`file` tables). `ledgerDB` is this service's **own** DB (owner rights).

| YAML key | Env override (per block) | Type | Default | Description |
|---|---|---|---|---|
| `host` | `FBS_ALKEMIODB_HOST` / `FBS_LEDGERDB_HOST` | string | — (**required**) | DB host. |
| `port` | `FBS_ALKEMIODB_PORT` / `FBS_LEDGERDB_PORT` | int | `5432` | DB port (1–65535). |
| `user` | `..._USER` | string | — (**required**) | Role name. |
| `password` | `..._PASSWORD` | string | — | Password. **Inject via env**, not YAML. |
| `dbName` | `..._DBNAME` | string | — (**required**) | Database name. |
| `sslMode` | `..._SSLMODE` | string | `require` | libpq sslmode (`disable`/`require`/`verify-full`/…). Prod uses `disable` only behind cluster network isolation. |

(`FBS_ALKEMIODB_HOST` can reuse the shared `DATABASE_HOST` value in a deployment.)

---

## Targets — `targets[]`

The **symmetric** backup destination list: every object goes to **every** target,
and an object is "done" only when all targets have it. There is no
primary/required/optional.

**The list can be defined entirely from env — no config file required.** Set
**`FBS_TARGETS`** to a comma-separated list of target names; each name becomes a
target whose fields are then supplied by `FBS_TARGET_<NAME>_<FIELD>` (below,
including `TYPE` and `PATH`). When `FBS_TARGETS` is set it is **authoritative**: a
name that also appears in the YAML keeps that entry as a base (env overlays it),
any other name is created fresh, and YAML targets not listed are dropped. Without
`FBS_TARGETS`, the YAML `targets:` list stands (and env still overlays fields).

Per-target secrets and any field can be injected with
`FBS_TARGET_<NAME>_<FIELD>`, where **`<NAME>`** is the target's `name` **upcased
with every non-alphanumeric replaced by `_`** (e.g. `offsite-eu` → `OFFSITE_EU`,
so `FBS_TARGET_OFFSITE_EU_ACCESSKEY`). Two names that collapse to the same token
are rejected at startup.

```bash
# fully env-defined, single local filesystem target (no config file):
FBS_TARGETS=local
FBS_TARGET_LOCAL_TYPE=filesystem
FBS_TARGET_LOCAL_PATH=/storage
```

| YAML key | Env override | Applies to | Description |
|---|---|---|---|
| (list) | `FBS_TARGETS` | all | Comma-separated target names — defines the list from env when set. |
| `name` | (from `FBS_TARGETS`) | all | Unique target name (≤ 64 chars — the ledger column width). |
| `type` | `FBS_TARGET_<N>_TYPE` | all | `s3` or `filesystem`. |
| `compression` | `FBS_TARGET_<N>_COMPRESSION` | all | `""`/`none` or `zstd` (per-target). |
| `worm` | `FBS_TARGET_<N>_WORM` | all | `true` for a write-once, read-denying (PutObject-only) target; audit expects its `Exists` to deny and won't alert. |
| `path` | `FBS_TARGET_<N>_PATH` | filesystem | Root directory (**required** for `filesystem`). Sharded two levels by hash. |
| `endpoint` | `FBS_TARGET_<N>_ENDPOINT` | s3 | S3 endpoint host (**required**). |
| `region` | `FBS_TARGET_<N>_REGION` | s3 | Region (**required** — PutObject-only creds can't auto-discover it; SigV4 signs it). |
| `bucket` | `FBS_TARGET_<N>_BUCKET` | s3 | Bucket (**required**). |
| `prefix` | `FBS_TARGET_<N>_PREFIX` | s3 | Optional key prefix. |
| `accessKey` | `FBS_TARGET_<N>_ACCESSKEY` | s3 | Access key (**required**; inject via env). |
| `secretKey` | `FBS_TARGET_<N>_SECRETKEY` | s3 | Secret key (**required**; inject via env). |
| `useSSL` | `FBS_TARGET_<N>_USESSL` | s3 | TLS to the endpoint. **Required true** unless `insecure`. |
| `sse` | `FBS_TARGET_<N>_SSE` | s3 | Server-side encryption at rest. **Required true** unless `insecure` (constitution §V). |
| `insecure` | `FBS_TARGET_<N>_INSECURE` | s3 | Conscious opt-out of TLS+SSE — **local dev only**. |

### Example — S3 prod target

```yaml
targets:
  - name: offsite
    type: s3
    endpoint: s3.nl-ams.scw.cloud
    region: nl-ams
    bucket: alkemio-file-backup
    useSSL: true
    sse: true
    worm: true          # immutable, PutObject-only credential
    compression: zstd
```
```sh
export FBS_TARGET_OFFSITE_ACCESSKEY=…
export FBS_TARGET_OFFSITE_SECRETKEY=…
```

---

## Which config each subcommand needs

| Subcommand | fileServiceBase | alkemioDB | ledgerDB | targets |
|---|:---:|:---:|:---:|:---:|
| `serve` | ✅ | ✅ (outbox) | ✅ | ✅ |
| `backfill` | ✅ | ✅ (`file` corpus) | ✅ | ✅ |
| `reconcile` | — | — | ✅ | ✅ |
| `audit` | — | — | ✅ | ✅ |
| `restore` / `verify` | — | — | — | ✅ (the `--from` target) |
| `migrate` | — | — | ✅ | — |
| `drill` *(planned)* | — | — | — | ✅ |

`reconcile`/`audit`/`restore`/`verify` run in a degraded/DR environment and
deliberately don't require file-service or the outbox DB. `drill` (a scheduled
restore drill — **not yet implemented**) will follow the same DR shape, exercising
restore against the targets; its config needs are expected to mirror
`restore`/`verify` (targets only) and will be confirmed when it lands.

---

## Validation

Config is validated at startup; a bad value fails **loudly** rather than
degrading silently. The cross-field rules (all enforced in `internal/config`):

- `staleTTLSec` **must exceed** `perObjectTimeoutSec + 2×15s` (else the reaper
  requeues a still-settling object).
- `circuitThreshold` **must be `<`** `maxAttempts` (else an object needing a
  down target dead-letters before the target's circuit trips — defeating
  defer-not-dead-letter).
- `fanoutStallSec` **must be `<`** `perObjectTimeoutSec` (else a hung target is
  never dropped before the object times out).
- `dbTimeoutSec` **must be `≥` 15s** (else the pool's `statement_timeout` aborts a
  detached bookkeeping write and strands the row).
- Each second-valued knob is capped at 1 week (overflow guard); `concurrency ≤
  1024`; `metricsPort ≤ 65535`; `maxAttempts`/`maxDeliveries ≤ 1000`.
- S3 targets require `endpoint` + `bucket` + `region` + `accessKey` + `secretKey`,
  and `useSSL` + `sse` unless `insecure`. Filesystem targets require `path`.

See also [`README.md`](../README.md) (overview + subcommands + operations) and
[`CLAUDE.md`](../CLAUDE.md) (the non-negotiable invariants).
