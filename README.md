# Alkemio File Backup Service

Continuous, off-cluster, immutable, integrity-verified **backup** for every
file-service object. It consumes the file-service transactional **outbox**,
fetches each object by its content hash, and fans it out to **N content-addressed
backup targets** (S3 + filesystem) — plus backfill, reconciliation, audit, and
operator restore. A worker + CLI, not a public API.

> **Spec (source of truth):** `agents-hq/specs/008-continuous-file-backup/`
> (workspace vertical feature). This repo implements the `file-backup-service` slice.

## How it works

1. On each store, file-service writes a row to a transactional **outbox** in the
   Alkemio DB. This service consumes it via Postgres `LISTEN/NOTIFY` + a polling
   floor + `FOR UPDATE SKIP LOCKED` (horizontally safe).
2. For each entry it fetches the plaintext once from file-service, streams it
   through a **SHA3-256 verifier**, and fans it out concurrently to every
   configured target — each target optionally applying **zstd** compression.
   Content is addressed by its hash (identity, key, and verifier), so every copy
   is self-verifying and restorable from bytes + a hash alone.
3. A target is "done" only when **every** target has it (symmetric targets). A
   persistently-down target is isolated by a per-chunk **stall-drop** and a
   sliding-window **circuit breaker**, so a single-target outage defers objects
   (re-claimable) rather than dead-lettering the corpus; **reconcile** refills the
   gaps when the target returns.
4. It owns a **ledger** in its own database (what is stored where), never trusts a
   target's own listing, and **never** has a delete path to the primary store or
   an immutable target.

## Subcommands

`file-backup-service <subcommand> [--flags]`

| Subcommand | What it does |
|---|---|
| `serve`     | The worker: drain the outbox continuously, fan out to all targets, run the RPO/coverage samplers + periodic ledger-snapshot manifests, serve `/live` `/health` `/metrics`. |
| `backfill`  | Back up the whole pre-existing corpus (the file-service `file` table) — for objects created before this service. Resumable + rate-limited (`--rate`). |
| `reconcile` | Repair under-replicated objects target-to-target: fetch from a holder, re-fan-out to the targets missing it (`--rate`). |
| `audit`     | Verify the ledger against reality (FR-014): sample objects per target and confirm the target still holds them (ledger→target). Also runs the WORM immutability drift-check, and — with `--inventory` — the target→ledger direction (diff each target's manifest against the ledger). Nonzero exit on missing/drift/unverifiable, for cron/CI (`--sample`, `--inventory`). |
| `restore`   | Operator DR. `restore [object] --hash --from [--to]` restores one object; `restore all --from [--to] [--concurrency]` restores the whole store (resumable, idempotent, prints per-target sizes; fails loud on **0 objects enumerated** — an empty/wrong source); `restore current --file-id --at [--hash] [--from] [--to]` restores a file's CURRENT backed-up version, guarded by `--at` (fail loud otherwise — see below). Every restored object is hash-verified. `--from` default picks the first **readable** (non-WORM) target; an **explicit** `--from <worm>` is allowed (restore from the sole surviving immutable copy is attempted with a read-capable credential — a 403 fails with an actionable error). |
| `verify`    | Confirm one stored object decodes + hashes correctly, no write (`--hash`, `--from`). |
| `migrate`   | Apply the embedded ledger migrations to the ledger DB (a one-shot init step / init-container). |
| `drill`     | Restore drill (FR-024): restore a random sample of the objects stored on a target to a scratch dir, hash-verify each, exit nonzero if any fails — proving the end-to-end restore *procedure*, not just byte existence (`--from`, `--sample`, `--to`, `--metrics-file`). |

## Configuration

A **YAML** base (`config.yaml`; see [`config.example.yaml`](./config.example.yaml))
overlaid by **`FBS_*` environment variables** — env wins (12-factor). **Secrets
(DB passwords, S3 keys) come from env only.** The complete parameter reference —
every field, env var, default, and constraint — is in
**[`docs/configuration.md`](./docs/configuration.md)**; a summary follows.

- `fileServiceBase` / `FBS_FILESERVICEBASE` — base URL for the internal content API.
- `alkemioDB` (host/port/user/password/dbName/sslMode) — the outbox DB, accessed
  via a **scoped SELECT/UPDATE role**. Env: `FBS_ALKEMIODB_HOST`, `FBS_ALKEMIODB_PASSWORD`, …
- `ledgerDB` (same shape) — this service's **own** DB (`filebackup`). Env: `FBS_LEDGERDB_*`.
- `targets[]` — the **symmetric** target list; each `{name, type: s3|filesystem,
  endpoint/bucket/region/path, compression, useSSL, sse, worm}`. Per-target secrets:
  `FBS_TARGET_<NAME>_ACCESSKEY` / `_SECRETKEY` / `_BUCKET` / … (`<NAME>` = the name
  upcased, non-alphanumerics → `_`). S3 requires TLS + server-side encryption
  unless the target is explicitly `insecure` (local dev only).
- Tunables (all `FBS_*`-overridable, sensible defaults applied): `concurrency`,
  `perObjectTimeoutSec`, `staleTTLSec`, `pollEverySec`, `maxAttempts`,
  `maxDeliveries`, `manifestEverySec`, `circuitThreshold`, `circuitCooldownSec`,
  `fanoutStallSec`, `dbTimeoutSec`, `metricsPort` (default 4004), `scratchDir`.

## Operations

- **Health:** `GET /live` (process alive) and `GET /health` (dependencies —
  outbox + ledger reachable; 503 when a probe fails). `GET /metrics` (Prometheus).
- **Key metrics:** per-target stored/failed/dedup counters, dead-letter and
  per-object-timeout totals, source-gone total, the RPO gauges (backlog depth,
  oldest-pending age, last-success age, targets-circuit-open, under-replicated
  objects), `filebackup_immutability_ok{target}` (WORM object-lock + versioning
  drift — 1 ok / 0 drift; emitted only for a target **verified this pass**), and
  `filebackup_immutability_unverifiable{target}` (1 while a WORM target the worker
  *should* be able to read has been unverifiable — a credential rotated to write-only,
  a wedged endpoint). A target that turns unverifiable **drops** its `_ok` series
  (never frozen stale-green) and raises `_unverifiable`, so a later real drift can't
  be masked. Alert on `filebackup_under_replicated_objects > 0`,
  `filebackup_targets_circuit_open > 0`, `filebackup_immutability_ok == 0`,
  `filebackup_immutability_unverifiable == 1` sustained, and a climbing last-success age.
- **Restore-drill metrics:** the `drill` subcommand is short-lived, so it exports
  `filebackup_restore_drill_pass` (1/0) + `filebackup_drill_last_success_timestamp_seconds`
  via a Prometheus **textfile** (`--metrics-file` / `FBS_DRILL_METRICS_FILE`, the
  node-exporter textfile-collector convention); its primary signal is the exit code
  (a failing drill Job trips `kube_job_status_failed`, like the audit job).
- **Restore by point-in-time (`restore current`):** the live `file` table holds only
  each file's *current* version (no history), so this restores the CURRENT backed-up
  version **guarded by `--at`** — it **fails loud** (never guesses): if the current
  version was last-modified after `--at`, or `updatedDate` is NULL, you must recover
  the historical `file.externalID` from a **DB point-in-time restore** and pass it via
  `--hash`. See [`restore-and-ops.md`](../agents-hq/specs/008-continuous-file-backup/contracts/restore-and-ops.md).

## Layout (hexagonal)

- `cmd/file-backup-service` — worker + CLI (the subcommands above).
- `internal/domain` — backup pipeline, `Sink` port, SHA3-256 hash-arbiter
  transform, circuit breaker, reconcile/audit/backfill/restore, samplers.
- `internal/adapter/inbound` — outbox consumer, HTTP health/metrics surface.
- `internal/adapter/outbound` — sink adapters (`s3`, `filesystem`), file-service
  content client, DB adapters (outbox + `file` corpus in the Alkemio DB; the
  ledger in its own DB via sqlc).
- `db/` — ledger migrations (golang-migrate, single source of truth) + sqlc queries.

## Develop

```sh
make build          # go build
make test           # go test -race (prints total coverage)
make cover-check    # fail if total coverage < 95% (constitution §VII)
make lint           # golangci-lint (config: .golangci.yml, inherited from file-service)
make sqlc-generate  # regenerate the ledger query layer from db/queries + db/migrations
make openapi        # apispec -> openapi.yaml
make setup-hooks    # install the pre-commit drift checks (sqlc + openapi)
```

CI uses the shared **`alkem-io/github-workflows`** reusable workflows
(`go-ci.yml`, `container-pr.yml`, `container-release.yml`), matching file-service.
Conventions and the non-negotiable invariants (SHA3-256 addressing, no delete
path, retain-all, `actorId` not `userId`, sqlc-only ledger SQL, >95% coverage)
are in [`CLAUDE.md`](./CLAUDE.md) and [`.specify/memory/constitution.md`](./.specify/memory/constitution.md).

## Status

**Implemented.** The worker (serve), backfill, reconcile, audit (with the WORM
immutability drift-check + the `--inventory` target→ledger direction), restore
(single object / `all` / `current`), verify, `drill`, and migrate are functional;
the streaming fan-out, circuit-breaker isolation, ledger, and observability are in
place. See `specs/008-continuous-file-backup/` for the task breakdown and status.

## License

EUPL-1.2 — see [LICENSE](./LICENSE).
