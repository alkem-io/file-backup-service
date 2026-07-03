# Alkemio File Backup Service (Go)

> **Workspace context.** This repo is part of the Alkemio polyrepo at
> [alkem-io/agents-hq](https://github.com/alkem-io/agents-hq).
> Cross-repo (vertical) feature specs live there under `specs/NNN-*/`. When
> working on a `feat/NNN-...` branch in this repo, the matching workspace spec
> (`specs/008-continuous-file-backup/`) is the single source of truth.

Go microservice that gives every file-service object a durable, off-cluster,
immutable, integrity-verified **backup**, captured continuously and with low
RPO. It consumes a transactional **outbox** the file-service writes on each
store, fetches the bytes by id, and fans them out to **N content-addressed
targets**. It also provides backfill, reconciliation, and operator restore.

## Tech Stack

- **Language**: Go 1.26 · **Architecture**: Hexagonal (ports & adapters)
- **Databases**: PostgreSQL via **pgx v5** — reads the **outbox** in the Alkemio DB via a scoped SELECT/UPDATE role; owns its **ledger** in its **own database**
- **Ledger schema/migrations**: **sqlc** (typed queries) + **golang-migrate** (`//go:embed`, `iofs`); sqlc `schema` points at the migrations dir (single source of truth)
- **Targets (sinks)**: S3-compatible object storage (AWS SDK Go v2 / minio-go) + POSIX filesystem, behind one `Sink` port
- **Compression**: klauspost/compress (**zstd**), adaptive, per-target; **no per-object codec field** (hash-arbiter)
- **Integrity**: SHA3-256 content hash (FIPS 202, `crypto/sha3`) — the object's identity, key, and verifier
- **Consumer**: Postgres `LISTEN/NOTIFY` + polling floor + `FOR UPDATE SKIP LOCKED`
- **Logging**: Zap (structured) · **HTTP**: chi v5 (health/metrics only) · **Metrics**: Prometheus
- **Reference service**: `file-service` (same conventions — mirror its tooling)

## Architecture

A worker + CLI, **not** a public API. Content is addressed by **SHA3-256**
(the file-service convention), which makes every copy self-verifying and every
target restorable with nothing but bytes + a hash check.

- **inbound**: the outbox consumer (LISTEN/NOTIFY + poll + SKIP LOCKED); a
  health/metrics HTTP surface. No public, auth-guarded endpoints.
- **domain**: the backup pipeline (fetch → optional zstd → fan-out → verify →
  ledger), the `Sink` port, the hash-arbiter transform, reconciliation.
- **outbound**: `Sink` adapters (s3, filesystem), the file-service content
  client (`GET /internal/file/{id}/content`), the DB adapters (outbox in the
  Alkemio DB; ledger in the own DB).
- **cmd**: subcommands — `serve`, `backfill`, `restore`, `verify`,
  `reconcile`, `drill`, `migrate`.

## Anti-Patterns — Strictly Prohibited

1. Do not apply speculative fixes — find root cause first
2. Do not keep code "just in case" or for backward compatibility unless explicitly requested
3. Do not duplicate logic — find or create a single shared implementation
4. Do not add superficial tests for coverage padding
5. Do not invent performance SLAs without evidence
6. Do not create abstractions for hypothetical future needs
7. Do not add comments explaining obvious code
8. Do not rely on training data for dependency versions — check online
9. Do not create documentation files unless explicitly requested
10. Do not assume — ask or search when something is unclear

## Development Workflow

- Always run `golangci-lint run` before committing (config in `.golangci.yml`, inherited from file-service)
- CI is the shared `alkem-io/github-workflows` reusable workflows (`go-ci.yml`, `container-pr.yml`, `container-release.yml`) — do not fork bespoke CI
- Tests must defend real invariants — no coverage-padding tests
- Root-cause analysis is mandatory before any bug fix; document the cause with evidence
- Verify latest dependency versions online (pkg.go.dev, GitHub releases) — never trust training data
- Migrations are the single source of truth for the ledger schema; regenerate sqlc after changing them (`make sqlc-generate`)
- Never a delete path to the primary store or the immutable target; never GC by default (retain-all)
- Use `actorId` internally, never `userId`

## Configuration (env / config file)

- `FILE_SERVICE_BASE` — base URL for `GET /internal/file/{id}/content`
- `ALKEMIO_DB_DSN` — scoped role for the outbox (SELECT/UPDATE only)
- `LEDGER_DB_DSN` — this service's own ledger database
- `TARGETS` — list of sinks (type, endpoint/bucket/path, required, compression, immutable, credentialsRef)
- `CONCURRENCY`, `BACKFILL_RATE_PER_SEC`, retry/attempt limits, reconciliation schedule, RPO SLO
- `METRICS_PORT` — health/metrics listen port (default 4004)

## Full Constitution

See `.specify/memory/constitution.md` — inherited and adapted from file-service.
