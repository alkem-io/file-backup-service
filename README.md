# Alkemio File Backup Service

Continuous file-backup worker for Alkemio. Consumes the file-service transactional
**outbox** and fans out each object to **N content-addressed backup targets**
(S3 + filesystem), with backfill, reconciliation, and operator restore.

> **Spec (source of truth):** `agents-hq/specs/008-continuous-file-backup/`
> (workspace vertical feature). This repo implements the `file-backup-service` slice.

## Layout (hexagonal)

- `cmd/file-backup-service` — worker + CLI (`serve` / `backfill` / `restore` / `verify` / `reconcile` / `drill` / `migrate`)
- `internal/domain` — backup pipeline, `Sink` port, hash-arbiter transform, integrity hashing
- `internal/adapter/inbound` — outbox consumer, HTTP health/metrics (`/live`, `/health`, `/metrics`)
- `internal/adapter/outbound` — sink adapters (`s3`, `filesystem`), file-service content client, DB ports (outbox + ledger)
- `db/` — ledger migrations (golang-migrate) + sqlc queries

## Develop

```sh
make build          # go build
make test           # go test -race
make lint           # golangci-lint (config: .golangci.yml, inherited from file-service)
make sqlc-generate  # regenerate the ledger query layer
make openapi        # apispec -> openapi.yaml
make setup-hooks    # install the pre-commit drift checks
```

CI uses the shared **`alkem-io/github-workflows`** reusable workflows
(`go-ci.yml`, `container-pr.yml`, `container-release.yml`), matching file-service.

## Status

Scaffold. Real dependencies (pgx, aws-sdk, klauspost/compress, golang-migrate,
prometheus) and the worker/sink/ledger implementations land per
`specs/008-continuous-file-backup/tasks/file-backup-service.md`.

## License

EUPL-1.2 — see [LICENSE](./LICENSE).
