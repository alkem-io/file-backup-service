module github.com/alkem-io/file-backup-service

go 1.26

// Scaffold deps: chi (HTTP), zap (logging), pgx (ledger/outbox). Remaining deps
// (aws-sdk-go-v2/minio-go, klauspost/compress zstd, golang-migrate, prometheus)
// land with the implementation tasks in
// specs/008-continuous-file-backup/tasks/file-backup-service.md.

require (
	github.com/go-chi/chi/v5 v5.3.0
	github.com/jackc/pgx/v5 v5.10.0
	go.uber.org/zap v1.28.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)
