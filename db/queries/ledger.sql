-- name: ListTargetStates :many
SELECT target, state FROM file_backup_target_status WHERE "externalID" = $1;

-- The object row + all per-target statuses are written by a single atomic CTE that
-- sqlc's static analyzer can't type (multi-arg unnest); it lives as a raw pgx query
-- in internal/adapter/outbound/db/ledger.go (RecordBackup).
