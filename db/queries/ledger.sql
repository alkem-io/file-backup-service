-- name: UpsertObject :exec
INSERT INTO file_backup_object ("externalID", size, "createdBy", "sourceCreatedDate", "mimeType")
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT ("externalID") DO NOTHING;

-- name: UpsertTargetStatus :exec
INSERT INTO file_backup_target_status ("externalID", target, state, "storedBytes", "verifiedAt")
VALUES ($1, $2, $3, $4, now())
ON CONFLICT ("externalID", target)
DO UPDATE SET state = EXCLUDED.state, "storedBytes" = EXCLUDED."storedBytes", "verifiedAt" = now();

-- name: GetObject :one
SELECT * FROM file_backup_object WHERE "externalID" = $1;
