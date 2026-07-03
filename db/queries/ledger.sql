-- name: UpsertObject :exec
INSERT INTO file_backup_object ("externalID", size, "createdBy", "sourceCreatedDate")
VALUES ($1, $2, $3, $4)
ON CONFLICT ("externalID") DO NOTHING;

-- name: UpsertTargetStatus :exec
INSERT INTO file_backup_target_status ("externalID", target, state, "storedBytes", "verifiedAt")
VALUES ($1, $2, $3, $4, CASE WHEN $3 = 'stored' THEN now() ELSE NULL END)
ON CONFLICT ("externalID", target)
DO UPDATE SET state = EXCLUDED.state,
              "storedBytes" = EXCLUDED."storedBytes",
              "verifiedAt" = CASE WHEN EXCLUDED.state = 'stored' THEN now()
                                  ELSE file_backup_target_status."verifiedAt" END;

-- name: GetObject :one
SELECT * FROM file_backup_object WHERE "externalID" = $1;

-- name: GetTargetStatus :one
SELECT state, "storedBytes" FROM file_backup_target_status
WHERE "externalID" = $1 AND target = $2;
