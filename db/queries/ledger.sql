-- name: UpsertObject :exec
INSERT INTO file_backup_object ("externalID", size, "createdBy", "sourceCreatedDate")
VALUES ($1, $2, $3, $4)
ON CONFLICT ("externalID") DO NOTHING;

-- name: BatchUpsertTargetStatus :batchexec
INSERT INTO file_backup_target_status ("externalID", target, state, "storedBytes", "verifiedAt")
VALUES ($1, $2, $3, $4, CASE WHEN $3 = 'stored' THEN now() ELSE NULL END)
ON CONFLICT ("externalID", target)
DO UPDATE SET
  -- never downgrade a durable 'stored' row to 'failed'
  state = CASE WHEN file_backup_target_status.state = 'stored' AND EXCLUDED.state <> 'stored'
               THEN file_backup_target_status.state ELSE EXCLUDED.state END,
  -- storedBytes / verifiedAt only advance on a fresh successful store
  "storedBytes" = CASE WHEN EXCLUDED.state = 'stored' THEN EXCLUDED."storedBytes"
                       ELSE file_backup_target_status."storedBytes" END,
  "verifiedAt" = CASE WHEN EXCLUDED.state = 'stored' THEN now()
                      ELSE file_backup_target_status."verifiedAt" END;

-- name: ListTargetStates :many
SELECT target, state FROM file_backup_target_status WHERE "externalID" = $1;
