-- Revert the "externalID" columns to the database default collation and rebuild the covering index.
-- Reversible counterpart of 000002 up; idempotent (ALTER to the default collation + IF EXISTS index).
ALTER TABLE file_backup_target_status
    ALTER COLUMN "externalID" TYPE VARCHAR(128) COLLATE "default";

ALTER TABLE file_backup_object
    ALTER COLUMN "externalID" TYPE VARCHAR(128) COLLATE "default";

DROP INDEX IF EXISTS file_backup_target_status_target_state_ext_idx;
CREATE INDEX IF NOT EXISTS file_backup_target_status_target_state_ext_idx
    ON file_backup_target_status (target, state, "externalID");
