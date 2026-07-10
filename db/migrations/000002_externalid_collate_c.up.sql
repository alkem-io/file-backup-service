-- Re-collate both "externalID" columns to "C" (byte order) and rebuild the covering keyset index
-- under that collation. This is a FORWARD migration (NOT an edit to the already-applied 000001):
-- golang-migrate no-ops an edit to an applied migration, so an existing database (dev / CI / the
-- sandbox) would keep the default collation and the COLLATE "C" queries could not use the covering
-- index (seq-scan + sort per keyset page). Applying it here re-collates the live columns.
--
-- Why "C": the audit target→ledger diff (domain.mergeInventory) lock-steps the ledger's paged
-- externalIDs against a target's manifest using Go's byte-order `<`, and manifestIterator enforces
-- strictly-ascending BYTE order — while the manifest is written from these tables ORDER BY
-- "externalID". Under a locale collation (e.g. en_US.UTF-8) the DB order would not match Go's byte
-- order, so the merge would mis-count drift AND a valid manifest could be rejected as non-ascending.
-- "C" makes DB order == byte order; since the keyset range predicate (`"externalID" > $after`) and
-- the covering index inherit the column collation, keyset pagination stays correct and index-backed.
--
-- ALTER COLUMN TYPE with the same base type only changes the collation (and rewrites the column +
-- rebuilds its dependent indexes). FK equality is collation-independent, so re-collating both sides
-- of the file_backup_target_status -> file_backup_object FK is safe.

ALTER TABLE file_backup_object
    ALTER COLUMN "externalID" TYPE VARCHAR(128) COLLATE "C";

ALTER TABLE file_backup_target_status
    ALTER COLUMN "externalID" TYPE VARCHAR(128) COLLATE "C";

-- Rebuild the covering keyset index so its "externalID" sorts byte-order (idempotent).
DROP INDEX IF EXISTS file_backup_target_status_target_state_ext_idx;
CREATE INDEX IF NOT EXISTS file_backup_target_status_target_state_ext_idx
    ON file_backup_target_status (target, state, "externalID");
