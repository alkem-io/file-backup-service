-- Ledger: file-backup-service's own database (own schema/migrations).
-- Rebuildable via reconciliation; added to the DB backup rotation. specs/008 FR-018.
--
-- COLLATE "C" on both "externalID" columns is LOAD-BEARING, so the columns are BORN with it (not
-- re-collated by a later migration — a fresh table has no rows, so there is no blocking table
-- rewrite): the audit target→ledger diff (domain.mergeInventory) lock-steps the ledger's paged
-- externalIDs against a target's manifest using Go's byte-order `<`, and manifestIterator enforces
-- strictly-ascending BYTE order, while the manifest is written from these tables ORDER BY
-- "externalID" COLLATE "C". Under a locale collation (e.g. en_US.UTF-8) DB order would not match Go's
-- byte order, so the merge would mis-count drift AND a valid manifest could be rejected as
-- non-ascending. "C" makes DB order == byte order; the keyset range predicate (`"externalID" > $after`)
-- and the covering index inherit the column collation, so keyset pagination stays correct + index-backed.

CREATE TABLE file_backup_object (
    "externalID"        VARCHAR(128) COLLATE "C" PRIMARY KEY, -- content hash (byte-ordered — see header)
    size                BIGINT       NOT NULL,
    "firstSeenAt"       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    "createdBy"         UUID,                              -- breadcrumb (outbox createdBy)
    "sourceCreatedDate" TIMESTAMPTZ                        -- breadcrumb (outbox createdDate)
);

CREATE TABLE file_backup_target_status (
    "externalID"  VARCHAR(128) COLLATE "C" NOT NULL REFERENCES file_backup_object("externalID"), -- byte-ordered (see header)
    target        VARCHAR(64)  NOT NULL,
    state         VARCHAR(16)  NOT NULL CHECK (state IN ('stored','failed')), -- domain.State{Stored,Failed}; the CHECK stops a typo'd/mis-cased value that every query's state='stored' filter would silently make invisible (perpetual under-replication)
    "storedBytes" BIGINT,
    "verifiedAt"  TIMESTAMPTZ,
    PRIMARY KEY ("externalID", target)
);

-- The RPO sampler reads max("verifiedAt") PER TARGET every 15s; this serves the
-- per-target max as an index-only lookup (a single-column "verifiedAt" index can't
-- answer a per-target GROUP BY).
CREATE INDEX file_backup_target_status_target_verified_idx
    ON file_backup_target_status (target, "verifiedAt" DESC)
    WHERE state = 'stored';

-- Per-target manifest / audit keyset-page the objects stored on one target, ORDER BY
-- "externalID" — this covers the WHERE (target, state) AND the index-ordered range scan
-- (no sort, no full-cursor hold).
CREATE INDEX file_backup_target_status_target_state_ext_idx
    ON file_backup_target_status (target, state, "externalID");
