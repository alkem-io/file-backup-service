-- Ledger: file-backup-service's own database (own schema/migrations).
-- Rebuildable via reconciliation; added to the DB backup rotation. specs/008 FR-018.

CREATE TABLE file_backup_object (
    "externalID"        VARCHAR(128) PRIMARY KEY,          -- content hash
    size                BIGINT       NOT NULL,
    "firstSeenAt"       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    "createdBy"         UUID,                              -- breadcrumb (outbox createdBy)
    "sourceCreatedDate" TIMESTAMPTZ                        -- breadcrumb (outbox createdDate)
);

CREATE TABLE file_backup_target_status (
    "externalID"  VARCHAR(128) NOT NULL REFERENCES file_backup_object("externalID"),
    target        VARCHAR(64)  NOT NULL,
    state         VARCHAR(16)  NOT NULL,                   -- stored | failed
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
