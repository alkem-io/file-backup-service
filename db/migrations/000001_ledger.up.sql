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

-- The RPO sampler reads max("verifiedAt") every 15s; without this it's a full-table
-- scan that degrades linearly with the corpus.
CREATE INDEX file_backup_target_status_verified_idx
    ON file_backup_target_status ("verifiedAt" DESC);

-- Per-target manifest / audit stream the objects stored on one target.
CREATE INDEX file_backup_target_status_target_state_idx
    ON file_backup_target_status (target, state);
