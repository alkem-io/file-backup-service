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
