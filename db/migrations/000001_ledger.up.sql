-- Ledger: file-backup-service's own database (own schema/migrations).
-- Rebuildable via reconciliation; added to the DB backup rotation. specs/008 FR-018.

-- "externalID" is byte-ordered (COLLATE "C") on BOTH tables. The audit target→ledger diff
-- (domain.mergeInventory) lock-steps the ledger's paged externalIDs against a manifest, comparing
-- them with Go's byte-order `<`; the manifest is itself written from these tables ORDER BY
-- "externalID". Under a locale collation (e.g. en_US.UTF-8) the DB order would NOT match Go's byte
-- order, so the merge would mis-count drift AND manifestIterator would reject the manifest as
-- "not strictly ascending". C collation makes DB order == byte order, and — since the keyset range
-- predicate (`"externalID" > after`) and the covering index below inherit the column collation —
-- keeps keyset pagination correct and index-backed.
CREATE TABLE file_backup_object (
    "externalID"        VARCHAR(128) COLLATE "C" PRIMARY KEY,  -- content hash (byte-ordered)
    size                BIGINT       NOT NULL,
    "firstSeenAt"       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    "createdBy"         UUID,                                  -- breadcrumb (outbox createdBy)
    "sourceCreatedDate" TIMESTAMPTZ                            -- breadcrumb (outbox createdDate)
);

CREATE TABLE file_backup_target_status (
    "externalID"  VARCHAR(128) COLLATE "C" NOT NULL REFERENCES file_backup_object("externalID"),
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
