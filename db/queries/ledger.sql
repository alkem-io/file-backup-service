-- The ledger tables are THIS service's own (db/migrations). Per constitution §IV all their
-- SQL is defined here and compiled by sqlc — the hand-written pgx exceptions were removed.
-- Every 'stored' literal below MUST equal domain.StateStored; it is a stable ledger-schema
-- value that changes only via a coordinated migration (grep 'stored' here finds each site).

-- name: RecordBackup :exec
-- The object row (FK parent) + every per-target status in ONE atomic data-modifying CTE:
-- the object is inserted first so the FK check on the status rows sees it (same statement).
-- The statuses arrive as a single jsonb array decoded via jsonb_to_recordset — a multi-arg
-- unnest(text[],text[],bigint[]) is what sqlc's analyzer can't type, so the whole statement
-- (and one-RTT atomicity) can now be compiled by sqlc instead of hand-written pgx. size is
-- corrected to a later VERIFIED value but never downgraded to unverified outbox hearsay; a
-- stored status stamps verifiedAt=now() and never regresses stored->failed.
WITH obj AS (
  INSERT INTO file_backup_object ("externalID", size, "createdBy", "sourceCreatedDate")
  VALUES (sqlc.arg(external_id), sqlc.arg(size), sqlc.arg(created_by), sqlc.arg(source_created_date))
  ON CONFLICT ("externalID") DO UPDATE
    SET size = CASE WHEN sqlc.arg(size_verified)::bool THEN EXCLUDED.size ELSE file_backup_object.size END
)
INSERT INTO file_backup_target_status ("externalID", target, state, "storedBytes", "verifiedAt")
SELECT sqlc.arg(external_id), t.target, t.state, t.bytes, CASE WHEN t.state = 'stored' THEN now() ELSE NULL END
FROM jsonb_to_recordset(sqlc.arg(statuses)::jsonb) AS t(target text, state text, bytes bigint)
ON CONFLICT ("externalID", target) DO UPDATE SET
  state = CASE WHEN file_backup_target_status.state = 'stored' AND EXCLUDED.state <> 'stored'
               THEN file_backup_target_status.state ELSE EXCLUDED.state END,
  "storedBytes" = CASE WHEN EXCLUDED.state = 'stored' THEN EXCLUDED."storedBytes"
                       ELSE file_backup_target_status."storedBytes" END,
  "verifiedAt" = CASE WHEN EXCLUDED.state = 'stored' THEN now()
                      ELSE file_backup_target_status."verifiedAt" END;

-- name: ListStoredTargets :many
-- The set of targets already in state='stored' for an object — the dedup source of truth.
SELECT target FROM file_backup_target_status
WHERE "externalID" = sqlc.arg(external_id) AND state = 'stored';

-- name: StoredCountByTarget :many
-- Count of objects currently stored per configured target — the restore-all completeness snapshot,
-- so an operator sees per-target disparity before trusting a single-source restore. Index-only on
-- (target, state). A target with no rows is simply absent from the result (the caller fills 0).
SELECT target, count(*)::bigint AS n
FROM file_backup_target_status
WHERE state = 'stored' AND target = ANY(sqlc.arg(targets)::text[])
GROUP BY target;

-- name: StoredObjectsPage :many
-- Objects stored ON one target (manifest/audit), keyset-paged by externalID; the
-- (target, state, "externalID") index makes the WHERE+ORDER an index-ordered range scan.
SELECT o."externalID", o.size, o."createdBy", o."sourceCreatedDate"
FROM file_backup_object o
JOIN file_backup_target_status ts ON ts."externalID" = o."externalID"
WHERE ts.target = sqlc.arg(target) AND ts.state = 'stored' AND o."externalID" > sqlc.arg(after)
ORDER BY o."externalID" LIMIT sqlc.arg(page_limit);

-- name: StoredExternalIDsPage :many
-- Just the externalIDs stored ON one target, keyset-paged by externalID — what the audit
-- sweep needs (it re-probes the sink and consumes ONLY the id). Unlike StoredObjectsPage this
-- does NOT join file_backup_object, so it is a covering INDEX-ONLY scan on
-- (target, state, "externalID") with no per-row heap fetch for size/createdBy the audit
-- discards. (StoredObjectsPage stays for the manifest export, which needs those columns.)
SELECT "externalID" FROM file_backup_target_status
WHERE target = sqlc.arg(target) AND state = 'stored' AND "externalID" > sqlc.arg(after)
ORDER BY "externalID" LIMIT sqlc.arg(page_limit);

-- name: TargetGapsPage :many
-- One keyset page (externalID order) of under-replicated objects with the CURRENT targets
-- that DO hold each. An object stored on all target_count targets is excluded (HAVING);
-- stale statuses for removed targets are filtered out (target = ANY(targets)).
SELECT o."externalID",
  COALESCE(array_agg(ts.target) FILTER (WHERE ts.state = 'stored' AND ts.target = ANY(sqlc.arg(targets)::text[])), '{}')::text[] AS stored
FROM file_backup_object o
LEFT JOIN file_backup_target_status ts ON ts."externalID" = o."externalID"
WHERE o."externalID" > sqlc.arg(after)
GROUP BY o."externalID"
HAVING count(*) FILTER (WHERE ts.state = 'stored' AND ts.target = ANY(sqlc.arg(targets)::text[])) < sqlc.arg(target_count)::int
ORDER BY o."externalID" LIMIT sqlc.arg(page_limit);

-- name: CoverageGaps :one
-- Objects NOT stored on every configured target = total objects MINUS fully-replicated
-- objects. The total is an EXACT count (a coverage backstop must not under-report), bounded
-- by the coarse sample cadence.
-- ::bigint on the result so sqlc emits int64 (count(*) is bigint); a bare subtraction was
-- inferred as int32, which would ERROR the scan past 2^31 objects rather than return the count.
SELECT (
  (SELECT count(*) FROM file_backup_object)
  - (SELECT count(*) FROM (
      SELECT "externalID" FROM file_backup_target_status
      WHERE state = 'stored' AND target = ANY(sqlc.arg(targets)::text[])
      GROUP BY "externalID" HAVING count(*) >= sqlc.arg(target_count)::int
    ) fully))::bigint AS gaps;

-- name: LastVerifiedAge :one
-- The RPO signal over the CONFIGURED targets: never_verified = count of targets that have
-- verified NOTHING, stalest_age_sec = age of the stalest target that HAS verified. Probing
-- each target via unnest counts a from-inception-empty target as never-verified rather than
-- dropping it from a GROUP BY. COALESCE(...,0) keeps the column NON-NULL (0 at bootstrap when
-- nothing has verified anywhere) so it scans into float64; the caller derives "nothing yet"
-- from never_verified == number of targets, not from a nullable age.
SELECT
  count(*) FILTER (WHERE mv IS NULL) AS never_verified,
  COALESCE(EXTRACT(EPOCH FROM max(now() - mv)), 0)::float8 AS stalest_age_sec
FROM (
  SELECT (SELECT max("verifiedAt") FROM file_backup_target_status
          WHERE target = t AND state = 'stored') AS mv
  FROM unnest(sqlc.arg(targets)::text[]) AS t
) per_target;

-- name: Probe :one
-- Both ledger tables exist + are readable via the pool's role: a skipped migration makes
-- the query ERROR (relation does not exist); an EMPTY table is success. EXISTS returns a
-- NON-NULL boolean (false on empty) so the row scans cleanly — a bare (SELECT 1 ... LIMIT 1)
-- would yield NULL on an empty table and fail a non-null scan.
SELECT EXISTS(SELECT 1 FROM file_backup_object) AS obj,
       EXISTS(SELECT 1 FROM file_backup_target_status) AS status;
