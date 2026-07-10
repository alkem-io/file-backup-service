// Package fsutil holds small, stdlib-only filesystem + content-addressing
// helpers shared by the sink adapters and the domain restore path. It is a leaf
// package with zero project dependencies, so the domain importing it does not
// violate the hexagonal rule (it is not "infrastructure").
package fsutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	manifestPrefix     = "_manifest"
	manifestLatestName = "LATEST"
	manifestSuffix     = ".jsonl"
	preflightPrefix    = "_preflight"
	// manifestTimeLayout is the fixed-width UTC-nanosecond prefix ManifestName stamps onto every
	// snapshot; IsTimestampedManifest PARSES it (not just the suffix) so a stray non-timestamped
	// `.jsonl` can't sort above real names and be picked as "newest".
	manifestTimeLayout = "2006-01-02T150405.000000000Z"
	// contentHashLen is the hex length of a SHA3-256 externalID (32 bytes -> 64 hex chars).
	contentHashLen = 64
)

// ValidateContentHash rejects an externalID that is not exactly 64 lowercase-hex
// characters (a SHA3-256 digest). It is the ONE gate that stops an untrusted or
// schema-drifted externalID — from the file-service outbox, or an operator's --hash
// argument — from becoming a directory-traversing filesystem path (a "../" resolves
// through filepath.Join and escapes the sink root / restore destDir) or an over-length
// ledger key (the file_backup_object / _target_status columns are VARCHAR(128)). Enforced
// at every ingress: the backup pipeline, restore/verify, and the filesystem sink.
func ValidateContentHash(s string) error {
	if len(s) != contentHashLen {
		return fmt.Errorf("invalid content hash: %d chars, want %d lowercase-hex", len(s), contentHashLen)
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return fmt.Errorf("invalid content hash: byte %d is not lowercase hex", i)
		}
	}
	return nil
}

// PreflightKey returns a unique reserved key for a startup write-probe object. It is
// unique per run so an object-lock/WORM bucket can't reject an overwrite on restart.
func PreflightKey() string {
	var b [6]byte
	_, _ = rand.Read(b[:]) //nolint:errcheck // rand.Read never fails; the value only needs to be unique-ish
	return path.Join(preflightPrefix, fmt.Sprintf("%d-%s", time.Now().UTC().UnixNano(), hex.EncodeToString(b[:])))
}

// ShardKey returns the two-level hex-sharded key for a content hash —
// <hash[0:2]>/<hash[2:4]>/<hash> — joined with "/" (S3/URL style; filesystem
// callers convert with filepath.FromSlash). Owning this layout in ONE place
// keeps the sinks and the restore path from diverging, which would silently
// break restore (the key is re-derived from the hash).
func ShardKey(hash string) string {
	if len(hash) >= 4 {
		return path.Join(hash[0:2], hash[2:4], hash)
	}
	return hash
}

// CreateTemp opens a unique "<prefix>.*.partial" temp under dir. The .partial
// suffix is the marker any orphan sweep / reconcile keys on.
func CreateTemp(dir, prefix string) (*os.File, string, error) {
	f, err := os.CreateTemp(dir, prefix+".*.partial")
	if err != nil {
		return nil, "", fmt.Errorf("create temp: %w", err)
	}
	return f, f.Name(), nil
}

// ProbeWritable verifies dir is writable by creating then removing a throwaway ".partial"
// temp — the write-probe shared by the filesystem sink's Preflight and the reconcile
// scratch-dir check, so both use ONE policy and the same sweepable temp naming (a probe that
// leaks after a crash is caught by the same orphan sweep as any other .partial). dir="" uses
// the OS temp dir.
func ProbeWritable(dir string) error {
	f, name, err := CreateTemp(dir, preflightPrefix)
	if err != nil {
		return err
	}
	_ = f.Close()
	_ = os.Remove(name)
	return nil
}

// CommitWrite is the durable-write spine shared by the filesystem sink and the DR
// restore path, so the temp/fsync/ctx-gate/commit policy can't drift between backup
// and restore. It: MkdirAll(dir,0755) -> temp -> fill(temp) -> fsync -> close ->
// ctx-cancel gate -> chmod(mode)+rename+dir-fsync, removing the temp on any failure
// or cancellation. fill writes the object body into the temp (an io.Copy, a decode).
func CommitWrite(ctx context.Context, dir, base string, fill func(*os.File) error) error {
	// 0755 (not 0750): blobs are chmod'd 0644 so a different-uid restore/reconcile/
	// file-service can read them, which needs world-execute to traverse the shard dirs.
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:gosec // content-addressed store, readable by the restore uid
		return fmt.Errorf("mkdir %s: %w", dir, err)
	}
	f, tmp, err := CreateTemp(dir, base)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = f.Close() // idempotent after an explicit Close below
			_ = os.Remove(tmp)
		}
	}()
	if err := fill(f); err != nil {
		return fmt.Errorf("write temp: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err // cancelled mid-write — don't commit a late/orphaned object
	}
	committed = true // commitFile owns the temp from here (removes it on its own error)
	return commitFile(tmp, filepath.Join(dir, base))
}

// commitFile durably publishes an already-written+closed temp to dest: chmod 0644,
// atomic rename, and a parent-dir fsync (atomic != durable). The temp is removed on
// any error so a failure never leaks it. Callers gate on ctx BEFORE calling this so
// a cancelled write is never committed. 0644 (so a different-uid restore/reconcile/
// file-service can read the blob) is fixed by design at both call sites — a constant, not
// a threaded parameter.
func commitFile(tmpName, dest string) error {
	if err := os.Chmod(tmpName, 0o644); err != nil { //nolint:gosec // content-addressed blob, must be readable by the restore uid
		_ = os.Remove(tmpName)
		return fmt.Errorf("chmod: %w", err)
	}
	if err := os.Rename(tmpName, dest); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("rename: %w", err)
	}
	return syncDir(filepath.Dir(dest))
}

// ManifestKey returns the slash-style key for a ledger-snapshot manifest object
// (`_manifest/<base>`). Owning this layout in ONE place keeps the sinks and the
// DR-restore tooling from diverging on where manifests live.
func ManifestKey(name string) string {
	return path.Join(manifestPrefix, path.Base(name))
}

// ManifestDir is the reserved manifest subdirectory/prefix ("_manifest"), so the audit
// target→ledger inventory reader lists a target's manifests in the SAME place PutManifest
// (via ManifestKey) writes them — one owner, so the write + enumerate paths can't diverge.
func ManifestDir() string { return manifestPrefix }

// ManifestLatestKey is the fixed pointer object (`_manifest/LATEST`) naming the newest manifest, so
// a reader single-GETs it instead of listing the whole prefix. PutManifest overwrites it each pass
// (a new object VERSION on a versioned/object-lock bucket, so it's WORM-safe).
func ManifestLatestKey() string { return path.Join(manifestPrefix, manifestLatestName) }

// IsTimestampedManifest reports whether base is a genuine timestamped manifest object: it must both
// carry the `.jsonl` suffix AND parse as the fixed-width UTC-nanosecond ManifestName layout. Merely
// checking the suffix let a stray `backup.jsonl` (which sorts ABOVE every real `2026-…Z.jsonl` name)
// be picked as "newest" and diffed as the inventory — validating the timestamp layout rejects it.
// The ONE naming rule shared by the s3 + filesystem sinks + selectLatestManifest, so they can't
// diverge on which object is "the newest manifest".
func IsTimestampedManifest(base string) bool {
	ts, ok := strings.CutSuffix(base, manifestSuffix)
	if !ok {
		return false
	}
	_, err := time.Parse(manifestTimeLayout, ts)
	return err == nil
}

// FormatManifestName is the object BASE name for a snapshot taken at t: <UTC-nanosecond-timestamp>.jsonl.
// It is the WRITE side of the manifest naming contract whose READ side is IsTimestampedManifest — both
// live here so the fixed-width layout + suffix have ONE owner and can't drift. (A format change here not
// mirrored in the parser would make IsTimestampedManifest silently reject EVERY newly-written manifest →
// OpenLatestManifest drops them all → the inventory drift-check goes blind on green health.)
// domain.ManifestName delegates here.
func FormatManifestName(t time.Time) string {
	return t.UTC().Format(manifestTimeLayout) + manifestSuffix
}

// ParseManifestPointer parses the raw bytes of the `_manifest/LATEST` pointer into a manifest base name,
// returning ok=false when the pointer is empty/whitespace-only. The ONE owner of the pointer's on-disk/
// on-object encoding, shared by the s3 and filesystem sinks' readManifestPointer (only the byte READ —
// GetObject vs os.ReadFile — is storage-specific), so the two sinks can't diverge on which pointer
// values are valid. The returned name is VALIDATED (as a timestamped manifest) by OpenLatestManifest.
func ParseManifestPointer(b []byte) (string, bool) {
	name := strings.TrimSpace(string(b))
	return name, name != ""
}

// OpenLatestManifest opens the newest manifest that STILL EXISTS — the SINGLE owner of manifest
// selection, so the s3 and filesystem sinks provide only primitive reads (readPointer/listFrom/open)
// and can't diverge on which object is "newest". readPointer returns the `_manifest/LATEST` hint
// (ok=false when absent/unreadable/empty); listFrom lists manifest base names strictly AFTER its arg
// (""=full scan); open opens a manifest by base name, signalling a since-DELETED manifest as a wrapped
// os.ErrNotExist (any OTHER error — a read-deny, a gone container — is surfaced so the caller reports
// the target Unverifiable, not "no manifest"). It returns the opened newest surviving manifest; a
// wrapped os.ErrNotExist when NONE exist (the caller maps that to NoData); or a non-ErrNotExist
// open/list error.
//
// The healthy case is ONE bounded list + ONE open. Only when the selected newest is GONE (a
// deleted/expired pointer target, or a since-deleted tip) does it pay a full scan for the newest
// SURVIVING manifest — so a vanished tip can't hide an orphan an OLDER surviving manifest would still
// reveal (returning the gone tip would make the caller misread NoData).
func OpenLatestManifest(
	readPointer func() (string, bool),
	listFrom func(after string) ([]string, error),
	open func(name string) (io.ReadCloser, error),
) (io.ReadCloser, error) {
	// Fast path: a VALID pointer is a HINT (its write is best-effort, so it can be STALE — name an
	// OLDER manifest). Bound the staleness check to manifests strictly NEWER than it (a cheap
	// `StartAfter=<pointer>` list, since the pointer is rewritten each snapshot); a newer listed
	// manifest overrides the stale pointer, so the diff can't miss an orphan added after it. On the
	// selected tip being GONE, fall through to the single full scan below (the fast path did only the
	// bounded list, so an invalid/absent pointer skips straight to that one full scan — never two).
	if pointer, ok := readPointer(); ok && IsTimestampedManifest(pointer) {
		names, err := listFrom(pointer)
		if err != nil {
			return nil, err
		}
		newest := pointer // the pointer is a candidate; a newer listed name overrides it below
		for _, n := range names {
			if IsTimestampedManifest(n) && n > newest {
				newest = n
			}
		}
		rc, err := open(newest)
		if err == nil {
			return rc, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err // read-deny / gone container → Unverifiable, not a benign "no manifest"
		}
		// the fast-path tip is GONE → fall through to a single full scan for the newest surviving.
	}
	// No valid pointer, OR the fast-path tip is gone → ONE full scan, newest-surviving first, so a
	// deleted tip can't hide an orphan an OLDER manifest still reveals. latestFirst tries newest-down.
	names, err := listFrom("")
	if err != nil {
		return nil, err
	}
	for _, n := range latestFirst(names) {
		rc, err := open(n)
		if err == nil {
			return rc, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("no manifest: %w", os.ErrNotExist)
}

// latestFirst returns the VALID timestamped manifest base names in descending (newest-first) order,
// dropping any non-timestamped stray so it can never be picked as "newest". The fixed-width UTC
// ManifestName layout makes lexical order == chronological order, so a reverse string sort is
// newest-first.
func latestFirst(names []string) []string {
	valid := make([]string, 0, len(names))
	for _, n := range names {
		if IsTimestampedManifest(n) {
			valid = append(valid, n)
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(valid)))
	return valid
}

// syncDir fsyncs a directory so a create/rename within it is durable
// (atomic != durable). Used after every content write.
func syncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // caller-provided path under a configured root
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}
