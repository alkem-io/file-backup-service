package domain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// manifestLine is one JSONL record of a target's ledger snapshot (FR-015) — enough to
// rebuild a target's object inventory standalone, without the ledger DB.
type manifestLine struct {
	ExternalID string `json:"externalID"`
	Size       int64  `json:"size"`
	CreatedBy  string `json:"createdBy,omitempty"`
	// *time.Time, not time.Time: omitempty is a no-op on a struct, so a zero time.Time
	// would serialize as a bogus "0001-01-01T00:00:00Z" that DR tooling reads as real.
	SourceCreatedDate *time.Time `json:"sourceCreatedDate,omitempty"`
}

// ManifestName is the object name for a snapshot taken at t: <UTC-timestamp>.jsonl. A
// full timestamp (not just the date) keeps each run's key unique so an object-lock/WORM
// target can't reject the write as an overwrite, and DR tooling picks the newest.
func ManifestName(t time.Time) string {
	return t.UTC().Format("2006-01-02T150405Z") + ".jsonl"
}

// WriteManifests writes each target's OWN inventory as a JSONL snapshot to its
// _manifest/<name> (FR-015), so any single target is restorable standalone without the
// ledger DB. Each target's snapshot lists only what that target holds. Targets run
// concurrently via RunParallel, which recovers a per-target panic (a driver panic in one
// target's manifest write must isolate to a best-effort DR snapshot, not crash serve).
func WriteManifests(ctx context.Context, led Ledger, targets []Target, name string) error {
	errs := RunParallel(targets,
		func(t Target) string { return "manifest to " + t.Sink.Name() },
		func(t Target) error {
			if err := writeManifest(ctx, led, t.Sink, name); err != nil {
				return fmt.Errorf("manifest to %s: %w", t.Sink.Name(), err)
			}
			return nil
		})
	return errors.Join(errs...)
}

func writeManifest(ctx context.Context, led Ledger, sink Sink, name string) error {
	rc := pipeThrough("manifest encode", func(w io.Writer) error {
		enc := json.NewEncoder(w) // JSONL: Encode appends a newline per record
		return eachStoredObject(ctx, led, sink.Name(), func(m ObjectMeta) error {
			var created *time.Time // nil (omitted) for a null/zero source date, not a bogus year-1
			if !m.SourceCreatedDate.IsZero() {
				created = &m.SourceCreatedDate
			}
			createdBy := "" // omitted when the breadcrumb is a NULL uuid
			if m.CreatedBy.Valid {
				createdBy = m.CreatedBy.UUID.String()
			}
			return enc.Encode(manifestLine{m.ExternalID, m.Size, createdBy, created})
		})
	})
	// callWithCtx so a filesystem PutManifest blocked in an uninterruptible io.Copy on a
	// WEDGED MOUNT is abandoned on ctx cancel — otherwise the manifest goroutine (and thus
	// serve's deferred bgWG.Wait at shutdown) hangs forever and the pod is SIGKILLed. The
	// backup path guards every sink write the same way (storeWithCtx).
	err := callWithCtx(ctx, func() error { return sink.PutManifest(ctx, name, rc) })
	// Unblock the encoder goroutine if PutManifest returned/was abandoned before draining
	// the pipe — otherwise it parks forever on the pipe write.
	_ = rc.Close()
	return err
}

// storedPageSize is the keyset page size for the manifest + audit sweeps.
const storedPageSize = 1000

// eachStoredObject pages through every object stored on target (Ledger.StoredObjectsPage)
// and invokes fn per object. Paging releases the ledger connection between pages, so a
// slow fn — a manifest's pipe write blocked on a slow upload — doesn't pin a pool
// connection for the whole sweep (the connection is held only for each fast page query).
func eachStoredObject(ctx context.Context, led Ledger, target string, fn func(ObjectMeta) error) error {
	return KeysetLoop("", storedPageSize,
		func(after string, limit int) ([]ObjectMeta, error) {
			return led.StoredObjectsPage(ctx, target, after, limit)
		},
		func(m ObjectMeta) string { return m.ExternalID },
		fn)
}
