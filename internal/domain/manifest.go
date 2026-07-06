package domain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
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
// ledger DB. Each target's snapshot lists only what that target holds. Per-target
// failure is isolated, and the targets are written concurrently (each is a distinct
// query + upload, so there's nothing to share).
func WriteManifests(ctx context.Context, led Ledger, targets []Target, name string) error {
	errs := make([]error, len(targets))
	var wg sync.WaitGroup
	for i, t := range targets {
		wg.Add(1)
		go func(i int, t Target) {
			defer wg.Done()
			if err := writeManifest(ctx, led, t.Sink, name); err != nil {
				errs[i] = fmt.Errorf("manifest to %s: %w", t.Sink.Name(), err)
			}
		}(i, t)
	}
	wg.Wait()
	return errors.Join(errs...)
}

func writeManifest(ctx context.Context, led Ledger, sink Sink, name string) error {
	pr, pw := io.Pipe()
	go func() {
		enc := json.NewEncoder(pw) // JSONL: Encode appends a newline per record
		err := led.EachStoredObject(ctx, sink.Name(), func(m ObjectMeta) error {
			var created *time.Time // nil (omitted) for a null/zero source date, not a bogus year-1
			if !m.SourceCreatedDate.IsZero() {
				created = &m.SourceCreatedDate
			}
			return enc.Encode(manifestLine{m.ExternalID, m.Size, m.CreatedBy, created})
		})
		_ = pw.CloseWithError(err)
	}()
	err := sink.PutManifest(ctx, name, pr)
	// Unblock the encoder goroutine if PutManifest returned before draining the pipe
	// (an upload error / timeout) — otherwise it parks forever on pw.Write, pinning the
	// ledger cursor's DB connection.
	_ = pr.CloseWithError(err)
	return err
}
