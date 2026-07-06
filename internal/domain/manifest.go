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
	ExternalID        string    `json:"externalID"`
	Size              int64     `json:"size"`
	CreatedBy         string    `json:"createdBy,omitempty"`
	SourceCreatedDate time.Time `json:"sourceCreatedDate,omitempty"`
}

// ManifestName is the object name for a snapshot taken at t: <UTC-timestamp>.jsonl. A
// full timestamp (not just the date) keeps each run's key unique so an object-lock/WORM
// target can't reject the write as an overwrite, and DR tooling picks the newest.
func ManifestName(t time.Time) string {
	return t.UTC().Format("2006-01-02T150405Z") + ".jsonl"
}

// WriteManifests streams the ledger as a JSONL snapshot to every target's
// _manifest/<name> (FR-015), so any single target is restorable standalone without the
// ledger DB. A per-target failure is isolated (the others still get their snapshot).
func WriteManifests(ctx context.Context, led Ledger, targets []Target, name string) error {
	var errs []error
	for _, t := range targets {
		if err := writeManifest(ctx, led, t.Sink, name); err != nil {
			errs = append(errs, fmt.Errorf("manifest to %s: %w", t.Sink.Name(), err))
		}
	}
	return errors.Join(errs...)
}

func writeManifest(ctx context.Context, led Ledger, sink Sink, name string) error {
	pr, pw := io.Pipe()
	go func() {
		enc := json.NewEncoder(pw) // JSONL: Encode appends a newline per record
		err := led.EachObject(ctx, func(m ObjectMeta) error {
			return enc.Encode(manifestLine{m.ExternalID, m.Size, m.CreatedBy, m.SourceCreatedDate})
		})
		_ = pw.CloseWithError(err)
	}()
	return sink.PutManifest(ctx, name, pr)
}
