package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alkem-io/file-backup-service/internal/domain"
)

type lifecycleFileReferences struct {
	referenced bool
	err        error
	hash       string
}

func (r *lifecycleFileReferences) HasReferences(_ context.Context, hash string) (bool, error) {
	r.hash = hash
	return r.referenced, r.err
}

type lifecycleLedger struct {
	fakeLedger
	recorded, forgotten int
}

func (l *lifecycleLedger) RecordBackup(context.Context, domain.ObjectMeta, []domain.TargetStatus) error {
	l.recorded++
	return nil
}

func (l *lifecycleLedger) ForgetUnstored(context.Context, string) (bool, error) {
	l.forgotten++
	return true, nil
}

func TestConsumerSourceGoneLifecycle(t *testing.T) {
	cases := []struct {
		name       string
		referenced bool
		readErr    error
		action     string
	}{
		{name: "deleted", action: "skip"},
		{name: "shared-reference-survives", referenced: true, action: "fail"},
		{name: "reference-database-unavailable", readErr: errors.New("database unavailable"), action: "fail"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			refs := &lifecycleFileReferences{referenced: tc.referenced, err: tc.readErr}
			ledger := &lifecycleLedger{}
			targets := []domain.Target{{Sink: newMemSink("offsite")}}
			pipeline := domain.NewPipeline(fakeSource{err: domain.ErrSourceGone}, ledger, targets).
				WithSourceLifecycle(domain.NewSourceLifecycle(refs, ledger, targets))
			outbox := &recordingOutbox{}
			c := New(Deps{Outbox: outbox, Pipeline: pipeline, PerObjectTimeout: time.Second})
			h := validHash(tc.name)
			c.process(context.Background(), domain.OutboxEntry{ID: 7, BackupItem: domain.BackupItem{ExternalID: h}})
			if outbox.action != tc.action || refs.hash != h {
				t.Fatalf("action=%q want=%q checked hash=%q want=%q", outbox.action, tc.action, refs.hash, h)
			}
			if tc.referenced && (ledger.recorded != 1 || ledger.forgotten != 0) {
				t.Fatalf("referenced source must be recorded failed, never forgotten: %+v", ledger)
			}
			if tc.readErr != nil && (ledger.recorded != 0 || ledger.forgotten != 0) {
				t.Fatalf("reference lookup failure must not mutate the ledger: %+v", ledger)
			}
			if tc.action == "skip" && ledger.forgotten != 1 {
				t.Fatalf("deleted source must clean failed-only metadata: %+v", ledger)
			}
		})
	}
}
