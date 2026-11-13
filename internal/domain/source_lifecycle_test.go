package domain

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type lifecycleReferences struct {
	referenced bool
	err        error
}

type lifecycleFaultLedger struct {
	*fakeLedger
	recordErr, forgetErr error
}

func (f lifecycleFaultLedger) RecordBackup(ctx context.Context, obj ObjectMeta, statuses []TargetStatus) error {
	if f.recordErr != nil {
		return f.recordErr
	}
	return f.fakeLedger.RecordBackup(ctx, obj, statuses)
}

func (f lifecycleFaultLedger) ForgetUnstored(ctx context.Context, hash string) (bool, error) {
	if f.forgetErr != nil {
		return false, f.forgetErr
	}
	return f.fakeLedger.ForgetUnstored(ctx, hash)
}

func TestSourceLifecycleLedgerErrorsAreNotBenignSkips(t *testing.T) {
	cause := errors.New("ledger unavailable")
	for _, referenced := range []bool{false, true} {
		led := lifecycleFaultLedger{fakeLedger: newFakeLedger(), recordErr: cause, forgetErr: cause}
		s := NewSourceLifecycle(lifecycleReferences{referenced: referenced}, led, []Target{{Sink: newMemSink("offsite")}})
		retired, err := s.ResolveSourceGone(context.Background(), BackupItem{ExternalID: hashOf("ledger fault")})
		if retired || !errors.Is(err, cause) || errors.Is(err, ErrSourceGone) {
			t.Fatalf("ledger errors must retry, not skip: retired=%v err=%v", retired, err)
		}
	}
}

func TestBackfillClassifiesSourceGoneByReferences(t *testing.T) {
	for _, referenced := range []bool{false, true} {
		led := newFakeLedger()
		targets := []Target{{Sink: newMemSink("offsite")}}
		p := NewPipeline(goneSource{}, led, targets).
			WithSourceLifecycle(NewSourceLifecycle(lifecycleReferences{referenced: referenced}, led, targets))
		corpus := fakeCorpus{entries: []BackupItem{{ExternalID: hashOf("missing during backfill")}}}
		st, err := NewBackfiller(corpus, p, time.Second, 1).Run(context.Background(), 0)
		if err != nil || (st.Failed == 1) != referenced || (st.Skipped == 1) == referenced {
			t.Fatalf("backfill must distinguish deletion from missing referenced content: referenced=%v stats=%+v err=%v", referenced, st, err)
		}
	}
}

func TestReconcilePrimaryProbeErrorsRetainMetadata(t *testing.T) {
	ctx := context.Background()
	h := hashOf("primary unavailable")
	led := newFakeLedger()
	targets := []Target{{Sink: newMemSink("offsite")}}
	if err := led.RecordBackup(ctx, ObjectMeta{ExternalID: h}, []TargetStatus{{Target: "offsite", State: StateFailed}}); err != nil {
		t.Fatal(err)
	}
	rec := NewReconciler(led, targets, time.Millisecond, t.TempDir(), 0, nil, 1).
		WithSourceLifecycle(hangingSource{}, NewSourceLifecycle(lifecycleReferences{}, led, targets))
	st, err := rec.Run(ctx, 0)
	if err != nil || st.Failed != 1 || st.Retired != 0 || !led.objects[h] {
		t.Fatalf("a failed primary check must preserve metadata: stats=%+v err=%v objects=%v", st, err, led.objects)
	}
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	var cancelledStats ReconcileStats
	var mu sync.Mutex
	rec.retireOrSkip(ctx, ctx, BackupItem{ExternalID: h}, &cancelledStats, &mu)
	if cancelledStats.Cancelled != 1 || cancelledStats.Failed != 0 || !led.objects[h] {
		t.Fatalf("shutdown must preserve metadata without a false failure: %+v", cancelledStats)
	}
}

func (r lifecycleReferences) HasReferences(context.Context, string) (bool, error) {
	return r.referenced, r.err
}

func (f *fakeLedger) ForgetUnstored(_ context.Context, externalID string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for key, state := range f.states {
		if strings.HasPrefix(key, externalID+"/") && state == StateStored {
			return false, nil
		}
	}
	_, existed := f.objects[externalID]
	delete(f.objects, externalID)
	delete(f.sizes, externalID)
	for key := range f.states {
		if strings.HasPrefix(key, externalID+"/") {
			delete(f.states, key)
		}
	}
	return existed, nil
}

func TestSourceLifecycleRetiresOnlyUnreferencedFailedMetadata(t *testing.T) {
	for _, storedTarget := range []string{"", "offsite", "removed-target"} {
		t.Run("stored-on-"+storedTarget, func(t *testing.T) {
			ctx := context.Background()
			h := hashOf("deleted content")
			led := newFakeLedger()
			targets := []Target{{Sink: newMemSink("offsite")}}
			statuses := []TargetStatus{{Target: "offsite", State: StateFailed}}
			if storedTarget == "offsite" {
				statuses[0] = TargetStatus{Target: storedTarget, State: StateStored}
			} else if storedTarget != "" {
				statuses = append(statuses, TargetStatus{Target: storedTarget, State: StateStored})
			}
			if err := led.RecordBackup(ctx, ObjectMeta{ExternalID: h}, statuses); err != nil {
				t.Fatal(err)
			}
			retired, err := NewSourceLifecycle(lifecycleReferences{}, led, targets).ResolveSourceGone(ctx, BackupItem{ExternalID: h})
			if err != nil || retired != (storedTarget == "") {
				t.Fatalf("retired=%v err=%v", retired, err)
			}
			if led.objects[h] != (storedTarget != "") {
				t.Fatalf("metadata retained=%v for stored target %q", led.objects[h], storedTarget)
			}
		})
	}
}

func TestSourceLifecycleReferencedMissingSourceRemainsAlertable(t *testing.T) {
	ctx := context.Background()
	h := hashOf("still needed")
	led := newFakeLedger()
	targets := []Target{{Sink: newMemSink("a")}, {Sink: newMemSink("b")}}
	if err := led.RecordBackup(ctx, ObjectMeta{ExternalID: h, Size: 42}, []TargetStatus{{Target: "a", State: StateStored}}); err != nil {
		t.Fatal(err)
	}
	retired, err := NewSourceLifecycle(lifecycleReferences{referenced: true}, led, targets).ResolveSourceGone(ctx, BackupItem{ExternalID: h})
	if retired || !errors.Is(err, ErrReferencedSourceGone) || errors.Is(err, ErrSourceGone) {
		t.Fatalf("referenced missing source must fail, never skip: retired=%v err=%v", retired, err)
	}
	if led.states[h+"/a"] != StateStored || led.states[h+"/b"] != StateFailed || led.sizes[h] != 42 {
		t.Fatalf("stored copy must survive and missing copy remain alertable: %+v %+v", led.states, led.sizes)
	}
}

func TestPipelineFirstFetchMissingReferenceCreatesFailedMetadata(t *testing.T) {
	ctx := context.Background()
	h := hashOf("first attempt missing")
	led := newFakeLedger()
	targets := []Target{{Sink: newMemSink("offsite")}}
	p := NewPipeline(goneSource{}, led, targets).
		WithSourceLifecycle(NewSourceLifecycle(lifecycleReferences{referenced: true}, led, targets))
	_, _, err := p.BackupOne(ctx, BackupItem{ExternalID: h})
	if !errors.Is(err, ErrReferencedSourceGone) || !led.objects[h] || led.states[h+"/offsite"] != StateFailed {
		t.Fatalf("first source miss must leave visible failure: err=%v objects=%v states=%v", err, led.objects, led.states)
	}
	var gaps int
	if err := led.TargetGaps(ctx, []string{"offsite"}, func(string, map[string]bool) error { gaps++; return nil }); err != nil || gaps != 1 {
		t.Fatalf("missing source must remain in coverage worklist: gaps=%d err=%v", gaps, err)
	}
}

func TestSourceLifecycleLookupFailureCannotRetireMetadata(t *testing.T) {
	ctx := context.Background()
	h := hashOf("reference lookup unavailable")
	led := newFakeLedger()
	if err := led.RecordBackup(ctx, ObjectMeta{ExternalID: h}, []TargetStatus{{Target: "offsite", State: StateFailed}}); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("database unavailable")
	_, err := NewSourceLifecycle(lifecycleReferences{err: cause}, led, nil).ResolveSourceGone(ctx, BackupItem{ExternalID: h})
	if !errors.Is(err, cause) || !led.objects[h] {
		t.Fatalf("failed reference lookup must retain metadata: err=%v objects=%v", err, led.objects)
	}
}

func TestReconcileRetiresOrphanWithoutOutboxEntry(t *testing.T) {
	ctx := context.Background()
	h := hashOf("failed upload whose pending outbox entry was removed")
	led := newFakeLedger()
	targets := []Target{{Sink: newMemSink("offsite")}}
	if err := led.RecordBackup(ctx, ObjectMeta{ExternalID: h}, []TargetStatus{{Target: "offsite", State: StateFailed}}); err != nil {
		t.Fatal(err)
	}
	rec := NewReconciler(led, targets, time.Second, t.TempDir(), 0, nil, 1).
		WithSourceLifecycle(goneSource{}, NewSourceLifecycle(lifecycleReferences{}, led, targets))
	st, err := rec.Run(ctx, 0)
	if err != nil || st.Retired != 1 || st.Skipped != 0 || st.Failed != 0 || led.objects[h] {
		t.Fatalf("orphan cleanup must converge without an outbox retry: stats=%+v err=%v objects=%v", st, err, led.objects)
	}
	st, err = rec.Run(ctx, 0)
	if err != nil || st != (ReconcileStats{}) {
		t.Fatalf("repeat reconcile must be clean: stats=%+v err=%v", st, err)
	}
}

func TestReconcileKeepsReferencedOrPresentPrimary(t *testing.T) {
	for _, referenced := range []bool{false, true} {
		for _, present := range []bool{false, true} {
			t.Run(strings.Join([]string{map[bool]string{true: "referenced", false: "unreferenced"}[referenced], map[bool]string{true: "present", false: "gone"}[present]}, "-"), func(t *testing.T) {
				ctx := context.Background()
				data := []byte("protected source")
				h := hashOf(string(data))
				led := newFakeLedger()
				targets := []Target{{Sink: newMemSink("offsite")}}
				if err := led.RecordBackup(ctx, ObjectMeta{ExternalID: h}, []TargetStatus{{Target: "offsite", State: StateFailed}}); err != nil {
					t.Fatal(err)
				}
				var src Source = goneSource{}
				if present {
					src = fakeSource{data}
				}
				rec := NewReconciler(led, targets, time.Second, t.TempDir(), 0, nil, 1).
					WithSourceLifecycle(src, NewSourceLifecycle(lifecycleReferences{referenced: referenced}, led, targets))
				st, err := rec.Run(ctx, 0)
				if err != nil {
					t.Fatal(err)
				}
				wantRetired := !referenced && !present
				if (st.Retired == 1) != wantRetired || led.objects[h] == wantRetired {
					t.Fatalf("unexpected retirement: stats=%+v objects=%v", st, led.objects)
				}
				if referenced && !present && st.Failed != 1 {
					t.Fatalf("referenced missing source must fail: %+v", st)
				}
			})
		}
	}
}
