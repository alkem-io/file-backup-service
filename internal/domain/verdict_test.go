package domain

import (
	"context"
	"testing"
)

// TestVerdictZeroValueFailsClosed (re-review A): the zero-value TargetVerdict (StatusUnknown) must
// NEVER read as a pass — an unpopulated verdict fails closed, so a probe that never wrote its slot
// can't let a target silently pass.
func TestVerdictZeroValueFailsClosed(t *testing.T) {
	var v TargetVerdict
	if v.Status != StatusUnknown || !v.Failed() {
		t.Fatalf("the zero-value verdict must fail closed (StatusUnknown, Failed), got %+v failed=%v", v, v.Failed())
	}
	// The policy on the enum: Unknown fails regardless of worm.
	if !StatusUnknown.Failed(true) || !StatusUnknown.Failed(false) {
		t.Fatal("StatusUnknown must fail closed for both worm and non-worm")
	}
}

// TestProbeTargetsProbePanicIsFault (re-review A): a panic in the probe closure is recovered → a
// failing Fault (fail-loud), never a silent pass, and is stamped with the target name.
func TestProbeTargetsProbePanicIsFault(t *testing.T) {
	out := probeTargets(context.Background(), []Target{{Sink: stubSink{name: "t"}}}, 0,
		func(context.Context, Target) TargetVerdict { panic("boom") })
	if v := out[0]; v.Status != StatusFault || !v.Failed() || v.Target != "t" {
		t.Fatalf("a probe panic must be a failing Fault stamped with the target, got %+v", v)
	}
}

// TestProbeTargetsNilSinkIsFault (re-review A): a nil-Sink deref in the probe is recovered → a
// failing Fault, and the recover handler + verdict stamping must NOT themselves panic on the nil sink
// (safeSinkName gives "<nil-sink>").
func TestProbeTargetsNilSinkIsFault(t *testing.T) {
	out := probeTargets(context.Background(), []Target{{Sink: nil}}, 0,
		func(_ context.Context, tt Target) TargetVerdict {
			return TargetVerdict{Status: StatusVerified, Detail: tt.Sink.Name()}
		})
	if v := out[0]; v.Status != StatusFault || !v.Failed() || v.Target != "<nil-sink>" {
		t.Fatalf("a nil-sink probe panic must be a failing Fault with a safe name, got %+v", v)
	}
}
