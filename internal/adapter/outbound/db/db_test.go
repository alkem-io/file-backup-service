package db

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestNullTime(t *testing.T) {
	now := time.Now()
	if got := nullTime(pgtype.Timestamptz{Time: now, Valid: true}); !got.Equal(now) {
		t.Fatalf("valid timestamptz must map to its time, got %v", got)
	}
	if got := nullTime(pgtype.Timestamptz{}); !got.IsZero() {
		t.Fatalf("a NULL timestamptz must map to the zero time, got %v", got)
	}
}

func TestSliceToSet(t *testing.T) {
	set := sliceToSet([]string{"a", "b", "a"})
	if len(set) != 2 || !set["a"] || !set["b"] {
		t.Fatalf("want {a,b}, got %v", set)
	}
	if got := sliceToSet(nil); got == nil || len(got) != 0 {
		t.Fatalf("nil input must yield a non-nil empty set, got %v", got)
	}
}
