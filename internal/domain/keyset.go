package domain

// KeysetPageSize is the ONE page size for every connection-releasing keyset sweep — the db
// adapter's corpus/gap sweeps (backfill EachFile, reconcile TargetGaps) AND the domain's
// manifest + audit sweeps. Paged (not a held cursor) so a slow per-item consumer never pins a
// pool connection for the whole pass; 1000 balances round-trips against per-page memory.
const KeysetPageSize = 1000

// KeysetLoop drives a keyset-paginated sweep: fetch pages via pageFn(after, pageSize) until
// a SHORT page (the last), invoking fn per item; cursorOf extracts the next `after` from the
// last item of a full page. It is the ONE owner of the after-cursor + short-page-stops loop,
// so a hand-rolled copy can't mis-handle a short page and silently stop a sweep early.
//
// It lives in domain (a pure generic, no infrastructure deps) so BOTH the db adapters
// (backfill EachFile, reconcile TargetGaps — keyed on uuid/string) AND the domain manifest
// sweep (eachStoredObject) reuse it across the hexagonal boundary instead of each
// re-implementing the loop. Paging (not a held cursor) releases the DB connection between
// pages, so a slow per-item consumer never pins a pool connection for the whole pass.
func KeysetLoop[C any, T any](start C, pageSize int, pageFn func(after C, limit int) ([]T, error), cursorOf func(T) C, fn func(T) error) error {
	next := keysetPull(start, pageSize, pageFn, cursorOf)
	for {
		item, ok, err := next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := fn(item); err != nil {
			return err
		}
	}
}

// keysetPull is the PULL form of the same keyset sweep KeysetLoop drives in PUSH form: it returns a
// next() that yields one item at a time, fetching the next page (short page = last) via the SAME
// pageFn(after,pageSize) + after-cursor contract, so a pull consumer (the inventory merge, which
// lock-steps two streams) and a push consumer share ONE paging implementation and can't diverge on
// the short-page-stops boundary. Paging (not a held cursor) releases the DB connection between pages.
func keysetPull[C any, T any](start C, pageSize int, pageFn func(after C, limit int) ([]T, error), cursorOf func(T) C) func() (T, bool, error) {
	after := start
	var page []T
	i := 0
	done := false
	return func() (T, bool, error) {
		var zero T
		for i >= len(page) {
			if done {
				return zero, false, nil
			}
			p, err := pageFn(after, pageSize)
			if err != nil {
				return zero, false, err
			}
			if len(p) < pageSize {
				done = true // a short page is the last
			}
			if len(p) == 0 {
				return zero, false, nil
			}
			page, i, after = p, 0, cursorOf(p[len(p)-1])
		}
		item := page[i]
		i++
		return item, true, nil
	}
}
