package domain

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
	after := start
	for {
		page, err := pageFn(after, pageSize)
		if err != nil {
			return err
		}
		for i := range page {
			if err := fn(page[i]); err != nil {
				return err
			}
		}
		if len(page) < pageSize {
			return nil // a short page is the last
		}
		after = cursorOf(page[len(page)-1])
	}
}
