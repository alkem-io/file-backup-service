package domain

import (
	"crypto/sha3"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
)

// newHash returns the content-hash function (FIPS 202 SHA3-256) — the ONE place
// the object-identity algorithm is chosen. Sum, VerifyReader, and the restore-side
// verify all route through it, so a digest change is a single edit and backup can
// never diverge from restore.
func newHash() hash.Hash { return sha3.New256() }

// hexSum renders a digest as lowercase hex (the externalID encoding).
func hexSum(h hash.Hash) string { return hex.EncodeToString(h.Sum(nil)) }

// Sum returns the lowercase-hex SHA3-256 of r — the file-service externalID
// scheme (FIPS 202), which is the object's identity, key, and verifier.
func sum(r io.Reader) (string, error) {
	h := newHash()
	if _, err := io.Copy(h, r); err != nil {
		return "", fmt.Errorf("hash: %w", err)
	}
	return hexSum(h), nil
}

// VerifyReader streams bytes through while hashing them; at end-of-stream it
// returns an error if the accumulated SHA3-256 does not match want — so a
// downstream writer (a Sink) sees the error mid-stream and never commits corrupt
// or wrong-hash data. Total is the plaintext byte count seen so far. This makes
// integrity a property of the stream, with no whole-object buffering.
type VerifyReader struct {
	r      io.Reader
	h      hash.Hash
	want   string
	maxLen int64 // 0 = unbounded; else fail mid-stream once Total exceeds it
	Total  int64
}

// NewVerifyReader wraps r, verifying against want and failing mid-stream if the source
// exceeds maxLen bytes (0 = unbounded). The cap is what makes the BACKUP path enforce the
// service's own restorability invariant: restore/verify/reconcile reject anything over
// maxObjectBytes, so an over-cap object must fail HERE (nothing committed) rather than be
// stored on every target and then be permanently unrestorable.
func NewVerifyReader(r io.Reader, want string, maxLen int64) *VerifyReader {
	return &VerifyReader{r: r, h: newHash(), want: want, maxLen: maxLen}
}

// Read implements io.Reader.
func (v *VerifyReader) Read(p []byte) (int, error) {
	n, err := v.r.Read(p)
	if n > 0 {
		_, _ = v.h.Write(p[:n])
		v.Total += int64(n)
	}
	if v.maxLen > 0 && v.Total > v.maxLen {
		return n, fmt.Errorf("integrity: object exceeds the %d-byte max (unrestorable if stored)", v.maxLen)
	}
	// Verify at ANY end-of-stream, not just a clean io.EOF: a truncated source (a
	// mid-stream connection drop with a known Content-Length) surfaces as
	// io.ErrUnexpectedEOF, which must fail the hash — otherwise the short bytes are
	// committed as a "verified" backup and only fail at DR-restore.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		if got := hexSum(v.h); got != v.want {
			return n, fmt.Errorf("integrity: stream hash %s != %s", got, v.want)
		}
	}
	return n, err
}
