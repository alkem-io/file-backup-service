package domain

import (
	"crypto/sha3"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
)

// Sum returns the lowercase-hex SHA3-256 of r — the file-service externalID
// scheme (FIPS 202), which is the object's identity, key, and verifier.
func Sum(r io.Reader) (string, error) {
	h := sha3.New256()
	if _, err := io.Copy(h, r); err != nil {
		return "", fmt.Errorf("hash: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Verify reports whether r hashes to want.
func Verify(want string, r io.Reader) (bool, error) {
	got, err := Sum(r)
	if err != nil {
		return false, err
	}
	return got == want, nil
}

// VerifyReader streams bytes through while hashing them; at EOF it returns an
// error if the accumulated SHA3-256 does not match want — so a downstream writer
// (a Sink) sees the error mid-stream and never commits corrupt or wrong-hash
// data. Total is the plaintext byte count seen so far. This makes integrity a
// property of the stream, with no whole-object buffering.
type VerifyReader struct {
	r     io.Reader
	h     hash.Hash
	want  string
	Total int64
}

// NewVerifyReader wraps r, verifying against want.
func NewVerifyReader(r io.Reader, want string) *VerifyReader {
	return &VerifyReader{r: r, h: sha3.New256(), want: want}
}

// Read implements io.Reader.
func (v *VerifyReader) Read(p []byte) (int, error) {
	n, err := v.r.Read(p)
	if n > 0 {
		_, _ = v.h.Write(p[:n])
		v.Total += int64(n)
	}
	if err == io.EOF {
		if got := hex.EncodeToString(v.h.Sum(nil)); got != v.want {
			return n, fmt.Errorf("integrity: stream hash %s != %s", got, v.want)
		}
	}
	return n, err
}
