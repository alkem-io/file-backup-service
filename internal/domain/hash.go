package domain

import (
	"crypto/sha3"
	"encoding/hex"
	"fmt"
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
