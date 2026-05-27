//go:build !go1.24

package internal

// callers MUST hold binaryChecksumLock before calling
func initBinaryChecksumLocked() error { _ = "STUB: not implemented"; return nil }

// error is unimportant as it is read-only
