package domain

// Metrics receives backup-pipeline observations. Use Nop when metrics are off.
type Metrics interface {
	// ObjectStored records a successful store of storedBytes to target.
	ObjectStored(target string, storedBytes int64)
	// ObjectFailed records a failed store to target.
	ObjectFailed(target string)
	// ObjectDedup records an object already present on target.
	ObjectDedup(target string)
}

// Nop is a no-op Metrics.
type Nop struct{}

// ObjectStored implements Metrics.
func (Nop) ObjectStored(string, int64) {}

// ObjectFailed implements Metrics.
func (Nop) ObjectFailed(string) {}

// ObjectDedup implements Metrics.
func (Nop) ObjectDedup(string) {}
