package limits

import "sync"

// Admission is a small non-blocking, byte-and-record admission controller.
// It intentionally rejects overload rather than buffering unbounded work in
// front of the Kafka producer.
type Admission struct {
	mu sync.Mutex

	maxRecords int64
	maxBytes   int64

	usedRecords int64
	usedBytes   int64
}

func NewAdmission(maxRecords, maxBytes int64) *Admission {
	return &Admission{
		maxRecords: maxRecords,
		maxBytes:   maxBytes,
	}
}

func (a *Admission) TryAcquire(records, bytes int64) bool {
	if a == nil {
		return true
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if records < 0 || bytes < 0 {
		return false
	}
	if a.maxRecords > 0 && a.usedRecords+records > a.maxRecords {
		return false
	}
	if a.maxBytes > 0 && a.usedBytes+bytes > a.maxBytes {
		return false
	}

	a.usedRecords += records
	a.usedBytes += bytes
	return true
}

func (a *Admission) Release(records, bytes int64) {
	if a == nil {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	a.usedRecords -= records
	if a.usedRecords < 0 {
		a.usedRecords = 0
	}
	a.usedBytes -= bytes
	if a.usedBytes < 0 {
		a.usedBytes = 0
	}
}

func (a *Admission) Snapshot() (usedRecords, maxRecords, usedBytes, maxBytes int64) {
	if a == nil {
		return 0, 0, 0, 0
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	return a.usedRecords, a.maxRecords, a.usedBytes, a.maxBytes
}
