package producer

import (
	"context"
	"errors"
)

var ErrOverloaded = errors.New("producer admission capacity exhausted")

// Header represents one Kafka record header.
type Header struct {
	Key   string
	Value []byte
}

// Record is the service-internal representation of a Kafka record.
//
// Key, Value, and Header.Value are borrowed by producer implementations. Callers
// must treat these byte slices as immutable after calling Produce. This lets the
// HTTP layer hand decoded request bytes directly to the Kafka accumulator without
// a second defensive copy on the hot path.
type Record struct {
	Topic     string
	Key       []byte
	Value     []byte
	Headers   []Header
	Partition *int32
}

// SizeBytes returns the approximate payload bytes held by this record.
func (r Record) SizeBytes() int64 {
	n := len(r.Key) + len(r.Value)
	for _, h := range r.Headers {
		n += len(h.Key) + len(h.Value)
	}
	return int64(n)
}

// Result is the per-record produce outcome returned to the HTTP layer.
type Result struct {
	Partition int32
	Offset    int64
	ErrorCode *int
	Err       error
}

// Producer produces batches of records and returns one Result per input record,
// preserving the original record ordering.
type Producer interface {
	Produce(ctx context.Context, records []Record) ([]Result, error)
	Ping(ctx context.Context) error
	Close()
}
