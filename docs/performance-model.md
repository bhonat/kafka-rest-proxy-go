# Producer performance model

This service follows the same broad shape as Confluent REST Proxy for producer
traffic:

1. The HTTP handler decodes a `POST /topics/{topic}` request.
2. Records are admitted through bounded local byte and record limits.
3. Records are handed to one long-lived Kafka producer client.
4. The Kafka producer accumulator batches records across concurrent HTTP
   requests.
5. Sender goroutines flush those batches to Kafka.
6. Per-record callbacks fill the original response positions, and the HTTP
   handler returns Confluent-style `offsets[]`.

The request is synchronous from the client's point of view because it waits for
real Kafka callbacks, but the Kafka path is asynchronous internally.

## Accumulator and sender behavior

The MVP uses `franz-go` as the producer engine. We deliberately do not add a
second application-level queue in front of it. Duplicating the Kafka producer
accumulator would increase memory pressure, complicate shutdown, and hide
backpressure from the HTTP layer.

Instead, the service configures franz-go's accumulator directly:

- `KAFKA_MAX_BUFFERED_RECORDS`
- `KAFKA_MAX_BUFFERED_BYTES`
- `KAFKA_BATCH_MAX_BYTES`
- `KAFKA_LINGER`
- `KAFKA_COMPRESSION`
- `KAFKA_REQUIRED_ACKS`
- `KAFKA_REQUEST_TIMEOUT`
- `KAFKA_DELIVERY_TIMEOUT`

The most important tuning knob observed so far is linger. Because this proxy
returns the HTTP response only after Kafka callbacks complete, linger directly
adds to client-visible latency. The default is therefore `0ms`, which matches
REST-style request/response traffic better and produced the best local
records-per-node estimate in the focused comparison.

- `0ms` is the default low-latency profile and should be the first production
  candidate.
- `5ms` is a throughput-biased profile only when callers tolerate added
  response latency and benchmarks prove fewer nodes for the actual workload.
- `1ms` has not been a good local setting in the current benchmark harness.

## Memory ownership

The hot path avoids a redundant copy between request decoding and Kafka
enqueueing. Decoded record keys, values, and header values are treated as
immutable borrowed byte slices while they are in flight.

Current memory choices:

- HTTP body reads are pre-sized when `Content-Length` is known.
- JSON values that are already compact are borrowed after trim validation.
- Pretty JSON is compacted only when needed.
- Kafka record wrapper objects are reused through a small `sync.Pool`.
- Key, value, and header bytes are not defensively cloned before `TryProduce`.
- Produce responses are encoded directly into JSON bytes to avoid per-record
  pointer allocations in the response hot path.

This goes beyond the earlier MVP path, which copied key/value/header bytes once
more before entering the Kafka producer. The producer result does not retain
payload bytes, so wrapper objects can be reset after franz-go invokes the
callback.

## Backpressure

Backpressure is intentionally applied before enqueueing into franz-go:

- request body size
- records per request
- decoded key/value/header limits
- global outstanding record count
- global outstanding payload bytes

When admission capacity is exhausted, the proxy rejects quickly rather than
creating an unbounded Go channel ahead of the producer. That keeps memory
predictable under broker slowdown or high client concurrency.

## Comparison target

The benchmark goal is not just "higher records/sec". A useful result must be
read with:

- records/sec
- p50/p95/p99 HTTP latency
- failure rate
- CPU and allocation profiles
- configured `acks`, compression, linger, request size, and records/request

The earlier local high-concurrency result showed Confluent ahead when the Go
proxy was configured with `KAFKA_LINGER=5ms`. A focused spot check with
`KAFKA_LINGER=0ms` reversed that result for the tested JSON shape: the Go proxy
produced more records/sec with lower median and tail latency. Future
optimization work should keep comparing equivalent linger, acks, compression,
payload, and batch settings before drawing capacity conclusions.
