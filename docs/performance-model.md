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

For high concurrency, the important tuning knob observed so far is linger:

- `0ms` favors low-latency, lightly loaded requests.
- `5ms` improves cross-request batching and high-concurrency throughput.
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

The current local high-concurrency result showed Confluent ahead on raw
records/sec in one spot check, while the Go proxy with `KAFKA_LINGER=5ms`
returned lower p99 latency and no failures. The next optimization work should
continue to target allocation pressure and producer enqueue efficiency before
adding any new application-level queuing.
