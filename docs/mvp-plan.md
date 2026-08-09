# MVP plan

## Goal

Build a Go producer-only Kafka REST gateway that is compatible with the Confluent REST Proxy producer endpoint used by typical clients:

```http
POST /topics/{topic}
```

The implementation has since expanded beyond the initial MVP to include the
v2 partition producer endpoint, schema-aware v2 producer media types, and the v3
records / records:batch producer surface.

The service should improve the implementation surface for throughput work by avoiding the full Confluent REST Proxy feature set and relying on a shared asynchronous Go Kafka producer.

## MVP compatibility target

Supported now:

- `application/vnd.kafka.json.v2+json`
- `application/vnd.kafka.binary.v2+json`
- `application/vnd.kafka.v2+json`
- `application/json`
- `records[]`
- `key`
- `value`
- `partition`
- `headers`
- `offsets[]` response
- Avro
- Protobuf
- JSON Schema
- Schema Registry integration

Still deferred:

- consumers
- topic admin endpoints
- exact Confluent error-code parity for every edge case
- complex Schema Registry reference/import graphs

## Architecture

```text
HTTP clients
    |
    v
net/http route: POST /topics/{topic}
    |
    v
Confluent request decoder
    |
    v
bounded admission control
    |
    v
shared franz-go kgo.Client
    |
    v
Kafka
    |
    v
per-record callbacks -> ordered offsets[] response
```

## Async producer model

The service uses `TryProduce` for each record and aggregates callbacks per HTTP request. This mirrors the important shape of Confluent REST Proxy:

- records are enqueued into a shared producer;
- Kafka batching happens across concurrent HTTP requests;
- the HTTP response waits for callbacks only so it can return offsets/errors.

## Backpressure

Backpressure exists at two layers:

1. Local admission controller:
   - max outstanding records
   - max outstanding payload bytes
2. `franz-go` producer limits:
   - `MaxBufferedRecords`
   - `MaxBufferedBytes`

The service returns `429` when admission is exhausted.

## First validation milestones

- Compile and unit-test the MVP.
- Run against a local Kafka cluster.
- Compare happy-path response shape against Confluent REST Proxy.
- Add a compatibility corpus from your real Confluent REST Proxy traffic.
- Benchmark records/sec/core and MB/sec/core under your real payload distribution.

## Next implementation tasks

- Expand the seed compatibility fixtures with captured responses from your current Confluent REST Proxy deployment.
- Add exact Confluent error-code mapping for common broker failures.
- Add pprof endpoint behind an opt-in flag.
- Add optional `confluent-kafka-go` backend for bake-off.
- Add production Helm objects such as PodDisruptionBudget, ServiceMonitor, and HPA examples.
- Split Helm templates into standard chart files.
