# Producer compatibility hardening

This directory contains compatibility assets for the producer-only Confluent REST
Proxy API implemented by this project: v2 topic/partition produce, v2
schema-aware media types, and the v3 records / records:batch producer surface.

## Golden fixture tests

The normal test suite runs static fixtures without Kafka:

```bash
go test ./compatibility
```

Those tests use a fake producer and validate the Go HTTP compatibility layer.

## Live differential tests

The differential harness sends the same request to the Go proxy and Confluent
REST Proxy, then compares normalized responses. It is disabled by default so
normal `go test ./...` does not require Docker or live services.

Start the local comparison stack first:

```bash
docker compose --profile comparison up --build
```

Then run:

```bash
KAFKA_REST_DIFFERENTIAL=1 go test ./compatibility -run TestDifferentialProducerCompatibility -v
```

Optional environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_REST_GO_URL` | `http://localhost:8080` | Go proxy base URL |
| `KAFKA_REST_CONFLUENT_URL` | `http://localhost:8082` | Confluent REST Proxy base URL |
| `KAFKA_REST_DIFFERENTIAL_TOPIC` | `orders` | Existing JSON test topic |
| `KAFKA_REST_DIFFERENTIAL_BINARY_TOPIC` | `binary-events` | Existing binary test topic |
| `KAFKA_REST_DIFFERENTIAL_CLUSTER_ID` | `MkU3OEVBNTcwNTJENDM2Qk` | Cluster id for v3 producer paths |
| `KAFKA_REST_DIFFERENTIAL_TIMEOUT` | `10s` | Per-request timeout |
| `KAFKA_REST_DIFFERENTIAL_V3` | unset | Set to `1` to also compare v3 records and records:batch |
| `KAFKA_REST_DIFFERENTIAL_SCHEMA` | unset | Set to `1` to also compare v2 Avro/Protobuf/JSON Schema requests |
| `KAFKA_REST_DIFFERENTIAL_EDGE` | unset | Set to `1` to also run diagnostic invalid topic/partition cases |

The full producer-surface comparison requires the comparison and Schema Registry
profiles:

```bash
docker compose --profile comparison --profile schema-registry up --build -d
make test-differential-full
```

The strict default scenarios are:

- JSON produce success
- JSON partition produce success
- missing record `key`
- `key:null`
- `value:null`
- `Content-Type` with `charset`
- wildcard `Accept`
- omitted/default `Accept`
- larger JSON batch with 10 records
- binary produce success
- binary partition produce success
- binary `key:null` / `value:null`
- binary bad base64
- malformed JSON
- unsupported media type
- unsupported `Accept`
- OpenAPI contract coverage for v2 JSON/binary/schema media types and v3
  records / records:batch endpoints

Unit coverage also exercises schema-aware producer encoding against in-memory and
mocked Schema Registry clients for Avro, Protobuf, and JSON Schema.

The Docker-backed Schema Registry integration gate verifies Avro, Protobuf, and
JSON Schema registration and Confluent wire-format production:

```bash
docker compose --profile schema-registry up --build -d
make test-schema-registry-integration
```

The default diagnostic scenarios are:

- empty `records[]`
- missing `records`
- `records:null`
- missing record `value`

These stay in the differential harness as diagnostics because exact error
messages can vary by Confluent REST Proxy version, but the Go proxy now follows
the observed broad behavior:

- empty, missing, or null `records` return HTTP `422`;
- missing record `value` is treated as a successful null-value produce.

Response normalization intentionally ignores nondeterministic offsets and exact
human-readable error strings while still comparing status, response shape,
`error_code`, nullability and content type where Confluent consistently returns
one.

The optional edge scenarios are diagnostic because local proxy policy, Kafka
cluster topology, or Confluent REST Proxy version can make them intentionally
differ. For example, the Go proxy may reject an invalid topic at the allowlist
layer while Confluent forwards it to Kafka and returns a per-record error.

Diagnostic scenarios currently include:

- invalid topic
- negative partition
- partition too large
- JSON record headers
- binary record headers

Record headers are diagnostic rather than strict because this project can decode
and forward Kafka headers, but this harness only compares producer HTTP
responses. It does not consume records back from Kafka to prove header
persistence, and Confluent REST Proxy header behavior can vary by API version.

With the local `confluentinc/cp-kafka-rest:7.7.1` comparison container, the
edge diagnostics currently show these known divergences:

- Invalid topics are rejected by the Go proxy's topic allowlist with `403`,
  while Confluent forwards to Kafka and returns HTTP `200` with a per-record
  Kafka error.
- Negative partitions are rejected by the Go proxy validation layer, while local
  Confluent returns HTTP `500`.
- Very large partition numbers return a per-record Kafka error from the Go
  proxy; local Confluent may time out before returning a response.
- Record `headers` are accepted by the Go proxy but rejected by local Confluent
  with `422` for this v2 producer API path.
