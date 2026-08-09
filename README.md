# kafka-rest-proxy-go

Module: `github.com/bhonat/kafka-rest-proxy-go`

Producer-only Kafka REST gateway in Go. The project implements the Confluent REST Proxy producer surface used by producer clients:

```http
POST /topics/{topic}
Content-Type: application/vnd.kafka.json.v2+json
Accept: application/vnd.kafka.v2+json
```

It is backed by a shared asynchronous `franz-go` producer. HTTP requests remain open until Kafka callbacks complete so the response can include per-record partition/offset/error metadata, while the producer batches records across concurrent requests internally.

## Current producer scope

- `POST /topics/{topic}`
- `POST /topics/{topic}/partitions/{partition}`
- `POST /v3/clusters/{cluster_id}/topics/{topic_name}/records`
- `POST /v3/clusters/{cluster_id}/topics/{topic_name}/records:batch`
- JSON payloads via `application/vnd.kafka.json.v2+json`
- Binary payloads via `application/vnd.kafka.binary.v2+json`
- Schema-aware v2 payloads via:
  - `application/vnd.kafka.avro.v2+json`
  - `application/vnd.kafka.protobuf.v2+json`
  - `application/vnd.kafka.jsonschema.v2+json`
- v3 record data types: `BINARY`, `STRING`, `JSON`, `AVRO`, `PROTOBUF`, `JSONSCHEMA`
- batched `records[]`
- nullable keys and values
- optional explicit partition
- optional headers
- Confluent-style `offsets[]` response
- async `franz-go` `TryProduce` callbacks
- bounded admission control
- health/readiness endpoints
- OpenTelemetry metrics exported in Prometheus format for Grafana
- optional `/debug/pprof` profiling endpoints
- optional Confluent-style request/byte rate limiting
- optional Schema Registry integration for schema-aware producer records
- release SBOM generation and keyless artifact/image signing
- Dockerfile and starter Helm chart

Out of scope:

- consumer API
- admin API
- exact byte-for-byte Confluent error compatibility
- full Schema Registry reference/import compatibility for complex schema graphs

## Run locally

```bash
export KAFKA_BROKERS=localhost:9092
go run ./cmd/kafka-rest-proxy-go
```

Or run Kafka and the proxy together with Docker Compose:

```bash
docker compose up --build
```

For a three-broker local Kafka cluster:

```bash
docker compose -f docker-compose.cluster.yml up --build
```

More details are in [docs/local-docker.md](docs/local-docker.md).

Smoke-test the running Compose stack:

```bash
scripts/compose-smoke.sh
```

Local observability endpoints:

```text
Grafana:    http://localhost:3000
Prometheus: http://localhost:9090
Metrics:    http://localhost:8080/metrics
```

Grafana is provisioned automatically with a Prometheus datasource and the
`Kafka REST Proxy Go` dashboard. The local login is `admin` / `admin`.

Start the optional Confluent REST Proxy comparison target on `localhost:8082`:

```bash
docker compose --profile comparison up --build
```

Start the optional Schema Registry integration target on `localhost:8081`:

```bash
docker compose --profile schema-registry up --build
```

Produce JSON:

```bash
curl -sS \
  -X POST "http://localhost:8080/topics/orders" \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  -H "Accept: application/vnd.kafka.v2+json" \
  -d '{"records":[{"key":"customer-123","value":{"order_id":"order-456"}}]}'
```

Expected response shape:

```json
{
  "offsets": [
    {
      "partition": 0,
      "offset": 123,
      "error_code": null,
      "error": null
    }
  ]
}
```

## Configuration

Configuration is environment-driven for the MVP.

| Variable | Default | Purpose |
| --- | --- | --- |
| `HTTP_ADDR` | `:8080` | HTTP listen address |
| `PPROF_ENABLE` | `false` | Enable `/debug/pprof` on the main HTTP server |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker list |
| `KAFKA_CLUSTER_ID` | `local` | Cluster id accepted by v3 producer paths |
| `KAFKA_CLIENT_ID` | `kafka-rest-proxy-go` | Kafka client id |
| `KAFKA_REQUIRED_ACKS` | `all` | `all`, `1`, or `0` |
| `KAFKA_COMPRESSION` | `lz4` | `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `KAFKA_LINGER` | `0ms` | Producer linger duration; use `5ms` only for throughput-biased profiles that can tolerate added response latency |
| `KAFKA_DELIVERY_TIMEOUT` | `30s` | Record delivery timeout |
| `KAFKA_REQUEST_TIMEOUT` | `10s` | Broker produce request timeout |
| `KAFKA_BATCH_MAX_BYTES` | `1048576` | Max Kafka record batch bytes |
| `KAFKA_MAX_BUFFERED_RECORDS` | `100000` | franz-go buffered record cap |
| `KAFKA_MAX_BUFFERED_BYTES` | `134217728` | franz-go buffered byte cap |
| `REQUEST_MAX_BYTES` | `8388608` | Max HTTP request body bytes |
| `REQUEST_MAX_RECORDS` | `1000` | Max records per HTTP request |
| `REQUEST_MAX_RECORD_BYTES` | `1048576` | Max decoded Kafka record bytes |
| `REQUEST_MAX_KEY_BYTES` | `1048576` | Max decoded Kafka key bytes |
| `REQUEST_MAX_HEADERS` | `64` | Max Kafka headers per record |
| `REQUEST_MAX_HEADER_BYTES` | `65536` | Max bytes per decoded Kafka header |
| `PRODUCE_TIMEOUT` | `30s` | HTTP wait timeout for Kafka callbacks |
| `TOPIC_ALLOWLIST` | empty | Optional comma-separated allowed topics; supports prefix wildcards like `integration-*` |
| `AUTH_BEARER_TOKENS` | empty | Optional comma-separated bearer tokens |
| `RATE_LIMIT_REQUESTS_PER_SECOND` | `0` | Optional global produce request rate limit; `0` disables |
| `RATE_LIMIT_REQUESTS_BURST` | `0` | Optional request rate burst; `0` derives from rate |
| `RATE_LIMIT_BYTES_PER_SECOND` | `0` | Optional global produce request-body byte rate limit; `0` disables |
| `RATE_LIMIT_BYTES_BURST` | `0` | Optional byte rate burst; `0` derives from request/body limits |
| `KAFKA_TLS_ENABLE` | `false` | Enable TLS to brokers |
| `KAFKA_TLS_INSECURE_SKIP_VERIFY` | `false` | Skip broker cert verification |
| `KAFKA_TLS_CA_FILE` | empty | Optional PEM CA bundle for Kafka TLS |
| `KAFKA_TLS_CERT_FILE` | empty | Optional PEM client certificate for Kafka mTLS |
| `KAFKA_TLS_KEY_FILE` | empty | Optional PEM client key for Kafka mTLS |
| `KAFKA_SASL_MECHANISM` | empty | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `KAFKA_SASL_USERNAME` | empty | SASL username |
| `KAFKA_SASL_PASSWORD` | empty | SASL password |
| `SCHEMA_REGISTRY_URL` | empty | Optional Schema Registry base URL for Avro/Protobuf/JSON Schema producer records |
| `SCHEMA_REGISTRY_USERNAME` | empty | Optional Schema Registry basic-auth username |
| `SCHEMA_REGISTRY_PASSWORD` | empty | Optional Schema Registry basic-auth password |

## Endpoints

| Endpoint | Method | Description |
| --- | --- | --- |
| `/topics/{topic}` | `POST` | Produce records |
| `/topics/{topic}/partitions/{partition}` | `POST` | Produce records to an explicit partition |
| `/v3/clusters/{cluster_id}/topics/{topic_name}/records` | `POST` | Produce one or more v3 record requests |
| `/v3/clusters/{cluster_id}/topics/{topic_name}/records:batch` | `POST` | Produce v3 batch request with `successes[]` and `failures[]` |
| `/healthz` | `GET` | Process health |
| `/readyz` | `GET` | Kafka reachability check |
| `/metrics` | `GET` | OpenTelemetry metrics exported in Prometheus format |
| `/debug/pprof/` | `GET` | Optional pprof index when `PPROF_ENABLE=true` |

## Design notes

The important part of the MVP is preserving Confluent's synchronous HTTP response while using asynchronous Kafka production internally:

1. HTTP handler validates and decodes the request.
2. Records are admitted through bounded local record/byte limits.
3. Each record is submitted with `franz-go` `TryProduce`.
4. A request-scoped collector records callbacks into the original input positions.
5. When all callbacks finish, the handler returns `offsets[]`.

This avoids a per-request synchronous Kafka call while still returning real broker offsets.

The producer accumulator, sender, batching, and memory ownership model are
documented in [docs/performance-model.md](docs/performance-model.md).

## Test

```bash
go test ./...
```

Security integration profiles cover SASL/PLAIN, SASL_SSL, mTLS client auth, bad
credentials, and Kafka ACL allow/deny behavior:

```bash
make prepare-security-secrets
docker compose -f docker-compose.security.yml --profile sasl-ssl --profile mtls --profile acl up --build -d
make test-security-integration
make test-sasl-ssl-integration
make test-mtls-integration
make test-acl-integration
```

Schema Registry integration coverage verifies Avro, Protobuf, and JSON Schema
registration plus Confluent wire-format production:

```bash
docker compose --profile schema-registry up --build -d
make test-schema-registry-integration
```

Run the strict live differential suite, including v3 producer requests and
schema-aware v2 producer media types, against the local Confluent REST Proxy
comparison target:

```bash
docker compose --profile comparison --profile schema-registry up --build -d
make test-differential-full
```

## Third-party license bundle

Regenerate the checked-in third-party Go module license bundle:

```bash
make generate-licenses
make licenses-check
```

## Build

```bash
make build
docker build -t kafka-rest-proxy-go:dev .
```

## Release artifacts

Release builds produce cross-platform binaries, SHA256 checksums, SBOMs, and
Sigstore/Cosign signatures. The GitHub release workflow also builds, pushes,
generates an SBOM for, and signs the release container image.

```bash
make build-release
make generate-sbom
make sign-release
```

`make sign-release` expects `cosign` to be installed. Set `IMAGE_REF` when a
container image should also be scanned or signed:

```bash
IMAGE_REF=ghcr.io/bhonat/kafka-rest-proxy-go:0.1.0 make generate-sbom
IMAGE_REF=ghcr.io/bhonat/kafka-rest-proxy-go:0.1.0 make sign-release
```

## Benchmark client

With the Compose stack running:

```bash
go run ./cmd/bench-produce \
  -url http://localhost:8080 \
  -topic orders \
  -duration 30s \
  -clients 32 \
  -records 10 \
  -payload-bytes 512 \
  -format json \
  -html dist/benchmark-report.html
```

Run a compact multi-scenario suite:

```bash
go run ./cmd/bench-produce \
  -suite \
  -url http://localhost:8080 \
  -topic orders \
  -duration 5s \
  -payload-sizes 128,512,1KiB \
  -records-per-request 1,10,100 \
  -client-counts 4,16 \
  -formats json,binary \
  -html dist/benchmark-suite.html
```

Compare this proxy with Confluent REST Proxy:

```bash
docker compose --profile comparison up --build

go run ./cmd/bench-produce \
  -suite \
  -target go=http://localhost:8080 \
  -target confluent=http://localhost:8082 \
  -topic orders \
  -duration 5s \
  -payload-sizes 128,512,1KiB \
  -records-per-request 1,10,100 \
  -client-counts 4,16 \
  -formats json,binary \
  -html dist/benchmark-comparison.html
```

The full workload matrix can be expressed as:

```bash
go run ./cmd/bench-produce \
  -suite \
  -target go=http://localhost:8080 \
  -target confluent=http://localhost:8082 \
  -topic orders \
  -duration 30s \
  -payload-sizes 128,512,1KiB,10KiB \
  -records-per-request 1,10,100,1000 \
  -client-counts 4,16,64,256 \
  -formats json,binary \
  -capacity-target-records 1000000 \
  -capacity-headroom 0.30 \
  -html dist/benchmark-comparison-full.html
```

Use `-format binary` for a single binary-media benchmark, or
`-formats json,binary` in suite mode. Confluent's binary media type still uses a
JSON request envelope, but each record `key` and `value` is base64 encoded.

Capacity estimates in the HTML report convert each target's measured
records/sec into an estimated node count:

```text
ceil(capacity-target-records * (1 + capacity-headroom) / measured-records-sec)
```

The defaults estimate nodes for `1,000,000` records/sec with `30%` headroom.
Use node estimates only from rows with near-zero failure rate; rows with
failures indicate local saturation or instability and should be treated as
capacity limits, not sizing recommendations.

Compression and acks are server-side producer settings. To compare them
honestly, run separate proxy instances configured with those settings and pass
each instance as a separate `-target name=url`. The benchmark also accepts
`-compression-labels` and `-acks-labels` so externally configured variants can
be labelled in the HTML report.

Capture live Confluent REST Proxy edge-case behavior for compatibility work:

```bash
make compose-up-comparison
make capture-confluent-fixtures
```

The generated capture report is written under `compatibility/captured/`.
