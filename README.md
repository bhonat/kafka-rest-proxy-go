# kafka-rest-proxy-go

Producer-only Kafka REST gateway in Go. The MVP implements the narrow Confluent REST Proxy producer shape:

```http
POST /topics/{topic}
Content-Type: application/vnd.kafka.json.v2+json
Accept: application/vnd.kafka.v2+json
```

It is backed by a shared asynchronous `franz-go` producer. HTTP requests remain open until Kafka callbacks complete so the response can include per-record partition/offset/error metadata, while the producer batches records across concurrent requests internally.

## Current MVP scope

- `POST /topics/{topic}`
- JSON payloads via `application/vnd.kafka.json.v2+json`
- Binary payloads via `application/vnd.kafka.binary.v2+json`
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
- Dockerfile and starter Helm chart

Out of scope for this MVP:

- consumer API
- admin API
- Schema Registry
- Avro / Protobuf / JSON Schema media types
- exact byte-for-byte Confluent error compatibility

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
| `KAFKA_CLIENT_ID` | `kafka-rest-proxy-go` | Kafka client id |
| `KAFKA_REQUIRED_ACKS` | `all` | `all`, `1`, or `0` |
| `KAFKA_COMPRESSION` | `lz4` | `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `KAFKA_LINGER` | `5ms` | Producer linger duration |
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
| `KAFKA_TLS_ENABLE` | `false` | Enable TLS to brokers |
| `KAFKA_TLS_INSECURE_SKIP_VERIFY` | `false` | Skip broker cert verification |
| `KAFKA_SASL_MECHANISM` | empty | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `KAFKA_SASL_USERNAME` | empty | SASL username |
| `KAFKA_SASL_PASSWORD` | empty | SASL password |

## Endpoints

| Endpoint | Method | Description |
| --- | --- | --- |
| `/topics/{topic}` | `POST` | Produce records |
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

## Build

```bash
make build
docker build -t kafka-rest-proxy-go:dev .
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
