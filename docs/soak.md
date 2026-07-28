# Producer Soak/Load Harness

`cmd/soak-produce` is a repo-native load gate for a running Kafka REST Proxy-compatible producer endpoint. It is intentionally separate from the HTML benchmark suite: benchmarks compare scenarios, while the soak harness answers one operational question:

> Can this proxy sustain this workload while staying inside the selected SLO thresholds?

The command is not part of normal `go test ./...` execution. Run it explicitly against a live proxy and Kafka cluster.

## Example

```bash
go run ./cmd/soak-produce \
  -url http://localhost:8080 \
  -topic orders \
  -duration 30m \
  -warmup 2m \
  -clients 64 \
  -records-per-request 100 \
  -payload-bytes 512 \
  -format json \
  -max-failure-rate 0 \
  -min-records-sec 100000 \
  -max-p99 50ms
```

The process exits with:

- `0` when all thresholds pass.
- `1` when one or more thresholds fail.
- `2` when options are invalid.

## Flags

| Flag | Default | Description |
|---|---:|---|
| `-url` | `http://localhost:8080` | REST proxy base URL. |
| `-topic` | `orders` | Kafka topic to produce to. |
| `-duration` | `10m` | Measured run duration. |
| `-warmup` | `0` | Optional warmup duration; results are discarded. |
| `-clients` | `32` | Concurrent HTTP clients. |
| `-records-per-request` | `10` | Records in each Confluent-style produce request. |
| `-payload-bytes` | `512` | Payload bytes per record. |
| `-format` | `json` | `json` or `binary`. |
| `-timeout` | `30s` | Per-request timeout. |
| `-max-latency-samples` | `1000000` | Maximum retained samples for percentile calculation. |
| `-max-failure-rate` | `0` | Maximum record failure rate. Supports fractions or percentages, for example `0.001` or `0.1%`. |
| `-min-records-sec` | `0` | Minimum successful records/sec. `0` disables this threshold. |
| `-max-p99` | `0` | Maximum p99 request latency. `0` disables this threshold. |

## Failure semantics

The harness treats a request as failed when:

- the HTTP request fails;
- the response status is not `2xx`;
- the response body is not a valid producer response;
- the response has a different number of `offsets[]` entries than requested;
- any `offsets[]` entry contains `error_code` or a non-empty `error`.

That last rule matters because Confluent REST Proxy can return HTTP `200` with per-record Kafka-side errors. The soak gate therefore tracks both request-level and record-level failure rates, and threshold evaluation uses record-level failure rate.

## Output

The command prints one concise summary line:

```text
soak_result=pass url=http://localhost:8080 topic=orders duration=30m0s elapsed=30m0s clients=64 records_per_request=100 payload_bytes=512 format=json requests=12345 success_requests=12345 failed_requests=0 attempted_records=1234500 success_records=1234500 failed_records=0 records_per_sec=68583.33 requests_per_sec=685.83 record_failure_rate=0.0000% request_failure_rate=0.0000% p50=4ms p95=12ms p99=25ms latency_samples=12345 thresholds=max_failure_rate:0.0000%,min_records_sec:100000.00,max_p99:50ms
```

When thresholds fail, it prints one `violation=...` line per failed SLO and exits non-zero:

```text
violation=min_records_sec actual=92000.00 threshold=>= 100000.00
violation=max_p99 actual=78ms threshold=<= 50ms
```

## Suggested gates

For a staging gate:

```bash
go run ./cmd/soak-produce \
  -duration 30m \
  -warmup 2m \
  -clients 64 \
  -records-per-request 100 \
  -payload-bytes 512 \
  -format json \
  -max-failure-rate 0 \
  -min-records-sec 100000 \
  -max-p99 50ms
```

For a longer production-candidate soak:

```bash
go run ./cmd/soak-produce \
  -duration 24h \
  -warmup 5m \
  -clients 128 \
  -records-per-request 100 \
  -payload-bytes 512 \
  -format json \
  -max-failure-rate 0 \
  -min-records-sec 100000 \
  -max-p99 75ms
```

Run binary format separately if binary producers are in scope:

```bash
go run ./cmd/soak-produce \
  -duration 2h \
  -clients 64 \
  -records-per-request 100 \
  -payload-bytes 512 \
  -format binary \
  -max-failure-rate 0 \
  -min-records-sec 100000 \
  -max-p99 75ms
```

## Limitations

- The harness drives one target at a time; use `cmd/bench-produce` for side-by-side reports.
- Percentiles are based on retained request-latency samples, capped by `-max-latency-samples`.
- It does not consume records back from Kafka; use the integration tests for produce/consume verification.
- It does not currently export machine-readable JSON. The line-oriented output is stable enough for simple CI logs and shell gates.
