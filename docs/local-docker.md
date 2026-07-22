# Local Docker stack

This project includes two Docker Compose setups:

1. `docker-compose.yml`: fast single-broker KRaft Kafka stack for local development.
2. `docker-compose.cluster.yml`: three-broker KRaft Kafka stack for replication and cluster behavior testing.

The REST proxy runs in the same Compose network as Kafka and connects with Docker-internal broker addresses. The host can call the proxy at `localhost:8080`.

The default stack also starts Prometheus and Grafana:

```text
REST proxy: http://localhost:8080
Prometheus: http://localhost:9090
Grafana:    http://localhost:3000
```

Grafana is provisioned with a Prometheus datasource and the `Kafka REST Proxy Go`
dashboard. The local login is `admin` / `admin`.

The same Compose project can also start Confluent REST Proxy as an optional
comparison target:

```bash
docker compose --profile comparison up --build
```

That exposes:

```text
Confluent REST Proxy: http://localhost:8082
```

## Fast local stack

```bash
docker compose up --build
```

Then produce through the Confluent-style endpoint:

```bash
curl -sS \
  -X POST "http://localhost:8080/topics/orders" \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  -H "Accept: application/vnd.kafka.v2+json" \
  -d '{"records":[{"key":"customer-123","value":{"order_id":"order-456","amount":42}}]}'
```

Kafka is also exposed to the host at:

```text
localhost:9092
```

## Three-broker stack

```bash
docker compose -f docker-compose.cluster.yml up --build
```

The proxy connects internally to:

```text
kafka-1:9092,kafka-2:9092,kafka-3:9092
```

Broker host ports:

```text
localhost:19092
localhost:29092
localhost:39092
```

## Smoke test

After either stack is up:

```bash
scripts/compose-smoke.sh
```

The script checks `/healthz`, `/readyz`, produces one JSON record, and prints the REST response.

Run the Docker-backed Go integration test:

```bash
KAFKA_INTEGRATION=1 go test ./integration -v
```

## Benchmark suite and comparison reports

Run a compact multi-scenario benchmark against the Go proxy:

```bash
make bench-suite
```

The report is written to:

```text
dist/benchmark-suite.html
```

Run the Go-vs-Confluent comparison:

```bash
make compose-up-comparison
make bench-compare
```

The comparison report is written to:

```text
dist/benchmark-comparison.html
```

The benchmark suite varies client-side dimensions such as payload size,
records/request and concurrent clients. Kafka producer compression and acks are
server-side settings, so compare those by running separate target instances
configured with the desired settings and passing each as `-target name=url`.

Use `-format binary` for a single binary-media benchmark, or
`-formats json,binary` in suite mode. Confluent's binary media type still uses a
JSON request envelope, but each record `key` and `value` is base64 encoded.

The benchmark HTML report also estimates how many proxy nodes are needed for a
target workload. By default it estimates `1,000,000` records/sec with `30%`
headroom. Override that with `-capacity-target-records` and
`-capacity-headroom`.

## Capture Confluent compatibility behavior

With the comparison profile running, capture live Confluent REST Proxy edge-case
responses:

```bash
make capture-confluent-fixtures
```

The capture report is written to:

```text
compatibility/captured/confluent-producer-edge-cases.json
```

## pprof

Set `PPROF_ENABLE=true` for the Go proxy to expose pprof on the main HTTP
server:

```text
http://localhost:8080/debug/pprof/
```

To verify the record from Kafka directly in the default stack:

```bash
docker compose exec -T kafka \
  kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic orders \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 5000
```

## Stop and clean up

Stop containers but keep Kafka volumes:

```bash
docker compose down
```

Remove local Kafka data too:

```bash
docker compose down -v
```

For the three-broker stack:

```bash
docker compose -f docker-compose.cluster.yml down -v
```

## Notes

- The default stack is intentionally single-broker for speed. It is still a Kafka KRaft cluster, just with one broker/controller node.
- Use the three-broker file when testing `acks=all`, replication factor behavior, leader movement, or broker failure.
- The local setup uses plaintext listeners and is not intended for production.
