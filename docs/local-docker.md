# Local Docker stack

This project includes two Docker Compose setups:

1. `docker-compose.yml`: fast single-broker KRaft Kafka stack for local development.
2. `docker-compose.cluster.yml`: three-broker KRaft Kafka stack for replication and cluster behavior testing.

The REST proxy runs in the same Compose network as Kafka and connects with Docker-internal broker addresses. The host can call the proxy at `localhost:8080`.

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
