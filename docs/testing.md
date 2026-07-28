# Test and hardening guide

## Fast checks

Run these on every change:

```bash
go test ./...
go vet ./...
go test -race ./internal/api ./internal/limits ./internal/producer/franz ./compatibility ./cmd/bench-produce ./cmd/soak-produce
```

Or:

```bash
make test
make test-race
```

## Docker-backed integration

Start the local stack:

```bash
docker compose up --build -d
```

Then run:

```bash
KAFKA_INTEGRATION=1 go test ./integration -v
```

The failure/recovery test intentionally stops and restarts the local Kafka
service. Run it separately:

```bash
KAFKA_INTEGRATION=1 KAFKA_FAILURE_INTEGRATION=1 go test ./integration -run TestComposeKafkaFailureRecovery -count=1 -v
```

## Live Confluent differential tests

Start the comparison profile:

```bash
docker compose --profile comparison up --build -d
```

Then run:

```bash
KAFKA_REST_DIFFERENTIAL=1 go test ./compatibility -run TestDifferentialProducerCompatibility -count=1 -v
```

Add diagnostic edge cases:

```bash
KAFKA_REST_DIFFERENTIAL=1 KAFKA_REST_DIFFERENTIAL_EDGE=1 go test ./compatibility -run TestDifferentialProducerCompatibility -count=1 -v
```

## SASL security integration

Start the optional SASL stack:

```bash
docker compose -f docker-compose.security.yml up --build -d
```

Then run:

```bash
KAFKA_SECURITY_INTEGRATION=1 go test ./integration -run TestSecurityIntegration -count=1 -v
```

The optional bad-credentials diagnostic requires the profile:

```bash
docker compose -f docker-compose.security.yml --profile bad-credentials up --build -d
KAFKA_SECURITY_INTEGRATION=1 KAFKA_SECURITY_BAD_CREDENTIALS=1 go test ./integration -run TestSecurityIntegrationBadCredentials -count=1 -v
```

## Soak gate

For a short local SLO gate:

```bash
make test-soak
```

For a production-like run, use longer durations and real thresholds:

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
