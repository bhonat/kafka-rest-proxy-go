# Test and hardening guide

## Fast checks

Run these on every change:

```bash
go test ./...
go vet ./...
go test -race ./internal/api ./internal/limits ./internal/producer/franz ./compatibility ./cmd/bench-produce ./cmd/soak-produce
make fmt-check
make openapi-check
make coverage
```

Or:

```bash
make test
make test-race
```

CI also runs vulnerability/static-analysis hooks. They download tools on demand:

```bash
make govulncheck
make staticcheck
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

## Three-broker Kafka rolling restart

Start the 3-broker stack:

```bash
docker compose -f docker-compose.cluster.yml up --build -d
```

Then run:

```bash
KAFKA_CLUSTER_INTEGRATION=1 go test ./integration -run TestClusterRollingBrokerRestart -count=1 -v
```

This intentionally stops and restarts `kafka-2` while producing records with
`acks=all`.

## Live Confluent differential tests

Start the comparison profile:

```bash
docker compose --profile comparison up --build -d
```

Then run:

```bash
KAFKA_REST_DIFFERENTIAL=1 go test ./compatibility -run TestDifferentialProducerCompatibility -count=1 -v
```

Run the full producer-surface comparison, including v3 records, v3
records:batch, and v2 schema-aware media types:

```bash
docker compose --profile comparison --profile schema-registry up --build -d
make test-differential-full
```

Add diagnostic edge cases:

```bash
KAFKA_REST_DIFFERENTIAL=1 KAFKA_REST_DIFFERENTIAL_EDGE=1 go test ./compatibility -run TestDifferentialProducerCompatibility -count=1 -v
```

## Schema Registry integration

Start the local Kafka, proxy, and Schema Registry stack:

```bash
docker compose --profile schema-registry up --build -d
```

Then run:

```bash
KAFKA_SCHEMA_REGISTRY_INTEGRATION=1 go test ./integration -run TestComposeSchemaRegistry -count=1 -v
```

Or:

```bash
make test-schema-registry-integration
```

These tests verify that Avro, Protobuf, and JSON Schema producer requests
register schemas through Schema Registry, return `value_schema_id`, and write
Kafka records with the Confluent wire-format magic byte and schema id prefix.

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

## SASL_SSL security integration

Generate the local test CA, broker certificate, and Kafka PKCS12 stores:

```bash
make prepare-security-secrets
```

Start the SASL_SSL profile:

```bash
docker compose -f docker-compose.security.yml --profile sasl-ssl up --build -d
```

Then run:

```bash
KAFKA_SECURITY_INTEGRATION=1 KAFKA_SASL_SSL_INTEGRATION=1 go test ./integration -run TestSecurityIntegrationSASLSSLProduceConsume -count=1 -v
```

Or:

```bash
make test-sasl-ssl-integration
```

## mTLS security integration

Generate the local test CA, broker certificate, client certificate, and Kafka
PKCS12 stores:

```bash
make prepare-security-secrets
```

Start the mTLS profile:

```bash
docker compose -f docker-compose.security.yml --profile mtls up --build -d
```

Then run:

```bash
KAFKA_SECURITY_INTEGRATION=1 KAFKA_MTLS_INTEGRATION=1 go test ./integration -run TestSecurityIntegrationMTLSProduceConsume -count=1 -v
```

Or:

```bash
make test-mtls-integration
```

## Kafka ACL integration

Start the ACL profile:

```bash
docker compose -f docker-compose.security.yml --profile acl up --build -d
```

Then run:

```bash
KAFKA_SECURITY_INTEGRATION=1 KAFKA_ACL_INTEGRATION=1 go test ./integration -run TestSecurityIntegrationACLAllowDeny -count=1 -v
```

Or:

```bash
make test-acl-integration
```

This test verifies both sides of the producer path: `acl-allowed` succeeds and
`acl-denied` returns a Confluent-style HTTP 200 response containing
`offsets[].error_code` and `offsets[].error` for the Kafka-side authorization
failure.

## Producer load integration

This is the Confluent-style repo load-test analogue for the supported producer
surface. Start the local Compose stack:

```bash
docker compose up --build -d
```

Then run:

```bash
KAFKA_INTEGRATION=1 KAFKA_LOAD_INTEGRATION=1 go test ./integration -run TestComposeProducerLoad -count=1 -v
```

Useful knobs:

```text
KAFKA_LOAD_CLIENTS
KAFKA_LOAD_RECORDS_PER_REQUEST
KAFKA_LOAD_DURATION
KAFKA_LOAD_MIN_RECORDS_PER_SECOND
KAFKA_LOAD_MAX_FAILURE_RATE
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

## Benchmark and release gates

```bash
make bench-regression
make build-release
make generate-sbom
```

Release signing is handled by CI with keyless Sigstore/Cosign signing. For a
local release rehearsal, install `cosign`, run `make build-release`, then run:

```bash
make sign-release
```
