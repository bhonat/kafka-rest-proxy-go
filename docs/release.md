# Release process

Before cutting a release, regenerate and review the checked-in third-party
license bundle:

```bash
make generate-licenses
make licenses-check
```

This document defines the minimum process before KafkaRestProxy-Go is treated as
a production release artifact.

## Versioning

- Version source: root `VERSION`.
- Release tags: `vMAJOR.MINOR.PATCH`.
- Pre-1.0 releases may change behavior, but API compatibility changes must be
  documented in `CHANGELOG.md`.

## Release candidate checklist

- [ ] `go test ./...`
- [ ] `go vet ./...`
- [ ] race tests for deterministic packages
- [ ] format check
- [ ] coverage report generated
- [ ] vulnerability scan
- [ ] static analysis
- [ ] Docker Compose config validation
- [ ] single-broker integration tests
- [ ] 3-broker integration tests
- [ ] Confluent differential tests
- [ ] full Confluent differential tests with v3 and schema-aware producer media types
- [ ] v2 schema-aware producer tests
- [ ] v3 records and records:batch producer tests
- [ ] live Schema Registry Avro, Protobuf, and JSON Schema integration tests
- [ ] SASL security integration tests
- [ ] SASL_SSL security integration tests
- [ ] mTLS security integration tests
- [ ] Kafka ACL allow/deny integration tests
- [ ] benchmark comparison report
- [ ] 2-hour soak
- [ ] release notes updated
- [ ] rollback notes updated
- [ ] license and notices reviewed
- [ ] SBOM generated
- [ ] release binaries signed
- [ ] release image signed

## Release artifacts

Expected artifacts:

- Linux amd64 binary.
- Linux arm64 binary.
- Darwin amd64 binary.
- Darwin arm64 binary.
- Docker image.
- Helm chart package.
- Benchmark report.
- Coverage report.
- SBOM.
- Sigstore/Cosign signature bundles for release binaries and checksums.

## Local release rehearsal

```bash
make fmt-check
make openapi-check
make licenses-check
go test ./...
go vet ./...
make build-release
make generate-sbom
```

If `cosign` is installed, sign the binary artifacts and checksums:

```bash
make sign-release
```

When a release image has been built and pushed, include it in the SBOM and
signature pass:

```bash
IMAGE_REF=ghcr.io/bhonat/kafka-rest-proxy-go:0.1.0 make generate-sbom
IMAGE_REF=ghcr.io/bhonat/kafka-rest-proxy-go:0.1.0 make sign-release
```

## Rollback

Every deployment should preserve:

- previous image tag;
- previous Helm values;
- previous benchmark report;
- known-good Kafka producer configuration.

Rollback should not require Kafka topic changes.
