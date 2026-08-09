# Repository parity plan

The goal is not to copy Confluent Kafka REST Proxy file-for-file. Their
repository implements the full REST Proxy product surface. KafkaRestProxy-Go is
intentionally producer-only.

The goal is to reach comparable repository maturity for the supported producer
surface:

- documented API contract;
- repeatable compatibility tests;
- repeatable failure and security tests;
- CI quality gates;
- release and rollback process;
- security/compliance process;
- performance regression process.

## Current parity status

| Area | Status | Notes |
|---|---|---|
| Producer API implementation | Added | v2 JSON/binary/schema-aware topic and partition producer endpoints plus v3 streaming records and records:batch endpoints. |
| API specification | Added | `api/v2/openapi.yaml` and `api/v3/openapi.yaml`. |
| Unit tests | Added | API, schema encoding/registry, limits, producer, benchmark, soak. |
| Live Kafka integration | Added | Single-broker Compose plus Schema Registry Avro, Protobuf, and JSON Schema integration. |
| Live Confluent differential | Added | Strict v2 JSON/binary matrix plus gated v2 schema-aware and v3 producer comparisons. |
| Kafka failure tests | Added | Single-broker stop/start recovery. |
| Multi-broker tests | Added | 3-broker rolling restart gate, env-gated. |
| Security tests | Added | Bearer auth, SASL/PLAIN, SASL_SSL, Kafka mTLS, Kafka ACL allow/deny, bad credentials, TLS config loading. |
| CI quality gates | Added | Format, vet, race, coverage, vulnerability/static analysis hooks, live integration gates, full differential gate, security gates, and soak smoke. |
| Release packaging | Added | Docker, Helm, release workflow, bin scripts, service/debian scaffold, SBOM generation, and keyless signing hooks. |
| License/compliance | Added | Apache-2.0 assumed, generated third-party license bundle, NOTICE docs; owner/legal review still required before publication. |
| Performance regression | Added | Manual benchmark/soak workflows and docs. |

## Remaining producer-only gaps

- Exact Confluent error body/status matching for every captured edge case.
- Complex Schema Registry reference/import compatibility for Avro/Protobuf/JSON Schema graphs.
- Long soak and benchmark baselines from production-like hosts.
- Backward-compatibility policy for any response-shape changes.
- Publication metadata still needs final owner decisions: canonical module path,
  GitHub org/repo, maintainer list, and legal approval for project name/NOTICE.
