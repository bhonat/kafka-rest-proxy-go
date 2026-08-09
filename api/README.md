# API contract

This directory contains the versioned REST contract for the producer-only
Kafka REST proxy.

- `v2/openapi.yaml` documents the Confluent REST Proxy v2-compatible producer
  surface implemented by this service.
- `v3/openapi.yaml` documents the Confluent REST Proxy v3 Records producer
  subset implemented by this service.

The project intentionally documents only the supported producer subset:

- `POST /topics/{topic}`
- `POST /topics/{topic}/partitions/{partition}`
- `POST /v3/clusters/{cluster_id}/topics/{topic_name}/records`
- `POST /v3/clusters/{cluster_id}/topics/{topic_name}/records:batch`
- JSON payloads: `application/vnd.kafka.json.v2+json`
- Binary payloads: `application/vnd.kafka.binary.v2+json`
- Schema-aware v2 payloads: Avro, Protobuf, JSON Schema
- v3 record data types: `BINARY`, `JSON`, `STRING`, `AVRO`, `JSONSCHEMA`,
  `PROTOBUF`
- Confluent-style `offsets[]` responses
- v3 delivery reports with `error_code: 200` on success
- v3 batch `207` responses with `successes[]` and `failures[]`

Consumer APIs and admin APIs are outside the current compatibility scope.
