# Changelog

All notable changes to KafkaRestProxy-Go should be documented in this file.

This project uses semantic versioning once public release tags begin.

## 0.1.0 - unreleased

Initial producer-only implementation and hardening baseline:

- Confluent REST Proxy v2-style `POST /topics/{topic}` producer API.
- JSON and binary producer payloads.
- Async `franz-go` producer engine with bounded admission.
- Confluent-style `offsets[]` response encoding.
- Local Docker Kafka, Confluent REST Proxy comparison, and 3-broker Kafka Compose stacks.
- Unit, integration, differential, failure-recovery, security, benchmark, and soak test scaffolding.
- Prometheus/Grafana local observability.
