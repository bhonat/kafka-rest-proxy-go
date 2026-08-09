# Configuration

The Confluent Kafka REST repository ships a `config/` directory with service
configuration examples. KafkaRestProxy-Go is environment-driven, so the checked
in sample is an `.env` file that can be sourced by the bundled `bin/` scripts,
Docker, systemd, or Kubernetes tooling.

Use `config/kafka-rest-proxy-go.env` as a starting point, then override secrets
through your platform secret manager rather than committing them here.
