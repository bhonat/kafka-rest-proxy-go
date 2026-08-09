# Local TLS fixture generator

Confluent's Kafka REST repository includes SSL fixtures for integration tests.
This directory holds the KafkaRestProxy-Go analogue: a local-only certificate
generator used by the SASL_SSL and mTLS Compose profiles.

Generated files are intentionally ignored by Git. Do not commit private keys.
