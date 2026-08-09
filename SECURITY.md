# Security policy

## Supported versions

KafkaRestProxy-Go is currently pre-1.0. Security fixes should target `main`
until release branches exist.

## Reporting vulnerabilities

Do not open public issues for suspected vulnerabilities.

Until a private disclosure channel is configured, report vulnerabilities to the
project owner directly using the organization-approved private channel.

## Current security scope

Implemented and tested:

- Optional bearer-token authentication at the proxy.
- Kafka SASL/PLAIN integration test.
- Kafka bad-credential diagnostic test.
- Kafka TLS client configuration knobs.
- Request-size, record-count, key-size, record-size, and header-size limits.
- Topic allowlist support.

Not yet implemented as release-blocking gates:

- SASL_SSL integration.
- SCRAM integration.
- mTLS client authentication.
- Kafka ACL allow/deny integration tests.
- Certificate rotation tests.
- External secret manager integration.

See [docs/security/matrix.md](docs/security/matrix.md) for the security test
matrix and release gates.
