# Security model

This MVP intentionally keeps security small and deterministic. It is a producer-only
Kafka REST proxy, so the current controls focus on request admission, topic
restriction, and reducing accidental diagnostic exposure.

## Current controls

- Bearer authentication is disabled unless `AUTH_BEARER_TOKENS` is configured.
- When bearer authentication is configured, `POST /topics/{topic}`, `/metrics`,
  and `/debug/pprof/*` require `Authorization: Bearer <token>`.
- `/healthz` and `/readyz` bypass bearer authentication so Kubernetes probes can
  work without embedding application tokens.
- Topic allowlisting is controlled by `TOPIC_ALLOWLIST`.
- Allowlist entries are exact by default, for example `orders` only allows
  `orders`.
- Allowlist entries ending in `*` are prefix matches, for example `tenant-*`
  allows `tenant-alpha` but not `tenantless-alpha`.
- `/debug/pprof/*` is disabled unless `PPROF_ENABLE=true`.
- Metrics are exposed on `/metrics` when the service is built with metrics; in
  the default binary this is enabled and should be protected by bearer auth or by
  network policy in production.

## Intentional probe behavior

`/healthz` returns process health and does not call Kafka.

`/readyz` calls the producer readiness check and returns unavailable when Kafka
is not reachable. It also bypasses bearer authentication because readiness should
represent dependency availability, not caller credentials.

## Production limitations

This MVP does not yet provide:

- TLS termination for HTTP clients;
- JWT/OIDC validation;
- per-tenant or per-principal topic authorization;
- dynamic token reload;
- mTLS client identity;
- a separate metrics or admin listener;
- structured audit logs;
- secret-manager integration.

For production, place the proxy behind an ingress/API gateway that provides TLS,
identity, rate limiting, and tenant authorization, and restrict diagnostic
endpoints with network policy even when bearer auth is enabled.
