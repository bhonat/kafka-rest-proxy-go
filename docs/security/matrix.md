# Security test matrix

| Capability | Local support | Automated test | Release gate |
|---|---:|---:|---:|
| No-auth local development | Yes | Compose smoke | Yes |
| Bearer-token proxy auth | Yes | Unit tests | Yes |
| Topic allowlist | Yes | Unit tests | Yes |
| Request size limits | Yes | Unit tests | Yes |
| Record/key/header limits | Yes | Unit tests | Yes |
| Kafka SASL/PLAIN | Yes | `TestSecurityIntegrationSASLProduceConsume` | Yes |
| Kafka bad credentials | Yes | `TestSecurityIntegrationBadCredentials` | Yes |
| Kafka TLS client config | Yes | `TestNewTLSConfigLoadsCAFile` | Yes |
| SASL_SSL | Yes | `TestSecurityIntegrationSASLSSLProduceConsume` | Yes |
| SCRAM-SHA-256/512 | Client supports it | Unit coverage added; Compose integration needed | No |
| Kafka ACL allow/deny | Yes | `TestSecurityIntegrationACLAllowDeny` | Yes |
| mTLS client auth | Yes | `TestSecurityIntegrationMTLSProduceConsume` | Yes |
| Certificate rotation | Not yet | Needed | No |
| Secrets manager integration | Not yet | Needed | No |

## Release-blocking next steps

- Add certificate reload/rotation behavior or document restart requirement.
