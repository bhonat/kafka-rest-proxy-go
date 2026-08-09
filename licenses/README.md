# Third-party dependency notices

Confluent's Kafka REST repository carries a checked-in license bundle. This
directory is the corresponding release-time home for KafkaRestProxy-Go
third-party dependency notices.

Before a public release:

1. Run `make generate-licenses`.
2. Generate an SBOM from `go.mod`, the container base image, and release
   artifacts.
3. Review direct and transitive dependency licenses.
4. Keep the root `NOTICE` file limited to project-level notices.

Suggested local commands:

```bash
go list -m -json all
make generate-licenses
make govulncheck
```
