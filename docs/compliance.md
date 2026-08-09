# Compliance and licensing

The repository includes an Apache-2.0 root `LICENSE` as the current project
license assumption. The owner should confirm this before public release.

## Minimum checks

- Dependency inventory.
- Dependency license review.
- Checked-in generated third-party license bundle.
- Container base image review.
- SBOM generation.
- Vulnerability scan.
- Secrets scan.
- NOTICE generation where required.

## Suggested commands

```bash
go list -m -json all
make generate-licenses
make licenses-check
go version -m bin/kafka-rest-proxy-go
```

Recommended external tools:

- `govulncheck`
- `trivy`
- `syft`
- `grype`
- `gitleaks`

These tools are intentionally optional in local development but should be
required in release CI.
