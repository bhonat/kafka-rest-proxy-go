#!/usr/bin/env sh
set -eu

out_dir="${1:-dist/release}"

if ! command -v cosign >/dev/null 2>&1; then
  echo "cosign is required to sign release artifacts." >&2
  echo "Install cosign locally or run this target in CI with OIDC enabled." >&2
  exit 127
fi

if [ ! -f "$out_dir/SHA256SUMS" ]; then
  echo "$out_dir/SHA256SUMS does not exist; run make build-release first." >&2
  exit 1
fi

find "$out_dir" -maxdepth 1 -type f \( -name 'kafka-rest-proxy-go-*' -o -name 'SHA256SUMS' -o -name '*.spdx.json' \) | sort | while IFS= read -r artifact; do
  cosign sign-blob --yes --bundle "$artifact.sigstore" "$artifact"
done

if [ -n "${IMAGE_REF:-}" ]; then
  cosign sign --yes "$IMAGE_REF"
fi
