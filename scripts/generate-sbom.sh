#!/usr/bin/env sh
set -eu

out_dir="${1:-dist/release}"
mkdir -p "$out_dir"

if ! command -v syft >/dev/null 2>&1; then
  echo "syft is required to generate release SBOMs." >&2
  echo "Install syft locally or run this target in CI." >&2
  exit 127
fi

syft dir:. -o spdx-json="$out_dir/source.spdx.json"

if [ -n "${IMAGE_REF:-}" ]; then
  syft "$IMAGE_REF" -o spdx-json="$out_dir/image.spdx.json"
fi
