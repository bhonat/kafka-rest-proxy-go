#!/usr/bin/env sh
set -eu

src="${1:-dist/benchmark-regression.html}"
out_dir="${2:-public}"
out="$out_dir/index.html"

if [ ! -f "$src" ]; then
  echo "benchmark report not found: $src" >&2
  exit 1
fi

mkdir -p "$out_dir"
cp "$src" "$out"

# Keep the published benchmark page free of local/user-specific identifiers.
# Local service URLs such as localhost:8080 are intentionally allowed because
# they describe the benchmark topology, not a person or workstation.
if grep -E -i '(/Users/|file://|MacBook|basarhamdionat|sonofdeus|bhonat@|users\.noreply|[[:alnum:]._%+-]+@[[:alnum:].-]+\.[[:alpha:]]{2,})' "$out" >/dev/null; then
  echo "refusing to publish benchmark report; possible personal identifier found" >&2
  grep -E -i '(/Users/|file://|MacBook|basarhamdionat|sonofdeus|bhonat@|users\.noreply|[[:alnum:]._%+-]+@[[:alnum:].-]+\.[[:alpha:]]{2,})' "$out" >&2 || true
  exit 1
fi
