#!/usr/bin/env bash
set -euo pipefail

base_url="${BASE_URL:-http://localhost:8080}"
topic="${TOPIC:-orders}"

wait_for_endpoint() {
  local endpoint="$1"
  local attempts="${2:-120}"

  for _ in $(seq 1 "${attempts}"); do
    if curl -fsS "${base_url}${endpoint}" >/dev/null; then
      return 0
    fi
    sleep 1
  done

  echo "Timed out waiting for ${base_url}${endpoint}" >&2
  return 1
}

wait_for_endpoint /healthz
wait_for_endpoint /readyz

curl -fsS \
  -X POST "${base_url}/topics/${topic}" \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  -H "Accept: application/vnd.kafka.v2+json" \
  -d '{"records":[{"key":"compose-smoke","value":{"source":"compose-smoke","ok":true}}]}'
