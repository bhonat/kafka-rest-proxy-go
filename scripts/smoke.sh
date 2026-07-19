#!/usr/bin/env bash
set -euo pipefail

base_url="${BASE_URL:-http://localhost:8080}"
topic="${TOPIC:-orders}"

curl -fsS "${base_url}/healthz" >/dev/null
curl -fsS "${base_url}/readyz" >/dev/null

curl -fsS \
  -X POST "${base_url}/topics/${topic}" \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  -H "Accept: application/vnd.kafka.v2+json" \
  -d '{"records":[{"key":"smoke","value":{"ok":true}}]}'
