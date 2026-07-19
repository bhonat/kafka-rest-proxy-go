#!/usr/bin/env bash
set -euo pipefail

curl -sS \
  -X POST "http://localhost:8080/topics/binary-events" \
  -H "Content-Type: application/vnd.kafka.binary.v2+json" \
  -H "Accept: application/vnd.kafka.v2+json" \
  -d '{
    "records": [
      {
        "key": "Y3VzdG9tZXItMTIz",
        "value": "aGVsbG8td29ybGQ="
      }
    ]
  }'
