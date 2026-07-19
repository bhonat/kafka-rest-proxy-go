#!/usr/bin/env bash
set -euo pipefail

curl -sS \
  -X POST "http://localhost:8080/topics/orders" \
  -H "Content-Type: application/vnd.kafka.json.v2+json" \
  -H "Accept: application/vnd.kafka.v2+json" \
  -d '{
    "records": [
      {
        "key": "customer-123",
        "value": {
          "order_id": "order-456",
          "amount": 42
        }
      }
    ]
  }'
