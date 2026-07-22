# API compatibility notes

## Producer endpoint

```http
POST /topics/{topic}
```

## JSON request

```http
Content-Type: application/vnd.kafka.json.v2+json
Accept: application/vnd.kafka.v2+json
```

```json
{
  "records": [
    {
      "key": "customer-123",
      "value": {
        "order_id": "order-456"
      },
      "partition": 2
    }
  ]
}
```

For JSON media types, `key` and `value` are compacted JSON bytes before being sent to Kafka.

## Binary request

```http
Content-Type: application/vnd.kafka.binary.v2+json
Accept: application/vnd.kafka.v2+json
```

```json
{
  "records": [
    {
      "key": "Y3VzdG9tZXItMTIz",
      "value": "aGVsbG8td29ybGQ="
    }
  ]
}
```

For binary media types, `key` and `value` must be base64 strings or `null`.

## Response

```json
{
  "offsets": [
    {
      "partition": 2,
      "offset": 91823,
      "error_code": null,
      "error": null
    }
  ],
  "key_schema_id": null,
  "value_schema_id": null
}
```

The response order is aligned with the input `records[]` order.

Kafka-side record failures are returned in the same `offsets[]` array with HTTP
`200`, matching captured Confluent REST Proxy behavior. Failed records use
`null` for `partition` and `offset`:

```json
{
  "offsets": [
    {
      "partition": 2,
      "offset": 91823,
      "error_code": null,
      "error": null
    },
    {
      "partition": null,
      "offset": null,
      "error_code": 50002,
      "error": "Invalid topics: [bad topic]"
    }
  ],
  "key_schema_id": null,
  "value_schema_id": null
}
```

The MVP maps Kafka/client callback-level record failures to Confluent's producer
record error code `50002`.

## Known MVP gaps

The MVP is intentionally close rather than exact:

- Error bodies are aligned for captured JSON/binary cases but not exhaustively code-compatible.
- Schema-aware media types are not implemented.
- Header compatibility should be validated against the exact Confluent REST Proxy version in use.
- Numeric JSON semantics should be validated against client expectations.
