# API compatibility notes

## Producer endpoint

```http
POST /topics/{topic}
```

```http
POST /topics/{topic}/partitions/{partition}
```

The partition endpoint mirrors Confluent REST Proxy v2's partition producer
resource. The path partition is applied to every decoded record.

## v3 producer endpoints

The producer-only v3 surface is documented in
[`api/v3/openapi.yaml`](../api/v3/openapi.yaml).

```http
POST /v3/clusters/{cluster_id}/topics/{topic_name}/records
```

```http
POST /v3/clusters/{cluster_id}/topics/{topic_name}/records:batch
```

`cluster_id` must match `KAFKA_CLUSTER_ID`, which defaults to `local`.

The single-record endpoint accepts one JSON producer request or a concatenated
stream of JSON producer requests and returns newline-delimited delivery reports.
The batch endpoint accepts:

```json
{
  "entries": [
    {
      "id": "record-1",
      "partition_id": 0,
      "key": {
        "type": "STRING",
        "data": "customer-123"
      },
      "value": {
        "type": "JSON",
        "data": {
          "order_id": "order-456"
        }
      }
    }
  ]
}
```

and returns `successes[]` and `failures[]` using the same per-record Kafka
delivery result model.

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

## Schema-aware v2 requests

Schema-aware producer media types are supported for the producer path:

```text
application/vnd.kafka.avro.v2+json
application/vnd.kafka.protobuf.v2+json
application/vnd.kafka.jsonschema.v2+json
```

The request can provide a schema id, schema text, or resolve the configured
subject through Schema Registry:

```json
{
  "value_schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}",
  "records": [
    {
      "key": "customer-123",
      "value": {
        "id": "order-456"
      }
    }
  ]
}
```

Schema-aware records are encoded with the Confluent wire-format prefix before
being produced to Kafka. Avro uses Avro binary encoding, JSON Schema validates
and stores compact JSON bytes, and Protobuf uses binary protobuf encoding for
the first message in the supplied schema.

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

The producer surface is intentionally close rather than byte-for-byte exact:

- Error bodies are aligned for captured JSON/binary cases but not exhaustively code-compatible.
- Complex Schema Registry reference/import graphs are not implemented yet.
- Header compatibility should be validated against the exact Confluent REST Proxy version in use.
- Numeric JSON semantics should be validated against client expectations.
