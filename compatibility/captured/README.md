# Captured Confluent REST Proxy behavior

This directory is for JSON reports captured from a live Confluent REST Proxy.

Generate a fresh producer edge-case capture with:

```bash
make capture-confluent-fixtures
```

The capture utility records the request shape and the exact response status,
content type and body returned by Confluent REST Proxy for edge cases such as
bad media types, malformed JSON, invalid partitions and binary decode errors.

Large request bodies are truncated in the report so captured fixtures stay
reviewable.
