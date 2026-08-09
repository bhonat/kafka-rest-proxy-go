# Performance regression process

The benchmark suite compares KafkaRestProxy-Go against Confluent REST Proxy under
the same Kafka cluster, payload, batching, and client-concurrency settings.

## Required comparison dimensions

- target: Go proxy and Confluent REST Proxy;
- payload format: JSON and binary;
- payload sizes: 128 B, 512 B, 1 KiB, 10 KiB;
- records/request: 1, 10, 100, 1000;
- clients: 4, 16, 64, 256;
- compression label: runtime-configured;
- acks label: runtime-configured.

## Acceptance gates

For a production candidate:

- request failure rate must be 0% during steady state;
- record failure rate must be 0% during steady state;
- p99 must stay within the documented SLO for the workload;
- throughput per CPU-equivalent node must beat or match Confluent for the target
  workload before replacing Confluent for that workload.

## Commands

```bash
make compose-up-comparison
make bench-compare
make test-soak
make test-load-integration
```

For CI/manual workflow runs, see `.github/workflows/benchmark-regression.yml`.
