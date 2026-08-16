# HED benchmark

Run the local core/committer benchmark with a 100K TPS target:

```bash
go run ./cmd/benchmark -tps 100000 -duration 10s -workers 64 -batch 2000 -flush 10ms
```

Write machine-readable output:

```bash
go run ./cmd/benchmark -tps 100000 -duration 30s -workers 128 -batch 2000 -flush 10ms -json benchmark.json
```

The harness reports accepted, committed and rejected transactions plus actual TPS and p50/p95/p99 submission latency.

**Important:** this benchmark measures HED's local committer path. It does not claim durable KeyDB/Yugabyte/Fabric throughput. Those backends require separate integration benchmarks with the real services enabled.
