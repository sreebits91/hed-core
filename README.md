# hed-core

HED (HyperEngine-Drunix) is a benchmark-oriented Hyperledger Fabric control plane for a high-throughput transaction pipeline.

## Architecture

This branch contains two layers:

1. **Stable HED v1 committer** — retained as the compatibility and performance baseline.
2. **HED v2 pipeline** — partitioned ingress, bounded queues, batchers, commit backend abstraction, ordering primitives, deduplication, metrics, and WAL/replay primitives.

```text
Ingress -> Deduplication -> Deterministic Partition Router
                              |       |       |
                              v       v       v
                           Queue -> Batcher -> Commit Backend
                              |
                              +---- N independent partitions

                    WAL / replay + metrics
```

## Key components

- `pkg/hlf/committer.go` — stable v1 high-throughput committer.
- `pkg/queue/` — queue abstraction and bounded ring implementation.
- `pkg/partition/` — deterministic transaction partitioning.
- `pkg/batch/` — partition-local batch collection.
- `pkg/commit/` — backend interface for ledger/Fabric integration.
- `pkg/dedup/` — transaction-id duplicate protection.
- `pkg/ordering/` — monotonic sequence generation.
- `pkg/recovery/` — append/sync/replay WAL primitive.
- `pkg/metrics/` — atomic pipeline counters.
- `pkg/pipeline/` — integrated partitioned pipeline.
- `pkg/dashboard/` — live benchmark and Fabric readiness dashboard.

## Benchmarking

The benchmark ladder remains 100K, 250K, 500K, 750K, 1M, 2M and 5M transactions. Committer-path TPS measures the HED pipeline boundary; true end-to-end Fabric TPS must include endorsement, ordering, validation and ledger persistence.

## Verification

```bash
go vet ./...
go test ./...
go test -race ./...
```

The profiling workflow additionally produces CPU, heap/allocation, mutex and block profiles for the selected load level.

## Local run

```bash
go run ./cmd/main.go
```

Then open `http://localhost:8080/` for the dashboard and `/api/metrics` for live metrics.
