# HED Fabric performance profiles

HED keeps Fabric configuration explicit and reproducible. These profiles are starting points for benchmarking, not claims of optimal production settings.

## Profiles

- `low-latency.yaml`: smaller batches and short timeout.
- `balanced.yaml`: general-purpose throughput/latency trade-off.
- `high-throughput.yaml`: larger batches and longer timeout for sustained load.
- `extreme-throughput.yaml`: stress profile for controlled benchmark environments.

Run the same workload against each profile and record:

- logical HED TPS
- committed Fabric TPS
- p50/p95/p99/p99.9 latency
- block size and transaction count
- endorsement, ordering, validation and commit latency
- peer/orderer CPU, memory and network

Do not compare HED logical TPS directly with Fabric committed TPS: batching intentionally makes one Fabric transaction represent multiple logical operations.
