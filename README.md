# hed-core

hed-core is a benchmark-oriented Hyperledger Fabric control plane for exercising a high-throughput transaction pipeline locally. It combines a dashboard, a sharded committer path, and a Fabric lifecycle bootstrap flow so the UI can surface readiness and throughput while the benchmark is running.

## What it does

- Starts a local web dashboard on port 8080.
- Exposes live metrics over Server-Sent Events at /api/metrics.
- Uses a sharded in-memory KeyDB engine plus a parallel committer path for benchmark traffic.
- Boots a Fabric deployment lifecycle through the existing Fabric deployer implementation.
- Marks the benchmark as Fabric-ready once the deploy flow has been triggered.


## Run locally

```bash
go run ./cmd/main.go
```

Then open:

- http://localhost:8080/
- http://localhost:8080/api/metrics
- http://localhost:8080/api/hlf/telemetry

## Key components

- cmd/main.go: application entrypoint.
- pkg/dashboard/server.go: metrics dashboard and benchmark loop.
- pkg/dashboard/hlf_server.go: Fabric lifecycle state and readiness flags.
- pkg/hlf/deployer.go: Fabric bootstrap/deployment workflow.
- pkg/hlf/committer.go: high-throughput transaction submitter.

## Current benchmark behavior

The dashboard emits live metrics including:

- TPS
- total transactions
- average DB call latency
- average transaction latency
- worker/channel configuration
- Fabric readiness state

The benchmark remains a local benchmark harness, but it now uses the Fabric deployment lifecycle as its readiness gate.

## Verification

You can verify the current state with:

```bash
go test ./pkg/dashboard ./pkg/hlf
```

And confirm the live metrics endpoint responds:

```bash
curl http://127.0.0.1:8080/api/metrics
```
