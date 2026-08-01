# hed-core

hed-core is a benchmark-oriented Hyperledger Fabric control plane for exercising a high-throughput transaction pipeline locally. It combines a dashboard, a sharded committer path, and a Fabric lifecycle bootstrap flow so the UI can surface readiness and throughput while the benchmark is running.

## What it does

- Starts a local web dashboard on port 8080.
- Exposes live metrics over Server-Sent Events at /api/metrics.
- Uses a sharded in-memory KeyDB engine plus a parallel committer path for benchmark traffic.
- Boots a Fabric deployment lifecycle through the existing Fabric deployer implementation.
- Marks the benchmark as Fabric-ready once the deploy flow has been triggered.

<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1000 920" width="100%" height="100%" style="background-color: #0b0f19; font-family: 'Inter', system-ui, -apple-system, sans-serif;">
  <defs>
    <!-- Modern Card Backgrounds -->
    <linearGradient id="bgGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#0b0f19"/>
      <stop offset="100%" stop-color="#111827"/>
    </linearGradient>

    <!-- Glowing Node Gradients -->
    <linearGradient id="clientGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#3b82f6"/><stop offset="100%" stop-color="#1d4ed8"/>
    </linearGradient>
    <linearGradient id="routerGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#0284c7"/><stop offset="100%" stop-color="#0369a1"/>
    </linearGradient>
    <linearGradient id="runtimeGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#0f766e"/><stop offset="100%" stop-color="#115e59"/>
    </linearGradient>
    <linearGradient id="storageGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#059669"/><stop offset="100%" stop-color="#065f46"/>
    </linearGradient>
    <linearGradient id="govGrad" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" stop-color="#6366f1"/><stop offset="100%" stop-color="#4338ca"/>
    </linearGradient>

    <!-- Drop Shadows for Glass Effect -->
    <filter id="shadow" x="-10%" y="-10%" width="120%" height="120%">
      <feDropShadow dx="0" dy="8" stdDeviation="6" flood-color="#000000" flood-opacity="0.5"/>
    </filter>
    <filter id="glow" x="-20%" y="-20%" width="140%" height="140%">
      <feGaussianBlur stdDeviation="4" result="blur"/>
      <feComposite in="SourceGraphic" in2="blur" operator="over"/>
    </filter>

    <!-- Arrow Heads -->
    <marker id="arrow" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
      <path d="M 0 0 L 10 5 L 0 10 z" fill="#38bdf8"/>
    </marker>
    <marker id="arrowDashed" viewBox="0 0 10 10" refX="6" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
      <path d="M 0 0 L 10 5 L 0 10 z" fill="#34d399"/>
    </marker>
  </defs>

  <style>
    .layer-title { fill: #f8fafc; font-size: 13px; font-weight: 700; letter-spacing: 1px; text-transform: uppercase; }
    .node-title { fill: #ffffff; font-size: 13px; font-weight: 700; }
    .node-sub { fill: #e2e8f0; font-size: 11px; opacity: 0.9; }
    .node-bullet { fill: #94a3b8; font-size: 11px; }
    .label { fill: #38bdf8; font-size: 11px; font-weight: 600; text-anchor: middle; }
    .line { stroke: #0284c7; stroke-width: 2; fill: none; marker-end: url(#arrow); }
    .dash-line { stroke: #34d399; stroke-width: 2; stroke-dasharray: 4,4; fill: none; marker-end: url(#arrowDashed); }
    .layer-box { fill: #1e293b; fill-opacity: 0.4; stroke: #334155; stroke-width: 1.5; rx: 12; }
    .node { rx: 8; stroke-width: 1.5; filter: url(#shadow); }
  </style>

  <rect width="100%" height="100%" fill="url(#bgGrad)"/>

  <!-- ================= LAYER 1 ================= -->
  <rect class="layer-box" x="40" y="20" width="920" height="250"/>
  <text class="layer-title" x="60" y="48" fill="#38bdf8">Layer 1: Ingress &amp; Dynamic Sharding Gateway</text>
  
  <rect class="node" x="350" y="65" width="300" height="42" fill="url(#clientGrad)" stroke="#60a5fa"/>
  <text class="node-title" x="500" y="91" text-anchor="middle">Client REST / gRPC API Requests</text>

  <rect class="node" x="250" y="132" width="500" height="45" fill="url(#routerGrad)" stroke="#38bdf8"/>
  <text class="node-title" x="500" y="151" text-anchor="middle">HED Gateway Router &amp; Load Balancer</text>
  <text class="node-sub" x="500" y="166" text-anchor="middle">(Murmur3 Consistent Hash Partitioning)</text>

  <!-- Sub-shards -->
  <rect class="layer-box" x="60" y="195" width="880" height="60" stroke="#1e293b"/>
  <text class="node-bullet" x="75" y="210">Sub-Channel Shards</text>
  
  <rect class="node" x="90" y="215" width="230" height="32" fill="url(#routerGrad)" stroke="#38bdf8"/>
  <text class="node-title" x="205" y="235" text-anchor="middle">Shard Channel #01</text>
  
  <rect class="node" x="385" y="215" width="230" height="32" fill="url(#routerGrad)" stroke="#38bdf8"/>
  <text class="node-title" x="500" y="235" text-anchor="middle">Shard Channel #02</text>

  <rect class="node" x="680" y="215" width="230" height="32" fill="url(#routerGrad)" stroke="#38bdf8"/>
  <text class="node-title" x="795" y="235" text-anchor="middle">Shard Channel #N</text>

  <!-- ================= LAYER 2 ================= -->
  <rect class="layer-box" x="40" y="300" width="920" height="180"/>
  <text class="layer-title" x="60" y="328" fill="#2dd4bf">Layer 2: Consolidated Peer Runtime (HED-PEER)</text>

  <rect class="node" x="70" y="345" width="400" height="115" fill="url(#runtimeGrad)" stroke="#2dd4bf"/>
  <text class="node-title" x="270" y="372" text-anchor="middle">Lock-Free Delta-CRDT Engine</text>
  <text class="node-bullet" x="90" y="397">• Thread-Safe Atomic Buffer</text>
  <text class="node-bullet" x="90" y="417">• Relative Mutations (Delta = +/-v)</text>
  <text class="node-bullet" x="90" y="437">• High-Concurrency In-Memory Aggregation</text>

  <rect class="node" x="530" y="345" width="400" height="115" fill="url(#runtimeGrad)" stroke="#2dd4bf"/>
  <text class="node-title" x="730" y="372" text-anchor="middle">Stateless Parallel Validator</text>
  <text class="node-bullet" x="550" y="397">• Multi-Threaded Signature Verification</text>
  <text class="node-bullet" x="550" y="417">• Policy &amp; Dependency Check</text>
  <text class="node-bullet" x="550" y="437">• Zero-Lock Access Validation</text>

  <!-- ================= BOTTOM SECTION ================= -->
  <!-- LAYER 3 -->
  <rect class="layer-box" x="40" y="510" width="445" height="380"/>
  <text class="layer-title" x="60" y="538" fill="#34d399">Layer 3: Dual-Tier Storage (Go Engine)</text>

  <rect class="node" x="65" y="560" width="395" height="130" fill="url(#storageGrad)" stroke="#34d399"/>
  <text class="node-title" x="262" y="587" text-anchor="middle">Tier 1: In-Memory Hot Path (KeyDB)</text>
  <text class="node-bullet" x="85" y="612">• Sub-millisecond State Execution</text>
  <text class="node-bullet" x="85" y="632">• 100k - 400k+ TPS Throughput</text>
  <text class="node-bullet" x="85" y="652">• Immediate Balance Verification</text>

  <rect class="node" x="65" y="735" width="395" height="130" fill="url(#storageGrad)" stroke="#34d399"/>
  <text class="node-title" x="262" y="762" text-anchor="middle">Tier 2: Async SQL Audit Ledger (YugabyteDB)</text>
  <text class="node-bullet" x="85" y="787">• Linearizable Persistence &amp; SQL Queries</text>
  <text class="node-bullet" x="85" y="807">• Compliance, Reporting &amp; Audit Logs</text>
  <text class="node-bullet" x="85" y="827">• Distributed Multi-Row Batch Inserts</text>

  <!-- LAYER 4 -->
  <rect class="layer-box" x="515" y="510" width="445" height="380"/>
  <text class="layer-title" x="535" y="538" fill="#a5b4fc">Layer 4: Consensus &amp; Governance</text>

  <rect class="node" x="540" y="560" width="395" height="305" fill="url(#govGrad)" stroke="#818cf8"/>
  <text class="node-title" x="737" y="597" text-anchor="middle">Drunix / Hyperledger Governance</text>
  <text class="node-bullet" x="565" y="635">• X.509 Certificate Authority &amp; MSP Identity</text>
  <text class="node-bullet" x="565" y="670">• Raft / SmartBFT Consensus Protocol</text>
  <text class="node-bullet" x="565" y="705">• Multi-Organization Permissioning</text>
  <text class="node-bullet" x="565" y="740">• Global State Block Ordering</text>

  <!-- ================= CONNECTORS ================= -->
  <path class="line" d="M 500,107 L 500,132"/>
  <text class="label" x="500" y="123">1. Submit Tx</text>

  <path class="line" d="M 350,177 L 205,215"/>
  <path class="line" d="M 500,177 L 500,215"/>
  <path class="line" d="M 650,177 L 795,215"/>

  <path class="line" d="M 205,247 L 270,345"/>
  <path class="line" d="M 500,247 L 270,345"/>
  <path class="line" d="M 795,247 L 270,345"/>
  <text class="label" x="270" y="280">2. Parallel Ingestion</text>

  <path class="line" d="M 470,402 L 530,402"/>
  <text class="label" x="500" y="394">Validation</text>

  <path class="line" d="M 270,460 L 262,560"/>
  <text class="label" x="266" y="500">3. Atomic State Update</text>

  <path class="dash-line" d="M 262,690 L 262,735"/>
  <text class="label" x="330" y="715" fill="#34d399">4. Async Batch Flush</text>

  <path class="line" d="M 380,460 L 737,560"/>
  <text class="label" x="590" y="500">5. Block Ordering</text>
</svg>


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
