# hed-core
High-performance, modular extension layer for NPCI Drunix scaling DLT workloads up to 400,000+ TPS using dynamic sharding, lock-free Delta-CRDT, and pluggable storage engines.

[User Clicks "Start Installation"]
              │
              ▼
JavaScript JSON POST payload ───► /api/deploy/start
                                         │
                                         ▼
                             Decodes `hlf.DeployOptions`
                                         │
                                         ▼
                            `go deployer.RunDeployment()`
                                         │
              ┌──────────────────────────┴──────────────────────────┐
              ▼                                                     ▼
   Background Execution                               Live SSE Stream (/api/deploy/stream)
(Stage 1..5 runs in terminal)                     (Broadcasting live terminal logs to UI)

curl -sSL https://bit.ly/2ysbOFE | bash -s -- 2.5.4 1.5.6

go run cmd/main.go

[ Client Request ] ──► [ Async HTTP Handler ] ──( 1. Immediate ACK )──► [ Client (202 Accepted) ]
                             │
                      ( 2. Push to Queue )
                             ▼
                    [ Lock-Free Ring Buffer ]
                             │
                      ( 3. Batch Pull )
                             ▼
                    [ 128 Worker Pool ] ──► [ In-Memory RAM Engine ]


┌────────────────────────┐      ┌───────────────────────────┐      ┌─────────────────────────┐
│ Async Ingestion Engine │ ───> │ Ring Buffer / Worker Pool │ ───> │ State Engine Storage    │
│ (HTTP 202 Immediate)   │      │ (128 Workers / 32 Chans)  │      │ (RAM / YugabyteDB)      │
└───────────┬────────────┘      └─────────────┬─────────────┘      └─────────────────────────┘
            │                                 │
            │                                 ▼
            │                   ┌───────────────────────────┐
            └─────────────────> │ Async Ack Callback Engine │
                                │ (WebSocket / Trace Channel)│
                                └───────────────────────────┘

┌──────────────────────────────────────────────┐
                      │    HyperEngine Worker Pool (128 Workers)     │
                      └──────────────────────┬───────────────────────┘
                                             │
            ┌────────────────────────────────┼────────────────────────────────┐
            ▼                                ▼                                ▼
┌──────────────────────┐        ┌──────────────────────┐        ┌──────────────────────┐
│  Channel Array [0..9]│        │ Channel Array [10..21]│       │Channel Array [22..31]│
└───────────┬──────────┘        └───────────┬──────────┘        └───────────┬──────────┘
            │                               │                               │
            ▼                               ▼                               ▼
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                            Lockless LMAX Disruptor Ring Buffer                       │
└──────────────────────────────────────────────────────────────────────────────────────┘


Client / Studio           HyperEngine API               Go Smart Contract          Fabric Peer / Orderer
       │                         │                               │                         │
       │── 1. Async Submit ────>│                               │                         │
       │   (Tx Payload + UUID)   │                               │                         │
       │<── 2. Immediate Ack ────│                               │                         │
       │    (HTTP 202 Accepted)  │                               │                         │
       │                         │── 3. Batch Submit (Channel) ─>│                         │
       │                         │                               │── 4. Parallel Exec ───>│
       │                         │                               │    (Optimistic Read)    │
       │<── 5. Async Event Stream│                               │                         │
       │    (WebSocket/gRPC)     │<── 6. Tx Committed Context ────│<── 5. Block Commit ─────│
Key Changes




Client / API Endpoint
          │
          │ 1. POST /api/v1/transact (Payload)
          ▼
┌────────────────────────────────────────────────────────┐
│  Ingress API Handler (Zero Allocation)                 │
│  - Generates Google UUID / ULID                        │
│  - Registers callback channel in Trace Context Map     │
│  - Pushes Tx object to High-Speed Ring Buffer          │
│  - Returns HTTP 202 Accepted { tx_id: "uuid-v4..." }   │
└──────────────────────────┬─────────────────────────────┘
                           │
                           │ 2. Ring Buffer Channel (Non-Blocking)
                           ▼
┌────────────────────────────────────────────────────────┐
│  Worker Thread Pool (e.g., 128 Parallel Workers)       │
│  - Batches 1,000 Txs per Micro-Batch (10ms window)      │
│  - Computes State Delta (In-Memory RAM / YugabyteDB)   │
└──────────────────────────┬─────────────────────────────┘
                           │
                           │ 3. Bulk Persist & Acknowledgment
                           ▼
┌────────────────────────────────────────────────────────┐
│  Async Ack Engine & Storage Driver                     │
│  - Bulk upsert to State DB                              │
│  - Signal Context Channel / WebSocket Trace Stream     │
└───────────────────────────────────────────────────────__