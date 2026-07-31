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
