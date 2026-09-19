# HED v2 Stability Contract

## Ordering

HED v2 guarantees FIFO ordering **within each partition**. The router deterministically maps a transaction key to exactly one partition. Each partition has one drain worker, and sequence numbers are assigned under the ingress mutex. There is intentionally no global ordering guarantee; requiring one global sequence would serialize otherwise independent partitions and defeat the scaling architecture.

Recovery preserves partition and sequence metadata. WAL replay is sorted by `(partition, sequence)` and returns only transactions without a corresponding commit record.

## Backpressure

- `NORMAL`: queue depth < 50%
- `BUSY`: queue depth >= 50% and < 80%
- `SATURATED`: queue depth >= 80% and < 100%
- `REJECTING`: queue is full, invalid, stopped, or otherwise unable to accept work

Rejections are explicit: `QUEUE_FULL`, `ENGINE_STOPPED`, `INVALID_TRANSACTION`, and `DUPLICATE_TRANSACTION`.

## Durability

The WAL uses prepare and commit records with CRC32 checksums. `SyncWAL=true` calls `fsync` after each record. Replay ignores a final truncated record but fails on a complete malformed or checksum-invalid record. Only prepared transactions lacking a commit record are replayed.

## Deduplication

Deduplication is bounded by capacity and expires entries after a TTL. A rejected enqueue or WAL append removes the provisional dedup entry so a caller can retry. The WAL is the durable source of truth for uncommitted work after a process restart.

## Commit semantics

Commits have a configurable maximum attempt count, exponential backoff and per-attempt timeout. `PermanentError` bypasses retries. The same HED transaction ID is sent to a real Fabric backend as the chaincode idempotency key. The Fabric chaincode must persist and reject an already processed HED ID so a timeout after orderer submission cannot cause a duplicate business operation.

## Shutdown

Stop first closes admission while holding the submission mutex, then closes every partition queue and waits for all partition workers to drain. No worker is left reading a queue after shutdown returns.

## Observability

The v2 metrics snapshot exposes per-partition accepted/committed/failed/retry/rejection counts, queue depth, last batch size, last commit latency, ordering errors, approximate partition TPS, rejection reasons, and recovery count.

## Qualification ladder

The CI workflow runs static analysis, unit tests, race tests, repeated concurrency tests, queue/channel benchmarks and partition benchmarks. A manual qualification dispatch runs the same ingress path at:

`100K -> 250K -> 500K -> 1M -> 2M -> 5M`

Fabric qualification remains a separate environment-gated stage because it requires a live Fabric Gateway, identities, TLS material, peers, orderer and ledger commit events.
