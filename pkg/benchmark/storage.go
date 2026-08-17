package benchmark

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"hed-core/pkg/plugin"
)

type StorageResult struct {
	Backend      string
	Transactions uint64
	Duration     time.Duration
	TPS          float64
	Workers      int
	BatchSize    int
	ErrorCount   uint64
}

// RunStorageBenchmark measures logical transaction throughput using the
// storage engine's batched write contract. A single atomic work cursor gives
// workers exact, non-overlapping batches, including when totalTx is not evenly
// divisible by workers or batchSize.
func RunStorageBenchmark(ctx context.Context, db plugin.StateEngine, totalTx, workers, batchSize int) StorageResult {
	if totalTx <= 0 || workers <= 0 || batchSize <= 0 || db == nil {
		return StorageResult{Backend: backendName(db), Workers: maxInt(workers, 0), BatchSize: batchSize}
	}
	if workers > totalTx {
		workers = totalTx
	}

	var next uint64
	var completed uint64
	var errors uint64
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				begin := int(atomic.AddUint64(&next, uint64(batchSize)) - uint64(batchSize))
				if begin >= totalTx {
					return
				}
				select {
				case <-ctx.Done():
					return
				default:
				}

			end := begin + batchSize
			if end > totalTx {
				end = totalTx
			}
			updates := make(map[string][]byte, end-begin)
			for id := begin; id < end; id++ {
				updates[fmt.Sprintf("tx-%d", id)] = []byte(fmt.Sprintf("%d", id))
			}
			if err := db.BatchWrite(fmt.Sprintf("bench-%d", worker), updates); err != nil {
				atomic.AddUint64(&errors, uint64(len(updates)))
				continue
			}
			atomic.AddUint64(&completed, uint64(len(updates)))
			}
		}(w)
	}
	wg.Wait()

	duration := time.Since(start)
	tps := 0.0
	if duration > 0 {
		tps = float64(completed) / duration.Seconds()
	}
	return StorageResult{Backend: backendName(db), Transactions: completed, Duration: duration, TPS: tps, Workers: workers, BatchSize: batchSize, ErrorCount: errors}
}

func backendName(db plugin.StateEngine) string {
	if db == nil {
		return "none"
	}
	return db.Name()
}

func RunStorageBenchmarkWithBatches(ctx context.Context, db plugin.StateEngine, totalTx int) StorageResult {
	return RunStorageBenchmark(ctx, db, totalTx, 256, 100)
}
