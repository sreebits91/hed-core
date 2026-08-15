package benchmark

import (
	"context"
	"fmt"
	"sync"
	"time"

	"hed-core/pkg/delta"
)

type Result struct {
	TotalTx     uint64
	Duration    time.Duration
	TPS         float64
	WorkerCount int
}

func Run256WorkerBenchmark(ctx context.Context, dEngine *delta.DeltaEngine, totalTx int, numWorkers int) Result {
	var wg sync.WaitGroup
	txPerWorker := totalTx / numWorkers

	// Pre-generate keys outside timed window
	workerKeys := make([][]string, numWorkers)
	for w := 0; w < numWorkers; w++ {
		workerKeys[w] = make([]string, txPerWorker)
		for i := 0; i < txPerWorker; i++ {
			workerKeys[w][i] = fmt.Sprintf("acc_%d_%d", w, i%100)
		}
	}

	dEngine.ResetTxCount()
	startTime := time.Now()

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			keys := workerKeys[workerID]

			for i := 0; i < txPerWorker; i++ {
				// Atomically add relative delta amounts (+10 per tx)
				dEngine.ApplyDelta("channel1", keys[i], 10)
			}
		}(w)
	}

	wg.Wait()
	duration := time.Since(startTime)

	totalCommitted := dEngine.GetTxCount()
	tps := float64(totalCommitted) / duration.Seconds()

	return Result{
		TotalTx:     totalCommitted,
		Duration:    duration,
		TPS:         tps,
		WorkerCount: numWorkers,
	}
}
