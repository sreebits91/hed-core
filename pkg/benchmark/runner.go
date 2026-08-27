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
	if totalTx <= 0 || numWorkers <= 0 {
		return Result{WorkerCount: numWorkers}
	}
	var wg sync.WaitGroup
	workerKeys := make([][]string, numWorkers)
	base := totalTx / numWorkers
	remainder := totalTx % numWorkers
	for w := 0; w < numWorkers; w++ {
		count := base
		if w < remainder { count++ }
		workerKeys[w] = make([]string, count)
		for i := range workerKeys[w] {
			workerKeys[w][i] = fmt.Sprintf("acc_%d_%d", w, i%100)
		}
	}

	dEngine.ResetTxCount()
	startTime := time.Now()
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for _, key := range workerKeys[workerID] {
				dEngine.ApplyDelta("channel1", key, 10)
			}
		}(w)
	}
	wg.Wait()
	duration := time.Since(startTime)
	totalCommitted := dEngine.GetTxCount()
	return Result{TotalTx: totalCommitted, Duration: duration, TPS: float64(totalCommitted) / duration.Seconds(), WorkerCount: numWorkers}
}
