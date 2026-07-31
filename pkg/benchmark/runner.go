package benchmark

import (
	"context"
	"fmt"
	"sync"
	"time"

	"hed-core/pkg/delta"
	"hed-core/pkg/router"
)

type Metrics struct {
	TPS             float64
	TotalTxs        uint64
	ActiveWorkerNum int
	ActiveChannels  int
}

type BenchRunner struct {
	Engine   *delta.DeltaEngine
	Router   *router.GatewayRouter
	Workers  int
	Channels int
}

func NewRunner(engine *delta.DeltaEngine, workers, channels int) *BenchRunner {
	return &BenchRunner{
		Engine:   engine,
		Router:   router.NewGatewayRouter(channels),
		Workers:  workers,
		Channels: channels,
	}
}

func (b *BenchRunner) Run(ctx context.Context, metricsChan chan<- Metrics) {
	var wg sync.WaitGroup
	b.Engine.ResetTxCount()

	for i := 0; i < b.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			accountID := fmt.Sprintf("account_%d", workerID%1000)
			channelName := b.Router.RouteToShard(accountID)

			for {
				select {
				case <-ctx.Done():
					return
				default:
					b.Engine.ApplyDelta(channelName, accountID, 10)
				}
			}
		}(i)
	}

	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	var lastCount uint64
	lastTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case t := <-ticker.C:
			currentCount := b.Engine.GetTxCount()
			duration := t.Sub(lastTime).Seconds()
			tps := float64(currentCount-lastCount) / duration

			lastCount = currentCount
			lastTime = t

			metricsChan <- Metrics{
				TPS:             tps,
				TotalTxs:        currentCount,
				ActiveWorkerNum: b.Workers,
				ActiveChannels:  b.Channels,
			}
		}
	}
}
