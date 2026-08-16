package delta

import (
	"context"
	"strconv"
	"time"

	"hed-core/pkg/plugin"
)

type TxPayload struct {
	Key   string
	Value string
}

type Aggregator struct {
	txChan chan TxPayload
	engine plugin.StateEngine
}

func NewAggregator(engine plugin.StateEngine, bufferSize int) *Aggregator {
	return &Aggregator{
		txChan: make(chan TxPayload, bufferSize),
		engine: engine,
	}
}

func (a *Aggregator) Submit(key, val string) {
	a.txChan <- TxPayload{Key: key, Value: val}
}

func (a *Aggregator) StartFlushers(ctx context.Context, numFlushers int) {
	for i := 0; i < numFlushers; i++ {
		go func() {
			batch := make(map[string]int64, 1000)
			ticker := time.NewTicker(1 * time.Millisecond)
			defer ticker.Stop()

			flush := func() {
				if len(batch) == 0 {
					return
				}
				if err := a.engine.BatchWrite("channel1", batch); err != nil {
					return
				}
				batch = make(map[string]int64, 1000)
			}

			for {
				select {
				case <-ctx.Done():
					flush()
					return
				case tx := <-a.txChan:
					value, err := strconv.ParseInt(tx.Value, 10, 64)
					if err != nil {
						continue
					}
					batch[tx.Key] += value
					if len(batch) >= 1000 {
						flush()
					}
				case <-ticker.C:
					flush()
				}
			}
		}()
	}
}
