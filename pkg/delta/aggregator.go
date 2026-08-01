package delta

import (
	"context"
	"time"

	"hed-core/pkg/plugin"
)

type TxPayload struct {
	Key   string
	Value string
}

type Aggregator struct {
	txChan chan TxPayload
	engine plugin.StateEngine // Updated to use StateEngine interface
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
			batch := make(map[string][]byte, 1000)
			ticker := time.NewTicker(1 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case tx := <-a.txChan:
					batch[tx.Key] = []byte(tx.Value)

					// Flush once we reach 1,000 transactions
					if len(batch) >= 1000 {
						_ = a.engine.BatchWrite("channel1", batch)
						batch = make(map[string][]byte, 1000)
					}
				case <-ticker.C:
					// Flush partial batches periodically
					if len(batch) > 0 {
						_ = a.engine.BatchWrite("channel1", batch)
						batch = make(map[string][]byte, 1000)
					}
				}
			}
		}()
	}
}