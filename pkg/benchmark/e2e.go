package benchmark

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"hed-core/pkg/engine"
)

// E2EResult records logical transaction outcomes. It intentionally measures
// the supplied sink rather than claiming Fabric durability unless the sink's
// Submit method waits for the Fabric commit event.
type E2EResult struct {
	Transactions uint64
	Succeeded uint64
	Failed uint64
	Elapsed time.Duration
}

// RunE2E drives the real HED pipeline and an optional downstream sink. It uses
// a fixed transaction count and atomically accounts every attempted operation.
func RunE2E(ctx context.Context, p *engine.Pipeline, sink func(context.Context, *engine.TxPayload) error, transactions, workers int) E2EResult {
	if transactions < 0 { transactions = 0 }
	if workers < 1 { workers = 1 }
	start := time.Now()
	var next uint64
	var ok, failed uint64
	var wg sync.WaitGroup
	for i:=0; i<workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				idx := atomic.AddUint64(&next, 1)-1
				if idx >= uint64(transactions) { return }
				tx := &engine.TxPayload{TxUUID: engine.GenerateUUID(), AccountID: "bench-"+itoa(idx), Amount: int64(idx%100)+1}
				if _, _, err := p.SubmitTransaction(tx); err != nil { atomic.AddUint64(&failed,1); continue }
				if sink != nil {
					if err := sink(ctx, tx); err != nil { atomic.AddUint64(&failed,1); continue }
				}
				atomic.AddUint64(&ok,1)
			}
		}(i)
	}
	wg.Wait()
	return E2EResult{Transactions:uint64(transactions), Succeeded:ok, Failed:failed, Elapsed:time.Since(start)}
}

func itoa(v uint64) string {
	if v==0 { return "0" }
	var b [20]byte; i:=len(b)
	for v>0 { i--; b[i]=byte('0'+v%10); v/=10 }
	return string(b[i:])
}
