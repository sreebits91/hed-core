package hlf

import (
    "sync"
    "testing"

    "hed-core/pkg/engine"
)

func TestHLFCommitterConcurrentSubmitAndStop(t *testing.T) {
    c := NewHLFCommitter(BatchConfig{MaxBatchSize: 32, WorkerCount: 4})
    var wg sync.WaitGroup
    for i := 0; i < 16; i++ {
        wg.Add(1)
        go func(i int) {
            defer wg.Done()
            for j := 0; j < 1000; j++ {
                c.SubmitTx(&engine.TxPayload{TxUUID: engine.GenerateUUID(), AccountID: "acc", Amount: int64(i + j)})
            }
        }(i)
    }
    wg.Add(1)
    go func() { defer wg.Done(); c.Stop() }()
    wg.Wait()

    // Stop is idempotent and no producer may panic after shutdown.
    c.Stop()
    if c.TotalCommitted()+c.TotalFailed() == 0 { t.Fatal("expected submitted work to be accounted for") }
}
