package hlf

import (
	"testing"
	"time"

	"hed-core/pkg/engine"
)

func TestHLFCommitterConcurrentSubmitAndStop(t *testing.T) {
	c := NewHLFCommitter(BatchConfig{
		MaxBatchSize: 256,
		FlushTimeout: time.Millisecond,
		WorkerCount:  8,
		QueueSize:    100000,
	})

	const producers = 16
	const perProducer = 5000
	accepted := make(chan int, producers)
	done := make(chan struct{})

	for p := 0; p < producers; p++ {
		go func(p int) {
			count := 0
			for i := 0; i < perProducer; i++ {
				tx := &engine.TxPayload{TxUUID: engine.GenerateUUID(), AccountID: "stress", Amount: int64(p*perProducer + i)}
				if c.SubmitTx(tx) {
					count++
				}
			}
			accepted <- count
		}(p)
	}

	totalAccepted := 0
	for i := 0; i < producers; i++ {
		totalAccepted += <-accepted
	}
	close(done)

	deadline := time.Now().Add(5 * time.Second)
	for c.TotalCommitted() < uint64(totalAccepted) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	if got := c.TotalCommitted(); got != uint64(totalAccepted) {
		t.Fatalf("committed=%d, accepted=%d, dropped=%d", got, totalAccepted, c.TotalDropped())
	}
	c.Stop()
}
