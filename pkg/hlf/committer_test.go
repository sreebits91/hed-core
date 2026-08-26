package hlf

import (
	"testing"
	"time"

	"hed-core/pkg/engine"
)

func TestHLFCommitterBatchesAndStops(t *testing.T) {
	const total = 10000
	c := NewHLFCommitter(BatchConfig{MaxBatchSize: 64, FlushTimeout: time.Millisecond, WorkerCount: 4, QueueSize: total + 1024})
	defer c.Stop()

	deadline := time.Now().Add(5 * time.Second)
	for i := 0; i < total; i++ {
		tx := &engine.TxPayload{TxUUID: engine.GenerateUUID(), AccountID: "acc", Amount: int64(i)}
		for !c.SubmitTx(tx) {
			if time.Now().After(deadline) {
				t.Fatalf("transaction %d could not be accepted before timeout", i)
			}
			time.Sleep(100 * time.Microsecond)
		}
	}

	for c.TotalCommitted() < total && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := c.TotalCommitted(); got != total {
		t.Fatalf("committed=%d, want=%d", got, total)
	}
	if got := c.TotalDropped(); got == 0 {
		t.Log("no transient queue drops observed during this run")
	}

	c.Stop()
	if c.SubmitTx(&engine.TxPayload{TxUUID: "after-stop"}) {
		t.Fatal("SubmitTx accepted transaction after Stop")
	}
}
