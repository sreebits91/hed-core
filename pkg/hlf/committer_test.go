package hlf

import (
	"testing"
	"time"

	"hed-core/pkg/engine"
)

func TestHLFCommitterBatchesAndStops(t *testing.T) {
	c := NewHLFCommitter(BatchConfig{MaxBatchSize: 64, FlushTimeout: time.Millisecond, WorkerCount: 4, QueueSize: 20000})

	const total = 10000
	for i := 0; i < total; i++ {
		tx := &engine.TxPayload{TxUUID: engine.GenerateUUID(), AccountID: "acc", Amount: int64(i)}
		if !c.SubmitTx(tx) {
			t.Fatalf("transaction %d was rejected", i)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for c.TotalCommitted() < total && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := c.TotalCommitted(); got != total {
		t.Fatalf("committed=%d, want=%d", got, total)
	}
	if got := c.TotalDropped(); got != 0 {
		t.Fatalf("dropped=%d, want=0", got)
	}

	c.Stop()
	if c.SubmitTx(&engine.TxPayload{TxUUID: "after-stop"}) {
		t.Fatal("SubmitTx accepted transaction after Stop")
	}
}
