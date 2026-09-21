package hlf

import (
	"context"
	"errors"
	"sync/atomic"
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


func TestHLFCommitterCommitBoundaryFailure(t *testing.T) {
	c := NewHLFCommitter(BatchConfig{MaxBatchSize: 4, FlushTimeout: time.Millisecond, WorkerCount: 2, QueueSize: 64})
	var calls atomic.Int64
	c.SetCommitFunc(func(ctx context.Context, batch []*engine.TxPayload) error {
		calls.Add(1)
		return errors.New("fabric unavailable")
	})
	for i := 0; i < 10; i++ {
		if !c.SubmitTx(&engine.TxPayload{TxUUID: engine.GenerateUUID(), AccountID: "acc", Amount: int64(i)}) {
			t.Fatal("transaction unexpectedly rejected")
		}
	}
	deadline := time.Now().Add(time.Second)
	for calls.Load() == 0 && time.Now().Before(deadline) { time.Sleep(time.Millisecond) }
	c.Stop()
	if calls.Load() == 0 { t.Fatal("commit boundary was never invoked") }
	if c.TotalCommitted() != 0 { t.Fatalf("committed=%d after commit failure", c.TotalCommitted()) }
	if c.TotalFailed() == 0 { t.Fatal("failed count did not increase") }
}
