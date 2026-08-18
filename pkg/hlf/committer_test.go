package hlf

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"hed-core/pkg/engine"
)

type recordingSink struct {
	mu       sync.Mutex
	batches  int
	txs      int
	fail     bool
}

func (s *recordingSink) Commit(_ context.Context, batch []*engine.TxPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("sink failure")
	}
	s.batches++
	s.txs += len(batch)
	return nil
}

func (s *recordingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.txs
}

func tx(id int) *engine.TxPayload {
	return &engine.TxPayload{TxUUID: GenerateUUID(), AccountID: "account", Amount: int64(id)}
}

func TestHLFCommitterStopDrainsAcceptedTransactions(t *testing.T) {
	sink := &recordingSink{}
	c := NewHLFCommitterWithSink(BatchConfig{
		MaxBatchSize: 8,
		FlushTimeout: time.Hour,
		WorkerCount:  4,
	}, sink)

	const total = 100
	accepted := 0
	for i := 0; i < total; i++ {
		if c.SubmitTx(tx(i)) {
			accepted++
		}
	}

	c.Stop()

	if got := c.TotalCommitted(); got != uint64(accepted) {
		t.Fatalf("committed=%d, accepted=%d", got, accepted)
	}
	if got := sink.count(); got != accepted {
		t.Fatalf("sink received=%d, accepted=%d", got, accepted)
	}
	if c.SubmitTx(tx(total)) {
		t.Fatal("submission succeeded after Stop")
	}
}

func TestHLFCommitterConcurrentSubmitAndStop(t *testing.T) {
	c := NewHLFCommitter(BatchConfig{
		MaxBatchSize: 32,
		FlushTimeout: time.Millisecond,
		WorkerCount:  8,
	})

	var accepted atomic.Uint64
	var wg sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				if c.SubmitTx(tx(worker*500 + i)) {
					accepted.Add(1)
				}
			}
		}(worker)
	}

	go func() {
		time.Sleep(2 * time.Millisecond)
		c.Stop()
	}()

	wg.Wait()
	c.Stop()

	if c.TotalCommitted() != accepted.Load() {
		t.Fatalf("committed=%d, accepted=%d", c.TotalCommitted(), accepted.Load())
	}
}

func TestHLFCommitterCountsSinkFailures(t *testing.T) {
	sink := &recordingSink{fail: true}
	c := NewHLFCommitterWithSink(BatchConfig{
		MaxBatchSize: 4,
		FlushTimeout: time.Hour,
		WorkerCount:  1,
	}, sink)

	for i := 0; i < 4; i++ {
		if !c.SubmitTx(tx(i)) {
			t.Fatal("expected submission to be accepted")
		}
	}
	c.Stop()

	if c.TotalCommitted() != 0 {
		t.Fatalf("committed=%d, want 0", c.TotalCommitted())
	}
	if c.TotalFailed() != 4 {
		t.Fatalf("failed=%d, want 4", c.TotalFailed())
	}
}
