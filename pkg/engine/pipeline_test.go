package engine

import (
	"sync"
	"testing"
)

func TestPipelineConcurrentSubmitAndEvents(t *testing.T) {
	p := NewPipeline(nil, 16)
	ch := p.SubscribeEvents()
	defer p.UnsubscribeEvents(ch)

	const workers = 32
	const perWorker = 1000
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				_, _, err := p.SubmitTransaction(&TxPayload{
					TxUUID:    GenerateUUID(),
					AccountID: "account",
					Amount:    int64(i),
				})
				if err != nil {
					t.Errorf("submit failed: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	if got := p.TotalCommitted(); got != workers*perWorker {
		t.Fatalf("committed=%d, want %d", got, workers*perWorker)
	}
	if got := p.TotalFailed(); got != 0 {
		t.Fatalf("failed=%d, want 0", got)
	}
}

func TestPipelineRejectsNilTransaction(t *testing.T) {
	p := NewPipeline(nil, 1)
	if _, _, err := p.SubmitTransaction(nil); err == nil {
		t.Fatal("expected nil transaction to be rejected")
	}
	if p.TotalFailed() != 1 {
		t.Fatalf("failed=%d, want 1", p.TotalFailed())
	}
}
