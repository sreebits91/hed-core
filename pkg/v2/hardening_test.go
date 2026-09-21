package v2

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestConcurrentIngressIsLossless(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Partitions = 8
	cfg.QueueCapacity = 16384
	cfg.BatchSize = 256
	cfg.WALPath = ""

	p, err := NewPipeline(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}

	const producers = 16
	const perProducer = 500
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := 0

	for producer := 0; producer < producers; producer++ {
		producer := producer
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				_, err := p.Submit(context.Background(), Tx{
					ID:      "concurrent-" + itoa(producer) + "-" + itoa(i),
					Key:     "account-" + itoa(i%64),
					Payload: []byte("payload"),
				})
				if err == nil {
					mu.Lock()
					accepted++
					mu.Unlock()
				} else if !errors.Is(err, ErrQueueFull) {
					t.Errorf("unexpected submit error: %v", err)
				}
			}
		}()
	}

	wg.Wait()
	p.Stop()

	if accepted != producers*perProducer {
		t.Fatalf("accepted=%d want=%d", accepted, producers*perProducer)
	}
}
