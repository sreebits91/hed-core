package engine

import (
	"fmt"
	"sync"
	"testing"
)

type concurrentStateEngine struct {
	mu sync.Mutex
	puts int
}

func (e *concurrentStateEngine) Name() string { return "test-engine" }
func (e *concurrentStateEngine) Init(map[string]string) error { return nil }
func (e *concurrentStateEngine) GetState(string, string) ([]byte, error) { return nil, nil }
func (e *concurrentStateEngine) PutState(string, string, []byte) error {
	e.mu.Lock()
	e.puts++
	e.mu.Unlock()
	return nil
}
func (e *concurrentStateEngine) BatchWrite(string, map[string][]byte) error { return nil }
func (e *concurrentStateEngine) Close() error { return nil }

func TestPipelineConcurrentTopologyAndSubscriptions(t *testing.T) {
	engine := &concurrentStateEngine{}
	p := NewPipeline(engine, 4)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				p.SetShards((j%16)+1)
				_, _, err := p.SubmitTransaction(&TxPayload{
					TxUUID: fmt.Sprintf("%d-%d", worker, j),
					AccountID: "account",
					Amount: int64(j),
				})
				if err != nil {
					t.Errorf("submit failed: %v", err)
					return
				}
			}
		}(i)
	}

	for i := 0; i < 32; i++ {
		ch := p.SubscribeEvents()
		wg.Add(1)
		go func(ch chan Event) {
			defer wg.Done()
			for range ch {
			}
		}(ch)
		wg.Add(1)
		go func(ch chan Event) {
			defer wg.Done()
			p.UnsubscribeEvents(ch)
		}(ch)
	}

	wg.Wait()
	if p.TotalCommitted() != 8*500 {
		t.Fatalf("committed=%d, want %d", p.TotalCommitted(), 8*500)
	}
}

func TestPipelineUnsubscribeIsIdempotent(t *testing.T) {
	p := NewPipeline(nil, 1)
	ch := p.SubscribeEvents()
	p.UnsubscribeEvents(ch)
	p.UnsubscribeEvents(ch)
}
