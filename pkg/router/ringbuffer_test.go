package router

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestRingBufferFIFO(t *testing.T) {
	rb := NewRingBuffer(8)
	for i := 0; i < 8; i++ {
		if err := rb.Push(TransactionPayload{TxID: string(rune('a' + i))}); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	if err := rb.Push(TransactionPayload{TxID: "overflow"}); err != ErrBufferFull {
		t.Fatalf("overflow error=%v, want %v", err, ErrBufferFull)
	}

	batch, err := rb.PopBatch(8)
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	for i, got := range batch {
		want := string(rune('a' + i))
		if got.TxID != want {
			t.Fatalf("item %d=%q, want %q", i, got.TxID, want)
		}
	}
	if _, err := rb.PopBatch(1); err != ErrBufferEmpty {
		t.Fatalf("empty error=%v, want %v", err, ErrBufferEmpty)
	}
}

func TestRingBufferMPMC(t *testing.T) {
	const (
		producers   = 16
		consumers   = 16
		perProducer = 2000
	)

	rb := NewRingBuffer(1024)
	seen := make(map[string]bool, producers*perProducer)
	var seenMu sync.Mutex
	var producerWG sync.WaitGroup
	var consumerWG sync.WaitGroup
	var producersDone atomic.Bool

	for p := 0; p < producers; p++ {
		producerWG.Add(1)
		go func(producer int) {
			defer producerWG.Done()
			for i := 0; i < perProducer; i++ {
				tx := TransactionPayload{TxID: formatID(producer, i), Payload: []byte{byte(i)}}
				for {
					if err := rb.Push(tx); err == nil {
						break
					}
				}
			}
		}(p)
	}

	for c := 0; c < consumers; c++ {
		consumerWG.Add(1)
		go func() {
			defer consumerWG.Done()
			for {
				batch, err := rb.PopBatch(32)
				if err == ErrBufferEmpty {
					if producersDone.Load() && rb.Length() == 0 {
						return
					}
					continue
				}
				for _, tx := range batch {
					seenMu.Lock()
					if seen[tx.TxID] {
						seenMu.Unlock()
						t.Errorf("duplicate transaction %s", tx.TxID)
						return
					}
					seen[tx.TxID] = true
					seenMu.Unlock()
				}
			}
		}()
	}

	producerWG.Wait()
	producersDone.Store(true)
	consumerWG.Wait()

	want := producers * perProducer
	if len(seen) != want {
		t.Fatalf("consumed=%d, want %d", len(seen), want)
	}
}

func formatID(producer, item int) string {
	return string(rune(producer)) + ":" + string(rune(item))
}
