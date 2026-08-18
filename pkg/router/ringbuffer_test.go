package router

import (
	"sync"
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
		producers = 16
		consumers = 16
		perProducer = 2000
	)

	rb := NewRingBuffer(1024)
	seen := make(map[string]bool, producers*perProducer)
	var seenMu sync.Mutex
	var wg sync.WaitGroup

	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(producer int) {
			defer wg.Done()
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

	var consumersWG sync.WaitGroup
	for c := 0; c < consumers; c++ {
		consumersWG.Add(1)
		go func() {
			defer consumersWG.Done()
			for {
				batch, err := rb.PopBatch(32)
				if err == ErrBufferEmpty {
					if done := rb.Length(); done == 0 {
						// Producers may still be running; check the producer
						// waitgroup without holding any queue lock.
						wg.Wait()
						if rb.Length() == 0 {
							return
						}
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

	wg.Wait()
	consumersWG.Wait()

	want := producers * perProducer
	if len(seen) != want {
		t.Fatalf("consumed=%d, want %d", len(seen), want)
	}
}

func formatID(producer, item int) string {
	// Avoid fmt in the hot path; IDs only need to be unique for this test.
	return string(rune(producer)) + ":" + string(rune(item))
}
