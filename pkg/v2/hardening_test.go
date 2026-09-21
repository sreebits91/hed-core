package v2

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
)

func TestWALAbortDoesNotReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hed.wal")
	w, err := OpenWAL(path, true)
	if err != nil {
		t.Fatal(err)
	}
	tx := Tx{ID: "queued-then-rejected", Key: "k", Payload: []byte("x"), Partition: 0, Sequence: 1}
	if err := w.Append(tx); err != nil {
		t.Fatal(err)
	}
	if err := w.Abort(tx.ID); err != nil {
		t.Fatal(err)
	}
	got, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("replayed aborted transaction: %#v", got)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryRestoresSequenceCounter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hed.wal")
	w, err := OpenWAL(path, true)
	if err != nil {
		t.Fatal(err)
	}
	tx := Tx{ID: "recovered", Key: "k", Payload: []byte("x"), Partition: 0, Sequence: 41}
	if err := w.Append(tx); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	cfg.Partitions = 1
	cfg.QueueCapacity = 128
	cfg.WALPath = path
	p, err := NewPipeline(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()

	report, err := p.Recover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Replayed != 1 {
		t.Fatalf("replayed=%d want=1", report.Replayed)
	}
	if got := p.parts[0].seq.Load(); got != 41 {
		t.Fatalf("sequence=%d want=41", got)
	}
}

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
					Key:      "account-" + itoa(i%64),
					Payload: []byte("payload"),
				})
				if err != nil {
					t.Errorf("unexpected submit error: %v", err)
					return
				}
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	p.Stop()

	if accepted != producers*perProducer {
		t.Fatalf("accepted=%d want=%d", accepted, producers*perProducer)
	}
}
