package v2

import (
	"context"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
	second, err := p.Recover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if second.Replayed != 0 || second.AlreadyPresent != 1 {
		t.Fatalf("second recovery=%+v; recovery must be idempotent", second)
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
					ID:      "concurrent-" + strconv.Itoa(producer) + "-" + strconv.Itoa(i),
					Key:      "account-" + strconv.Itoa(i%64),
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

func TestDedupForgetRemainsBounded(t *testing.T) {
	d, err := NewDedup(32, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	for i := 0; i < 10000; i++ {
		id := strconv.Itoa(i)
		d.SeenOrAdd(id, now)
		d.Forget(id)
	}
	if len(d.m) > 32 || len(d.order) > 64 {
		t.Fatalf("dedup grew unexpectedly: map=%d order=%d", len(d.m), len(d.order))
	}
}

func TestBackpressureLevels(t *testing.T) {
	if Level(0, 100) != Normal {
		t.Fatal("0 should be NORMAL")
	}
	if Level(50, 100) != Busy {
		t.Fatal("50% should be BUSY")
	}
	if Level(80, 100) != Saturated {
		t.Fatal("80% should be SATURATED")
	}
	if Level(100, 100) != Rejecting {
		t.Fatal("full should be REJECTING")
	}
}

type fakeLedger struct {
	status LedgerTxStatus
	err    error
	calls  int
}

func (f *fakeLedger) Status(context.Context, string) (LedgerTxStatus, error) {
	f.calls++
	return f.status, f.err
}

func TestReconcilerClassifiesLedgerState(t *testing.T) {
	f := &fakeLedger{status: LedgerCommitted}
	r := NewReconciler(f)
	status, err := r.Reconcile(context.Background(), "tx-1")
	if err != nil || status != LedgerCommitted {
		t.Fatalf("status=%s err=%v", status, err)
	}
	if f.calls != 1 {
		t.Fatalf("calls=%d want=1", f.calls)
	}

	f.status = LedgerNotCommitted
	status, err = r.Reconcile(context.Background(), "tx-2")
	if err != nil || status != LedgerNotCommitted {
		t.Fatalf("status=%s err=%v", status, err)
	}

	f.status = LedgerUnknown
	if _, err = r.Reconcile(context.Background(), "tx-3"); err != ErrLedgerStateUnknown {
		t.Fatalf("expected unknown-state error, got %v", err)
	}
}

type blockingBackend struct {
	started   chan struct{}
	release   chan struct{}
	committed chan struct{}
	once      sync.Once
}

func (b *blockingBackend) Commit(ctx context.Context, tx Tx) error {
	b.once.Do(func() { close(b.started) })
	select {
	case <-b.release:
		b.committed <- struct{}{}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestQueueFullAbortsWALAndAllowsRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hed.wal")
	cfg := DefaultConfig()
	cfg.Partitions = 1
	cfg.QueueCapacity = 1
	cfg.WALPath = path
	cfg.BatchSize = 1
	cfg.FlushInterval = time.Hour
	b := &blockingBackend{started: make(chan struct{}), release: make(chan struct{}), committed: make(chan struct{}, 2)}
	p, err := NewPipeline(cfg, b)
	if err != nil {
		t.Fatal(err)
	}

	tx1 := Tx{ID: "queue-one", Key: "k", Payload: []byte("x")}
	if _, err = p.Submit(context.Background(), tx1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-b.started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start first commit")
	}

	tx2 := Tx{ID: "queue-two", Key: "k", Payload: []byte("x")}
	if _, err = p.Submit(context.Background(), tx2); err != nil {
		t.Fatal(err)
	}
	tx3 := Tx{ID: "queue-three", Key: "k", Payload: []byte("x")}
	if _, err = p.Submit(context.Background(), tx3); err != ErrQueueFull {
		t.Fatalf("err=%v want queue full", err)
	}

	close(b.release)
	deadline := time.Now().Add(time.Second)
	for atomic.LoadUint64(&p.metrics.partitions[0].committed) < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("expected two committed transactions, got %d", atomic.LoadUint64(&p.metrics.partitions[0].committed))
		}
		time.Sleep(time.Millisecond)
	}

	if _, err = p.Submit(context.Background(), tx3); err != nil {
		t.Fatalf("retry failed after queue drained: %v", err)
	}
	p.Stop()

	w, err := OpenWAL(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	txs, err := w.Replay()
	if err != nil {
		t.Fatal(err)
	}
	for _, tx := range txs {
		if tx.ID == tx3.ID {
			t.Fatalf("aborted transaction remained pending after retry: %+v", tx)
		}
	}
}
