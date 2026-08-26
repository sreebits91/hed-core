package v2

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

type recoveryLedger struct{ states map[string]LedgerTxStatus }
func (l recoveryLedger) Status(_ context.Context, id string) (LedgerTxStatus, error) { s, ok := l.states[id]; if !ok { return LedgerNotCommitted, nil }; return s, nil }

func TestRecoverReplaysOnlyUncommittedAndRebuildsDedup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hed.wal")
	cfg := DefaultConfig(); cfg.Partitions = 4; cfg.QueueCapacity = 32; cfg.BatchSize = 4; cfg.WALPath = path; cfg.SyncWAL = true
	p, err := NewPipeline(cfg, nil); if err != nil { t.Fatal(err) }
	for i := 0; i < 3; i++ { _, err = p.Submit(context.Background(), Tx{ID: "tx-"+string(rune('a'+i)), Key: "key", Payload: []byte("payload")}); if err != nil { t.Fatal(err) } }
	if err := p.wal.Commit("tx-a"); err != nil { t.Fatal(err) }
	p.Stop()

	p2, err := NewPipeline(cfg, nil); if err != nil { t.Fatal(err) }
	defer p2.Stop()
	report, err := p2.Recover(context.Background()); if err != nil { t.Fatal(err) }
	if report.Replayed != 2 { t.Fatalf("replayed=%d, want 2", report.Replayed) }
	if p2.dedup.SeenOrAdd("tx-b", time.Now()) != true { t.Fatal("replayed transaction was not restored to dedup") }
	if p2.dedup.SeenOrAdd("tx-a", time.Now()) != true { t.Fatal("committed transaction should remain known after replay") }
}

func TestReconcilerDoesNotResubmitCommittedTransactions(t *testing.T) {
	r := NewReconciler(recoveryLedger{states: map[string]LedgerTxStatus{"tx-1": LedgerCommitted}})
	status, err := r.Reconcile(context.Background(), "tx-1")
	if err != nil || status != LedgerCommitted { t.Fatalf("status=%v err=%v", status, err) }
}

func TestReconcilerReturnsUnknownForUncertainLedgerState(t *testing.T) {
	r := NewReconciler(recoveryLedger{states: map[string]LedgerTxStatus{"tx-1": LedgerUnknown}})
	status, err := r.Reconcile(context.Background(), "tx-1")
	if !errors.Is(err, ErrLedgerStateUnknown) || status != LedgerUnknown { t.Fatalf("status=%v err=%v", status, err) }
}
