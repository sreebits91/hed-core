package v2

import (
	"context"
	"errors"
	"sort"
	"sync/atomic"
	"time"
)

// LedgerState is the minimum read-only contract required to reconcile HED's WAL
// with the authoritative ledger after an uncertain commit outcome.
type LedgerState interface {
	Status(ctx context.Context, txID string) (LedgerTxStatus, error)
}

type LedgerTxStatus string

const (
	LedgerCommitted    LedgerTxStatus = "COMMITTED"
	LedgerNotCommitted LedgerTxStatus = "NOT_COMMITTED"
	LedgerUnknown      LedgerTxStatus = "UNKNOWN"
)

var ErrLedgerStateUnknown = errors.New("ledger transaction state is unknown")

// RecoveryReport describes a restart/reconciliation pass without exposing
// implementation details of the WAL or Fabric client.
type RecoveryReport struct {
	Replayed      int
	AlreadyPresent int
	Committed     int
	Pending       int
	Unknown       int
	Duration      time.Duration
}

// Recover replays only WAL transactions without a commit record. It rebuilds
// the in-memory deduplication state before admitting the recovered transaction
// back into its original partition. Replay order is partition/sequence order.
func (p *Pipeline) Recover(ctx context.Context) (RecoveryReport, error) {
	start := time.Now()
	var report RecoveryReport
	if ctx == nil {
		ctx = context.Background()
	}
	if p == nil || p.wal == nil {
		return report, nil
	}
	txs, err := p.wal.Replay()
	if err != nil {
		return report, err
	}
	sort.Slice(txs, func(i, j int) bool {
		if txs[i].Partition == txs[j].Partition {
			return txs[i].Sequence < txs[j].Sequence
		}
		return txs[i].Partition < txs[j].Partition
	})
	for _, tx := range txs {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		idx := tx.Partition
		if idx < 0 || idx >= len(p.parts) {
			return report, ErrInvalidConfig
		}
		// A replayed ID is deliberately installed before enqueue so a concurrent
		// client cannot create a second in-flight transaction during recovery.
		if p.dedup.SeenOrAdd(tx.ID, time.Now()) {
			report.AlreadyPresent++
			continue
		}
		if err := p.parts[idx].q.Push(tx); err != nil {
			p.dedup.Forget(tx.ID)
			return report, err
		}
		report.Replayed++
		atomic.AddUint64(&p.metrics.partitions[idx].accepted, 1)
		atomic.StoreUint64(&p.metrics.partitions[idx].queueDepth, uint64(p.parts[idx].q.Len()))
	}
	report.Pending = report.Replayed
	report.Duration = time.Since(start)
	p.metrics.recovered.Add(uint64(report.Replayed))
	return report, nil
}

// Reconciler closes the dangerous "submitted but commit confirmation was
// lost" window. It never re-submits a transaction whose ledger state is
// already committed.
type Reconciler struct {
	ledger LedgerState
}

func NewReconciler(ledger LedgerState) *Reconciler { return &Reconciler{ledger: ledger} }

func (r *Reconciler) Reconcile(ctx context.Context, txID string) (LedgerTxStatus, error) {
	if r == nil || r.ledger == nil {
		return LedgerUnknown, ErrLedgerStateUnknown
	}
	status, err := r.ledger.Status(ctx, txID)
	if err != nil {
		return LedgerUnknown, err
	}
	if status == LedgerUnknown {
		return status, ErrLedgerStateUnknown
	}
	return status, nil
}
