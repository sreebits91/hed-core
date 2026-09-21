package v2

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"sync/atomic"
	"time"
)

type LedgerState interface { Status(context.Context, string) (LedgerTxStatus, error) }
type LedgerTxStatus string
const ( LedgerCommitted LedgerTxStatus = "COMMITTED"; LedgerNotCommitted LedgerTxStatus = "NOT_COMMITTED"; LedgerUnknown LedgerTxStatus = "UNKNOWN" )
var ErrLedgerStateUnknown = errors.New("ledger transaction state is unknown")
type RecoveryReport struct { Replayed int; AlreadyPresent int; Committed int; Pending int; Unknown int; Duration time.Duration }

// ReplayIDs reconstructs transaction identity from all valid prepare records,
// including records that already have a commit marker.
func (w *WAL) ReplayIDs() (map[string]struct{}, error) {
	ids:=map[string]struct{}{}; if w==nil{return ids,nil}; w.mu.Lock();defer w.mu.Unlock()
	if _,err:=w.f.Seek(0,0);err!=nil{return nil,err}; r:=bufio.NewReader(w.f)
	for { line,err:=r.ReadBytes('
'); if err!=nil&&len(line)==0 {if err==io.EOF{break};return nil,err}; if len(line)==0 {if err!=nil{break};continue}; var rec WALRecord; if json.Unmarshal(line,&rec)!=nil||rec.Checksum!=recordChecksum(rec.Kind,rec.Tx,rec.ID){return nil,ErrWALCorrupt}; if rec.Kind=="prepare"{ids[rec.Tx.ID]=struct{}{}}; if err!=nil{break} }
	return ids,nil
}

// Recover rebuilds deduplication state first, then requeues only WAL records
// without a corresponding commit marker. Replay order is partition/sequence.
func(p *Pipeline) Recover(ctx context.Context)(RecoveryReport,error){
	start:=time.Now();var report RecoveryReport;if ctx==nil{ctx=context.Background()};if p==nil||p.wal==nil{return report,nil}
	ids,err:=p.wal.ReplayIDs();if err!=nil{return report,err}
	txs,err:=p.wal.Replay();if err!=nil{return report,err};sort.Slice(txs,func(i,j int)bool{if txs[i].Partition==txs[j].Partition{return txs[i].Sequence<txs[j].Sequence};return txs[i].Partition<txs[j].Partition})
	pendingIDs:=make(map[string]struct{},len(txs));for _,tx:=range txs{pendingIDs[tx.ID]=struct{}{}}
	now:=time.Now();for id:=range ids{if _,pending:=pendingIDs[id];!pending{p.dedup.SeenOrAdd(id,now)}}
	for _,tx:=range txs{if err:=ctx.Err();err!=nil{return report,err};if p.dedup.SeenOrAdd(tx.ID,now){report.AlreadyPresent++;continue};idx:=tx.Partition;if idx<0||idx>=len(p.parts){p.dedup.Forget(tx.ID);report.Unknown++;continue};if tx.Sequence>p.parts[idx].seq.Load(){p.parts[idx].seq.Store(tx.Sequence)};if err:=p.parts[idx].q.Push(tx);err!=nil{p.dedup.Forget(tx.ID);report.Pending++;continue};report.Replayed++;atomic.AddUint64(&p.metrics.partitions[idx].accepted,1);atomic.StoreUint64(&p.metrics.partitions[idx].queueDepth,uint64(p.parts[idx].q.Len()))}
	report.Duration=time.Since(start);p.metrics.recovered.Add(uint64(report.Replayed));return report,nil
}

type Reconciler struct{ledger LedgerState}
func NewReconciler(ledger LedgerState)*Reconciler{return &Reconciler{ledger:ledger}}
func(r *Reconciler)Reconcile(ctx context.Context,txID string)(LedgerTxStatus,error){if r==nil||r.ledger==nil{return LedgerUnknown,ErrLedgerStateUnknown};status,err:=r.ledger.Status(ctx,txID);if err!=nil{return LedgerUnknown,err};if status==LedgerUnknown{return status,ErrLedgerStateUnknown};return status,nil}
