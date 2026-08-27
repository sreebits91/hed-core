package v2

import("context";"errors";"path/filepath";"testing";"time")
type recoveryLedger struct{states map[string]LedgerTxStatus}
func(l recoveryLedger)Status(_ context.Context,id string)(LedgerTxStatus,error){s,ok:=l.states[id];if !ok{return LedgerNotCommitted,nil};return s,nil}

func TestRecoverReplaysOnlyUncommittedAndRebuildsDedup(t *testing.T){
	path:=filepath.Join(t.TempDir(),"hed.wal");cfg:=DefaultConfig();cfg.Partitions=4;cfg.QueueCapacity=32;cfg.BatchSize=4;cfg.WALPath=path;cfg.SyncWAL=true
	w,err:=OpenWAL(path,true);if err!=nil{t.Fatal(err)}
	for i,id:=range []string{"tx-a","tx-b","tx-c"}{tx:=Tx{ID:id,Key:"key",Payload:[]byte("payload"),Partition:i%4,Sequence:1};if err=w.Append(tx);err!=nil{t.Fatal(err)}}
	if err=w.Commit("tx-a");err!=nil{t.Fatal(err)};if err=w.Close();err!=nil{t.Fatal(err)}
	p,err:=NewPipeline(cfg,nil);if err!=nil{t.Fatal(err)};defer p.Stop()
	report,err:=p.Recover(context.Background());if err!=nil{t.Fatal(err)};if report.Replayed!=2{t.Fatalf("replayed=%d,want 2",report.Replayed)}
	if !p.dedup.SeenOrAdd("tx-b",time.Now()){t.Fatal("replayed transaction was not restored to dedup")};if !p.dedup.SeenOrAdd("tx-a",time.Now()){t.Fatal("committed transaction was not restored to dedup")}
}
func TestReconcilerDoesNotResubmitCommittedTransactions(t *testing.T){r:=NewReconciler(recoveryLedger{states:map[string]LedgerTxStatus{"tx-1":LedgerCommitted}});status,err:=r.Reconcile(context.Background(),"tx-1");if err!=nil||status!=LedgerCommitted{t.Fatalf("status=%v err=%v",status,err)}}
func TestReconcilerReturnsUnknownForUncertainLedgerState(t *testing.T){r:=NewReconciler(recoveryLedger{states:map[string]LedgerTxStatus{"tx-1":LedgerUnknown}});status,err:=r.Reconcile(context.Background(),"tx-1");if !errors.Is(err,ErrLedgerStateUnknown)||status!=LedgerUnknown{t.Fatalf("status=%v err=%v",status,err)}}
