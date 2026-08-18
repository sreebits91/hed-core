package e2e

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"hed-core/pkg/engine"
	"hed-core/pkg/hlf"
)

// Runner measures only transactions that complete the complete HED path:
// generate -> storage -> Fabric submit -> Fabric commit confirmation.
// A transaction is counted as successful only when FabricLifecycle.Submit
// returns nil, which is the commit-aware boundary of the real Fabric client.
type Runner struct {
	Pipeline *engine.Pipeline
	Fabric   *hlf.FabricLifecycle
	Workers  int
}

type Result struct {
	Requested       uint64
	Generated       uint64
	StorageSuccess  uint64
	FabricConfirmed uint64
	Failed          uint64
	Duration        time.Duration
	TPS             float64
}

func (r *Runner) Run(ctx context.Context, count uint64) (Result, error) {
	if r == nil || r.Pipeline == nil || r.Fabric == nil { return Result{}, errors.New("e2e: pipeline and Fabric lifecycle are required") }
	workers:=r.Workers; if workers<=0 {workers=32}
	start:=time.Now()
	var generated, storageSuccess, confirmed, failed uint64
	jobs:=make(chan *engine.TxPayload, workers*4)
	var wg sync.WaitGroup
	worker:=func(){
		defer wg.Done()
		for tx:=range jobs {
			if _,_,err:=r.Pipeline.SubmitTransaction(tx); err!=nil {atomic.AddUint64(&failed,1); continue}
			atomic.AddUint64(&storageSuccess,1)
			if err:=r.Fabric.Submit(ctx,tx); err!=nil {atomic.AddUint64(&failed,1); continue}
			atomic.AddUint64(&confirmed,1)
		}
	}
	wg.Add(workers); for i:=0;i<workers;i++ {go worker()}
	for i:=uint64(0); i<count; i++ {
		select { case <-ctx.Done(): close(jobs); wg.Wait(); d:=time.Since(start); return Result{Requested:count,Generated:generated,StorageSuccess:storageSuccess,FabricConfirmed:confirmed,Failed:failed,Duration:d,TPS:float64(confirmed)/d.Seconds()},ctx.Err()
		default: }
		tx:=&engine.TxPayload{TxUUID:engine.GenerateUUID(),AccountID:"bench-"+engine.GenerateUUID(),Amount:int64(i+1)}
		jobs<-tx; atomic.AddUint64(&generated,1)
	}
	close(jobs); wg.Wait()
	d:=time.Since(start)
	return Result{Requested:count,Generated:generated,StorageSuccess:storageSuccess,FabricConfirmed:confirmed,Failed:failed,Duration:d,TPS:float64(confirmed)/d.Seconds()},nil
}
