package v2

import (
    "context"
    "testing"
)

type discardBackend struct{}
func (discardBackend) Commit(context.Context, Tx) error { return nil }

func BenchmarkQueue(b *testing.B){q,_:=NewQueue(1<<16);tx:=Tx{ID:"bench",Key:"k",Payload:[]byte("payload")};b.ReportAllocs();b.ResetTimer();for i:=0;i<b.N;i++{for q.Push(tx)==ErrQueueFull{q.PopBatch(256)};if i%256==255{q.PopBatch(256)}}}
func BenchmarkChannel(b *testing.B){ch:=make(chan Tx,1<<16);tx:=Tx{ID:"bench",Key:"k",Payload:[]byte("payload")};b.ReportAllocs();b.ResetTimer();for i:=0;i<b.N;i++{select{case ch<-tx:default:<-ch;ch<-tx};if i%256==255{for j:=0;j<256;j++{<-ch}}}}
func BenchmarkPartitionRouting(b *testing.B){for _,n:=range []int{1,4,8,16,32}{b.Run("partitions="+itoa(n),func(b *testing.B){r,_:=NewRouter(n);b.ReportAllocs();b.ResetTimer();for i:=0;i<b.N;i++{_ = r.Partition("account-"+itoa(i))}})}}
func BenchmarkPipelineIngress(b *testing.B){for _,n:=range []int{1,4,8,16,32}{b.Run("partitions="+itoa(n),func(b *testing.B){c:=DefaultConfig();c.Partitions=n;c.QueueCapacity=1<<16;c.BatchSize=256;p,_:=NewPipeline(c,discardBackend{});defer p.Stop();b.ReportAllocs();b.ResetTimer();for i:=0;i<b.N;i++{_,_=p.Submit(context.Background(),Tx{ID:"tx-"+itoa(i),Key:"account-"+itoa(i%10000),Payload:[]byte("x")})}})}}
