package router

import("strconv";"sync";"sync/atomic";"testing";"time")

func TestRingBufferConcurrentStress(t *testing.T){rb:=NewRingBuffer(1024);const total=50000;var wg sync.WaitGroup;var pushed,popped atomic.Int64;for p:=0;p<8;p++{wg.Add(1);go func(base int){defer wg.Done();for i:=0;i<total/8;i++{tx:=TransactionPayload{TxID:strconv.Itoa(base*(total/8)+i),Payload:[]byte("x")};for rb.Push(tx)==ErrBufferFull{time.Sleep(time.Microsecond)};pushed.Add(1)}}(p)};for c:=0;c<8;c++{wg.Add(1);go func(){defer wg.Done();for popped.Load()<total{batch,err:=rb.PopBatch(32);if err==ErrBufferEmpty{time.Sleep(time.Microsecond);continue};if err!=nil{t.Errorf("pop: %v",err);return};popped.Add(int64(len(batch)))}}()};wg.Wait();if pushed.Load()!=total||popped.Load()!=total{t.Fatalf("lost queue items pushed=%d popped=%d",pushed.Load(),popped.Load())}}

func TestRingBufferDoesNotStarve(t *testing.T){rb:=NewRingBuffer(64);done:=make(chan struct{});go func(){defer close(done);for i:=0;i<10000;i++{for rb.Push(TransactionPayload{TxID:strconv.Itoa(i)})==ErrBufferFull{rb.PopBatch(8)}}}();deadline:=time.After(2*time.Second);for rb.Length()==0{select{case<-deadline:t.Fatal("producer starved");case<-time.After(time.Millisecond):}};select{case<-done:case<-deadline:t.Fatal("producer did not make progress")}}
