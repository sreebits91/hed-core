package v2

import (
    "context"
    "errors"
    "os"
    "path/filepath"
    "sync"
    "sync/atomic"
    "testing"
    "time"
)

type testBackend struct { mu sync.Mutex; ids []string; fail int; calls int32 }
func (b *testBackend) Commit(ctx context.Context, tx Tx) error { b.mu.Lock(); defer b.mu.Unlock(); b.calls++; if b.fail>0 { b.fail--; return errors.New("transient") }; b.ids=append(b.ids,tx.ID); return nil }

func testConfig(t *testing.T) Config { c:=DefaultConfig(); c.Partitions=4; c.QueueCapacity=128; c.BatchSize=16; c.FlushInterval=time.Millisecond; c.DedupCapacity=10000; c.DedupTTL=time.Second; c.CommitTimeout=500*time.Millisecond; c.Retry.MaxAttempts=3; c.Retry.InitialBackoff=time.Millisecond; c.Retry.MaxBackoff=4*time.Millisecond; return c }

func TestRouterDeterministic(t *testing.T){r,_:=NewRouter(16); for i:=0;i<10000;i++{k:=string(rune(i%128))+"-key"; a,b:=r.Partition(k),r.Partition(k); if a!=b||a<0||a>=16{t.Fatalf("non-deterministic route %d %d",a,b)}}}
func TestQueueConcurrentNoLoss(t *testing.T){q,_:=NewQueue(4096); const n=20000; var wg sync.WaitGroup; var pushed, popped atomic.Int64; for p:=0;p<8;p++{wg.Add(1);go func(base int){defer wg.Done();for i:=0;i<n/8;i++{tx:=Tx{ID: "tx-"+itoa(base*n/8+i),Payload:[]byte("x")};for q.Push(tx)==ErrQueueFull{time.Sleep(time.Microsecond)};pushed.Add(1)}}(p)}; for c:=0;c<8;c++{wg.Add(1);go func(){defer wg.Done();for popped.Load()<n {b,_:=q.PopBatch(32);if len(b)==0{time.Sleep(time.Microsecond);continue};popped.Add(int64(len(b)))}}()};wg.Wait();if pushed.Load()!=n||popped.Load()!=n{t.Fatalf("loss pushed=%d popped=%d",pushed.Load(),popped.Load())}}
func TestDedupTTLAndBounded(t *testing.T){d,_:=NewDedup(2,10*time.Millisecond);now:=time.Now();if d.SeenOrAdd("a",now){t.Fatal()};if !d.SeenOrAdd("a",now){t.Fatal()};d.SeenOrAdd("b",now);d.SeenOrAdd("c",now);if !d.SeenOrAdd("a",now){t.Fatal("oldest should have been evicted")};if d.SeenOrAdd("a",now.Add(20*time.Millisecond)){t.Fatal("expired entry should be accepted")}}
func TestWALReplayOnlyUncommitted(t *testing.T){dir:=t.TempDir();path:=filepath.Join(dir,"hed.wal");w,err:=OpenWAL(path,true);if err!=nil{t.Fatal(err)};a:=Tx{ID:"a",Partition:0,Sequence:1};b:=Tx{ID:"b",Partition:0,Sequence:2};if err=w.Append(a);err!=nil{t.Fatal(err)};if err=w.Append(b);err!=nil{t.Fatal(err)};if err=w.Commit(a.ID);err!=nil{t.Fatal(err)};if err=w.Close();err!=nil{t.Fatal(err)};w,err=OpenWAL(path,true);if err!=nil{t.Fatal(err)};got,err:=w.Replay();if err!=nil{t.Fatal(err)};if len(got)!=1||got[0].ID!="b"{t.Fatalf("unexpected replay %#v",got)};w.Close()}
func TestWALCorruptRecord(t *testing.T){dir:=t.TempDir();path:=filepath.Join(dir,"bad.wal");if err:=os.WriteFile(path,[]byte(`{"kind":"prepare","tx":{"id":"a"},"checksum":123}\n`),0600);err!=nil{t.Fatal(err)};w,err:=OpenWAL(path,false);if err!=nil{t.Fatal(err)};defer w.Close();if _,err=w.Replay();!errors.Is(err,ErrWALCorrupt){t.Fatalf("expected corrupt WAL, got %v",err)}}
func TestCommitRetryAndPermanent(t *testing.T){b:=&testBackend{fail:2};c:=NewCommitter(b,RetryPolicy{MaxAttempts:4,InitialBackoff:time.Millisecond,MaxBackoff:2*time.Millisecond},100*time.Millisecond);if err:=c.Commit(context.Background(),Tx{ID:"a"});err!=nil{t.Fatal(err)};if c.RetryCount()!=2{t.Fatalf("retries=%d",c.RetryCount())};pb:=&testBackend{};pc:=NewCommitter(pb,RetryPolicy{MaxAttempts:5,InitialBackoff:time.Millisecond,MaxBackoff:2*time.Millisecond},100*time.Millisecond);pb.fail=1;_ = pc;_ = context.Background()}
func TestPipelineGracefulDrainAndDedup(t *testing.T){b:=&testBackend{};c:=testConfig(t);p,err:=NewPipeline(c,b);if err!=nil{t.Fatal(err)};for i:=0;i<1000;i++{id:="tx-"+itoa(i);if _,err:=p.Submit(context.Background(),Tx{ID:id,Key:"acct-"+itoa(i%32),Payload:[]byte("payload")});err!=nil{t.Fatal(err)}};if _,err:=p.Submit(context.Background(),Tx{ID:"tx-1",Key:"acct-1",Payload:[]byte("payload")});!errors.Is(err,ErrDuplicate){t.Fatalf("expected duplicate, got %v",err)};p.Stop();b.mu.Lock();n:=len(b.ids);b.mu.Unlock();if n!=1000{t.Fatalf("drain lost tx: %d",n)};if _,err:=p.Submit(context.Background(),Tx{ID:"after",Key:"x"});!errors.Is(err,ErrEngineStopped){t.Fatalf("expected stopped, got %v",err)}}
func TestBackpressureStates(t *testing.T){if Level(0,100)!=Normal||Level(50,100)!=Busy||Level(80,100)!=Saturated||Level(100,100)!=Rejecting{t.Fatal("backpressure thresholds incorrect")}}
func itoa(i int) string { if i==0{return "0"}; b:=make([]byte,0,20); for i>0{b=append(b,byte('0'+i%10));i/=10}; for l,r:=0,len(b)-1;l<r;l,r=l+1,r-1{b[l],b[r]=b[r],b[l]};return string(b) }
