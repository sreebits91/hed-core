package v2

import (
 "bufio"
 "context"
 "encoding/json"
 "errors"
 "hash/crc32"
 "hash/fnv"
 "io"
 "os"
 "sort"
 "strings"
 "sync"
 "sync/atomic"
 "time"
)

var ( ErrInvalidTxID=errors.New("invalid transaction id"); ErrPayloadTooLarge=errors.New("payload exceeds configured limit"); ErrQueueFull=errors.New("queue is full"); ErrEngineStopped=errors.New("engine is stopped"); ErrDuplicate=errors.New("duplicate transaction"); ErrInvalidConfig=errors.New("invalid configuration"); ErrWALCorrupt=errors.New("corrupt WAL record") )

type Tx struct { ID string `json:"id"`; Key string `json:"key"`; Payload []byte `json:"payload"`; Partition int `json:"partition"`; Sequence uint64 `json:"sequence"` }
type Config struct { Partitions,QueueCapacity,BatchSize,MaxPayloadBytes,DedupCapacity int; FlushInterval,DedupTTL,CommitTimeout time.Duration; Retry RetryPolicy; WALPath string; SyncWAL bool }
func DefaultConfig() Config{return Config{Partitions:8,QueueCapacity:65536,BatchSize:256,FlushInterval:2*time.Millisecond,MaxPayloadBytes:1<<20,DedupCapacity:1_000_000,DedupTTL:10*time.Minute,CommitTimeout:5*time.Second,Retry:RetryPolicy{MaxAttempts:5,InitialBackoff:2*time.Millisecond,MaxBackoff:250*time.Millisecond}}}
func(c Config)Validate()error{if c.Partitions<=0||c.QueueCapacity<=0||c.BatchSize<=0||c.FlushInterval<=0||c.MaxPayloadBytes<=0||c.DedupCapacity<=0||c.DedupTTL<=0||c.CommitTimeout<=0||c.Retry.MaxAttempts<=0||c.Retry.InitialBackoff<=0||c.Retry.MaxBackoff<c.Retry.InitialBackoff{return ErrInvalidConfig};return nil}
func ValidateTx(tx Tx,maxPayload int)error{if len(tx.ID)==0||len(tx.ID)>256{return ErrInvalidTxID};for _,r:=range tx.ID{if !(r>='a'&&r<='z')&&!(r>='A'&&r<='Z')&&!(r>='0'&&r<='9')&&r!='-'&&r!='_'&&r!='.'{return ErrInvalidTxID}};if maxPayload<=0||len(tx.Payload)>maxPayload{return ErrPayloadTooLarge};return nil}

type Router struct{partitions int}
func NewRouter(n int)(*Router,error){if n<=0{return nil,ErrInvalidConfig};return &Router{partitions:n},nil}
func(r *Router)Partition(key string)int{h:=fnv.New64a();_,_=h.Write([]byte(key));return int(h.Sum64()%uint64(r.partitions))}
func(r *Router)PartitionCount()int{return r.partitions}

type Queue struct{mu sync.Mutex;items []Tx;cap int;closed bool}
func NewQueue(n int)(*Queue,error){if n<=0{return nil,ErrInvalidConfig};return &Queue{items:make([]Tx,0,n),cap:n},nil}
func(q *Queue)Push(tx Tx)error{q.mu.Lock();defer q.mu.Unlock();if q.closed{return ErrEngineStopped};if len(q.items)>=q.cap{return ErrQueueFull};q.items=append(q.items,tx);return nil}
func(q *Queue)PopBatch(n int)([]Tx,bool){q.mu.Lock();defer q.mu.Unlock();if len(q.items)==0{return nil,q.closed};if n<=0||n>len(q.items){n=len(q.items)};out:=make([]Tx,n);copy(out,q.items[:n]);copy(q.items,q.items[n:]);q.items=q.items[:len(q.items)-n];return out,false}
func(q *Queue)Close(){q.mu.Lock();q.closed=true;q.mu.Unlock()}
func(q *Queue)Len()int{q.mu.Lock();defer q.mu.Unlock();return len(q.items)}
func(q *Queue)Cap()int{return q.cap}

type BackpressureLevel string
const(Normal BackpressureLevel="NORMAL";Busy BackpressureLevel="BUSY";Saturated BackpressureLevel="SATURATED";Rejecting BackpressureLevel="REJECTING")
type RejectReason string
const(RejectQueueFull RejectReason="QUEUE_FULL";RejectStopped RejectReason="ENGINE_STOPPED";RejectInvalid RejectReason="INVALID_TRANSACTION";RejectDuplicate RejectReason="DUPLICATE_TRANSACTION")
func Level(depth,capacity int)BackpressureLevel{if capacity<=0||depth>=capacity{return Rejecting};r:=float64(depth)/float64(capacity);if r>=.8{return Saturated};if r>=.5{return Busy};return Normal}

type dedupEntry struct{expires time.Time}
type Dedup struct{mu sync.Mutex;m map[string]dedupEntry;order []string;capacity int;ttl time.Duration}
func NewDedup(n int,ttl time.Duration)(*Dedup,error){if n<=0||ttl<=0{return nil,ErrInvalidConfig};return &Dedup{m:make(map[string]dedupEntry,n),capacity:n,ttl:ttl},nil}
func(d *Dedup)SeenOrAdd(id string,now time.Time)bool{d.mu.Lock();defer d.mu.Unlock();d.expireLocked(now);if _,ok:=d.m[id];ok{return true};if len(d.m)>=d.capacity{old:=d.order[0];d.order=d.order[1:];delete(d.m,old)};d.m[id]=dedupEntry{expires:now.Add(d.ttl)};d.order=append(d.order,id);return false}
func(d *Dedup)Forget(id string){d.mu.Lock();defer d.mu.Unlock();delete(d.m,id)}
func(d *Dedup)expireLocked(now time.Time){i:=0;for i<len(d.order){id:=d.order[i];e,ok:=d.m[id];if !ok||!now.Before(e.expires){delete(d.m,id);i++;continue};break};if i>0{d.order=append([]string(nil),d.order[i:]...)}}

type WALRecord struct{Kind string `json:"kind"`;Tx Tx `json:"tx,omitempty"`;ID string `json:"id,omitempty"`;Checksum uint32 `json:"checksum"`}
type WAL struct{mu sync.Mutex;f *os.File;syncOnWrite bool}
func OpenWAL(path string,syncOnWrite bool)(*WAL,error){if path==""{return nil,nil};f,e:=os.OpenFile(path,os.O_CREATE|os.O_RDWR|os.O_APPEND,0600);if e!=nil{return nil,e};return &WAL{f:f,syncOnWrite:syncOnWrite},nil}
func recordChecksum(k string,tx Tx,id string)uint32{b,_:=json.Marshal(struct{K string;T Tx;I string}{k,tx,id});return crc32.ChecksumIEEE(b)}
func(w *WAL)appendRecord(r WALRecord)error{b,e:=json.Marshal(r);if e!=nil{return e};if _,e=w.f.Write(append(b,'\n'));e!=nil{return e};if w.syncOnWrite{return w.f.Sync()};return nil}
func(w *WAL)Append(tx Tx)error{if w==nil{return nil};w.mu.Lock();defer w.mu.Unlock();return w.appendRecord(WALRecord{Kind:"prepare",Tx:tx,Checksum:recordChecksum("prepare",tx,"")})}
func(w *WAL)Commit(id string)error{if w==nil{return nil};w.mu.Lock();defer w.mu.Unlock();return w.appendRecord(WALRecord{Kind:"commit",ID:id,Checksum:recordChecksum("commit",Tx{},id)})}
func(w *WAL)Close()error{if w==nil{return nil};w.mu.Lock();defer w.mu.Unlock();return w.f.Close()}
func(w *WAL)Replay()([]Tx,error){if w==nil{return nil,nil};w.mu.Lock();defer w.mu.Unlock();if _,e:=w.f.Seek(0,0);e!=nil{return nil,e};r:=bufio.NewReader(w.f);pending:=map[string]Tx{};for{line,e:=r.ReadBytes('\n');if e!=nil&&len(line)==0{if e==io.EOF{break};return nil,e};line=[]byte(strings.TrimSpace(string(line)));if len(line)==0{if e!=nil{break};continue};var rec WALRecord;if x:=json.Unmarshal(line,&rec);x!=nil{if e==io.EOF{break};return nil,ErrWALCorrupt};if rec.Checksum!=recordChecksum(rec.Kind,rec.Tx,rec.ID){return nil,ErrWALCorrupt};switch rec.Kind{case"prepare":pending[rec.Tx.ID]=rec.Tx;case"commit":delete(pending,rec.ID);default:return nil,ErrWALCorrupt};if e!=nil{break}};out:=make([]Tx,0,len(pending));for _,tx:=range pending{out=append(out,tx)};sort.Slice(out,func(i,j int)bool{if out[i].Partition==out[j].Partition{return out[i].Sequence<out[j].Sequence};return out[i].Partition<out[j].Partition});return out,nil}

 type RetryPolicy struct{MaxAttempts int;InitialBackoff,MaxBackoff time.Duration}
type CommitBackend interface{Commit(context.Context,Tx)error}
type PermanentError struct{Err error}
func(e PermanentError)Error()string{return e.Err.Error()};func(e PermanentError)Unwrap()error{return e.Err}
type Committer struct{backend CommitBackend;policy RetryPolicy;timeout time.Duration;retries atomic.Uint64}
func NewCommitter(b CommitBackend,p RetryPolicy,t time.Duration)*Committer{return &Committer{backend:b,policy:p,timeout:t}}
func(c *Committer)Commit(ctx context.Context,tx Tx)error{if c.backend==nil{return nil};var err error;back:=c.policy.InitialBackoff;for a:=1;a<=c.policy.MaxAttempts;a++{cc,cancel:=context.WithTimeout(ctx,c.timeout);err=c.backend.Commit(cc,tx);cancel();if err==nil{return nil};var pe PermanentError;if errors.As(err,&pe){return err};if a==c.policy.MaxAttempts{break};c.retries.Add(1);tm:=time.NewTimer(back);select{case<-ctx.Done():tm.Stop();return ctx.Err();case<-tm.C:};back*=2;if back>c.policy.MaxBackoff{back=c.policy.MaxBackoff}};return err}
func(c *Committer)RetryCount()uint64{return c.retries.Load()}

type PartitionMetrics struct{accepted,committed,failed,retries,rejected,queueDepth,batchSize,commitLatencyNs,orderingErrors uint64}
type Metrics struct{partitions []PartitionMetrics;rejectionMu sync.Mutex;rejection map[RejectReason]uint64;recovered atomic.Uint64;started time.Time}
func newMetrics(n int)*Metrics{return &Metrics{partitions:make([]PartitionMetrics,n),rejection:map[RejectReason]uint64{},started:time.Now()}}
func(m *Metrics)Snapshot()map[string]any{out:=map[string]any{};elapsed:=time.Since(m.started).Seconds();if elapsed<.001{elapsed=.001};parts:=make([]map[string]uint64,len(m.partitions));for i:=range m.partitions{p:=&m.partitions[i];accepted:=atomic.LoadUint64(&p.accepted);parts[i]=map[string]uint64{"accepted":accepted,"committed":atomic.LoadUint64(&p.committed),"failed":atomic.LoadUint64(&p.failed),"retries":atomic.LoadUint64(&p.retries),"rejected":atomic.LoadUint64(&p.rejected),"queue_depth":atomic.LoadUint64(&p.queueDepth),"batch_size":atomic.LoadUint64(&p.batchSize),"commit_latency_ns":atomic.LoadUint64(&p.commitLatencyNs),"ordering_errors":atomic.LoadUint64(&p.orderingErrors),"tps":uint64(float64(accepted)/elapsed)}};out["partitions"]=parts;out["recovered"]=m.recovered.Load();m.rejectionMu.Lock();rej:=map[string]uint64{};for k,v:=range m.rejection{rej[string(k)]=v};m.rejectionMu.Unlock();out["rejection_reasons"]=rej;return out}
func(m *Metrics)reject(r RejectReason){m.rejectionMu.Lock();m.rejection[r]++;m.rejectionMu.Unlock()}

type partition struct{q *Queue;seq atomic.Uint64}
type Pipeline struct{cfg Config;router *Router;dedup *Dedup;wal *WAL;committer *Committer;metrics *Metrics;parts []partition;ctx context.Context;cancel context.CancelFunc;submitMu sync.Mutex;accepting bool;wg sync.WaitGroup;startOnce,stopOnce sync.Once}
func NewPipeline(cfg Config,backend CommitBackend)(*Pipeline,error){if e:=cfg.Validate();e!=nil{return nil,e};r,_:=NewRouter(cfg.Partitions);d,_:=NewDedup(cfg.DedupCapacity,cfg.DedupTTL);w,e:=OpenWAL(cfg.WALPath,cfg.SyncWAL);if e!=nil{return nil,e};ctx,cancel:=context.WithCancel(context.Background());p:=&Pipeline{cfg:cfg,router:r,dedup:d,wal:w,committer:NewCommitter(backend,cfg.Retry,cfg.CommitTimeout),metrics:newMetrics(cfg.Partitions),parts:make([]partition,cfg.Partitions),ctx:ctx,cancel:cancel,accepting:true};for i:=range p.parts{p.parts[i].q,_=NewQueue(cfg.QueueCapacity)};p.start();return p,nil}
func(p *Pipeline)start(){p.startOnce.Do(func(){for i:=range p.parts{p.wg.Add(1);go p.worker(i)}})}
func(p *Pipeline)worker(idx int){defer p.wg.Done();ticker:=time.NewTicker(p.cfg.FlushInterval);defer ticker.Stop();var expected uint64;for{batch,closed:=p.parts[idx].q.PopBatch(p.cfg.BatchSize);if len(batch)>0{for _,tx:=range batch{if tx.Sequence!=expected+1{atomic.AddUint64(&p.metrics.partitions[idx].orderingErrors,1)};expected=tx.Sequence};p.flush(idx,batch)};if closed{return};select{case<-ticker.C:case<-p.ctx.Done():for{b,c:=p.parts[idx].q.PopBatch(p.cfg.BatchSize);if len(b)>0{for _,tx:=range b{if tx.Sequence!=expected+1{atomic.AddUint64(&p.metrics.partitions[idx].orderingErrors,1)};expected=tx.Sequence};p.flush(idx,b);continue};if c{return};break}};atomic.StoreUint64(&p.metrics.partitions[idx].queueDepth,uint64(p.parts[idx].q.Len()))}}
func(p *Pipeline)flush(idx int,batch []Tx){atomic.StoreUint64(&p.metrics.partitions[idx].batchSize,uint64(len(batch)));for _,tx:=range batch{start:=time.Now();err:=p.committer.Commit(p.ctx,tx);atomic.StoreUint64(&p.metrics.partitions[idx].retries,p.committer.RetryCount());if err!=nil{atomic.AddUint64(&p.metrics.partitions[idx].failed,1);continue};if p.wal!=nil{if err=p.wal.Commit(tx.ID);err!=nil{atomic.AddUint64(&p.metrics.partitions[idx].failed,1);continue}};atomic.AddUint64(&p.metrics.partitions[idx].committed,1);atomic.StoreUint64(&p.metrics.partitions[idx].commitLatencyNs,uint64(time.Since(start).Nanoseconds()))}}
func(p *Pipeline)Submit(ctx context.Context,tx Tx)(int,error){if e:=ctx.Err();e!=nil{return -1,e};if e:=ValidateTx(tx,p.cfg.MaxPayloadBytes);e!=nil{atomic.AddUint64(&p.metrics.partitions[0].rejected,1);p.metrics.reject(RejectInvalid);return -1,e};p.submitMu.Lock();defer p.submitMu.Unlock();if !p.accepting{p.metrics.reject(RejectStopped);return -1,ErrEngineStopped};if p.dedup.SeenOrAdd(tx.ID,time.Now()){p.metrics.reject(RejectDuplicate);return -1,ErrDuplicate};idx:=p.router.Partition(tx.Key);tx.Partition=idx;tx.Sequence=p.parts[idx].seq.Add(1);if e:=p.wal.Append(tx);e!=nil{p.dedup.Forget(tx.ID);return -1,e};if e:=p.parts[idx].q.Push(tx);e!=nil{p.dedup.Forget(tx.ID);p.metrics.reject(RejectQueueFull);atomic.AddUint64(&p.metrics.partitions[idx].rejected,1);return idx,e};atomic.AddUint64(&p.metrics.partitions[idx].accepted,1);atomic.StoreUint64(&p.metrics.partitions[idx].queueDepth,uint64(p.parts[idx].q.Len()));return idx,nil}
func(p *Pipeline)Backpressure(idx int)BackpressureLevel{if idx<0||idx>=len(p.parts){return Rejecting};return Level(p.parts[idx].q.Len(),p.parts[idx].q.Cap())}
func(p *Pipeline)Metrics()*Metrics{return p.metrics}
func(p *Pipeline)ReplayUncommitted()([]Tx,error){if p.wal==nil{return nil,nil};txs,e:=p.wal.Replay();if e!=nil{return nil,e};p.metrics.recovered.Add(uint64(len(txs)));return txs,nil}
func(p *Pipeline)Stop(){p.stopOnce.Do(func(){p.submitMu.Lock();p.accepting=false;for i:=range p.parts{p.parts[i].q.Close()};p.submitMu.Unlock();p.wg.Wait();p.cancel();if p.wal!=nil{_=p.wal.Close()}})}
