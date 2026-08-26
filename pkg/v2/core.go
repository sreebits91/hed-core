package v2

import (
    "bufio"
    "context"
    "encoding/hex"
    "encoding/json"
    "errors"
    "fmt"
    "hash/fnv"
    "os"
    "sync"
    "sync/atomic"
    "time"
    "hash/crc32"
)

var (
    ErrInvalidTxID       = errors.New("invalid transaction id")
    ErrPayloadTooLarge   = errors.New("payload exceeds configured limit")
    ErrQueueFull         = errors.New("queue is full")
    ErrEngineStopped     = errors.New("engine is stopped")
    ErrDuplicate         = errors.New("duplicate transaction")
    ErrInvalidConfig     = errors.New("invalid configuration")
    ErrWALCorrupt        = errors.New("corrupt WAL record")
)

type Tx struct {
    ID        string `json:"id"`
    Key       string `json:"key"`
    Payload   []byte `json:"payload"`
    Partition int    `json:"partition"`
    Sequence  uint64 `json:"sequence"`
}

type Config struct {
    Partitions      int
    QueueCapacity   int
    BatchSize       int
    FlushInterval   time.Duration
    MaxPayloadBytes int
    DedupCapacity   int
    DedupTTL        time.Duration
    CommitTimeout   time.Duration
    Retry           RetryPolicy
    WALPath         string
    SyncWAL         bool
}

func DefaultConfig() Config {
    return Config{Partitions: 8, QueueCapacity: 65536, BatchSize: 256, FlushInterval: 2 * time.Millisecond, MaxPayloadBytes: 1 << 20, DedupCapacity: 1_000_000, DedupTTL: 10 * time.Minute, CommitTimeout: 5 * time.Second, Retry: RetryPolicy{MaxAttempts: 5, InitialBackoff: 2 * time.Millisecond, MaxBackoff: 250 * time.Millisecond}}
}

func (c Config) Validate() error {
    if c.Partitions <= 0 || c.QueueCapacity <= 0 || c.BatchSize <= 0 || c.FlushInterval <= 0 || c.MaxPayloadBytes <= 0 || c.DedupCapacity <= 0 || c.DedupTTL <= 0 || c.CommitTimeout <= 0 {
        return ErrInvalidConfig
    }
    if c.Retry.MaxAttempts <= 0 || c.Retry.InitialBackoff <= 0 || c.Retry.MaxBackoff < c.Retry.InitialBackoff {
        return ErrInvalidConfig
    }
    return nil
}

func ValidateTx(tx Tx, maxPayload int) error {
    if len(tx.ID) == 0 || len(tx.ID) > 256 || tx.ID != string([]byte(tx.ID)) {
        return ErrInvalidTxID
    }
    for _, r := range tx.ID {
        if !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') && r != '-' && r != '_' && r != '.' {
            return ErrInvalidTxID
        }
    }
    if maxPayload <= 0 || len(tx.Payload) > maxPayload {
        return ErrPayloadTooLarge
    }
    return nil
}

type Router struct{ partitions int }
func NewRouter(partitions int) (*Router, error) { if partitions <= 0 { return nil, ErrInvalidConfig }; return &Router{partitions: partitions}, nil }
func (r *Router) Partition(key string) int { h := fnv.New64a(); _, _ = h.Write([]byte(key)); return int(h.Sum64() % uint64(r.partitions)) }
func (r *Router) PartitionCount() int { return r.partitions }

// Queue is a bounded MPMC queue. It deliberately uses a short critical section
// instead of the previous unsafe head-publication ring implementation: an item
// is visible to consumers only after its payload is fully published.
type Queue struct { mu sync.Mutex; items []Tx; cap int; closed bool }
func NewQueue(capacity int) (*Queue, error) { if capacity <= 0 { return nil, ErrInvalidConfig }; return &Queue{items: make([]Tx, 0, capacity), cap: capacity}, nil }
func (q *Queue) Push(tx Tx) error { q.mu.Lock(); defer q.mu.Unlock(); if q.closed { return ErrEngineStopped }; if len(q.items) >= q.cap { return ErrQueueFull }; q.items = append(q.items, tx); return nil }
func (q *Queue) PopBatch(max int) ([]Tx, bool) { q.mu.Lock(); defer q.mu.Unlock(); if len(q.items)==0 { return nil, q.closed }; if max<=0 || max>len(q.items) { max=len(q.items) }; out:=make([]Tx,max); copy(out,q.items[:max]); copy(q.items,q.items[max:]); q.items=q.items[:len(q.items)-max]; return out,false }
func (q *Queue) Close() { q.mu.Lock(); q.closed=true; q.mu.Unlock() }
func (q *Queue) Len() int { q.mu.Lock(); defer q.mu.Unlock(); return len(q.items) }
func (q *Queue) Cap() int { return q.cap }

 type BackpressureLevel string
const ( Normal BackpressureLevel = "NORMAL"; Busy BackpressureLevel = "BUSY"; Saturated BackpressureLevel = "SATURATED"; Rejecting BackpressureLevel = "REJECTING" )
type RejectReason string
const ( RejectQueueFull RejectReason = "QUEUE_FULL"; RejectStopped RejectReason = "ENGINE_STOPPED"; RejectInvalid RejectReason = "INVALID_TRANSACTION"; RejectDuplicate RejectReason = "DUPLICATE_TRANSACTION" )
func Level(depth, capacity int) BackpressureLevel { if capacity<=0 || depth>=capacity { return Rejecting }; ratio:=float64(depth)/float64(capacity); if ratio>=0.8{return Saturated}; if ratio>=0.5{return Busy}; return Normal }

 type dedupEntry struct { expires time.Time }
type Dedup struct { mu sync.Mutex; m map[string]dedupEntry; order []string; capacity int; ttl time.Duration }
func NewDedup(capacity int, ttl time.Duration) (*Dedup,error) { if capacity<=0||ttl<=0{return nil,ErrInvalidConfig}; return &Dedup{m:make(map[string]dedupEntry,capacity),capacity:capacity,ttl:ttl},nil }
func (d *Dedup) SeenOrAdd(id string, now time.Time) bool { d.mu.Lock(); defer d.mu.Unlock(); d.expireLocked(now); if _,ok:=d.m[id];ok{return true}; if len(d.m)>=d.capacity { old:=d.order[0]; d.order=d.order[1:]; delete(d.m,old) }; d.m[id]=dedupEntry{expires:now.Add(d.ttl)}; d.order=append(d.order,id); return false }
func (d *Dedup) expireLocked(now time.Time) { i:=0; for i<len(d.order) { id:=d.order[i]; e,ok:=d.m[id]; if !ok || now.After(e.expires) { delete(d.m,id); i++; continue }; break }; if i>0 { d.order=append([]string(nil),d.order[i:]...) } }

 type WALRecord struct { Kind string `json:"kind"`; Tx Tx `json:"tx,omitempty"`; ID string `json:"id,omitempty"`; Checksum string `json:"checksum"` }
type WAL struct { mu sync.Mutex; f *os.File; syncOnWrite bool }
func OpenWAL(path string, syncOnWrite bool) (*WAL,error) { if path=="" { return nil,nil }; f,err:=os.OpenFile(path,os.O_CREATE|os.O_RDWR|os.O_APPEND,0600); if err!=nil{return nil,err}; return &WAL{f:f,syncOnWrite:syncOnWrite},nil }
func checksum(kind string, tx Tx, id string) string { b,_:=json.Marshal(struct{K string;T Tx;I string}{kind,tx,id}); c:=crc32.ChecksumIEEE(b); return hex.EncodeToString([]byte(fmt.Sprintf("%08x",c))) }
func (w *WAL) appendRecord(r WALRecord) error { b,_:=json.Marshal(r); if _,err:=w.f.Write(append(b,'\n'));err!=nil{return err}; if w.syncOnWrite{return w.f.Sync()};return nil }
func (w *WAL) Append(tx Tx) error { if w==nil{return nil}; w.mu.Lock(); defer w.mu.Unlock(); return w.appendRecord(WALRecord{Kind:"prepare",Tx:tx,Checksum:checksum("prepare",tx,"")}) }
func (w *WAL) Commit(id string) error { if w==nil{return nil}; w.mu.Lock(); defer w.mu.Unlock(); return w.appendRecord(WALRecord{Kind:"commit",ID:id,Checksum:checksum("commit",Tx{},id)}) }
func (w *WAL) Close() error { if w==nil{return nil}; w.mu.Lock(); defer w.mu.Unlock(); return w.f.Close() }
func (w *WAL) Replay() ([]Tx,error) { if w==nil{return nil,nil}; w.mu.Lock(); defer w.mu.Unlock(); if _,err:=w.f.Seek(0,0);err!=nil{return nil,err}; s:=bufio.NewScanner(w.f); pending:=make(map[string]Tx); for s.Scan(){ line:=s.Bytes(); var r WALRecord; if err:=json.Unmarshal(line,&r);err!=nil { if s.Err()!=nil{return nil,s.Err()}; continue }; if r.Checksum!=checksum(r.Kind,r.Tx,r.ID){ return nil,ErrWALCorrupt }; switch r.Kind { case "prepare": pending[r.Tx.ID]=r.Tx; case "commit": delete(pending,r.ID); default:return nil,ErrWALCorrupt } }; if err:=s.Err();err!=nil{return nil,err}; out:=make([]Tx,0,len(pending)); for _,tx:=range pending{out=append(out,tx)}; return out,nil }

 type RetryPolicy struct { MaxAttempts int; InitialBackoff, MaxBackoff time.Duration }
type CommitBackend interface { Commit(context.Context, Tx) error }
type PermanentError struct{ Err error }
func (e PermanentError) Error() string { return e.Err.Error() }
func (e PermanentError) Unwrap() error { return e.Err }
type Committer struct { backend CommitBackend; policy RetryPolicy; timeout time.Duration; retries atomic.Uint64 }
func NewCommitter(b CommitBackend, p RetryPolicy, timeout time.Duration) *Committer { return &Committer{backend:b,policy:p,timeout:timeout} }
func (c *Committer) Commit(ctx context.Context, tx Tx) error { var err error; back:=c.policy.InitialBackoff; for attempt:=1;attempt<=c.policy.MaxAttempts;attempt++ { callCtx,cancel:=context.WithTimeout(ctx,c.timeout); err=c.backend.Commit(callCtx,tx); cancel(); if err==nil{return nil}; var pe PermanentError; if errors.As(err,&pe){return err}; if attempt==c.policy.MaxAttempts{break}; c.retries.Add(1); timer:=time.NewTimer(back); select{case <-ctx.Done(): timer.Stop(); return ctx.Err(); case <-timer.C:}; back*=2; if back>c.policy.MaxBackoff{back=c.policy.MaxBackoff} }; return err }
func (c *Committer) RetryCount() uint64 { return c.retries.Load() }

 type PartitionMetrics struct { accepted, committed, failed, retries, rejected, queueDepth, batchSize, commitLatencyNs uint64 }
type Metrics struct { partitions []PartitionMetrics; rejectionMu sync.Mutex; rejection map[RejectReason]uint64; recovered atomic.Uint64 }
func newMetrics(n int)*Metrics{m:=&Metrics{partitions:make([]PartitionMetrics,n),rejection:make(map[RejectReason]uint64)};return m}
func(m *Metrics) Snapshot() map[string]any { out:=map[string]any{}; parts:=make([]map[string]uint64,len(m.partitions)); for i:=range m.partitions {p:=&m.partitions[i]; parts[i]=map[string]uint64{"accepted":atomic.LoadUint64(&p.accepted),"committed":atomic.LoadUint64(&p.committed),"failed":atomic.LoadUint64(&p.failed),"retries":atomic.LoadUint64(&p.retries),"rejected":atomic.LoadUint64(&p.rejected),"queue_depth":atomic.LoadUint64(&p.queueDepth),"batch_size":atomic.LoadUint64(&p.batchSize),"commit_latency_ns":atomic.LoadUint64(&p.commitLatencyNs)}};out["partitions"]=parts;out["recovered"]=m.recovered.Load();return out }
func(m *Metrics) reject(r RejectReason){m.rejectionMu.Lock();m.rejection[r]++;m.rejectionMu.Unlock()}

 type partition struct { q *Queue; seq atomic.Uint64; expected uint64 }
type Pipeline struct { cfg Config; router *Router; dedup *Dedup; wal *WAL; committer *Committer; metrics *Metrics; parts []partition; ctx context.Context; cancel context.CancelFunc; submitMu sync.Mutex; accepting bool; wg sync.WaitGroup; startOnce sync.Once; stopOnce sync.Once }
func NewPipeline(cfg Config, backend CommitBackend) (*Pipeline,error) { if err:=cfg.Validate();err!=nil{return nil,err}; r,_:=NewRouter(cfg.Partitions); d,_:=NewDedup(cfg.DedupCapacity,cfg.DedupTTL); w,err:=OpenWAL(cfg.WALPath,cfg.SyncWAL);if err!=nil{return nil,err};ctx,cancel:=context.WithCancel(context.Background());p:=&Pipeline{cfg:cfg,router:r,dedup:d,wal:w,committer:NewCommitter(backend,cfg.Retry,cfg.CommitTimeout),metrics:newMetrics(cfg.Partitions),parts:make([]partition,cfg.Partitions),ctx:ctx,cancel:cancel,accepting:true};for i:=range p.parts{p.parts[i].q,_=NewQueue(cfg.QueueCapacity)};p.start();return p,nil }
func(p *Pipeline) start(){p.startOnce.Do(func(){for i:=range p.parts{p.wg.Add(1);go p.worker(i)}})}
func(p *Pipeline) worker(idx int){defer p.wg.Done(); ticker:=time.NewTicker(p.cfg.FlushInterval);defer ticker.Stop();for{batch,closed:=p.parts[idx].q.PopBatch(p.cfg.BatchSize);if len(batch)>0{p.flush(idx,batch)};if closed{return};select{case <-p.ctx.Done(): for {b,c:=p.parts[idx].q.PopBatch(p.cfg.BatchSize);if len(b)>0{p.flush(idx,b);continue};if c{return};break}; case <-ticker.C: };atomic.StoreUint64(&p.metrics.partitions[idx].queueDepth,uint64(p.parts[idx].q.Len())) }}
func(p *Pipeline) flush(idx int,batch []Tx){atomic.StoreUint64(&p.metrics.partitions[idx].batchSize,uint64(len(batch)));for _,tx:=range batch{start:=time.Now();err:=p.committer.Commit(p.ctx,tx);atomic.StoreUint64(&p.metrics.partitions[idx].retries,p.committer.RetryCount());if err!=nil{atomic.AddUint64(&p.metrics.partitions[idx].failed,1);continue};if p.wal!=nil {if err=p.wal.Commit(tx.ID);err!=nil{atomic.AddUint64(&p.metrics.partitions[idx].failed,1);continue}};atomic.AddUint64(&p.metrics.partitions[idx].committed,1);atomic.StoreUint64(&p.metrics.partitions[idx].commitLatencyNs,uint64(time.Since(start).Nanoseconds()))}}
func(p *Pipeline) Submit(ctx context.Context,tx Tx)(int,error){if err:=ValidateTx(tx,p.cfg.MaxPayloadBytes);err!=nil{p.metrics.reject(RejectInvalid);return -1,err};p.submitMu.Lock();defer p.submitMu.Unlock();if !p.accepting{p.metrics.reject(RejectStopped);return -1,ErrEngineStopped};if p.dedup.SeenOrAdd(tx.ID,time.Now()){p.metrics.reject(RejectDuplicate);return -1,ErrDuplicate};idx:=p.router.Partition(tx.Key);tx.Partition=idx;tx.Sequence=p.parts[idx].seq.Add(1);if err:=p.wal.Append(tx);err!=nil{return -1,err};if err:=p.parts[idx].q.Push(tx);err!=nil{p.metrics.reject(RejectQueueFull);return idx,err};atomic.AddUint64(&p.metrics.partitions[idx].accepted,1);atomic.StoreUint64(&p.metrics.partitions[idx].queueDepth,uint64(p.parts[idx].q.Len()));return idx,nil}
func(p *Pipeline) Backpressure(idx int) BackpressureLevel{if idx<0||idx>=len(p.parts){return Rejecting};return Level(p.parts[idx].q.Len(),p.parts[idx].q.Cap())}
func(p *Pipeline) Metrics()*Metrics{return p.metrics}
func(p *Pipeline) ReplayUncommitted()([]Tx,error){txs,err:=p.wal.Replay();if err!=nil{return nil,err};p.metrics.recovered.Add(uint64(len(txs)));return txs,nil}
func(p *Pipeline) Stop(){p.stopOnce.Do(func(){p.submitMu.Lock();p.accepting=false;for i:=range p.parts{p.parts[i].q.Close()};p.submitMu.Unlock();p.wg.Wait();p.cancel();if p.wal!=nil{_ = p.wal.Close()}})}
