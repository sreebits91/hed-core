package hlf

import (
    "context"
    "sync"
    "sync/atomic"
    "time"

    "hed-core/pkg/engine"
)

type BatchConfig struct { MaxBatchSize int; FlushTimeout time.Duration; WorkerCount int }

type HLFCommitter struct {
    txQueue chan *engine.TxPayload
    committed uint64
    failed uint64
    cfg BatchConfig
    ctx context.Context
    cancel context.CancelFunc
    wg sync.WaitGroup
    lifecycleMu sync.RWMutex
    stopped bool
    stopOnce sync.Once
}

func NewHLFCommitter(cfg BatchConfig) *HLFCommitter {
    if cfg.MaxBatchSize <= 0 { cfg.MaxBatchSize = 2000 }
    if cfg.FlushTimeout <= 0 { cfg.FlushTimeout = 10 * time.Millisecond }
    if cfg.WorkerCount <= 0 { cfg.WorkerCount = 16 }
    ctx, cancel := context.WithCancel(context.Background())
    c := &HLFCommitter{txQueue: make(chan *engine.TxPayload, 200000), cfg: cfg, ctx: ctx, cancel: cancel}
    c.startWorkers(); return c
}

func (c *HLFCommitter) startWorkers() { for i := 0; i < c.cfg.WorkerCount; i++ { c.wg.Add(1); go c.workerLoop(i) } }

func (c *HLFCommitter) workerLoop(workerID int) {
    defer c.wg.Done()
    batch := make([]*engine.TxPayload, 0, c.cfg.MaxBatchSize)
    ticker := time.NewTicker(c.cfg.FlushTimeout); defer ticker.Stop()
    for {
        select {
        case <-c.ctx.Done():
            if len(batch) > 0 { c.flushBatch(batch) }
            return
        case tx, ok := <-c.txQueue:
            if !ok { if len(batch) > 0 { c.flushBatch(batch) }; return }
            if tx == nil { atomic.AddUint64(&c.failed, 1); continue }
            batch = append(batch, tx)
            if len(batch) >= c.cfg.MaxBatchSize { c.flushBatch(batch); batch = make([]*engine.TxPayload, 0, c.cfg.MaxBatchSize) }
        case <-ticker.C:
            if len(batch) > 0 { c.flushBatch(batch); batch = make([]*engine.TxPayload, 0, c.cfg.MaxBatchSize) }
        }
    }
}

// SubmitTx is safe against concurrent Stop. The lifecycle read lock prevents
// the channel from being closed while this method is evaluating/sending.
func (c *HLFCommitter) SubmitTx(tx *engine.TxPayload) bool {
    if tx == nil { atomic.AddUint64(&c.failed, 1); return false }
    c.lifecycleMu.RLock(); defer c.lifecycleMu.RUnlock()
    if c.stopped { atomic.AddUint64(&c.failed, 1); return false }
    select { case c.txQueue <- tx: return true; default: atomic.AddUint64(&c.failed, 1); return false }
}

func (c *HLFCommitter) flushBatch(batch []*engine.TxPayload) {
    // NOTE: this remains a local benchmark counter, not a Fabric commit receipt.
    atomic.AddUint64(&c.committed, uint64(len(batch)))
}
func (c *HLFCommitter) TotalCommitted() uint64 { return atomic.LoadUint64(&c.committed) }
func (c *HLFCommitter) TotalFailed() uint64 { return atomic.LoadUint64(&c.failed) }

// Stop closes the producer side first and lets workers drain queued work. The
// lifecycle lock prevents concurrent producers from sending to a closed channel.
func (c *HLFCommitter) Stop() {
    c.stopOnce.Do(func() {
        c.lifecycleMu.Lock()
        c.stopped = true
        close(c.txQueue)
        c.lifecycleMu.Unlock()
        c.wg.Wait()
        c.cancel()
    })
}
